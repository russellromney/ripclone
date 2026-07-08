use crate::bench::Benchmark;
use crate::cas::{Cas, hash as cas_hash};
use crate::clonepack::{ChunkRef, ClonepackManifest, MetadataChunk, PackEntry, hash_to_hex};
use crate::extract::{extract_archive_from_chunk_receiver, extract_clonepack_streaming};
use crate::git;
use crate::mode::CloneMode;
use crate::overlay;
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use prost::Message;
use serde::Deserialize;
use sha2::Digest as Sha2Digest;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

mod tuning;
use tuning::ClientTuning;

/// Sent on every request so the server can attribute usage and nudge upgrades.
const USER_AGENT: &str = concat!("ripclone/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Deserialize)]
struct ServerError {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    code: Option<String>,
}

/// A presigned artifact URL failed (most likely expired mid-clone). The bytes
/// are served ONLY by the signed URLs in the ref response — the managed cloud no
/// longer serves content by bare hash — so the right recovery is to re-resolve
/// the ref for fresh URLs and retry, which also re-runs the server's access
/// check. Surfaced as a typed error so the clone driver can detect it.
#[derive(Debug)]
pub struct StaleSignedUrl;

impl std::fmt::Display for StaleSignedUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a presigned artifact URL failed (likely expired); re-resolve the ref for fresh URLs"
        )
    }
}

impl std::error::Error for StaleSignedUrl {}

/// True if `err` (or any cause in its chain) is a [`StaleSignedUrl`].
pub fn is_stale_signed_url(err: &anyhow::Error) -> bool {
    err.chain().any(|e| e.is::<StaleSignedUrl>())
}

/// The clone driver's retry decision, factored out so it is unit-testable: retry
/// the next attempt only when this one failed with a stale signed URL and we are
/// still under `max_retries`. `attempt` is the number of retries already taken
/// (0 on the first failure).
pub fn should_retry_stale(attempt: u32, max_retries: u32, err: &anyhow::Error) -> bool {
    attempt < max_retries && is_stale_signed_url(err)
}

/// The innermost cause of a reqwest transport error — e.g. "Connection refused
/// (os error 61)" — without the noisy "error sending request for url (...)"
/// wrapper that hides the real reason a first-run user can't reach the server.
fn transport_cause(e: &reqwest::Error) -> String {
    let mut src: &dyn std::error::Error = e;
    while let Some(next) = src.source() {
        src = next;
    }
    src.to_string()
}

/// Turn a non-success HTTP response into a clear, actionable error. Parses the
/// `{ "error", "code" }` body the gateway returns and appends a next-step hint
/// keyed on status/code. Surfaces an upgrade nudge from `X-Ripclone-Upgrade`.
async fn server_error(context: &str, resp: reqwest::Response) -> anyhow::Error {
    let status = resp.status();
    let origin = resp.url().origin().unicode_serialization();
    let is_cloud = origin == "https://ripclone.com";
    let upgrade = resp
        .headers()
        .get("x-ripclone-upgrade")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let text = resp.text().await.unwrap_or_default();
    let parsed: Option<ServerError> = serde_json::from_str(&text).ok();
    let code = parsed.as_ref().and_then(|p| p.code.as_deref());
    let msg: String = parsed
        .as_ref()
        .and_then(|p| p.error.clone())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            if text.is_empty() {
                status.to_string()
            } else {
                text.clone()
            }
        });
    let hint = error_hint(status.as_u16(), code, is_cloud);
    if let Some(u) = upgrade {
        eprintln!("ripclone: {u}");
    }
    anyhow::anyhow!("{context}: {msg}{hint}")
}

/// Next-step hint for a failed server response, keyed on HTTP status, the
/// gateway's `code`, and whether we are talking to the managed cloud. Pure so
/// the paywall path stays testable: a paid-plan block (403 `no_plan`) must
/// carry the machine-parseable `code` plus the subscribe URL so an agent fleet
/// can detect and route it without scraping prose.
fn error_hint(status: u16, code: Option<&str>, is_cloud: bool) -> &'static str {
    match (status, code, is_cloud) {
        (401, _, true) => "\n  → run `ripclone login`",
        (401, _, false) => {
            "\n  → run `ripclone login --server <server>` or set RIPCLONE_SERVER_TOKEN"
        }
        (402, _, _) => "\n  → this repo needs a paid plan; subscribe at https://ripclone.com",
        (403, Some("no_plan"), true) => {
            "\n  → this org needs a plan; the owner can subscribe at https://ripclone.com"
        }
        (403, Some("no_access"), true) => "\n  → you don't have GitHub access to this repo",
        (403, _, true) => "\n  → the org may need a plan, or you lack GitHub access",
        (403, _, false) => "\n  → access denied by the configured server",
        (429, _, _) => "\n  → rate limited; wait a moment and retry",
        (404, Some("repo_not_added"), _) => "\n  → run `ripclone add <repo>`",
        (502 | 503, _, _) => "\n  → ripclone is briefly unavailable; retry shortly",
        _ => "",
    }
}

/// Build a reqwest client that always sends our User-Agent (and any default
/// headers, e.g. the auth token).
fn build_http_client(headers: reqwest::header::HeaderMap) -> reqwest::Client {
    // Fail loudly if the client can't be built: the old fallback to
    // `Client::new()` silently dropped the default headers (including auth), so
    // every request would go out unauthenticated. A build failure here is a
    // TLS/config problem worth surfacing at startup, not papering over.
    reqwest::ClientBuilder::new()
        .user_agent(USER_AGENT)
        .default_headers(headers)
        .build()
        .expect("build HTTP client")
}

#[derive(Debug, Clone, Deserialize)]
pub struct RefResponse {
    pub owner: String,
    pub repo: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub origin_url: String,
    pub branch: String,
    pub default_branch: String,
    pub commit: String,
    pub parent_commit: Option<String>,
    pub full_pack: String,
    #[serde(default)]
    pub clonepack_manifest: String,
    #[serde(default)]
    pub clonepack_manifest_url: Option<String>,
    #[serde(default)]
    pub metadata_chunk: String,
    #[serde(default)]
    pub metadata_chunk_url: Option<String>,
    #[serde(default)]
    pub archive_chunk_urls: Option<Vec<Option<String>>>,
    #[serde(default)]
    pub head_blobs_chunk_urls: Option<Vec<Option<String>>>,
    #[serde(default)]
    pub head_blobs_idx_url: Option<String>,
    /// Signed URL for each editable pack, ordered to match `manifest.packs`.
    #[serde(default)]
    pub pack_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for each editable pack's idx, ordered to match `manifest.packs`.
    #[serde(default)]
    pub pack_idx_urls: Option<Vec<Option<String>>>,
    /// Signed URL for the pre-built multi-pack-index (`manifest.midx`).
    #[serde(default)]
    pub midx_url: Option<String>,
    /// Signed URL for the concatenated idx bundle (`manifest.idx_bundle`).
    #[serde(default)]
    pub idx_bundle_url: Option<String>,
    /// True when the returned clonepack is a shallow (depth=1) snapshot.
    #[serde(default)]
    pub shallow: bool,
    /// True once the full clonepack's archive is built (files mode can clone).
    /// Defaults true so an older server that always shipped the archive is treated
    /// as ready.
    #[serde(default = "ref_archive_ready_default")]
    pub archive_ready: bool,
    /// The managed cloud's per-clone id, captured from the `X-Ripclone-Clone-Id`
    /// response header (not part of the JSON body). `None` for a self-hosted or
    /// older server that doesn't mint one — in that case the post-clone metrics
    /// report is skipped entirely.
    #[serde(skip)]
    pub clone_id: Option<String>,
    /// True when resolving this ref required a 202/poll (a cold build) rather
    /// than hitting an already-warm repo. Captured from the resolve loop, not the
    /// JSON body.
    #[serde(skip)]
    pub cold: bool,
}

fn ref_archive_ready_default() -> bool {
    true
}

/// Return the chunk refs that make up the head-blobs pack, falling back to the
/// deprecated single-pack field for older manifests.
#[allow(deprecated)]
pub(crate) fn head_blobs_chunk_refs(
    clonepack: &ClonepackManifest,
) -> Vec<crate::clonepack::ChunkRef> {
    if !clonepack.head_blobs_chunks.is_empty() {
        clonepack.head_blobs_chunks.clone()
    } else if let Some(pack) = &clonepack.head_blobs_pack {
        vec![pack.clone()]
    } else {
        Vec::new()
    }
}

/// `(max_attempts, base_backoff_ms)` for artifact downloads, from the
/// environment. Defaults: 3 attempts, 100 ms base backoff.
fn fetch_retry_config() -> (u32, u64) {
    let attempts = std::env::var("RIPCLONE_FETCH_MAX_ATTEMPTS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(3)
        .max(1);
    let backoff_ms = std::env::var("RIPCLONE_FETCH_BACKOFF_MS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(100);
    (attempts, backoff_ms)
}

/// Exponential backoff with jitter for retry `attempt` (1-based), capped at 5 s.
/// Jitter (in `[capped/2, capped]`) decorrelates the retries of concurrent
/// fetches so they don't hammer a recovering server in lockstep.
fn fetch_backoff(base_ms: u64, attempt: u32) -> std::time::Duration {
    let mult = 1u64 << attempt.saturating_sub(1).min(16);
    let capped = base_ms.saturating_mul(mult).min(5_000);
    if capped == 0 {
        return std::time::Duration::from_millis(0);
    }
    let half = capped / 2;
    let span = capped - half;
    let jitter = pseudo_rand_u64() % (span + 1);
    std::time::Duration::from_millis(half + jitter)
}

/// Cheap thread-local pseudo-randomness (xorshift64) for backoff jitter, so we
/// don't pull in a `rand` dependency for this.
fn pseudo_rand_u64() -> u64 {
    use std::cell::Cell;
    thread_local!(static STATE: Cell<u64> = const { Cell::new(0) });
    STATE.with(|s| {
        let mut x = s.get();
        if x == 0 {
            x = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9E37_79B9_7F4A_7C15)
                | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x
    })
}

/// Run the download-with-retry loop against a specific client and URL.
async fn fetch_artifact_with_retry(
    client: &reqwest::Client,
    url: &str,
    hash: &str,
) -> Result<bytes::Bytes> {
    let (max_attempts, base_backoff_ms) = fetch_retry_config();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match fetch_artifact_once(client, url, hash).await {
            Ok(bytes) => return Ok(bytes),
            Err((retryable, err)) => {
                if retryable && attempt < max_attempts {
                    let backoff = fetch_backoff(base_backoff_ms, attempt);
                    tracing::debug!(
                        "artifact {hash} fetch attempt {attempt}/{max_attempts} failed: {err:#}; retrying in {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
}

/// Fetch an artifact once and verify its hash. Returns `(retryable, error)` on
/// failure so the caller can decide whether to retry.
async fn fetch_artifact_once(
    client: &reqwest::Client,
    fetch_url: &str,
    hash: &str,
) -> std::result::Result<bytes::Bytes, (bool, anyhow::Error)> {
    let resp = match client.get(fetch_url).send().await {
        Ok(r) => r,
        // Transport errors (connect/reset/timeout) are transient.
        Err(e) => return Err((true, anyhow::anyhow!("artifact fetch transport error: {e}"))),
    };
    let status = resp.status();
    if !status.is_success() {
        let retryable = status.is_server_error()
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::REQUEST_TIMEOUT;
        return Err((
            retryable,
            anyhow::anyhow!("artifact fetch failed: {status}"),
        ));
    }
    // R1: keep the body as `Bytes` (a refcounted buffer) instead of copying it
    // into a fresh Vec — it flows through the cache and on to the consumer
    // (decompress/write, which read `&[u8]`) without a second per-artifact copy.
    let data = match resp.bytes().await {
        Ok(b) => b,
        // A body read can fail mid-stream; retry.
        Err(e) => return Err((true, anyhow::anyhow!("artifact body read error: {e}"))),
    };
    // Content-addressed artifacts must match their hash. A full body with the
    // wrong hash is deterministic corruption (retrying re-fetches the same
    // bytes), so treat it as permanent. Genuine truncation surfaces as a
    // transport/body-read error above, which *is* retried.
    let actual_hash = crate::cas::hash(&data);
    if actual_hash != hash {
        return Err((
            false,
            anyhow::anyhow!("artifact hash mismatch: expected {hash}, got {actual_hash}"),
        ));
    }
    Ok(data)
}

async fn fetch_artifact_to_temp_with_retry(
    client: &reqwest::Client,
    url: &str,
    hash: &str,
    dir: &Path,
) -> Result<(tempfile::NamedTempFile, u64)> {
    let (max_attempts, base_backoff_ms) = fetch_retry_config();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match fetch_artifact_to_temp_once(client, url, hash, dir).await {
            Ok(tmp) => return Ok(tmp),
            Err((retryable, err)) => {
                if retryable && attempt < max_attempts {
                    let backoff = fetch_backoff(base_backoff_ms, attempt);
                    tracing::debug!(
                        "artifact {hash} streaming fetch attempt {attempt}/{max_attempts} failed: {err:#}; retrying in {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
}

async fn fetch_artifact_to_temp_once(
    client: &reqwest::Client,
    fetch_url: &str,
    hash: &str,
    dir: &Path,
) -> std::result::Result<(tempfile::NamedTempFile, u64), (bool, anyhow::Error)> {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;

    let resp = match client.get(fetch_url).send().await {
        Ok(r) => r,
        Err(e) => return Err((true, anyhow::anyhow!("artifact fetch transport error: {e}"))),
    };
    let status = resp.status();
    if !status.is_success() {
        let retryable = status.is_server_error()
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::REQUEST_TIMEOUT;
        return Err((
            retryable,
            anyhow::anyhow!("artifact streaming fetch failed: {status}"),
        ));
    }

    let tmp = tempfile::Builder::new()
        .suffix(".ripclone-download")
        .tempfile_in(dir)
        .map_err(|e| {
            (
                false,
                anyhow::Error::new(e).context("create artifact temp file"),
            )
        })?;
    let std_file = tmp.as_file().try_clone().map_err(|e| {
        (
            false,
            anyhow::Error::new(e).context("clone artifact temp file"),
        )
    })?;
    let mut file = tokio::fs::File::from_std(std_file);
    let mut stream = resp.bytes_stream();
    let mut hasher = sha2::Sha256::new();
    let mut len = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(e) => return Err((true, anyhow::anyhow!("artifact body read error: {e}"))),
        };
        hasher.update(&chunk);
        len += chunk.len() as u64;
        if let Err(e) = file.write_all(&chunk).await {
            return Err((
                false,
                anyhow::Error::new(e).context("write artifact temp file"),
            ));
        }
    }
    if let Err(e) = file.flush().await {
        return Err((
            false,
            anyhow::Error::new(e).context("flush artifact temp file"),
        ));
    }
    drop(file);
    let actual = hex::encode(hasher.finalize());
    if actual != hash {
        return Err((
            false,
            anyhow::anyhow!("artifact hash mismatch: expected {hash}, got {actual}"),
        ));
    }
    Ok((tmp, len))
}

fn metadata_bytes(metadata: &MetadataChunk) -> u64 {
    // The metadata chunk is the protobuf encoding of skeleton pack/idx, index,
    // frame table, and file table. The actual encoded size is dominated by the
    // three byte blobs; add a small estimate for the repeated message overhead.
    (metadata.skeleton_pack.len()
        + metadata.skeleton_idx.len()
        + metadata.prebuilt_index.len()
        + metadata.frames.len() * 24
        + metadata.files.len() * 64) as u64
}

/// Create a temp install directory next to `target`. Returns the `TempDir`
/// handle (not a bare path): the caller must keep it alive so that on *any*
/// failure before the final rename, the partial install is removed on drop. On
/// success the dir is renamed onto `target`, after which the handle's drop is a
/// harmless no-op (the path no longer exists).
fn temp_install_dir(target: &Path) -> Result<tempfile::TempDir> {
    let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
    tempfile::Builder::new()
        .prefix(&format!(
            "{}.",
            target
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("ripclone"))
                .to_string_lossy()
        ))
        .suffix(".tmp")
        .tempdir_in(parent.unwrap_or_else(|| Path::new(".")))
        .context("create temp install directory")
}

/// True when `RIPCLONE_FSYNC` asks for a durability barrier before the clone
/// reports success. Off by default: the extra fsyncs add latency, and the clone
/// is already crash-consistent (temp dir + atomic rename). Turn it on when a
/// crash right after the clone must not leave a torn tree that `git status`
/// would call clean.
fn fsync_requested() -> bool {
    std::env::var("RIPCLONE_FSYNC")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// fsync one directory so its newly created entries survive a crash. On Unix
/// this opens the directory and syncs it; elsewhere it is a best-effort no-op.
#[cfg(unix)]
fn fsync_dir(path: &Path) -> Result<()> {
    let dir = std::fs::File::open(path)
        .with_context(|| format!("open dir for fsync {}", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("fsync dir {}", path.display()))
}

#[cfg(not(unix))]
fn fsync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

/// Recursively fsync every regular file and directory under `root`, so the whole
/// materialized tree is durable before the clone reports success. A symlink is
/// persisted by syncing its parent directory, not by following the link.
fn fsync_tree(root: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(root)
        .with_context(|| format!("stat for fsync {}", root.display()))?;
    if meta.is_dir() {
        for entry in
            std::fs::read_dir(root).with_context(|| format!("read dir {}", root.display()))?
        {
            fsync_tree(&entry?.path())?;
        }
        fsync_dir(root)?;
    } else if meta.is_file() {
        let f = std::fs::File::open(root)
            .with_context(|| format!("open file for fsync {}", root.display()))?;
        f.sync_all()
            .with_context(|| format!("fsync file {}", root.display()))?;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct SnapshotResponse {
    pub owner: String,
    pub repo: String,
    pub branch: String,
    pub commit: String,
    pub snapshot_hash: String,
    pub size: u64,
    pub hot_files: usize,
}

#[derive(Debug, Deserialize)]
pub struct HotfilesResponse {
    pub files: Vec<String>,
}

/// What a finished clone learned, for the best-effort post-clone metrics report.
/// Carries the managed cloud's per-clone id (when one was minted), the resolved
/// repo/commit, and the bytes/timing the client measured. The end-to-end wall
/// clock is supplied by the caller (the CLI), which owns the outer timer.
#[derive(Debug, Clone)]
pub struct CloneOutcome {
    pub provider: String,
    pub owner: String,
    pub name: String,
    pub commit: String,
    /// `files` | `depth1` | `full`.
    pub mode: &'static str,
    /// True when the resolve had to poll a cold build (202) before succeeding.
    pub cold: bool,
    /// The cloud's `X-Ripclone-Clone-Id`. `None` ⇒ self-hosted/older server ⇒
    /// no metrics report.
    pub clone_id: Option<String>,
    /// Total bytes downloaded (metadata + pack/archive chunks).
    pub bytes: u64,
}

#[derive(Clone)]
pub struct Client {
    server: String,
    /// Client that sends the ripclone auth token on every request.
    http: reqwest::Client,
    /// Client with no default auth headers, used for presigned URLs.
    raw_http: reqwest::Client,
    /// Full `Authorization` header value sent on `http` ("Ripclone <hash>" or
    /// "Bearer <jwt>"). Threaded into the streaming extractor so its separate
    /// blocking client authenticates the gateway artifact-fetch fallback the same
    /// way the main client does.
    auth_header: Option<String>,
    cache: Option<Cas>,
    /// Upstream git provider instance id (e.g. "github", "gitlab").
    provider: String,
    /// Upstream credential token sent as `X-Upstream-Token`.
    upstream_token: Option<String>,
    /// When true, suppress the post-clone metrics report regardless of env.
    skip_metrics: bool,
}

impl Client {
    pub fn new(server: String) -> Self {
        Self::new_with_token(server, None)
    }

    /// Create a client that sends the given token in the `Authorization`
    /// header for every request. The token is sent verbatim; callers that
    /// want the hashed ripclone token format should hash it before calling.
    ///
    /// Caching is opt-in. Set `RIPCLONE_CACHE_DIR=/path/to/cache` to enable a
    /// local artifact cache; otherwise no cache is used. `RIPCLONE_NO_CACHE=1`
    /// forcibly disables caching even when `RIPCLONE_CACHE_DIR` is set.
    pub fn new_with_token(server: String, token: Option<String>) -> Self {
        let cache_dir = if std::env::var_os("RIPCLONE_NO_CACHE").is_some() {
            None
        } else {
            std::env::var_os("RIPCLONE_CACHE_DIR").map(PathBuf::from)
        };
        Self::new_with_token_and_cache(server, token, cache_dir.as_deref())
    }

    pub fn new_with_token_and_cache(
        server: String,
        token: Option<String>,
        cache_dir: Option<&Path>,
    ) -> Self {
        let auth = token.as_ref().map(|t| format!("Ripclone {t}"));
        Self::new_with_auth(server, auth, cache_dir)
    }

    /// Create a client that authenticates with a `Bearer <jwt>` session token
    /// (from `ripclone auth login`) instead of the shared `Ripclone <hash>`
    /// scheme.
    pub fn new_with_bearer(server: String, jwt: String) -> Self {
        let cache_dir = if std::env::var_os("RIPCLONE_NO_CACHE").is_some() {
            None
        } else {
            std::env::var_os("RIPCLONE_CACHE_DIR").map(PathBuf::from)
        };
        let auth = Some(format!("Bearer {jwt}"));
        Self::new_with_auth(server, auth, cache_dir.as_deref())
    }

    fn new_with_auth(server: String, auth_value: Option<String>, cache_dir: Option<&Path>) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(value) = &auth_value
            && let Ok(header_value) = reqwest::header::HeaderValue::from_str(value)
        {
            headers.insert(reqwest::header::AUTHORIZATION, header_value);
        }
        // Advertise the wire protocol so the server can reject an incompatible
        // (too-new) client with an actionable error instead of a confusing 4xx.
        if let Ok(pv) = reqwest::header::HeaderValue::from_str(&crate::PROTOCOL_VERSION.to_string())
        {
            headers.insert("x-ripclone-protocol", pv);
        }
        let http = build_http_client(headers);
        let cache = cache_dir.and_then(|dir| Cas::new(dir).ok());
        Self {
            server,
            http,
            raw_http: build_http_client(reqwest::header::HeaderMap::new()),
            auth_header: auth_value,
            cache,
            provider: "github".to_string(),
            upstream_token: None,
            skip_metrics: false,
        }
    }

    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self
    }

    pub fn with_upstream_token(mut self, token: impl Into<String>) -> Self {
        self.upstream_token = Some(token.into());
        self
    }

    pub fn with_upstream_token_opt(mut self, token: Option<String>) -> Self {
        self.upstream_token = token;
        self
    }

    /// Suppress the fire-and-forget metrics report for clones made through this
    /// client. This is the `--no-metrics` path; `RIPCLONE_NO_METRICS` is still
    /// honored via `clone_metrics::opted_out`.
    pub fn with_metrics_disabled(mut self) -> Self {
        self.skip_metrics = true;
        self
    }

    fn cache_key_from_artifact_url(&self, url: &str) -> Option<String> {
        url.rsplit('/').next().map(|s| s.to_string())
    }

    /// Build a request URL for `repo_path`. GitHub repos keep the legacy
    /// `/v1/repos/{owner}/{repo}` shape; other providers are routed under
    /// `/v1/repos/{provider}/{repo_path}`.
    fn repo_url(&self, repo_path: &str, suffix: &str) -> String {
        format!(
            "{}/v1/repos/{}/{repo_path}{suffix}",
            self.server, self.provider
        )
    }

    /// Start a request against the ripclone server, attaching the upstream
    /// credential when one was configured.
    fn request(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.http.request(method, url);
        if let Some(token) = &self.upstream_token
            && let Ok(value) = reqwest::header::HeaderValue::from_str(token)
        {
            req = req.header("X-Upstream-Token", value);
        }
        req
    }

    /// Send a request, turning a transport-level failure — server unreachable,
    /// connection refused, DNS failure, timeout — into an actionable message that
    /// names the server. The most common first-run mistake is pointing at a
    /// server that isn't running (or a wrong `--server` / `RIPCLONE_SERVER`), and
    /// the bare reqwest chain ("connection refused (os error 61)") hides that.
    async fn send(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        req.send().await.map_err(|e| {
            if e.is_connect() || e.is_timeout() {
                anyhow::anyhow!(
                    "could not reach ripclone server at {}: {}\n  → is the server running? check --server / RIPCLONE_SERVER",
                    self.server,
                    transport_cause(&e),
                )
            } else {
                anyhow::Error::new(e).context("request to ripclone server failed")
            }
        })
    }
}

impl Client {
    pub async fn resolve_ref(&self, repo_path: &str, branch: &str) -> Result<RefResponse> {
        self.resolve_ref_with_clonepack(repo_path, branch, None, None)
            .await
    }

    pub async fn resolve_ref_with_clonepack(
        &self,
        repo_path: &str,
        branch: &str,
        clonepack: Option<&str>,
        rev: Option<&str>,
    ) -> Result<RefResponse> {
        let mut url = self.repo_url(repo_path, &format!("/refs/{branch}"));
        let mut q: Vec<String> = Vec::new();
        if let Some(kind) = clonepack {
            q.push(format!("clonepack={}", kind));
        }
        if let Some(r) = rev {
            q.push(format!("rev={}", urlencoding::encode(r)));
        }
        if !q.is_empty() {
            url.push('?');
            url.push_str(&q.join("&"));
        }
        // The cloud returns 202 while it warms a cold repo (the webhook-sync
        // queue builds it on demand), or 503 if its queue is briefly full. Poll
        // the same request until it's built, then read the ref.
        let max_attempts = 40usize;
        // Track whether any attempt polled a cold build (202/503) before
        // success, so the post-clone metrics report can label the clone cold.
        let mut polled = false;
        for attempt in 0..max_attempts {
            let resp = self.send(self.request(reqwest::Method::GET, &url)).await?;
            let status = resp.status();
            if status == reqwest::StatusCode::ACCEPTED
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                polled = true;
                if attempt == 0 {
                    eprintln!("ripclone: warming {repo_path} — this can take a moment…");
                }
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
                anyhow::bail!("{repo_path} is still building after {max_attempts} attempts");
            }
            if status.is_success() {
                // Capture the managed cloud's per-clone id from the response
                // header before the body is consumed. Absent on a self-hosted or
                // older server, which leaves `clone_id` None.
                let clone_id = resp
                    .headers()
                    .get("x-ripclone-clone-id")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let mut info: RefResponse = resp.json().await?;
                info.clone_id = clone_id;
                info.cold = polled;
                return Ok(info);
            }
            return Err(server_error("ref lookup failed", resp).await);
        }
        anyhow::bail!("ref lookup did not complete")
    }

    pub async fn fetch_object(&self, sha: &str) -> Result<Vec<u8>> {
        let url = format!("{}/v1/objects/{}", self.server, sha);
        let resp = self.send(self.http.get(&url)).await?;
        if !resp.status().is_success() {
            anyhow::bail!("object fetch failed: {}", resp.status());
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Fetch any content-addressed artifact (pack, idx, index, archive, manifest).
    ///
    /// Caches the bytes locally when `RIPCLONE_CACHE_DIR` is set, so repeat
    /// clones of the same repo/commit bypass the network entirely.
    pub async fn fetch_artifact(&self, hash: &str) -> Result<bytes::Bytes> {
        self.fetch_artifact_with_url(hash, None).await
    }

    /// Fetch an artifact, optionally using a pre-signed URL directly. Falls back
    /// to `/v1/artifacts/{hash}` when `signed_url` is `None`.
    pub async fn fetch_artifact_with_url(
        &self,
        hash: &str,
        signed_url: Option<&str>,
    ) -> Result<bytes::Bytes> {
        let gateway_url = format!("{}/v1/artifacts/{}", self.server, hash);
        let fetch_url = signed_url.unwrap_or(&gateway_url);
        let use_signed_url = signed_url.is_some();

        if let Some(cache) = &self.cache
            && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
            && let Ok(data) = cache.get(&key)
        {
            return Ok(data.into());
        }

        // Presigned URLs are self-authenticating, so use the no-auth client to
        // avoid leaking the ripclone token to object storage. On failure (most
        // likely expiry) we do NOT fall back to a by-hash gateway fetch — the
        // cloud no longer serves content by hash. Instead surface a typed
        // StaleSignedUrl so the clone driver re-resolves the ref for fresh URLs.
        // When there's no signed URL at all (a self-hosted backend without object
        // storage), the by-hash fetch against that backend IS the path.
        let data = if use_signed_url {
            fetch_artifact_with_retry(&self.raw_http, fetch_url, hash)
                .await
                .map_err(|signed_err| {
                    anyhow::Error::new(StaleSignedUrl).context(format!(
                        "signed-URL fetch for {hash} failed: {signed_err:#}"
                    ))
                })?
        } else {
            fetch_artifact_with_retry(&self.http, &gateway_url, hash).await?
        };

        if let Some(cache) = &self.cache
            && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
        {
            let _ = cache.put_with_hash(&key, &data);
        }

        Ok(data)
    }

    /// Fetch an artifact referenced by a `ChunkRef`, optionally using a signed URL.
    pub async fn fetch_chunk_ref(
        &self,
        chunk: &crate::clonepack::ChunkRef,
        signed_url: Option<&str>,
    ) -> Result<bytes::Bytes> {
        let hash = hash_to_hex(&chunk.hash);
        let data = self.fetch_artifact_with_url(&hash, signed_url).await?;
        if data.len() as u64 != chunk.len {
            anyhow::bail!(
                "chunk {} size mismatch: expected {}, got {}",
                hash,
                chunk.len,
                data.len()
            );
        }
        Ok(data)
    }

    async fn fetch_chunk_ref_to_temp(
        &self,
        chunk: &crate::clonepack::ChunkRef,
        signed_url: Option<&str>,
        dir: &Path,
    ) -> Result<(tempfile::NamedTempFile, u64)> {
        let hash = hash_to_hex(&chunk.hash);
        let gateway_url = format!("{}/v1/artifacts/{}", self.server, hash);
        let fetch_url = signed_url.unwrap_or(&gateway_url);
        let use_signed_url = signed_url.is_some();
        let result = if use_signed_url {
            fetch_artifact_to_temp_with_retry(&self.raw_http, fetch_url, &hash, dir)
                .await
                .map_err(|signed_err| {
                    anyhow::Error::new(StaleSignedUrl).context(format!(
                        "signed-URL streaming fetch for {hash} failed: {signed_err:#}"
                    ))
                })?
        } else {
            fetch_artifact_to_temp_with_retry(&self.http, &gateway_url, &hash, dir).await?
        };
        if result.1 != chunk.len {
            anyhow::bail!(
                "chunk {} size mismatch: expected {}, got {}",
                hash,
                chunk.len,
                result.1
            );
        }
        Ok(result)
    }

    /// Fetch many chunk refs in parallel, preserving order.
    ///
    /// `signed_urls` is indexed by chunk position; `None` entries fall back to
    /// the gateway. Concurrency defaults to 6 but can be overridden with
    /// the fixed fetch concurrency.
    pub async fn fetch_chunk_refs(
        &self,
        chunks: &[crate::clonepack::ChunkRef],
        signed_urls: Option<&[Option<String>]>,
    ) -> Result<Vec<bytes::Bytes>> {
        use futures::TryStreamExt;
        use futures::stream::{self, StreamExt};
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        let concurrency = ClientTuning::load().fetch_concurrency;
        let jobs: Vec<(usize, crate::clonepack::ChunkRef, Option<String>)> = chunks
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, chunk)| {
                let signed_url = signed_urls
                    .and_then(|urls| urls.get(i))
                    .and_then(|o| o.clone());
                (i, chunk, signed_url)
            })
            .collect();
        let mut results: Vec<(usize, bytes::Bytes)> = stream::iter(jobs)
            .map(|(i, chunk, signed_url)| async move {
                let data = self.fetch_chunk_ref(&chunk, signed_url.as_deref()).await?;
                Ok::<_, anyhow::Error>((i, data))
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;
        results.sort_by_key(|(i, _)| *i);
        Ok(results.into_iter().map(|(_, d)| d).collect())
    }

    /// Fetch the top-level clonepack manifest and the metadata chunk it points to.
    /// Uses signed URLs from the ref response when available.
    pub async fn fetch_clonepack(
        &self,
        info: &RefResponse,
    ) -> Result<(ClonepackManifest, Arc<MetadataChunk>)> {
        if info.clonepack_manifest.is_empty() {
            anyhow::bail!("ref is missing clonepack manifest; run sync first");
        }
        let manifest_data = self
            .fetch_artifact_with_url(
                &info.clonepack_manifest,
                info.clonepack_manifest_url.as_deref(),
            )
            .await?;
        let clonepack = ClonepackManifest::decode(manifest_data.as_ref())
            .context("decode clonepack manifest")?;
        let metadata_ref = clonepack
            .metadata_chunk
            .as_ref()
            .context("clonepack manifest missing metadata chunk")?;
        let metadata_hash = hash_to_hex(&metadata_ref.hash);
        let metadata_data = self
            .fetch_artifact_with_url(&metadata_hash, info.metadata_chunk_url.as_deref())
            .await?;
        let metadata =
            MetadataChunk::decode(metadata_data.as_ref()).context("decode metadata chunk")?;
        Ok((clonepack, Arc::new(metadata)))
    }

    pub async fn create_snapshot(
        &self,
        repo_path: &str,
        branch: &str,
        hot_files: usize,
    ) -> Result<SnapshotResponse> {
        let url = self.repo_url(
            repo_path,
            &format!("/snapshot?branch={branch}&hot_files={hot_files}"),
        );
        let resp = self.send(self.request(reqwest::Method::POST, &url)).await?;
        if !resp.status().is_success() {
            return Err(server_error("snapshot create failed", resp).await);
        }
        Ok(resp.json().await?)
    }

    pub async fn fetch_snapshot(&self, hash: &str) -> Result<bytes::Bytes> {
        self.fetch_artifact(hash).await
    }

    pub async fn hot_files(
        &self,
        repo_path: &str,
        branch: &str,
        count: usize,
    ) -> Result<Vec<String>> {
        let url = self.repo_url(
            repo_path,
            &format!("/hotfiles?branch={branch}&count={count}"),
        );
        let resp = self.send(self.request(reqwest::Method::GET, &url)).await?;
        if !resp.status().is_success() {
            return Err(server_error("hotfiles failed", resp).await);
        }
        let body: HotfilesResponse = resp.json().await?;
        Ok(body.files)
    }

    /// Fetch a batch of working-tree files as a tar archive.
    pub async fn fetch_batch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<Vec<u8>> {
        let url = format!("{}/v1/repos/{}/{}/batch", self.server, owner, repo);
        let body = serde_json::json!({
            "paths": paths,
            "branch": branch,
            "commit": commit,
        });
        let resp = self.send(self.http.post(&url).json(&body)).await?;
        if !resp.status().is_success() {
            return Err(server_error("batch fetch failed", resp).await);
        }
        Ok(resp.bytes().await?.to_vec())
    }

    pub async fn sync_repo(&self, repo_path: &str, depth: Option<usize>) -> Result<RefResponse> {
        self.sync_repo_at(repo_path, None, depth).await
    }

    pub async fn add_repo(&self, repo_path: &str) -> Result<RefResponse> {
        let mut url = self.repo_url(repo_path, "/add");
        url.push_str("?source=cli");
        let max_attempts = std::env::var("RIPCLONE_SYNC_MAX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(40usize);
        for attempt in 0..max_attempts {
            let resp = self.send(self.request(reqwest::Method::POST, &url)).await?;
            let status = resp.status();
            if status == reqwest::StatusCode::OK {
                return Ok(resp.json().await?);
            }
            if status == reqwest::StatusCode::ACCEPTED
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
                anyhow::bail!("add still building after {max_attempts} attempts");
            }
            return Err(server_error("add failed", resp).await);
        }
        anyhow::bail!("add did not complete")
    }

    /// Like [`sync_repo`] but builds at `rev` (e.g. "HEAD~5" or a SHA) instead of
    /// the branch tip. The resolved commit is used as the ref-store key, so
    /// different revs that resolve to the same commit share a build. Useful for
    /// exercising the incremental build path deterministically without waiting for
    /// upstream to advance.
    pub async fn sync_repo_at(
        &self,
        repo_path: &str,
        rev: Option<&str>,
        depth: Option<usize>,
    ) -> Result<RefResponse> {
        self.sync_inner(repo_path, None, rev, depth).await
    }

    /// Sync a specific branch instead of the repo's default. Each branch is its
    /// own ref + clonepack, so this lets several distinct builds for one repo run
    /// at once (unlike `?rev=`, which the server keys by resolved commit).
    pub async fn sync_branch(&self, repo_path: &str, branch: &str) -> Result<RefResponse> {
        self.sync_inner(repo_path, Some(branch), None, None).await
    }

    async fn sync_inner(
        &self,
        repo_path: &str,
        branch: Option<&str>,
        rev: Option<&str>,
        depth: Option<usize>,
    ) -> Result<RefResponse> {
        let mut url = self.repo_url(repo_path, "/sync");
        let mut q: Vec<String> = Vec::new();
        if let Some(b) = branch {
            q.push(format!("branch={}", urlencoding::encode(b)));
        }
        if let Some(d) = depth {
            q.push(format!("depth={}", d));
        }
        if let Some(r) = rev {
            q.push(format!("rev={}", urlencoding::encode(r)));
        }
        if !q.is_empty() {
            url.push('?');
            url.push_str(&q.join("&"));
        }
        // With the async build queue the server may return 202 (build still
        // running) or 503 (queue full). Each POST blocks server-side until its
        // wait window elapses, so we just retry — coalescing means a retry
        // re-attaches to the same in-flight build rather than starting a new one.
        // Test hook: bound the poll so a negative-case test can fail fast instead
        // of waiting out the full ceiling. Never set in production.
        let max_attempts = std::env::var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(40);
        for attempt in 0..max_attempts {
            let resp = self.send(self.request(reqwest::Method::POST, &url)).await?;
            let status = resp.status();
            if status == reqwest::StatusCode::OK {
                return Ok(resp.json().await?);
            }
            if status == reqwest::StatusCode::ACCEPTED
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
                anyhow::bail!("sync still building after {max_attempts} attempts");
            }
            return Err(server_error("sync failed", resp).await);
        }
        anyhow::bail!("sync did not complete")
    }

    /// Fast install: download prebuilt `.git` artifacts and the working-tree
    /// archive, lay everything down directly, and extract the archive.
    ///
    /// No `git init`, `index-pack`, `read-tree`, or `update-index` is run on the
    /// client. The server has already done all of that work.
    pub async fn install_repo<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        target: P,
    ) -> Result<()> {
        self.install_repo_with_mode(owner, repo, branch, target, CloneMode::Editable, None, None)
            .await
            .map(|_| ())
    }

    /// Install a repo with a specific clone mode and optional per-phase benchmark
    /// instrumentation.
    #[allow(clippy::too_many_arguments)]
    pub async fn install_repo_with_mode<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        target: P,
        mode: CloneMode,
        clonepack: Option<&str>,
        bench: Option<&mut Benchmark>,
    ) -> Result<CloneOutcome> {
        self.install_repo_with_mode_at(
            &format!("{owner}/{repo}"),
            branch,
            None,
            target,
            mode,
            clonepack,
            bench,
        )
        .await
    }

    /// Like [`install_repo_with_mode`] but resolves `rev` (e.g. "HEAD~5") instead
    /// of the branch tip — clones the artifacts a `sync --at <rev>` built.
    #[allow(clippy::too_many_arguments)]
    pub async fn install_repo_with_mode_at<P: AsRef<Path>>(
        &self,
        repo_path: &str,
        branch: &str,
        rev: Option<&str>,
        target: P,
        mode: CloneMode,
        clonepack: Option<&str>,
        bench: Option<&mut Benchmark>,
    ) -> Result<CloneOutcome> {
        let target = target.as_ref().to_path_buf();
        info!(
            "installing {}#{} into {} with mode {:?}",
            repo_path,
            branch,
            target.display(),
            mode
        );

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        let mut local_bench = Benchmark::new();
        let bench = bench.unwrap_or(&mut local_bench);
        crate::perf::reset_perf_counters();
        let _ = crate::worktree_writer::take_write_timing();

        // 1. Resolve ref (full-history by default; fast clones can request shallow).
        let mut info = self
            .resolve_ref_with_clonepack(repo_path, branch, clonepack, rev)
            .await?;
        bench.mark_resolve();
        // Capture the metrics-relevant facts from the FIRST successful resolve:
        // the cloud mints a fresh clone id (and usage event) on each resolve, so
        // the archive-ready re-resolve below would otherwise overwrite the id we
        // want to report against.
        let clone_id = info.clone_id.clone();
        let mut cold = info.cold;
        // `files` | `depth1` | `full`, derived from the mode and requested
        // clonepack variant (depth=1 ⇒ "shallow").
        let metric_mode: &'static str = if mode.needs_archive() {
            "files"
        } else if clonepack == Some("shallow") {
            "depth1"
        } else {
            "full"
        };
        info!("resolved to commit {}", &info.commit[..7]);

        // Files mode needs the zstd archive. The server publishes an editable
        // clonepack first and adds the archive a moment later, so wait for it
        // (editable clones don't need it and skip this). Waiting here means the
        // archive is being built on demand — a cold build — which the
        // ref-resolve 202 poll above doesn't capture for files mode, so record
        // it for the metrics label.
        if mode.needs_archive() && !info.archive_ready {
            cold = true;
            let max = 40usize;
            for _ in 0..max {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                info = self
                    .resolve_ref_with_clonepack(repo_path, branch, clonepack, rev)
                    .await?;
                if info.archive_ready {
                    break;
                }
            }
            if !info.archive_ready {
                anyhow::bail!("archive still building for {repo_path}");
            }
        }

        if info.clonepack_manifest.is_empty() {
            anyhow::bail!("ref is missing clonepack manifest; run sync first");
        }

        // Hand the decoded manifest to the archive downloader over a oneshot.
        // It latches the value (no lost-wakeup race) and signals manifest
        // failure by dropping the sender (the receiver then errors), so the
        // downloader can never hang waiting for a manifest that will never come.
        let (manifest_tx, manifest_rx) = tokio::sync::oneshot::channel::<Arc<ClonepackManifest>>();

        // 2. Start manifest + metadata downloads concurrently.
        let manifest_task = self.clone().spawn_fetch_manifest(
            info.clonepack_manifest.clone(),
            info.clonepack_manifest_url.clone(),
            manifest_tx,
        );

        let metadata_hash = info.metadata_chunk.clone();
        let metadata_url = info.metadata_chunk_url.clone();
        let metadata_task = self
            .clone()
            .spawn_fetch_metadata(metadata_hash, metadata_url);

        // 3. Spawn the archive-chunk downloader. It waits for the manifest to be
        // decoded before fetching anything (so it follows the manifest's chunk
        // table, not a possibly-stale signed-URL list), then fetches chunks with
        // a bounded concurrency semaphore and forwards them over this bounded
        // channel. Peak memory is therefore bounded by the fetch concurrency
        // plus the channel depth, not the chunk count.
        let archive_channel_depth = info
            .archive_chunk_urls
            .as_ref()
            .map_or(2, |urls| urls.len().clamp(2, 64));
        let (archive_async_tx, mut archive_async_rx) =
            tokio::sync::mpsc::channel::<(usize, Result<bytes::Bytes>)>(archive_channel_depth);
        let (archive_tx, archive_rx): (
            Sender<(usize, Result<bytes::Bytes>)>,
            Receiver<(usize, Result<bytes::Bytes>)>,
        ) = bounded(archive_channel_depth);
        let archive_bridge = if mode.needs_archive() {
            let archive_tx = archive_tx.clone();
            Some(tokio::task::spawn_blocking(move || {
                while let Some(msg) = archive_async_rx.blocking_recv() {
                    let send_start = Instant::now();
                    if archive_tx.send(msg).is_err() {
                        break;
                    }
                    crate::perf::record_archive_send_wait(send_start.elapsed());
                }
            }))
        } else {
            None
        };

        let archive_urls = info.archive_chunk_urls.clone();
        let archive_downloads = if mode.needs_archive() {
            bench.start_archive_download();
            Some(
                self.clone()
                    .spawn_chunk_downloads(archive_urls, manifest_rx, archive_async_tx),
            )
        } else {
            drop(archive_async_tx);
            drop(manifest_rx);
            None
        };
        drop(archive_tx);

        // 4. Wait for manifest + metadata.
        let (manifest, metadata) =
            tokio::try_join!(manifest_task, metadata_task).context("fetch manifest/metadata")?;
        let manifest = manifest.context("fetch clonepack manifest")?;
        let metadata = metadata.context("fetch metadata chunk")?;
        let metadata = Arc::new(metadata);
        bench.mark_manifest();
        bench.add_bytes(metadata_bytes(&metadata), 0);
        if mode.needs_archive() && !metadata.files.is_empty() && manifest.archive_chunks.is_empty()
        {
            anyhow::bail!(
                "selected clonepack has no archive chunks for files mode; rerun sync or request a clonepack variant with archive chunks"
            );
        }

        // 5. Decide where to install (temp dir, possibly overlay).
        let staging_dir = overlay::staging_dir();
        let use_overlay =
            mode.needs_worktree() && self.should_use_overlay(&metadata, &staging_dir).await;

        let overlay_dirs = if use_overlay {
            Some(
                overlay::OverlayDirs::create(&staging_dir, &target)
                    .context("create overlay staging dirs")?,
            )
        } else {
            None
        };

        // Hold the temp-dir handle for the whole install so any early failure
        // removes the partial directory on drop. After a successful rename onto
        // `target`, its drop is a no-op.
        let mut _temp_install: Option<tempfile::TempDir> = None;
        let install_root = if let Some(ref dirs) = overlay_dirs {
            dirs.lower.clone()
        } else {
            let tmp = temp_install_dir(&target)?;
            let path = tmp.path().to_path_buf();
            _temp_install = Some(tmp);
            path
        };
        let git_dir = install_root.join(".git");
        let files_only = matches!(mode, CloneMode::Files);

        if !files_only {
            std::fs::create_dir_all(&git_dir)?;
            std::fs::create_dir_all(git_dir.join("refs").join("heads"))?;
            std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
            std::fs::create_dir_all(git_dir.join("info"))?;

            let branch_name = if branch == "HEAD" {
                if info.default_branch.is_empty() {
                    "main"
                } else {
                    &info.default_branch
                }
            } else {
                branch
            };

            std::fs::write(
                git_dir.join("HEAD"),
                format!("ref: refs/heads/{branch_name}\n"),
            )?;
            let branch_ref = git_dir.join("refs").join("heads").join(branch_name);
            if let Some(parent) = branch_ref.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(branch_ref, format!("{}\n", info.commit))?;
            std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;
            if info.shallow {
                // Mark HEAD as a shallow boundary so git does not try to traverse
                // missing parents.
                std::fs::write(git_dir.join("shallow"), format!("{}\n", info.commit))?;
            }
        }

        // 6. Write the small .git artifacts from the metadata chunk.
        let pack_dir = git_dir.join("objects").join("pack");
        if !files_only {
            std::fs::create_dir_all(&pack_dir)?;
            let skeleton_hash = cas_hash(&metadata.skeleton_pack);
            std::fs::write(
                pack_dir.join(format!("pack-{}.pack", skeleton_hash)),
                &metadata.skeleton_pack,
            )?;
            std::fs::write(
                pack_dir.join(format!("pack-{}.idx", skeleton_hash)),
                &metadata.skeleton_idx,
            )?;
            std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;
            info!(
                "wrote skeleton pack + idx + prebuilt index ({} bytes)",
                metadata.skeleton_pack.len()
                    + metadata.skeleton_idx.len()
                    + metadata.prebuilt_index.len()
            );
        } else {
            info!("files mode: skipped .git skeleton pack, idx, and index install");
        }
        bench.mark_metadata();

        // 7. Start the working-tree materialization workers.
        let mut manifest_tmp = tempfile::NamedTempFile::new().context("create temp manifest")?;
        metadata
            .write(&mut manifest_tmp)
            .context("write temp manifest")?;
        let manifest_path = manifest_tmp.path().to_path_buf();

        let archive_worker = if mode.needs_archive() {
            let rx = archive_rx;
            let manifest_path = manifest_path.clone();
            let work_tree = install_root.clone();
            Some(tokio::task::spawn_blocking(move || {
                // Keep the temp manifest file alive for the duration of extraction.
                let _guard = manifest_tmp;
                extract_archive_from_chunk_receiver(&manifest_path, Some(&work_tree), None, rx)
            }))
        } else {
            drop(archive_rx);
            // The temp file can be dropped; nothing needs the manifest on disk.
            drop(manifest_tmp);
            None
        };

        // 8. Wait for downloads + workers.
        let mut archive_bytes = 0u64;
        if let Some(handle) = archive_downloads {
            let bytes = handle.await.context("archive download coordinator")??;
            archive_bytes = bytes;
        }
        if let Some(handle) = archive_bridge {
            handle.await.context("archive download bridge")?;
        }
        // Editable single-download path: download the small depth packs in
        // parallel and, as each lands, install it and extract its blobs into the
        // working tree. Download and extraction overlap.
        let mut prebuilt_blob_pack_bytes = 0u64;
        if mode.needs_pack_worktree() {
            prebuilt_blob_pack_bytes = self
                .install_editable_packs(&manifest, &info, &pack_dir, &install_root, &metadata)
                .await
                .context("install editable packs")?;
            bench.mark_write();
            info!(
                "installed + extracted {} editable packs ({} bytes)",
                manifest.packs.len(),
                prebuilt_blob_pack_bytes
            );
        }
        if let Some(handle) = archive_worker {
            let stats = handle
                .await
                .context("archive worker join")?
                .context("archive extraction")?;
            bench.mark_write();
            info!(
                "extracted {} files ({} raw bytes) from archive chunks",
                stats.files, stats.raw_bytes
            );
        }
        // `mark_archive_download` sets (overwrites) `archive_bytes`, so no
        // separate `add_bytes` for the archive total is needed.
        bench.mark_archive_download(archive_bytes + prebuilt_blob_pack_bytes);

        // 9. Origin config + finalization.
        if !files_only {
            let origin_url = if info.origin_url.is_empty() {
                if let Some((owner, repo)) = repo_path.split_once('/') {
                    format!("https://github.com/{owner}/{repo}.git")
                } else {
                    format!("https://github.com/{repo_path}.git")
                }
            } else {
                info.origin_url.clone()
            };
            self.write_origin_config(&origin_url, &git_dir)?;
        }

        if let Some(dirs) = overlay_dirs {
            overlay::mount_dirs(&dirs).context("mount overlay at target")?;
            // Mount succeeded; keep the staging tree (it backs the mount). Any
            // failure before this point drops `dirs` and removes the staging.
            dirs.mark_mounted();
            info!(
                "mounted overlay {} -> {} (staging {})",
                dirs.lower.display(),
                target.display(),
                staging_dir.display()
            );
        } else {
            // Optional durability barrier: flush the staged tree, publish it,
            // then flush the parent directory so the rename itself is durable.
            if fsync_requested() {
                fsync_tree(&install_root).context("fsync staged clone before publish")?;
            }
            std::fs::rename(&install_root, &target).with_context(|| {
                format!("rename {} to {}", install_root.display(), target.display())
            })?;
            if fsync_requested()
                && let Some(parent) = target.parent().filter(|p| !p.as_os_str().is_empty())
            {
                fsync_dir(parent).context("fsync parent directory after publish")?;
            }
        }

        let report = bench.finish();
        let perf = crate::perf::take_perf_counters();
        let write_timing = crate::worktree_writer::take_write_timing();
        if report.total_ms > 0 {
            info!(
                "clone benchmark: resolve={}ms manifest={}ms metadata={}ms archive_download={}ms write={}ms total={}ms",
                report.resolve_ms,
                report.manifest_ms,
                report.metadata_ms,
                report.archive_download_ms,
                report.write_ms,
                report.total_ms,
            );
            info!(
                "clone perf counters: archive_send_wait={}ms archive_download_inner={}ms/{}B zstd={}ms/{}->{}B zlib={}ms/{}->{}B sha1={}ms/{}B cas_read={}ms/{}B cas_write={}ms/{}B cas_fsync={}ms storage_upload={}ms/{}B archive_bundle_assembly={}ms/{}B editable_pack_fetch={}ms/{}B writer_prep={}ms writer_io={}ms writer_mtime={}ms writer_files={} writer_bytes={}",
                perf.archive_send_wait_ns / 1_000_000,
                perf.archive_download_ns / 1_000_000,
                perf.archive_download_bytes,
                perf.zstd_inflate_ns / 1_000_000,
                perf.zstd_inflate_in_bytes,
                perf.zstd_inflate_out_bytes,
                perf.zlib_inflate_ns / 1_000_000,
                perf.zlib_inflate_in_bytes,
                perf.zlib_inflate_out_bytes,
                perf.sha1_ns / 1_000_000,
                perf.sha1_bytes,
                perf.cas_read_ns / 1_000_000,
                perf.cas_read_bytes,
                perf.cas_write_ns / 1_000_000,
                perf.cas_write_bytes,
                perf.cas_fsync_ns / 1_000_000,
                perf.storage_upload_ns / 1_000_000,
                perf.storage_upload_bytes,
                perf.archive_bundle_assembly_ns / 1_000_000,
                perf.archive_bundle_assembly_bytes,
                perf.editable_pack_fetch_ns / 1_000_000,
                perf.editable_pack_fetch_bytes,
                write_timing.prep_ns / 1_000_000,
                write_timing.io_ns / 1_000_000,
                write_timing.mtime_ns / 1_000_000,
                write_timing.files,
                write_timing.bytes,
            );
        }

        info!(
            "installed {}#{} into {} with mode {:?}",
            repo_path,
            branch,
            target.display(),
            mode
        );
        let provider = if info.provider.is_empty() {
            self.provider.clone()
        } else {
            info.provider.clone()
        };
        Ok(CloneOutcome {
            provider,
            owner: info.owner.clone(),
            name: info.repo.clone(),
            commit: info.commit.clone(),
            mode: metric_mode,
            cold,
            clone_id,
            bytes: report.total_bytes(),
        })
    }

    /// Best-effort, fire-and-forget POST of clone metrics to the managed cloud,
    /// sent AFTER the clone has printed success. Never returns an error and never
    /// panics: a metrics failure must not change the clone's exit status.
    ///
    /// Skipped entirely when the cloud didn't mint a clone id (self-hosted/older
    /// server) or when the user opted out via `RIPCLONE_NO_METRICS`. The request
    /// carries the same `Authorization` header as every other call (the cloud
    /// requires an authenticated caller to attribute the metric), and uses a
    /// short timeout so a slow endpoint can't stall the CLI's exit.
    pub async fn report_clone_metrics(&self, outcome: &CloneOutcome, total_ms: u64) {
        use crate::clone_metrics::{ClientInfo, CloneMetric, RepoId, opted_out};
        if self.skip_metrics || opted_out() {
            return;
        }
        let Some(clone_id) = outcome.clone_id.clone() else {
            return;
        };
        let payload = CloneMetric {
            clone_id: clone_id.clone(),
            repo: RepoId {
                provider: outcome.provider.clone(),
                owner: outcome.owner.clone(),
                name: outcome.name.clone(),
            },
            commit: outcome.commit.clone(),
            mode: outcome.mode.to_string(),
            cold: outcome.cold,
            total_ms,
            bytes: outcome.bytes,
            // v1 omits downloadMs: the client can't cleanly isolate pure
            // chunk-download time from manifest fetch + extraction, and a biased
            // number would skew the cloud's bytes/downloadMs throughput (the
            // headline metric). Better no throughput than a wrong one — the cloud
            // simply won't compute it. Reinstated when the phase is isolated (v2).
            download_ms: None,
            client: ClientInfo::current(),
        };
        let url = format!("{}/v1/clones/{}/metrics", self.server, clone_id);
        // Swallow every outcome — transport error, timeout, or a non-2xx status.
        // The clone already succeeded; this is advertising-grade telemetry.
        //
        // The request is awaited inline (true detach is impossible: the CLI exits
        // right after, killing any in-flight request), so the timeout is the hard
        // ceiling on how long a hung/black-hole endpoint can delay exit. Keep it
        // short — a clone we sell as sub-second must not gain ~seconds here.
        let _ = self
            .http
            .post(&url)
            .json(&payload)
            .timeout(std::time::Duration::from_millis(400))
            .send()
            .await;
    }

    /// Download the pre-built head-blobs pack + index and install them into
    /// `.git/objects/pack/`. Returns the total bytes downloaded.
    ///
    /// Chunks are downloaded with bounded concurrency and written directly into
    /// a pre-allocated temp pack file at their final offsets. Peak memory is
    /// ~`concurrency * chunk_size`, no bytes are re-hashed, and the pack is
    /// written exactly once.
    #[allow(deprecated)]
    pub async fn install_prebuilt_blob_pack(
        &self,
        clonepack: &ClonepackManifest,
        info: &RefResponse,
        pack_dir: &std::path::Path,
    ) -> Result<u64> {
        let head_blobs_refs = head_blobs_chunk_refs(clonepack);
        if head_blobs_refs.is_empty() {
            anyhow::bail!("clonepack missing head-blobs pack for hybrid install");
        }
        let idx_ref = clonepack
            .head_blobs_idx
            .as_ref()
            .context("clonepack missing head-blobs idx")?;
        self.install_chunked_pack(
            "head-blobs",
            &head_blobs_refs,
            info.head_blobs_chunk_urls.as_deref(),
            idx_ref,
            info.head_blobs_idx_url.as_deref(),
            pack_dir,
        )
        .await
    }

    /// Editable single-download path: download the small depth packs in parallel
    /// and, as each lands, install it into `pack_dir` and extract the blobs it
    /// contains into `work_tree` — so download and extraction overlap. Uses the
    /// manifest file table to map blobs to paths. Returns total bytes downloaded.
    async fn install_editable_packs(
        &self,
        manifest: &ClonepackManifest,
        info: &RefResponse,
        pack_dir: &std::path::Path,
        work_tree: &std::path::Path,
        metadata: &MetadataChunk,
    ) -> Result<u64> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        if manifest.packs.is_empty() {
            anyhow::bail!("clonepack has no packs for editable install");
        }
        std::fs::create_dir_all(pack_dir)
            .with_context(|| format!("create pack dir {}", pack_dir.display()))?;

        let idx_bundle_task = manifest.idx_bundle.as_ref().map(|bundle_ref| {
            let client = self.clone();
            let bundle_ref = bundle_ref.clone();
            let idx_bundle_url = info.idx_bundle_url.clone();
            tokio::spawn(async move {
                client
                    .fetch_chunk_ref(&bundle_ref, idx_bundle_url.as_deref())
                    .await
                    .context("fetch idx bundle")
            })
        });
        let midx_task = manifest.midx.as_ref().map(|midx_ref| {
            let client = self.clone();
            let midx_ref = midx_ref.clone();
            let midx_url = info.midx_url.clone();
            tokio::spawn(async move {
                client
                    .fetch_chunk_ref(&midx_ref, midx_url.as_deref())
                    .await
                    .context("fetch pre-built multi-pack-index")
            })
        });

        // Validate every blob sha1 length up front so `build_blob_path_map`
        // indexes every file and the files-written guard below is exact (a
        // non-20-byte sha1 would otherwise be silently skipped and trip a
        // misleading count mismatch).
        for f in &metadata.files {
            if f.blob_sha1.len() != 20 {
                anyhow::bail!(
                    "manifest blob_sha1 for {} is {} bytes, expected 20",
                    String::from_utf8_lossy(&f.path),
                    f.blob_sha1.len()
                );
            }
        }

        // Build the blob→paths map and pre-create directories single-threaded
        // before the parallel writers run.
        let blob_map = Arc::new(crate::extract::build_blob_path_map(&metadata.files));
        crate::extract::prepare_worktree_dirs(work_tree, &metadata.files)
            .context("prepare worktree dirs")?;
        let worktree_writer = Arc::new(crate::worktree_writer::WorktreeWriter::new()?);

        // Download and pack parsing are decoupled stages with independent
        // concurrency. Keep zlib inflate + SHA-1 parse workers capped at core
        // count even when the writer backend can profit from deeper io_uring
        // submission; otherwise `2 * cores` write defaults turn into `2 * cores`
        // CPU parse tasks.
        let tuning = ClientTuning::load();
        let download_conc = tuning.editable_download_concurrency;
        let parse_conc = tuning.pack_parse_threads;

        // Signed URLs (one per pack/idx, matching manifest.packs order). Empty
        // entries fall back to the gateway by hash; with an object-store backend
        // these point straight at the bucket so bytes bypass the server.
        let pack_urls = info.pack_chunk_urls.clone().unwrap_or_default();
        let idx_urls = info.pack_idx_urls.clone().unwrap_or_default();

        // If the manifest ships a single idx bundle, fetch it ONCE and slice each
        // pack's idx out of it locally — instead of one GET per pack idx (cuts
        // per-pack round-trips from 2 to 1). Falls back to per-pack idx fetches
        // for older manifests without a bundle.
        let idx_bundle: Option<Arc<bytes::Bytes>> = match idx_bundle_task {
            Some(task) => Some(Arc::new(task.await.context("idx bundle fetch task")??)),
            None => None,
        };

        let jobs: Vec<(usize, PackEntry)> = manifest.packs.iter().cloned().enumerate().collect();

        enum PackBody {
            Buffered(bytes::Bytes),
            TempFile {
                file: tempfile::NamedTempFile,
                len: u64,
            },
        }

        impl PackBody {
            fn len(&self) -> usize {
                match self {
                    PackBody::Buffered(bytes) => bytes.len(),
                    PackBody::TempFile { len, .. } => *len as usize,
                }
            }
        }

        // Stage 1: download packs (network concurrency `download_conc`).
        let downloads = stream::iter(jobs).map(|(i, entry)| {
            let client = self.clone();
            let pack_url = pack_urls.get(i).and_then(|o| o.clone());
            let idx_url = idx_urls.get(i).and_then(|o| o.clone());
            let idx_bundle = idx_bundle.clone();
            let history_only = entry.history_only;
            let pack_dir = pack_dir.to_path_buf();
            async move {
                let pack_ref = entry
                    .pack
                    .as_ref()
                    .with_context(|| format!("pack {} missing pack ref", i))?;
                let idx_ref = entry
                    .idx
                    .as_ref()
                    .with_context(|| format!("pack {} missing idx ref", i))?;
                let idx_bytes = if let Some(bundle) = idx_bundle.as_ref() {
                    // Slice this pack's idx from the bundle and verify its hash;
                    // only the pack itself needs a network fetch.
                    let off = entry.idx_bundle_offset as usize;
                    let end = off
                        .checked_add(idx_ref.len as usize)
                        .context("idx bundle offset overflow")?;
                    // Zero-copy view into the shared bundle (refcounted Bytes).
                    if bundle.get(off..end).is_none() {
                        anyhow::bail!("idx {} slice out of bundle range", i);
                    }
                    let slice = bundle.slice(off..end);
                    let want = hash_to_hex(&idx_ref.hash);
                    let got = crate::cas::hash(&slice);
                    if got != want {
                        anyhow::bail!(
                            "idx {i} bundle slice hash mismatch: expected {want}, got {got}"
                        );
                    }
                    slice
                } else {
                    client
                        .fetch_chunk_ref(idx_ref, idx_url.as_deref())
                        .await
                        .with_context(|| format!("fetch idx {}", i))?
                };
                let pack_fetch_start = std::time::Instant::now();
                let pack_body = if history_only {
                    let (file, len) = client
                        .fetch_chunk_ref_to_temp(pack_ref, pack_url.as_deref(), &pack_dir)
                        .await
                        .with_context(|| format!("stream history pack {}", i))?;
                    crate::perf::record_editable_pack_fetch(pack_fetch_start.elapsed(), len);
                    PackBody::TempFile { file, len }
                } else {
                    let bytes = client
                        .fetch_chunk_ref(pack_ref, pack_url.as_deref())
                        .await
                        .with_context(|| format!("fetch head pack {}", i))?;
                    crate::perf::record_editable_pack_fetch(
                        pack_fetch_start.elapsed(),
                        bytes.len() as u64,
                    );
                    PackBody::Buffered(bytes)
                };
                Ok::<(usize, bool, PackBody, bytes::Bytes), anyhow::Error>((
                    i,
                    history_only,
                    pack_body,
                    idx_bytes,
                ))
            }
        });

        // Stage 2: install each pack; hand-parse for the worktree only when it's
        // a HEAD-closure (undeltified) pack. History-only packs are deltified —
        // installed for the object DB, read by git, never hand-parsed.
        let total = downloads
            .buffer_unordered(download_conc)
            .map(|res| {
                let pack_dir = pack_dir.to_path_buf();
                let work_tree = work_tree.to_path_buf();
                let blob_map = Arc::clone(&blob_map);
                let worktree_writer = Arc::clone(&worktree_writer);
                async move {
                    let (i, history_only, pack_body, idx_bytes) = res?;
                    let bytes = (pack_body.len() + idx_bytes.len()) as u64;
                    let result = tokio::task::spawn_blocking(
                        move || -> Result<crate::extract::PackExtractResult> {
                            if pack_body.len() < 20 {
                                anyhow::bail!("pack {} too short ({} bytes)", i, pack_body.len());
                            }
                            let (name, pack_bytes) = match pack_body {
                                PackBody::Buffered(pack_bytes) => {
                                    // Git names packs by the 20-byte trailer sha; the idx
                                    // pairs to the pack by basename.
                                    let name = hex::encode(&pack_bytes[pack_bytes.len() - 20..]);
                                    std::fs::write(
                                        pack_dir.join(format!("pack-{}.pack", name)),
                                        &pack_bytes,
                                    )
                                    .with_context(|| format!("write pack {}", name))?;
                                    (name, Some(pack_bytes))
                                }
                                PackBody::TempFile { file, len } => {
                                    use std::io::{Read, Seek, SeekFrom};
                                    let mut reader = file
                                        .as_file()
                                        .try_clone()
                                        .context("clone streamed pack file")?;
                                    reader
                                        .seek(SeekFrom::Start(len - 20))
                                        .context("seek streamed pack trailer")?;
                                    let mut trailer = [0u8; 20];
                                    reader
                                        .read_exact(&mut trailer)
                                        .context("read streamed pack trailer")?;
                                    let name = hex::encode(trailer);
                                    file.persist(pack_dir.join(format!("pack-{}.pack", name)))
                                        .with_context(|| {
                                            format!("install streamed pack {}", name)
                                        })?;
                                    (name, None)
                                }
                            };
                            std::fs::write(pack_dir.join(format!("pack-{}.idx", name)), &idx_bytes)
                                .with_context(|| format!("write idx {}", name))?;
                            if history_only {
                                return Ok(crate::extract::PackExtractResult {
                                    files: 0,
                                    stats: Vec::new(),
                                });
                            }
                            let Some(pack_bytes) = pack_bytes else {
                                anyhow::bail!("head pack {} was not buffered for extraction", i);
                            };
                            crate::extract::extract_blobs_from_pack_bytes(
                                &pack_bytes,
                                &blob_map,
                                &work_tree,
                                &worktree_writer,
                            )
                            .with_context(|| format!("extract pack {}", name))
                        },
                    )
                    .await
                    .context("spawn pack install task")??;
                    Ok::<(u64, crate::extract::PackExtractResult), anyhow::Error>((bytes, result))
                }
            })
            .buffer_unordered(parse_conc)
            .try_fold(
                (0u64, 0usize, Vec::new()),
                |(ab, aw, mut stats), (b, result)| async move {
                    stats.extend(result.stats);
                    Ok((ab + b, aw + result.files, stats))
                },
            )
            .await?;
        let (total, files_written, stat_cache) = total;

        // Guard against silent under-extraction (e.g. a sha/format mismatch):
        // every tracked path must have been materialized.
        if files_written != metadata.files.len() {
            anyhow::bail!(
                "editable extraction wrote {} files but manifest lists {}",
                files_written,
                metadata.files.len()
            );
        }

        // Files are materialized; clear skip-worktree for every tracked path.
        let path_bytes: Vec<Vec<u8>> = metadata.files.iter().map(|e| e.path.clone()).collect();
        let work_tree2 = work_tree.to_path_buf();
        tokio::task::spawn_blocking(move || {
            crate::git::clear_skip_worktree_index_with_stats_byte_iter(
                &work_tree2,
                path_bytes.iter().map(Vec::as_slice),
                &stat_cache,
            )
        })
        .await
        .context("spawn clear skip-worktree and refresh index stats")??;

        // Install the multi-pack-index so git object lookups stay O(log) across
        // the many installed packs. Prefer the server-pregenerated MIDX (zero
        // client CPU; it indexes the same `pack-<trailer>` files we just wrote);
        // fall back to building it locally for older manifests without one. Best
        // effort either way — without a MIDX the clone is still correct, just
        // with slower per-object lookups.
        if let Some(midx_task) = midx_task {
            match midx_task
                .await
                .context("pre-built MIDX fetch task")
                .and_then(|r| r)
            {
                Ok(midx_bytes) => {
                    tokio::fs::write(pack_dir.join("multi-pack-index"), &midx_bytes)
                        .await
                        .context("write pre-built multi-pack-index")?;
                }
                Err(e) => {
                    tracing::warn!("pre-built MIDX fetch failed ({e:#}); building locally");
                    let work_tree3 = work_tree.to_path_buf();
                    let _ = tokio::task::spawn_blocking(move || {
                        crate::git::write_multi_pack_index(&work_tree3)
                    })
                    .await;
                }
            }
        } else {
            let work_tree3 = work_tree.to_path_buf();
            let _ = tokio::task::spawn_blocking(move || {
                crate::git::write_multi_pack_index(&work_tree3)
            })
            .await;
        }

        Ok(total)
    }

    /// Download a content-addressed, chunk-split git pack + its idx and install
    /// them into `pack_dir` (`.git/objects/pack`). Returns total bytes
    /// downloaded.
    ///
    /// Chunks are downloaded with bounded concurrency and written directly into
    /// a pre-allocated temp pack file at their final offsets. Peak memory is
    /// ~`concurrency * chunk_size`, no bytes are re-hashed, and the pack is
    /// written exactly once. `label` is used only for log/error messages.
    async fn install_chunked_pack(
        &self,
        label: &str,
        chunk_refs: &[ChunkRef],
        chunk_urls: Option<&[Option<String>]>,
        idx_ref: &ChunkRef,
        idx_url: Option<&str>,
        pack_dir: &std::path::Path,
    ) -> Result<u64> {
        use std::os::unix::fs::FileExt;

        if chunk_refs.is_empty() {
            anyhow::bail!("{} pack has no chunks", label);
        }

        // Download the small index concurrently with the pack chunks.
        let idx_data = self
            .fetch_chunk_ref(idx_ref, idx_url)
            .await
            .with_context(|| format!("fetch {} idx", label))?;

        std::fs::create_dir_all(pack_dir)
            .with_context(|| format!("create pack dir {}", pack_dir.display()))?;
        let tmp = tempfile::Builder::new()
            .suffix(".tmp")
            .tempfile_in(pack_dir)
            .with_context(|| format!("create temp {} pack", label))?;
        let file = tmp
            .as_file()
            .try_clone()
            .with_context(|| format!("clone temp {} pack fd", label))?;

        let signed_urls = chunk_urls.unwrap_or(&[]);
        let concurrency = ClientTuning::load().fetch_concurrency;

        // Compute final pack size and per-chunk byte offsets.
        let mut offsets = Vec::with_capacity(chunk_refs.len());
        let mut total_len = 0u64;
        for chunk in chunk_refs {
            offsets.push(total_len);
            total_len += chunk.len;
        }

        // Pre-allocate the temp file on the blocking pool so the async worker
        // is not pinned during the syscall.
        {
            let file = file.try_clone().context("clone fd for preallocate")?;
            let label = label.to_string();
            tokio::task::spawn_blocking(move || {
                file.set_len(total_len)
                    .with_context(|| format!("preallocate temp {} pack file", label))
            })
            .await
            .context("spawn preallocate task")??;
        }

        let jobs: Vec<(usize, ChunkRef, Option<String>, u64)> = chunk_refs
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, chunk)| {
                let signed_url = signed_urls.get(i).and_then(|o| o.clone());
                let offset = offsets[i];
                (i, chunk, signed_url, offset)
            })
            .collect();

        use futures::stream::{self, StreamExt, TryStreamExt};
        let pack_bytes: u64 = stream::iter(jobs)
            .map(|(i, chunk, signed_url, offset)| {
                let client = self.clone();
                let file = file
                    .try_clone()
                    .with_context(|| format!("clone pack fd for {} chunk {}", label, i));
                let write_label = label.to_string();
                async move {
                    let file = file?;
                    let data = client
                        .fetch_chunk_ref(&chunk, signed_url.as_deref())
                        .await
                        .with_context(|| format!("fetch {} chunk {}", label, i))?;
                    let len = data.len() as u64;
                    tokio::task::spawn_blocking(move || {
                        file.write_all_at(&data, offset).with_context(|| {
                            format!("write {} chunk {} at offset {}", write_label, i, offset)
                        })
                    })
                    .await
                    .context("spawn chunk write task")??;
                    Ok::<_, anyhow::Error>(len)
                }
            })
            .buffer_unordered(concurrency)
            .try_fold(0u64, |acc, len| async move { Ok(acc + len) })
            .await?;

        // Git names pack files after the 20-byte SHA-1 trailer at the end of the
        // pack. Read it directly instead of re-hashing the whole file.
        if total_len < 20 {
            anyhow::bail!("{} pack is too short ({} bytes)", label, total_len);
        }
        let pack_hash = {
            let file = file.try_clone().context("clone fd for trailer read")?;
            let label = label.to_string();
            let trailer = tokio::task::spawn_blocking(move || {
                let mut trailer = [0u8; 20];
                file.read_at(&mut trailer, total_len - 20)
                    .with_context(|| format!("read {} pack trailer", label))?;
                Ok::<_, anyhow::Error>(trailer)
            })
            .await
            .context("spawn read trailer task")??;
            hex::encode(trailer)
        };
        drop(file);

        let final_path = pack_dir.join(format!("pack-{}.pack", pack_hash));
        tmp.persist(&final_path)
            .with_context(|| format!("rename {} pack to {}", label, final_path.display()))?;
        std::fs::write(pack_dir.join(format!("pack-{}.idx", pack_hash)), &idx_data)
            .with_context(|| format!("write {} idx {}", label, pack_hash))?;
        info!("wrote {} pack {} ({} bytes)", label, pack_hash, pack_bytes);
        Ok(pack_bytes + idx_data.len() as u64)
    }

    #[allow(deprecated)]
    fn spawn_fetch_manifest(
        self,
        hash: String,
        signed_url: Option<String>,
        manifest_tx: tokio::sync::oneshot::Sender<Arc<ClonepackManifest>>,
    ) -> tokio::task::JoinHandle<Result<Arc<ClonepackManifest>>> {
        tokio::spawn(async move {
            let data = self
                .fetch_artifact_with_url(&hash, signed_url.as_deref())
                .await
                .context("fetch clonepack manifest")?;
            let mut manifest =
                ClonepackManifest::decode(data.as_ref()).context("decode clonepack manifest")?;
            // Backwards compatibility: older manifests store the head-blobs pack as
            // a single `head_blobs_pack` field. Treat it as one chunk so the
            // pipeline can use it without special cases.
            if manifest.head_blobs_chunks.is_empty()
                && let Some(pack) = manifest.head_blobs_pack.take()
            {
                manifest.head_blobs_chunks.push(pack);
            }
            let manifest = Arc::new(manifest);
            // Hand the manifest to the downloader. Ignore the error: the receiver
            // is absent in non-archive modes. On the failure paths above, the
            // sender is dropped instead, so the receiver observes the failure.
            let _ = manifest_tx.send(Arc::clone(&manifest));
            Ok(manifest)
        })
    }

    fn spawn_fetch_metadata(
        self,
        hash: String,
        signed_url: Option<String>,
    ) -> tokio::task::JoinHandle<Result<MetadataChunk>> {
        tokio::spawn(async move {
            let data = self
                .fetch_artifact_with_url(&hash, signed_url.as_deref())
                .await
                .with_context(|| format!("fetch metadata chunk {hash}"))?;
            let metadata = MetadataChunk::decode(data.as_ref()).context("decode metadata chunk")?;
            Ok(metadata)
        })
    }

    fn spawn_chunk_downloads(
        self,
        signed_urls: Option<Vec<Option<String>>>,
        manifest_rx: tokio::sync::oneshot::Receiver<Arc<ClonepackManifest>>,
        tx: tokio::sync::mpsc::Sender<(usize, Result<bytes::Bytes>)>,
    ) -> tokio::task::JoinHandle<Result<u64>> {
        tokio::spawn(async move {
            use futures::stream::{self, StreamExt, TryStreamExt};

            let signed_urls: Vec<Option<String>> = signed_urls.unwrap_or_default();

            // Wait for the manifest so the downloader follows its chunk table,
            // not a possibly-stale signed-URL list. A receive error means the
            // manifest fetch failed (sender dropped); stop and let `tx` drop so
            // the extractor sees EOF. The real error surfaces from the manifest
            // task itself.
            let manifest = match manifest_rx.await {
                Ok(manifest) => manifest,
                Err(_) => return Ok(0),
            };
            // Bound concurrent chunk downloads. Backpressure is async: if the
            // downstream bridge/extractor falls behind, futures await `send()`
            // without blocking Tokio worker threads.
            let conc = ClientTuning::load().archive_fetch_concurrency;
            let jobs: Vec<(usize, ChunkRef, Option<String>)> = manifest
                .archive_chunks
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, chunk_ref)| {
                    let signed_url = signed_urls.get(index).cloned().flatten();
                    (index, chunk_ref, signed_url)
                })
                .collect();

            stream::iter(jobs)
                .map(|(index, chunk_ref, signed_url)| {
                    let client = self.clone();
                    let tx = tx.clone();
                    async move {
                        // No by-hash gateway fallback: a failed signed URL surfaces as
                        // StaleSignedUrl and the clone driver re-resolves for fresh URLs.
                        let fetch_start = Instant::now();
                        let bytes = client
                            .fetch_chunk_ref(&chunk_ref, signed_url.as_deref())
                            .await
                            .with_context(|| format!("fetch archive chunk {}", index))?;
                        let len = bytes.len() as u64;
                        crate::perf::record_archive_download(fetch_start.elapsed(), len);
                        tx.send((index, Ok(bytes))).await.map_err(|_| {
                            anyhow::anyhow!("archive chunk {} receiver dropped", index)
                        })?;
                        Ok::<u64, anyhow::Error>(len)
                    }
                })
                .buffer_unordered(conc)
                .try_fold(0u64, |acc, len| async move { Ok(acc + len) })
                .await
        })
    }

    fn write_origin_config(&self, origin_url: &str, git_dir: &Path) -> Result<()> {
        let config = format!(
            "[core]\n\tsymlinks = true\n\tcheckStat = minimal\n[remote \"origin\"]\n\turl = {origin_url}\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
        );
        std::fs::write(git_dir.join("config"), config)?;
        Ok(())
    }

    /// Materialize the working tree for a git worktree into `work_tree`.
    /// `git_dir` is the worktree-specific metadata directory (usually inside
    /// the main repo's `.git/worktrees/`). Objects are shared with the main
    /// repo via `commondir`, so we only need the skeleton/head packs and the
    /// prebuilt index for this commit.
    pub async fn install_worktree_files<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        _owner: &str,
        _repo: &str,
        info: &RefResponse,
        clonepack: &ClonepackManifest,
        metadata: Arc<MetadataChunk>,
        archive_chunks: &[String],
        signed_chunk_urls: Option<Vec<Option<String>>>,
        git_dir: P,
        work_tree: Q,
    ) -> Result<()> {
        let git_dir = git_dir.as_ref().to_path_buf();
        let work_tree = work_tree.as_ref().to_path_buf();

        std::fs::create_dir_all(&git_dir)?;
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;

        let skeleton_hash = cas_hash(&metadata.skeleton_pack);

        std::fs::write(
            pack_dir.join(format!("pack-{}.pack", skeleton_hash)),
            &metadata.skeleton_pack,
        )?;
        std::fs::write(
            pack_dir.join(format!("pack-{}.idx", skeleton_hash)),
            &metadata.skeleton_idx,
        )?;

        // Head-blobs pack is fetched separately. Archive-extraction does not
        // need it; direct-install needs the blob objects for checkout-index.
        let use_archive =
            std::env::var_os("RIPCLONE_EXTRACT_ARCHIVE").is_some() && !archive_chunks.is_empty();
        if !use_archive {
            self.install_prebuilt_blob_pack(clonepack, info, &pack_dir)
                .await
                .context("install head-blobs pack")?;
        }

        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;

        let checkout_start = Instant::now();
        tokio::task::spawn_blocking({
            let git_dir = git_dir.clone();
            move || git::clear_skip_worktree_all_git_dir(&git_dir)
        })
        .await
        .context("clear skip-worktree task")??;

        if use_archive {
            let archive_chunks = archive_chunks.to_vec();
            let work_tree2 = work_tree.clone();
            let server = self.server.clone();
            let auth_header = self.auth_header.clone();
            tokio::task::spawn_blocking(move || {
                let mut manifest_tmp =
                    tempfile::NamedTempFile::new().context("create temp manifest")?;
                metadata
                    .write(&mut manifest_tmp)
                    .context("write temp manifest")?;
                let manifest_path = manifest_tmp.path().to_path_buf();
                let _tmp = manifest_tmp;
                extract_clonepack_streaming(
                    &manifest_path,
                    &archive_chunks,
                    signed_chunk_urls,
                    &work_tree2,
                    None,
                    &server,
                    auth_header.as_deref(),
                )
            })
            .await
            .context("archive extraction task")??;
            info!(
                "extracted worktree files from archive chunks into {} in {:?}",
                work_tree.display(),
                checkout_start.elapsed()
            );
        } else {
            tokio::task::spawn_blocking({
                let git_dir = git_dir.clone();
                let work_tree = work_tree.clone();
                move || git::checkout_index_with_git_dir(&git_dir, &work_tree)
            })
            .await
            .context("checkout-index task")??;
            info!(
                "checked out worktree files into {} in {:?}",
                work_tree.display(),
                checkout_start.elapsed()
            );
        }

        Ok(())
    }

    /// Add a git worktree at `target` for `branch` of `repo_path`, using the
    /// main repo at `main_repo`. The working tree files are materialized
    /// directly or through overlay staging (when available and beneficial),
    /// just like `install_repo`.
    pub async fn add_worktree<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        repo_path: &str,
        branch: &str,
        main_repo: P,
        target: Q,
    ) -> Result<()> {
        let main_repo = main_repo.as_ref().to_path_buf();
        let target = target.as_ref().to_path_buf();

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        let info = self
            .resolve_ref_with_clonepack(repo_path, branch, None, None)
            .await?;
        let commit = info.commit.clone();

        let (clonepack, metadata) = self.fetch_clonepack(&info).await?;
        let archive_chunks: Vec<String> = clonepack
            .archive_chunks
            .iter()
            .map(|r| hash_to_hex(&r.hash))
            .collect();

        // Ask git to create the worktree metadata, but do not populate files.
        let add_start = Instant::now();
        tokio::task::spawn_blocking({
            let main_repo = main_repo.clone();
            let target = target.clone();
            let commit = commit.clone();
            move || {
                // Remove stale registrations from earlier interrupted runs.
                let _ = std::process::Command::new("git")
                    .arg("-C")
                    .arg(&main_repo)
                    .args(["worktree", "prune"])
                    .status();

                let status = std::process::Command::new("git")
                    .arg("-C")
                    .arg(&main_repo)
                    .args([
                        "worktree",
                        "add",
                        "--no-checkout",
                        "--detach",
                        target.to_str().unwrap(),
                        &commit,
                    ])
                    .status()
                    .context("spawn git worktree add")?;
                if !status.success() {
                    anyhow::bail!("git worktree add failed");
                }
                Ok(())
            }
        })
        .await
        .context("worktree add task")??;
        info!(
            "git worktree add metadata created in {:?}",
            add_start.elapsed()
        );

        // The .git file created by git points to the worktree metadata dir.
        let git_file = target.join(".git");
        let git_file_content = tokio::fs::read_to_string(&git_file)
            .await
            .with_context(|| format!("reading {}", git_file.display()))?;
        let git_dir = git_file_content
            .lines()
            .find_map(|line| line.strip_prefix("gitdir:"))
            .map(|s| PathBuf::from(s.trim()))
            .context("missing gitdir: in worktree .git file")?;

        // Decide whether to stage files in tmpfs and expose them through overlay.
        let staging_dir = overlay::staging_dir();
        let use_overlay = self.should_use_overlay(&metadata, &staging_dir).await;

        let local_index = main_repo.join(".git").join("index");
        let local_commit = local_rev_parse(&main_repo, branch).ok();
        let reuse_local = local_commit.as_ref() == Some(&commit) && local_index.exists();

        let materialize = |git_dir: &Path, work_tree: &Path| -> Result<()> {
            if reuse_local {
                std::fs::copy(&local_index, git_dir.join("index"))
                    .context("copy main repo index to worktree")?;
            }
            tokio::task::block_in_place(|| git::clear_skip_worktree_all_git_dir(git_dir))?;
            tokio::task::block_in_place(|| git::checkout_index_with_git_dir(git_dir, work_tree))?;
            Ok(())
        };

        if use_overlay {
            let dirs = overlay::OverlayDirs::create(&staging_dir, &target)
                .context("create overlay staging dirs")?;

            if reuse_local {
                materialize(&git_dir, &dirs.lower)
                    .context("materialize worktree files into overlay lower dir")?;
            } else {
                self.install_worktree_files(
                    "",
                    "",
                    &info,
                    &clonepack,
                    Arc::clone(&metadata),
                    &archive_chunks,
                    info.archive_chunk_urls.clone(),
                    &git_dir,
                    &dirs.lower,
                )
                .await?;
            }

            // Preserve the worktree pointer inside the overlay lower dir.
            std::fs::write(dirs.lower.join(".git"), &git_file_content)?;

            // Remove the empty placeholder directory before mounting.
            std::fs::remove_dir_all(&target)
                .with_context(|| format!("remove placeholder {}", target.display()))?;
            std::fs::create_dir_all(&target)?;

            overlay::mount_dirs(&dirs).context("mount overlay at worktree")?;
            info!(
                "mounted overlay worktree {} -> {} (staging {})",
                dirs.lower.display(),
                target.display(),
                staging_dir.display()
            );
        } else {
            if reuse_local {
                materialize(&git_dir, &target).context("materialize worktree files")?;
            } else {
                self.install_worktree_files(
                    "",
                    "",
                    &info,
                    &clonepack,
                    Arc::clone(&metadata),
                    &archive_chunks,
                    info.archive_chunk_urls.clone(),
                    &git_dir,
                    &target,
                )
                .await?;
            }
            std::fs::write(target.join(".git"), &git_file_content)?;
        }

        info!(
            "added worktree {}@{} at {}",
            repo_path,
            branch,
            target.display()
        );
        Ok(())
    }

    async fn should_use_overlay(&self, metadata: &MetadataChunk, staging_dir: &Path) -> bool {
        if !overlay::is_available() {
            return false;
        }
        let raw_bytes: u64 = metadata.files.iter().map(|f| f.total_len()).sum();
        // Sum the compressed length of every frame; archive chunks contain only
        // frames, so this is the total compressed archive size.
        let compressed_bytes: u64 = metadata
            .frames
            .iter()
            .map(|f| f.compressed_len as u64)
            .sum();

        // No size threshold: overlay is opt-in (see overlay::is_available), so if
        // the operator asked for it we honor it for any repo, falling back only
        // when there isn't enough tmpfs space or the kernel disallows the mount.
        let margin_mb: u64 = 128;
        let required = raw_bytes + compressed_bytes + margin_mb * 1024 * 1024;
        let available = overlay::available_space(staging_dir).unwrap_or(0);
        if available < required {
            warn!(
                "overlay staging wants {} MB but only {} MB available in {}; falling back to direct extraction",
                required / 1024 / 1024,
                available / 1024 / 1024,
                staging_dir.display()
            );
            return false;
        }

        if !overlay::test_mount(staging_dir) {
            warn!("overlay test mount failed; falling back to direct extraction");
            return false;
        }

        info!(
            "using overlay staging (raw {} MB, compressed {} MB, available {} MB)",
            raw_bytes / 1024 / 1024,
            compressed_bytes / 1024 / 1024,
            available / 1024 / 1024
        );
        true
    }

    /// Install only the `.git` objects, packs, and prebuilt index needed for
    /// git to check out the working tree itself. Used by the git remote helper
    /// so that `git clone`/`git fetch` can proceed normally after the helper
    /// seeds the object database.
    /// Install every pack referenced by `manifest.packs` into `pack_dir`. The
    /// git remote helper acts as a first-class transport, so a `git clone
    /// ripclone://...` must materialize the full history (not just the HEAD
    /// closure) just like any other remote.
    async fn install_manifest_packs(
        &self,
        manifest: &ClonepackManifest,
        info: &RefResponse,
        pack_dir: &std::path::Path,
    ) -> Result<u64> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let packs: Vec<_> = manifest.packs.to_vec();
        if packs.is_empty() {
            anyhow::bail!("clonepack has no packs for git-dir install");
        }

        let idx_bundle: Option<Arc<bytes::Bytes>> = match manifest.idx_bundle.as_ref() {
            Some(b) => Some(Arc::new(
                self.fetch_chunk_ref(b, info.idx_bundle_url.as_deref())
                    .await
                    .context("fetch idx bundle")?,
            )),
            None => None,
        };

        let pack_urls = info.pack_chunk_urls.clone().unwrap_or_default();
        let idx_urls = info.pack_idx_urls.clone().unwrap_or_default();

        let downloads = stream::iter(packs.into_iter().enumerate()).map(|(i, entry)| {
            let client = self.clone();
            let pack_url = pack_urls.get(i).and_then(|o| o.clone());
            let idx_url = idx_urls.get(i).and_then(|o| o.clone());
            let idx_bundle = idx_bundle.clone();
            async move {
                let pack_ref = entry
                    .pack
                    .as_ref()
                    .with_context(|| format!("pack {} missing pack ref", i))?;
                let idx_ref = entry
                    .idx
                    .as_ref()
                    .with_context(|| format!("pack {} missing idx ref", i))?;
                let (pack_bytes, idx_bytes) = if let Some(bundle) = idx_bundle.as_ref() {
                    let off = entry.idx_bundle_offset as usize;
                    let end = off
                        .checked_add(idx_ref.len as usize)
                        .context("idx bundle offset overflow")?;
                    // Zero-copy view into the shared bundle (refcounted Bytes).
                    if bundle.get(off..end).is_none() {
                        anyhow::bail!("idx {} slice out of bundle range", i);
                    }
                    let slice = bundle.slice(off..end);
                    let want = hash_to_hex(&idx_ref.hash);
                    let got = crate::cas::hash(&slice);
                    if got != want {
                        anyhow::bail!(
                            "idx {i} bundle slice hash mismatch: expected {want}, got {got}"
                        );
                    }
                    let pack_bytes = client
                        .fetch_chunk_ref(pack_ref, pack_url.as_deref())
                        .await
                        .with_context(|| format!("fetch pack {}", i))?;
                    (pack_bytes, slice)
                } else {
                    tokio::try_join!(
                        client.fetch_chunk_ref(pack_ref, pack_url.as_deref()),
                        client.fetch_chunk_ref(idx_ref, idx_url.as_deref()),
                    )
                    .with_context(|| format!("fetch pack {}", i))?
                };
                Ok::<(bytes::Bytes, bytes::Bytes), anyhow::Error>((pack_bytes, idx_bytes))
            }
        });

        let results: Vec<_> = downloads.buffer_unordered(4).try_collect().await?;
        let mut total = 0u64;
        for (pack_bytes, idx_bytes) in results {
            if pack_bytes.len() < 20 {
                anyhow::bail!("pack too short ({} bytes)", pack_bytes.len());
            }
            let name = hex::encode(&pack_bytes[pack_bytes.len() - 20..]);
            std::fs::write(pack_dir.join(format!("pack-{}.pack", name)), &pack_bytes)
                .with_context(|| format!("write pack {}", name))?;
            std::fs::write(pack_dir.join(format!("pack-{}.idx", name)), &idx_bytes)
                .with_context(|| format!("write idx {}", name))?;
            total += (pack_bytes.len() + idx_bytes.len()) as u64;
        }
        Ok(total)
    }

    pub async fn install_git_dir<P: AsRef<Path>>(
        &self,
        branch: &str,
        info: &RefResponse,
        git_dir: P,
    ) -> Result<()> {
        let git_dir = git_dir.as_ref().to_path_buf();

        if info.clonepack_manifest.is_empty() {
            anyhow::bail!("ref is missing clonepack manifest; run sync first");
        }

        std::fs::create_dir_all(&git_dir)?;
        std::fs::create_dir_all(git_dir.join("refs").join("heads"))?;
        std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
        std::fs::create_dir_all(git_dir.join("info"))?;

        let branch_name = if branch == "HEAD" {
            if info.default_branch.is_empty() {
                "main"
            } else {
                &info.default_branch
            }
        } else {
            branch
        };

        std::fs::write(
            git_dir.join("HEAD"),
            format!("ref: refs/heads/{}\n", branch_name),
        )?;
        let branch_ref = git_dir.join("refs").join("heads").join(branch_name);
        if let Some(parent) = branch_ref.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(branch_ref, format!("{}\n", info.commit))?;
        std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;
        if info.shallow {
            std::fs::write(git_dir.join("shallow"), format!("{}\n", info.commit))?;
        }

        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;

        let dl_start = Instant::now();
        let (clonepack, metadata) = self.fetch_clonepack(info).await?;
        info!(
            "downloaded metadata chunk ({} bytes) in {:?}",
            metadata.skeleton_pack.len()
                + metadata.skeleton_idx.len()
                + metadata.prebuilt_index.len(),
            dl_start.elapsed()
        );

        let skeleton_hash = cas_hash(&metadata.skeleton_pack);
        std::fs::write(
            pack_dir.join(format!("pack-{}.pack", skeleton_hash)),
            &metadata.skeleton_pack,
        )?;
        std::fs::write(
            pack_dir.join(format!("pack-{}.idx", skeleton_hash)),
            &metadata.skeleton_idx,
        )?;

        let head_blobs_refs = head_blobs_chunk_refs(&clonepack);
        let total = if !head_blobs_refs.is_empty() {
            let idx_ref = clonepack
                .head_blobs_idx
                .as_ref()
                .context("clonepack missing head-blobs idx")?;
            let (chunks, idx_data) = tokio::join!(
                self.fetch_chunk_refs(&head_blobs_refs, info.head_blobs_chunk_urls.as_deref()),
                self.fetch_chunk_ref(idx_ref, info.head_blobs_idx_url.as_deref()),
            );
            let chunks = chunks?;
            let idx_data = idx_data?;
            let pack_data: Vec<u8> = chunks.into_iter().flatten().collect();
            let head_blobs_hash = cas_hash(&pack_data);
            std::fs::write(
                pack_dir.join(format!("pack-{}.pack", head_blobs_hash)),
                &pack_data,
            )?;
            std::fs::write(
                pack_dir.join(format!("pack-{}.idx", head_blobs_hash)),
                &idx_data,
            )?;
            info!("wrote legacy head-blobs pack ({} bytes)", pack_data.len());
            (pack_data.len() + idx_data.len()) as u64
        } else if clonepack.packs.iter().any(|p| !p.history_only) {
            self.install_manifest_packs(&clonepack, info, &pack_dir)
                .await
                .context("install HEAD-closure packs")?
        } else {
            anyhow::bail!("clonepack missing head-blobs pack");
        };

        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;

        info!(
            "installed .git for {} refs/heads/{} ({} bytes)",
            &info.commit[..7],
            branch_name,
            total
        );
        Ok(())
    }

    /// Install prebuilt artifacts into an existing `.git` directory and
    /// materialize the working tree at `work_tree`. Used by the git remote
    /// helper, which already owns the `.git` directory and remote config.
    pub async fn install_ref<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        _owner: &str,
        _repo: &str,
        branch: &str,
        info: &RefResponse,
        clonepack: &ClonepackManifest,
        metadata: Arc<MetadataChunk>,
        archive_chunks: &[String],
        signed_chunk_urls: Option<Vec<Option<String>>>,
        git_dir: P,
        work_tree: Q,
    ) -> Result<()> {
        let git_dir = git_dir.as_ref().to_path_buf();
        let work_tree = work_tree.as_ref().to_path_buf();

        std::fs::create_dir_all(&git_dir)?;
        std::fs::create_dir_all(git_dir.join("refs").join("heads"))?;
        std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
        std::fs::create_dir_all(git_dir.join("info"))?;

        let branch_name = if branch == "HEAD" {
            if info.default_branch.is_empty() {
                "main"
            } else {
                &info.default_branch
            }
        } else {
            branch
        };

        // HEAD points at the resolved branch; create the branch ref as well so
        // `git upload-pack` and checkout have a ref to advertise/fetch.
        std::fs::write(
            git_dir.join("HEAD"),
            format!("ref: refs/heads/{}\n", branch_name),
        )?;
        let branch_ref = git_dir.join("refs").join("heads").join(branch_name);
        if let Some(parent) = branch_ref.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(branch_ref, format!("{}\n", info.commit))?;

        // Exclude ripclone metadata from git status.
        std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;

        // Object packs.
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;

        // Write the .git artifacts from the metadata chunk. The working tree is
        // materialized with `git checkout-index` by default; set
        // `RIPCLONE_EXTRACT_ARCHIVE=1` to materialize from archive chunks instead.
        let skeleton_hash = cas_hash(&metadata.skeleton_pack);

        std::fs::write(
            pack_dir.join(format!("pack-{}.pack", skeleton_hash)),
            &metadata.skeleton_pack,
        )?;
        std::fs::write(
            pack_dir.join(format!("pack-{}.idx", skeleton_hash)),
            &metadata.skeleton_idx,
        )?;
        info!(
            "wrote skeleton pack ({} bytes)",
            metadata.skeleton_pack.len()
        );

        // Head-blobs pack is fetched separately. Archive-extraction does not
        // need it because the working tree is built from archive chunks; only
        // direct-install (`git checkout-index`) needs the blob objects.
        let use_archive =
            std::env::var_os("RIPCLONE_EXTRACT_ARCHIVE").is_some() && !archive_chunks.is_empty();
        if !use_archive {
            let head_blobs_refs = head_blobs_chunk_refs(clonepack);
            if head_blobs_refs.is_empty() {
                anyhow::bail!("clonepack missing head-blobs pack for direct-install");
            }
            let idx_ref = clonepack
                .head_blobs_idx
                .as_ref()
                .context("clonepack missing head-blobs idx")?;
            let (chunks, idx_data) = tokio::join!(
                self.fetch_chunk_refs(&head_blobs_refs, info.head_blobs_chunk_urls.as_deref()),
                self.fetch_chunk_ref(idx_ref, info.head_blobs_idx_url.as_deref()),
            );
            let chunks = chunks?;
            let idx_data = idx_data?;
            let pack_data: Vec<u8> = chunks.into_iter().flatten().collect();
            let head_blobs_hash = cas_hash(&pack_data);
            std::fs::write(
                pack_dir.join(format!("pack-{}.pack", head_blobs_hash)),
                &pack_data,
            )?;
            std::fs::write(
                pack_dir.join(format!("pack-{}.idx", head_blobs_hash)),
                &idx_data,
            )?;
            info!("wrote head-blobs pack ({} bytes)", pack_data.len());
        }

        // Prebuilt index.
        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;
        info!(
            "wrote prebuilt index ({} bytes)",
            metadata.prebuilt_index.len()
        );

        // Clear skip-worktree bits so git will actually materialize files,
        // then let git write the working tree efficiently.
        let checkout_start = Instant::now();
        let cleared = tokio::task::spawn_blocking({
            let work_tree = work_tree.clone();
            move || git::clear_skip_worktree_all(&work_tree)
        })
        .await
        .context("clear skip-worktree task")??;
        info!(
            "cleared skip-worktree for {} entries in {:?}",
            cleared,
            checkout_start.elapsed()
        );

        if std::env::var_os("RIPCLONE_EXTRACT_ARCHIVE").is_some() && !archive_chunks.is_empty() {
            let archive_chunks = archive_chunks.to_vec();
            let work_tree2 = work_tree.clone();
            let server = self.server.clone();
            let auth_header = self.auth_header.clone();
            tokio::task::spawn_blocking(move || {
                let mut manifest_tmp =
                    tempfile::NamedTempFile::new().context("create temp manifest")?;
                metadata
                    .write(&mut manifest_tmp)
                    .context("write temp manifest")?;
                let manifest_path = manifest_tmp.path().to_path_buf();
                let _tmp = manifest_tmp;
                extract_clonepack_streaming(
                    &manifest_path,
                    &archive_chunks,
                    signed_chunk_urls,
                    &work_tree2,
                    None,
                    &server,
                    auth_header.as_deref(),
                )
            })
            .await
            .context("archive extraction task")??;
            info!(
                "extracted working tree from archive chunks into {} in {:?}",
                work_tree.display(),
                checkout_start.elapsed()
            );
        } else {
            tokio::task::spawn_blocking({
                let work_tree = work_tree.clone();
                move || git::checkout_index(&work_tree)
            })
            .await
            .context("checkout-index task")??;
            info!(
                "checked out working tree into {} in {:?}",
                work_tree.display(),
                checkout_start.elapsed()
            );
        }

        info!(
            "installed ref into {} / {}",
            git_dir.display(),
            work_tree.display()
        );
        Ok(())
    }

    /// Fetch a single file's content from the server.
    pub async fn cat_file(&self, repo_path: &str, branch: &str, path: &str) -> Result<Vec<u8>> {
        self.fetch_file(repo_path, branch, path).await
    }

    /// Fetch a single file's content from the server by path.
    pub async fn fetch_file(&self, repo_path: &str, branch: &str, path: &str) -> Result<Vec<u8>> {
        let url = self.repo_url(
            repo_path,
            &format!("/cat?path={}&branch={}", urlencoding::encode(path), branch),
        );
        let resp = self.send(self.request(reqwest::Method::GET, &url)).await?;
        if !resp.status().is_success() {
            return Err(server_error("cat failed", resp).await);
        }
        Ok(resp.bytes().await?.to_vec())
    }
}

/// Try to resolve `branch` in a local repo without contacting the server.
fn local_rev_parse(main_repo: &Path, branch: &str) -> Result<String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(main_repo)
        .args(["rev-parse", branch])
        .output()
        .context("spawn git rev-parse")?;
    if !output.status.success() {
        anyhow::bail!("git rev-parse {} failed", branch);
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paywall_hint_is_machine_parseable_with_subscribe_url() {
        // A paid-plan block on the managed cloud must point at the subscribe
        // URL so an agent fleet can detect and route it.
        let hint = error_hint(403, Some("no_plan"), true);
        assert!(
            hint.contains("https://ripclone.com"),
            "no_plan hint: {hint}"
        );
        // A bare 402 (payment required) also carries the subscribe URL.
        let hint = error_hint(402, None, true);
        assert!(hint.contains("https://ripclone.com"), "402 hint: {hint}");
        // A generic self-host 403 does not fabricate a subscribe URL.
        assert!(!error_hint(403, None, false).contains("ripclone.com"));
    }

    #[test]
    fn fsync_tree_walks_files_dirs_and_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("sub/nested")).unwrap();
        std::fs::write(root.join("top.txt"), b"hello").unwrap();
        std::fs::write(root.join("sub/inner.txt"), b"world").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("top.txt", root.join("link")).unwrap();
        // A full tree with a dangling-capable symlink must fsync cleanly.
        fsync_tree(root).expect("fsync whole tree");
    }

    #[test]
    fn fsync_requested_reads_env_flag() {
        // Default (unset) is off; explicit truthy values turn it on. Guarded so
        // the env var is restored regardless of the assertions.
        let prev = std::env::var("RIPCLONE_FSYNC").ok();
        unsafe {
            std::env::remove_var("RIPCLONE_FSYNC");
        }
        assert!(!fsync_requested());
        unsafe {
            std::env::set_var("RIPCLONE_FSYNC", "1");
        }
        assert!(fsync_requested());
        unsafe {
            match prev {
                Some(v) => std::env::set_var("RIPCLONE_FSYNC", v),
                None => std::env::remove_var("RIPCLONE_FSYNC"),
            }
        }
    }

    #[test]
    fn detects_stale_signed_url_through_the_error_chain() {
        // As surfaced from fetch_artifact_with_url, then wrapped with context as
        // it propagates up through the install pipeline.
        let err = anyhow::Error::new(StaleSignedUrl)
            .context("signed-URL fetch for abc123 failed")
            .context("fetch archive chunk 4")
            .context("install editable packs");
        assert!(is_stale_signed_url(&err));
    }

    #[test]
    fn ordinary_errors_are_not_stale() {
        let err = anyhow::anyhow!("ref lookup failed: 404").context("clone");
        assert!(!is_stale_signed_url(&err));
    }

    #[test]
    fn retry_decision_retries_stale_until_the_cap_then_stops() {
        let stale = anyhow::Error::new(StaleSignedUrl).context("fetch chunk");
        // The clone driver takes up to 2 retries (attempts 0 and 1), then stops.
        assert!(should_retry_stale(0, 2, &stale), "first failure retries");
        assert!(should_retry_stale(1, 2, &stale), "second failure retries");
        assert!(
            !should_retry_stale(2, 2, &stale),
            "stops once the retry cap is reached"
        );
    }

    #[test]
    fn retry_decision_never_retries_a_non_stale_error() {
        let other = anyhow::anyhow!("repo not found");
        assert!(!should_retry_stale(0, 2, &other));
    }

    /// A first-run user who points at a server that isn't running (or a wrong
    /// `--server` / `RIPCLONE_SERVER`) must get a message that names the server
    /// and says what to check — not the bare reqwest "error sending request"
    /// chain that hides the real cause.
    #[tokio::test]
    async fn unreachable_server_names_the_server_and_hints() {
        // Bind then immediately drop to claim a port nothing is listening on, so
        // the connect is refused deterministically.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client = Client::new(format!("http://127.0.0.1:{port}"));
        let err = client.add_repo("acme/widget").await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("could not reach ripclone server"),
            "message names the unreachable server: {msg}"
        );
        assert!(
            msg.contains("RIPCLONE_SERVER") || msg.contains("--server"),
            "message says what to check: {msg}"
        );
        assert!(
            !msg.contains("error sending request for url"),
            "the noisy reqwest wrapper is replaced, not surfaced: {msg}"
        );
    }
}
