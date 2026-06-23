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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

/// Sent on every request so the server can attribute usage and nudge upgrades.
const USER_AGENT: &str = concat!("ripclone/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Deserialize)]
struct ServerError {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    code: Option<String>,
}

/// Turn a non-success HTTP response into a clear, actionable error. Parses the
/// `{ "error", "code" }` body the gateway returns and appends a next-step hint
/// keyed on status/code. Surfaces an upgrade nudge from `X-Ripclone-Upgrade`.
async fn server_error(context: &str, resp: reqwest::Response) -> anyhow::Error {
    let status = resp.status();
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
    let hint = match (status.as_u16(), code) {
        (401, _) => "\n  → set RIPCLONE_TOKEN (create one at https://ripclone.com/tokens)",
        (403, Some("no_plan")) => {
            "\n  → this org needs a plan; the owner can subscribe at https://ripclone.com"
        }
        (403, Some("no_access")) => "\n  → you don't have GitHub access to this repo",
        (403, _) => "\n  → the org may need a plan, or you lack GitHub access",
        (429, _) => "\n  → rate limited; wait a moment and retry",
        (502 | 503, _) => "\n  → ripclone is briefly unavailable; retry shortly",
        _ => "",
    };
    if let Some(u) = upgrade {
        eprintln!("ripclone: {u}");
    }
    anyhow::anyhow!("{context}: {msg}{hint}")
}

/// Build a reqwest client that always sends our User-Agent (and any default
/// headers, e.g. the auth token).
fn build_http_client(headers: reqwest::header::HeaderMap) -> reqwest::Client {
    reqwest::ClientBuilder::new()
        .user_agent(USER_AGENT)
        .default_headers(headers)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

#[derive(Debug, Clone, Deserialize)]
pub struct RefResponse {
    pub owner: String,
    pub repo: String,
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
) -> Result<Vec<u8>> {
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
) -> std::result::Result<Vec<u8>, (bool, anyhow::Error)> {
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
    let data = match resp.bytes().await {
        Ok(b) => b.to_vec(),
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

#[derive(Clone)]
pub struct Client {
    server: String,
    /// Client that sends the ripclone auth token on every request.
    http: reqwest::Client,
    /// Client with no default auth headers, used for presigned URLs.
    raw_http: reqwest::Client,
    token: Option<String>,
    cache: Option<Cas>,
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
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = &token {
            let value = format!("Ripclone {}", token);
            if let Ok(header_value) = reqwest::header::HeaderValue::from_str(&value) {
                headers.insert(reqwest::header::AUTHORIZATION, header_value);
            }
        }
        let http = build_http_client(headers);
        let cache = cache_dir.and_then(|dir| Cas::new(dir).ok());
        Self {
            server,
            http,
            raw_http: build_http_client(reqwest::header::HeaderMap::new()),
            token,
            cache,
        }
    }

    fn cache_key_from_artifact_url(&self, url: &str) -> Option<String> {
        url.rsplit('/').next().map(|s| s.to_string())
    }
}

fn default_cache_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| {
        let mut path = PathBuf::from(home);
        path.push(".cache");
        path.push("ripclone");
        path
    })
}

impl Client {
    pub async fn resolve_ref(&self, owner: &str, repo: &str, branch: &str) -> Result<RefResponse> {
        self.resolve_ref_with_clonepack(owner, repo, branch, None, None)
            .await
    }

    pub async fn resolve_ref_with_clonepack(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        clonepack: Option<&str>,
        rev: Option<&str>,
    ) -> Result<RefResponse> {
        let mut url = format!(
            "{}/v1/repos/{}/{}/refs/{}",
            self.server, owner, repo, branch
        );
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
        let max_attempts = std::env::var("RIPCLONE_CLONE_MAX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(40usize);
        for attempt in 0..max_attempts {
            let resp = self.http.get(&url).send().await?;
            let status = resp.status();
            if status.is_success() {
                return Ok(resp.json().await?);
            }
            if status == reqwest::StatusCode::ACCEPTED
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                if attempt == 0 {
                    eprintln!("ripclone: warming {owner}/{repo} — this can take a moment…");
                }
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
                anyhow::bail!("{owner}/{repo} is still building after {max_attempts} attempts");
            }
            return Err(server_error("ref lookup failed", resp).await);
        }
        anyhow::bail!("ref lookup did not complete")
    }

    pub async fn fetch_pack(&self, hash: &str) -> Result<Vec<u8>> {
        self.fetch_artifact(hash).await
    }

    pub async fn fetch_object(&self, sha: &str) -> Result<Vec<u8>> {
        let url = format!("{}/v1/objects/{}", self.server, sha);
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("object fetch failed: {}", resp.status());
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Fetch any content-addressed artifact (pack, idx, index, archive, manifest).
    ///
    /// Caches the bytes locally when `RIPCLONE_CACHE_DIR` is set, so repeat
    /// clones of the same repo/commit bypass the network entirely.
    pub async fn fetch_artifact(&self, hash: &str) -> Result<Vec<u8>> {
        self.fetch_artifact_with_url(hash, None).await
    }

    /// Fetch an artifact, optionally using a pre-signed URL directly. Falls back
    /// to `/v1/artifacts/{hash}` when `signed_url` is `None`.
    pub async fn fetch_artifact_with_url(
        &self,
        hash: &str,
        signed_url: Option<&str>,
    ) -> Result<Vec<u8>> {
        let gateway_url = format!("{}/v1/artifacts/{}", self.server, hash);
        let fetch_url = signed_url.unwrap_or(&gateway_url);
        let use_signed_url = signed_url.is_some();

        if let Some(cache) = &self.cache
            && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
            && let Ok(data) = cache.get(&key)
        {
            return Ok(data);
        }

        // Fetch with retry+backoff. Presigned URLs are self-authenticating, so
        // use the no-auth client to avoid leaking the ripclone token to object
        // storage. If a presigned URL fails for good (e.g. expired), fall back
        // to the authenticated gateway once.
        let data = if use_signed_url {
            match fetch_artifact_with_retry(&self.raw_http, fetch_url, hash).await {
                Ok(d) => d,
                Err(signed_err) => {
                    tracing::debug!(
                        "signed-URL fetch for {hash} failed ({signed_err:#}); falling back to gateway"
                    );
                    fetch_artifact_with_retry(&self.http, &gateway_url, hash)
                        .await
                        .map_err(|gw_err| {
                            anyhow::anyhow!(
                                "artifact {hash} fetch failed via signed URL ({signed_err:#}) and gateway ({gw_err:#})"
                            )
                        })?
                }
            }
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
    ) -> Result<Vec<u8>> {
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

    /// Fetch many chunk refs in parallel, preserving order.
    ///
    /// `signed_urls` is indexed by chunk position; `None` entries fall back to
    /// the gateway. Concurrency defaults to 6 but can be overridden with
    /// `RIPCLONE_FETCH_CONCURRENCY`.
    pub async fn fetch_chunk_refs(
        &self,
        chunks: &[crate::clonepack::ChunkRef],
        signed_urls: Option<&[Option<String>]>,
    ) -> Result<Vec<Vec<u8>>> {
        use futures::TryStreamExt;
        use futures::stream::{self, StreamExt};
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        let concurrency: usize = std::env::var("RIPCLONE_FETCH_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6)
            .max(1);
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
        let mut results: Vec<(usize, Vec<u8>)> = stream::iter(jobs)
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
        let clonepack = ClonepackManifest::decode(manifest_data.as_slice())
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
            MetadataChunk::decode(metadata_data.as_slice()).context("decode metadata chunk")?;
        Ok((clonepack, Arc::new(metadata)))
    }

    pub async fn create_snapshot(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        hot_files: usize,
    ) -> Result<SnapshotResponse> {
        let url = format!(
            "{}/v1/repos/{}/{}/snapshot?branch={}&hot_files={}",
            self.server, owner, repo, branch, hot_files
        );
        let resp = self.http.post(&url).send().await?;
        if !resp.status().is_success() {
            return Err(server_error("snapshot create failed", resp).await);
        }
        Ok(resp.json().await?)
    }

    pub async fn fetch_snapshot(&self, hash: &str) -> Result<Vec<u8>> {
        self.fetch_artifact(hash).await
    }

    pub async fn hot_files(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        count: usize,
    ) -> Result<Vec<String>> {
        let url = format!(
            "{}/v1/repos/{}/{}/hotfiles?branch={}&count={}",
            self.server, owner, repo, branch, count
        );
        let resp = self.http.get(&url).send().await?;
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
        let resp = self.http.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            return Err(server_error("batch fetch failed", resp).await);
        }
        Ok(resp.bytes().await?.to_vec())
    }

    pub async fn sync_repo(
        &self,
        owner: &str,
        repo: &str,
        depth: Option<usize>,
        github_token: Option<&str>,
    ) -> Result<RefResponse> {
        self.sync_repo_at(owner, repo, None, depth, github_token)
            .await
    }

    /// Like [`sync_repo`] but builds at `rev` (e.g. "HEAD~5" or a SHA) instead of
    /// the branch tip. The branch is still the ref-store key; only the build
    /// commit is overridden. Useful for exercising the incremental build path
    /// deterministically without waiting for upstream to advance.
    pub async fn sync_repo_at(
        &self,
        owner: &str,
        repo: &str,
        rev: Option<&str>,
        depth: Option<usize>,
        github_token: Option<&str>,
    ) -> Result<RefResponse> {
        let mut url = format!("{}/v1/repos/{}/{}/sync", self.server, owner, repo);
        let mut q: Vec<String> = Vec::new();
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
        let max_attempts = std::env::var("RIPCLONE_SYNC_MAX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(40usize);
        for attempt in 0..max_attempts {
            let mut req = self.http.post(&url);
            if let Some(token) = github_token {
                req = req.header("X-GitHub-Token", token);
            }
            let resp = req.send().await?;
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
    ) -> Result<()> {
        self.install_repo_with_mode_at(owner, repo, branch, None, target, mode, clonepack, bench)
            .await
    }

    /// Like [`install_repo_with_mode`] but resolves `rev` (e.g. "HEAD~5") instead
    /// of the branch tip — clones the artifacts a `sync --at <rev>` built.
    #[allow(clippy::too_many_arguments)]
    pub async fn install_repo_with_mode_at<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        rev: Option<&str>,
        target: P,
        mode: CloneMode,
        clonepack: Option<&str>,
        bench: Option<&mut Benchmark>,
    ) -> Result<()> {
        let target = target.as_ref().to_path_buf();
        info!(
            "installing {}/{}#{} into {} with mode {:?}",
            owner,
            repo,
            branch,
            target.display(),
            mode
        );

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        let mut local_bench = Benchmark::new();
        let bench = bench.unwrap_or(&mut local_bench);

        // 1. Resolve ref (full-history by default; fast clones can request shallow).
        let info = self
            .resolve_ref_with_clonepack(owner, repo, branch, clonepack, rev)
            .await?;
        bench.mark_resolve();
        info!("resolved to commit {}", &info.commit[..7]);

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
        let archive_channel_depth = std::env::var("RIPCLONE_ARCHIVE_CHANNEL_DEPTH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                info.archive_chunk_urls
                    .as_ref()
                    .map_or(2, |urls| urls.len().clamp(2, 64))
            });
        let (archive_tx, archive_rx): (
            Sender<(usize, Result<Vec<u8>>)>,
            Receiver<(usize, Result<Vec<u8>>)>,
        ) = bounded(archive_channel_depth);

        let archive_urls = info.archive_chunk_urls.clone();
        let archive_downloads = if mode.needs_archive() {
            bench.start_archive_download();
            Some(
                self.clone()
                    .spawn_chunk_downloads(archive_urls, manifest_rx, archive_tx),
            )
        } else {
            drop(archive_tx);
            drop(manifest_rx);
            None
        };

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

        // 6. Write the small .git artifacts from the metadata chunk.
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
        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;
        bench.mark_metadata();
        info!(
            "wrote skeleton pack + idx + prebuilt index ({} bytes)",
            metadata.skeleton_pack.len()
                + metadata.skeleton_idx.len()
                + metadata.prebuilt_index.len()
        );

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
            let git_dir_for_blobs = if mode.needs_blob_pack() {
                Some(git_dir.clone())
            } else {
                None
            };
            Some(tokio::task::spawn_blocking(move || {
                // Keep the temp manifest file alive for the duration of extraction.
                let _guard = manifest_tmp;
                extract_archive_from_chunk_receiver(
                    &manifest_path,
                    Some(&work_tree),
                    git_dir_for_blobs.as_deref(),
                    None,
                    rx,
                )
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
        bench.add_bytes(0, archive_bytes + prebuilt_blob_pack_bytes);
        bench.mark_archive_download(archive_bytes + prebuilt_blob_pack_bytes);

        // 9. Origin config + finalization.
        self.write_origin_config(owner, repo, &git_dir)?;

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
            std::fs::rename(&install_root, &target).with_context(|| {
                format!("rename {} to {}", install_root.display(), target.display())
            })?;
        }

        let report = bench.finish();
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
        }

        info!(
            "installed {}/{}#{} into {} with mode {:?}",
            owner,
            repo,
            branch,
            target.display(),
            mode
        );
        Ok(())
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

        // Download and extraction are decoupled stages with independent
        // concurrency. POSIX performed best at one fetch/write worker per core;
        // io_uring benefits from one fetch worker and two write workers per
        // core because each writer can submit larger batched windows.
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let default_download_conc = cores.max(1);
        let default_write_conc = if worktree_writer.is_io_uring() {
            (cores * 2).max(1)
        } else {
            cores.max(1)
        };
        let download_conc: usize = std::env::var("RIPCLONE_FETCH_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_download_conc)
            .max(1);
        let write_conc: usize = std::env::var("RIPCLONE_WRITE_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_write_conc)
            .max(1);

        // Signed URLs (one per pack/idx, matching manifest.packs order). Empty
        // entries fall back to the gateway by hash; with an object-store backend
        // these point straight at the bucket so bytes bypass the server.
        let pack_urls = info.pack_chunk_urls.clone().unwrap_or_default();
        let idx_urls = info.pack_idx_urls.clone().unwrap_or_default();

        // If the manifest ships a single idx bundle, fetch it ONCE and slice each
        // pack's idx out of it locally — instead of one GET per pack idx (cuts
        // per-pack round-trips from 2 to 1). Falls back to per-pack idx fetches
        // for older manifests without a bundle.
        let idx_bundle: Option<Arc<Vec<u8>>> = match manifest.idx_bundle.as_ref() {
            Some(b) => Some(Arc::new(
                self.fetch_chunk_ref(b, info.idx_bundle_url.as_deref())
                    .await
                    .context("fetch idx bundle")?,
            )),
            None => None,
        };

        let jobs: Vec<(usize, PackEntry)> = manifest.packs.iter().cloned().enumerate().collect();

        // Stage 1: download packs (network concurrency `download_conc`).
        let downloads = stream::iter(jobs).map(|(i, entry)| {
            let client = self.clone();
            let pack_url = pack_urls.get(i).and_then(|o| o.clone());
            let idx_url = idx_urls.get(i).and_then(|o| o.clone());
            let idx_bundle = idx_bundle.clone();
            let history_only = entry.history_only;
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
                    // Slice this pack's idx from the bundle and verify its hash;
                    // only the pack itself needs a network fetch.
                    let off = entry.idx_bundle_offset as usize;
                    let end = off
                        .checked_add(idx_ref.len as usize)
                        .context("idx bundle offset overflow")?;
                    let slice = bundle
                        .get(off..end)
                        .with_context(|| format!("idx {} slice out of bundle range", i))?
                        .to_vec();
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
                Ok::<(usize, bool, Vec<u8>, Vec<u8>), anyhow::Error>((
                    i,
                    history_only,
                    pack_bytes,
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
                    let (i, history_only, pack_bytes, idx_bytes) = res?;
                    let bytes = (pack_bytes.len() + idx_bytes.len()) as u64;
                    let result = tokio::task::spawn_blocking(
                        move || -> Result<crate::extract::PackExtractResult> {
                            if pack_bytes.len() < 20 {
                                anyhow::bail!("pack {} too short ({} bytes)", i, pack_bytes.len());
                            }
                            // Git names packs by the 20-byte trailer sha; the idx
                            // pairs to the pack by basename.
                            let name = hex::encode(&pack_bytes[pack_bytes.len() - 20..]);
                            std::fs::write(
                                pack_dir.join(format!("pack-{}.pack", name)),
                                &pack_bytes,
                            )
                            .with_context(|| format!("write pack {}", name))?;
                            std::fs::write(pack_dir.join(format!("pack-{}.idx", name)), &idx_bytes)
                                .with_context(|| format!("write idx {}", name))?;
                            if history_only {
                                return Ok(crate::extract::PackExtractResult {
                                    files: 0,
                                    stats: Vec::new(),
                                });
                            }
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
            .buffer_unordered(write_conc)
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
        let paths: Vec<String> = metadata
            .files
            .iter()
            .map(|e| String::from_utf8_lossy(&e.path).into_owned())
            .collect();
        let work_tree2 = work_tree.to_path_buf();
        tokio::task::spawn_blocking(move || {
            crate::git::clear_skip_worktree_index_with_stats(&work_tree2, &paths, &stat_cache)
        })
        .await
        .context("spawn clear skip-worktree and refresh index stats")??;

        // Install the multi-pack-index so git object lookups stay O(log) across
        // the many installed packs. Prefer the server-pregenerated MIDX (zero
        // client CPU; it indexes the same `pack-<trailer>` files we just wrote);
        // fall back to building it locally for older manifests without one. Best
        // effort either way — without a MIDX the clone is still correct, just
        // with slower per-object lookups.
        if let Some(midx_ref) = manifest.midx.as_ref() {
            match self
                .fetch_chunk_ref(midx_ref, info.midx_url.as_deref())
                .await
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
        let concurrency: usize = std::env::var("RIPCLONE_FETCH_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6)
            .max(1);

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
                ClonepackManifest::decode(data.as_slice()).context("decode clonepack manifest")?;
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
            let metadata =
                MetadataChunk::decode(data.as_slice()).context("decode metadata chunk")?;
            Ok(metadata)
        })
    }

    fn spawn_chunk_downloads(
        self,
        signed_urls: Option<Vec<Option<String>>>,
        manifest_rx: tokio::sync::oneshot::Receiver<Arc<ClonepackManifest>>,
        tx: Sender<(usize, Result<Vec<u8>>)>,
    ) -> tokio::task::JoinHandle<Result<u64>> {
        tokio::spawn(async move {
            let signed_urls: Vec<Option<String>> = signed_urls.unwrap_or_default();
            let mut total_bytes = 0u64;
            let mut handles = Vec::new();

            // Wait for the manifest so the downloader follows its chunk table,
            // not a possibly-stale signed-URL list. A receive error means the
            // manifest fetch failed (sender dropped); stop and let `tx` drop so
            // the extractor sees EOF. The real error surfaces from the manifest
            // task itself.
            let manifest = match manifest_rx.await {
                Ok(manifest) => manifest,
                Err(_) => return Ok(0),
            };
            // Bound concurrent chunk downloads. With CDC the archive can have
            // hundreds-to-thousands of chunks (one per ~4 MiB frame); without a
            // cap, spawning a request + buffering a frame for every chunk at once
            // would exhaust the connection pool and spike memory.
            let conc = std::env::var("RIPCLONE_FETCH_CONCURRENCY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(16usize)
                .max(1);
            let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(conc));
            for (index, chunk_ref) in manifest.archive_chunks.iter().cloned().enumerate() {
                let client = self.clone();
                let tx = tx.clone();
                let signed_url = signed_urls.get(index).cloned().flatten();
                let sem = std::sync::Arc::clone(&sem);
                let handle = tokio::spawn(async move {
                    let _permit = sem
                        .acquire()
                        .await
                        .map_err(|_| anyhow::anyhow!("download semaphore closed"))?;
                    let bytes = match client
                        .fetch_chunk_ref(&chunk_ref, signed_url.as_deref())
                        .await
                    {
                        Ok(bytes) => bytes,
                        Err(e) if signed_url.is_some() => client
                            .fetch_chunk_ref(&chunk_ref, None)
                            .await
                            .with_context(|| {
                                format!(
                                    "fetch archive chunk {} via gateway after signed URL failed: {e:#}",
                                    index
                                )
                            })?,
                        Err(e) => return Err(e).with_context(|| format!("fetch archive chunk {}", index)),
                    };
                    let len = bytes.len() as u64;
                    tx.send((index, Ok(bytes)))
                        .map_err(|_| anyhow::anyhow!("archive chunk {} receiver dropped", index))?;
                    Ok::<u64, anyhow::Error>(len)
                });
                handles.push(handle);
            }

            drop(tx);

            for handle in handles {
                total_bytes += handle.await.context("chunk download task")??;
            }
            Ok(total_bytes)
        })
    }

    fn write_origin_config(&self, owner: &str, repo: &str, git_dir: &Path) -> Result<()> {
        let config = format!(
            "[core]\n\tsymlinks = true\n\tcheckStat = minimal\n[remote \"origin\"]\n\turl = https://github.com/{}/{}.git\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
            owner, repo
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
            let token = self.token.clone();
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
                    None,
                    &server,
                    token.as_deref(),
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

    /// Add a git worktree at `target` for `branch` of `owner/repo`, using the
    /// main repo at `main_repo`. The working tree files are materialized
    /// directly or through overlay staging (when available and beneficial),
    /// just like `install_repo`.
    pub async fn add_worktree<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        main_repo: P,
        target: Q,
    ) -> Result<()> {
        let main_repo = main_repo.as_ref().to_path_buf();
        let target = target.as_ref().to_path_buf();

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        let info = self.resolve_ref(owner, repo, branch).await?;
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
                    owner,
                    repo,
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
                    owner,
                    repo,
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
            "added worktree {} for {}@{} at {}",
            owner,
            repo,
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
        let margin_mb: u64 = std::env::var("RIPCLONE_OVERLAY_MARGIN_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128);
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
        if head_blobs_refs.is_empty() {
            anyhow::bail!("clonepack missing head-blobs pack");
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

        std::fs::write(git_dir.join("index"), &metadata.prebuilt_index)?;

        info!(
            "installed .git for {} refs/heads/{}",
            &info.commit[..7],
            branch_name
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
            let token = self.token.clone();
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
                    None,
                    &server,
                    token.as_deref(),
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

    /// Full clone: download full pack, index-pack it, set HEAD, checkout.
    pub async fn full_clone<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        target: P,
    ) -> Result<()> {
        let target = target.as_ref();
        info!(
            "full cloning {}/{}#{} into {}",
            owner,
            repo,
            branch,
            target.display()
        );

        let info = self.resolve_ref(owner, repo, branch).await?;
        info!("resolved to commit {}", &info.commit[..7]);

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        if info.full_pack.is_empty() {
            anyhow::bail!("full pack not available for this ref");
        }

        let pack_data = self.fetch_pack(&info.full_pack).await?;
        info!("downloaded full pack: {} bytes", pack_data.len());

        git::init(target)?;
        let git_dir = target.join(".git");
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;
        let pack_path = pack_dir.join("full.pack");
        std::fs::write(&pack_path, &pack_data)?;

        git::index_pack(&git_dir, &pack_path)?;
        git::set_head(&git_dir, &info.commit)?;

        info!("checking out working tree...");
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(target.as_os_str())
            .args(["checkout", "-f", &info.commit])
            .status()
            .context("git checkout")?;
        if !status.success() {
            anyhow::bail!("git checkout failed");
        }

        info!(
            "cloned {}/{}#{} into {}",
            owner,
            repo,
            branch,
            target.display()
        );
        Ok(())
    }

    /// Skeleton clone: download skeleton pack, index-pack it, set HEAD. No working tree.
    pub async fn skeleton_clone<P: AsRef<Path>>(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        target: P,
    ) -> Result<()> {
        let target = target.as_ref();
        info!(
            "skeleton cloning {}/{}#{} into {}",
            owner,
            repo,
            branch,
            target.display()
        );

        let info = self.resolve_ref(owner, repo, branch).await?;
        info!("resolved to commit {}", &info.commit[..7]);

        if target.exists() {
            anyhow::bail!("target directory already exists: {}", target.display());
        }

        let (_clonepack, metadata) = self.fetch_clonepack(&info).await?;
        info!(
            "downloaded skeleton pack: {} bytes",
            metadata.skeleton_pack.len()
        );

        git::init(target)?;
        let git_dir = target.join(".git");
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;
        let pack_path = pack_dir.join("skeleton.pack");
        std::fs::write(&pack_path, &metadata.skeleton_pack)?;

        git::index_pack(&git_dir, &pack_path)?;
        git::set_head(&git_dir, &info.commit)?;
        git::read_tree(&git_dir, &info.commit)?;

        // Pre-fill cached file sizes in the index so git status can rely on stat
        // instead of re-reading every blob through the lazy filesystem.
        let sizes = self.fetch_sizes(owner, repo, branch).await?;
        git::update_index_sizes(&git_dir, &sizes)?;

        // Keep symlinks as symlinks and trust only size/mtime for index stat
        // checks. With the materialized archive we set exact modes and mtimes,
        // so we leave core.fileMode at its default (true on Unix) so mode
        // changes remain visible to git.
        std::process::Command::new("git")
            .arg("-C")
            .arg(target.as_os_str())
            .args(["config", "core.symlinks", "true"])
            .status()
            .ok();
        std::process::Command::new("git")
            .arg("-C")
            .arg(target.as_os_str())
            .args(["config", "core.checkStat", "minimal"])
            .status()
            .ok();

        // Add the canonical GitHub remote so the resulting repo behaves like a
        // normal clone (fetch/push work, IDEs recognize origin, etc.).
        let origin_url = format!("https://github.com/{}/{}.git", owner, repo);
        std::process::Command::new("git")
            .arg("-C")
            .arg(target.as_os_str())
            .args(["remote", "add", "origin", &origin_url])
            .status()
            .ok();

        info!(
            "skeleton cloned {}/{}#{} into {}",
            owner,
            repo,
            branch,
            target.display()
        );
        Ok(())
    }

    /// Fetch a single file's content from the server.
    pub async fn cat_file(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        path: &str,
    ) -> Result<Vec<u8>> {
        self.fetch_file(owner, repo, branch, path).await
    }

    /// Fetch a single file's content from the server by path.
    pub async fn fetch_file(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        path: &str,
    ) -> Result<Vec<u8>> {
        let url = format!(
            "{}/v1/repos/{}/{}/cat?path={}&branch={}",
            self.server,
            owner,
            repo,
            urlencoding::encode(path),
            branch
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(server_error("cat failed", resp).await);
        }
        Ok(resp.bytes().await?.to_vec())
    }

    /// Fetch a map of working-tree path to blob size for the given ref.
    pub async fn fetch_sizes(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<std::collections::HashMap<String, u64>> {
        let url = format!(
            "{}/v1/repos/{}/{}/sizes?branch={}",
            self.server, owner, repo, branch
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(server_error("sizes failed", resp).await);
        }
        Ok(resp.json().await?)
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
