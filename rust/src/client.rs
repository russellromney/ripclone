use crate::ExactResultKind;
use crate::bench::Benchmark;
use crate::cas::{Cas, hash as cas_hash};
use crate::clonepack::{
    ChunkRef, ClonepackManifest, MetadataChunk, PackEntry, hash_to_hex, manifest_pack_idx_bytes,
};
use crate::extract::extract_archive_from_chunk_receiver;
use crate::mode::CloneMode;
use crate::overlay;
use crate::provider::{ProviderInstance, ProviderInstanceId, ProviderKind, RepoId};
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use prost::Message;
use serde::Deserialize;
use sha2::Digest as Sha2Digest;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

mod tuning;
use tuning::ClientTuning;

/// Sent on every request so the server can attribute usage and nudge upgrades.
const USER_AGENT: &str = concat!("ripclone/", env!("CARGO_PKG_VERSION"));

static TEST_DOWNLOAD_AUDIT_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Deserialize)]
struct ServerError {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    code: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ArtifactPendingResponse {
    code: String,
    commit: String,
    branch: String,
    status: String,
    queue_depth: usize,
    top_up_supported: Option<bool>,
    top_up_base: Option<RefResponse>,
}

#[derive(Debug, Deserialize)]
struct SyncAcceptedResponse {
    status: String,
    queue_depth: usize,
    commit: String,
    branch: String,
}

#[derive(Debug, Deserialize)]
struct ExactRevisionUnavailableResponse {
    error: String,
    commit: String,
    branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyncAtPin {
    commit: String,
    branch: Option<String>,
}

fn exact_commit_from_revision(rev: Option<&str>) -> Option<String> {
    rev.filter(|value| crate::validation::validate_object_id(value).is_ok())
        .map(str::to_string)
}

fn observe_sync_at_identity(
    pin: &mut Option<SyncAtPin>,
    commit: &str,
    branch: &str,
    response_kind: &str,
) -> Result<()> {
    crate::validation::validate_object_id(commit)
        .with_context(|| format!("validate commit in exact revision {response_kind} response"))?;
    let detached_allowed = pin
        .as_ref()
        .is_some_and(|identity| identity.branch.as_deref().is_none_or(str::is_empty));
    if branch.is_empty() && !detached_allowed {
        anyhow::bail!(
            "sync integrity error: exact revision {response_kind} response omitted branch"
        );
    }
    if !branch.is_empty() {
        crate::validation::validate_checkout_name(branch).with_context(|| {
            format!("validate branch in exact revision {response_kind} response")
        })?;
    }

    if let Some(expected) = pin {
        if expected.commit != commit
            || expected
                .branch
                .as_deref()
                .is_some_and(|expected_branch| expected_branch != branch)
        {
            anyhow::bail!(
                "sync integrity error: exact revision pin {}@{} changed to {branch}@{commit} in a {response_kind} response",
                expected.branch.as_deref().unwrap_or("<unresolved>"),
                expected.commit
            );
        }
        if expected.branch.is_none() {
            expected.branch = Some(branch.to_string());
        }
    } else {
        *pin = Some(SyncAtPin {
            commit: commit.to_string(),
            branch: Some(branch.to_string()),
        });
    }
    Ok(())
}

fn validate_ref_response_identity(
    expected_commit: Option<&str>,
    expected_branch: Option<&str>,
    detached_checkout_allowed: bool,
    commit: &str,
    branch: &str,
    response_kind: &str,
    repo_path: &str,
) -> Result<()> {
    crate::validation::validate_object_id(commit)
        .with_context(|| format!("invalid {response_kind} commit for {repo_path}"))?;
    if branch.is_empty() && !detached_checkout_allowed {
        anyhow::bail!("invalid {response_kind} branch for {repo_path}: checkout name is empty");
    }
    if !branch.is_empty() {
        crate::validation::validate_checkout_name(branch)
            .with_context(|| format!("invalid {response_kind} branch for {repo_path}"))?;
    }
    if let Some(expected) = expected_commit
        && commit != expected
    {
        anyhow::bail!(
            "ref integrity error: expected commit {expected} changed to {commit} in a {response_kind} response"
        );
    }
    if let Some(expected) = expected_branch
        && expected != "HEAD"
        && branch != expected
    {
        anyhow::bail!(
            "ref integrity error: resolved branch {expected} changed to {branch} in a {response_kind} response"
        );
    }
    Ok(())
}

/// Result of one ordinary sync/add admission request. A caller that only needs
/// readiness can use [`Client::sync_repo`] / [`Client::add_repo`], which poll
/// exact pinned metadata after a 202. CLI and webhook-style callers can use the
/// admission methods and return as soon as this value is available.
#[derive(Debug, Clone)]
pub struct SyncAdmission {
    pub commit: String,
    pub branch: String,
    pub accepted: bool,
    pub ready: Option<RefResponse>,
    pub status: String,
    pub queue_depth: usize,
}

/// The selected artifact is not yet available for the commit this clone pinned.
#[derive(Debug)]
pub struct ArtifactPending {
    pub commit: String,
    pub mode: String,
}

impl std::fmt::Display for ArtifactPending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} artifact is still pending for {}; retry the same clone command with `--at {}`",
            self.mode, self.commit, self.commit
        )
    }
}

impl std::error::Error for ArtifactPending {}

fn validate_manifest_commit(manifest: &ClonepackManifest, pinned: &str) -> Result<()> {
    if manifest.commit != pinned {
        anyhow::bail!(
            "clonepack integrity error: manifest commit {} does not match pinned commit {pinned}",
            manifest.commit
        );
    }
    Ok(())
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
/// server's `code`, and whether we are talking to the default hosted server.
/// Pure so the access-error paths stay unit-testable: a failure carries the
/// machine-parseable `code` so an agent fleet can detect and route it without
/// scraping prose.
fn error_hint(status: u16, code: Option<&str>, is_cloud: bool) -> &'static str {
    match (status, code, is_cloud) {
        (401, _, true) => "\n  → run `ripclone login`",
        (401, _, false) => {
            "\n  → run `ripclone login --server <server>` or set RIPCLONE_SERVER_TOKEN"
        }
        (402, _, _) => "\n  → the server returned 402 (payment required) for this repo",
        (403, Some("no_access"), _) => "\n  → you don't have access to this repo",
        (403, _, _) => "\n  → access denied by the configured server",
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
    pub provider: String,
    pub host: String,
    pub origin_url: String,
    pub branch: String,
    pub commit: String,
    pub parent_commit: Option<String>,
    pub clonepack_manifest: String,
    #[serde(default)]
    pub clonepack_manifest_url: Option<String>,
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
    /// Signed URL for the pre-built multi-pack-index (`manifest.midx`).
    #[serde(default)]
    pub midx_url: Option<String>,
    /// Signed URL for the concatenated idx bundle (`manifest.idx_bundle`).
    #[serde(default)]
    pub idx_bundle_url: Option<String>,
    pub result: ExactResultKind,
    /// The hosted server's per-clone id, captured from the `X-Ripclone-Clone-Id`
    /// response header (not part of the JSON body). `None` when the server does
    /// not mint one; in that case the post-clone metrics report is skipped.
    #[serde(skip)]
    pub clone_id: Option<String>,
    /// True when resolving this ref required a 202/poll (a cold build) rather
    /// than hitting an already-warm repo. Captured from the resolve loop, not the
    /// JSON body.
    #[serde(skip)]
    pub cold: bool,
}

/// Immutable identity used when one streamed artifact needs a fresh signed
/// URL. This is operation-local and contains no durable clone state: every
/// refresh asks the existing ref endpoint for the exact target already pinned
/// by the initial response.
#[derive(Clone)]
struct PinnedArtifactRefresh {
    repo_path: String,
    request_branch: String,
    checkout_branch: String,
    rev: Option<String>,
    result: ExactResultKind,
    target: String,
    artifact: String,
    clonepack_manifest: String,
    metadata_chunk: String,
    urls: tokio::sync::watch::Sender<ArtifactUrlSnapshot>,
    refresh_gate: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone)]
struct ArtifactUrlSnapshot {
    generation: u64,
    response: Arc<RefResponse>,
}

struct ArtifactDownloadUrl {
    generation: u64,
    signed_url: Option<String>,
}

#[derive(Clone, Copy)]
enum ArtifactUrlKind {
    Manifest,
    Metadata,
    ArchiveChunk(usize),
    PackChunk(usize),
    Midx,
    IdxBundle,
}

impl ArtifactUrlKind {
    fn select(self, info: &RefResponse) -> Option<String> {
        match self {
            Self::Manifest => info.clonepack_manifest_url.clone(),
            Self::Metadata => info.metadata_chunk_url.clone(),
            Self::ArchiveChunk(index) => info
                .archive_chunk_urls
                .as_ref()
                .and_then(|urls| urls.get(index))
                .cloned()
                .flatten(),
            Self::PackChunk(index) => info
                .pack_chunk_urls
                .as_ref()
                .and_then(|urls| urls.get(index))
                .cloned()
                .flatten(),
            Self::Midx => info.midx_url.clone(),
            Self::IdxBundle => info.idx_bundle_url.clone(),
        }
    }

    fn label(self) -> String {
        match self {
            Self::Manifest => "manifest".to_string(),
            Self::Metadata => "metadata".to_string(),
            Self::ArchiveChunk(index) => format!("archive chunk {index}"),
            Self::PackChunk(index) => format!("pack {index}"),
            Self::Midx => "multi-pack-index".to_string(),
            Self::IdxBundle => "idx bundle".to_string(),
        }
    }
}

impl PinnedArtifactRefresh {
    fn current_url(&self, kind: ArtifactUrlKind) -> ArtifactDownloadUrl {
        let snapshot = self.urls.borrow();
        ArtifactDownloadUrl {
            generation: snapshot.generation,
            signed_url: kind.select(&snapshot.response),
        }
    }
}

fn ref_poll_config() -> (usize, std::time::Duration) {
    const DEFAULT_ATTEMPTS: usize = 40;
    const DEFAULT_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
    if std::env::var_os("RIPCLONE_TESTING").is_none() {
        return (DEFAULT_ATTEMPTS, DEFAULT_DELAY);
    }
    let attempts = std::env::var("RIPCLONE_TEST_REF_MAX_ATTEMPTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_ATTEMPTS);
    let delay = std::env::var("RIPCLONE_TEST_REF_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
        .unwrap_or(DEFAULT_DELAY);
    (attempts, delay)
}

fn managed_git_timeout() -> Duration {
    const PRODUCTION_TIMEOUT: Duration = Duration::from_secs(300);
    if std::env::var_os("RIPCLONE_TESTING").is_none() {
        return PRODUCTION_TIMEOUT;
    }
    std::env::var("RIPCLONE_TEST_GIT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or(PRODUCTION_TIMEOUT)
}

fn local_provider_origin(provider: &ProviderInstance, repo_path: &str) -> String {
    if provider.is_github_default()
        && provider.host == "github.com"
        && let Some(base) = std::env::var("RIPCLONE_ORIGIN_BASE")
            .ok()
            .filter(|base| !base.is_empty())
    {
        format!("{}/{}.git", base.trim_end_matches('/'), repo_path)
    } else {
        provider.clone_url(repo_path)
    }
}

async fn wait_test_top_up_staging_barrier(staging: &Path) -> Result<()> {
    if std::env::var_os("RIPCLONE_TESTING").as_deref() != Some(std::ffi::OsStr::new("1")) {
        return Ok(());
    }
    let Some(dir) = std::env::var_os("RIPCLONE_TEST_TOP_UP_STAGING_BARRIER_DIR").map(PathBuf::from)
    else {
        return Ok(());
    };
    std::fs::create_dir_all(&dir).context("create top-up staging barrier directory")?;
    let entered = dir.join("entered");
    let mut entered_file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&entered)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => return Err(error).context("create top-up staging barrier marker"),
    };
    use std::io::Write;
    writeln!(entered_file, "{}", staging.display()).context("record first top-up staging path")?;
    let proceed = dir.join("proceed");
    for _ in 0..1_000 {
        if proceed.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("top-up staging barrier was not released within 10 seconds")
}

/// Test-only synchronization point after the first ordinary pending response
/// has established B, but before the client makes its pinned top-up request.
/// This lets the direct proof advance the upstream branch only after pinning.
async fn wait_test_top_up_pin_barrier() -> Result<()> {
    if std::env::var_os("RIPCLONE_TESTING").as_deref() != Some(std::ffi::OsStr::new("1")) {
        return Ok(());
    }
    let Some(dir) = std::env::var_os("RIPCLONE_TEST_TOP_UP_PIN_BARRIER_DIR").map(PathBuf::from)
    else {
        return Ok(());
    };
    std::fs::create_dir_all(&dir).context("create top-up pin barrier directory")?;
    let entered = dir.join("entered");
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&entered)
    {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => return Err(error).context("create top-up pin barrier marker"),
    }
    let proceed = dir.join("proceed");
    for _ in 0..1_000 {
        if proceed.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!("top-up pin barrier was not released within 10 seconds")
}

fn record_test_managed_git(command: &str, elapsed: Duration) -> Result<()> {
    if std::env::var_os("RIPCLONE_TESTING").as_deref() != Some(std::ffi::OsStr::new("1")) {
        return Ok(());
    }
    let Some(log) = std::env::var_os("RIPCLONE_TEST_TOP_UP_GIT_LOG") else {
        return Ok(());
    };
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .context("open managed top-up Git timing log")?;
    writeln!(
        file,
        "command={command}\tduration_us={}",
        elapsed.as_micros()
    )
    .context("record managed top-up Git timing")
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

/// Sleep between 202/503 sync polls. Production default is 2s. E2e suites may
/// set `RIPCLONE_TEST_SYNC_POLL_MS` (e.g. 100) so a build that outlives the
/// server's wait window is re-attached quickly without changing prod traffic.
fn test_sync_poll_interval() -> std::time::Duration {
    let ms = std::env::var("RIPCLONE_TEST_SYNC_POLL_MS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(2_000u64)
        .max(1);
    std::time::Duration::from_millis(ms)
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
                .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
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

/// Why one artifact download attempt failed, and what the caller may do next.
///
/// This is the single classification both artifact outputs share: the buffered
/// byte download and the streamed temporary-file download run the same status,
/// retry, verification, and credential rules and differ only in where the bytes
/// land.
#[derive(Debug)]
enum FetchFailure {
    /// Transport error, 408, 429, or 5xx — retry the same URL.
    Retry(anyhow::Error),
    /// 401 or 403 from a signed object URL — the streamed downloader refreshes
    /// this artifact's URL for the same pinned commit.
    RefreshUrl(anyhow::Error),
    /// 404, wrong length, wrong hash, or a local I/O failure — fail now.
    Permanent(anyhow::Error),
}

impl FetchFailure {
    fn retryable(&self) -> bool {
        matches!(self, FetchFailure::Retry(_))
    }

    /// Surface the underlying failure to the caller. The streamed downloader
    /// handles `RefreshUrl` before conversion; buffered artifacts fail without
    /// an outer clone retry.
    fn into_error(self) -> anyhow::Error {
        match self {
            FetchFailure::Retry(err)
            | FetchFailure::RefreshUrl(err)
            | FetchFailure::Permanent(err) => err,
        }
    }
}

/// The status rule. `signed` is true for a self-authenticating object-storage
/// URL, where 401/403 means the signature expired or was revoked rather than
/// that the caller is unauthenticated.
fn classify_fetch_status(
    status: reqwest::StatusCode,
    signed: bool,
    hash: &str,
) -> Option<FetchFailure> {
    if status.is_success() {
        return None;
    }
    let err = anyhow::anyhow!("artifact {hash} fetch failed: {status}");
    Some(
        if status.is_server_error()
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::REQUEST_TIMEOUT
        {
            FetchFailure::Retry(err)
        } else if signed
            && (status == reqwest::StatusCode::UNAUTHORIZED
                || status == reqwest::StatusCode::FORBIDDEN)
        {
            FetchFailure::RefreshUrl(err)
        } else {
            FetchFailure::Permanent(err)
        },
    )
}

/// The verification rule. An artifact must have the length the manifest
/// promised (when it promised one) and must hash to its content address. Both
/// are deterministic corruption — refetching returns the same bytes — so they
/// are permanent failures, never a retry and never a URL refresh. Genuine
/// truncation surfaces as a transport/body-read error, which *is* retried.
fn verify_fetched_artifact(
    hash: &str,
    expected_len: Option<u64>,
    actual_len: u64,
    actual_hash: &str,
) -> std::result::Result<(), FetchFailure> {
    if let Some(expected) = expected_len
        && actual_len != expected
    {
        return Err(FetchFailure::Permanent(anyhow::anyhow!(
            "artifact {hash} size mismatch: expected {expected}, got {actual_len}"
        )));
    }
    if actual_hash != hash {
        return Err(FetchFailure::Permanent(anyhow::anyhow!(
            "artifact hash mismatch: expected {hash}, got {actual_hash}"
        )));
    }
    Ok(())
}

/// The retry rule: run `attempt_once` until it succeeds, fails permanently, or
/// exhausts the bounded attempt budget, sleeping the existing jittered backoff
/// between transient failures.
async fn with_fetch_retry<T, F, Fut>(hash: &str, label: &str, mut attempt_once: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, FetchFailure>>,
{
    let (max_attempts, base_backoff_ms) = fetch_retry_config();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match attempt_once().await {
            Ok(value) => return Ok(value),
            Err(failure) => {
                if failure.retryable() && attempt < max_attempts {
                    let backoff = fetch_backoff(base_backoff_ms, attempt);
                    tracing::debug!(
                        "artifact {hash} {label} attempt {attempt}/{max_attempts} failed; retrying in {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                return Err(failure.into_error());
            }
        }
    }
}

fn spawn_downloads_to_bounded_channel<J, O, F, Fut>(
    jobs: Vec<J>,
    concurrency: usize,
    channel_depth: usize,
    download: F,
) -> (
    tokio::task::JoinHandle<Result<Duration>>,
    tokio::sync::mpsc::Receiver<Result<O>>,
)
where
    J: Send + 'static,
    O: Send + 'static,
    F: Fn(J) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    use futures::stream::{self, StreamExt, TryStreamExt};

    let (tx, rx) = tokio::sync::mpsc::channel(channel_depth.max(1));
    let download = Arc::new(download);
    let task = tokio::spawn(async move {
        let started = Instant::now();
        stream::iter(jobs)
            .map(|job| {
                let tx = tx.clone();
                let download = Arc::clone(&download);
                async move {
                    let result = download(job).await;
                    let failed = result.is_err();
                    tx.send(result)
                        .await
                        .map_err(|_| anyhow::anyhow!("download stage receiver dropped"))?;
                    if failed {
                        anyhow::bail!("download stage worker failed");
                    }
                    Ok::<(), anyhow::Error>(())
                }
            })
            .buffer_unordered(concurrency.max(1))
            .try_collect::<Vec<()>>()
            .await?;
        Ok(started.elapsed())
    });
    (task, rx)
}

/// Download an artifact into memory. Used for artifacts small enough to hold
/// whole: manifests, metadata, pack indexes, archive chunks, and HEAD packs.
async fn fetch_artifact_bytes(
    client: &reqwest::Client,
    url: &str,
    hash: &str,
    expected_len: Option<u64>,
    signed: bool,
) -> Result<bytes::Bytes> {
    with_fetch_retry(hash, "fetch", || {
        fetch_artifact_bytes_once(client, url, hash, expected_len, signed)
    })
    .await
}

async fn fetch_artifact_bytes_once(
    client: &reqwest::Client,
    url: &str,
    hash: &str,
    expected_len: Option<u64>,
    signed: bool,
) -> std::result::Result<bytes::Bytes, FetchFailure> {
    wait_for_test_before_buffered_artifact(hash)
        .await
        .map_err(FetchFailure::Permanent)?;
    record_test_download_request(hash, 0, signed, url);
    let resp = client.get(url).send().await.map_err(|e| {
        // Transport errors (connect/reset/timeout) are transient.
        FetchFailure::Retry(anyhow::anyhow!("artifact fetch transport error: {e}"))
    })?;
    if let Some(failure) = classify_fetch_status(resp.status(), signed, hash) {
        return Err(failure);
    }
    // R1: keep the body as `Bytes` (a refcounted buffer) instead of copying
    // it into a fresh Vec — it flows through the cache and on to the consumer
    // without a second per-artifact copy. A retry of this buffered path starts
    // only this artifact again from byte zero.
    let data = resp
        .bytes()
        .await
        .map_err(|e| FetchFailure::Retry(anyhow::anyhow!("artifact body read error: {e}")))?;
    let hash_bytes = data.clone();
    let actual_hash = tokio::task::spawn_blocking(move || crate::cas::hash(&hash_bytes))
        .await
        .map_err(|error| {
            FetchFailure::Permanent(anyhow::anyhow!(
                "artifact hash worker failed before verification: {error}"
            ))
        })?;
    verify_fetched_artifact(hash, expected_len, data.len() as u64, &actual_hash)?;
    Ok(data)
}

fn validate_content_range(
    value: Option<&reqwest::header::HeaderValue>,
    offset: u64,
    expected_len: u64,
) -> Result<()> {
    let value = value
        .context("resumed artifact response omitted Content-Range")?
        .to_str()
        .context("resumed artifact response has non-text Content-Range")?;
    let value = value
        .strip_prefix("bytes ")
        .context("resumed artifact response has invalid Content-Range unit")?;
    let (bounds, total) = value
        .split_once('/')
        .context("resumed artifact response has malformed Content-Range")?;
    let (start, end) = bounds
        .split_once('-')
        .context("resumed artifact response has malformed Content-Range bounds")?;
    let start = start
        .parse::<u64>()
        .context("resumed artifact Content-Range start is not a number")?;
    let end = end
        .parse::<u64>()
        .context("resumed artifact Content-Range end is not a number")?;
    let total = total
        .parse::<u64>()
        .context("resumed artifact Content-Range total is not a number")?;
    anyhow::ensure!(
        start == offset && end == expected_len.saturating_sub(1) && total == expected_len,
        "invalid Content-Range for resumed artifact: expected bytes {offset}-{}/{expected_len}, got {value}",
        expected_len.saturating_sub(1)
    );
    Ok(())
}

fn record_test_download_request(hash: &str, offset: u64, signed: bool, url: &str) {
    if std::env::var_os("RIPCLONE_TESTING").as_deref() != Some(std::ffi::OsStr::new("1")) {
        return;
    }
    let Some(path) = std::env::var_os("RIPCLONE_TEST_DOWNLOAD_AUDIT").map(PathBuf::from) else {
        return;
    };
    let host = url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| "invalid".to_string());
    let Ok(_guard) = TEST_DOWNLOAD_AUDIT_LOCK.lock() else {
        return;
    };
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        use std::io::Write;
        let _ = writeln!(
            file,
            "hash={hash} offset={offset} signed={signed} ripclone_authorization={} host={host}",
            !signed,
        );
    }
}

/// Test-fixture-only deterministic client interruption. The real response is
/// cancelled after bytes have been persisted, then the fixture controls when
/// the retry may proceed (for example, after a MinIO URL really expires).
async fn wait_for_test_download_interrupt(hash: &str, saved: u64) -> Result<bool> {
    if std::env::var_os("RIPCLONE_TESTING").is_none()
        || std::env::var("RIPCLONE_TEST_INTERRUPT_ARTIFACT")
            .ok()
            .as_deref()
            != Some(hash)
    {
        return Ok(false);
    }
    let after = std::env::var("RIPCLONE_TEST_INTERRUPT_AFTER_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    if saved < after {
        return Ok(false);
    }
    let Some(dir) = std::env::var_os("RIPCLONE_TEST_INTERRUPT_DIR").map(PathBuf::from) else {
        return Ok(false);
    };
    std::fs::create_dir_all(&dir).context("create download interruption fixture directory")?;
    let entered = dir.join("entered");
    if entered.exists() {
        return Ok(false);
    }
    std::fs::write(&entered, saved.to_string())
        .context("record interrupted download byte count")?;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !dir.join("proceed").exists() {
        if Instant::now() >= deadline {
            anyhow::bail!("download interruption fixture exceeded 120 seconds");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(true)
}

/// Test-fixture-only pause before a buffered artifact's first real request.
/// This lets the MinIO acceptance row expire the signed URL without inserting
/// a proxy or changing production retry timing.
async fn wait_for_test_before_buffered_artifact(hash: &str) -> Result<()> {
    if std::env::var_os("RIPCLONE_TESTING").as_deref() != Some(std::ffi::OsStr::new("1"))
        || std::env::var("RIPCLONE_TEST_PAUSE_BUFFERED_ARTIFACT")
            .ok()
            .as_deref()
            != Some(hash)
    {
        return Ok(());
    }
    let Some(dir) = std::env::var_os("RIPCLONE_TEST_PAUSE_BUFFERED_DIR").map(PathBuf::from) else {
        return Ok(());
    };
    std::fs::create_dir_all(&dir).context("create buffered artifact pause directory")?;
    let entered = dir.join("entered");
    if entered.exists() {
        return Ok(());
    }
    std::fs::write(&entered, hash).context("record paused buffered artifact")?;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !dir.join("proceed").exists() {
        if Instant::now() >= deadline {
            anyhow::bail!("buffered artifact pause exceeded 120 seconds");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

/// Download one large history pack into one temporary file. Connection retries
/// append from the verified saved byte count with an open-ended Range request;
/// a server that ignores Range restarts this artifact (and only this artifact)
/// in the same file. The caller renames the file only after length and SHA-256
/// verification succeed.
async fn fetch_artifact_to_temp(
    client: &Client,
    hash: &str,
    expected_len: u64,
    dir: &Path,
    refresh: &PinnedArtifactRefresh,
    pack_index: usize,
) -> Result<(tempfile::NamedTempFile, u64)> {
    use futures::StreamExt;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    let tmp = tempfile::Builder::new()
        .suffix(".ripclone-download")
        .tempfile_in(dir)
        .context("create artifact temp file")?;
    let std_file = tmp
        .as_file()
        .try_clone()
        .context("clone artifact temp file")?;
    let mut file = tokio::fs::File::from_std(std_file);
    let mut hasher = sha2::Sha256::new();
    let mut saved = 0u64;
    let kind = ArtifactUrlKind::PackChunk(pack_index);
    let mut artifact_url = refresh.current_url(kind);
    let (max_attempts, base_backoff_ms) = fetch_retry_config();

    for attempt in 1..=max_attempts {
        if saved == expected_len {
            let actual_hash = hex::encode(hasher.clone().finalize());
            verify_fetched_artifact(hash, Some(expected_len), saved, &actual_hash)
                .map_err(FetchFailure::into_error)?;
            file.flush().await.context("flush artifact temp file")?;
            drop(file);
            return Ok((tmp, saved));
        }
        if saved > expected_len {
            anyhow::bail!(
                "artifact {hash} size mismatch: expected {expected_len}, got at least {saved}"
            );
        }

        let gateway_url = format!("{}/v1/artifacts/{hash}", client.server);
        let (http, url, signed) =
            client.artifact_endpoint(artifact_url.signed_url.as_deref(), &gateway_url);
        record_test_download_request(hash, saved, signed, url);
        let mut request = http.get(url);
        if saved > 0 {
            request = request.header(reqwest::header::RANGE, format!("bytes={saved}-"));
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                if attempt < max_attempts {
                    tokio::time::sleep(fetch_backoff(base_backoff_ms, attempt)).await;
                    continue;
                }
                return Err(anyhow::anyhow!("artifact fetch transport error: {error}"));
            }
        };

        if let Some(failure) = classify_fetch_status(response.status(), signed, hash) {
            match failure {
                FetchFailure::RefreshUrl(_) if attempt < max_attempts => {
                    artifact_url = client
                        .refresh_pinned_artifact_url(refresh, kind, artifact_url.generation)
                        .await
                        .with_context(|| format!("refresh URL for interrupted artifact {hash}"))?;
                    continue;
                }
                FetchFailure::Retry(_) if attempt < max_attempts => {
                    tokio::time::sleep(fetch_backoff(base_backoff_ms, attempt)).await;
                    continue;
                }
                failure => return Err(failure.into_error()),
            }
        }

        if saved == 0 {
            anyhow::ensure!(
                response.status() == reqwest::StatusCode::OK,
                "artifact {hash} initial response had unexpected status {}",
                response.status()
            );
        } else if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            validate_content_range(
                response.headers().get(reqwest::header::CONTENT_RANGE),
                saved,
                expected_len,
            )?;
            if let Some(content_len) = response.content_length() {
                anyhow::ensure!(
                    content_len == expected_len - saved,
                    "resumed artifact Content-Length mismatch: expected {}, got {content_len}",
                    expected_len - saved
                );
            }
        } else if response.status() == reqwest::StatusCode::OK {
            // Range unsupported: restart only this artifact in its existing
            // private temporary file.
            file.set_len(0)
                .await
                .context("truncate artifact after ignored Range")?;
            file.seek(std::io::SeekFrom::Start(0))
                .await
                .context("seek restarted artifact temp file")?;
            hasher = sha2::Sha256::new();
            saved = 0;
        } else {
            anyhow::bail!(
                "artifact {hash} resume response had unexpected status {}",
                response.status()
            );
        }

        let mut stream = response.bytes_stream();
        let mut body_error = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    file.write_all(&chunk)
                        .await
                        .context("write artifact temp file")?;
                    hasher.update(&chunk);
                    saved = saved
                        .checked_add(u64::try_from(chunk.len()).context("chunk length overflow")?)
                        .context("artifact byte count overflow")?;
                    if saved > expected_len {
                        anyhow::bail!(
                            "artifact {hash} size mismatch: expected {expected_len}, got at least {saved}"
                        );
                    }
                    if wait_for_test_download_interrupt(hash, saved).await? {
                        body_error = Some(anyhow::anyhow!(
                            "deterministic client-side artifact interruption after {saved} bytes"
                        ));
                        break;
                    }
                }
                Err(error) => {
                    body_error = Some(anyhow::anyhow!("artifact body read error: {error}"));
                    break;
                }
            }
        }
        if let Some(error) = body_error {
            if attempt < max_attempts {
                tokio::time::sleep(fetch_backoff(base_backoff_ms, attempt)).await;
                continue;
            }
            return Err(error);
        }

        let actual_hash = hex::encode(hasher.clone().finalize());
        verify_fetched_artifact(hash, Some(expected_len), saved, &actual_hash)
            .map_err(FetchFailure::into_error)?;
        file.flush().await.context("flush artifact temp file")?;
        drop(file);
        return Ok((tmp, saved));
    }
    unreachable!("positive fetch attempt count")
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

const INSTALL_STAGING_MARKER: &str = ".ripclone-install-staging";

#[cfg(unix)]
fn lock_staging_marker(marker: &std::fs::File, nonblocking: bool) -> Result<()> {
    use std::os::fd::AsRawFd;

    let mut operation = libc::LOCK_EX;
    if nonblocking {
        operation |= libc::LOCK_NB;
    }
    // SAFETY: `marker` owns this valid descriptor for the whole call. `flock`
    // changes only the advisory lock associated with that open file.
    if unsafe { libc::flock(marker.as_raw_fd(), operation) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("lock clone staging marker")
    }
}

struct StagedTempDir {
    dir: tempfile::TempDir,
    #[cfg(unix)]
    _marker_lock: std::fs::File,
}

impl StagedTempDir {
    fn path(&self) -> &Path {
        self.dir.path()
    }
}

#[cfg(unix)]
fn cleanup_stale_install_dirs(target: &Path, parent: &Path) {
    use std::io::Read;

    let target_name = target
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("ripclone"))
        .to_string_lossy();
    let prefix = format!("{target_name}.ripclone-");
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(rest) = name.strip_prefix(&prefix) else {
            continue;
        };
        let Some(rest) = rest.strip_suffix(".tmp") else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        let path = entry.path();
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let Ok(mut marker) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(INSTALL_STAGING_MARKER))
        else {
            continue;
        };
        // A live clone holds this lock even across PID namespaces. Failure to
        // acquire it is therefore a preserve decision, including unexpected
        // filesystem/locking errors.
        if lock_staging_marker(&marker, true).is_err() {
            continue;
        }
        let mut contents = String::new();
        if marker.read_to_string(&mut contents).is_err() || contents != "ripclone\n" {
            continue;
        }
        if let Err(error) = std::fs::remove_dir_all(&path) {
            warn!(
                "cannot remove stale clone staging directory {}: {error}",
                path.display()
            );
        } else {
            info!("removed stale clone staging directory {}", path.display());
        }
    }
}

#[cfg(not(unix))]
fn cleanup_stale_install_dirs(_target: &Path, _parent: &Path) {}

/// Create a temp install directory next to `target`. Returns its owning handle
/// (not a bare path): the caller must keep it alive so that on *any* failure
/// before the final rename, the partial install is removed on drop and other
/// processes can see that its marker remains locked. On success the dir is
/// renamed onto `target`, after which the handle's drop is a harmless no-op
/// (the path no longer exists).
fn temp_install_dir(target: &Path) -> Result<StagedTempDir> {
    let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    cleanup_stale_install_dirs(target, parent);
    let target_name = target
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("ripclone"))
        .to_string_lossy();
    let tmp = tempfile::Builder::new()
        .prefix(&format!("{target_name}.ripclone-"))
        .suffix(".tmp")
        .tempdir_in(parent)
        .context("create temp install directory")?;
    let marker_path = tmp.path().join(INSTALL_STAGING_MARKER);
    #[cfg(unix)]
    {
        use std::io::Write;

        let mut marker_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&marker_path)
            .context("create clone staging marker")?;
        lock_staging_marker(&marker_lock, false)?;
        marker_lock
            .write_all(b"ripclone\n")
            .context("mark temp install directory")?;
        Ok(StagedTempDir {
            dir: tmp,
            _marker_lock: marker_lock,
        })
    }
    #[cfg(not(unix))]
    {
        std::fs::write(marker_path, b"ripclone\n").context("mark temp install directory")?;
        Ok(StagedTempDir { dir: tmp })
    }
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

/// Resolve the directory to fsync after the atomic rename publishes `target`,
/// so the rename entry itself is durable. A bare relative target directory (the
/// README quickstart uses `bun`) has an empty parent; fall back to the current
/// directory — the actual container — exactly as `temp_install_dir` does, so
/// the post-rename fsync is never silently skipped.
fn post_rename_fsync_dir(target: &Path) -> &Path {
    target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
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

/// Walk `root` and collect every regular file into `files` and every directory
/// into `dirs`. A symlink is persisted by syncing its parent directory, not by
/// following the link, so it needs no entry of its own. The `.git/index` written
/// during extraction is a regular file under `root`, so this picks it up — the
/// index stat cache is flushed alongside the working-tree files it describes.
fn collect_fsync_targets(
    root: &Path,
    files: &mut Vec<PathBuf>,
    dirs: &mut Vec<PathBuf>,
) -> Result<()> {
    let meta = std::fs::symlink_metadata(root)
        .with_context(|| format!("stat for fsync {}", root.display()))?;
    if meta.is_dir() {
        for entry in
            std::fs::read_dir(root).with_context(|| format!("read dir {}", root.display()))?
        {
            collect_fsync_targets(&entry?.path(), files, dirs)?;
        }
        dirs.push(root.to_path_buf());
    } else if meta.is_file() {
        files.push(root.to_path_buf());
    }
    Ok(())
}

/// Flush the whole materialized tree under `root` — every file (including the
/// `.git/index` stat cache), every directory that holds one — before the clone
/// reports success, so a crash cannot leave a torn tree that `git status` would
/// call clean. Batches `IORING_OP_FSYNC` on the io_uring writer path and falls
/// back to sequential `fsync` on the POSIX path; both are gated on
/// `RIPCLONE_FSYNC` (off by default, D6).
fn fsync_tree(root: &Path) -> Result<()> {
    let mut files = Vec::new();
    let mut dirs = Vec::new();
    collect_fsync_targets(root, &mut files, &mut dirs)?;
    crate::worktree_writer::fsync_paths_durable(&files, &dirs)
}

/// Pause one real pack-install worker after its pack and index have been
/// written into attempt staging. This test-only barrier proves cancellation
/// drains already-running blocking work before removing private staging.
fn wait_for_test_pack_worker(pack_index: usize) -> Result<()> {
    if std::env::var_os("RIPCLONE_TESTING").as_deref() != Some(std::ffi::OsStr::new("1")) {
        return Ok(());
    }
    let Some(selected) = std::env::var_os("RIPCLONE_TEST_PACK_WORKER_INDEX") else {
        return Ok(());
    };
    if selected.to_string_lossy().parse::<usize>().ok() != Some(pack_index) {
        return Ok(());
    }
    let (Some(entered), Some(proceed)) = (
        std::env::var_os("RIPCLONE_TEST_PACK_WORKER_ENTERED"),
        std::env::var_os("RIPCLONE_TEST_PACK_WORKER_PROCEED"),
    ) else {
        return Ok(());
    };
    let entered = PathBuf::from(entered);
    let proceed = PathBuf::from(proceed);
    let mut entered_file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&entered)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => return Err(error).context("create pack-worker test marker"),
    };
    use std::io::Write;
    entered_file
        .write_all(b"active")
        .context("mark pack worker active")?;
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while !proceed.exists() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("pack-worker test barrier exceeded 120 seconds");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::write(entered, b"released").context("mark pack worker released")
}

/// What a finished clone learned, for the best-effort post-clone metrics report.
/// Carries the hosted server's per-clone id (when one was minted), the resolved
/// repo/commit, and the bytes/timing the client measured. The end-to-end wall
/// clock is supplied by the caller (the CLI), which owns the outer timer.
#[derive(Debug, Clone)]
pub struct CloneOutcome {
    pub provider: String,
    pub owner: String,
    pub name: String,
    pub commit: String,
    /// `head` | `full` | `files`.
    pub mode: &'static str,
    /// True when the resolve had to poll a cold build (202) before succeeding.
    pub cold: bool,
    /// The cloud's `X-Ripclone-Clone-Id`. `None` means no metrics report.
    pub clone_id: Option<String>,
    /// Total bytes downloaded (metadata + pack/archive chunks).
    pub bytes: u64,
}

#[derive(Debug)]
enum InstallPlan {
    Exact {
        target: String,
        artifact: String,
        response: RefResponse,
    },
    TopUp {
        target: String,
        artifact: String,
        base_response: RefResponse,
    },
}

impl InstallPlan {
    fn into_parts(self) -> (String, String, RefResponse, bool) {
        match self {
            Self::Exact {
                target,
                artifact,
                response,
            } => (target, artifact, response, false),
            Self::TopUp {
                target,
                artifact,
                base_response,
            } => (target, artifact, base_response, true),
        }
    }
}

#[derive(Default)]
struct InstallIdentity {
    pinned: Option<String>,
    resolved_branch: Option<String>,
    clone_id: Option<String>,
    cold: bool,
}

type CleanupFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Default)]
struct AttemptCleanupInner {
    pending: Mutex<Vec<CleanupFuture>>,
    active_guards: AtomicUsize,
    closed: AtomicBool,
    changed: tokio::sync::Notify,
}

#[derive(Clone, Default)]
struct AttemptCleanup(Arc<AttemptCleanupInner>);

#[derive(Default)]
struct AttemptStaging {
    overlay_dirs: Option<overlay::OverlayDirs>,
    temp_install: Option<StagedTempDir>,
}

type SharedAttemptStaging = Arc<Mutex<Option<AttemptStaging>>>;

// This cleanup ownership deliberately covers arbitrary cancellation of the
// public install future, not only the ordinary stale-URL retry path. Tokio
// cannot stop spawn_blocking work that has begun, so detached cancellation
// must retain the staging tree until every registered writer has exited.
fn spawn_attempt_reaper(
    cleanup: AttemptCleanup,
    staging: SharedAttemptStaging,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        cleanup.drain_closed().await;
        let staging = staging.lock().unwrap_or_else(|e| e.into_inner()).take();
        drop(staging);
    })
}

impl AttemptCleanup {
    fn guard_started(&self) {
        self.0.active_guards.fetch_add(1, Ordering::SeqCst);
    }

    fn guard_finished(&self) {
        let previous = self.0.active_guards.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0, "attempt cleanup guard underflow");
        self.0.changed.notify_one();
    }

    fn push(&self, future: CleanupFuture) {
        self.0
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(future);
        self.0.changed.notify_one();
    }

    fn close(&self) {
        self.0.closed.store(true, Ordering::SeqCst);
        self.0.changed.notify_one();
    }

    async fn drain(&self) {
        loop {
            let pending = {
                let mut pending = self.0.pending.lock().unwrap_or_else(|e| e.into_inner());
                std::mem::take(&mut *pending)
            };
            if pending.is_empty() {
                break;
            }
            // Awaiting one cancelled parent can drop child guards that append
            // more handles, so drain to a fixed point before retrying or
            // removing staging.
            futures::future::join_all(pending).await;
        }
    }

    async fn drain_closed(&self) {
        loop {
            let changed = self.0.changed.notified();
            self.drain().await;
            if self.0.closed.load(Ordering::SeqCst)
                && self.0.active_guards.load(Ordering::SeqCst) == 0
            {
                // No guard can enqueue after the attempt is closed and all
                // registered guards have finished. One last drain closes the
                // race with a guard that enqueued immediately before decrement.
                self.drain().await;
                return;
            }
            changed.await;
        }
    }
}

struct CloseAttemptOnDrop(AttemptCleanup);

impl Drop for CloseAttemptOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

struct AbortOnDrop<T: Send + 'static> {
    handle: Option<tokio::task::JoinHandle<T>>,
    cleanup: AttemptCleanup,
}

struct ManagedGitChild {
    child: Option<tokio::process::Child>,
    process_group: Option<i32>,
    cleanup: AttemptCleanup,
}

impl ManagedGitChild {
    fn new(child: tokio::process::Child, cleanup: AttemptCleanup) -> Self {
        Self {
            process_group: child.id().and_then(|pid| i32::try_from(pid).ok()),
            child: Some(child),
            cleanup,
        }
    }

    fn terminate(&mut self) {
        #[cfg(unix)]
        if let Some(group) = self.process_group {
            // The child starts a fresh process group, so this also kills an
            // child transport or hook-like descendant before staging drops.
            // SAFETY: `group` is the positive pid of a child placed in its own
            // process group below; negating it targets that group. `kill` does
            // not dereference any Rust memory.
            unsafe {
                libc::kill(-group, libc::SIGKILL);
            }
        }
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

impl Drop for ManagedGitChild {
    fn drop(&mut self) {
        if self.child.is_some() {
            self.terminate();
        }
        if let Some(mut child) = self.child.take() {
            self.cleanup.push(Box::pin(async move {
                let _ = child.wait().await;
            }));
        }
    }
}

impl<T: Send + 'static> AbortOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>, cleanup: AttemptCleanup) -> Self {
        cleanup.guard_started();
        Self {
            handle: Some(handle),
            cleanup,
        }
    }

    async fn join(mut self) -> std::result::Result<T, tokio::task::JoinError> {
        let result = self.handle.as_mut().expect("join handle present").await;
        self.handle.take();
        self.cleanup.guard_finished();
        result
    }
}

impl<T: Send + 'static> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            self.cleanup.push(Box::pin(async move {
                // `abort` cancels async tasks immediately. A blocking task
                // already running cannot be cancelled, so awaiting its
                // handle is what prevents a stale-URL retry from overlapping
                // the prior attempt's archive or pack worker.
                let _ = handle.await;
            }));
            self.cleanup.guard_finished();
        }
    }
}

#[derive(Clone)]
pub struct Client {
    server: String,
    /// Client that sends the ripclone auth token on every request.
    http: reqwest::Client,
    /// Client with no default auth headers, used for presigned URLs.
    raw_http: reqwest::Client,
    cache: Option<Cas>,
    /// Upstream git provider instance id (e.g. "github", "gitlab").
    provider: String,
    /// Locally configured provider boundary used for a pending Full top-up.
    /// A non-default provider id alone is insufficient: its host/template must
    /// come from the client's own registry, never from the server response.
    provider_instance: Option<ProviderInstance>,
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
        // Declare the sole wire protocol so an incompatible client/server pair
        // fails at the boundary with an actionable error.
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
            cache,
            provider: "github".to_string(),
            provider_instance: Some(ProviderInstance {
                id: ProviderInstanceId::new("github"),
                kind: ProviderKind::GitHub,
                host: "github.com".to_string(),
                auth_template: None,
                auth_header_name: None,
            }),
            upstream_token: None,
            skip_metrics: false,
        }
    }

    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        if self.provider != "github" {
            self.provider_instance = None;
        }
        self
    }

    pub fn with_provider_instance(mut self, provider: ProviderInstance) -> Self {
        self.provider = provider.id.as_str().to_string();
        self.provider_instance = Some(provider);
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

    /// Build a provider-qualified request URL for `repo_path`.
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
    /// Return the server's complete added-repository set.
    pub async fn list_repos(&self) -> Result<Vec<RepoId>> {
        let url = format!("{}/v1/repos", self.server);
        let resp = self.send(self.request(reqwest::Method::GET, &url)).await?;
        if !resp.status().is_success() {
            return Err(server_error("list failed", resp).await);
        }
        resp.json()
            .await
            .context("invalid repository list response")
    }

    /// Remove one repository registration without contacting its upstream.
    pub async fn remove_repo(&self, repo_path: &str) -> Result<()> {
        let url = self.repo_url(repo_path, "/add");
        let resp = self
            .send(self.request(reqwest::Method::DELETE, &url))
            .await?;
        if !resp.status().is_success() {
            return Err(server_error("rm failed", resp).await);
        }
        Ok(())
    }

    pub async fn resolve_ref(&self, repo_path: &str, branch: &str) -> Result<RefResponse> {
        self.resolve_exact_result(repo_path, branch, ExactResultKind::Full, None)
            .await
    }

    pub async fn resolve_exact_result(
        &self,
        repo_path: &str,
        branch: &str,
        result: ExactResultKind,
        rev: Option<&str>,
    ) -> Result<RefResponse> {
        let expected_commit = exact_commit_from_revision(rev);
        let mut pinned = None;
        let mut resolved_branch = None;
        let mut clone_id = None;
        let mut cold = false;
        match self
            .resolve_ref_for_operation(
                repo_path,
                branch,
                result,
                rev,
                expected_commit.as_deref(),
                &mut pinned,
                &mut resolved_branch,
                &result.to_string(),
                false,
                &mut clone_id,
                &mut cold,
            )
            .await?
        {
            InstallPlan::Exact { response, .. } => Ok(response),
            InstallPlan::TopUp { .. } => unreachable!("top-up was not requested"),
        }
    }

    async fn resolve_ref_for_operation(
        &self,
        repo_path: &str,
        branch: &str,
        result: ExactResultKind,
        rev: Option<&str>,
        expected_commit: Option<&str>,
        pinned: &mut Option<String>,
        resolved_branch: &mut Option<String>,
        pending_mode: &str,
        allow_top_up: bool,
        first_clone_id: &mut Option<String>,
        cold: &mut bool,
    ) -> Result<InstallPlan> {
        let (max_attempts, poll_delay) = ref_poll_config();
        let detached_checkout_allowed = branch == "HEAD"
            && rev.is_some_and(|value| crate::validation::validate_object_id(value).is_ok());
        // Track whether any attempt polled a cold build (202/503) before
        // success, so the post-clone metrics report can label the clone cold.
        let mut polled = false;
        for attempt in 0..max_attempts {
            let request_was_pinned = pinned.is_some();
            let requested_top_up = allow_top_up && pinned.is_some() && rev.is_none();
            let request_branch = if pinned.is_some() {
                resolved_branch
                    .as_deref()
                    .filter(|name| !name.is_empty())
                    .unwrap_or(branch)
            } else {
                branch
            };
            // Branches are wildcard path values, not URL syntax. Re-encode the
            // concrete branch learned from the first response before composing
            // the next request so valid delimiters such as `#` and `%` cannot
            // become a fragment or otherwise change the request target.
            let encoded_branch = urlencoding::encode(request_branch);
            let mut url = self.repo_url(repo_path, &format!("/refs/{encoded_branch}"));
            let mut q: Vec<String> = Vec::new();
            q.push(format!("result={result}"));
            if let Some(commit) = pinned.as_deref() {
                q.push(format!("pinned={commit}"));
                if requested_top_up {
                    q.push("top_up=true".to_string());
                }
            }
            // Keep the explicit historical selector on pinned readiness polls.
            // This is what distinguishes the established `sync --at` result
            // lane from an ordinary branch pin. Ordinary pins may reuse an
            // already-existing exact artifact, but never create historical work.
            if let Some(r) = rev {
                q.push(format!("rev={}", urlencoding::encode(r)));
            }
            if !q.is_empty() {
                url.push('?');
                url.push_str(&q.join("&"));
            }
            let resp = self.send(self.request(reqwest::Method::GET, &url)).await?;
            let status = resp.status();
            if status == reqwest::StatusCode::ACCEPTED {
                let response_clone_id = resp
                    .headers()
                    .get("x-ripclone-clone-id")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                if first_clone_id.is_none() {
                    *first_clone_id = response_clone_id;
                }
                let pending_branch = resp
                    .headers()
                    .get(reqwest::header::CONTENT_LOCATION)
                    .map(|value| {
                        value
                            .to_str()
                            .context("invalid Content-Location on pending ref response")
                            .and_then(|value| {
                                urlencoding::decode(value)
                                    .context("invalid escaped branch in pending Content-Location")
                                    .map(|value| value.into_owned())
                            })
                    })
                    .transpose()?;
                let pending: ArtifactPendingResponse = resp
                    .json()
                    .await
                    .with_context(|| format!("invalid pending response for {repo_path}"))?;
                if pending.code != "artifact_pending" || pending.status != "building" {
                    anyhow::bail!(
                        "invalid pending response for {repo_path}: code={:?}, status={:?}",
                        pending.code,
                        pending.status
                    );
                }
                validate_ref_response_identity(
                    pinned.as_deref().or(expected_commit),
                    resolved_branch.as_deref().or(Some(branch)),
                    detached_checkout_allowed,
                    &pending.commit,
                    &pending.branch,
                    "pending",
                    repo_path,
                )?;
                if let Some(content_location_branch) = pending_branch.as_deref()
                    && content_location_branch != pending.branch
                {
                    anyhow::bail!(
                        "ref integrity error: pending response branch {} disagrees with Content-Location {content_location_branch}",
                        pending.branch
                    );
                }
                *pinned = Some(pending.commit.clone());
                *resolved_branch = Some(pending.branch.clone());
                polled = true;
                *cold = true;
                if requested_top_up {
                    if pending.top_up_supported != Some(true) {
                        anyhow::bail!(
                            "invalid pending response for {repo_path}: pinned Full top-up support was not declared"
                        );
                    }
                    let target = pinned
                        .clone()
                        .context("pending response did not establish a pinned commit")?;
                    let Some(base_response) = pending.top_up_base else {
                        return Err(anyhow::Error::new(ArtifactPending {
                            commit: target,
                            mode: pending_mode.to_string(),
                        }));
                    };
                    crate::validation::validate_object_id(&base_response.commit)
                        .with_context(|| format!("invalid top-up base commit for {repo_path}"))?;
                    crate::validation::validate_checkout_name(&base_response.branch)
                        .with_context(|| format!("invalid top-up base branch for {repo_path}"))?;
                    if let Some(expected) = resolved_branch.as_deref()
                        && base_response.branch != expected
                    {
                        anyhow::bail!(
                            "ref integrity error: top-up base branch {} does not match resolved branch {expected}",
                            base_response.branch
                        );
                    }
                    if base_response.result != ExactResultKind::Full
                        || base_response.clonepack_manifest.is_empty()
                    {
                        anyhow::bail!(
                            "invalid top-up plan for pinned commit {target}: base is not a complete Full artifact"
                        );
                    }
                    if base_response.commit == target {
                        anyhow::bail!(
                            "invalid top-up plan for pinned commit {target}: base is not distinct"
                        );
                    }
                    return Ok(InstallPlan::TopUp {
                        target,
                        artifact: base_response.commit.clone(),
                        base_response,
                    });
                }
                if attempt == 0 {
                    eprintln!(
                        "ripclone: warming {repo_path} (queue depth {}) — this can take a moment…",
                        pending.queue_depth
                    );
                }
                // The first ordinary 202 exists to establish B. Eligible Full
                // clones immediately make the one pinned opt-in request.
                if allow_top_up && !requested_top_up {
                    // This is after the ordinary 202 has established B and
                    // before the first pinned/top_up request.
                    wait_test_top_up_pin_barrier().await?;
                    continue;
                }
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(poll_delay).await;
                    continue;
                }
                let commit = pinned
                    .clone()
                    .context("pending responses did not establish a pinned commit")?;
                return Err(anyhow::Error::new(ArtifactPending {
                    commit,
                    mode: pending_mode.to_string(),
                }));
            }
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                polled = true;
                let body = resp
                    .bytes()
                    .await
                    .with_context(|| format!("read unavailable response for {repo_path}"))?;
                let unavailable =
                    match serde_json::from_slice::<ExactRevisionUnavailableResponse>(&body) {
                        Ok(unavailable) => {
                            validate_ref_response_identity(
                                pinned.as_deref().or(expected_commit),
                                resolved_branch.as_deref().or(Some(branch)),
                                detached_checkout_allowed,
                                &unavailable.commit,
                                &unavailable.branch,
                                "503",
                                repo_path,
                            )?;
                            if pinned.is_none() {
                                *pinned = Some(unavailable.commit.clone());
                            }
                            *resolved_branch = Some(unavailable.branch.clone());
                            Some(unavailable)
                        }
                        Err(error) => {
                            return Err(error).with_context(|| {
                                format!("invalid unavailable response for {repo_path}")
                            });
                        }
                    };
                if request_was_pinned && let Some(unavailable) = unavailable.as_ref() {
                    anyhow::bail!(
                        "ref lookup for pinned commit {} failed: {}",
                        unavailable.commit,
                        unavailable.error
                    );
                }
                if attempt == 0 {
                    eprintln!("ripclone: warming {repo_path} — this can take a moment…");
                }
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(poll_delay).await;
                    continue;
                }
                if let Some(commit) = pinned.as_deref() {
                    anyhow::bail!(
                        "ref lookup for pinned commit {commit} remained unavailable after {max_attempts} attempts"
                    );
                }
                if let Some(unavailable) = unavailable {
                    anyhow::bail!(
                        "ref lookup for exact commit {} remained unavailable after {max_attempts} attempts: {}",
                        unavailable.commit,
                        unavailable.error
                    );
                }
                anyhow::bail!("{repo_path} is still building after {max_attempts} attempts");
            }
            if status == reqwest::StatusCode::OK {
                // Capture the hosted server's per-clone id from the response
                // header before the body is consumed. If absent, `clone_id`
                // remains `None`.
                let clone_id = resp
                    .headers()
                    .get("x-ripclone-clone-id")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                if first_clone_id.is_none() {
                    *first_clone_id = clone_id;
                }
                let mut info: RefResponse = resp.json().await?;
                validate_ref_response_identity(
                    pinned.as_deref().or(expected_commit),
                    resolved_branch.as_deref().or(Some(branch)),
                    detached_checkout_allowed,
                    &info.commit,
                    &info.branch,
                    "ready",
                    repo_path,
                )?;
                anyhow::ensure!(
                    info.result == result,
                    "ref result mismatch for {repo_path}: requested {result}, server returned {}",
                    info.result
                );
                *pinned = Some(info.commit.clone());
                *resolved_branch = Some(info.branch.clone());
                *cold |= polled;
                info.clone_id = first_clone_id.clone();
                info.cold = *cold;
                return Ok(InstallPlan::Exact {
                    target: pinned
                        .clone()
                        .context("ready response did not establish a pinned commit")?,
                    artifact: info.commit.clone(),
                    response: info,
                });
            }
            let authorization_failure = matches!(
                status,
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
            );
            let error = server_error("ref lookup failed", resp).await;
            if let Some(commit) = pinned.as_deref() {
                let context = if authorization_failure {
                    format!("refresh of pinned commit {commit} was not authorized")
                } else {
                    format!("refresh of pinned commit {commit} failed")
                };
                return Err(error.context(context));
            }
            return Err(error);
        }
        anyhow::bail!("ref lookup did not complete")
    }

    /// Refresh one signed artifact URL without re-running selector resolution
    /// or restarting any other artifact. The metadata read is pinned directly
    /// to the immutable artifact commit. For an ordinary plan that is the
    /// operation target; for a top-up it is base A, independently of whether
    /// Full(B) is still pending, has become ready, or has failed.
    async fn refresh_pinned_artifact_url(
        &self,
        refresh: &PinnedArtifactRefresh,
        kind: ArtifactUrlKind,
        observed_generation: u64,
    ) -> Result<ArtifactDownloadUrl> {
        // One operation owns one complete URL-set snapshot. Serialize only
        // refresh I/O; after acquiring the permit, recheck the generation so
        // concurrent artifacts reuse the leader's complete response instead
        // of each asking the server to sign the full artifact list again.
        let _permit = refresh
            .refresh_gate
            .acquire()
            .await
            .expect("operation-local URL refresh semaphore is never closed");
        let latest = refresh.current_url(kind);
        if latest.generation > observed_generation {
            anyhow::ensure!(
                latest.signed_url.is_some(),
                "pinned artifact URL refresh omitted {}",
                kind.label()
            );
            return Ok(latest);
        }

        let encoded_branch = urlencoding::encode(&refresh.request_branch);
        let mut url = self.repo_url(&refresh.repo_path, &format!("/refs/{encoded_branch}"));
        url.push_str(&format!(
            "?result={}&pinned={}",
            refresh.result, refresh.artifact
        ));
        if let Some(rev) = refresh.rev.as_deref() {
            url.push_str("&rev=");
            url.push_str(&urlencoding::encode(rev));
        }

        let response = self.send(self.request(reqwest::Method::GET, &url)).await?;
        let status = response.status();
        let info = if status == reqwest::StatusCode::OK {
            response
                .json::<RefResponse>()
                .await
                .context("decode pinned URL refresh response")?
        } else {
            let authorization_failure = matches!(
                status,
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
            );
            let error = server_error("pinned artifact URL refresh failed", response).await;
            let context = if authorization_failure {
                format!(
                    "refresh of pinned commit {} was not authorized",
                    refresh.target
                )
            } else {
                format!("refresh of pinned commit {} failed", refresh.target)
            };
            return Err(error.context(context));
        };

        validate_ref_response_identity(
            Some(&refresh.artifact),
            Some(&refresh.checkout_branch),
            refresh.checkout_branch.is_empty(),
            &info.commit,
            &info.branch,
            "artifact URL refresh",
            &refresh.repo_path,
        )?;
        let artifact_unchanged = info.result == refresh.result
            && info.clonepack_manifest == refresh.clonepack_manifest
            && info.metadata_chunk == refresh.metadata_chunk;
        anyhow::ensure!(
            artifact_unchanged,
            "pinned artifact URL refresh changed the selected artifact"
        );
        let signed_url = kind
            .select(&info)
            .with_context(|| format!("pinned artifact URL refresh omitted {}", kind.label()))?;
        let generation = latest
            .generation
            .checked_add(1)
            .context("artifact URL refresh generation overflow")?;
        refresh.urls.send_replace(ArtifactUrlSnapshot {
            generation,
            response: Arc::new(info),
        });
        Ok(ArtifactDownloadUrl {
            generation,
            signed_url: Some(signed_url),
        })
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
        self.fetch_verified_artifact(hash, signed_url, None).await
    }

    /// The credential rule: a presigned URL is self-authenticating, so it is
    /// fetched with the no-auth client and never carries the ripclone token to
    /// object storage. Without a signed URL the by-hash gateway fetch against
    /// the configured backend IS the path, and it uses the authenticated
    /// client. Returns `(client, url, signed)`.
    fn artifact_endpoint<'a>(
        &'a self,
        signed_url: Option<&'a str>,
        gateway_url: &'a str,
    ) -> (&'a reqwest::Client, &'a str, bool) {
        match signed_url {
            Some(url) => (&self.raw_http, url, true),
            None => (&self.http, gateway_url, false),
        }
    }

    /// Buffered artifact download through the shared rules, with the local
    /// complete-object cache in front of it. `expected_len` is the length the
    /// manifest promised, when the caller knows one.
    ///
    /// On a failed signed URL we do NOT fall back to a by-hash gateway fetch —
    /// the cloud no longer serves content by hash. Operation installs use the
    /// refresh-aware sibling below; standalone callers fail in place. A
    /// missing, short, or corrupt object always fails without restarting the
    /// clone.
    async fn fetch_verified_artifact(
        &self,
        hash: &str,
        signed_url: Option<&str>,
        expected_len: Option<u64>,
    ) -> Result<bytes::Bytes> {
        let gateway_url = format!("{}/v1/artifacts/{}", self.server, hash);

        if let Some(cache) = &self.cache
            && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
            && let Ok(data) = cache.get(&key)
        {
            return Ok(data.into());
        }

        let (client, url, signed) = self.artifact_endpoint(signed_url, &gateway_url);
        let data = fetch_artifact_bytes(client, url, hash, expected_len, signed).await?;

        if let Some(cache) = &self.cache
            && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
        {
            let _ = cache.put_with_hash(&key, &data);
        }

        Ok(data)
    }

    /// Fetch one buffered artifact for an already-pinned install. Transport
    /// retries and refreshed signed URLs restart only these bytes from zero;
    /// no selector is resolved again and no other artifact is repeated.
    async fn fetch_verified_artifact_for_install(
        &self,
        hash: &str,
        expected_len: Option<u64>,
        refresh: &PinnedArtifactRefresh,
        kind: ArtifactUrlKind,
    ) -> Result<bytes::Bytes> {
        let gateway_url = format!("{}/v1/artifacts/{}", self.server, hash);

        if let Some(cache) = &self.cache
            && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
            && let Ok(data) = cache.get(&key)
        {
            return Ok(data.into());
        }

        let mut artifact_url = refresh.current_url(kind);
        let (max_attempts, base_backoff_ms) = fetch_retry_config();
        for attempt in 1..=max_attempts {
            let (http, url, signed) =
                self.artifact_endpoint(artifact_url.signed_url.as_deref(), &gateway_url);
            match fetch_artifact_bytes_once(http, url, hash, expected_len, signed).await {
                Ok(data) => {
                    if let Some(cache) = &self.cache
                        && let Some(key) = self.cache_key_from_artifact_url(&gateway_url)
                    {
                        let _ = cache.put_with_hash(&key, &data);
                    }
                    return Ok(data);
                }
                Err(FetchFailure::RefreshUrl(_)) if attempt < max_attempts => {
                    artifact_url = self
                        .refresh_pinned_artifact_url(refresh, kind, artifact_url.generation)
                        .await
                        .with_context(|| {
                            format!(
                                "refresh URL for buffered artifact {hash} ({})",
                                kind.label()
                            )
                        })?;
                }
                Err(FetchFailure::Retry(_)) if attempt < max_attempts => {
                    tokio::time::sleep(fetch_backoff(base_backoff_ms, attempt)).await;
                }
                Err(failure) => return Err(failure.into_error()),
            }
        }
        unreachable!("positive fetch attempt count")
    }

    async fn fetch_validated_manifest(
        &self,
        hash: &str,
        signed_url: Option<&str>,
        pinned: &str,
    ) -> Result<ClonepackManifest> {
        // The CAS is keyed by the verified content hash, so immutable bytes may
        // be retained even when their embedded commit is wrong for this
        // operation. Identity remains a per-use check: every cached or fetched
        // manifest is decoded and compared with the operation pin here before
        // any installation work starts.
        let data = self.fetch_artifact_with_url(hash, signed_url).await?;
        let manifest =
            ClonepackManifest::decode(data.as_ref()).context("decode clonepack manifest")?;
        validate_manifest_commit(&manifest, pinned)?;
        Ok(manifest)
    }

    /// Fetch an artifact referenced by a `ChunkRef`, optionally using a signed URL.
    pub async fn fetch_chunk_ref(
        &self,
        chunk: &crate::clonepack::ChunkRef,
        signed_url: Option<&str>,
    ) -> Result<bytes::Bytes> {
        let hash = hash_to_hex(&chunk.hash);
        self.fetch_verified_artifact(&hash, signed_url, Some(chunk.len))
            .await
    }

    /// Stream an artifact referenced by a `ChunkRef` to a temporary file in
    /// `dir`, verifying it as it streams. Same rules as [`Self::fetch_chunk_ref`];
    /// only the output differs, so a large pack never lands in memory. The
    /// complete-object cache is deliberately not used here: caching a large
    /// streamed pack would add the second full-file copy this path exists to
    /// avoid.
    async fn fetch_chunk_ref_to_temp(
        &self,
        chunk: &crate::clonepack::ChunkRef,
        dir: &Path,
        refresh: &PinnedArtifactRefresh,
        pack_index: usize,
    ) -> Result<(tempfile::NamedTempFile, u64)> {
        let hash = hash_to_hex(&chunk.hash);
        fetch_artifact_to_temp(self, &hash, chunk.len, dir, refresh, pack_index).await
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
        let clonepack = self
            .fetch_validated_manifest(
                &info.clonepack_manifest,
                info.clonepack_manifest_url.as_deref(),
                &info.commit,
            )
            .await?;
        let metadata_ref = clonepack
            .metadata_chunk
            .as_ref()
            .context("clonepack manifest missing metadata chunk")?;
        let metadata_hash = hash_to_hex(&metadata_ref.hash);
        let metadata_data = self
            .fetch_artifact_with_url(&metadata_hash, info.metadata_chunk_url.as_deref())
            .await?;
        let metadata = MetadataChunk::decode_and_validate(metadata_data.as_ref())?;
        Ok((clonepack, Arc::new(metadata)))
    }

    pub async fn sync_repo(&self, repo_path: &str, depth: Option<usize>) -> Result<RefResponse> {
        let admission = self.admit_sync_repo(repo_path, depth).await?;
        if let Some(ready) = admission.ready {
            return Ok(ready);
        }
        self.wait_for_admitted_sync(repo_path, &admission).await
    }

    pub async fn add_repo(&self, repo_path: &str) -> Result<RefResponse> {
        let admission = self.admit_add_repo(repo_path).await?;
        if let Some(ready) = admission.ready {
            return Ok(ready);
        }
        self.wait_for_admitted_sync(repo_path, &admission).await
    }

    /// Admit an ordinary default-branch sync and return immediately after a
    /// ready hit or queue acceptance. This is the fast path used by the CLI.
    pub async fn admit_sync_repo(
        &self,
        repo_path: &str,
        depth: Option<usize>,
    ) -> Result<SyncAdmission> {
        self.admit_sync_request(repo_path, None, depth).await
    }

    /// Register and admit an ordinary default-branch build, returning after the
    /// durable registration plus ready detection or queue acceptance.
    pub async fn admit_add_repo(&self, repo_path: &str) -> Result<SyncAdmission> {
        let mut url = self.repo_url(repo_path, "/add");
        url.push_str("?source=cli");
        let resp = self.send(self.request(reqwest::Method::POST, &url)).await?;
        if resp.status() == reqwest::StatusCode::OK {
            let ready: RefResponse = resp.json().await?;
            return Ok(SyncAdmission {
                commit: ready.commit.clone(),
                branch: ready.branch.clone(),
                accepted: false,
                ready: Some(ready),
                status: "ready".to_string(),
                queue_depth: 0,
            });
        }
        self.parse_sync_admission_response(resp, "add").await
    }

    /// Like [`sync_repo`] but builds at `rev` (e.g. "HEAD~5" or a SHA) instead of
    /// the branch tip. The resolved commit is the result and job identity, so
    /// different revs that resolve to the same commit share a build. Useful for
    /// exercising the incremental build path deterministically without waiting for
    /// upstream to advance.
    pub async fn sync_repo_at(
        &self,
        repo_path: &str,
        rev: Option<&str>,
        depth: Option<usize>,
    ) -> Result<RefResponse> {
        if let Some(rev) = rev {
            self.sync_at_revision(repo_path, None, rev, depth).await
        } else {
            self.sync_repo(repo_path, depth).await
        }
    }

    /// Resolve a specific branch once instead of using the repo's default, then
    /// admit its exact commit. Checkout names are request-local: two names that
    /// resolve to the same commit share one durable result and job.
    pub async fn sync_branch(&self, repo_path: &str, branch: &str) -> Result<RefResponse> {
        self.sync_inner(repo_path, Some(branch), None).await
    }

    async fn admit_sync_request(
        &self,
        repo_path: &str,
        branch: Option<&str>,
        depth: Option<usize>,
    ) -> Result<SyncAdmission> {
        let mut url = self.repo_url(repo_path, "/sync");
        let mut q: Vec<String> = Vec::new();
        if let Some(branch) = branch {
            q.push(format!("branch={}", urlencoding::encode(branch)));
        }
        if let Some(depth) = depth {
            q.push(format!("depth={depth}"));
        }
        if !q.is_empty() {
            url.push('?');
            url.push_str(&q.join("&"));
        }
        let resp = self.send(self.request(reqwest::Method::POST, &url)).await?;
        if resp.status() == reqwest::StatusCode::OK {
            let ready: RefResponse = resp.json().await?;
            return Ok(SyncAdmission {
                commit: ready.commit.clone(),
                branch: ready.branch.clone(),
                accepted: false,
                ready: Some(ready),
                status: "ready".to_string(),
                queue_depth: 0,
            });
        }
        self.parse_sync_admission_response(resp, "sync").await
    }

    async fn parse_sync_admission_response(
        &self,
        resp: reqwest::Response,
        context: &str,
    ) -> Result<SyncAdmission> {
        if resp.status() != reqwest::StatusCode::ACCEPTED {
            return Err(server_error(&format!("{context} failed"), resp).await);
        }
        let accepted: SyncAcceptedResponse = resp
            .json()
            .await
            .with_context(|| format!("invalid exact {context} admission response"))?;
        let commit = accepted.commit;
        let branch = accepted.branch;
        crate::validation::validate_object_id(&commit)
            .context("server returned invalid admitted commit")?;
        if branch != "HEAD" {
            crate::validation::validate_checkout_name(&branch)
                .context("server returned invalid admitted branch")?;
        }
        Ok(SyncAdmission {
            commit,
            branch,
            accepted: true,
            ready: None,
            status: accepted.status,
            queue_depth: accepted.queue_depth,
        })
    }

    async fn wait_for_admitted_sync(
        &self,
        repo_path: &str,
        admission: &SyncAdmission,
    ) -> Result<RefResponse> {
        let mut pinned = Some(admission.commit.clone());
        // HEAD is a selector, not the concrete metadata key. Let the first
        // exact pinned GET learn the advertised default branch (for example,
        // `main`) from its response identity; concrete admissions remain pinned to
        // their admitted branch.
        let mut resolved_branch = (admission.branch != "HEAD").then(|| admission.branch.clone());
        let mut clone_id = None;
        let mut cold = false;
        match self
            .resolve_ref_for_operation(
                repo_path,
                &admission.branch,
                ExactResultKind::Full,
                None,
                None,
                &mut pinned,
                &mut resolved_branch,
                "full",
                false,
                &mut clone_id,
                &mut cold,
            )
            .await?
        {
            InstallPlan::Exact { response, .. } => Ok(response),
            InstallPlan::TopUp { .. } => unreachable!("sync readiness does not top up"),
        }
    }

    async fn sync_inner(
        &self,
        repo_path: &str,
        branch: Option<&str>,
        depth: Option<usize>,
    ) -> Result<RefResponse> {
        let admission = self.admit_sync_request(repo_path, branch, depth).await?;
        if let Some(ready) = admission.ready {
            return Ok(ready);
        }
        self.wait_for_admitted_sync(repo_path, &admission).await
    }

    async fn sync_at_revision(
        &self,
        repo_path: &str,
        branch: Option<&str>,
        rev: &str,
        depth: Option<usize>,
    ) -> Result<RefResponse> {
        let mut selected_rev = rev.to_string();
        let mut selected_branch = branch.map(str::to_string);
        let mut pin = exact_commit_from_revision(Some(rev)).map(|commit| SyncAtPin {
            commit,
            branch: branch.map(str::to_string),
        });
        // With the async build queue the server may return 202 (build still
        // running) or 503 (queue full). Each POST blocks server-side until its
        // wait window elapses, so we just retry — coalescing means a retry
        // re-attaches to the same in-flight build rather than starting a new one.
        // Test hooks (never set in production):
        //   RIPCLONE_TEST_SYNC_MAX_ATTEMPTS — bound the poll for negative-case tests
        //   RIPCLONE_TEST_SYNC_POLL_MS — shorter sleep between 202s (e2e speed)
        let max_attempts = std::env::var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(40);
        let poll = test_sync_poll_interval();
        for attempt in 0..max_attempts {
            let mut url = self.repo_url(repo_path, "/sync");
            let mut q: Vec<String> = Vec::new();
            if let Some(branch) = selected_branch.as_deref() {
                q.push(format!("branch={}", urlencoding::encode(branch)));
            }
            if let Some(depth) = depth {
                q.push(format!("depth={depth}"));
            }
            q.push(format!("rev={}", urlencoding::encode(&selected_rev)));
            url.push('?');
            url.push_str(&q.join("&"));
            let resp = self.send(self.request(reqwest::Method::POST, &url)).await?;
            let status = resp.status();
            if status == reqwest::StatusCode::OK {
                let ready: RefResponse = resp
                    .json()
                    .await
                    .context("decode exact revision ready response")?;
                observe_sync_at_identity(&mut pin, &ready.commit, &ready.branch, "200")?;
                return Ok(ready);
            }
            if status == reqwest::StatusCode::ACCEPTED {
                let pending: ArtifactPendingResponse = resp
                    .json()
                    .await
                    .context("decode exact revision pending response")?;
                if pending.code != "artifact_pending" || pending.status != "building" {
                    anyhow::bail!("invalid exact revision pending response");
                }
                observe_sync_at_identity(&mut pin, &pending.commit, &pending.branch, "202")?;
                selected_rev = pending.commit;
                selected_branch = (!pending.branch.is_empty()).then_some(pending.branch);
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(poll).await;
                    continue;
                }
                anyhow::bail!("sync still building after {max_attempts} attempts");
            }
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                let unavailable: ExactRevisionUnavailableResponse = resp
                    .json()
                    .await
                    .context("decode exact revision queue response")?;
                observe_sync_at_identity(
                    &mut pin,
                    &unavailable.commit,
                    &unavailable.branch,
                    "503",
                )?;
                selected_rev = unavailable.commit;
                selected_branch = (!unavailable.branch.is_empty()).then_some(unavailable.branch);
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(poll).await;
                    continue;
                }
                anyhow::bail!(
                    "sync unavailable after {max_attempts} attempts: {}",
                    unavailable.error
                );
            }
            return Err(server_error("sync failed", resp).await);
        }
        anyhow::bail!("sync did not complete")
    }

    /// Fast install: download prebuilt `.git` artifacts and the working-tree
    /// archive, lay everything down directly, and extract the archive. `rev`
    /// (e.g. "HEAD~5") clones the artifacts a `sync --at <rev>` built; `None`
    /// clones the branch tip.
    ///
    /// No `git init`, `index-pack`, `read-tree`, or `update-index` is run on the
    /// client. The server has already done all of that work.
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
        let mut identity = InstallIdentity::default();
        let cleanup = AttemptCleanup::default();
        self.install_repo_with_mode_at_attempt(
            repo_path,
            branch,
            rev,
            target.as_ref(),
            mode,
            clonepack,
            bench,
            &mut identity,
            &cleanup,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn install_repo_with_mode_at_attempt<P: AsRef<Path>>(
        &self,
        repo_path: &str,
        branch: &str,
        rev: Option<&str>,
        target: P,
        mode: CloneMode,
        clonepack: Option<&str>,
        bench: Option<&mut Benchmark>,
        identity: &mut InstallIdentity,
        cleanup: &AttemptCleanup,
    ) -> Result<CloneOutcome> {
        // Staging owns filesystem cleanup outside the inner attempt future.
        // Task guards unwind first; blocking workers are then joined; only
        // after that is it safe to remove a temp tree or unmounted overlay.
        let staging = Arc::new(Mutex::new(Some(AttemptStaging::default())));
        let reaper = spawn_attempt_reaper(cleanup.clone(), Arc::clone(&staging));
        let close_on_drop = CloseAttemptOnDrop(cleanup.clone());
        let result = self
            .install_repo_with_mode_at_attempt_inner(
                repo_path, branch, rev, target, mode, clonepack, bench, identity, cleanup, &staging,
            )
            .await;
        drop(close_on_drop);
        reaper.await.context("attempt cleanup task")?;
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn install_repo_with_mode_at_attempt_inner<P: AsRef<Path>>(
        &self,
        repo_path: &str,
        branch: &str,
        rev: Option<&str>,
        target: P,
        mode: CloneMode,
        clonepack: Option<&str>,
        bench: Option<&mut Benchmark>,
        identity: &mut InstallIdentity,
        cleanup: &AttemptCleanup,
        staging: &SharedAttemptStaging,
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
        let expected_commit = exact_commit_from_revision(rev);
        crate::perf::reset_perf_counters();
        let _ = crate::worktree_writer::take_write_timing();

        let result = if mode.needs_archive() {
            ExactResultKind::Files
        } else if clonepack == Some("shallow") {
            ExactResultKind::Head
        } else {
            ExactResultKind::Full
        };
        let metric_mode = match result {
            ExactResultKind::Head => "head",
            ExactResultKind::Full => "full",
            ExactResultKind::Files => "files",
        };
        // 1. Resolve the moving selector once. A pending or ready response
        // establishes `identity.pinned`; every later poll and retry uses the
        // metadata-only pinned query.
        let allow_top_up =
            rev.is_none() && mode == CloneMode::Editable && result == ExactResultKind::Full;
        let plan = self
            .resolve_ref_for_operation(
                repo_path,
                branch,
                result,
                rev,
                expected_commit.as_deref(),
                &mut identity.pinned,
                &mut identity.resolved_branch,
                metric_mode,
                allow_top_up,
                &mut identity.clone_id,
                &mut identity.cold,
            )
            .await?;
        bench.mark_resolve();
        let (plan_target, artifact, info, top_up) = plan.into_parts();
        let pinned = identity
            .pinned
            .clone()
            .context("ref resolution completed without a pinned commit")?;
        if plan_target != pinned {
            anyhow::bail!(
                "ref integrity error: plan target {plan_target} does not match pinned commit {pinned}"
            );
        }
        if info.commit != artifact {
            anyhow::bail!(
                "ref integrity error: response commit {} does not match artifact commit {artifact}",
                info.commit
            );
        }
        // A top-up's provider requirement is known as soon as the server has
        // supplied its plan. Fail before downloading Full(A), rather than after
        // spending the base artifact transfer on an impossible exact fetch.
        if top_up {
            self.top_up_provider_for(repo_path)?;
        }
        info!(
            "resolved target {} using {} artifact {}",
            &pinned[..7],
            if top_up { "base" } else { "exact" },
            &artifact[..7]
        );

        if info.clonepack_manifest.is_empty() {
            anyhow::bail!("ref is missing clonepack manifest; run sync first");
        }
        let (artifact_urls, _) = tokio::sync::watch::channel(ArtifactUrlSnapshot {
            generation: 0,
            response: Arc::new(info.clone()),
        });
        let artifact_refresh = PinnedArtifactRefresh {
            repo_path: repo_path.to_string(),
            request_branch: identity
                .resolved_branch
                .clone()
                .filter(|branch| !branch.is_empty())
                .unwrap_or_else(|| branch.to_string()),
            checkout_branch: identity
                .resolved_branch
                .clone()
                .context("ref resolution completed without a checkout branch")?,
            rev: rev.map(str::to_string),
            result,
            target: pinned.clone(),
            artifact: artifact.clone(),
            clonepack_manifest: info.clonepack_manifest.clone(),
            metadata_chunk: info.metadata_chunk.clone(),
            urls: artifact_urls,
            refresh_gate: Arc::new(tokio::sync::Semaphore::new(1)),
        };

        // Hand the decoded manifest to the archive downloader over a oneshot.
        // It latches the value (no lost-wakeup race) and signals manifest
        // failure by dropping the sender (the receiver then errors), so the
        // downloader can never hang waiting for a manifest that will never come.
        let (manifest_tx, manifest_rx) = tokio::sync::oneshot::channel::<Arc<ClonepackManifest>>();

        // 2. Start manifest + metadata downloads concurrently.
        let manifest_task = AbortOnDrop::new(
            self.clone().spawn_fetch_manifest(
                info.clonepack_manifest.clone(),
                artifact.clone(),
                artifact_refresh.clone(),
                manifest_tx,
            ),
            cleanup.clone(),
        );

        let metadata_hash = info.metadata_chunk.clone();
        let metadata_task = AbortOnDrop::new(
            self.clone()
                .spawn_fetch_metadata(metadata_hash, artifact_refresh.clone()),
            cleanup.clone(),
        );

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
            Some(AbortOnDrop::new(
                tokio::task::spawn_blocking(move || {
                    while let Some(msg) = archive_async_rx.blocking_recv() {
                        let send_start = Instant::now();
                        if archive_tx.send(msg).is_err() {
                            break;
                        }
                        crate::perf::record_archive_send_wait(send_start.elapsed());
                    }
                }),
                cleanup.clone(),
            ))
        } else {
            None
        };

        let archive_downloads = if mode.needs_archive() {
            bench.start_archive_download();
            Some(AbortOnDrop::new(
                self.clone().spawn_chunk_downloads(
                    artifact_refresh.clone(),
                    manifest_rx,
                    archive_async_tx,
                ),
                cleanup.clone(),
            ))
        } else {
            drop(archive_async_tx);
            drop(manifest_rx);
            None
        };
        drop(archive_tx);

        // 4. Wait for manifest + metadata.
        let manifest = async {
            manifest_task
                .join()
                .await
                .context("join clonepack manifest fetch")?
                .context("fetch clonepack manifest")
        };
        let metadata = async {
            metadata_task
                .join()
                .await
                .context("join metadata chunk fetch")?
                .context("fetch metadata chunk")
        };
        let (manifest, metadata) =
            tokio::try_join!(manifest, metadata).context("fetch manifest/metadata")?;
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
        let overlay_lower = overlay_dirs.as_ref().map(|dirs| dirs.lower.clone());
        {
            let mut staging_state = staging.lock().unwrap_or_else(|e| e.into_inner());
            staging_state
                .as_mut()
                .context("attempt staging state missing")?
                .overlay_dirs = overlay_dirs;
        }

        // Hold the temp-dir handle for the whole install so any early failure
        // removes the partial directory on drop. After a successful rename onto
        // `target`, its drop is a no-op.
        let install_root = if let Some(lower) = overlay_lower {
            lower
        } else {
            let tmp = temp_install_dir(&target)?;
            let path = tmp.path().to_path_buf();
            let mut staging_state = staging.lock().unwrap_or_else(|e| e.into_inner());
            staging_state
                .as_mut()
                .context("attempt staging state missing")?
                .temp_install = Some(tmp);
            path
        };
        if top_up {
            wait_test_top_up_staging_barrier(&install_root).await?;
        }
        let git_dir = install_root.join(".git");
        let files_only = matches!(mode, CloneMode::Files);
        if !files_only {
            std::fs::create_dir_all(&git_dir)?;
            std::fs::create_dir_all(git_dir.join("refs").join("heads"))?;
            std::fs::create_dir_all(git_dir.join("refs").join("tags"))?;
            std::fs::create_dir_all(git_dir.join("info"))?;

            let branch_name = identity.resolved_branch.as_deref().unwrap_or(branch);
            if branch_name.is_empty() {
                std::fs::write(git_dir.join("HEAD"), format!("{}\n", info.commit))?;
            } else {
                crate::validation::validate_checkout_name(branch_name)
                    .context("server returned invalid checkout branch")?;
                std::fs::write(
                    git_dir.join("HEAD"),
                    format!("ref: refs/heads/{branch_name}\n"),
                )?;
                let branch_ref = git_dir.join("refs").join("heads").join(branch_name);
                if let Some(parent) = branch_ref.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(branch_ref, format!("{}\n", info.commit))?;
            }
            std::fs::write(git_dir.join("info").join("exclude"), b".ripclone/\n")?;
            if info.result == ExactResultKind::Head {
                // Mark HEAD as a shallow boundary so git does not try to traverse
                // missing parents.
                std::fs::write(git_dir.join("shallow"), format!("{}\n", info.commit))?;
            }
        }

        // 6. Write the small .git artifacts from the metadata chunk.
        let pack_dir = git_dir.join("objects").join("pack");
        let mut editable_front_matter_tx = None;
        let editable_packs = if mode.needs_pack_worktree() {
            let (front_matter_tx, front_matter_rx) = tokio::sync::oneshot::channel();
            editable_front_matter_tx = Some(front_matter_tx);
            let client = self.clone();
            let manifest = Arc::clone(&manifest);
            let metadata = Arc::clone(&metadata);
            let pack_dir = pack_dir.clone();
            let work_tree = install_root.clone();
            let task_cleanup = cleanup.clone();
            let artifact_refresh = artifact_refresh.clone();
            Some(AbortOnDrop::new(
                tokio::spawn(async move {
                    client
                        .install_editable_packs(
                            manifest,
                            pack_dir,
                            work_tree,
                            metadata,
                            task_cleanup,
                            artifact_refresh,
                            front_matter_rx,
                        )
                        .await
                }),
                cleanup.clone(),
            ))
        } else {
            None
        };
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
        if let Some(front_matter_tx) = editable_front_matter_tx.take() {
            let _ = front_matter_tx.send(());
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
            Some(AbortOnDrop::new(
                tokio::task::spawn_blocking(move || {
                    // Keep the temp manifest file alive for the duration of extraction.
                    let _guard = manifest_tmp;
                    extract_archive_from_chunk_receiver(&manifest_path, Some(&work_tree), None, rx)
                }),
                cleanup.clone(),
            ))
        } else {
            drop(archive_rx);
            // The temp file can be dropped; nothing needs the manifest on disk.
            drop(manifest_tmp);
            None
        };

        // 8. Wait for downloads + workers.
        let mut archive_bytes = 0u64;
        if let Some(handle) = archive_downloads {
            let bytes = handle
                .join()
                .await
                .context("archive download coordinator")??;
            archive_bytes = bytes;
        }
        if let Some(handle) = archive_bridge {
            handle.join().await.context("archive download bridge")?;
        }
        // Editable single-download path: download the small depth packs in
        // parallel and, as each lands, install it and extract its blobs into the
        // working tree. Download and extraction overlap.
        let mut prebuilt_blob_pack_bytes = 0u64;
        if let Some(handle) = editable_packs {
            prebuilt_blob_pack_bytes =
                handle.join().await.context("editable pack coordinator")??;
            bench.mark_write();
            info!(
                "installed + extracted {} editable packs ({} bytes)",
                manifest.packs.len(),
                prebuilt_blob_pack_bytes
            );
        }
        if let Some(handle) = archive_worker {
            let stats = handle
                .join()
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

        // 9. A top-up installs Full(A) privately, then fetches and checks the
        // exact pinned B before any rename or mount can expose the staging tree.
        if top_up {
            self.top_up_staged_repo(repo_path, &install_root, &artifact, &pinned, cleanup)
                .await
                .with_context(|| {
                    format!("top-up of pinned commit {pinned} from base {artifact}")
                })?;
        }

        // 10. Origin config + finalization.
        if !files_only {
            let origin_url = if top_up {
                let provider = self
                    .provider_instance
                    .as_ref()
                    .context("top-up requires the locally configured provider instance")?;
                local_provider_origin(provider, repo_path)
            } else if info.origin_url.is_empty() {
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

        let overlay_dirs = {
            let mut staging_state = staging.lock().unwrap_or_else(|e| e.into_inner());
            staging_state
                .as_mut()
                .context("attempt staging state missing")?
                .overlay_dirs
                .take()
        };
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
            std::fs::remove_file(install_root.join(INSTALL_STAGING_MARKER))
                .context("remove clone staging marker before publish")?;
            if fsync_requested() {
                fsync_tree(&install_root).context("fsync staged clone before publish")?;
            }
            std::fs::rename(&install_root, &target).with_context(|| {
                format!("rename {} to {}", install_root.display(), target.display())
            })?;
            if fsync_requested() {
                fsync_dir(post_rename_fsync_dir(&target))
                    .context("fsync parent directory after publish")?;
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
            commit: pinned,
            mode: metric_mode,
            cold: identity.cold,
            clone_id: identity.clone_id.clone(),
            bytes: report.total_bytes(),
        })
    }

    /// Best-effort, fire-and-forget POST of clone metrics to the hosted server,
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
            // Omit downloadMs: the client can't cleanly isolate pure
            // chunk-download time from manifest fetch + extraction, and a biased
            // number would skew the cloud's bytes/downloadMs throughput (the
            // headline metric). Better no throughput than a wrong one — the cloud
            // simply won't compute it. It can be reinstated once that phase is isolated.
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

    /// Editable single-download path: download depth packs in parallel and, as
    /// each lands, install it into `pack_dir` and extract its blobs into
    /// `work_tree` so download and extraction overlap. History packs use the
    /// file-backed resumable path. Current-tree packs remain buffered because
    /// extraction consumes their bytes directly; one oversized Git object can
    /// therefore still make client memory scale with that HEAD pack. Uses the
    /// manifest file table to map blobs to paths. Returns total bytes downloaded.
    async fn install_editable_packs(
        &self,
        manifest: Arc<ClonepackManifest>,
        pack_dir: PathBuf,
        work_tree: PathBuf,
        metadata: Arc<MetadataChunk>,
        cleanup: AttemptCleanup,
        artifact_refresh: PinnedArtifactRefresh,
        front_matter_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<u64> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        if manifest.packs.is_empty() {
            anyhow::bail!("clonepack has no packs for editable install");
        }
        std::fs::create_dir_all(&pack_dir)
            .with_context(|| format!("create pack dir {}", pack_dir.display()))?;

        let tuning = ClientTuning::load();
        let download_conc = tuning.editable_download_concurrency;
        let parse_conc = tuning.pack_parse_threads;
        let pipeline_started = Instant::now();

        let bundle_ref = manifest
            .idx_bundle
            .as_ref()
            .context("clonepack manifest is missing required idx bundle")?;
        let idx_bundle_task = {
            let client = self.clone();
            let bundle_ref = bundle_ref.clone();
            let artifact_refresh = artifact_refresh.clone();
            AbortOnDrop::new(
                tokio::spawn(async move {
                    let hash = hash_to_hex(&bundle_ref.hash);
                    client
                        .fetch_verified_artifact_for_install(
                            &hash,
                            Some(bundle_ref.len),
                            &artifact_refresh,
                            ArtifactUrlKind::IdxBundle,
                        )
                        .await
                        .context("fetch idx bundle")
                }),
                cleanup.clone(),
            )
        };
        let midx_task = manifest.midx.as_ref().map(|midx_ref| {
            let client = self.clone();
            let midx_ref = midx_ref.clone();
            let artifact_refresh = artifact_refresh.clone();
            AbortOnDrop::new(
                tokio::spawn(async move {
                    let hash = hash_to_hex(&midx_ref.hash);
                    client
                        .fetch_verified_artifact_for_install(
                            &hash,
                            Some(midx_ref.len),
                            &artifact_refresh,
                            ArtifactUrlKind::Midx,
                        )
                        .await
                        .context("fetch pre-built multi-pack-index")
                }),
                cleanup.clone(),
            )
        });

        enum PackBody {
            Buffered(bytes::Bytes),
            TempFile {
                file: tempfile::NamedTempFile,
                len: u64,
            },
        }

        impl PackBody {
            fn len(&self) -> u64 {
                match self {
                    PackBody::Buffered(bytes) => bytes.len() as u64,
                    PackBody::TempFile { len, .. } => *len,
                }
            }
        }

        let mut jobs: Vec<(usize, PackEntry)> =
            manifest.packs.iter().cloned().enumerate().collect();
        jobs.sort_by_key(|(_, entry)| {
            std::cmp::Reverse(entry.pack.as_ref().map_or(0, |pack| pack.len))
        });
        let download_channel_depth = jobs.len().min(download_conc).max(1);
        let download_client = self.clone();
        let download_pack_dir = pack_dir.clone();
        let download_refresh = artifact_refresh.clone();
        let (download_handle, download_rx) = spawn_downloads_to_bounded_channel(
            jobs,
            download_conc,
            download_channel_depth,
            move |(i, entry)| {
                let client = download_client.clone();
                let pack_dir = download_pack_dir.clone();
                let artifact_refresh = download_refresh.clone();
                async move {
                    let pack_ref = entry
                        .pack
                        .as_ref()
                        .with_context(|| format!("pack {} missing pack ref", i))?;
                    let pack_fetch_start = std::time::Instant::now();
                    let pack_body = if entry.history_only {
                        let (file, len) = client
                            .fetch_chunk_ref_to_temp(pack_ref, &pack_dir, &artifact_refresh, i)
                            .await
                            .with_context(|| format!("stream history pack {}", i))?;
                        crate::perf::record_editable_pack_fetch(pack_fetch_start.elapsed(), len);
                        PackBody::TempFile { file, len }
                    } else {
                        let hash = hash_to_hex(&pack_ref.hash);
                        let bytes = client
                            .fetch_verified_artifact_for_install(
                                &hash,
                                Some(pack_ref.len),
                                &artifact_refresh,
                                ArtifactUrlKind::PackChunk(i),
                            )
                            .await
                            .with_context(|| format!("fetch head pack {}", i))?;
                        crate::perf::record_editable_pack_fetch(
                            pack_fetch_start.elapsed(),
                            bytes.len() as u64,
                        );
                        PackBody::Buffered(bytes)
                    };
                    Ok((i, entry.history_only, entry, pack_body))
                }
            },
        );
        let download_task = AbortOnDrop::new(download_handle, cleanup.clone());

        let prep_metadata = Arc::clone(&metadata);
        let prep_work_tree = work_tree.clone();
        let prep_task = AbortOnDrop::new(
            tokio::task::spawn_blocking(move || {
                // Validate every blob sha1 length before building the complete
                // blob map so the files-written guard below remains exact.
                for file in &prep_metadata.files {
                    if file.blob_sha1.len() != 20 {
                        anyhow::bail!(
                            "manifest blob_sha1 for {} is {} bytes, expected 20",
                            String::from_utf8_lossy(&file.path),
                            file.blob_sha1.len()
                        );
                    }
                }
                let blob_map = Arc::new(crate::extract::build_blob_path_map(&prep_metadata.files));
                crate::extract::prepare_worktree_dirs(&prep_work_tree, &prep_metadata.files)
                    .context("prepare worktree dirs")?;
                let writer = Arc::new(crate::worktree_writer::WorktreeWriter::new()?);
                Ok::<_, anyhow::Error>((blob_map, writer))
            }),
            cleanup.clone(),
        );

        let idx_bundle = async {
            idx_bundle_task
                .join()
                .await
                .context("idx bundle fetch task")?
        };
        let prep = async {
            prep_task
                .join()
                .await
                .context("worktree directory preparation task")?
        };
        let (idx_bundle, (blob_map, worktree_writer)) = tokio::try_join!(idx_bundle, prep)?;
        let idx_bundle = Arc::new(idx_bundle);

        // Pack bodies, the idx bundle, worktree directory creation, and the
        // small skeleton writes all start independently. Installation waits at
        // this one-way boundary so the pack database cannot race its skeleton.
        front_matter_rx
            .await
            .context("editable front matter did not finish")?;

        let downloads = stream::unfold(download_rx, |mut rx| async move {
            rx.recv().await.map(|result| (result, rx))
        });

        // Stage 2: install each pack; hand-parse for the worktree only when it's
        // a HEAD-closure (undeltified) pack. History-only packs are deltified —
        // installed for the object DB, read by git, never hand-parsed.
        let total = downloads
            .map(|res| {
                let pack_dir = pack_dir.to_path_buf();
                let work_tree = work_tree.to_path_buf();
                let idx_bundle = Arc::clone(&idx_bundle);
                let blob_map = Arc::clone(&blob_map);
                let worktree_writer = Arc::clone(&worktree_writer);
                let cleanup = cleanup.clone();
                async move {
                    let (i, history_only, entry, pack_body) = res?;
                    let idx_bytes = manifest_pack_idx_bytes(&entry, i, &idx_bundle)?;
                    let idx_len = u64::try_from(idx_bytes.len())
                        .context("pack idx length does not fit in u64")?;
                    let bytes = pack_body
                        .len()
                        .checked_add(idx_len)
                        .context("pack and idx length overflow")?;
                    let fd_permit = if history_only {
                        None
                    } else {
                        Some(tuning::acquire_pack_parse_fd_permit().await)
                    };
                    let result = AbortOnDrop::new(
                        tokio::task::spawn_blocking(
                            move || -> Result<crate::extract::PackExtractResult> {
                                // Own the global Linux descriptor lease for the
                                // complete parser/writer lifetime, including
                                // deferred io_uring windows. On other platforms
                                // this is a zero-sized no-op guard.
                                let _fd_permit = fd_permit;
                                if pack_body.len() < 20 {
                                    anyhow::bail!(
                                        "pack {} too short ({} bytes)",
                                        i,
                                        pack_body.len()
                                    );
                                }
                                let (name, pack_bytes) = match pack_body {
                                    PackBody::Buffered(pack_bytes) => {
                                        // Git names packs by the 20-byte trailer sha; the idx
                                        // pairs to the pack by basename.
                                        let name =
                                            hex::encode(&pack_bytes[pack_bytes.len() - 20..]);
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
                                std::fs::write(
                                    pack_dir.join(format!("pack-{}.idx", name)),
                                    &idx_bytes,
                                )
                                .with_context(|| format!("write idx {}", name))?;
                                wait_for_test_pack_worker(i)?;
                                if history_only {
                                    return Ok(crate::extract::PackExtractResult {
                                        files: 0,
                                        stats: Vec::new(),
                                    });
                                }
                                let Some(pack_bytes) = pack_bytes else {
                                    anyhow::bail!(
                                        "head pack {} was not buffered for extraction",
                                        i
                                    );
                                };
                                crate::extract::extract_blobs_from_pack_bytes(
                                    &pack_bytes,
                                    &blob_map,
                                    &work_tree,
                                    &worktree_writer,
                                )
                                .with_context(|| format!("extract pack {}", name))
                            },
                        ),
                        cleanup,
                    )
                    .join()
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
        let download_elapsed = download_task
            .join()
            .await
            .context("editable pack download coordinator")??;
        info!(
            download_wall_ms = download_elapsed.as_millis(),
            pipeline_wall_ms = pipeline_started.elapsed().as_millis(),
            "editable pack downloads overlapped independent install work"
        );
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
        AbortOnDrop::new(
            tokio::task::spawn_blocking(move || {
                crate::git::clear_skip_worktree_index_with_stats_byte_iter(
                    &work_tree2,
                    path_bytes.iter().map(Vec::as_slice),
                    &stat_cache,
                )
            }),
            cleanup.clone(),
        )
        .join()
        .await
        .context("spawn clear skip-worktree and refresh index stats")??;

        // Install the multi-pack-index so git object lookups stay O(log) across
        // the many installed packs. A cold build supplies the prebuilt MIDX;
        // an incremental shallow build may omit it when base packs are remote,
        // in which case the client builds it from the installed pack indexes.
        if let Some(midx_task) = midx_task {
            match midx_task
                .join()
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
                    let _ = AbortOnDrop::new(
                        tokio::task::spawn_blocking(move || {
                            crate::git::write_multi_pack_index(&work_tree3)
                        }),
                        cleanup.clone(),
                    )
                    .join()
                    .await;
                }
            }
        } else {
            let work_tree3 = work_tree.to_path_buf();
            let _ = AbortOnDrop::new(
                tokio::task::spawn_blocking(move || {
                    crate::git::write_multi_pack_index(&work_tree3)
                }),
                cleanup.clone(),
            )
            .join()
            .await;
        }

        Ok(total)
    }
    fn spawn_fetch_manifest(
        self,
        hash: String,
        pinned: String,
        refresh: PinnedArtifactRefresh,
        manifest_tx: tokio::sync::oneshot::Sender<Arc<ClonepackManifest>>,
    ) -> tokio::task::JoinHandle<Result<Arc<ClonepackManifest>>> {
        tokio::spawn(async move {
            let data = self
                .fetch_verified_artifact_for_install(
                    &hash,
                    None,
                    &refresh,
                    ArtifactUrlKind::Manifest,
                )
                .await
                .context("fetch clonepack manifest")?;
            let manifest =
                ClonepackManifest::decode(data.as_ref()).context("decode clonepack manifest")?;
            validate_manifest_commit(&manifest, &pinned)?;
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
        refresh: PinnedArtifactRefresh,
    ) -> tokio::task::JoinHandle<Result<MetadataChunk>> {
        tokio::spawn(async move {
            let data = self
                .fetch_verified_artifact_for_install(
                    &hash,
                    None,
                    &refresh,
                    ArtifactUrlKind::Metadata,
                )
                .await
                .with_context(|| format!("fetch metadata chunk {hash}"))?;
            let metadata = MetadataChunk::decode_and_validate(data.as_ref())?;
            Ok(metadata)
        })
    }

    fn spawn_chunk_downloads(
        self,
        refresh: PinnedArtifactRefresh,
        manifest_rx: tokio::sync::oneshot::Receiver<Arc<ClonepackManifest>>,
        tx: tokio::sync::mpsc::Sender<(usize, Result<bytes::Bytes>)>,
    ) -> tokio::task::JoinHandle<Result<u64>> {
        tokio::spawn(async move {
            use futures::stream::{self, StreamExt, TryStreamExt};

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
            let jobs: Vec<(usize, ChunkRef)> = manifest
                .archive_chunks
                .iter()
                .cloned()
                .enumerate()
                .collect();

            stream::iter(jobs)
                .map(|(index, chunk_ref)| {
                    let client = self.clone();
                    let refresh = refresh.clone();
                    let tx = tx.clone();
                    async move {
                        let fetch_start = Instant::now();
                        let hash = hash_to_hex(&chunk_ref.hash);
                        let bytes = client
                            .fetch_verified_artifact_for_install(
                                &hash,
                                Some(chunk_ref.len),
                                &refresh,
                                ArtifactUrlKind::ArchiveChunk(index),
                            )
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
                .try_fold(0u64, |acc, len| async move {
                    acc.checked_add(len)
                        .context("downloaded archive byte count overflow")
                })
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

    fn top_up_provider_for(&self, repo_path: &str) -> Result<&ProviderInstance> {
        let provider = self
            .provider_instance
            .as_ref()
            .filter(|provider| provider.id.as_str() == self.provider)
            .context(
                "top-up is unavailable because the selected provider has no local client configuration",
            )?;
        let repo_id = RepoId {
            provider: provider.id.clone(),
            path: repo_path.to_string(),
        };
        crate::validation::validate_repo_path(provider, &repo_id)
            .context("invalid local provider repository path for top-up")?;
        Ok(provider)
    }

    async fn top_up_staged_repo(
        &self,
        repo_path: &str,
        install_root: &Path,
        base: &str,
        target: &str,
        cleanup: &AttemptCleanup,
    ) -> Result<()> {
        let top_up_started = Instant::now();
        crate::validation::validate_object_id(base).context("invalid top-up base object id")?;
        crate::validation::validate_object_id(target).context("invalid top-up target object id")?;
        let provider = self.top_up_provider_for(repo_path)?;

        if install_root.join(".git/shallow").exists() {
            anyhow::bail!("top-up base unexpectedly contains a shallow boundary");
        }
        let installed = self
            .run_managed_git(install_root, ["rev-parse", "HEAD"], None, cleanup)
            .await?;
        if installed.trim() != base {
            anyhow::bail!(
                "installed top-up base HEAD {} does not match declared base {base}",
                installed.trim()
            );
        }
        let test_unchanged = if std::env::var_os("RIPCLONE_TESTING").is_some() {
            std::env::var_os("RIPCLONE_TEST_TOP_UP_UNCHANGED_PATH").map(PathBuf::from)
        } else {
            None
        };
        let unchanged_mtime_before = test_unchanged
            .as_ref()
            .and_then(|path| std::fs::metadata(install_root.join(path)).ok())
            .and_then(|metadata| metadata.modified().ok());

        let origin = local_provider_origin(provider, repo_path);
        let auth_header = match self.upstream_token.as_deref() {
            Some(token) => Some(provider.auth_header(token).context(
                "the locally configured provider cannot construct its authentication header",
            )?),
            None => None,
        };
        let fetch_args = vec![
            "fetch".to_string(),
            "--no-write-fetch-head".to_string(),
            "--no-tags".to_string(),
            "--no-recurse-submodules".to_string(),
            "--refmap=".to_string(),
            "--".to_string(),
            origin,
            target.to_string(),
        ];
        self.run_managed_git_owned(install_root, fetch_args, auth_header.as_ref(), cleanup)
            .await
            .with_context(|| format!("exact upstream fetch of pinned commit {target}"))?;

        let repo = install_root.to_path_buf();
        let target_for_parse = target.to_string();
        let parent = AbortOnDrop::new(
            tokio::task::spawn_blocking(move || {
                crate::git::parent_commit(&repo, &target_for_parse)
            }),
            cleanup.clone(),
        )
        .join()
        .await
        .context("join top-up commit validation")??;
        if parent.as_deref() != Some(base) {
            anyhow::bail!(
                "pinned commit {target} is not a single-parent child of top-up base {base}"
            );
        }

        self.run_managed_git(install_root, ["reset", "--hard", target], None, cleanup)
            .await
            .with_context(|| format!("update staged worktree to pinned commit {target}"))?;
        let head = self
            .run_managed_git(install_root, ["rev-parse", "HEAD"], None, cleanup)
            .await?;
        if head.trim() != target {
            anyhow::bail!(
                "top-up staged HEAD {} does not match pinned commit {target}",
                head.trim()
            );
        }
        let head_ref = self
            .run_managed_git(install_root, ["symbolic-ref", "-q", "HEAD"], None, cleanup)
            .await?;
        if !head_ref.trim().starts_with("refs/heads/") {
            anyhow::bail!("top-up staged HEAD is not attached to a branch");
        }
        let status = self
            .run_managed_git(
                install_root,
                [
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                    "--",
                    ".",
                    ":(exclude).ripclone-install-staging",
                ],
                None,
                cleanup,
            )
            .await?;
        if !status.trim().is_empty() {
            anyhow::bail!("top-up staged worktree is not clean");
        }
        if let (Some(path), Some(before), Some(log)) = (
            test_unchanged,
            unchanged_mtime_before,
            std::env::var_os("RIPCLONE_TEST_TOP_UP_METRICS_LOG"),
        ) {
            let after = std::fs::metadata(install_root.join(path))
                .context("stat test top-up unchanged path after update")?
                .modified()
                .context("read test top-up unchanged mtime after update")?;
            let nanos = |time: std::time::SystemTime| {
                time.duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            };
            std::fs::write(
                log,
                format!(
                    "before_mtime_ns={}\nafter_mtime_ns={}\ntop_up_phase_us={}\n",
                    nanos(before),
                    nanos(after),
                    top_up_started.elapsed().as_micros()
                ),
            )
            .context("write test top-up metrics log")?;
        }
        Ok(())
    }

    async fn run_managed_git<const N: usize>(
        &self,
        repo: &Path,
        args: [&str; N],
        auth_header: Option<&(String, String)>,
        cleanup: &AttemptCleanup,
    ) -> Result<String> {
        self.run_managed_git_owned(
            repo,
            args.into_iter().map(str::to_string).collect(),
            auth_header,
            cleanup,
        )
        .await
    }

    async fn run_managed_git_owned(
        &self,
        repo: &Path,
        args: Vec<String>,
        auth_header: Option<&(String, String)>,
        cleanup: &AttemptCleanup,
    ) -> Result<String> {
        use std::io::{Read, Seek};
        use std::process::Stdio;

        let command_started = Instant::now();
        let command_name = args.first().cloned().unwrap_or_default();
        let mut stdout_file = tempfile::tempfile().context("create top-up Git stdout file")?;
        let child_stdout = stdout_file
            .try_clone()
            .context("clone top-up Git stdout file")?;
        let mut stderr = tempfile::tempfile().context("create top-up Git stderr file")?;
        let child_stderr = stderr.try_clone().context("clone top-up Git stderr file")?;
        let mut command = tokio::process::Command::new("git");
        command
            .env_clear()
            .env("HOME", "/nonexistent")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_NO_REPLACE_OBJECTS", "1")
            .env("GIT_PAGER", "cat")
            .env("LC_ALL", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::from(child_stdout))
            .stderr(Stdio::from(child_stderr));
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        if let Some(root) = std::env::var_os("SystemRoot") {
            command.env("SystemRoot", root);
        }
        command.args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "credential.helper=",
            "-c",
            "maintenance.auto=false",
            "-c",
            "gc.auto=0",
            "-c",
            "protocol.version=2",
            "-c",
            "fetch.fsckObjects=true",
            "-c",
            "transfer.fsckObjects=true",
        ]);
        if let Some((name, value)) = auth_header {
            command
                .args(["-c", &format!("http.extraHeader={name}: {value}")])
                .args(["-c", "http.followRedirects=false"]);
        }
        command.arg("-C").arg(repo).args(&args);
        #[cfg(unix)]
        // SAFETY: `pre_exec` runs after fork and before exec. The closure only
        // calls async-signal-safe `setpgid` and constructs an OS error from
        // thread-local errno on failure; it captures no borrowed state.
        unsafe {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let child = command
            .spawn()
            .context("spawn managed top-up Git command")?;
        let mut managed = ManagedGitChild::new(child, cleanup.clone());
        let timeout = managed_git_timeout();
        let status = match tokio::time::timeout(
            timeout,
            managed
                .child
                .as_mut()
                .context("managed top-up child missing")?
                .wait(),
        )
        .await
        {
            Ok(status) => status.context("wait for managed top-up Git command")?,
            Err(_) => {
                managed.terminate();
                let _ = managed
                    .child
                    .as_mut()
                    .context("managed top-up child missing during timeout cleanup")?
                    .wait()
                    .await;
                managed.child.take();
                anyhow::bail!(
                    "top-up Git command timed out after {} seconds",
                    timeout.as_secs()
                );
            }
        };
        managed.child.take();
        if !status.success() {
            stderr.rewind().context("rewind top-up Git stderr")?;
            let mut detail = String::new();
            stderr
                .read_to_string(&mut detail)
                .context("read top-up Git stderr")?;
            if let Some((_, value)) = auth_header {
                detail = detail.replace(value, "<redacted>");
            }
            if let Some(token) = self.upstream_token.as_deref() {
                detail = detail.replace(token, "<redacted>");
            }
            let detail = detail
                .lines()
                .rev()
                .filter(|line| !line.trim().is_empty())
                .take(4)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n");
            if detail.is_empty() {
                anyhow::bail!("top-up Git command failed with {status}");
            }
            anyhow::bail!("top-up Git command failed: {detail}");
        }
        stdout_file
            .rewind()
            .context("rewind managed top-up Git stdout")?;
        let mut stdout = Vec::new();
        stdout_file
            .read_to_end(&mut stdout)
            .context("read managed top-up Git stdout")?;
        record_test_managed_git(&command_name, command_started.elapsed())?;
        Ok(String::from_utf8_lossy(&stdout).into_owned())
    }
    async fn should_use_overlay(&self, metadata: &MetadataChunk, staging_dir: &Path) -> bool {
        if !overlay::is_available() {
            return false;
        }
        let raw_bytes = match metadata.files.iter().try_fold(0u64, |total, file| {
            total
                .checked_add(file.checked_total_len()?)
                .context("overlay raw file length total overflow")
        }) {
            Ok(total) => total,
            Err(e) => {
                warn!("overlay staging size calculation overflowed raw file lengths");
                tracing::debug!(error = %e, "overlay raw size calculation failed");
                return false;
            }
        };
        // Sum the compressed length of every frame; archive chunks contain only
        // frames, so this is the total compressed archive size.
        let compressed_bytes = match metadata.frames.iter().try_fold(0u64, |total, frame| {
            total.checked_add(u64::from(frame.compressed_len))
        }) {
            Some(total) => total,
            None => {
                warn!("overlay staging size calculation overflowed compressed frame lengths");
                return false;
            }
        };

        // No size threshold: overlay is opt-in (see overlay::is_available), so if
        // the operator asked for it we honor it for any repo, falling back only
        // when there isn't enough tmpfs space or the kernel disallows the mount.
        let margin_mb: u64 = 128;
        let required = match raw_bytes
            .checked_add(compressed_bytes)
            .and_then(|total| total.checked_add(margin_mb * 1024 * 1024))
        {
            Some(required) => required,
            None => {
                warn!("overlay staging size calculation overflowed total size");
                return false;
            }
        };
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYNC_AT_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SYNC_AT_C: &str = "cccccccccccccccccccccccccccccccccccccccc";

    #[test]
    fn sync_at_response_sequence_keeps_one_identity_across_all_statuses() {
        let mut pin = None;
        observe_sync_at_identity(&mut pin, SYNC_AT_B, "main", "202").unwrap();
        observe_sync_at_identity(&mut pin, SYNC_AT_B, "main", "503").unwrap();
        observe_sync_at_identity(&mut pin, SYNC_AT_B, "main", "200").unwrap();
        assert_eq!(
            pin,
            Some(SyncAtPin {
                commit: SYNC_AT_B.to_string(),
                branch: Some("main".to_string()),
            })
        );
    }

    #[test]
    fn explicit_sync_sha_rejects_initial_commit_change_for_every_response_status() {
        for response_kind in ["202", "503", "200"] {
            let mut pin = exact_commit_from_revision(Some(SYNC_AT_B)).map(|commit| SyncAtPin {
                commit,
                branch: None,
            });
            let error = observe_sync_at_identity(&mut pin, SYNC_AT_C, "main", response_kind)
                .expect_err("an explicit SHA must be the initial sync pin");
            assert!(
                format!("{error:#}").contains("sync integrity error"),
                "unexpected {response_kind} error: {error:#}"
            );
        }
    }

    #[test]
    fn sync_at_response_sequence_rejects_commit_change_on_later_202() {
        let mut pin = None;
        observe_sync_at_identity(&mut pin, SYNC_AT_B, "main", "202").unwrap();
        let error = observe_sync_at_identity(&mut pin, SYNC_AT_C, "main", "202")
            .expect_err("later pending response must not repin");
        assert!(format!("{error:#}").contains("sync integrity error"));
    }

    #[test]
    fn sync_at_response_sequence_rejects_branch_change_on_later_503() {
        let mut pin = None;
        observe_sync_at_identity(&mut pin, SYNC_AT_B, "main", "202").unwrap();
        let error = observe_sync_at_identity(&mut pin, SYNC_AT_B, "release", "503")
            .expect_err("queue response must preserve the selected branch");
        assert!(format!("{error:#}").contains("sync integrity error"));
    }

    #[test]
    fn sync_at_response_sequence_rejects_commit_change_on_final_200() {
        let mut pin = None;
        observe_sync_at_identity(&mut pin, SYNC_AT_B, "main", "202").unwrap();
        let error = observe_sync_at_identity(&mut pin, SYNC_AT_C, "main", "200")
            .expect_err("ready response must preserve the selected commit");
        assert!(format!("{error:#}").contains("sync integrity error"));
    }

    #[test]
    fn sync_at_pending_response_requires_branch_identity() {
        let mut pin = None;
        let error = observe_sync_at_identity(&mut pin, SYNC_AT_B, "", "202")
            .expect_err("pending response without a branch must fail closed");
        assert!(format!("{error:#}").contains("omitted branch"));
        assert!(pin.is_none());
    }

    #[test]
    fn access_error_hints_are_actionable() {
        // A 403 access denial is terminal and says so, without fabricating any
        // upgrade/subscribe prose an agent fleet would have to scrape.
        let hint = error_hint(403, Some("no_access"), true);
        assert!(hint.contains("access"), "no_access hint: {hint}");
        assert!(!hint.contains("ripclone.com"), "no_access hint: {hint}");
        // A 401 routes to the right login path depending on the server.
        assert!(error_hint(401, None, true).contains("login"));
        assert!(error_hint(401, None, false).contains("RIPCLONE_SERVER_TOKEN"));
        // Transient statuses hint at retry, not a terminal action.
        assert!(error_hint(429, None, false).contains("retry"));
        // No hint fabricates a subscribe/upgrade URL.
        assert!(!error_hint(402, None, true).contains("ripclone.com"));
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

    #[cfg(unix)]
    #[test]
    fn next_clone_removes_only_marked_unlocked_staging() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("linux");

        let stale = dir.path().join("linux.ripclone-old.tmp");
        std::fs::create_dir(&stale).unwrap();
        std::fs::write(stale.join(INSTALL_STAGING_MARKER), b"ripclone\n").unwrap();
        std::fs::write(stale.join("partial"), b"partial clone").unwrap();

        let live = temp_install_dir(&target).unwrap();
        let live_path = live.path().to_path_buf();

        let unmarked = dir.path().join("linux.ripclone-unmarked.tmp");
        std::fs::create_dir(&unmarked).unwrap();
        std::fs::write(unmarked.join("user-data"), b"preserve").unwrap();

        let created = temp_install_dir(&target).unwrap();
        assert!(!stale.exists());
        assert!(live_path.exists());
        assert!(unmarked.exists());
        assert!(created.path().join(INSTALL_STAGING_MARKER).is_file());
    }

    #[test]
    fn collect_fsync_targets_covers_files_dirs_and_index() {
        // The durability barrier must flush the working-tree files, every
        // directory that holds one, AND the `.git/index` stat cache that git
        // consults to decide clean vs dirty. Missing the index is the exact way
        // a crash leaves a torn tree that `git status` calls clean (D6 / U3).
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join(".git/index"), b"INDEX").unwrap();
        std::fs::write(root.join("src/main.rs"), b"fn main() {}").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("src/main.rs", root.join("link")).unwrap();

        let mut files = Vec::new();
        let mut dirs = Vec::new();
        collect_fsync_targets(root, &mut files, &mut dirs).expect("collect targets");

        assert!(
            files.iter().any(|p| p.ends_with(".git/index")),
            "index stat cache must be in the fsync set: {files:?}"
        );
        assert!(files.iter().any(|p| p.ends_with("src/main.rs")));
        // The symlink is persisted via its parent dir, so it is not fsynced
        // directly.
        assert!(!files.iter().any(|p| p.ends_with("link")));
        // Every directory that holds a materialized entry is flushed.
        assert!(dirs.iter().any(|p| p.ends_with(".git")));
        assert!(dirs.iter().any(|p| p.ends_with("src")));
        assert!(dirs.iter().any(|p| p == root));

        // The batched barrier itself must run cleanly over the collected set
        // (POSIX path here; the io_uring path is exercised on Linux CI).
        crate::worktree_writer::fsync_paths_durable(&files, &dirs)
            .expect("durable fsync over collected tree");
    }

    #[test]
    fn post_rename_fsync_dir_resolves_relative_target_to_cwd() {
        // The post-rename durability fsync makes the atomic rename itself
        // durable (BUILD_OPTIONS: "the target's parent directory after the
        // atomic rename"). A bare relative target — the common case, the
        // README quickstart uses `bun` — has an empty parent. The
        // resolver must fall back to the containing directory (cwd `.`) so the
        // fsync runs, not be dropped/skipped (D6).
        assert_eq!(
            post_rename_fsync_dir(Path::new("bun")),
            Path::new("."),
            "bare relative target must fsync its container (cwd), not be skipped"
        );
        // Nested relative and absolute targets already resolve to their real
        // parent — the fallback must not disturb those.
        assert_eq!(
            post_rename_fsync_dir(Path::new("out/bun")),
            Path::new("out")
        );
        assert_eq!(
            post_rename_fsync_dir(Path::new("/work/bun")),
            Path::new("/work")
        );
        // A trailing slash and a multi-component relative path must still map to
        // the real container, not the empty/skipped path.
        assert_eq!(post_rename_fsync_dir(Path::new("bun/")), Path::new("."));
        assert_eq!(post_rename_fsync_dir(Path::new("a/b/c")), Path::new("a/b"));
    }

    #[test]
    fn post_rename_fsync_dir_result_is_syncable() {
        // Behavior, not just path strings: the resolver's output must be a real
        // directory that the post-rename `fsync_dir` can actually open and
        // flush. Pre-fix the bare-relative branch was skipped outright — no
        // fsync ran at all — so exercise the real syscall for both the relative
        // (cwd) and nested cases to prove the barrier is not silently dropped.
        fsync_dir(post_rename_fsync_dir(Path::new("bun")))
            .expect("bare relative target's container (cwd) must be fsync-able");
        let tmp = tempfile::tempdir().expect("tempdir");
        let nested = tmp.path().join("bun");
        assert_eq!(post_rename_fsync_dir(&nested), tmp.path());
        fsync_dir(post_rename_fsync_dir(&nested))
            .expect("nested target's real parent must be fsync-able");
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

    /// The shared status rule. Break any arm of this table and a real download
    /// path changes: a retryable status stops retrying, a permanent failure
    /// enters the signed-URL refresh loop, or an expired signature fails the
    /// clone instead of refreshing.
    #[test]
    fn fetch_status_rule_separates_retry_refresh_and_permanent() {
        use reqwest::StatusCode;
        let classify = |status: StatusCode, signed: bool| {
            classify_fetch_status(status, signed, "abc").map(|f| match f {
                FetchFailure::Retry(_) => "retry",
                FetchFailure::RefreshUrl(_) => "refresh",
                FetchFailure::Permanent(_) => "permanent",
            })
        };

        assert_eq!(classify(StatusCode::OK, true), None);
        assert_eq!(classify(StatusCode::OK, false), None);

        for status in [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert_eq!(classify(status, true), Some("retry"), "{status} signed");
            assert_eq!(classify(status, false), Some("retry"), "{status} gateway");
        }

        for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
            assert_eq!(classify(status, true), Some("refresh"), "{status} signed");
            // The authenticated gateway has no URL to refresh; a rejected
            // credential is a permanent failure, not a re-resolve loop.
            assert_eq!(
                classify(status, false),
                Some("permanent"),
                "{status} gateway"
            );
        }

        for status in [
            StatusCode::NOT_FOUND,
            StatusCode::GONE,
            StatusCode::BAD_REQUEST,
        ] {
            assert_eq!(classify(status, true), Some("permanent"), "{status} signed");
            assert_eq!(
                classify(status, false),
                Some("permanent"),
                "{status} gateway"
            );
        }
    }

    /// Refresh is consumed only by the streamed downloader. Converting any
    /// failure for a buffered caller preserves the concrete HTTP error without
    /// requesting an outer clone retry.
    #[test]
    fn converted_fetch_failures_preserve_their_concrete_errors() {
        let refresh = FetchFailure::RefreshUrl(anyhow::anyhow!("expired")).into_error();
        assert_eq!(format!("{refresh:#}"), "expired");
        let retry = FetchFailure::Retry(anyhow::anyhow!("503")).into_error();
        assert_eq!(format!("{retry:#}"), "503");
        let permanent = FetchFailure::Permanent(anyhow::anyhow!("404")).into_error();
        assert_eq!(format!("{permanent:#}"), "404");
    }

    /// The shared verification rule: length first, then the content address, and
    /// both are permanent.
    #[test]
    fn artifact_verification_rejects_wrong_length_and_wrong_hash() {
        let hash = "a".repeat(64);
        verify_fetched_artifact(&hash, Some(10), 10, &hash).expect("matching artifact is accepted");
        verify_fetched_artifact(&hash, None, 7, &hash).expect("unknown length is accepted");

        let short = verify_fetched_artifact(&hash, Some(10), 9, &hash)
            .expect_err("a short artifact is rejected");
        assert!(matches!(short, FetchFailure::Permanent(_)));
        assert!(format!("{:#}", short.into_error()).contains("size mismatch"));

        let wrong = verify_fetched_artifact(&hash, Some(10), 10, &"b".repeat(64))
            .expect_err("a wrong content address is rejected");
        assert!(matches!(wrong, FetchFailure::Permanent(_)));
        assert!(format!("{:#}", wrong.into_error()).contains("hash mismatch"));
    }

    #[tokio::test]
    async fn spawned_download_stage_progresses_while_consumer_is_stalled() {
        let entered = Arc::new(tokio::sync::Barrier::new(3));
        let completed = Arc::new(AtomicUsize::new(0));
        let download_entered = Arc::clone(&entered);
        let download_completed = Arc::clone(&completed);
        let (task, mut rx) =
            spawn_downloads_to_bounded_channel(vec![0usize, 1usize], 2, 2, move |job| {
                let entered = Arc::clone(&download_entered);
                let completed = Arc::clone(&download_completed);
                async move {
                    entered.wait().await;
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(job)
                }
            });

        // Both downloads have started. Do not receive anything yet: this
        // models every parser slot being occupied by blocking extraction.
        entered.wait().await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while completed.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("spawned downloads progress without consumer polling");

        task.await
            .expect("download coordinator task")
            .expect("download coordinator result");
        let mut values = vec![
            rx.recv().await.expect("first queued result").unwrap(),
            rx.recv().await.expect("second queued result").unwrap(),
        ];
        values.sort_unstable();
        assert_eq!(values, vec![0, 1]);
    }

    #[tokio::test]
    async fn cancelled_join_keeps_blocking_sibling_armed_until_operation_cleanup() {
        let cleanup = AttemptCleanup::default();
        let fixture = tempfile::tempdir().unwrap();
        let target = fixture.path().join("target");
        let staging_owner = temp_install_dir(&target).unwrap();
        let staging = staging_owner.path().to_path_buf();
        let staging_worker = staging.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let sibling = AbortOnDrop::new(
            tokio::task::spawn_blocking(move || {
                std::fs::write(staging_worker.join("partial"), b"started").unwrap();
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                std::fs::write(staging_worker.join("late-write"), b"finished").unwrap();
                Ok::<(), anyhow::Error>(())
            }),
            cleanup.clone(),
        );
        let failed = AbortOnDrop::new(
            tokio::spawn(async move {
                started_rx.await.unwrap();
                Err::<(), anyhow::Error>(anyhow::anyhow!("download failed"))
            }),
            cleanup.clone(),
        );

        let failed_join = async {
            failed.join().await.context("join failed task")??;
            Ok::<(), anyhow::Error>(())
        };
        let sibling_join = async {
            sibling.join().await.context("join blocking sibling")??;
            Ok::<(), anyhow::Error>(())
        };
        tokio::try_join!(failed_join, sibling_join)
            .expect_err("failed task cancels the sibling join future");

        let cleanup_wait = cleanup.clone();
        let next_attempt_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let next_attempt_flag = Arc::clone(&next_attempt_started);
        let staging_after_cleanup = staging.clone();
        let mut drain_then_retry = Box::pin(async move {
            cleanup_wait.drain().await;
            drop(staging_owner);
            assert!(!staging_after_cleanup.exists());
            next_attempt_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut drain_then_retry)
                .await
                .is_err(),
            "operation cleanup must wait for the cancelled join's blocking sibling"
        );
        assert!(!next_attempt_started.load(std::sync::atomic::Ordering::SeqCst));
        assert!(staging.exists());
        assert!(!target.exists());
        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), drain_then_retry)
            .await
            .expect("blocking sibling joined before cleanup");
        assert!(next_attempt_started.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!staging.exists());
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn cancelled_clone_reaper_joins_worker_before_removing_staging() {
        let cleanup = AttemptCleanup::default();
        let fixture = tempfile::tempdir().unwrap();
        let target = fixture.path().join("target");
        let staging_owner = temp_install_dir(&target).unwrap();
        let staging_path = staging_owner.path().to_path_buf();
        let staging = Arc::new(Mutex::new(Some(AttemptStaging {
            overlay_dirs: None,
            temp_install: Some(staging_owner),
        })));
        let mut reaper = spawn_attempt_reaper(cleanup.clone(), Arc::clone(&staging));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_path = staging_path.clone();
        let operation_cleanup = cleanup.clone();
        let mut operation = tokio::spawn(async move {
            let _close_on_drop = CloseAttemptOnDrop(operation_cleanup.clone());
            let _worker = AbortOnDrop::new(
                tokio::task::spawn_blocking(move || {
                    std::fs::write(worker_path.join("partial"), b"started").unwrap();
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    std::fs::write(worker_path.join("late-write"), b"finished").unwrap();
                }),
                operation_cleanup,
            );
            std::future::pending::<()>().await;
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), started_rx)
            .await
            .expect("blocking worker started")
            .expect("worker start signal");
        operation.abort();
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut operation)
            .await
            .expect("cancelled clone task joined")
            .expect_err("clone task was cancelled");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut reaper)
                .await
                .is_err(),
            "operation cancellation must not remove staging before blocking work exits"
        );
        assert!(staging_path.exists());
        assert!(!target.exists());

        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut reaper)
            .await
            .expect("cancelled clone reaper completed")
            .expect("reaper task joined");
        assert!(!staging_path.exists());
        assert!(!target.exists());
    }

    #[test]
    fn manifest_commit_must_match_the_operation_pin() {
        let manifest = ClonepackManifest {
            commit: "b".repeat(40),
            ..Default::default()
        };
        let error = validate_manifest_commit(&manifest, &"a".repeat(40)).unwrap_err();
        assert!(format!("{error:#}").contains("clonepack integrity error"));
    }

    #[test]
    fn pending_artifact_guidance_preserves_the_repository_argument() {
        let commit = "a".repeat(40);
        let rendered = ArtifactPending {
            commit: commit.clone(),
            mode: "full".to_string(),
        }
        .to_string();
        assert!(
            rendered.contains(&format!(
                "retry the same clone command with `--at {commit}`"
            )),
            "pending guidance must tell the user how to amend their complete command: {rendered}"
        );
        assert!(
            !rendered.contains("`ripclone clone --at"),
            "pending guidance must not print a command with the required repository omitted: {rendered}"
        );
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
