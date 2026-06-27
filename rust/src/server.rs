use crate::RefInfo;
use crate::archive::ArchiveBuilder;
use crate::auth::access::{AccessDecision, AccessVerifier, HttpAccessVerifier};
use crate::auth::broker::{CredentialBroker, StaticBroker};
use crate::backends::{self, QueueBackend};
use crate::cas::Cas;
use crate::clonepack::{ChunkRef, ClonepackManifest, hash_from_hex, hash_to_hex};
use crate::git;
use crate::metrics::Metrics;
use crate::oidc::OidcVerifier;
use crate::pack::PackBuilder;
use crate::provider::{ProviderInstance, ProviderRegistry, RepoId};
use crate::queue::{BuildJob, EnqueueOutcome, JobQueueRef, JobState};
use crate::ref_store::{RefStore, migrate_legacy_refs};
use crate::remote_gc::{GcConfig, RemoteGc};
use crate::retention::Retention;
use crate::snapshot::SnapshotBuilder;
use crate::storage::StorageRef;
use crate::validation;
use crate::webhook::{EventKind, WebhookConfig};
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, DefaultBodyLimit, OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct ServerState {
    pub cas: Cas,
    pub storage: StorageRef,
    pub repo_root: PathBuf,
    pub ref_store: Arc<dyn RefStore>,
    pub provider_registry: ProviderRegistry,
    pub broker: Arc<dyn CredentialBroker>,
    pub token_hash: Option<String>,
    pub metrics: Arc<Metrics>,
    pub rate_limiter: RateLimiter,
    pub retention: Arc<Retention>,
    pub build_queue: JobQueueRef,
    pub build_queue_depth: Arc<AtomicUsize>,
    /// Waiters for in-flight background builds, keyed by `owner/repo/branch`. A
    /// `/sync` registers a oneshot here and enqueues a job only if it is the
    /// first waiter for that key (coalescing); the worker signals all waiters
    /// when the build finishes.
    pub build_waiters: BuildWaiters,
    pub oidc_verifier: Option<Arc<OidcVerifier>>,
    /// Per-repo mutexes so concurrent syncs for the same repo cannot corrupt
    /// the bare mirror directory.
    pub sync_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Last time each `owner/repo/branch` mirror was fetched. Used to skip a
    /// redundant `git fetch` on the resolve hot path when the mirror is fresh.
    pub mirror_freshness: Arc<std::sync::Mutex<HashMap<String, Instant>>>,
    /// How long a mirror fetch stays "fresh". Resolves within this window skip
    /// the fetch (`RIPCLONE_MIRROR_FRESH_TTL_SECS`, default 60s).
    pub mirror_fresh_ttl: Duration,
    /// Short-lived cache of complete ref responses, including signed URLs.
    /// This smooths repeated clone startup latency when signing or ref-store
    /// lookup has a cold tail.
    pub ref_response_cache: Arc<std::sync::Mutex<HashMap<String, CachedRefResponse>>>,
    /// Count of artifact GETs served, used only by the test-only fault injector.
    /// Per-server so tests don't leak state into each other.
    pub artifact_fetch_count: Arc<AtomicUsize>,
    /// Test-only fault injection: make the first N artifact GETs fail with 503.
    /// Read once from `RIPCLONE_TEST_FAIL_FIRST_FETCHES` at construction (0 =
    /// off), so the hot path never touches the environment in production.
    pub fail_first_fetches: usize,
    /// Cached `/readyz` result `(checked_at, ready)`. Bounds backend probe cost
    /// (S3 round-trips) and damps load-balancer flapping on a transient blip.
    pub readyz_cache: Arc<std::sync::Mutex<Option<(Instant, bool)>>>,
    /// Per-repo read authorization (AU1): proves the caller may read a private
    /// repo (public repos are anonymous). Used by every repo-read entry point
    /// before serving content or signing URLs, unless `require_repo_auth` is off.
    pub access_verifier: Arc<dyn AccessVerifier>,
    /// When true (default), private repos are gated by `access_verifier` on every
    /// read. Set false by `RIPCLONE_TRUST_GATEWAY=1` for a single-tenant
    /// self-host that fully trusts whoever holds the shared server token (the old
    /// behavior); then visibility falls back to the client-supplied header.
    pub require_repo_auth: bool,
    /// Webhook receiver config: per-provider secret + optional repo allowlist.
    /// Empty (no secrets) means every `/webhooks/{provider}` returns 503.
    pub webhook_config: Arc<WebhookConfig>,
}

impl ServerState {
    /// Assemble state for a standalone `ripclone-worker`. It uses the real
    /// durable backends but none of the HTTP-only features (auth, rate limiting,
    /// OIDC, fault injection) since it never serves requests — it only runs
    /// [`process_build_job`]. It builds its own provider registry + credential
    /// broker from the environment, exactly as the server does, so it can resolve
    /// upstream credentials for the repos it builds.
    pub fn for_worker(
        b: backends::Backends,
        queue: JobQueueRef,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let provider_registry = ProviderRegistry::load().context("load provider registry")?;
        let broker: Arc<dyn CredentialBroker> =
            Arc::new(StaticBroker::new(provider_registry.clone()));
        Ok(ServerState {
            cas: b.cas,
            storage: b.storage,
            repo_root: b.repo_root,
            ref_store: b.ref_store,
            provider_registry,
            broker,
            token_hash: None,
            metrics,
            rate_limiter: RateLimiter::new(60, 10.0),
            retention: b.retention,
            build_queue: queue,
            build_queue_depth: Arc::new(AtomicUsize::new(0)),
            build_waiters: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            oidc_verifier: None,
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            mirror_freshness: Arc::new(std::sync::Mutex::new(HashMap::new())),
            mirror_fresh_ttl: Duration::from_secs(60),
            ref_response_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            artifact_fetch_count: Arc::new(AtomicUsize::new(0)),
            fail_first_fetches: 0,
            readyz_cache: Arc::new(std::sync::Mutex::new(None)),
            // The worker never serves reads; a verifier is required by the type
            // but unused, and auth enforcement is irrelevant here.
            access_verifier: Arc::new(HttpAccessVerifier::new()),
            require_repo_auth: false,
            // The worker never serves HTTP, so it never receives webhooks.
            webhook_config: Arc::new(WebhookConfig::empty()),
        })
    }
}

/// Whether per-repo read authz is enforced. On by default (multi-tenant safe);
/// `RIPCLONE_TRUST_GATEWAY=1` turns it off for a single-tenant self-host that
/// trusts the shared server token as the only authz layer.
fn require_repo_auth_from_env() -> bool {
    !std::env::var("RIPCLONE_TRUST_GATEWAY")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Read the test-only fault-injection threshold once at startup. Logs loudly if
/// it is active so it can never silently degrade a production server.
fn fail_first_fetches_from_env() -> usize {
    let n = std::env::var("RIPCLONE_TEST_FAIL_FIRST_FETCHES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    if n > 0 {
        tracing::warn!(
            "TEST FAULT INJECTION ACTIVE: failing the first {n} artifact fetches \
             (RIPCLONE_TEST_FAIL_FIRST_FETCHES); this must NOT be set in production"
        );
    }
    n
}

#[derive(Clone)]
pub struct CachedRefResponse {
    response: RefResponse,
    inserted: Instant,
}

/// Simple in-memory token-bucket rate limiter keyed by real client IP.
/// The map is bounded and pruned periodically to avoid unbounded memory growth.
#[derive(Clone)]
pub struct RateLimiter {
    buckets: Arc<StdMutex<HashMap<String, (Instant, u32)>>>,
    max_burst: u32,
    restore_rate_per_sec: f64,
    max_entries: usize,
}

impl RateLimiter {
    pub fn new(max_burst: u32, restore_rate_per_sec: f64) -> Self {
        Self {
            buckets: Arc::new(StdMutex::new(HashMap::new())),
            max_burst,
            restore_rate_per_sec,
            max_entries: 10_000,
        }
    }

    pub fn check(&self, key: &str) -> bool {
        let now = Instant::now();
        // Recover from a poisoned mutex rather than wedging the server.
        let mut buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());

        // Prune stale entries before adding a new one.
        let stale_threshold = Duration::from_secs(3600);
        buckets.retain(|_, (last, _)| now.duration_since(*last) < stale_threshold);
        if buckets.len() >= self.max_entries && !buckets.contains_key(key) {
            // Map is full of live entries and this IP is new: evict the oldest.
            if let Some(oldest) = buckets
                .iter()
                .min_by_key(|(_, (last, _))| *last)
                .map(|(k, _)| k.clone())
            {
                buckets.remove(&oldest);
            }
        }

        let entry = buckets
            .entry(key.to_string())
            .or_insert_with(|| (now, self.max_burst));
        let elapsed = now.duration_since(entry.0).as_secs_f64();
        entry.1 =
            ((entry.1 as f64 + elapsed * self.restore_rate_per_sec) as u32).min(self.max_burst);
        entry.0 = now;
        if entry.1 == 0 {
            return false;
        }
        entry.1 -= 1;
        true
    }
}

#[derive(Deserialize)]
pub struct SyncRequest {
    #[serde(default = "default_branch_value")]
    pub branch: String,
    /// Optional git rev to build at instead of the branch tip (e.g. `HEAD~5` or
    /// a SHA). The branch is still fetched and used as the ref-store key; only
    /// the build commit is overridden. Useful for testing the incremental path
    /// (sync at HEAD~N, then HEAD~N-1) without waiting for upstream to advance.
    #[serde(default)]
    pub rev: Option<String>,
}

#[derive(Deserialize)]
pub struct RefQuery {
    /// Clonepack variant to return: "full" (all reachable history) or
    /// "shallow" (depth=1). Defaults to "full" for backward compatibility.
    #[serde(default = "default_clonepack_kind")]
    pub clonepack: String,
    /// Optional git rev to resolve instead of the branch tip (e.g. "HEAD~5").
    /// Pairs with `sync?rev=...`: clone the artifacts built for that commit.
    #[serde(default)]
    pub rev: Option<String>,
}

fn default_clonepack_kind() -> String {
    "full".to_string()
}

#[derive(Deserialize)]
pub struct CatRequest {
    pub path: String,
    #[serde(default = "default_branch_value")]
    pub branch: String,
}

#[derive(Deserialize)]
pub struct SizesRequest {
    #[serde(default = "default_branch_value")]
    pub branch: String,
}

#[derive(Deserialize)]
pub struct SnapshotRequest {
    #[serde(default = "default_branch_value")]
    pub branch: String,
    #[serde(default = "default_hot_files")]
    pub hot_files: usize,
}

#[derive(Deserialize)]
pub struct HotfilesRequest {
    #[serde(default = "default_branch_value")]
    pub branch: String,
    #[serde(default = "default_hot_files")]
    pub count: usize,
}

#[derive(Deserialize)]
pub struct BatchRequest {
    pub paths: Vec<String>,
    #[serde(default = "default_branch_value")]
    pub branch: String,
    pub commit: Option<String>,
}

#[derive(Deserialize)]
pub struct BuildRequest {
    pub owner: String,
    pub repo: String,
    pub commit: String,
    #[serde(rename = "ref")]
    pub ref_name: String,
}

#[derive(Serialize)]
pub struct BuildResponse {
    pub status: String,
    pub queue_depth: usize,
}

/// Waiters for in-flight background builds, keyed by `owner/repo/branch`.
pub type BuildWaiters = Arc<
    tokio::sync::Mutex<
        std::collections::HashMap<String, Vec<tokio::sync::oneshot::Sender<Result<(), String>>>>,
    >,
>;

fn default_branch_value() -> String {
    "HEAD".to_string()
}

/// Resolve a `{*rest}` path segment from `/v1/repos/{*rest}/...` into a
/// `(RepoId, ProviderInstance)` pair.
///
/// The first path segment MUST be a registered provider instance id; the
/// remainder is the opaque repo path. There is no legacy fallback: callers
/// must address repos as `/v1/repos/{provider}/{path}/...`, even for the
/// built-in `github` default instance.
fn resolve_repo_id<'a>(
    registry: &'a ProviderRegistry,
    rest: &str,
) -> Option<(RepoId, &'a ProviderInstance)> {
    let segments: Vec<&str> = rest.split('/').collect();
    if segments.len() < 2 {
        return None;
    }
    let provider_id = segments[0];
    let path = segments[1..].join("/");
    let provider = registry.get(provider_id)?;
    Some((
        RepoId {
            provider: provider.id.clone(),
            path,
        },
        provider,
    ))
}

/// Extract an upstream credential token from request headers.
///
/// `X-Upstream-Token` is the canonical header; `X-GitHub-Token` is accepted as a
/// back-compat alias for existing clients and scripts.
fn unknown_provider_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "unknown provider".to_string(),
        }),
    )
        .into_response()
}

fn upstream_token_from_headers(headers: &HeaderMap) -> Option<secrecy::SecretString> {
    headers
        .get("X-Upstream-Token")
        .or_else(|| headers.get("X-GitHub-Token"))
        .and_then(|v| v.to_str().ok())
        .map(|s| secrecy::SecretString::new(s.to_string().into()))
}

fn default_hot_files() -> usize {
    0
}

/// Validate an `owner` or `repo` path segment. GitHub identifiers are limited
/// to ASCII alphanumeric plus `.`, `-`, and `_`, must not be empty, and must
/// not contain path separators.
fn validate_repo_id(id: &str) -> Result<()> {
    if id.is_empty() {
        anyhow::bail!("repo identifier must not be empty");
    }
    if id.len() > 128 {
        anyhow::bail!("repo identifier too long: {}", id.len());
    }
    if id.contains('/') || id.contains('\\') || id.contains('\0') {
        anyhow::bail!("repo identifier contains path separator: {}", id);
    }
    if id == "." || id == ".." {
        anyhow::bail!("repo identifier cannot be '.' or '..'");
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
    {
        anyhow::bail!("repo identifier contains invalid characters: {}", id);
    }
    Ok(())
}

async fn repo_lock(
    locks: &Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    repo_id: &RepoId,
) -> Arc<tokio::sync::Mutex<()>> {
    let key = repo_id.storage_key();
    let mut map = locks.lock().await;
    map.entry(key)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn reject_invalid_repo_ids(owner: &str, repo: &str) -> Option<Response> {
    if let Err(e) = validate_repo_id(owner).and_then(|_| validate_repo_id(repo)) {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response(),
        );
    }
    None
}

#[derive(Clone, Serialize)]
pub struct RefResponse {
    pub owner: String,
    pub repo: String,
    /// Provider instance id (e.g. "github", "gitlab", "my-gitea").
    pub provider: String,
    /// Hostname of the upstream git provider.
    pub host: String,
    /// Canonical HTTPS origin URL for the repo.
    pub origin_url: String,
    pub branch: String,
    pub default_branch: String,
    pub commit: String,
    pub parent_commit: Option<String>,
    pub full_pack: String,
    pub clonepack_manifest: String,
    /// Signed URL for the clonepack manifest itself, if the backend supports it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clonepack_manifest_url: Option<String>,
    /// Metadata chunk hash (protobuf). The client uses this to verify the
    /// metadata bytes it downloads concurrently with the manifest.
    pub metadata_chunk: String,
    /// Signed URL for the metadata chunk, if the backend supports it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_chunk_url: Option<String>,
    /// Signed URL for each archive chunk. `None` entries fall back to the
    /// gateway's `/v1/artifacts/{hash}` endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for each chunk of the head-blobs pack, in order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_blobs_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for the optional head-blobs idx.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_blobs_idx_url: Option<String>,
    /// Signed URL for each editable pack, ordered to match `manifest.packs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pack_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for each editable pack's idx, ordered to match `manifest.packs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pack_idx_urls: Option<Vec<Option<String>>>,
    /// Signed URL for the pre-built multi-pack-index (`manifest.midx`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub midx_url: Option<String>,
    /// Signed URL for the concatenated idx bundle (`manifest.idx_bundle`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idx_bundle_url: Option<String>,
    /// True when the returned clonepack is a shallow (depth=1) snapshot.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub shallow: bool,
    /// True once the full clonepack's archive is built. The server publishes an
    /// editable clonepack first and adds the archive a moment later, so a files
    /// clone waits for this. Editable clones ignore it. Defaults true for older
    /// servers that always built the archive before publishing.
    #[serde(default = "default_true")]
    pub archive_ready: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Serialize)]
pub struct SnapshotResponse {
    pub owner: String,
    pub repo: String,
    pub branch: String,
    pub commit: String,
    pub snapshot_hash: String,
    pub size: u64,
    pub hot_files: usize,
}

#[derive(Serialize, Deserialize)]
pub struct RepoStatusResponse {
    pub owner: String,
    pub repo: String,
    pub refs: Vec<BranchStatusEntry>,
    pub total_bytes: u64,
    pub total_unique_bytes: u64,
    pub regions: Vec<RegionStorageEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct BranchStatusEntry {
    pub branch: String,
    pub commit: String,
    pub manifest: String,
    pub bytes: u64,
    pub unique_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub built_at: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct RegionStorageEntry {
    pub region: String,
    pub unique_bytes: u64,
}

pub fn build_app(state: ServerState) -> Router {
    let protected = Router::new()
        // Single catch-all route for all repo endpoints. The dispatcher parses
        // the legacy 2-segment GitHub form ("owner/repo/refs/main") and the
        // multi-provider form ("gitlab/group/sub/proj/sync") from the path.
        .route("/v1/repos/{*path}", get(dispatch_repos_get))
        .route("/v1/repos/{*path}", post(dispatch_repos_post))
        .route("/v1/packs/{hash}", get(get_pack))
        .route("/v1/objects/{sha}", get(get_object))
        .route("/v1/artifacts/{hash}", get(get_artifact))
        .route("/v1/archives/{hash}", get(get_artifact))
        .route("/v1/manifests/{hash}", get(get_artifact))
        // Single catch-all route for git smart-http endpoints.
        .route("/v1/git/{*path}", get(dispatch_git_get))
        .route("/v1/git/{*path}", post(dispatch_git_post))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .route_layer(middleware::from_fn(protocol_guard))
        .with_state(state.clone());

    let rate_limited = Router::new()
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .route("/v1/build", post(build_handler))
        // Provider-agnostic webhook receiver. Rate-limited like the rest, but
        // NOT behind `auth_middleware`: a webhook authenticates itself with the
        // provider HMAC over the raw body, not the server's bearer token.
        .route("/webhooks/{provider}", post(webhook_handler))
        .merge(protected)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ))
        .with_state(state.clone());

    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/version", get(version_handler))
        .merge(rate_limited)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

/// Maximum request body size accepted by the server. This bounds the
/// `git-upload-pack` body and any other large POST payload.
const MAX_REQUEST_BODY_BYTES: usize = 256 * 1024 * 1024;
const MAX_UPLOAD_PACK_BODY_BYTES: usize = 256 * 1024 * 1024;
/// Cap for a webhook request body. The HMAC can only be verified after the whole
/// body is buffered, so this bounds what an unauthenticated caller can make the
/// server hold before the signature is checked. GitHub's webhook payloads are
/// capped at ~25 MiB; this is comfortably above that and far below the global
/// request limit.
const MAX_WEBHOOK_BODY_BYTES: usize = 25 * 1024 * 1024;

/// Reject a client whose wire protocol is newer than this server understands,
/// with a clear 426 instead of a confusing downstream error. A missing header
/// (a legacy client) or an older/equal protocol is allowed, so this never breaks
/// existing clients. Compatibility is keyed on `PROTOCOL_VERSION`, not the build
/// version.
async fn protocol_guard(
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    if let Some(client_proto) = headers
        .get("x-ripclone-protocol")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u32>().ok())
        && client_proto > crate::PROTOCOL_VERSION
    {
        return (
            StatusCode::UPGRADE_REQUIRED,
            Json(ErrorResponse {
                error: format!(
                    "client protocol {client_proto} is newer than this server's {}; upgrade the server (or use an older ripclone)",
                    crate::PROTOCOL_VERSION
                ),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

async fn auth_middleware(
    State(state): State<ServerState>,
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    if let Some(expected) = &state.token_hash {
        let authorized = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|v| check_auth_header(v, expected))
            .unwrap_or(false);
        if !authorized {
            // Smart-HTTP clients (vanilla git) expect a Basic challenge so they
            // can retry with the credentials embedded in the URL.
            if path.starts_with("/v1/git/") {
                return (
                    StatusCode::UNAUTHORIZED,
                    [("WWW-Authenticate", r#"Basic realm="ripclone""#)],
                    Json(ErrorResponse {
                        error: "unauthorized".to_string(),
                    }),
                )
                    .into_response();
            }
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "unauthorized".to_string(),
                }),
            )
                .into_response();
        }
    }
    next.run(request).await
}

fn constant_time_eq_str(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

fn check_auth_header(header: &str, expected: &str) -> bool {
    if let Some(token) = header.strip_prefix("Ripclone ") {
        return constant_time_eq_str(token, expected);
    }
    if let Some(credentials) = header.strip_prefix("Basic ")
        && let Ok(decoded) =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, credentials)
        && let Ok(decoded) = String::from_utf8(decoded)
    {
        // Accept "<username>:<password>"; compare the password to the
        // expected hash so vanilla git can use
        // http://user:<hash>@host/... URLs.
        if let Some((_, password)) = decoded.split_once(':') {
            return constant_time_eq_str(password, expected);
        }
    }
    false
}

/// Trust a forwarded-for header for the rate-limit key. Off by default: the
/// header is client-spoofable, so only honor it when the operator has put a
/// reverse proxy directly in front (`RIPCLONE_TRUST_FORWARDED_FOR=1`). Read once.
fn trust_forwarded_for() -> bool {
    static TRUST: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRUST.get_or_init(|| {
        std::env::var("RIPCLONE_TRUST_FORWARDED_FOR")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Rate-limit bucket key for a request. Keying on the raw socket IP is useless
/// behind a reverse proxy (every request shares the proxy's IP → one global
/// bucket) and bypassable over IPv6 (a /64 gives 2^64 addresses, each a fresh
/// bucket). So: derive the client IP from the trusted forwarded-for header when
/// enabled, and collapse IPv6 to its /64 network so an attacker can't rotate
/// addresses within their allocation (AU2).
fn rate_limit_key(headers: &HeaderMap, socket: SocketAddr, trust_forwarded: bool) -> String {
    let ip = if trust_forwarded {
        headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            // Rightmost entry = the address our immediately-trusted proxy saw;
            // entries a client prepends are ignored. Assumes a single trusted
            // proxy directly in front.
            .and_then(|v| v.rsplit(',').next())
            .map(str::trim)
            .and_then(|s| s.parse::<IpAddr>().ok())
            .unwrap_or_else(|| socket.ip())
    } else {
        socket.ip()
    };
    normalize_ip_for_rate_limit(ip)
}

fn normalize_ip_for_rate_limit(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            // Collapse to the /64 network (the first four 16-bit groups).
            let s = v6.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
        }
    }
}

async fn rate_limit_middleware(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let key = rate_limit_key(request.headers(), addr, trust_forwarded_for());
    if !state.rate_limiter.check(&key) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse {
                error: "rate limit exceeded".to_string(),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// Public version endpoint. Reports the server's build version and the wire
/// protocol version it speaks, so a client can check compatibility without
/// authenticating. Compatibility is keyed on `protocol`, not the build version.
async fn version_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": crate::PROTOCOL_VERSION,
    }))
}

/// Readiness probe: 200 only when storage and the ref store are both reachable,
/// 503 otherwise (with the specific problems). Unlike `/healthz` (liveness),
/// this fails when a dependency is broken (e.g. the data volume is unmounted) so
/// a load balancer stops routing to a server that can't serve clones.
const READYZ_CACHE_TTL: Duration = Duration::from_secs(3);

async fn readyz(State(state): State<ServerState>) -> impl IntoResponse {
    // Serve a cached result within the TTL: bounds backend probe cost (e.g. S3
    // round-trips on this unauthenticated endpoint) and damps load-balancer
    // flapping on a single transient blip.
    if let Some((at, ready)) = *state.readyz_cache.lock().unwrap_or_else(|e| e.into_inner())
        && at.elapsed() < READYZ_CACHE_TTL
    {
        return readyz_response(ready);
    }

    let mut problems: Vec<String> = Vec::new();

    // The storage probe is synchronous (filesystem / S3); keep it off the async
    // worker.
    let storage = state.storage.clone();
    match tokio::task::spawn_blocking(move || storage.health()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => problems.push(format!("storage: {e:#}")),
        Err(e) => problems.push(format!("storage probe failed to run: {e}")),
    }

    if let Err(e) = state.ref_store.health().await {
        problems.push(format!("ref_store: {e:#}"));
    }

    let ready = problems.is_empty();
    if !ready {
        // Log details server-side; the public (unauthenticated) body stays
        // generic so internal paths aren't leaked.
        warn!("readiness check failed: {}", problems.join("; "));
    }
    *state.readyz_cache.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), ready));
    readyz_response(ready)
}

fn readyz_response(ready: bool) -> Response {
    if ready {
        (StatusCode::OK, Json(serde_json::json!({"status": "ready"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status": "not_ready"})),
        )
            .into_response()
    }
}

async fn metrics_handler(State(state): State<ServerState>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.prometheus(),
    )
}

#[derive(Deserialize)]
struct GitServiceQuery {
    service: String,
}

/// Smart-HTTP `info/refs` fallback. Advertises refs so a vanilla git client can
/// talk to ripclone when the archive-first path is not available.
async fn git_info_refs_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    query: GitServiceQuery,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if query.service != "git-upload-pack" {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "only git-upload-pack is supported".to_string(),
            }),
        )
            .into_response();
    }

    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let request_token = upstream_token_from_headers(&headers);
    let credential = state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref());
    // AU1: gate the vanilla-git read surface too (it serves the private repo's
    // refs/objects directly from the mirror).
    if let Err(resp) =
        authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
    {
        return resp;
    }
    let lock = repo_lock(&state.sync_locks, &repo_id).await;
    let _guard = lock.lock().await;
    if let Err(e) = ensure_mirror(
        &mirror_dir,
        &provider,
        &repo_id,
        "HEAD",
        credential.as_ref(),
    )
    .await
    {
        state.metrics.record_error();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("mirror sync failed: {}", e),
            }),
        )
            .into_response();
    }
    drop(_guard);

    match advertise_refs(&mirror_dir).await {
        Ok(body) => (
            StatusCode::OK,
            [(
                "content-type",
                "application/x-git-upload-pack-advertisement",
            )],
            body,
        )
            .into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("advertise-refs failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

async fn advertise_refs(mirror_dir: &std::path::Path) -> Result<Vec<u8>> {
    let mirror_dir = mirror_dir.to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("git")
            .arg("upload-pack")
            .arg("--advertise-refs")
            .arg(&mirror_dir)
            .output()
    })
    .await
    .context("advertise-refs task")?;

    let output = output.context("git upload-pack --advertise-refs")?;
    if !output.status.success() {
        anyhow::bail!(
            "git upload-pack --advertise-refs failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Smart-HTTP advertisement prefix.
    let mut body = Vec::new();
    body.extend_from_slice(b"001e# service=git-upload-pack\n0000");
    body.extend_from_slice(&output.stdout);
    Ok(body)
}

/// Smart-HTTP `git-upload-pack` RPC fallback. Pipes the client's request body
/// through `git upload-pack --stateless-rpc` on the local bare mirror.
async fn git_upload_pack_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    body: Body,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let request_token = upstream_token_from_headers(&headers);
    let credential = state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref());
    // AU1: gate the vanilla-git upload-pack read surface.
    if let Err(resp) =
        authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
    {
        return resp;
    }
    let lock = repo_lock(&state.sync_locks, &repo_id).await;
    let _guard = lock.lock().await;
    if let Err(e) = ensure_mirror(
        &mirror_dir,
        &provider,
        &repo_id,
        "HEAD",
        credential.as_ref(),
    )
    .await
    {
        state.metrics.record_error();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("mirror sync failed: {}", e),
            }),
        )
            .into_response();
    }
    drop(_guard);

    let bytes = match axum::body::to_bytes(body, MAX_UPLOAD_PACK_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            state.metrics.record_error();
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("read body failed: {}", e),
                }),
            )
                .into_response();
        }
    };

    match upload_pack_rpc(&mirror_dir, bytes).await {
        Ok(output) => (
            StatusCode::OK,
            [("content-type", "application/x-git-upload-pack-result")],
            output,
        )
            .into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("upload-pack rpc failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

async fn dispatch_repos_get(
    Path(path): Path<String>,
    Query(params): Query<RefQuery>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    if let Some((repo_path, branch)) = path.rsplit_once("/refs/") {
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return get_ref_inner(
            repo_id,
            provider.clone(),
            branch.to_string(),
            params,
            headers,
            state,
        )
        .await;
    }

    if path.ends_with("/status") {
        let repo_path = path.strip_suffix("/status").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<RepoStatusQuery>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response();
            }
        };
        return repo_status_inner(repo_id, provider.clone(), query, headers, state).await;
    }

    if path.ends_with("/cat") {
        let repo_path = path.strip_suffix("/cat").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<CatRequest>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response();
            }
        };
        return cat_file_inner(repo_id, provider.clone(), query, state).await;
    }

    if path.ends_with("/sizes") {
        let repo_path = path.strip_suffix("/sizes").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<SizesRequest>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response();
            }
        };
        return file_sizes_inner(repo_id, provider.clone(), query, state).await;
    }

    if path.ends_with("/hotfiles") {
        let repo_path = path.strip_suffix("/hotfiles").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<HotfilesRequest>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response();
            }
        };
        return get_hotfiles_inner(repo_id, provider.clone(), query, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn dispatch_repos_post(
    Path(path): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
    body: Body,
) -> impl IntoResponse {
    if path.ends_with("/sync") {
        let repo_path = path.strip_suffix("/sync").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<SyncRequest>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid sync request: {}", e),
                    }),
                )
                    .into_response();
            }
        };
        return sync_repo_inner(repo_id, provider.clone(), query, headers, state).await;
    }

    if path.ends_with("/snapshot") {
        let repo_path = path.strip_suffix("/snapshot").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<SnapshotRequest>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response();
            }
        };
        return create_snapshot_inner(repo_id, provider.clone(), query, headers, state).await;
    }

    if path.ends_with("/batch") {
        let repo_path = path.strip_suffix("/batch").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let bytes = match axum::body::to_bytes(body, MAX_REQUEST_BODY_BYTES).await {
            Ok(b) => b,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("read body failed: {}", e),
                    }),
                )
                    .into_response();
            }
        };
        let body: BatchRequest = match serde_json::from_slice(&bytes) {
            Ok(b) => b,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid batch request: {}", e),
                    }),
                )
                    .into_response();
            }
        };
        return batch_files_inner(repo_id, provider.clone(), body, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn dispatch_git_get(
    Path(path): Path<String>,
    Query(query): Query<GitServiceQuery>,
    headers: HeaderMap,
    State(state): State<ServerState>,
) -> Response {
    if path.ends_with("/info/refs") {
        let repo_path = path.strip_suffix("/info/refs").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return git_info_refs_inner(repo_id, provider.clone(), query, headers, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn dispatch_git_post(
    Path(path): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    body: Body,
) -> Response {
    if path.ends_with("/git-upload-pack") {
        let repo_path = path.strip_suffix("/git-upload-pack").unwrap();
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return git_upload_pack_inner(repo_id, provider.clone(), body, headers, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn upload_pack_rpc(mirror_dir: &std::path::Path, input: Bytes) -> Result<Vec<u8>> {
    let mirror_dir = mirror_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut child = std::process::Command::new("git")
            .arg("upload-pack")
            .arg("--stateless-rpc")
            .arg(&mirror_dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("spawn git upload-pack --stateless-rpc")?;

        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin.write_all(&input)?;
        }

        let output = child.wait_with_output().context("wait for upload-pack")?;
        if !output.status.success() {
            anyhow::bail!(
                "git upload-pack --stateless-rpc failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(output.stdout)
    })
    .await
    .context("upload-pack rpc task")?
}

async fn ensure_mirror(
    mirror_dir: &std::path::Path,
    provider: &ProviderInstance,
    repo_id: &RepoId,
    branch: &str,
    credential: Option<&secrecy::SecretString>,
) -> Result<()> {
    let mirror_dir = mirror_dir.to_path_buf();
    let provider = provider.clone();
    let repo_id = repo_id.clone();
    let branch = branch.to_string();
    let credential = credential.cloned();
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror(
            &mirror_dir,
            &provider,
            &repo_id,
            &branch,
            credential.as_ref(),
        )
    })
    .await
    .context("ensure mirror task")?
}

/// True if the mirror for `key` (`owner/repo/branch`) was fetched within the
/// freshness TTL, so a resolve can skip the `git fetch`.
fn mirror_is_fresh(state: &ServerState, key: &str) -> bool {
    state
        .mirror_freshness
        .lock()
        .unwrap()
        .get(key)
        .map(|t| t.elapsed() < state.mirror_fresh_ttl)
        .unwrap_or(false)
}

/// Record that the mirror for `key` was just fetched. Prunes expired entries so
/// the map stays bounded by the set of refs active within the TTL.
fn stamp_mirror_fresh(state: &ServerState, key: &str) {
    let ttl = state.mirror_fresh_ttl;
    let mut map = state.mirror_freshness.lock().unwrap();
    map.retain(|_, t| t.elapsed() < ttl);
    map.insert(key.to_string(), Instant::now());
}

const REF_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(30);

fn ref_response_cache_key(repo_id: &RepoId, branch: &str, clonepack: &str) -> String {
    format!("{}\0{branch}\0{clonepack}", repo_id.storage_key())
}

fn ref_response_cache_ttl(state: &ServerState) -> Duration {
    std::cmp::min(REF_RESPONSE_CACHE_TTL, state.mirror_fresh_ttl)
}

fn cached_ref_response(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
    clonepack: &str,
) -> Option<RefResponse> {
    let ttl = ref_response_cache_ttl(state);
    if ttl.is_zero() {
        return None;
    }
    let key = ref_response_cache_key(repo_id, branch, clonepack);
    let mut cache = state
        .ref_response_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    cache.retain(|_, cached| cached.inserted.elapsed() < ttl);
    cache.get(&key).map(|cached| cached.response.clone())
}

fn cache_ref_response(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
    clonepack: &str,
    response: &RefResponse,
) {
    let ttl = ref_response_cache_ttl(state);
    if ttl.is_zero() {
        return;
    }
    let key = ref_response_cache_key(repo_id, branch, clonepack);
    let mut cache = state
        .ref_response_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    cache.retain(|_, cached| cached.inserted.elapsed() < ttl);
    cache.insert(
        key,
        CachedRefResponse {
            response: response.clone(),
            inserted: Instant::now(),
        },
    );
}

fn invalidate_ref_response_cache(state: &ServerState, repo_id: &RepoId, branch: &str) {
    let prefix = format!("{}\0{branch}\0", repo_id.storage_key());
    let mut cache = state
        .ref_response_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    cache.retain(|key, _| !key.starts_with(&prefix));
}

async fn get_ref_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    branch: String,
    params: RefQuery,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&branch)) {
        return resp;
    }
    if let Some(rev) = params.rev.as_deref()
        && let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(rev))
    {
        return resp;
    }
    state.metrics.record_ref_lookup();
    let key = format!("{}/{}", repo_id.storage_key(), branch);
    // Optional build-commit override: resolve this rev instead of the branch tip
    // so a clone can fetch the artifacts a `sync?rev=...` built. The response
    // cache is bypassed for rev requests (a testing path, low volume).
    let resolve_target = params.rev.clone().unwrap_or_else(|| branch.clone());

    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let request_token = upstream_token_from_headers(&headers);
    let credential = state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref());

    // AU1: authorize the caller for this repo BEFORE the cache-hit return below,
    // so a cached private repo is never served to a caller without access.
    let private =
        match authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    // Serialize syncs for this repo so concurrent fetches do not corrupt the
    // bare mirror directory. Acquiring the lock also means any in-progress sync
    // for this repo has finished by the time we proceed.
    let fresh_key = format!("{}/{}", repo_id.storage_key(), branch);
    let lock = repo_lock(&state.sync_locks, &repo_id).await;
    let _guard = lock.lock().await;
    if params.rev.is_none()
        && let Some(resp) = cached_ref_response(&state, &repo_id, &branch, &params.clonepack)
    {
        return (StatusCode::OK, Json(resp)).into_response();
    }
    // Skip the `git fetch` when the mirror was refreshed within the TTL — by a
    // recent resolve, or by the sync we just waited on while holding the lock.
    if !mirror_is_fresh(&state, &fresh_key) {
        if let Err(e) = ensure_mirror(
            &mirror_dir,
            &provider,
            &repo_id,
            &branch,
            credential.as_ref(),
        )
        .await
        {
            state.metrics.record_error();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("mirror sync failed: {}", e),
                }),
            )
                .into_response();
        }
        stamp_mirror_fresh(&state, &fresh_key);
    }
    drop(_guard);

    // Resolve HEAD to the default branch for artifact lookup and the response.
    // The client may request refs/HEAD; we still need to hand back the artifacts
    // stored under the concrete default branch name.
    let default_branch = git::default_branch(&mirror_dir).unwrap_or_else(|_| "HEAD".to_string());
    let effective_branch = if branch == "HEAD" {
        default_branch.clone()
    } else {
        branch.clone()
    };

    // Load the stored info, if any. A rev request loads the rolling test key the
    // matching `sync?rev=...` stored under (isolated from the real branch entry).
    let store_key = ref_store_key(&effective_branch, params.rev.as_deref());
    let fallback = state
        .ref_store
        .load_branch(&repo_id, &store_key)
        .await
        .ok()
        .flatten();

    let resolve_target2 = resolve_target.clone();
    let mirror_dir2 = mirror_dir.clone();
    match tokio::task::spawn_blocking(move || git::resolve_commit(&mirror_dir2, &resolve_target2))
        .await
    {
        Ok(Ok(commit)) => {
            // Only reuse stored artifact hashes when they match the resolved
            // commit; otherwise we would hand out signed URLs for stale chunks.
            let fallback = fallback.filter(|info| info.commit == commit);
            let info = fallback.unwrap_or_else(|| RefInfo {
                commit: commit.clone(),
                parent_commit: None,
                default_branch: default_branch.clone(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                head_blobs_pack: String::new(),
                head_blobs_idx: String::new(),
                head_blobs_chunks: Vec::new(),
                packs: Vec::new(),
                prebuilt_index: String::new(),
                archive: String::new(),
                manifest: String::new(),
                full_pack: String::new(),
                clonepack_manifest: String::new(),
                metadata_chunk: String::new(),
                archive_chunks: Vec::new(),
                full_clonepack: crate::ClonepackArtifacts::default(),
                shallow_clonepack: crate::ClonepackArtifacts::default(),
                history_levels: Vec::new(),
                head_buckets: Vec::new(),
                head_base_commit: String::new(),
                head_base_packs: Vec::new(),
                archive_frames: Vec::new(),
                build_status: None,
                synced_at: None,
            });
            let resp = ref_response(
                &repo_id,
                &provider,
                effective_branch.clone(),
                &info,
                &state.storage,
                &params.clonepack,
                private,
            );
            if info.build_status.is_none() && params.rev.is_none() {
                cache_ref_response(
                    &state,
                    &repo_id,
                    &effective_branch,
                    &params.clonepack,
                    &resp,
                );
            }
            (StatusCode::OK, Json(resp)).into_response()
        }
        _ => {
            state.metrics.record_error();
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("ref not found: {}", key),
                }),
            )
                .into_response()
        }
    }
}

/// TTL for signed chunk URLs returned in ref responses. Public repos get a long
/// window (20 min) so slow clones and large archives finish. Private repos get a
/// short window (5 min) so a leaked signed URL — or a caller who later loses
/// GitHub access — only works briefly; this is the revocation window for the
/// direct-to-storage path that bypasses the gateway. The cloud gateway tags the
/// request with `X-Ripclone-Visibility`; absent (e.g. a self-hosted client
/// talking to the backend directly) means the public TTL. Both are env-tunable.
const REF_SIGNED_URL_TTL_PUBLIC_SECS: u64 = 1200;
const REF_SIGNED_URL_TTL_PRIVATE_SECS: u64 = 300;

fn ref_signed_url_ttl(private: bool) -> Duration {
    if private {
        Duration::from_secs(env_bytes(
            "RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS",
            REF_SIGNED_URL_TTL_PRIVATE_SECS,
        ))
    } else {
        Duration::from_secs(env_bytes(
            "RIPCLONE_SIGNED_URL_TTL_SECS",
            REF_SIGNED_URL_TTL_PUBLIC_SECS,
        ))
    }
}

fn ref_response(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    info: &RefInfo,
    storage: &crate::storage::StorageRef,
    clonepack_kind: &str,
    private: bool,
) -> RefResponse {
    let ttl = ref_signed_url_ttl(private);
    let artifacts = if clonepack_kind == "shallow" && !info.shallow_clonepack.manifest.is_empty() {
        &info.shallow_clonepack
    } else {
        &info.full_clonepack
    };

    // Fallback to the legacy top-level fields if the new struct is empty (older
    // stored refs).
    let clonepack_manifest = if artifacts.manifest.is_empty() {
        info.clonepack_manifest.clone()
    } else {
        artifacts.manifest.clone()
    };
    let metadata_chunk = if artifacts.metadata_chunk.is_empty() {
        info.metadata_chunk.clone()
    } else {
        artifacts.metadata_chunk.clone()
    };

    let clonepack_manifest_url = signed_url(storage, ttl, &clonepack_manifest);
    let metadata_chunk_url = signed_url(storage, ttl, &metadata_chunk);
    let archive_chunk_urls = if info.archive_chunks.is_empty() {
        None
    } else {
        let urls: Vec<Option<String>> = info
            .archive_chunks
            .iter()
            .map(|h| signed_url(storage, ttl, h))
            .collect();
        if urls.iter().all(|u| u.is_none()) {
            None
        } else {
            Some(urls)
        }
    };

    let head_blobs_chunk_urls = if info.head_blobs_chunks.is_empty() {
        None
    } else {
        let urls: Vec<Option<String>> = info
            .head_blobs_chunks
            .iter()
            .map(|h| signed_url(storage, ttl, h))
            .collect();
        if urls.iter().all(|u| u.is_none()) {
            None
        } else {
            Some(urls)
        }
    };
    let head_blobs_idx_url = signed_url(storage, ttl, &info.head_blobs_idx);

    // Sign each editable pack + idx so the client fetches them straight from
    // object storage. `None` entries (e.g. local backend) fall back to the
    // gateway. Ordered to match the manifest's `packs` list.
    let (pack_chunk_urls, pack_idx_urls) = if info.packs.is_empty() {
        (None, None)
    } else {
        let packs: Vec<Option<String>> = info
            .packs
            .iter()
            .map(|p| signed_url(storage, ttl, &p.pack))
            .collect();
        let idxs: Vec<Option<String>> = info
            .packs
            .iter()
            .map(|p| signed_url(storage, ttl, &p.idx))
            .collect();
        let packs = if packs.iter().all(Option::is_none) {
            None
        } else {
            Some(packs)
        };
        let idxs = if idxs.iter().all(Option::is_none) {
            None
        } else {
            Some(idxs)
        };
        (packs, idxs)
    };

    // Sign the pre-built MIDX for the selected variant so the client installs it
    // directly instead of running `git multi-pack-index write`.
    let midx_url = signed_url(storage, ttl, &artifacts.midx);
    // Sign the idx bundle so the client fetches all idx in one GET.
    let idx_bundle_url = signed_url(storage, ttl, &artifacts.idx_bundle);

    // The served commit is the selected variant's commit — which may differ from
    // RefInfo.commit during two-phase publish (depth=0 serves the previous commit
    // until the new full history is built). The client writes HEAD to this, so it
    // must match the installed objects.
    let served_commit = if artifacts.commit.is_empty() {
        info.commit.clone()
    } else {
        artifacts.commit.clone()
    };

    let (owner, repo) = repo_id
        .github_owner_repo()
        .map(|(o, r)| (o.to_string(), r.to_string()))
        .unwrap_or_else(|| (repo_id.provider.as_str().to_string(), repo_id.path.clone()));
    let origin_url = provider.clone_url(&repo_id.path);
    RefResponse {
        owner,
        repo,
        provider: provider.id.as_str().to_string(),
        host: provider.host.clone(),
        origin_url,
        branch,
        default_branch: info.default_branch.clone(),
        commit: served_commit,
        parent_commit: info.parent_commit.clone(),
        full_pack: info.full_pack.clone(),
        clonepack_manifest,
        clonepack_manifest_url,
        metadata_chunk,
        metadata_chunk_url,
        archive_chunk_urls,
        head_blobs_chunk_urls,
        head_blobs_idx_url,
        pack_chunk_urls,
        pack_idx_urls,
        midx_url,
        idx_bundle_url,
        shallow: clonepack_kind == "shallow",
        archive_ready: !info.archive_chunks.is_empty(),
    }
}

fn signed_url(storage: &crate::storage::StorageRef, ttl: Duration, hash: &str) -> Option<String> {
    if hash.is_empty() {
        return None;
    }
    storage.signed_url(hash, ttl)
}

/// Single-tenant trust mode only: the client tags a request with the visibility
/// it resolved. Absent → treat as public. This is advisory and trusted ONLY when
/// `require_repo_auth` is off; the enforced path derives visibility from the
/// provider via [`authorize_repo_read`] instead.
fn visibility_is_private(headers: &HeaderMap) -> bool {
    headers
        .get("x-ripclone-visibility")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("private"))
        .unwrap_or(false)
}

/// 403 for a caller that may not read this repo.
fn forbidden_repo_response() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(ErrorResponse {
            error: "not authorized for this repository".to_string(),
        }),
    )
        .into_response()
}

/// Per-repo read authorization gate (AU1). Every repo-read entry point calls
/// this before serving content or signing URLs. On success it returns whether
/// the repo is private (for signed-URL TTL); on failure it returns a 403 the
/// caller must propagate.
///
/// Enforced path (`require_repo_auth`): public repos are served anonymously,
/// private repos require the caller's own credential to prove read access
/// against the provider (cached). This is what stops a holder of the shared
/// server token from reading an already-cached private repo it has no access to.
/// Trust mode (`RIPCLONE_TRUST_GATEWAY=1`): the gate is skipped and visibility
/// comes from the client header (single-tenant self-host behavior).
async fn authorize_repo_read(
    state: &ServerState,
    provider: &ProviderInstance,
    repo_id: &RepoId,
    credential: Option<&secrecy::SecretString>,
    headers: &HeaderMap,
) -> Result<bool, Response> {
    if !state.require_repo_auth {
        return Ok(visibility_is_private(headers));
    }
    match state
        .access_verifier
        .verify(provider, &repo_id.path, credential)
        .await
    {
        AccessDecision::Public => Ok(false),
        AccessDecision::PrivateAuthorized => Ok(true),
        AccessDecision::Denied => Err(forbidden_repo_response()),
    }
}

#[derive(Deserialize, Default)]
struct RepoStatusQuery {
    #[serde(default)]
    public: bool,
    #[serde(default)]
    fork_of: Option<String>,
}

async fn repo_status_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    query: RepoStatusQuery,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    // AU1: status reveals a private repo's existence, commit, and byte sizes.
    let request_token = upstream_token_from_headers(&headers);
    let credential = state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref());
    if let Err(resp) =
        authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
    {
        return resp;
    }
    match build_repo_status(&state, &repo_id, query.public, query.fork_of.as_deref()).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("status failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

fn manifest_chunk_refs(manifest: &ClonepackManifest) -> Vec<&ChunkRef> {
    let mut refs = Vec::new();
    if let Some(ref meta) = manifest.metadata_chunk {
        refs.push(meta);
    }
    refs.extend(&manifest.archive_chunks);
    refs.extend(&manifest.head_blobs_chunks);
    if let Some(ref idx) = manifest.head_blobs_idx {
        refs.push(idx);
    }
    for pack in &manifest.packs {
        if let Some(ref pack_chunk) = pack.pack {
            refs.push(pack_chunk);
        }
        if let Some(ref idx_chunk) = pack.idx {
            refs.push(idx_chunk);
        }
    }
    refs
}

fn record_chunk(unique_chunks: &mut HashMap<String, u64>, hash: &str, len: u64) {
    if hash.is_empty() || len == 0 {
        return;
    }
    unique_chunks.insert(hash.to_string(), len);
}

fn collect_manifest_hashes(info: &crate::RefInfo) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    if !info.full_clonepack.manifest.is_empty() && seen.insert(info.full_clonepack.manifest.clone())
    {
        out.push(info.full_clonepack.manifest.clone());
    }
    if !info.shallow_clonepack.manifest.is_empty()
        && seen.insert(info.shallow_clonepack.manifest.clone())
    {
        out.push(info.shallow_clonepack.manifest.clone());
    }
    if !info.clonepack_manifest.is_empty() && seen.insert(info.clonepack_manifest.clone()) {
        out.push(info.clonepack_manifest.clone());
    }
    out
}

async fn build_repo_status(
    state: &ServerState,
    repo_id: &RepoId,
    public: bool,
    fork_of: Option<&str>,
) -> Result<RepoStatusResponse> {
    let branches = state.ref_store.list_branches(repo_id).await?;
    let mut refs = Vec::new();
    let mut unique_chunks: HashMap<String, u64> = HashMap::new();

    for branch in branches {
        let Some(info) = state.ref_store.load_branch(repo_id, &branch).await? else {
            continue;
        };

        let manifest_hashes = collect_manifest_hashes(&info);
        if manifest_hashes.is_empty() && info.history_levels.is_empty() {
            continue;
        }

        let mut ref_bytes = 0u64;

        // Manifest-based clonepack variants (shallow, full, legacy). Each
        // manifest is itself a stored artifact and references chunks.
        for manifest_hash in manifest_hashes {
            // Read the manifest from the authoritative storage backend rather
            // than the local CAS, because remote backends remove local copies
            // after upload to save disk.
            let manifest_bytes = state.storage.get(&manifest_hash)?;
            let manifest_len = manifest_bytes.len() as u64;
            record_chunk(&mut unique_chunks, &manifest_hash, manifest_len);
            ref_bytes += manifest_len;

            let manifest = ClonepackManifest::decode(manifest_bytes.as_slice())
                .context("decode clonepack manifest for status")?;
            for chunk in manifest_chunk_refs(&manifest) {
                ref_bytes += chunk.len;
                record_chunk(&mut unique_chunks, &hash_to_hex(&chunk.hash), chunk.len);
            }
        }

        // LSM sealed history levels: each level stores its own pack/idx hashes.
        for level in &info.history_levels {
            for pack in &level.packs {
                if !pack.pack.is_empty() {
                    record_chunk(&mut unique_chunks, &pack.pack, pack.pack_len);
                    ref_bytes += pack.pack_len;
                }
                if !pack.idx.is_empty() {
                    record_chunk(&mut unique_chunks, &pack.idx, pack.idx_len);
                    ref_bytes += pack.idx_len;
                }
            }
        }

        let built_at = info.synced_at.and_then(|secs| {
            chrono::DateTime::from_timestamp(secs as i64, 0).map(|dt| dt.to_rfc3339())
        });

        // Report the primary manifest: prefer full, then shallow, then legacy.
        let primary_manifest = if !info.full_clonepack.manifest.is_empty() {
            info.full_clonepack.manifest.clone()
        } else if !info.shallow_clonepack.manifest.is_empty() {
            info.shallow_clonepack.manifest.clone()
        } else {
            info.clonepack_manifest.clone()
        };

        // Public forks of public projects are free; everything else pays its
        // own logical bytes for now.
        let is_public_fork = public && fork_of.is_some();
        let branch_unique_bytes = if is_public_fork { 0 } else { ref_bytes };

        refs.push(BranchStatusEntry {
            branch,
            commit: info.commit,
            manifest: primary_manifest,
            bytes: ref_bytes,
            unique_bytes: branch_unique_bytes,
            built_at,
        });
    }

    refs.sort_by(|a, b| a.branch.cmp(&b.branch));
    let total_bytes = unique_chunks.values().sum();
    // TODO: cross-repo fork-network dedup for private repos. For now, public
    // forks are free and everything else pays logical repo bytes.
    let is_public_fork = public && fork_of.is_some();
    let total_unique_bytes = if is_public_fork { 0 } else { total_bytes };
    let regions = state
        .storage
        .regions()
        .into_iter()
        .map(|region| RegionStorageEntry {
            region,
            unique_bytes: total_unique_bytes,
        })
        .collect();

    let (owner, repo) = repo_id
        .github_owner_repo()
        .map(|(o, r)| (o.to_string(), r.to_string()))
        .unwrap_or_default();
    Ok(RepoStatusResponse {
        owner,
        repo,
        refs,
        total_bytes,
        total_unique_bytes,
        regions,
    })
}

/// Target size for each chunk of the head-blobs pack on the client fetch path.
/// 8 MB matches the archive chunk target and keeps per-request overhead low.
const HEAD_BLOBS_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Split a pack file into content-addressed chunks and store them in the CAS.
/// Returns the `ChunkRef`s in the order needed to reconstruct the pack.
fn split_and_store_pack(cas: &crate::cas::Cas, pack: &[u8]) -> Result<Vec<ChunkRef>> {
    let mut refs = Vec::new();
    for chunk in pack.chunks(HEAD_BLOBS_CHUNK_SIZE) {
        let hash = cas.put(chunk)?;
        refs.push(ChunkRef {
            hash: hash_from_hex(&hash)?,
            len: chunk.len() as u64,
        });
    }
    Ok(refs)
}

/// Outcome of placing a sync on the build queue. The caller decides whether to
/// wait for the build (`/sync`) or return immediately (the webhook receiver).
enum SyncEnqueued {
    /// In-process queue: await `rx` for the build result; the worker signals it
    /// on completion. Dropping `rx` makes the enqueue fire-and-forget.
    InProc {
        rx: tokio::sync::oneshot::Receiver<Result<(), String>>,
    },
    /// Cross-process queue: poll `job_id` until the worker reports completion.
    CrossProc { job_id: crate::queue::JobId },
}

/// Why a sync could not be queued.
enum EnqueueError {
    /// Queue at capacity (HTTP 503).
    Full,
    /// `?rev=` override is not supported on the cross-process queue (HTTP 501).
    RevUnsupported,
    /// The queue returned no job id to poll (HTTP 500).
    NoJobId,
    /// The enqueue call itself failed (HTTP 503).
    QueueError(String),
}

/// Place a sync build on the queue: coalesce concurrent requests for the same
/// `(repo, branch, rev)` onto one build and bump the queue-depth metric. This is
/// the single enqueue path shared by `POST /sync` and the webhook receiver — do
/// not duplicate build logic. The caller awaits the returned handle (`/sync`) or
/// drops it for fire-and-forget warming (webhook).
async fn enqueue_sync(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
    rev: Option<&str>,
    credential: Option<secrecy::SecretString>,
) -> Result<SyncEnqueued, EnqueueError> {
    if state.build_queue.inproc_wait() {
        // In-process queue: coalesce via build_waiters; the same-process worker
        // signals completion on a oneshot. Include the rev override in the
        // coalescing key so syncs for different build commits don't share one
        // build. This key must match the one the worker computes.
        let key = format!("{}/{branch}#{}", repo_id.storage_key(), rev.unwrap_or(""));
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let first = {
            let mut w = state.build_waiters.lock().await;
            let v = w.entry(key.clone()).or_default();
            let was_empty = v.is_empty();
            v.push(tx);
            was_empty
        };
        if first {
            // Mirror the /build handler: the worker decrements the metrics gauge
            // for every job it drains, so every enqueue must increment it (else
            // the gauge underflows). The local queue owns the build_queue_depth
            // counter (enqueue +1, worker -1).
            state.metrics.record_build_queued();
            let job = BuildJob {
                repo_id: repo_id.clone(),
                branch: branch.to_string(),
                rev: rev.map(|s| s.to_string()),
                credential,
            };
            let full = match state.build_queue.enqueue(job).await {
                Ok(enq) => enq.outcome == EnqueueOutcome::Full,
                Err(_) => true,
            };
            if full {
                state.metrics.record_build_rejected();
                state.build_waiters.lock().await.remove(&key);
                return Err(EnqueueError::Full);
            }
        }
        Ok(SyncEnqueued::InProc { rx })
    } else {
        // Cross-process queue: the build runs in a separate ripclone-worker. The
        // rev override is not carried across the queue (not persisted; the worker
        // builds the branch tip), so honoring `?rev=` here would build the wrong
        // commit. Reject it explicitly rather than mis-build.
        if rev.is_some() {
            return Err(EnqueueError::RevUnsupported);
        }
        // The per-request upstream credential rides with the job: the queue
        // persists it (base64) and the worker uses it for the mirror fetch, so a
        // private repo the worker has no standing token for still builds.
        let job = BuildJob {
            repo_id: repo_id.clone(),
            branch: branch.to_string(),
            rev: None,
            credential,
        };
        let enq = state
            .build_queue
            .enqueue(job)
            .await
            .map_err(|e| EnqueueError::QueueError(e.to_string()))?;
        // Only count a genuinely new job (not a coalesced duplicate).
        if enq.outcome == EnqueueOutcome::Enqueued {
            state.metrics.record_build_queued();
        }
        if enq.outcome == EnqueueOutcome::Full {
            state.metrics.record_build_rejected();
            return Err(EnqueueError::Full);
        }
        // The SQL queue always returns a job id to poll; treat its absence as an
        // internal error rather than spinning to the deadline.
        let Some(job_id) = enq.job_id else {
            return Err(EnqueueError::NoJobId);
        };
        Ok(SyncEnqueued::CrossProc { job_id })
    }
}

#[derive(Serialize)]
struct WebhookAccepted {
    ok: bool,
}

#[derive(Serialize)]
struct WebhookIgnored {
    ignored: &'static str,
}

/// Acknowledge an event we deliberately don't act on. Always `200` so the
/// provider doesn't retry a delivery we simply chose to ignore.
fn webhook_ignored(reason: &'static str) -> Response {
    (StatusCode::OK, Json(WebhookIgnored { ignored: reason })).into_response()
}

/// `POST /webhooks/{provider}` — provider-agnostic webhook receiver.
///
/// verify (HMAC over the RAW body) → normalize → enqueue. Responds 2xx fast and
/// lets the build run async on the shared queue. Fail-closed: no configured
/// secret ⇒ 503; bad signature ⇒ 401. The payload is trusted only for routing
/// (which repo / ref to warm), never to choose a credential or escalate.
async fn webhook_handler(
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    body: Body,
) -> Response {
    // Resolve the configured provider instance from the path.
    let Some(provider) = state.provider_registry.get(&provider_id).cloned() else {
        return unknown_provider_response();
    };
    // Phase 1: only GitHub has a webhook adapter; other kinds are follow-ups.
    let Some(adapter) = crate::webhook::provider_for(provider.kind) else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                error: format!(
                    "webhooks not yet implemented for provider kind '{}'",
                    provider.kind.as_str()
                ),
            }),
        )
            .into_response();
    };
    // Fail closed: no configured secret for this provider ⇒ 503. Never process
    // an unverified webhook.
    let Some(secret) = state.webhook_config.secret(provider.id.as_str()).cloned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!(
                    "no webhook secret configured for provider '{}'",
                    provider.id
                ),
            }),
        )
            .into_response();
    };
    // Read the RAW body before any JSON parse — the HMAC covers these exact
    // bytes. Cap the buffer at `MAX_WEBHOOK_BODY_BYTES`: the signature can only
    // be checked after the whole body is buffered, so an *unauthenticated*
    // caller must not be able to make us hold the full 256 MiB request limit.
    // GitHub caps webhook payloads well under this.
    let raw = match axum::body::to_bytes(body, MAX_WEBHOOK_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: format!("webhook body too large or unreadable: {e}"),
                }),
            )
                .into_response();
        }
    };
    // Verify the signature in constant time over the raw bytes.
    if !adapter.verify(&headers, &raw, secret.expose_secret()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid webhook signature".to_string(),
            }),
        )
            .into_response();
    }
    // Normalize. Unhandled events parse to None and are acknowledged as ignored.
    let Some(event) = adapter.parse(&headers, &raw) else {
        return webhook_ignored("unhandled event");
    };
    match event.kind {
        EventKind::Ping => (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response(),
        EventKind::Push => webhook_dispatch_push(&state, &provider, event).await,
        EventKind::BranchDelete => webhook_dispatch_delete(&state, &provider, event).await,
    }
}

/// Push → warm. Applies the allowlist gate and the branch policy (always warm
/// the default branch; other branches only if already tracked), then enqueues
/// onto the shared build queue and returns immediately (fire-and-forget).
async fn webhook_dispatch_push(
    state: &ServerState,
    provider: &ProviderInstance,
    event: crate::webhook::CanonicalEvent,
) -> Response {
    let repo_id = RepoId {
        provider: provider.id.clone(),
        path: event.repo.clone(),
    };
    // Validate the payload-supplied path so a hostile push can't escape storage
    // keys. We trust the payload only for routing.
    if validation::validate_repo_path(provider, &repo_id).is_err() {
        return webhook_ignored("invalid repo path");
    }
    // Allowlist gate (allow-all when unconfigured).
    if !state.webhook_config.allows(&repo_id.storage_key()) {
        return webhook_ignored("repo not in webhook allowlist");
    }
    // Phase 1 warms branches only; tags and other refs are ignored.
    let Some(branch) = event
        .ref_
        .strip_prefix("refs/heads/")
        .filter(|b| !b.is_empty())
    else {
        return webhook_ignored("non-branch ref");
    };
    let branch = branch.to_string();
    // Validate the payload-derived branch before it reaches the queue / git.
    // Contained already (storage keys are slugged and git re-validates), but
    // keep the trust boundary explicit and skip a doomed enqueue+build.
    if validation::validate_git_rev(&branch).is_err() {
        return webhook_ignored("invalid branch");
    }
    // Determine the repo's default branch. Prefer the payload, but fall back to
    // the local mirror's HEAD (populated by any prior sync) when a provider
    // omits it. GitHub always sends `default_branch`; the fallback keeps the
    // policy correct for adapters/payloads that don't. A brand-new repo with
    // neither is treated as untracked until first warmed via /sync.
    let default_branch = event.default_branch.clone().or_else(|| {
        let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
        git::default_branch(&mirror_dir)
            .ok()
            .filter(|b| !b.is_empty())
    });
    // Policy: always warm the default branch; warm other branches only if a
    // build for them already exists, so a webhook can't warm every throwaway
    // feature branch.
    let is_default = default_branch.as_deref() == Some(branch.as_str());
    if !is_default {
        let tracked = matches!(
            state.ref_store.load_branch(&repo_id, &branch).await,
            Ok(Some(_))
        );
        if !tracked {
            return webhook_ignored("non-default branch not tracked");
        }
    }
    // The webhook carries no token; use the configured StaticBroker credential
    // for private clones.
    let credential = state.broker.fetch_credential(&repo_id, None);
    match enqueue_sync(state, &repo_id, &branch, None, credential).await {
        Ok(_) => {
            info!(
                "webhook: enqueued sync for {}@{branch}",
                repo_id.storage_key()
            );
            (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response()
        }
        Err(EnqueueError::Full) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "build queue full; retry shortly".to_string(),
                }),
            )
                .into_response()
        }
        Err(_) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed to enqueue sync".to_string(),
                }),
            )
                .into_response()
        }
    }
}

/// Branch delete → clean up that ref's stored metadata. Never builds.
async fn webhook_dispatch_delete(
    state: &ServerState,
    provider: &ProviderInstance,
    event: crate::webhook::CanonicalEvent,
) -> Response {
    let repo_id = RepoId {
        provider: provider.id.clone(),
        path: event.repo.clone(),
    };
    if validation::validate_repo_path(provider, &repo_id).is_err() {
        return webhook_ignored("invalid repo path");
    }
    // Gate cleanup by the same allowlist as push: the receiver only acts on
    // in-scope repos, so an out-of-scope delete is ignored just like its push
    // would be. (Such a repo was never warmed, so this is a no-op either way —
    // applying the gate just keeps push and delete symmetric.)
    if !state.webhook_config.allows(&repo_id.storage_key()) {
        return webhook_ignored("repo not in webhook allowlist");
    }
    let Some(branch) = event
        .ref_
        .strip_prefix("refs/heads/")
        .filter(|b| !b.is_empty())
    else {
        return webhook_ignored("non-branch ref");
    };
    if validation::validate_git_rev(branch).is_err() {
        return webhook_ignored("invalid branch");
    }
    // Drop the stored ref and any cached copy. Best-effort: a delete we can't
    // complete is logged, not surfaced to the provider (it would just retry).
    if let Err(e) = state.ref_store.delete_branch(&repo_id, branch).await {
        warn!(
            "webhook: failed to delete ref {}@{branch}: {e}",
            repo_id.storage_key()
        );
    }
    state.ref_store.invalidate(&repo_id, branch).await;
    invalidate_ref_response_cache(state, &repo_id, branch);
    info!(
        "webhook: cleaned up deleted branch {}@{branch}",
        repo_id.storage_key()
    );
    (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response()
}

async fn sync_repo_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    params: SyncRequest,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&params.branch))
    {
        return resp;
    }
    if let Some(rev) = params.rev.as_deref()
        && let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(rev))
    {
        return resp;
    }
    let start = Instant::now();
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let branch = params.branch;
    let at_rev = params.rev;

    let request_token = upstream_token_from_headers(&headers);
    let credential = state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref());
    // AU1: a sync both builds and returns the ref (with signed URLs), so gate it.
    let private =
        match authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    // Async build queue: enqueue the build onto the bounded background worker so
    // it survives client disconnect / HTTP timeout (the key win for huge repos)
    // and is rate-bounded under load. Coalesce concurrent `/sync` for the same
    // key onto one build, wait up to RIPCLONE_SYNC_WAIT_SECS, then 202.
    if async_build_enabled() {
        // Keep this comfortably under edge/proxy request timeouts (e.g. Fly's
        // ~60s): on a long build we return 202 and let the client retry, rather
        // than holding the connection until it is reset mid-request.
        let wait = Duration::from_secs(env_bytes("RIPCLONE_SYNC_WAIT_SECS", 25));

        // Place the build on the queue via the shared enqueue path (the same one
        // the webhook receiver uses), then wait for it here so `/sync` can return
        // the freshly built ref. Webhook callers drop the handle for
        // fire-and-forget warming.
        match enqueue_sync(&state, &repo_id, &branch, at_rev.as_deref(), credential).await {
            // In-process queue: the same-process worker signals completion on a
            // oneshot. Wait up to `wait`, then 202 and let the client retry.
            Ok(SyncEnqueued::InProc { rx }) => match tokio::time::timeout(wait, rx).await {
                Ok(Ok(Ok(()))) => {
                    // Resolve HEAD to the concrete default branch before loading
                    // the persisted ref; do_sync stores artifacts under the real
                    // branch.
                    let effective_branch = if branch == "HEAD" {
                        git::default_branch(&mirror_dir)
                            .ok()
                            .filter(|b| !b.is_empty())
                            .unwrap_or_else(|| branch.clone())
                    } else {
                        branch.clone()
                    };
                    let load_key = ref_store_key(&effective_branch, at_rev.as_deref());
                    match state.ref_store.load_branch(&repo_id, &load_key).await {
                        Ok(Some(info)) => {
                            state.metrics.record_sync(start.elapsed());
                            let resp = ref_response(
                                &repo_id,
                                &provider,
                                effective_branch,
                                &info,
                                &state.storage,
                                "full",
                                private,
                            );
                            return (StatusCode::OK, Json(resp)).into_response();
                        }
                        _ => {
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ErrorResponse {
                                    error: "build finished but ref missing".to_string(),
                                }),
                            )
                                .into_response();
                        }
                    }
                }
                Ok(Ok(Err(e))) => {
                    state.metrics.record_error();
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: format!("sync failed: {e}"),
                        }),
                    )
                        .into_response();
                }
                Ok(Err(_)) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "build worker dropped".to_string(),
                        }),
                    )
                        .into_response();
                }
                Err(_) => {
                    return (
                        StatusCode::ACCEPTED,
                        Json(BuildResponse {
                            status: "building".to_string(),
                            queue_depth: state.build_queue.depth().await,
                        }),
                    )
                        .into_response();
                }
            },
            // Cross-process queue: poll the job's status, since the build runs in
            // a separate ripclone-worker.
            Ok(SyncEnqueued::CrossProc { job_id }) => {
                let deadline = Instant::now() + wait;
                let mut consecutive_errors = 0u32;
                loop {
                    match state.build_queue.job_status(job_id).await {
                        Ok(JobState::Done) => {
                            // The build ran in another process, so this server's
                            // ref caches may be stale — drop them before reading.
                            let effective_branch = if branch == "HEAD" {
                                git::default_branch(&mirror_dir)
                                    .ok()
                                    .filter(|b| !b.is_empty())
                                    .unwrap_or_else(|| branch.clone())
                            } else {
                                branch.clone()
                            };
                            state.ref_store.invalidate(&repo_id, &branch).await;
                            state
                                .ref_store
                                .invalidate(&repo_id, &effective_branch)
                                .await;
                            invalidate_ref_response_cache(&state, &repo_id, &effective_branch);
                            let load_key = ref_store_key(&effective_branch, at_rev.as_deref());
                            match state.ref_store.load_branch(&repo_id, &load_key).await {
                                // Guard on a non-empty commit: a HEAD row can exist
                                // as a build_status placeholder (empty commit).
                                // Never return that as a successful ref.
                                Ok(Some(info)) if !info.commit.is_empty() => {
                                    state.metrics.record_sync(start.elapsed());
                                    let resp = ref_response(
                                        &repo_id,
                                        &provider,
                                        effective_branch,
                                        &info,
                                        &state.storage,
                                        "full",
                                        private,
                                    );
                                    return (StatusCode::OK, Json(resp)).into_response();
                                }
                                _ => {
                                    return (
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        Json(ErrorResponse {
                                            error: "build finished but ref missing".to_string(),
                                        }),
                                    )
                                        .into_response();
                                }
                            }
                        }
                        Ok(JobState::Failed(e)) => {
                            state.metrics.record_error();
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ErrorResponse {
                                    error: format!("sync failed: {e}"),
                                }),
                            )
                                .into_response();
                        }
                        Ok(_) => consecutive_errors = 0,
                        Err(e) => {
                            // Don't mask a persistent queue outage as backpressure.
                            consecutive_errors += 1;
                            warn!("queue job_status poll failed ({consecutive_errors}): {e}");
                            if consecutive_errors >= 5 {
                                state.metrics.record_error();
                                return (
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    Json(ErrorResponse {
                                        error: format!("build queue unavailable: {e}"),
                                    }),
                                )
                                    .into_response();
                            }
                        }
                    }
                    if Instant::now() >= deadline {
                        return (
                            StatusCode::ACCEPTED,
                            Json(BuildResponse {
                                status: "building".to_string(),
                                queue_depth: state.build_queue.depth().await,
                            }),
                        )
                            .into_response();
                    }
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
            }
            Err(EnqueueError::Full) => {
                state.metrics.record_error();
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: "build queue full; retry shortly".to_string(),
                    }),
                )
                    .into_response();
            }
            Err(EnqueueError::RevUnsupported) => {
                return (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(ErrorResponse {
                        error: "rev override (?rev=) is not supported on the cross-process \
                                queue; use the local queue (RIPCLONE_QUEUE=local)"
                            .to_string(),
                    }),
                )
                    .into_response();
            }
            Err(EnqueueError::QueueError(e)) => {
                state.metrics.record_error();
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: format!("failed to enqueue build: {e}"),
                    }),
                )
                    .into_response();
            }
            Err(EnqueueError::NoJobId) => {
                state.metrics.record_error();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "queue returned no job id".to_string(),
                    }),
                )
                    .into_response();
            }
        }
    }

    let lock = repo_lock(&state.sync_locks, &repo_id).await;
    let _guard = lock.lock().await;
    match do_sync(
        &state.cas,
        &mirror_dir,
        &repo_id,
        &branch,
        at_rev.as_deref(),
        &state.ref_store,
        false,
        // In-process server: phase 2 runs in the background for a fast response.
        false,
        &state.storage,
        &state.retention,
        &provider,
        credential.as_ref(),
    )
    .await
    {
        Ok(info) => {
            state.metrics.record_sync(start.elapsed());
            // Resolve HEAD to the concrete default branch for caching/response.
            let effective_branch = if branch == "HEAD" {
                info.default_branch.clone()
            } else {
                branch.clone()
            };
            // The mirror was just fetched; let the immediately-following resolve
            // skip its own fetch. Stamp both the concrete branch and the original
            // requested branch (e.g. HEAD) so callers resolving either key avoid a
            // redundant fetch.
            stamp_mirror_fresh(
                &state,
                &format!("{}/{}", repo_id.storage_key(), effective_branch),
            );
            if branch != effective_branch {
                stamp_mirror_fresh(&state, &format!("{}/{}", repo_id.storage_key(), branch));
            }
            invalidate_ref_response_cache(&state, &repo_id, &effective_branch);
            let resp = ref_response(
                &repo_id,
                &provider,
                effective_branch,
                &info,
                &state.storage,
                "full",
                private,
            );
            drop(_guard);
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => {
            drop(_guard);
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("sync failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

async fn build_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(body): Json<BuildRequest>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&body.owner, &body.repo) {
        return resp;
    }
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&body.commit))
    {
        return resp;
    }
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&body.ref_name))
    {
        return resp;
    }
    // The build endpoint accepts GitHub's OIDC token in the standard
    // Authorization header and the ripclone token in a dedicated header.
    let oidc_token = match headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "missing OIDC Authorization: Bearer token".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Verify the ripclone token if one is configured.
    if let Some(expected) = &state.token_hash {
        let ripclone_header = headers
            .get("X-Ripclone-Token")
            .and_then(|v| v.to_str().ok());
        let authorized = ripclone_header
            .map(|v| check_auth_header(&format!("Ripclone {v}"), expected))
            .unwrap_or(false);
        if !authorized {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "unauthorized".to_string(),
                }),
            )
                .into_response();
        }
    }

    let verifier = match &state.oidc_verifier {
        Some(v) => v,
        None => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                Json(ErrorResponse {
                    error: "OIDC verification is not configured".to_string(),
                }),
            )
                .into_response();
        }
    };

    if let Err(e) = verifier.verify(oidc_token, &body.owner, &body.repo).await {
        state.metrics.record_error();
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: format!("OIDC verification failed: {}", e),
            }),
        )
            .into_response();
    }

    let job_repo_id = RepoId::github(format!("{}/{}", body.owner, body.repo));
    let credential = state.broker.fetch_credential(&job_repo_id, None);
    let job = BuildJob {
        repo_id: job_repo_id,
        branch: "HEAD".to_string(),
        rev: None,
        credential,
    };

    state.metrics.record_build_queued();
    let enq = state.build_queue.enqueue(job).await;
    let accepted = matches!(&enq, Ok(e) if e.outcome != EnqueueOutcome::Full);
    if !accepted {
        state.metrics.record_build_rejected();
        let error = match enq {
            Err(e) => format!("build queue unavailable: {e}"),
            _ => "build queue full".to_string(),
        };
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse { error }),
        )
            .into_response();
    }
    state.metrics.record_build_accepted();

    (
        StatusCode::ACCEPTED,
        Json(BuildResponse {
            status: "queued".to_string(),
            queue_depth: state.build_queue.depth().await,
        }),
    )
        .into_response()
}

async fn cat_file_inner(
    repo_id: RepoId,
    _provider: ProviderInstance,
    query: CatRequest,
    state: ServerState,
) -> Response {
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&query.branch))
    {
        return resp;
    }
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let path = query.path;
    let branch = query.branch;

    let result = tokio::task::spawn_blocking(move || {
        let commit = git::resolve_commit(&mirror_dir, &branch)?;
        let entry = git::ls_tree_entry(&mirror_dir, &commit, &path)?;
        let (_, sha) = entry.ok_or_else(|| anyhow::anyhow!("path not found: {}", path))?;
        git::cat_file(&mirror_dir, &sha)
    })
    .await;

    match result {
        Ok(Ok(data)) => (StatusCode::OK, data).into_response(),
        Ok(Err(e)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("cat failed: {}", e),
            }),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "cat task panicked".to_string(),
            }),
        )
            .into_response(),
    }
}

async fn file_sizes_inner(
    repo_id: RepoId,
    _provider: ProviderInstance,
    query: SizesRequest,
    state: ServerState,
) -> Response {
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&query.branch))
    {
        return resp;
    }
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let branch = query.branch;

    let result = tokio::task::spawn_blocking(move || {
        let commit = git::resolve_commit(&mirror_dir, &branch)?;
        git::ls_tree_sizes(&mirror_dir, &commit)
    })
    .await;

    match result {
        Ok(Ok(map)) => (StatusCode::OK, Json(map)).into_response(),
        Ok(Err(e)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("sizes failed: {}", e),
            }),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "sizes task panicked".to_string(),
            }),
        )
            .into_response(),
    }
}

async fn create_snapshot_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    query: SnapshotRequest,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&query.branch))
    {
        return resp;
    }
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let request_token = upstream_token_from_headers(&headers);
    let credential = state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref());
    let branch = query.branch.clone();

    let lock = repo_lock(&state.sync_locks, &repo_id).await;
    let _guard = lock.lock().await;
    let info = match do_sync(
        &state.cas,
        &mirror_dir,
        &repo_id,
        &branch,
        None,
        &state.ref_store,
        false,
        // In-process server: phase 2 runs in the background for a fast response.
        false,
        &state.storage,
        &state.retention,
        &provider,
        credential.as_ref(),
    )
    .await
    {
        Ok(info) => {
            invalidate_ref_response_cache(&state, &repo_id, &branch);
            drop(_guard);
            info
        }
        Err(e) => {
            drop(_guard);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("sync failed: {}", e),
                }),
            )
                .into_response();
        }
    };

    let mirror_dir2 = mirror_dir.clone();
    let cas2 = state.cas.clone();
    let commit = info.commit.clone();
    let skeleton_pack = info.skeleton_pack.clone();
    let hot_files = query.hot_files;

    match tokio::task::spawn_blocking(move || {
        let builder = SnapshotBuilder::new(&mirror_dir2, &cas2);
        builder.build(&commit, &skeleton_pack, hot_files)
    })
    .await
    {
        Ok(Ok(snap)) => {
            let (resp_owner, resp_repo) = repo_id
                .github_owner_repo()
                .map(|(o, r)| (o.to_string(), r.to_string()))
                .unwrap_or_else(|| (repo_id.provider.as_str().to_string(), repo_id.path.clone()));
            (
                StatusCode::OK,
                Json(SnapshotResponse {
                    owner: resp_owner,
                    repo: resp_repo,
                    branch: query.branch,
                    commit: snap.commit,
                    snapshot_hash: snap.hash,
                    size: snap.size,
                    hot_files: snap.hot_files,
                }),
            )
                .into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("snapshot build failed: {}", e),
            }),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "snapshot task panicked".to_string(),
            }),
        )
            .into_response(),
    }
}

async fn get_hotfiles_inner(
    repo_id: RepoId,
    _provider: ProviderInstance,
    query: HotfilesRequest,
    state: ServerState,
) -> Response {
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let branch = query.branch;
    let count = query.count;

    let result = tokio::task::spawn_blocking(move || {
        let commit = git::resolve_commit(&mirror_dir, &branch)?;
        git::hot_files(&mirror_dir, &commit, count, 5)
    })
    .await;

    match result {
        Ok(Ok(files)) => {
            (StatusCode::OK, Json(serde_json::json!({ "files": files }))).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("hotfiles failed: {}", e),
            }),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "hotfiles task panicked".to_string(),
            }),
        )
            .into_response(),
    }
}

async fn batch_files_inner(
    repo_id: RepoId,
    _provider: ProviderInstance,
    body: BatchRequest,
    state: ServerState,
) -> Response {
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&body.branch))
    {
        return resp;
    }
    if let Some(commit) = &body.commit
        && let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(commit))
    {
        return resp;
    }
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let branch = body.branch;
    let commit_hint = body.commit;
    let paths = body.paths;

    // Defensive ceiling to keep response sizes bounded.
    const MAX_BATCH: usize = 1000;
    if paths.len() > MAX_BATCH {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("batch too large: {} > {}", paths.len(), MAX_BATCH),
            }),
        )
            .into_response();
    }

    let result = tokio::task::spawn_blocking(move || {
        let commit = match commit_hint {
            Some(c) => c,
            None => git::resolve_commit(&mirror_dir, &branch)?,
        };
        git::build_path_tar(&mirror_dir, &commit, &paths)
    })
    .await;

    match result {
        Ok(Ok(tar)) => {
            (StatusCode::OK, [("content-type", "application/x-tar")], tar).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("batch failed: {}", e),
            }),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "batch task panicked".to_string(),
            }),
        )
            .into_response(),
    }
}

fn validate_artifact_hash(hash: &str) -> Option<Response> {
    if let Err(e) = crate::cas::Cas::validate_artifact_id(hash) {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response(),
        );
    }
    None
}

async fn get_pack(Path(hash): Path<String>, State(state): State<ServerState>) -> impl IntoResponse {
    if let Some(resp) = validate_artifact_hash(&hash) {
        return resp;
    }
    serve_artifact(hash, state, None).await.into_response()
}

async fn get_object(
    Path(sha): Path<String>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = validate_artifact_hash(&sha) {
        return resp;
    }
    serve_artifact(sha, state, None).await.into_response()
}

/// Test-only fault injection. When the server was started with
/// `RIPCLONE_TEST_FAIL_FIRST_FETCHES=N`, the first N artifact GETs return 503 so
/// the client's retry/backoff can be exercised end to end. The threshold is read
/// once at construction (0 = off, the production default), so this is a single
/// atomic load on the hot path. The counter lives in `ServerState`, so each
/// test's server starts fresh.
fn maybe_inject_artifact_fault(state: &ServerState) -> Option<Response> {
    if state.fail_first_fetches == 0 {
        return None;
    }
    let seen = state.artifact_fetch_count.fetch_add(1, Ordering::Relaxed);
    if seen < state.fail_first_fetches {
        Some((StatusCode::SERVICE_UNAVAILABLE, "injected transient fault").into_response())
    } else {
        None
    }
}

async fn get_artifact(
    Path(hash): Path<String>,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = maybe_inject_artifact_fault(&state) {
        return resp;
    }
    if let Some(resp) = validate_artifact_hash(&hash) {
        return resp;
    }
    serve_artifact(hash, state, Some(headers))
        .await
        .into_response()
}

async fn serve_artifact(
    hash: String,
    state: ServerState,
    headers: Option<axum::http::HeaderMap>,
) -> impl IntoResponse {
    // If the backend can hand out a signed URL, redirect the client there.
    // The client can then use its own Range requests against the CDN/object store.
    if let Some(url) = state
        .storage
        .signed_url(&hash, std::time::Duration::from_secs(900))
    {
        state.metrics.record_artifact_request(0);
        return (
            StatusCode::TEMPORARY_REDIRECT,
            [("location", url.as_str())],
            Vec::new(),
        )
            .into_response();
    }

    let total_size = match tokio::task::spawn_blocking({
        let storage = state.storage.clone();
        let hash = hash.clone();
        move || -> anyhow::Result<u64> { storage.size(&hash) }
    })
    .await
    {
        Ok(Ok(size)) => size,
        _ => {
            state.metrics.record_error();
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("artifact not found: {}", hash),
                }),
            )
                .into_response();
        }
    };

    // Parse Range header if present.
    let range = headers.as_ref().and_then(|h| {
        h.get(axum::http::header::RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| parse_byte_range(v, total_size))
    });

    match range {
        Some((start, end)) => {
            let len = end - start + 1;
            let hash_clone = hash.clone();
            match tokio::task::spawn_blocking(move || {
                state.storage.get_range(&hash_clone, start, len)
            })
            .await
            {
                Ok(Ok(data)) => {
                    state.metrics.record_artifact_request(data.len() as u64);
                    let content_range = format!("bytes {}-{}/{}", start, end, total_size);
                    (
                        StatusCode::PARTIAL_CONTENT,
                        [
                            ("content-range", content_range.as_str()),
                            ("content-length", &data.len().to_string()),
                        ],
                        data,
                    )
                        .into_response()
                }
                _ => {
                    state.metrics.record_error();
                    (
                        StatusCode::NOT_FOUND,
                        Json(ErrorResponse {
                            error: format!("artifact not found: {}", hash),
                        }),
                    )
                        .into_response()
                }
            }
        }
        None => {
            let hash_clone = hash.clone();
            match tokio::task::spawn_blocking(move || state.storage.get(&hash_clone)).await {
                Ok(Ok(data)) => {
                    state.metrics.record_artifact_request(data.len() as u64);
                    (StatusCode::OK, data).into_response()
                }
                _ => {
                    state.metrics.record_error();
                    (
                        StatusCode::NOT_FOUND,
                        Json(ErrorResponse {
                            error: format!("artifact not found: {}", hash),
                        }),
                    )
                        .into_response()
                }
            }
        }
    }
}

/// Parse a single `bytes=start-end` range. Returns inclusive (start, end).
///
/// Clients with off-by-one range math may ask for an end past the object end;
/// clamp to the last byte rather than rejecting so the partial response still
/// satisfies the request.
fn parse_byte_range(range: &str, size: u64) -> Option<(u64, u64)> {
    let range = range.strip_prefix("bytes=")?;
    let (start_str, end_str) = range.split_once('-')?;
    let start: u64 = start_str.parse().ok()?;
    if start >= size {
        return None;
    }
    let end = if end_str.is_empty() {
        size.saturating_sub(1)
    } else {
        end_str.parse::<u64>().ok()?.min(size.saturating_sub(1))
    };
    if start > end {
        return None;
    }
    Some((start, end))
}

/// Remove `.tmp*` entries under `dir` whose mtime is older than `max_age`.
/// Best-effort cleanup of build temp dirs leaked by a killed sync.
fn sweep_stale_tempdirs(dir: &std::path::Path, max_age: Duration) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(".tmp") {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .map(|age| age > max_age)
            .unwrap_or(false);
        if !stale {
            continue;
        }
        let path = entry.path();
        let _ = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
    }
}

/// Outcome of the depth-pack build, which differs between the default
/// rebuild-everything path and the LSM incremental path.
enum DepthBuild {
    Full {
        head_packs: Vec<(String, u64, String, u64)>,
        history_packs: Vec<(String, u64, String, u64)>,
    },
    Lsm(crate::pack::IncrementalPacks),
}

/// Ref-store key for a build. Rev-targeted builds (sync/clone `--at <rev>`) use a
/// rolling test key so they never overwrite the real branch entry that normal
/// tip clients depend on; sequential rev syncs still share this key, so they stay
/// incremental (each rev build's prev is the previous rev build). Tip builds use
/// the branch directly. The git ref-name grammar forbids `#`, so this can never
/// collide with a real branch.
fn ref_store_key(branch: &str, at_rev: Option<&str>) -> String {
    if at_rev.is_some() {
        format!("{branch}#atrev")
    } else {
        branch.to_string()
    }
}

fn tuple_to_sized(p: &(String, u64, String, u64)) -> crate::SizedPack {
    crate::SizedPack {
        pack: p.0.clone(),
        pack_len: p.1,
        idx: p.2.clone(),
        idx_len: p.3,
    }
}

fn sized_to_tuple(p: &crate::SizedPack) -> (String, u64, String, u64) {
    (p.pack.clone(), p.pack_len, p.idx.clone(), p.idx_len)
}

/// True when two-phase publish is enabled (publish depth=1 first, build full
/// history in the background). On by default — the depth=1-first path is the
/// largest lever for fast sync. Disable with `RIPCLONE_TWO_PHASE=0`.
fn two_phase_enabled() -> bool {
    std::env::var("RIPCLONE_TWO_PHASE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true)
}

/// True when `/sync` routes the build through the bounded background worker
/// (survives client disconnect, rate-bounded). On by default — disable with
/// `RIPCLONE_ASYNC_BUILD=0`.
fn async_build_enabled() -> bool {
    std::env::var("RIPCLONE_ASYNC_BUILD")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true)
}

fn env_bytes(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// LSM incremental-history configuration.
struct LsmConfig {
    /// When on, only the tail past the last sealed level is built each sync;
    /// prior levels are reused by hash from object storage (Tigris). On by
    /// default — disable with `RIPCLONE_LSM=0`.
    enabled: bool,
    /// Compact down to at most this many levels (merging the smallest adjacent
    /// pair) so the level count stays bounded under seal-every-sync.
    max_levels: usize,
}

fn lsm_config() -> LsmConfig {
    let enabled = std::env::var("RIPCLONE_LSM")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    let max_levels = std::env::var("RIPCLONE_LSM_MAX_LEVELS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16usize);
    LsmConfig {
        enabled,
        max_levels,
    }
}

/// Given the prior sealed levels and a freshly built tail, decide whether to
/// seal the tail into a new level, then compact the level set back under
/// `max_levels`. Returns `(history_packs, new_pack_tuples, new_levels)` where
/// `history_packs` is every history pack this manifest references (all levels
/// flattened — prior levels reused by hash), `new_pack_tuples` is the packs
/// freshly built this sync (the tail plus any compaction output — what to
/// upload/evict), and `new_levels` is the levels to persist for the next sync.
/// `head_packs` is not included here (the caller handles the HEAD closure).
#[allow(clippy::too_many_arguments)]
async fn seal_and_compact(
    mirror_dir: &std::path::Path,
    cas: &Cas,
    commit: &str,
    prev_levels: Vec<crate::HistoryLevel>,
    sealed_tip: Option<String>,
    tail_packs: Vec<(String, u64, String, u64)>,
    history_target: u64,
    cfg: &LsmConfig,
) -> Result<(
    Vec<(String, u64, String, u64)>,
    Vec<(String, u64, String, u64)>,
    Vec<crate::HistoryLevel>,
)> {
    // Seal the tail into a new immutable level whenever HEAD advanced past the
    // last sealed tip and the tail is non-empty. The cold base (sealed_tip None)
    // always advances, so it always seals and becomes level 0. Compaction keeps
    // the level count bounded.
    let advances = sealed_tip.as_deref() != Some(commit);
    let seal = advances && !tail_packs.is_empty();
    let mut levels = prev_levels;
    let mut new_tuples = tail_packs.clone();
    if seal {
        levels.push(crate::HistoryLevel {
            tip_commit: commit.to_string(),
            packs: tail_packs.iter().map(tuple_to_sized).collect(),
        });
        let packed_mib: u64 =
            tail_packs.iter().map(|(_, plen, _, _)| plen).sum::<u64>() / (1024 * 1024);
        info!(
            "LSM: sealed level {} at {} ({} packs, {} MiB packed)",
            levels.len() - 1,
            &commit[..7.min(commit.len())],
            tail_packs.len(),
            packed_mib
        );
    }

    // Compact (off-thread; it re-packs ranges) so the level count stays bounded.
    if levels.len() > cfg.max_levels {
        let before = levels.len();
        let (md, c, levels_in, max, tgt) = (
            mirror_dir.to_path_buf(),
            cas.clone(),
            levels.clone(),
            cfg.max_levels,
            history_target,
        );
        let res = tokio::task::spawn_blocking(move || {
            PackBuilder::new(&md, &c).compact_levels(levels_in, max, tgt)
        })
        .await
        .context("compaction task")??;
        new_tuples.extend(res.new_packs.iter().cloned());
        levels = res.levels;
        info!("LSM: compacted {} levels -> {}", before, levels.len());
    }

    // Manifest history = every sealed level's packs (prior reused by hash + any
    // compaction output), flattened. We always seal an advancing non-empty tail,
    // so there is never an unsealed `(sealed_tip, HEAD]` remainder to append: the
    // only time `seal` is false is when the tail is empty (HEAD didn't advance).
    let history_packs: Vec<(String, u64, String, u64)> = levels
        .iter()
        .flat_map(|l| l.packs.iter().map(sized_to_tuple))
        .collect();
    Ok((history_packs, new_tuples, levels))
}

/// Load and decode a prior sync's metadata chunk and return its files table.
/// Bytes come from local CAS or object storage (the metadata may have been
/// evicted locally after a prior upload). Returns `None` on any failure — the
/// caller then falls back to a full (non-incremental) build, so this is purely
/// best-effort optimization, never a correctness dependency.
fn load_metadata_files(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    metadata_hash: &str,
) -> Option<Vec<crate::clonepack::FileEntry>> {
    let bytes = cas
        .get(metadata_hash)
        .or_else(|_| storage.get(metadata_hash))
        .ok()?;
    let md = crate::clonepack::MetadataChunk::decode(bytes.as_slice()).ok()?;
    Some(md.files)
}

/// Build one variant's PackEntry list + concatenated idx bundle. Free-function
/// form (used by the two-phase sync path and its background phase-2 task).
fn assemble_variant(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    tagged: &[(&(String, u64, String, u64), bool)],
) -> Result<(Vec<crate::clonepack::PackEntry>, Option<ChunkRef>, String)> {
    if tagged.is_empty() {
        return Ok((Vec::new(), None, String::new()));
    }
    // Fetch every pack's idx concurrently — there can be 100+ of them (each a local
    // read or an object-store GET), so a serial loop dominates the assemble. Results
    // stay in order, so the concatenation + offsets below are deterministic.
    use rayon::prelude::*;
    let idxs: Vec<Vec<u8>> = tagged
        .par_iter()
        .map(|&(pack, _)| cas.get(&pack.2).or_else(|_| storage.get(&pack.2)))
        .collect::<Result<Vec<_>>>()?;
    let mut buf: Vec<u8> = Vec::with_capacity(idxs.iter().map(|b| b.len()).sum());
    let mut entries = Vec::with_capacity(tagged.len());
    for (&(pack, history_only), idx_bytes) in tagged.iter().zip(&idxs) {
        let offset = buf.len() as u64;
        buf.extend_from_slice(idx_bytes);
        entries.push(crate::clonepack::PackEntry {
            pack: Some(ChunkRef {
                hash: hash_from_hex(&pack.0)?,
                len: pack.1,
            }),
            idx: Some(ChunkRef {
                hash: hash_from_hex(&pack.2)?,
                len: pack.3,
            }),
            history_only,
            idx_bundle_offset: offset,
        });
    }
    let len = buf.len() as u64;
    let hash = cas.put(&buf)?;
    Ok((
        entries,
        Some(ChunkRef {
            hash: hash_from_hex(&hash)?,
            len,
        }),
        hash,
    ))
}

/// Build a multi-pack-index over `packs` from local CAS. Free-function form.
///
/// Reads each pack's bytes from the *local* CAS (no object-storage fallback), so
/// only call this when every pack was built this sync and is still local — e.g.
/// the head MIDX is shipped only when all head buckets were freshly built. For a
/// set with reused (already-evicted) packs, omit the MIDX and let the client
/// build its own.
fn assemble_midx(
    cas: &Cas,
    packs: &[(String, u64, String, u64)],
) -> Result<(Option<ChunkRef>, String)> {
    if packs.is_empty() {
        return Ok((None, String::new()));
    }
    use rayon::prelude::*;
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = packs
        .par_iter()
        .map(|(ph, _, ih, _)| Ok((cas.get(ph)?, cas.get(ih)?)))
        .collect::<Result<Vec<_>>>()?;
    let midx = crate::git::build_multi_pack_index_bytes(&pairs)?;
    let len = midx.len() as u64;
    let hash = cas.put(&midx)?;
    Ok((
        Some(ChunkRef {
            hash: hash_from_hex(&hash)?,
            len,
        }),
        hash,
    ))
}

#[allow(clippy::too_many_arguments)]
fn make_manifest(
    commit: &str,
    parent: &Option<String>,
    default_branch: &str,
    archive_chunks: &[ChunkRef],
    metadata_hash: &str,
    metadata_len: u64,
    packs: Vec<crate::clonepack::PackEntry>,
    midx: Option<ChunkRef>,
    idx_bundle: Option<ChunkRef>,
) -> Result<ClonepackManifest> {
    Ok(ClonepackManifest {
        commit: commit.to_string(),
        parent_commit: parent.clone(),
        default_branch: default_branch.to_string(),
        metadata_chunk: Some(ChunkRef {
            hash: hash_from_hex(metadata_hash)?,
            len: metadata_len,
        }),
        archive_chunks: archive_chunks.to_vec(),
        packs,
        midx,
        idx_bundle,
        ..Default::default()
    })
}

/// Build the `[ChunkRef]` list for the archive chunks of a metadata chunk.
fn archive_chunk_refs(
    archive_chunk_hashes: &[String],
    metadata_chunk: &crate::clonepack::MetadataChunk,
) -> Result<Vec<ChunkRef>> {
    let lengths = crate::clonepack::archive_chunk_lengths(metadata_chunk);
    archive_chunk_hashes
        .iter()
        .zip(lengths.iter())
        .map(|(hash, len)| {
            Ok(ChunkRef {
                hash: hash_from_hex(hash)?,
                len: *len,
            })
        })
        .collect()
}

/// Concurrency for artifact uploads. Defaults to 2x CPU cores — connection
/// reuse makes high concurrency cheap, so scale it to the machine — overridable
/// with `RIPCLONE_UPLOAD_CONCURRENCY`.
fn upload_concurrency() -> usize {
    std::env::var("RIPCLONE_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get() * 2)
                .unwrap_or(8)
        })
        .max(1)
}

/// Upload `hashes` from CAS to storage with bounded concurrency.
async fn upload_artifacts(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    hashes: Vec<String>,
    conc: usize,
) -> Result<()> {
    futures::stream::iter(hashes.into_iter().map(|hash| {
        let cas = cas.clone();
        let storage = storage.clone();
        async move {
            // Read the chunk off the async worker (blocking disk I/O), then
            // upload via the async path so the PUT runs on this runtime with the
            // pooled client — connections stay warm across chunks instead of
            // re-handshaking per upload.
            let read_hash = hash.clone();
            let data = tokio::task::spawn_blocking(move || cas.get(&read_hash))
                .await
                .context("read task")?
                .with_context(|| format!("read artifact {} for upload", hash))?;
            storage
                .put_async(&hash, &data)
                .await
                .with_context(|| format!("upload artifact {}", hash))
        }
    }))
    .buffer_unordered(conc.max(1))
    .try_collect::<Vec<()>>()
    .await
    .map(|_| ())
}

/// After upload: on a remote backend drop local pack copies (keeping the tiny
/// idx files for future bundle/MIDX rebuilds); on a local backend protect them
/// from retention instead.
async fn settle_storage(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    uploaded: Vec<String>,
    keep_idx: std::collections::HashSet<String>,
) {
    if storage.is_remote() {
        for h in uploaded.iter().filter(|h| !keep_idx.contains(*h)) {
            let _ = cas.remove(h);
        }
    } else {
        retention.protect(uploaded).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn do_sync(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    branch: &str,
    // Optional build-commit override (e.g. "HEAD~5"); when None the branch tip is
    // used. The branch is still the ref-store key and fetch target.
    at_rev: Option<&str>,
    ref_store: &Arc<dyn RefStore>,
    build_full_pack: bool,
    // When true, the two-phase build runs phase 2 (full history) inline and only
    // returns once it is durable, instead of detaching it into a background task.
    // The caller must hold `repo_lock` for the whole call. Set by an ephemeral
    // (cross-process) worker so it never acks `done` while phase 2 is still
    // unbuilt — a detached task would die with the worker and the full clonepack
    // would never be built (A3). The long-lived in-process server leaves this
    // false: its detached phase-2 task survives, keeping `/sync` fast.
    inline_full_history: bool,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    provider: &ProviderInstance,
    credential: Option<&secrecy::SecretString>,
) -> Result<RefInfo> {
    info!("syncing {}@{}", repo_id.storage_key(), branch);

    // Per-phase timers so sync cost can be tuned with real numbers (RIPCLONE_LOG
    // shows them at INFO). `t_total` spans the whole build; `t` is reset at each
    // phase boundary.
    let t_total = Instant::now();
    let mut t = t_total;

    // Best-effort: remove stale build temp dirs left by a previously killed
    // sync. `tempfile` cleans up on drop, but not on SIGKILL/OOM, so a crashed
    // build leaks a `.tmp*` dir in TMPDIR (= repo_root). Only sweep old ones so a
    // concurrent build's temp dir is never touched.
    if let Some(repo_root) = mirror_dir.parent() {
        sweep_stale_tempdirs(repo_root, Duration::from_secs(2 * 3600));
    }

    // Sync the bare mirror synchronously (blocking git call).
    let mirror_dir_sync = mirror_dir.to_path_buf();
    let mirror_dir = mirror_dir.to_path_buf();
    let provider_sync = provider.clone();
    let repo_id_sync = repo_id.clone();
    let branch_sync = branch.to_string();
    let credential_sync = credential.cloned();
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror(
            &mirror_dir_sync,
            &provider_sync,
            &repo_id_sync,
            &branch_sync,
            credential_sync.as_ref(),
        )
    })
    .await
    .context("sync task")??;
    info!("sync phase: mirror fetch {:?}", t.elapsed());
    t = Instant::now();

    // Resolve the build commit: the rev override (e.g. "HEAD~5") when given,
    // else the branch tip. The override is relative to the just-fetched mirror.
    let commit = git::resolve_commit(&mirror_dir, at_rev.unwrap_or(branch))?;
    let parent = git::parent_commit(&mirror_dir, &commit).ok().flatten();
    let default_branch = git::default_branch(&mirror_dir).unwrap_or_else(|_| "HEAD".to_string());

    // If the caller asked for HEAD, store artifacts under the concrete default
    // branch name so both /refs/HEAD and /refs/<branch> find the same build.
    let branch = if branch == "HEAD" {
        default_branch.as_str()
    } else {
        branch
    };

    // Ref-store key. Rev builds use a rolling test key so they never overwrite
    // the real branch entry; everything below stores/loads under this key. The
    // mirror fetch + commit resolution above used the real branch/rev.
    let ref_key = ref_store_key(branch, at_rev);
    let branch = ref_key.as_str();

    // No-op fast path: if a *completed full* build already exists for exactly
    // this commit, the prior clonepack artifacts are still valid — reuse them and
    // build nothing (skips commit-graph/bitmap/skeleton/history/archive), so a
    // poke-to-check sync of an unchanged repo returns near-instantly. Keying on
    // `full_clonepack.commit == commit` (not `build_status`) is robust: it is set
    // only when phase 2 finishes for this commit, so it correctly excludes the
    // Option-A carried-prior case, in-flight/failed phase 2, and the async
    // worker's transient "building" status (which would otherwise mask a prior
    // completed build).
    if let Ok(Some(prev)) = ref_store.load_branch(repo_id, branch).await
        && prev.full_clonepack.commit == commit
        && !prev.full_clonepack.manifest.is_empty()
    {
        info!(
            "sync no-op: {} already current at {} (reusing prior clonepack)",
            repo_id.storage_key(),
            &commit[..7.min(commit.len())]
        );
        return Ok(prev);
    }

    // Write a commit-graph so the rev-list walks in the skeleton + layered-pack
    // builds below are fast (a fresh --mirror clone has none). Best-effort.
    let cg_dir = mirror_dir.clone();
    let _ = tokio::task::spawn_blocking(move || git::write_commit_graph(&cg_dir)).await;
    info!("sync phase: commit-graph {:?}", t.elapsed());
    t = Instant::now();

    info!("building artifacts for {}", &commit[..7]);

    // Two-phase publish: build + publish the depth=1 clonepack now, build full
    // history in the background. Removes the dominant history-deltification cost
    // from "time to clonable".
    if two_phase_enabled() {
        return build_and_publish_two_phase(
            cas,
            &mirror_dir,
            repo_id,
            branch,
            &commit,
            parent,
            &default_branch,
            ref_store,
            storage,
            retention,
            inline_full_history,
            t_total,
        )
        .await;
    }

    // Single-phase: write a reachability bitmap before the heavy full-history
    // enumerations (skeleton + history). Best-effort; off the two-phase depth=1
    // path (that branch returned above).
    let bm_dir = mirror_dir.clone();
    let _ = tokio::task::spawn_blocking(move || git::write_bitmap(&bm_dir)).await;
    info!("sync phase: reachability bitmap {:?}", t.elapsed());
    t = Instant::now();

    // No full skeleton: the full variant reuses the shallow (HEAD) skeleton; the
    // full history's commits+trees live in the history packs.

    // Shallow depth=1 skeleton pack + idx.
    let mirror_dir2s = mirror_dir.clone();
    let cas2s = cas.clone();
    let commit2s = commit.clone();
    let shallow_skeleton_handle = tokio::task::spawn_blocking(move || {
        let builder = PackBuilder::new(&mirror_dir2s, &cas2s);
        builder.build_shallow_skeleton_pack(&commit2s)
    });

    // Depth-1 packs: the complete object closure for HEAD (commit + tree + every
    // blob), split into self-contained packs the client installs + extracts in
    // parallel. This is a RAW (uncompressed) target; the undeltified HEAD closure
    // compresses ~3x, so 12 MiB raw lands ~4 MB download frames. Bigger frames =
    // fewer packs = fewer round-trips (each pack costs a pack GET + an idx GET),
    // which is a wash on a fast link but a real win on a slow/high-latency one;
    // still many frames, so parallelism is preserved. Carried in the manifest's
    // `packs` list.
    let pack_target_raw: u64 = std::env::var("RIPCLONE_PACK_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12 * 1024 * 1024);
    // History packs are install-only (git reads them; the client never
    // hand-parses). They must be bigger than the small HEAD packs — the 6 MB
    // HEAD target explodes a big repo into ~1k packs/spawns — but still many, so
    // the client downloads them in parallel. This is a RAW (uncompressed) target;
    // deltified history compresses ~18-20x, so 512 MiB raw lands ~28-32 MB
    // download pieces (bun: ~12 history packs fetched concurrently).
    let history_target_raw: u64 = std::env::var("RIPCLONE_HISTORY_PACK_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(512 * 1024 * 1024);

    // LSM incremental history build (default on; disable with RIPCLONE_LSM=0).
    // When on, only the tail past the last sealed level is built; prior levels
    // are reused by hash from object storage (Tigris). The level set is sealed
    // every advancing sync and compacted back under a bound. See ROADMAP "LSM
    // incremental history build".
    let lsm_cfg = lsm_config();
    let lsm = lsm_cfg.enabled;
    let prev_levels: Vec<crate::HistoryLevel> = if lsm {
        ref_store
            .load_branch(repo_id, branch)
            .await
            .ok()
            .flatten()
            .map(|i| i.history_levels)
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let sealed_tip: Option<String> = prev_levels.last().map(|l| l.tip_commit.clone());

    let mirror_dir3 = mirror_dir.clone();
    let cas3 = cas.clone();
    let commit3 = commit.clone();
    let sealed_tip3 = sealed_tip.clone();
    let depth_packs_handle = tokio::task::spawn_blocking(move || -> Result<DepthBuild> {
        let s = Instant::now();
        let builder = PackBuilder::new(&mirror_dir3, &cas3);
        let r = if lsm {
            // LSM: build only the tail (HEAD closure + objects since last seal).
            let inc = builder.build_incremental_packs(
                &commit3,
                sealed_tip3.as_deref(),
                pack_target_raw,
                history_target_raw,
            )?;
            DepthBuild::Lsm(inc)
        } else {
            // Default: small HEAD-closure packs + few large full-history packs.
            let (head_packs, history_packs) =
                builder.build_layered_packs(&commit3, pack_target_raw, history_target_raw)?;
            DepthBuild::Full {
                head_packs,
                history_packs,
            }
        };
        info!("build task: depth packs (head+history) {:?}", s.elapsed());
        Ok(r)
    });

    // Working-tree archive + manifest.
    let mirror_dir4 = mirror_dir.clone();
    let cas4 = cas.clone();
    let commit4 = commit.clone();
    let archive_handle = tokio::task::spawn_blocking(move || {
        let s = Instant::now();
        let builder = ArchiveBuilder::new(&mirror_dir4);
        let r = builder.build_into_cas(&commit4, &cas4, 6, None);
        info!("build task: working-tree archive {:?}", s.elapsed());
        r
    });

    let (shallow_skeleton_pack, shallow_skeleton_idx) = shallow_skeleton_handle
        .await
        .context("shallow skeleton pack task")??;
    let depth_build = depth_packs_handle.await.context("depth packs task")??;
    let (archive_chunk_hashes, mut metadata_chunk) =
        archive_handle.await.context("archive task")??;
    info!(
        "sync phase: build skeletons+packs+archive {:?}",
        t.elapsed()
    );
    t = Instant::now();

    // Resolve the build into:
    // - head_packs:        undeltified HEAD-closure packs (worktree source).
    // - history_packs:     the history packs this manifest references (for LSM,
    //                      prior sealed levels + the new tail; otherwise the
    //                      full history). Used for manifest entries + signing.
    // - new_pack_tuples:   packs actually built this sync (head + new history),
    //                      i.e. what to upload + evict. Prior LSM levels are
    //                      already durable in object storage.
    // - new_levels:        the LSM levels to persist for the next sync.
    // - server_full_midx:  whether to pre-build the full-variant MIDX (only when
    //                      all its packs are local this sync — i.e. non-LSM).
    let (head_packs, history_packs, new_pack_tuples, new_levels, server_full_midx) =
        match depth_build {
            DepthBuild::Full {
                head_packs,
                history_packs,
            } => {
                let mut new_tuples = head_packs.clone();
                new_tuples.extend(history_packs.iter().cloned());
                (head_packs, history_packs, new_tuples, Vec::new(), true)
            }
            DepthBuild::Lsm(inc) => {
                // Seal the new tail + compact; prior levels are reused by hash
                // from object storage (Tigris) — never rebuilt.
                let (history_packs, tail_tuples, new_levels) = seal_and_compact(
                    &mirror_dir,
                    cas,
                    &commit,
                    prev_levels,
                    sealed_tip.clone(),
                    inc.tail_packs,
                    history_target_raw,
                    &lsm_cfg,
                )
                .await?;
                // Newly built this sync = HEAD closure (always fresh) + tail +
                // any compaction output. Prior levels are already durable.
                let mut new_tuples = inc.head_packs.clone();
                new_tuples.extend(tail_tuples);
                (inc.head_packs, history_packs, new_tuples, new_levels, false)
            }
        };

    // Prebuilt index from the shallow (HEAD) skeleton — the HEAD index is the
    // same for both variants, so the full variant reuses it.
    let mirror_dir5s = mirror_dir.clone();
    let cas5s = cas.clone();
    let commit5s = commit.clone();
    let shallow_skeleton_pack_for_index = shallow_skeleton_pack.clone();
    let shallow_prebuilt_index_handle = tokio::task::spawn_blocking(move || {
        let builder = PackBuilder::new(&mirror_dir5s, &cas5s);
        builder.build_prebuilt_index(&commit5s, &shallow_skeleton_pack_for_index)
    });
    let shallow_prebuilt_index = shallow_prebuilt_index_handle
        .await
        .context("shallow prebuilt index task")??;
    info!("sync phase: prebuilt index {:?}", t.elapsed());
    t = Instant::now();

    // Assemble the full-history metadata chunk. The skeleton + prebuilt index are
    // the shallow (HEAD) ones; the full history's commits+trees come from the
    // history packs. The frame/file tables describe the working tree.
    metadata_chunk.skeleton_pack = cas.get(&shallow_skeleton_pack)?;
    metadata_chunk.skeleton_idx = cas.get(&shallow_skeleton_idx)?;
    metadata_chunk.prebuilt_index = cas.get(&shallow_prebuilt_index)?;
    let metadata_data = metadata_chunk.encode_to_vec();
    let metadata_hash = cas.put(&metadata_data)?;

    // Assemble the shallow depth=1 metadata chunk. The archive and head-blobs
    // chunks are identical; only the skeleton/index differ.
    let mut shallow_metadata_chunk = metadata_chunk.clone();
    shallow_metadata_chunk.skeleton_pack = cas.get(&shallow_skeleton_pack)?;
    shallow_metadata_chunk.skeleton_idx = cas.get(&shallow_skeleton_idx)?;
    shallow_metadata_chunk.prebuilt_index = cas.get(&shallow_prebuilt_index)?;
    let shallow_metadata_data = shallow_metadata_chunk.encode_to_vec();
    let shallow_metadata_hash = cas.put(&shallow_metadata_data)?;

    // Build manifest `packs` entries. Each is a self-contained git pack + idx,
    // fetched and installed independently. The shallow (depth=1) clonepack lists
    // only the HEAD-closure packs; the full clonepack lists HEAD + history. Order
    // is HEAD-first so a shallow client's URL indices line up with the prefix of
    // the (head+history) signed-URL list.
    // Build a variant's PackEntry list AND its idx bundle in one pass: every
    // pack's idx is concatenated into a single content-addressed blob, and each
    // entry records its offset into it. The client fetches the bundle once and
    // slices each idx locally, instead of one GET per pack idx. idx bytes come
    // from the local CAS (kept after upload) with an object-storage fallback.
    // Returns (entries, bundle ChunkRef, bundle CAS hash).
    let build_variant = |tagged: &[(&(String, u64, String, u64), bool)]| -> Result<(
        Vec<crate::clonepack::PackEntry>,
        Option<ChunkRef>,
        String,
    )> {
        if tagged.is_empty() {
            return Ok((Vec::new(), None, String::new()));
        }
        let mut buf: Vec<u8> = Vec::new();
        let mut entries = Vec::with_capacity(tagged.len());
        for &(pack, history_only) in tagged {
            let offset = buf.len() as u64;
            let idx_bytes = cas.get(&pack.2).or_else(|_| storage.get(&pack.2))?;
            buf.extend_from_slice(&idx_bytes);
            entries.push(crate::clonepack::PackEntry {
                pack: Some(ChunkRef {
                    hash: hash_from_hex(&pack.0)?,
                    len: pack.1,
                }),
                idx: Some(ChunkRef {
                    hash: hash_from_hex(&pack.2)?,
                    len: pack.3,
                }),
                history_only,
                idx_bundle_offset: offset,
            });
        }
        let len = buf.len() as u64;
        let hash = cas.put(&buf)?;
        Ok((
            entries,
            Some(ChunkRef {
                hash: hash_from_hex(&hash)?,
                len,
            }),
            hash,
        ))
    };
    let head_tagged: Vec<(&(String, u64, String, u64), bool)> =
        head_packs.iter().map(|p| (p, false)).collect();
    let full_tagged: Vec<(&(String, u64, String, u64), bool)> = head_packs
        .iter()
        .map(|p| (p, false))
        .chain(history_packs.iter().map(|p| (p, true)))
        .collect();
    let (head_entries, head_idx_bundle_ref, head_idx_bundle_hash) = build_variant(&head_tagged)?;
    let (full_entries, full_idx_bundle_ref, full_idx_bundle_hash) = build_variant(&full_tagged)?;

    // Pre-build a multi-pack-index per variant over exactly the packs that
    // variant ships, using the client's `pack-<trailer>` filenames. The client
    // drops it in directly instead of spending CPU on `git multi-pack-index
    // write`. Returns (manifest ChunkRef, CAS hash) — the hash is also tracked
    // for upload + retention.
    let build_midx = |packs: &[(String, u64, String, u64)]| -> Result<(Option<ChunkRef>, String)> {
        if packs.is_empty() {
            return Ok((None, String::new()));
        }
        let mut pairs = Vec::with_capacity(packs.len());
        for (ph, _, ih, _) in packs {
            pairs.push((cas.get(ph)?, cas.get(ih)?));
        }
        let midx = crate::git::build_multi_pack_index_bytes(&pairs)?;
        let len = midx.len() as u64;
        let hash = cas.put(&midx)?;
        Ok((
            Some(ChunkRef {
                hash: hash_from_hex(&hash)?,
                len,
            }),
            hash,
        ))
    };
    let (head_midx_ref, head_midx_hash) = build_midx(&head_packs)?;
    // The full MIDX needs every full-variant pack present locally. Under LSM the
    // prior levels were evicted, so only build it on the rebuild-all path; the
    // LSM client builds its own full MIDX.
    let (full_midx_ref, full_midx_hash) = if server_full_midx {
        let full_pack_list: Vec<(String, u64, String, u64)> = head_packs
            .iter()
            .chain(history_packs.iter())
            .cloned()
            .collect();
        build_midx(&full_pack_list)?
    } else {
        (None, String::new())
    };

    // pack_artifacts: every pack the manifest references (HEAD + history), so the
    // ref endpoint can sign each URL — even prior LSM levels (signed by hash).
    let manifest_packs: Vec<&(String, u64, String, u64)> =
        head_packs.iter().chain(history_packs.iter()).collect();
    let pack_artifacts: Vec<crate::PackArtifact> = manifest_packs
        .iter()
        .map(|(p, _, i, _)| crate::PackArtifact {
            pack: p.clone(),
            idx: i.clone(),
        })
        .collect();
    // depth_pack_hashes: only packs built this sync (HEAD + new history) — these
    // are uploaded and then evicted. Prior LSM levels are already durable.
    let depth_pack_hashes: Vec<String> = new_pack_tuples
        .iter()
        .flat_map(|(p, _, i, _)| [p.clone(), i.clone()])
        .collect();

    let archive_chunk_lengths = crate::clonepack::archive_chunk_lengths(&metadata_chunk);
    let archive_chunks: Vec<ChunkRef> = archive_chunk_hashes
        .iter()
        .zip(archive_chunk_lengths.iter())
        .map(|(hash, len)| {
            anyhow::Ok(ChunkRef {
                hash: hash_from_hex(hash)?,
                len: *len,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let make_clonepack = |metadata_hash: String,
                          metadata_len: u64,
                          packs: Vec<crate::clonepack::PackEntry>,
                          midx: Option<ChunkRef>,
                          idx_bundle: Option<ChunkRef>|
     -> Result<ClonepackManifest> {
        Ok(ClonepackManifest {
            commit: commit.clone(),
            parent_commit: parent.clone(),
            default_branch: default_branch.clone(),
            metadata_chunk: Some(ChunkRef {
                hash: hash_from_hex(&metadata_hash)?,
                len: metadata_len,
            }),
            archive_chunks: archive_chunks.clone(),
            packs,
            midx,
            idx_bundle,
            ..Default::default()
        })
    };

    // Full clonepack: HEAD closure + all history. Shallow: HEAD closure only.
    let full_clonepack_manifest = make_clonepack(
        metadata_hash.clone(),
        metadata_data.len() as u64,
        full_entries,
        full_midx_ref.clone(),
        full_idx_bundle_ref.clone(),
    )?;
    let full_clonepack_data = full_clonepack_manifest.encode_to_vec();
    let full_clonepack_hash = cas.put(&full_clonepack_data)?;

    let shallow_clonepack_manifest = make_clonepack(
        shallow_metadata_hash.clone(),
        shallow_metadata_data.len() as u64,
        head_entries,
        head_midx_ref.clone(),
        head_idx_bundle_ref.clone(),
    )?;
    let shallow_clonepack_data = shallow_clonepack_manifest.encode_to_vec();
    let shallow_clonepack_hash = cas.put(&shallow_clonepack_data)?;

    let full_pack = if build_full_pack {
        let mirror_dir6 = mirror_dir.clone();
        let cas6 = cas.clone();
        let commit6 = commit.clone();
        tokio::task::spawn_blocking(move || {
            let builder = PackBuilder::new(&mirror_dir6, &cas6);
            builder.build_full_pack(&commit6).map(|(pack, _idx)| pack)
        })
        .await
        .context("full pack task")??
    } else {
        String::new()
    };

    let info = RefInfo {
        commit: commit.clone(),
        parent_commit: parent.clone(),
        default_branch: default_branch.clone(),
        skeleton_pack: shallow_skeleton_pack.clone(),
        skeleton_idx: shallow_skeleton_idx.clone(),
        head_blobs_pack: String::new(),
        head_blobs_idx: String::new(),
        head_blobs_chunks: Vec::new(),
        packs: pack_artifacts.clone(),
        prebuilt_index: shallow_prebuilt_index.clone(),
        archive: archive_chunk_hashes.first().cloned().unwrap_or_default(),
        manifest: metadata_hash.clone(),
        full_pack,
        clonepack_manifest: full_clonepack_hash.clone(),
        metadata_chunk: metadata_hash.clone(),
        archive_chunks: archive_chunk_hashes.clone(),
        full_clonepack: crate::ClonepackArtifacts {
            manifest: full_clonepack_hash.clone(),
            metadata_chunk: metadata_hash.clone(),
            skeleton_pack: shallow_skeleton_pack.clone(),
            skeleton_idx: shallow_skeleton_idx.clone(),
            prebuilt_index: shallow_prebuilt_index.clone(),
            midx: full_midx_hash.clone(),
            idx_bundle: full_idx_bundle_hash.clone(),
            commit: commit.clone(),
        },
        shallow_clonepack: crate::ClonepackArtifacts {
            manifest: shallow_clonepack_hash.clone(),
            metadata_chunk: shallow_metadata_hash.clone(),
            skeleton_pack: shallow_skeleton_pack.clone(),
            skeleton_idx: shallow_skeleton_idx.clone(),
            prebuilt_index: shallow_prebuilt_index.clone(),
            midx: head_midx_hash.clone(),
            idx_bundle: head_idx_bundle_hash.clone(),
            commit: commit.clone(),
        },
        history_levels: new_levels,
        // Incremental head-pack reuse is two-phase only; single-phase leaves these
        // empty (a later two-phase sync starts its base fresh).
        head_buckets: Vec::new(),
        head_base_commit: String::new(),
        head_base_packs: Vec::new(),
        // Single-phase builds the full archive non-incrementally (build_into_cas);
        // it does not populate the frame index (a later two-phase sync rebuilds).
        archive_frames: Vec::new(),
        build_status: None,
        synced_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs()),
    };

    // Push every built artifact to the configured storage backend. For a local
    // backend this is a no-op (CAS already holds it); for S3/R2/Tigris this
    // makes the artifact durable and available via signed URL. Include both the
    // full and the shallow clonepack artifacts.
    let mut artifact_hashes: Vec<&str> = vec![
        &info.skeleton_pack,
        &info.skeleton_idx,
        &info.head_blobs_idx,
        &info.prebuilt_index,
        &info.manifest,
        &info.clonepack_manifest,
        &info.shallow_clonepack.skeleton_pack,
        &info.shallow_clonepack.skeleton_idx,
        &info.shallow_clonepack.prebuilt_index,
        &info.shallow_clonepack.metadata_chunk,
        &info.shallow_clonepack.manifest,
        &info.full_clonepack.midx,
        &info.shallow_clonepack.midx,
        &info.full_clonepack.idx_bundle,
        &info.shallow_clonepack.idx_bundle,
    ];
    artifact_hashes.extend(info.head_blobs_chunks.iter().map(|s| s.as_str()));
    artifact_hashes.extend(info.archive_chunks.iter().map(|s| s.as_str()));
    // The editable depth packs + their idxs.
    artifact_hashes.extend(depth_pack_hashes.iter().map(|s| s.as_str()));
    info!(
        "sync phase: assemble metadata/idx-bundles/midx/manifests {:?}",
        t.elapsed()
    );
    t = Instant::now();
    // Upload with bounded concurrency instead of one-at-a-time. Each put is a
    // blocking S3 call, so run them on the blocking pool; ~400 MB of serial
    // wall-clock collapses to roughly bandwidth-bound.
    let upload_hashes: Vec<String> = artifact_hashes
        .iter()
        .filter(|h| !h.is_empty())
        .map(|h| h.to_string())
        .collect();
    let upload_count = upload_hashes.len();
    let upload_conc = upload_concurrency();
    futures::stream::iter(upload_hashes.into_iter().map(|hash| {
        let cas = cas.clone();
        let storage = storage.clone();
        async move {
            let read_hash = hash.clone();
            let data = tokio::task::spawn_blocking(move || cas.get(&read_hash))
                .await
                .context("read task")?
                .with_context(|| format!("read artifact {} from CAS for upload", hash))?;
            storage
                .put_async(&hash, &data)
                .await
                .with_context(|| format!("upload artifact {} to storage", hash))
        }
    }))
    .buffer_unordered(upload_conc)
    .try_collect::<Vec<()>>()
    .await?;
    info!(
        "sync phase: upload {} artifacts {:?}",
        upload_count,
        t.elapsed()
    );

    if storage.is_remote() {
        // Object storage is now the source of truth and clients read straight
        // from it via signed URLs. The local CAS copies were only build scratch
        // (a full bun sync writes ~400 MB), so drop them to keep the volume
        // small. They are re-fetched from storage on the rare gateway path.
        //
        // EXCEPT pack idx files: they are tiny and reused every sync to rebuild
        // the idx bundle + MIDX (incl. for prior LSM levels), so keeping them
        // local avoids re-downloading them from object storage on each build.
        let keep_idx: std::collections::HashSet<&str> = new_pack_tuples
            .iter()
            .map(|(_, _, ih, _)| ih.as_str())
            .collect();
        let mut freed = 0u64;
        for hash in artifact_hashes.iter().filter(|h| !h.is_empty()) {
            if keep_idx.contains(*hash) {
                continue;
            }
            if let Ok(sz) = cas.path(hash).metadata().map(|m| m.len()) {
                freed += sz;
            }
            let _ = cas.remove(hash);
        }
        info!(
            "evicted {} local CAS artifacts after upload (~{} MiB freed)",
            artifact_hashes.iter().filter(|h| !h.is_empty()).count(),
            freed / (1024 * 1024)
        );
    } else {
        // Local backend: the CAS is the source of truth — protect the current
        // HEAD's artifacts from retention eviction instead of dropping them.
        // Include every manifest pack (so prior LSM levels, which aren't in the
        // upload set, are also protected).
        let protect_hashes: Vec<String> = artifact_hashes
            .iter()
            .filter(|h| !h.is_empty())
            .map(|h| h.to_string())
            .chain(
                pack_artifacts
                    .iter()
                    .flat_map(|p| [p.pack.clone(), p.idx.clone()]),
            )
            .chain(std::iter::once(info.full_pack.clone()).filter(|h| !h.is_empty()))
            .collect();
        retention.protect(protect_hashes).await;
    }

    let mut info = info;
    info.build_status = None;
    ref_store
        .save_branch(repo_id, branch, &info)
        .await
        .with_context(|| format!("persist ref store for {}@{branch}", repo_id.storage_key()))?;

    info!(
        "synced {} at {} (total build {:?})",
        repo_id.storage_key(),
        &commit[..7],
        t_total.elapsed()
    );
    Ok(info)
}

fn pack_artifacts_of(packs: &[(String, u64, String, u64)]) -> Vec<crate::PackArtifact> {
    packs
        .iter()
        .map(|(p, _, i, _)| crate::PackArtifact {
            pack: p.clone(),
            idx: i.clone(),
        })
        .collect()
}

/// Two-phase publish. Phase 1 (foreground) builds + publishes the depth=1
/// clonepack and returns; phase 2 (background) builds full history and upgrades
/// the full clonepack. depth=0 keeps serving the previous commit until phase 2
/// finishes (option A — never fails, briefly one commit stale).
/// Result of the phase-1 HEAD-closure build: a small delta pack against the
/// immutable base, or a fresh full base on a cold sync / rebase. See
/// `build_head_delta_pack` / `build_head_packs`.
struct HeadBuild {
    /// Every current HEAD pack (base + delta), manifest order — for the clonepack.
    all_packs: Vec<(String, u64, String, u64)>,
    /// Only the packs built this sync (to upload). Reused base packs are durable.
    new_built: Vec<(String, u64, String, u64)>,
    /// The commit whose closure `base_packs` covers (carried, or = commit on cold).
    base_commit: String,
    /// The base packs (closure of `base_commit`), carried unchanged across deltas.
    base_packs: Vec<crate::SizedPack>,
    /// True when every pack was built this sync (cold/rebase) → head MIDX buildable.
    all_local: bool,
}

#[allow(clippy::too_many_arguments)]
async fn build_and_publish_two_phase(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    branch: &str,
    commit: &str,
    parent: Option<String>,
    default_branch: &str,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    inline_full_history: bool,
    t_total: Instant,
) -> Result<RefInfo> {
    let history_target = env_bytes("RIPCLONE_HISTORY_PACK_BYTES", 512 * 1024 * 1024);
    let upload_conc = upload_concurrency();

    // Load the previous synced ref once: used both for the files-table by-diff
    // below and for Option-A full-clonepack carry later in this phase.
    let prev = ref_store.load_branch(repo_id, branch).await.ok().flatten();

    // ---- PHASE 1: HEAD closure + archive + shallow skeleton -> publish depth=1 ----
    let mut t = Instant::now();
    let (md1, c1, cm1) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let shallow_skeleton_handle = tokio::task::spawn_blocking(move || {
        let s = Instant::now();
        let r = PackBuilder::new(&md1, &c1).build_shallow_skeleton_pack(&cm1);
        info!("p1 sub: shallow skeleton {:?}", s.elapsed());
        r
    });
    // Head-closure packs, incremental by delta against an immutable base: keep the
    // base packs (closure of `head_base_commit`) and pack only the depth-1 objects
    // new since that base (`closure(HEAD) − closure(base)`) into a delta pack. The
    // base and delta are disjoint by construction, so no object is ever in two HEAD
    // packs (which would double-materialize a worktree file). A cold sync (no base)
    // packs the full closure as the base. The cumulative delta grows as HEAD moves
    // from the base; phase 2 rebases (rebuilds the base at HEAD) once it exceeds
    // RIPCLONE_HEAD_REBASE_BYTES, off the depth=1 critical path.
    let head_target = env_bytes("RIPCLONE_PACK_BYTES", 12 * 1024 * 1024);
    let prev_base_commit: Option<String> = prev
        .as_ref()
        .map(|p| p.head_base_commit.clone())
        .filter(|c| !c.is_empty());
    let prev_base_packs: Vec<crate::SizedPack> = prev
        .as_ref()
        .map(|p| p.head_base_packs.clone())
        .unwrap_or_default();
    let (md2, c2, cm2) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let head_handle = tokio::task::spawn_blocking(move || -> Result<HeadBuild> {
        let s = Instant::now();
        let b = PackBuilder::new(&md2, &c2);
        match (prev_base_packs.is_empty(), prev_base_commit) {
            // Delta path: a base exists; pack only what is new since the base.
            (false, Some(base_commit)) => {
                let delta = b.build_head_delta_pack(&cm2, &base_commit, head_target)?;
                let mut all_packs: Vec<(String, u64, String, u64)> =
                    prev_base_packs.iter().map(sized_to_tuple).collect();
                all_packs.extend(delta.iter().cloned());
                info!(
                    "p1 sub: head packs (delta vs base: {} new pack(s), {} total) {:?}",
                    delta.len(),
                    all_packs.len(),
                    s.elapsed()
                );
                Ok(HeadBuild {
                    all_packs,
                    new_built: delta,
                    base_commit,
                    base_packs: prev_base_packs,
                    all_local: false,
                })
            }
            // Cold path: no base yet → pack the full closure as the base.
            _ => {
                let base = b.build_head_packs(&cm2, head_target)?;
                let base_packs = base.iter().map(tuple_to_sized).collect();
                info!(
                    "p1 sub: head packs (full base, {} packs) {:?}",
                    base.len(),
                    s.elapsed()
                );
                Ok(HeadBuild {
                    all_packs: base.clone(),
                    new_built: base,
                    base_commit: cm2,
                    base_packs,
                    all_local: true,
                })
            }
        }
    });
    // Phase 1 builds only the cheap files table (no zstd frames): editable
    // depth=1 materializes the worktree from the HEAD-closure packs, so it does
    // not need the archive. The full zstd archive (for files mode) is built in
    // phase 2, off the time-to-depth=1 critical path.
    //
    // Files-table by-diff: when a prior sync exists, reuse its content hashes for
    // unchanged paths and read+hash only the blobs that changed since the prior
    // commit (O(changed) instead of O(worktree)). The no-op fast path in do_sync
    // guarantees commit != prev.commit here, so the diff is non-trivial. Falls
    // back to a full table when there is no prior table.
    let prev_files: Option<Vec<crate::clonepack::FileEntry>> = match prev.as_ref() {
        Some(p) if !p.commit.is_empty() && !p.shallow_clonepack.metadata_chunk.is_empty() => {
            load_metadata_files(cas, storage, &p.shallow_clonepack.metadata_chunk)
        }
        _ => None,
    };
    let prev_commit_for_diff = prev.as_ref().map(|p| p.commit.clone());
    // Carry the prior files table + commit so the bounded archive can hash only
    // changed files and reuse frames for the unchanged prefix/suffix.
    let prev_files_for_archive: Vec<crate::clonepack::FileEntry> =
        prev_files.clone().unwrap_or_default();
    let prev_archive_commit: Option<String> = prev
        .as_ref()
        .map(|p| p.commit.clone())
        .filter(|c| !c.is_empty());
    let (md3, cm3) = (mirror_dir.to_path_buf(), commit.to_string());
    let files_table_handle = match (prev_files, prev_commit_for_diff) {
        (Some(pf), Some(from)) if !from.is_empty() => {
            let (md, cm, frm) = (md3.clone(), cm3.clone(), from);
            tokio::task::spawn_blocking(move || {
                let s = Instant::now();
                // If the diff fails (e.g. prev.commit was pruned after a
                // force-push), fall back to a full rebuild rather than failing
                // the sync — reuse is purely an optimization.
                match crate::git::diff_name_set(&md, &frm, &cm) {
                    Ok(changed) => {
                        let r = ArchiveBuilder::new(&md)
                            .build_files_table_incremental(&cm, &pf, &changed);
                        info!(
                            "p1 sub: files-table (incremental, {} changed) {:?}",
                            changed.len(),
                            s.elapsed()
                        );
                        r
                    }
                    Err(e) => {
                        warn!("files-table diff failed ({e:#}); full rebuild");
                        let r = ArchiveBuilder::new(&md).build_files_table(&cm);
                        info!(
                            "p1 sub: files-table (full, diff fallback) {:?}",
                            s.elapsed()
                        );
                        r
                    }
                }
            })
        }
        _ => tokio::task::spawn_blocking(move || {
            let s = Instant::now();
            let r = ArchiveBuilder::new(&md3).build_files_table(&cm3);
            info!("p1 sub: files-table (full) {:?}", s.elapsed());
            r
        }),
    };
    let (shallow_skeleton_pack, shallow_skeleton_idx) = shallow_skeleton_handle
        .await
        .context("shallow skeleton")??;
    let head_built = head_handle.await.context("head packs")??;
    let head_packs = head_built.all_packs.clone();
    let metadata_base = files_table_handle.await.context("files table")??;
    info!(
        "two-phase p1: head+shallow-skeleton+files-table {:?}",
        t.elapsed()
    );
    t = Instant::now();

    let (md4, c4, cm4, skp) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        commit.to_string(),
        shallow_skeleton_pack.clone(),
    );
    let shallow_prebuilt_index = tokio::task::spawn_blocking(move || {
        PackBuilder::new(&md4, &c4).build_prebuilt_index(&cm4, &skp)
    })
    .await
    .context("shallow prebuilt index")??;

    let mut shallow_meta = metadata_base.clone();
    shallow_meta.skeleton_pack = cas.get(&shallow_skeleton_pack)?;
    shallow_meta.skeleton_idx = cas.get(&shallow_skeleton_idx)?;
    shallow_meta.prebuilt_index = cas.get(&shallow_prebuilt_index)?;
    let shallow_meta_data = shallow_meta.encode_to_vec();
    let shallow_metadata_hash = cas.put(&shallow_meta_data)?;

    // No archive frames in phase 1 (files mode is served by the full variant
    // after phase 2). Editable depth=1 ignores archive chunks.
    let archive_chunks = archive_chunk_refs(&[], &metadata_base)?;
    let head_tagged: Vec<(&(String, u64, String, u64), bool)> =
        head_packs.iter().map(|p| (p, false)).collect();
    let (head_entries, head_idx_bundle_ref, head_idx_bundle_hash) =
        assemble_variant(cas, storage, &head_tagged)?;
    // Ship the head MIDX only on a cold full base (all pack bytes still local).
    // On a delta re-sync the base packs are already evicted, so omit it — the
    // client builds its own MIDX from the per-pack idxs.
    let all_built = head_built.all_local;
    let (head_midx_ref, head_midx_hash) = if all_built {
        assemble_midx(cas, &head_packs)?
    } else {
        (None, String::new())
    };

    let shallow_manifest = make_manifest(
        commit,
        &parent,
        default_branch,
        &archive_chunks,
        &shallow_metadata_hash,
        shallow_meta_data.len() as u64,
        head_entries,
        head_midx_ref,
        head_idx_bundle_ref,
    )?;
    let shallow_clonepack_hash = cas.put(&shallow_manifest.encode_to_vec())?;

    // Option A: carry the previous commit's full clonepack so depth=0 keeps
    // working (one commit stale) until phase 2 publishes the new full. (`prev`
    // was loaded at the top of this phase.)
    let carried_full = prev
        .as_ref()
        .map(|p| p.full_clonepack.clone())
        .unwrap_or_default();
    let carried_full_manifest = prev
        .as_ref()
        .map(|p| p.clonepack_manifest.clone())
        .unwrap_or_default();
    let carried_full_pack = prev
        .as_ref()
        .map(|p| p.full_pack.clone())
        .unwrap_or_default();
    let carried_levels = prev
        .as_ref()
        .map(|p| p.history_levels.clone())
        .unwrap_or_default();
    // Prior sealed levels for phase 2's incremental build (reuse from Tigris).
    let prev_levels_for_p2 = carried_levels.clone();
    // Carry the prior archive frame index so phase 2 can reuse unchanged frames;
    // phase 2 overwrites this with the freshly built index.
    let carried_archive_frames = prev
        .as_ref()
        .map(|p| p.archive_frames.clone())
        .unwrap_or_default();
    let prev_archive_frames_for_p2 = carried_archive_frames.clone();

    let info = RefInfo {
        commit: commit.to_string(),
        parent_commit: parent.clone(),
        default_branch: default_branch.to_string(),
        skeleton_pack: shallow_skeleton_pack.clone(),
        skeleton_idx: shallow_skeleton_idx.clone(),
        head_blobs_pack: String::new(),
        head_blobs_idx: String::new(),
        head_blobs_chunks: Vec::new(),
        packs: pack_artifacts_of(&head_packs),
        prebuilt_index: shallow_prebuilt_index.clone(),
        archive: String::new(),
        manifest: shallow_metadata_hash.clone(),
        full_pack: carried_full_pack,
        clonepack_manifest: carried_full_manifest,
        metadata_chunk: shallow_metadata_hash.clone(),
        archive_chunks: Vec::new(),
        full_clonepack: carried_full,
        shallow_clonepack: crate::ClonepackArtifacts {
            manifest: shallow_clonepack_hash.clone(),
            metadata_chunk: shallow_metadata_hash.clone(),
            skeleton_pack: shallow_skeleton_pack.clone(),
            skeleton_idx: shallow_skeleton_idx.clone(),
            prebuilt_index: shallow_prebuilt_index.clone(),
            midx: head_midx_hash.clone(),
            idx_bundle: head_idx_bundle_hash.clone(),
            commit: commit.to_string(),
        },
        history_levels: carried_levels,
        head_buckets: Vec::new(),
        head_base_commit: head_built.base_commit.clone(),
        head_base_packs: head_built.base_packs.clone(),
        archive_frames: carried_archive_frames,
        build_status: Some("full history building".to_string()),
        synced_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs()),
    };

    // Upload phase-1 artifacts (shallow skeleton/index/metadata, head idx-bundle
    // + midx, shallow manifest, and only the FRESHLY BUILT head packs+idx).
    // Reused bucket packs are already durable in storage from a prior sync.
    let mut p1: Vec<String> = vec![
        shallow_skeleton_pack.clone(),
        shallow_skeleton_idx.clone(),
        shallow_prebuilt_index.clone(),
        shallow_metadata_hash.clone(),
        shallow_clonepack_hash.clone(),
        head_idx_bundle_hash.clone(),
        head_midx_hash.clone(),
    ];
    for (p, _, i, _) in &head_built.new_built {
        p1.push(p.clone());
        p1.push(i.clone());
    }
    p1.retain(|h| !h.is_empty());
    let head_idx_keep: std::collections::HashSet<String> =
        head_packs.iter().map(|(_, _, ih, _)| ih.clone()).collect();
    upload_artifacts(cas, storage, p1.clone(), upload_conc).await?;
    settle_storage(cas, storage, retention, p1, head_idx_keep).await;

    ref_store
        .save_branch(repo_id, branch, &info)
        .await
        .with_context(|| format!("persist depth=1 ref for {}@{branch}", repo_id.storage_key()))?;
    info!(
        "two-phase p1: published depth=1 for {} in {:?} (full history building in background)",
        &commit[..7.min(commit.len())],
        t_total.elapsed()
    );
    let _ = t; // p1 assemble/upload time folded into the total above

    // ---- PHASE 2: full history, in the background (survives the request) ----
    let cas2 = cas.clone();
    let storage2 = storage.clone();
    let ref_store2 = ref_store.clone();
    let retention2 = retention.clone();
    let mirror2 = mirror_dir.to_path_buf();
    let repo_id2 = repo_id.clone();
    let branch2 = branch.to_string();
    let commit2 = commit.to_string();
    let parent2 = parent.clone();
    let default_branch2 = default_branch.to_string();
    let sk_pack = shallow_skeleton_pack.clone();
    let sk_idx = shallow_skeleton_idx.clone();
    let sk_prebuilt = shallow_prebuilt_index.clone();
    let sk_meta = shallow_metadata_hash.clone();
    let sk_meta_len = shallow_meta_data.len() as u64;
    let head_base_pack_count_for_p2 = head_built.base_packs.len();
    let phase2 = async move {
        let started = Instant::now();
        let res = build_full_in_background(
            &cas2,
            &mirror2,
            &repo_id2,
            &branch2,
            &commit2,
            parent2,
            &default_branch2,
            &ref_store2,
            &storage2,
            &retention2,
            head_packs,
            sk_pack,
            sk_idx,
            sk_prebuilt,
            sk_meta,
            sk_meta_len,
            head_idx_bundle_hash,
            head_midx_hash,
            history_target,
            upload_conc,
            prev_levels_for_p2,
            prev_archive_frames_for_p2,
            prev_files_for_archive,
            prev_archive_commit,
            head_base_pack_count_for_p2,
        )
        .await;
        match &res {
            Ok(()) => info!(
                "full clone ready for {} in {:?}",
                &commit2[..7.min(commit2.len())],
                started.elapsed()
            ),
            Err(e) => error!(
                "full clone build failed for {}: {e:#}",
                repo_id2.storage_key()
            ),
        }
        res
    };

    if inline_full_history {
        // Ephemeral/cross-process worker: build phase 2 now, before returning, so
        // the job is never acked `done` while the full clonepack is still unbuilt
        // (A3). This runs under the caller's `repo_lock` (process_build_job holds
        // it across the whole do_sync), so phase 2 is serialized against other
        // syncs for this repo. A crash mid-build leaves the claim stale → the
        // queue reclaims and retries it (and dead-letters after the cap).
        phase2.await.context("phase 2 (full history) build")?;
    } else {
        // Long-lived in-process server: detach so `/sync` returns as soon as the
        // depth=1 clonepack is live. The task outlives the request because the
        // server process keeps running.
        tokio::spawn(phase2);
    }

    Ok(info)
}

/// Phase 2 of two-phase publish: build the full-history artifacts and upgrade
/// the ref's full clonepack. The depth=1 clonepack is already live.
#[allow(clippy::too_many_arguments)]
async fn build_full_in_background(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    branch: &str,
    commit: &str,
    parent: Option<String>,
    default_branch: &str,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    head_packs: Vec<(String, u64, String, u64)>,
    // Phase 1's shallow skeleton + prebuilt index (HEAD trees + HEAD index). The
    // full variant reuses these — the full history's commits+trees are already in
    // the history packs, so a separate full skeleton is redundant. (hashes)
    shallow_skeleton_pack: String,
    shallow_skeleton_idx: String,
    shallow_prebuilt_index: String,
    // Phase 1's shallow metadata chunk (files table + skeleton, no archive frames).
    // The editable full clonepack (phase 2a) reuses it verbatim: an editable depth=0
    // clone reads only the files table + packs, never the archive frames. Phase 2b
    // builds a frames-bearing metadata for files mode.
    shallow_metadata_hash: String,
    shallow_metadata_len: u64,
    _head_idx_bundle_hash: String,
    _head_midx_hash: String,
    history_target: u64,
    upload_conc: usize,
    prev_levels: Vec<crate::HistoryLevel>,
    prev_archive_frames: Vec<crate::ArchiveFrame>,
    // The prior sync's files table + commit, for the bounded archive: it hashes
    // only changed files and reuses frames for the unchanged prefix/suffix.
    prev_files: Vec<crate::clonepack::FileEntry>,
    prev_archive_commit: Option<String>,
    // How many of `head_packs` (above) are base packs; the rest are the cumulative
    // delta. When the delta's byte size exceeds RIPCLONE_HEAD_REBASE_BYTES this
    // phase rebases — rebuilds a fresh base at the current commit (off the depth=1
    // critical path) — so the delta never grows unbounded.
    head_base_pack_count: usize,
) -> Result<()> {
    // Incremental history: build only the tail past the last sealed level; prior
    // levels are reused by hash from object storage (Tigris) — never rebuilt.
    let lsm_cfg = lsm_config();
    let sealed_tip: Option<String> = if lsm_cfg.enabled {
        prev_levels.last().map(|l| l.tip_commit.clone())
    } else {
        None
    };

    // Write a reachability bitmap once, before the heavy full enumerations
    // (skeleton + history). This is in the background phase, so it never delays
    // the depth=1 publish. Best-effort.
    let bm_dir = mirror_dir.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || git::write_bitmap(&bm_dir)).await;

    // History tail + the full zstd archive (deferred from phase 1), concurrently.
    // No full skeleton: the full variant reuses phase 1's shallow skeleton, and
    // the full history's commits+trees live in the history packs.
    //
    // The archive reuses unchanged frames (their raw bytes hash the same, so the
    // prior compressed chunk is reused — no recompress, no re-upload). When a prior
    // commit + frames are known, the bounded build also skips *reading* the
    // unchanged prefix/suffix and only touches the changed middle; it falls back to
    // the full read on its own when that doesn't apply. RIPCLONE_ARCHIVE_BOUNDED=0
    // forces the full read.
    let bounded = std::env::var("RIPCLONE_ARCHIVE_BOUNDED")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
        && !prev_archive_frames.is_empty()
        && prev_archive_commit.is_some();
    let prev_frame_map: std::collections::HashMap<String, (String, u64)> = prev_archive_frames
        .iter()
        .map(|f| (f.raw_hash.clone(), (f.chunk_hash.clone(), f.compressed_len)))
        .collect();
    let (mda, ca, cma) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let archive_handle = tokio::task::spawn_blocking(move || {
        let b = ArchiveBuilder::new(&mda);
        if bounded {
            b.build_into_cas_bounded(
                &cma,
                &ca,
                6,
                None,
                &prev_archive_frames,
                &prev_files,
                prev_archive_commit.as_deref().unwrap_or_default(),
            )
        } else {
            b.build_into_cas_incremental(&cma, &ca, 6, None, &prev_frame_map)
        }
    });
    let (md2, c2, cm2, st2, lsm2) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        commit.to_string(),
        sealed_tip.clone(),
        lsm_cfg.enabled,
    );
    type BuiltHistory = (Vec<(String, u64, String, u64)>, bool);
    let history_handle = tokio::task::spawn_blocking(move || -> Result<BuiltHistory> {
        let b = PackBuilder::new(&md2, &c2);
        if lsm2 {
            let tail = b.build_history_tail(&cm2, st2.as_deref(), history_target)?;
            Ok((tail, true))
        } else {
            Ok((b.build_history_packs(&cm2, history_target)?, false))
        }
    });
    // History is enough to publish an editable clone: it reads only the files
    // table and the packs, never the archive. So publish as soon as history is
    // ready instead of waiting for the zstd archive (which only files mode needs).
    let t_editable = Instant::now();
    let (built_history, is_tail) = history_handle.await.context("history packs")??;

    // Once the cumulative delta grows past the threshold, rebuild a fresh base at
    // the current commit. depth=1 is already live, so this never blocks a clone.
    // The fresh base is kept only if the ref still points at our commit.
    let rebase_bytes = env_bytes("RIPCLONE_HEAD_REBASE_BYTES", 128 * 1024 * 1024);
    let delta_bytes: u64 = head_packs
        .iter()
        .skip(head_base_pack_count)
        .map(|(_, pack_len, _, _)| *pack_len)
        .sum();
    let (head_packs, new_head_packs, rebased_base): (
        Vec<(String, u64, String, u64)>,
        Vec<(String, u64, String, u64)>,
        Option<Vec<crate::SizedPack>>,
    ) = if delta_bytes >= rebase_bytes {
        let head_target = env_bytes("RIPCLONE_PACK_BYTES", 12 * 1024 * 1024);
        let (mdc, cc, cmc) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
        let base = tokio::task::spawn_blocking(move || {
            PackBuilder::new(&mdc, &cc).build_head_packs(&cmc, head_target)
        })
        .await
        .context("rebase head base")??;
        info!(
            "rebased HEAD base ({} MiB delta -> fresh base of {} packs)",
            delta_bytes / (1024 * 1024),
            base.len()
        );
        let sized: Vec<crate::SizedPack> = base.iter().map(tuple_to_sized).collect();
        (base.clone(), base, Some(sized))
    } else {
        (head_packs, Vec::new(), None)
    };

    // Flatten the history levels for the manifest; collect the freshly built packs
    // to upload and the levels to persist.
    let (history_packs, new_history_tuples, new_levels) = if is_tail {
        seal_and_compact(
            mirror_dir,
            cas,
            commit,
            prev_levels,
            sealed_tip,
            built_history,
            history_target,
            &lsm_cfg,
        )
        .await?
    } else {
        (built_history.clone(), built_history, Vec::new())
    };

    // Pack entries + idx bundle over head + history. Built once; the files manifest
    // below reuses them, since the packs are the same. MIDX is omitted (head packs
    // were evicted) — the client builds it.
    let full_tagged: Vec<(&(String, u64, String, u64), bool)> = head_packs
        .iter()
        .map(|p| (p, false))
        .chain(history_packs.iter().map(|p| (p, true)))
        .collect();
    let (full_entries, full_idx_bundle_ref, full_idx_bundle_hash) =
        assemble_variant(cas, storage, &full_tagged)?;

    // The shallow metadata already has the files table and skeleton, and an
    // editable clone ignores the archive, so reuse it and leave archive_chunks
    // empty.
    let editable_manifest = make_manifest(
        commit,
        &parent,
        default_branch,
        &[],
        &shallow_metadata_hash,
        shallow_metadata_len,
        full_entries.clone(),
        None,
        full_idx_bundle_ref.clone(),
    )?;
    let editable_clonepack_hash = cas.put(&editable_manifest.encode_to_vec())?;

    // Upload the history packs+idx, the idx bundle, the manifest, and any rebase
    // base. Non-rebase head packs + the shallow skeleton are already in storage.
    let mut uploads: Vec<String> = vec![
        editable_clonepack_hash.clone(),
        full_idx_bundle_hash.clone(),
    ];
    for (p, _, i, _) in &new_history_tuples {
        uploads.push(p.clone());
        uploads.push(i.clone());
    }
    for (p, _, i, _) in &new_head_packs {
        uploads.push(p.clone());
        uploads.push(i.clone());
    }
    uploads.retain(|h| !h.is_empty());
    let idx_keep: std::collections::HashSet<String> = new_history_tuples
        .iter()
        .map(|(_, _, ih, _)| ih.clone())
        .chain(new_head_packs.iter().map(|(_, _, ih, _)| ih.clone()))
        .collect();
    upload_artifacts(cas, storage, uploads.clone(), upload_conc).await?;

    // Publish the editable full clonepack. archive_chunks stays empty until the
    // archive is built below; a files clone waits for it.
    {
        let mut info = ref_store
            .load_branch(repo_id, branch)
            .await?
            .ok_or_else(|| anyhow::anyhow!("ref vanished before editable publish"))?;
        let mut all_packs = head_packs.clone();
        all_packs.extend(history_packs.iter().cloned());
        info.packs = pack_artifacts_of(&all_packs);
        info.skeleton_pack = shallow_skeleton_pack.clone();
        info.skeleton_idx = shallow_skeleton_idx.clone();
        info.prebuilt_index = shallow_prebuilt_index.clone();
        info.metadata_chunk = shallow_metadata_hash.clone();
        info.manifest = shallow_metadata_hash.clone();
        info.archive = String::new();
        info.archive_chunks = Vec::new();
        info.clonepack_manifest = editable_clonepack_hash.clone();
        info.full_clonepack = crate::ClonepackArtifacts {
            manifest: editable_clonepack_hash.clone(),
            metadata_chunk: shallow_metadata_hash.clone(),
            skeleton_pack: shallow_skeleton_pack.clone(),
            skeleton_idx: shallow_skeleton_idx.clone(),
            prebuilt_index: shallow_prebuilt_index.clone(),
            midx: String::new(),
            idx_bundle: full_idx_bundle_hash.clone(),
            commit: commit.to_string(),
        };
        info.history_levels = new_levels;
        // The head base is owned by the depth=1 build, which writes it consistently
        // with the commit. Only adopt a rebase here if the ref still points at our
        // commit; otherwise a newer sync owns it.
        if let Some(sized) = rebased_base
            && info.commit == commit
        {
            info.head_buckets = Vec::new();
            info.head_base_commit = commit.to_string();
            info.head_base_packs = sized;
        }
        info.build_status = Some("archive building".to_string());
        ref_store
            .save_branch(repo_id, branch, &info)
            .await
            .with_context(|| {
                format!(
                    "persist editable ref for {}@{branch}",
                    repo_id.storage_key()
                )
            })?;
    }
    settle_storage(cas, storage, retention, uploads, idx_keep).await;
    info!(
        "published editable full clone for {} in {:?}",
        &commit[..7.min(commit.len())],
        t_editable.elapsed()
    );

    // Test hook: hold the archive back so a test can observe the editable clone
    // being ready while files mode is not.
    if let Ok(ms) = std::env::var("RIPCLONE_TEST_ARCHIVE_DELAY_MS")
        && let Ok(ms) = ms.parse::<u64>()
    {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }

    // Now the zstd archive, which files mode needs.
    let t_archive = Instant::now();
    let (archive_chunk_hashes, archive_meta, new_archive_chunks, archive_frames) =
        archive_handle.await.context("full archive")??;
    info!(
        "archive {} frames ({} rebuilt)",
        archive_frames.len(),
        new_archive_chunks.len()
    );
    let fetch = |h: &str| -> Result<Vec<u8>> { cas.get(h).or_else(|_| storage.get(h)) };
    let mut full_meta = archive_meta;
    full_meta.skeleton_pack = fetch(&shallow_skeleton_pack)?;
    full_meta.skeleton_idx = fetch(&shallow_skeleton_idx)?;
    full_meta.prebuilt_index = fetch(&shallow_prebuilt_index)?;
    let full_meta_data = full_meta.encode_to_vec();
    let files_metadata_hash = cas.put(&full_meta_data)?;
    let archive_chunks = archive_chunk_refs(&archive_chunk_hashes, &full_meta)?;
    // Same packs + idx bundle as the editable manifest, now with the archive.
    let files_manifest = make_manifest(
        commit,
        &parent,
        default_branch,
        &archive_chunks,
        &files_metadata_hash,
        full_meta_data.len() as u64,
        full_entries,
        None,
        full_idx_bundle_ref,
    )?;
    let files_clonepack_hash = cas.put(&files_manifest.encode_to_vec())?;

    let mut uploads: Vec<String> = vec![files_metadata_hash.clone(), files_clonepack_hash.clone()];
    uploads.extend(new_archive_chunks.iter().cloned());
    uploads.retain(|h| !h.is_empty());
    upload_artifacts(cas, storage, uploads.clone(), upload_conc).await?;

    // Add the archive to the full clonepack, only if the ref still points at our
    // commit (a newer sync owns it otherwise, and brings its own archive).
    {
        let mut info = ref_store
            .load_branch(repo_id, branch)
            .await?
            .ok_or_else(|| anyhow::anyhow!("ref vanished before archive publish"))?;
        if info.commit == commit {
            info.metadata_chunk = files_metadata_hash.clone();
            info.manifest = files_metadata_hash.clone();
            info.archive = archive_chunk_hashes.first().cloned().unwrap_or_default();
            info.archive_chunks = archive_chunk_hashes.clone();
            info.clonepack_manifest = files_clonepack_hash.clone();
            info.full_clonepack.manifest = files_clonepack_hash.clone();
            info.full_clonepack.metadata_chunk = files_metadata_hash.clone();
            info.archive_frames = archive_frames;
            info.build_status = None;
            ref_store
                .save_branch(repo_id, branch, &info)
                .await
                .with_context(|| {
                    format!("persist files ref for {}@{branch}", repo_id.storage_key())
                })?;
        }
    }
    settle_storage(
        cas,
        storage,
        retention,
        uploads,
        std::collections::HashSet::new(),
    )
    .await;
    info!(
        "published files archive for {} in {:?}",
        &commit[..7.min(commit.len())],
        t_archive.elapsed()
    );
    Ok(())
}

/// Run one build to completion: mark `building` in the metadata store, sync,
/// then mark `done`/`failed`. Returns the result string so the caller can signal
/// in-process waiters (local queue) or ack the job (worker process).
///
/// This is the unit of work shared by the in-process worker loop and the
/// standalone `ripclone-worker`. It touches only the durable backends + provider
/// registry, so it runs unchanged in any process that shares the same storage,
/// metadata store, and provider config.
pub async fn process_build_job(state: &ServerState, job: &BuildJob) -> Result<(), String> {
    let repo_id = &job.repo_id;
    let branch = &job.branch;
    let at_rev = job.rev.clone();

    // Mark as building in the shared metadata store.
    let _ = update_build_status(state, repo_id, "building").await;
    invalidate_ref_response_cache(state, repo_id, branch);

    let start = std::time::Instant::now();
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let provider = match state.provider_registry.get(repo_id.provider.as_str()) {
        Some(p) => p.clone(),
        None => {
            let _ = update_build_status(state, repo_id, "error").await;
            warn!(
                "unknown provider {} for build job",
                repo_id.provider.as_str()
            );
            return Err(format!("unknown provider {}", repo_id.provider.as_str()));
        }
    };
    let lock = repo_lock(&state.sync_locks, repo_id).await;
    let _guard = lock.lock().await;
    // An ephemeral cross-process worker (SQL queue, `inproc_wait() == false`)
    // must finish the two-phase full history before it acks `done`, or a worker
    // that exits after acking would lose the detached phase-2 task (A3). The
    // in-process server keeps phase 2 in the background for a fast `/sync`.
    let inline_full_history = !state.build_queue.inproc_wait();
    let result = do_sync(
        &state.cas,
        &mirror_dir,
        repo_id,
        branch,
        at_rev.as_deref(),
        &state.ref_store,
        false,
        inline_full_history,
        &state.storage,
        &state.retention,
        &provider,
        job.credential.as_ref(),
    )
    .await;
    drop(_guard);

    // Resolve HEAD to the concrete default branch for cache/log keys.
    let effective_branch = match &result {
        Ok(info) if branch == "HEAD" => info.default_branch.clone(),
        _ => branch.clone(),
    };
    match &result {
        Ok(info) => {
            state.metrics.record_build_completed(start.elapsed());
            // Cross-process resolution: a server that didn't run this build has no
            // local mirror, so it cannot map a requested `HEAD` to the concrete
            // default branch `do_sync` stored the ref under. Persist the real ref
            // under the literal `HEAD` key too (plain HEAD request, no rev
            // override) so any process can resolve `/sync HEAD` from the shared
            // metadata store alone. The `HEAD` row already exists (build_status
            // placeholder) and already shows in `list_branches`, so this only
            // fills it with real data. `update_build_status` below then stamps
            // `done` onto it.
            if branch == "HEAD"
                && at_rev.is_none()
                && effective_branch != "HEAD"
                && let Err(e) = state.ref_store.save_branch(repo_id, "HEAD", info).await
            {
                warn!(
                    "failed to write HEAD ref alias for {}: {e}",
                    repo_id.storage_key()
                );
            }
            let _ = update_build_status(state, repo_id, "done").await;
            // A successful sync marks the mirror fresh so a following resolve
            // doesn't re-fetch. Stamp both the concrete branch and the original
            // requested branch (e.g. HEAD).
            stamp_mirror_fresh(
                state,
                &format!("{}/{effective_branch}", repo_id.storage_key()),
            );
            if branch != &effective_branch {
                stamp_mirror_fresh(state, &format!("{}/{branch}", repo_id.storage_key()));
            }
            invalidate_ref_response_cache(state, repo_id, &effective_branch);
            info!(
                "background build completed for {}@{effective_branch}",
                repo_id.storage_key()
            );
            Ok(())
        }
        Err(e) => {
            state.metrics.record_build_failed();
            let _ = update_build_status(state, repo_id, &format!("failed: {e}")).await;
            invalidate_ref_response_cache(state, repo_id, &effective_branch);
            warn!(
                "background build failed for {}@{effective_branch}: {e}",
                repo_id.storage_key()
            );
            Err(format!("{e}"))
        }
    }
}

/// Spawn the in-process worker loop for the local queue. Each finished job
/// decrements the shared depth counter and signals any coalesced `/sync` waiters
/// via their oneshots.
fn spawn_build_worker(state: ServerState, mut rx: tokio::sync::mpsc::Receiver<BuildJob>) {
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            // The waiter key must match the enqueue key, which includes the rev
            // override. Compute it before the job is moved into the build task.
            let key = format!(
                "{}/{}#{}",
                job.repo_id.storage_key(),
                job.branch,
                job.rev.as_deref().unwrap_or("")
            );
            // Isolate the build so a panic fails just this job (signalling its
            // waiters) instead of killing the worker and wedging the queue.
            let st = state.clone();
            let result = match tokio::spawn(async move { process_build_job(&st, &job).await }).await
            {
                Ok(r) => r,
                Err(e) => Err(format!("build task panicked: {e}")),
            };
            state.build_queue_depth.fetch_sub(1, Ordering::Relaxed);
            if let Some(senders) = state.build_waiters.lock().await.remove(&key) {
                for s in senders {
                    let _ = s.send(result.clone());
                }
            }
        }
    });
}

async fn update_build_status(state: &ServerState, repo_id: &RepoId, status: &str) -> Result<()> {
    let mut info = match state.ref_store.load(repo_id).await? {
        Some(info) => info,
        None => RefInfo {
            commit: String::new(),
            parent_commit: None,
            default_branch: String::new(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: Vec::new(),
            packs: Vec::new(),
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            full_pack: String::new(),
            clonepack_manifest: String::new(),
            metadata_chunk: String::new(),
            archive_chunks: Vec::new(),
            full_clonepack: crate::ClonepackArtifacts::default(),
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            head_buckets: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            synced_at: None,
        },
    };
    info.build_status = Some(status.to_string());
    state.ref_store.save(repo_id, &info).await?;
    Ok(())
}

/// Hash the auth token, or fail if it is missing/empty. Pure (no env access) so
/// it is unit-testable without starting a server or touching global state.
fn auth_token_hash(raw: Option<String>) -> Result<String> {
    raw.filter(|t| !t.is_empty())
        .map(|t| format!("{:x}", Sha256::digest(t.as_bytes())))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "RIPCLONE_SERVER_TOKEN is not set. Refusing to start an unauthenticated server."
            )
        })
}

/// Read the server auth token from the environment.
///
/// Precedence:
///   1. RIPCLONE_SERVER_TOKEN_HASH (already hashed)
///   2. RIPCLONE_SERVER_TOKEN (raw)
///   3. RIPCLONE_TOKEN_HASH (deprecated, already hashed)
///   4. RIPCLONE_TOKEN (deprecated, raw)
fn read_server_auth_token() -> Result<String> {
    if let Some(hash) = env::var("RIPCLONE_SERVER_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
    {
        return Ok(hash);
    }
    if let Some(raw) = env::var("RIPCLONE_SERVER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
    {
        return Ok(format!("{:x}", Sha256::digest(raw.as_bytes())));
    }
    if let Some(hash) = env::var("RIPCLONE_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
    {
        eprintln!(
            "warning: RIPCLONE_TOKEN_HASH is deprecated for server auth; use RIPCLONE_SERVER_TOKEN_HASH"
        );
        return Ok(hash);
    }
    if let Some(raw) = env::var("RIPCLONE_TOKEN").ok().filter(|t| !t.is_empty()) {
        eprintln!(
            "warning: RIPCLONE_TOKEN is deprecated for server auth; use RIPCLONE_SERVER_TOKEN"
        );
        return Ok(format!("{:x}", Sha256::digest(raw.as_bytes())));
    }
    auth_token_hash(None)
}

pub async fn run_server(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    host: &str,
    port: u16,
) -> Result<()> {
    std::fs::create_dir_all(cas_dir)?;
    std::fs::create_dir_all(repo_root)?;

    let token_hash = read_server_auth_token()?;
    info!("server auth token configured; auth middleware enabled");

    let provider_registry = ProviderRegistry::load().context("load provider registry")?;
    info!(
        "provider registry loaded with {} instance(s)",
        provider_registry.iter().count()
    );
    let broker: Arc<dyn CredentialBroker> = Arc::new(StaticBroker::new(provider_registry.clone()));

    let rate_burst: u32 = env::var("RIPCLONE_RATE_LIMIT_BURST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let rate_per_sec: f64 = env::var("RIPCLONE_RATE_LIMIT_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10.0);
    let rate_limiter = RateLimiter::new(rate_burst, rate_per_sec);
    info!(
        "rate limiter enabled: burst={}, restore={}/s",
        rate_burst, rate_per_sec
    );

    let metrics = Metrics::new();
    // Pluggable storage + metadata store + retention, shared with ripclone-worker.
    let b = backends::Backends::from_env(cas_dir, repo_root, &metrics).await?;
    let retention_interval: Duration = env::var("RIPCLONE_RETENTION_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(300));
    Retention::clone(&b.retention).spawn(retention_interval);

    let remote_gc_interval: Duration = env::var("RIPCLONE_REMOTE_GC_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(0));
    let remote_gc = RemoteGc::new(b.storage.clone(), b.ref_store.clone(), GcConfig::from_env());
    remote_gc.spawn(remote_gc_interval);

    let refs_path = repo_root.join(".ripclone-refs.json");
    if let Err(e) = migrate_legacy_refs(b.ref_store.as_ref(), &refs_path).await {
        warn!("failed to migrate legacy refs: {}", e);
    }

    let oidc_audience = env::var("RIPCLONE_OIDC_AUDIENCE")
        .ok()
        .filter(|t| !t.is_empty());
    let oidc_verifier = oidc_audience.map(OidcVerifier::new);
    if oidc_verifier.is_some() {
        info!("OIDC verification enabled for audience configured via RIPCLONE_OIDC_AUDIENCE");
    }

    // Select the queue backend. The local queue drives an in-process worker; the
    // SQL queues' builds run in separate ripclone-worker processes, so the server
    // only enqueues.
    let (build_queue, build_queue_depth, local_rx) = match backends::select_queue().await? {
        QueueBackend::Local { queue, rx, depth } => (queue, depth, Some(rx)),
        QueueBackend::Sql { queue } => (queue, Arc::new(AtomicUsize::new(0)), None),
    };

    // Webhook receiver config (per-provider secret + optional allowlist). Built
    // before the registry is moved into the state.
    let webhook_config = Arc::new(WebhookConfig::from_env(&provider_registry));

    let state = ServerState {
        cas: b.cas,
        storage: b.storage,
        repo_root: repo_root.to_path_buf(),
        ref_store: b.ref_store,
        provider_registry,
        broker,
        token_hash: Some(token_hash),
        metrics,
        rate_limiter,
        retention: b.retention,
        build_queue,
        build_queue_depth,
        build_waiters: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        oidc_verifier,
        sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        mirror_freshness: Arc::new(std::sync::Mutex::new(HashMap::new())),
        mirror_fresh_ttl: Duration::from_secs(
            env::var("RIPCLONE_MIRROR_FRESH_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
        ),
        ref_response_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
        artifact_fetch_count: Arc::new(AtomicUsize::new(0)),
        fail_first_fetches: fail_first_fetches_from_env(),
        readyz_cache: Arc::new(std::sync::Mutex::new(None)),
        access_verifier: Arc::new(HttpAccessVerifier::new()),
        require_repo_auth: require_repo_auth_from_env(),
        webhook_config,
    };

    // Only the local queue runs builds in-process.
    if let Some(rx) = local_rx {
        spawn_build_worker(state.clone(), rx);
    }

    let app = build_app(state);
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    if require_repo_auth_from_env() {
        info!(
            "per-repo access enforcement ON: private repos require the caller's credential on every read (RIPCLONE_TRUST_GATEWAY=1 to disable for single-tenant self-host)"
        );
    } else {
        warn!(
            "per-repo access enforcement OFF (RIPCLONE_TRUST_GATEWAY): any holder of the shared server token can read any cached repo — keep this backend network-isolated and single-tenant"
        );
    }
    info!("ripclone server listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::util::ServiceExt;

    fn test_state(tmp: &tempfile::TempDir) -> ServerState {
        let cas_root = tmp.path().join("cas");
        let cas = Cas::new(&cas_root).unwrap();
        let storage = crate::storage::local(&cas_root).unwrap();
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&repo_root).unwrap();
        let ref_store: Arc<dyn RefStore> =
            Arc::new(crate::ref_store::FileRefStore::new(&repo_root));
        let token_hash = format!("{:x}", Sha256::digest("secret"));
        let metrics = Metrics::new();
        let retention = Arc::new(Retention::new(cas.clone(), metrics.clone()).unwrap());
        let (local_queue, _build_rx, _depth) = crate::queue::LocalJobQueue::new(16);
        let build_queue: JobQueueRef = Arc::new(local_queue);
        let provider_registry = ProviderRegistry::new();
        let broker: Arc<dyn CredentialBroker> =
            Arc::new(StaticBroker::new(provider_registry.clone()));
        ServerState {
            cas,
            storage,
            repo_root,
            ref_store,
            provider_registry,
            broker,
            token_hash: Some(token_hash),
            metrics,
            rate_limiter: RateLimiter::new(100, 100.0),
            retention,
            build_queue,
            build_queue_depth: Arc::new(AtomicUsize::new(0)),
            build_waiters: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            oidc_verifier: None,
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            mirror_freshness: Arc::new(std::sync::Mutex::new(HashMap::new())),
            mirror_fresh_ttl: Duration::from_secs(60),
            ref_response_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            artifact_fetch_count: Arc::new(AtomicUsize::new(0)),
            fail_first_fetches: fail_first_fetches_from_env(),
            readyz_cache: Arc::new(std::sync::Mutex::new(None)),
            // Default tests to single-tenant trust (no network access checks);
            // the authz-specific tests override these two fields with a fake.
            access_verifier: Arc::new(HttpAccessVerifier::new()),
            require_repo_auth: false,
            // No webhook secret by default; webhook tests install one explicitly.
            webhook_config: Arc::new(WebhookConfig::empty()),
        }
    }

    fn auth_header() -> String {
        format!("Ripclone {:x}", Sha256::digest("secret"))
    }

    #[test]
    fn validate_repo_id_accepts_github_identifiers() {
        assert!(validate_repo_id("ripclone").is_ok());
        assert!(validate_repo_id("ripclone-rs").is_ok());
        assert!(validate_repo_id("ripclone.rs").is_ok());
        assert!(validate_repo_id("rip_clone").is_ok());
    }

    #[test]
    fn validate_repo_id_rejects_path_traversal() {
        assert!(validate_repo_id("..").is_err());
        assert!(validate_repo_id("foo/bar").is_err());
        assert!(validate_repo_id("foo\\bar").is_err());
        assert!(validate_repo_id("foo\0bar").is_err());
        assert!(validate_repo_id("").is_err());
    }

    #[test]
    fn auth_token_hash_requires_a_nonempty_token() {
        // Missing or empty token must be rejected with a clear message...
        for missing in [None, Some(String::new())] {
            let err = auth_token_hash(missing).unwrap_err().to_string();
            assert!(
                err.contains("RIPCLONE_SERVER_TOKEN"),
                "error should mention missing token: {err}"
            );
        }
        // ...and a real token hashes to the same digest the auth middleware checks.
        let hash = auth_token_hash(Some("secret".to_string())).unwrap();
        assert_eq!(hash, format!("{:x}", Sha256::digest("secret")));
    }

    #[test]
    fn read_server_auth_token_prefers_new_env_vars() {
        // Clean deprecated vars.
        unsafe {
            env::remove_var("RIPCLONE_TOKEN");
            env::remove_var("RIPCLONE_TOKEN_HASH");
            env::remove_var("RIPCLONE_SERVER_TOKEN");
            env::remove_var("RIPCLONE_SERVER_TOKEN_HASH");
        }
        unsafe { env::set_var("RIPCLONE_SERVER_TOKEN", "new-secret") };
        assert_eq!(
            read_server_auth_token().unwrap(),
            format!("{:x}", Sha256::digest("new-secret"))
        );
        unsafe { env::set_var("RIPCLONE_SERVER_TOKEN_HASH", "prefixed-hash") };
        assert_eq!(read_server_auth_token().unwrap(), "prefixed-hash");
        unsafe {
            env::remove_var("RIPCLONE_SERVER_TOKEN");
            env::remove_var("RIPCLONE_SERVER_TOKEN_HASH");
        }
    }

    #[test]
    fn rate_limiter_keys_by_ip_and_is_bounded() {
        let limiter = RateLimiter::new(10, 10.0);
        let first = "192.168.1.1";
        let second = "192.168.1.2";
        assert!(limiter.check(first));
        assert!(limiter.check(second));

        // Exhaust the burst for a third IP and ensure it is rejected.
        let third = "192.168.1.3";
        for _ in 0..10 {
            assert!(limiter.check(third));
        }
        assert!(!limiter.check(third));

        // Many distinct IPs should not grow the map without bound.
        for i in 0..20_000u64 {
            let ip = format!("10.0.{}. {}", i / 256, i % 256);
            limiter.check(&ip);
        }
        let len = limiter.buckets.lock().unwrap().len();
        assert!(len <= 10_000, "rate limiter map grew unbounded: {}", len);
    }

    #[test]
    fn rate_limit_key_collapses_ipv6_and_honors_trusted_forwarded() {
        use std::net::Ipv6Addr;
        let socket = SocketAddr::from(([203, 0, 113, 7], 51000));

        // Untrusted: always the socket IP, ignore any forwarded-for header.
        let mut spoof = HeaderMap::new();
        spoof.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());
        assert_eq!(rate_limit_key(&spoof, socket, false), "203.0.113.7");

        // Trusted: take the rightmost forwarded-for entry (what our proxy saw),
        // ignoring entries a client prepends.
        let mut xff = HeaderMap::new();
        xff.insert("x-forwarded-for", "9.9.9.9, 198.51.100.23".parse().unwrap());
        assert_eq!(rate_limit_key(&xff, socket, true), "198.51.100.23");

        // IPv6 collapses to its /64 so an attacker can't rotate within a /64.
        let a = SocketAddr::new(
            std::net::IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xab, 0xcd, 1, 2, 3, 4)),
            0,
        );
        let b = SocketAddr::new(
            std::net::IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xab, 0xcd, 9, 9, 9, 9)),
            0,
        );
        let ka = rate_limit_key(&HeaderMap::new(), a, false);
        let kb = rate_limit_key(&HeaderMap::new(), b, false);
        assert_eq!(ka, kb, "same /64 must share a bucket");
        assert_eq!(ka, "2001:db8:ab:cd::/64");
    }

    #[test]
    fn local_storage_does_not_produce_signed_urls() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = crate::storage::local(tmp.path()).unwrap();
        let info = RefInfo {
            commit: "abc".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: Vec::new(),
            packs: Vec::new(),
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            full_pack: String::new(),
            clonepack_manifest: "manifest".to_string(),
            metadata_chunk: "metadata".to_string(),
            archive_chunks: vec!["chunk1".to_string(), "chunk2".to_string()],
            full_clonepack: crate::ClonepackArtifacts::default(),
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            head_buckets: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            synced_at: None,
        };
        let provider = ProviderRegistry::new().default_provider().clone();
        let repo_id = RepoId::github("o/r");
        let resp = ref_response(
            &repo_id,
            &provider,
            "main".to_string(),
            &info,
            &storage,
            "full",
            false,
        );
        assert!(resp.clonepack_manifest_url.is_none());
        assert!(resp.metadata_chunk_url.is_none());
        assert!(resp.archive_chunk_urls.is_none());
    }

    #[test]
    fn signed_url_ttl_is_shorter_for_private() {
        // Defaults (no env override): public 20m, private 5m. Private must be the
        // shorter window — it bounds how long a leaked/stale signed URL works
        // after a caller loses GitHub access.
        assert_eq!(ref_signed_url_ttl(false), Duration::from_secs(1200));
        assert_eq!(ref_signed_url_ttl(true), Duration::from_secs(300));
        assert!(ref_signed_url_ttl(true) < ref_signed_url_ttl(false));
    }

    #[test]
    fn visibility_header_is_parsed_case_insensitively() {
        use axum::http::HeaderValue;
        let mut h = HeaderMap::new();
        assert!(!visibility_is_private(&h)); // absent → public (self-host direct)
        h.insert("x-ripclone-visibility", HeaderValue::from_static("private"));
        assert!(visibility_is_private(&h));
        h.insert("x-ripclone-visibility", HeaderValue::from_static("PRIVATE"));
        assert!(visibility_is_private(&h));
        h.insert("x-ripclone-visibility", HeaderValue::from_static("public"));
        assert!(!visibility_is_private(&h));
    }

    /// A canned [`AccessVerifier`] for the authz wiring tests.
    struct StubVerifier(AccessDecision);

    #[async_trait::async_trait]
    impl AccessVerifier for StubVerifier {
        async fn verify(
            &self,
            _p: &ProviderInstance,
            _path: &str,
            _c: Option<&secrecy::SecretString>,
        ) -> AccessDecision {
            self.0
        }
    }

    /// AU1 gate decisions: trust mode falls back to the header; enforced mode
    /// maps the verifier's decision to public/private/403.
    #[tokio::test]
    async fn authorize_repo_read_maps_decisions() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let provider = state.provider_registry.get("github").unwrap().clone();
        let repo = RepoId::github("o/r");
        let headers = HeaderMap::new();

        // Trust mode: gate skipped, visibility from header (absent → public).
        state.require_repo_auth = false;
        assert!(
            !authorize_repo_read(&state, &provider, &repo, None, &headers)
                .await
                .unwrap()
        );

        // Enforced + public → served anonymously (private = false).
        state.require_repo_auth = true;
        state.access_verifier = Arc::new(StubVerifier(AccessDecision::Public));
        assert!(
            !authorize_repo_read(&state, &provider, &repo, None, &headers)
                .await
                .unwrap()
        );

        // Enforced + authorized private → private = true.
        state.access_verifier = Arc::new(StubVerifier(AccessDecision::PrivateAuthorized));
        assert!(
            authorize_repo_read(&state, &provider, &repo, None, &headers)
                .await
                .unwrap()
        );

        // Enforced + denied → 403.
        state.access_verifier = Arc::new(StubVerifier(AccessDecision::Denied));
        let resp = authorize_repo_read(&state, &provider, &repo, None, &headers)
            .await
            .unwrap_err();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    /// End-to-end: a `refs` read for a repo the caller can't access returns 403
    /// through the real route — and never reaches the build/mirror path. (Before
    /// AU1, a cached private repo here returned 200 to any shared-token holder.)
    #[tokio::test]
    async fn ref_read_is_forbidden_when_access_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        state.require_repo_auth = true;
        state.access_verifier = Arc::new(StubVerifier(AccessDecision::Denied));
        let app = build_app(state);

        let resp = app
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/o/r/refs/main",
                Some(&auth_header()),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn ref_response_cache_hits_and_invalidates_by_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let resp = RefResponse {
            owner: "acme".to_string(),
            repo: "secret".to_string(),
            provider: "github".to_string(),
            host: "github.com".to_string(),
            origin_url: "https://github.com/acme/secret.git".to_string(),
            branch: "main".to_string(),
            default_branch: "main".to_string(),
            commit: "commit1".to_string(),
            parent_commit: None,
            full_pack: String::new(),
            clonepack_manifest: "manifest".to_string(),
            clonepack_manifest_url: Some("https://example.invalid/manifest".to_string()),
            metadata_chunk: "metadata".to_string(),
            metadata_chunk_url: Some("https://example.invalid/metadata".to_string()),
            archive_chunk_urls: Some(vec![Some("https://example.invalid/archive".to_string())]),
            head_blobs_chunk_urls: None,
            head_blobs_idx_url: None,
            pack_chunk_urls: None,
            pack_idx_urls: None,
            midx_url: None,
            idx_bundle_url: None,
            shallow: true,
            archive_ready: true,
        };

        let cache_repo_id = RepoId::github("acme/secret");
        cache_ref_response(&state, &cache_repo_id, "main", "shallow", &resp);
        let cached = cached_ref_response(&state, &cache_repo_id, "main", "shallow")
            .expect("cached ref response");
        assert_eq!(cached.commit, "commit1");
        assert_eq!(
            cached.clonepack_manifest_url.as_deref(),
            Some("https://example.invalid/manifest")
        );
        assert!(cached_ref_response(&state, &cache_repo_id, "main", "full").is_none());

        invalidate_ref_response_cache(&state, &cache_repo_id, "main");
        assert!(cached_ref_response(&state, &cache_repo_id, "main", "shallow").is_none());

        let mut no_cache_state = state;
        no_cache_state.mirror_fresh_ttl = Duration::ZERO;
        cache_ref_response(&no_cache_state, &cache_repo_id, "main", "shallow", &resp);
        assert!(cached_ref_response(&no_cache_state, &cache_repo_id, "main", "shallow").is_none());
    }

    fn test_request(method: &str, uri: &str) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("Authorization", auth_header())
            .body(Body::empty())
            .unwrap()
    }

    /// Like `test_request` but with an explicit (or absent) `Authorization`
    /// header, for exercising the auth middleware's reject path.
    fn request_with_auth(method: &str, uri: &str, auth: Option<&str>) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
        if let Some(a) = auth {
            b = b.header("Authorization", a);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn protected_route_rejects_missing_and_wrong_token() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        // No Authorization header.
        let missing = app
            .clone()
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/acme/secret/status",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        // Present but wrong token.
        let wrong = app
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/acme/secret/status",
                Some("Ripclone deadbeef"),
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn public_endpoints_require_no_auth() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        // Liveness, readiness, and the Prometheus scrape must be reachable with
        // no credentials (load balancers / scrapers don't authenticate). They
        // must never return 401 from the auth middleware.
        for path in ["/healthz", "/readyz", "/metrics", "/v1/version"] {
            let resp = app
                .clone()
                .oneshot(request_with_auth("GET", path, None))
                .await
                .unwrap();
            assert_ne!(
                resp.status(),
                StatusCode::UNAUTHORIZED,
                "{path} must not require auth"
            );
        }
    }

    #[tokio::test]
    async fn version_endpoint_reports_build_and_protocol() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app
            .oneshot(request_with_auth("GET", "/v1/version", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(v["protocol"], crate::PROTOCOL_VERSION);
    }

    fn protocol_request(uri: &str, protocol: Option<&str>) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method("GET")
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("Authorization", auth_header());
        if let Some(p) = protocol {
            b = b.header("x-ripclone-protocol", p);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn protocol_guard_rejects_newer_client_only() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        // A client advertising a newer protocol than the server -> 426.
        let too_new = app
            .clone()
            .oneshot(protocol_request(
                "/v1/repos/acme/secret/status",
                Some("999"),
            ))
            .await
            .unwrap();
        assert_eq!(too_new.status(), StatusCode::UPGRADE_REQUIRED);
        // The current protocol, and a missing header (legacy client), are allowed.
        let current = crate::PROTOCOL_VERSION.to_string();
        for proto in [Some(current.as_str()), None] {
            let resp = app
                .clone()
                .oneshot(protocol_request("/v1/repos/acme/secret/status", proto))
                .await
                .unwrap();
            assert_ne!(resp.status(), StatusCode::UPGRADE_REQUIRED);
        }
    }

    #[tokio::test]
    async fn repo_status_returns_empty_for_cold_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/github/acme/secret/status"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.owner, "acme");
        assert_eq!(status.repo, "secret");
        assert!(status.refs.is_empty());
        assert_eq!(status.total_bytes, 0);
        assert_eq!(status.total_unique_bytes, 0);
        assert_eq!(status.regions.len(), 1);
        assert_eq!(status.regions[0].region, "local");
        assert_eq!(status.regions[0].unique_bytes, 0);
    }

    #[tokio::test]
    async fn readyz_ready_when_healthy() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_not_ready_when_storage_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        // Simulate the data volume being unmounted/removed under the server.
        std::fs::remove_dir_all(tmp.path().join("cas")).unwrap();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_endpoint_is_prometheus_text() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        state.metrics.record_ref_lookup();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/metrics")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let ct = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        assert!(ct.starts_with("text/plain"), "content-type was {ct}");
        assert!(ct.contains("version=0.0.4"), "content-type was {ct}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("# TYPE ripclone_ref_lookups_total counter"));
        assert!(text.contains("\nripclone_ref_lookups_total 1\n"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn readyz_not_ready_when_storage_read_only() {
        // root ignores directory permissions, so this probe can't be exercised
        // as root (common in CI containers); skip there.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping read-only probe test: running as root");
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let cas = tmp.path().join("cas");
        // r-x only: the dir still stats as a directory, but writes fail — the
        // case the old is_dir() check missed.
        std::fs::set_permissions(&cas, std::fs::Permissions::from_mode(0o500)).unwrap();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        std::fs::set_permissions(&cas, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "read-only CAS must report not ready"
        );
    }

    #[tokio::test]
    async fn readyz_not_ready_when_ref_store_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        std::fs::remove_dir_all(tmp.path().join("repos")).unwrap();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn repo_status_reports_warmed_branch_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let metadata = ChunkRef {
            hash: hash_from_hex(&"a".repeat(64)).unwrap(),
            len: 100,
        };
        let archive = ChunkRef {
            hash: hash_from_hex(&"b".repeat(64)).unwrap(),
            len: 200,
        };
        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            metadata_chunk: Some(metadata),
            archive_chunks: vec![archive],
            head_blobs_idx: None,
            head_blobs_chunks: vec![],
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            full_pack: String::new(),
            clonepack_manifest: manifest_hash.clone(),
            metadata_chunk: "a".repeat(64),
            archive_chunks: vec!["b".repeat(64)],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: "a".repeat(64),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            head_buckets: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            synced_at: Some(1_718_812_800),
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/github/acme/secret/status"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.refs.len(), 1);
        let branch = &status.refs[0];
        assert_eq!(branch.branch, "main");
        assert_eq!(branch.commit, "commit1");
        assert_eq!(branch.manifest, manifest_hash);
        let expected_bytes = 300 + manifest_data.len() as u64;
        assert_eq!(branch.bytes, expected_bytes);
        assert_eq!(branch.unique_bytes, expected_bytes); // fallback until cross-repo dedup
        assert!(branch.built_at.is_some());
        assert_eq!(status.total_bytes, expected_bytes);
        assert_eq!(status.total_unique_bytes, expected_bytes);
        assert_eq!(status.regions.len(), 1);
        assert_eq!(status.regions[0].region, "local");
        assert_eq!(status.regions[0].unique_bytes, expected_bytes);
    }

    #[tokio::test]
    async fn repo_status_dedups_shared_chunks_across_branches() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let metadata_hash = "a".repeat(64);
        let archive_hash = "b".repeat(64);
        let metadata = ChunkRef {
            hash: hash_from_hex(&metadata_hash).unwrap(),
            len: 100,
        };
        let archive = ChunkRef {
            hash: hash_from_hex(&archive_hash).unwrap(),
            len: 200,
        };
        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            metadata_chunk: Some(metadata),
            archive_chunks: vec![archive],
            head_blobs_idx: None,
            head_blobs_chunks: vec![],
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            full_pack: String::new(),
            clonepack_manifest: manifest_hash.clone(),
            metadata_chunk: metadata_hash.clone(),
            archive_chunks: vec![archive_hash.clone()],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: metadata_hash.clone(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            head_buckets: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            synced_at: None,
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "develop", &info)
            .await
            .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/github/acme/secret/status"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.refs.len(), 2);
        let expected_total = 300 + manifest_data.len() as u64;
        assert_eq!(status.total_bytes, expected_total);
        assert_eq!(status.total_unique_bytes, expected_total); // fallback: no dedup
        for branch in &status.refs {
            assert_eq!(branch.bytes, expected_total);
            assert_eq!(branch.unique_bytes, expected_total); // per-branch fallback
        }
        assert_eq!(status.regions.len(), 1);
        assert_eq!(status.regions[0].unique_bytes, expected_total);
    }

    #[tokio::test]
    async fn repo_status_public_fork_is_free() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let metadata = ChunkRef {
            hash: hash_from_hex(&"a".repeat(64)).unwrap(),
            len: 100,
        };
        let archive = ChunkRef {
            hash: hash_from_hex(&"b".repeat(64)).unwrap(),
            len: 200,
        };
        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            metadata_chunk: Some(metadata),
            archive_chunks: vec![archive],
            head_blobs_idx: None,
            head_blobs_chunks: vec![],
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            full_pack: String::new(),
            clonepack_manifest: manifest_hash.clone(),
            metadata_chunk: "a".repeat(64),
            archive_chunks: vec!["b".repeat(64)],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: String::new(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            build_status: None,
            synced_at: None,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/fork"), "main", &info)
            .await
            .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(test_request(
                "GET",
                "/v1/repos/github/acme/fork/status?public=true&fork_of=oven-sh/bun",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        let expected_total = 300 + manifest_data.len() as u64;
        assert_eq!(status.total_bytes, expected_total);
        assert_eq!(status.total_unique_bytes, 0);
        assert_eq!(status.refs[0].bytes, expected_total);
        assert_eq!(status.refs[0].unique_bytes, 0);
        assert_eq!(status.regions[0].unique_bytes, 0);
    }

    #[tokio::test]
    async fn repo_status_counts_history_levels() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            full_pack: String::new(),
            clonepack_manifest: manifest_hash.clone(),
            metadata_chunk: String::new(),
            archive_chunks: vec![],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: String::new(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: vec![crate::HistoryLevel {
                tip_commit: "older".to_string(),
                packs: vec![
                    crate::SizedPack {
                        pack: "p1".to_string(),
                        pack_len: 500,
                        idx: "i1".to_string(),
                        idx_len: 50,
                    },
                    crate::SizedPack {
                        pack: "p2".to_string(),
                        pack_len: 700,
                        idx: "i2".to_string(),
                        idx_len: 70,
                    },
                ],
            }],
            build_status: None,
            synced_at: None,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/github/acme/secret/status"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        let expected = manifest_data.len() as u64 + 500 + 50 + 700 + 70;
        assert_eq!(status.refs.len(), 1);
        assert_eq!(status.refs[0].bytes, expected);
        assert_eq!(status.total_bytes, expected);
        assert_eq!(status.total_unique_bytes, expected);
    }

    #[tokio::test]
    async fn repo_status_counts_pack_entries_in_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let pack = crate::clonepack::PackEntry {
            pack: Some(ChunkRef {
                hash: hash_from_hex(&"c".repeat(64)).unwrap(),
                len: 400,
            }),
            idx: Some(ChunkRef {
                hash: hash_from_hex(&"d".repeat(64)).unwrap(),
                len: 40,
            }),
            ..Default::default()
        };
        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            packs: vec![pack],
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            full_pack: String::new(),
            clonepack_manifest: manifest_hash.clone(),
            metadata_chunk: String::new(),
            archive_chunks: vec![],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: String::new(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            build_status: None,
            synced_at: None,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/github/acme/secret/status"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        let expected = manifest_data.len() as u64 + 400 + 40;
        assert_eq!(status.refs[0].bytes, expected);
        assert_eq!(status.total_bytes, expected);
    }

    #[tokio::test]
    async fn sync_rejects_invalid_branch_name() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app
            .oneshot(test_request(
                "POST",
                "/v1/repos/github/acme/secret/sync?branch=../evil",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // ---- Webhook receiver ----

    const WEBHOOK_SECRET: &str = "shhh-very-secret";

    /// GitHub-style `sha256=<hex>` HMAC of the body, as a real delivery sends.
    fn gh_sign(secret: &str, body: &[u8]) -> String {
        use hmac::{Hmac, Mac};
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn webhook_request(
        provider: &str,
        event: &str,
        signature: Option<&str>,
        body: Vec<u8>,
    ) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/webhooks/{provider}"))
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-GitHub-Event", event);
        if let Some(sig) = signature {
            b = b.header("X-Hub-Signature-256", sig);
        }
        b.body(Body::from(body)).unwrap()
    }

    fn gh_push_body(
        owner: &str,
        repo: &str,
        ref_: &str,
        after: &str,
        default_branch: &str,
        deleted: bool,
    ) -> Vec<u8> {
        serde_json::json!({
            "ref": ref_,
            "after": after,
            "deleted": deleted,
            "repository": {
                "name": repo,
                "owner": {"login": owner},
                "default_branch": default_branch,
                "private": false
            }
        })
        .to_string()
        .into_bytes()
    }

    /// A push payload that omits `repository.default_branch` (as a non-GitHub
    /// provider might), to exercise the mirror fallback.
    fn gh_push_body_no_default(owner: &str, repo: &str, ref_: &str, after: &str) -> Vec<u8> {
        serde_json::json!({
            "ref": ref_,
            "after": after,
            "deleted": false,
            "repository": {
                "name": repo,
                "owner": {"login": owner},
                "private": false
            }
        })
        .to_string()
        .into_bytes()
    }

    /// A test state whose local-queue receiver is kept alive (so an enqueue
    /// succeeds instead of reporting Full) with a configured `github` webhook
    /// secret. Returns the receiver so a test can assert what was enqueued.
    fn webhook_state(
        tmp: &tempfile::TempDir,
    ) -> (ServerState, tokio::sync::mpsc::Receiver<BuildJob>) {
        let mut state = test_state(tmp);
        let (queue, rx, depth) = crate::queue::LocalJobQueue::new(16);
        state.build_queue = Arc::new(queue);
        state.build_queue_depth = depth;
        state.webhook_config = Arc::new(WebhookConfig::with_secret("github", WEBHOOK_SECRET));
        (state, rx)
    }

    #[tokio::test]
    async fn webhook_without_secret_returns_503() {
        let tmp = tempfile::tempdir().unwrap();
        // Default test_state has no webhook secret configured.
        let state = test_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn webhook_push_enqueues_sync() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // The default-branch push must have placed exactly one build on the queue.
        let job = rx.try_recv().expect("a build job was enqueued");
        assert_eq!(job.repo_id, RepoId::github("acme/widget"));
        assert_eq!(job.branch, "main");
        assert!(rx.try_recv().is_err(), "exactly one job enqueued");
    }

    #[tokio::test]
    async fn webhook_invalid_signature_returns_401() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        // Signed with a different secret than the server holds.
        let sig = gh_sign("wrong-secret", &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(rx.try_recv().is_err(), "a bad signature must not enqueue");
    }

    #[tokio::test]
    async fn webhook_missing_signature_returns_401() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, _rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let resp = app
            .oneshot(webhook_request("github", "push", None, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn webhook_ping_is_acknowledged_without_enqueue() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = br#"{"zen":"keep it simple"}"#.to_vec();
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "ping", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "ping must not enqueue");
    }

    #[tokio::test]
    async fn webhook_untracked_non_default_branch_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        // Push to a non-default branch that has no stored build.
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/feature",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            rx.try_recv().is_err(),
            "untracked non-default branch must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_tracked_non_default_branch_enqueues() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let repo = RepoId::github("acme/widget");
        // Pre-store a build for the non-default branch so policy warms it.
        let info = RefInfo {
            commit: "deadbeef".to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&repo, "feature", &info)
            .await
            .unwrap();
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/feature",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let job = rx.try_recv().expect("tracked branch enqueues");
        assert_eq!(job.branch, "feature");
    }

    #[tokio::test]
    async fn webhook_allowlist_blocks_unlisted_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let (queue, mut rx, depth) = crate::queue::LocalJobQueue::new(16);
        state.build_queue = Arc::new(queue);
        state.build_queue_depth = depth;
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                .with_allowlist(["acme/allowed".to_string()]),
        );
        let app = build_app(state);
        // Push to a repo that is not on the allowlist.
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "unlisted repo must not enqueue");
    }

    #[tokio::test]
    async fn webhook_branch_delete_cleans_up_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let repo = RepoId::github("acme/widget");
        // Pre-store a ref that the delete webhook should remove.
        let info = RefInfo {
            commit: "deadbeef".to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&repo, "feature", &info)
            .await
            .unwrap();
        assert!(
            state
                .ref_store
                .load_branch(&repo, "feature")
                .await
                .unwrap()
                .is_some(),
            "ref present before delete"
        );
        let ref_store = state.ref_store.clone();
        let app = build_app(state);
        // GitHub signals a delete with an all-zeros `after` and `deleted: true`.
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/feature",
            &"0".repeat(40),
            "main",
            true,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            ref_store
                .load_branch(&repo, "feature")
                .await
                .unwrap()
                .is_none(),
            "deleted branch ref is cleaned up"
        );
        assert!(rx.try_recv().is_err(), "a delete must not enqueue a build");
    }

    #[tokio::test]
    async fn webhook_unknown_provider_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, _rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = br#"{}"#.to_vec();
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("nope", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn webhook_allowlist_allows_listed_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let (queue, mut rx, depth) = crate::queue::LocalJobQueue::new(16);
        state.build_queue = Arc::new(queue);
        state.build_queue_depth = depth;
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                .with_allowlist(["acme/widget".to_string()]),
        );
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let job = rx.try_recv().expect("a listed repo enqueues");
        assert_eq!(job.repo_id, RepoId::github("acme/widget"));
    }

    #[tokio::test]
    async fn webhook_tag_push_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        // A tag ref is not a branch; phase 1 warms branches only.
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/tags/v1.0.0",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "a tag push must not enqueue");
    }

    #[tokio::test]
    async fn webhook_non_github_provider_returns_501() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        // Register a GitLab instance — phase 1 has no GitLab webhook adapter yet.
        let mut registry = ProviderRegistry::new();
        registry
            .merge_one(crate::provider::ProviderConfig {
                id: "gitlab".to_string(),
                kind: Some("gitlab".to_string()),
                host: Some("gitlab.com".to_string()),
                ..Default::default()
            })
            .unwrap();
        state.provider_registry = registry;
        // A secret is configured, but the kind has no adapter → 501 (before any
        // verify), not 503.
        state.webhook_config = Arc::new(WebhookConfig::with_secret("gitlab", WEBHOOK_SECRET));
        let app = build_app(state);
        let resp = app
            .oneshot(webhook_request(
                "gitlab",
                "push",
                Some("whatever"),
                br#"{}"#.to_vec(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn webhook_tampered_body_returns_401() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        // Sign body A with the CORRECT secret, then deliver a different body B.
        // This pins that the handler verifies over the raw bytes it received,
        // not a re-serialized parse.
        let body_a = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body_a);
        let body_b = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"2".repeat(40),
            "main",
            false,
        );
        assert_ne!(body_a, body_b);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body_b))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(rx.try_recv().is_err(), "a tampered body must not enqueue");
    }

    #[tokio::test]
    async fn webhook_duplicate_pushes_coalesce_to_one_build() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        // Two identical signed pushes (no worker draining between them) must
        // collapse onto a single queued build via the shared coalescing path.
        let r1 = app
            .clone()
            .oneshot(webhook_request("github", "push", Some(&sig), body.clone()))
            .await
            .unwrap();
        assert_eq!(r1.status(), StatusCode::OK);
        let r2 = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(r2.status(), StatusCode::OK);
        assert!(rx.try_recv().is_ok(), "first push enqueues a build");
        assert!(
            rx.try_recv().is_err(),
            "the duplicate push coalesces — exactly one build"
        );
    }

    #[tokio::test]
    async fn webhook_tag_delete_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        // A tag deletion (refs/tags/... with deleted:true) routes to the delete
        // path but is not a branch — ignored, and never enqueues.
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/tags/v1.0.0",
            &"0".repeat(40),
            "main",
            true,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "a tag delete must not enqueue");
    }

    #[tokio::test]
    async fn webhook_hostile_branch_name_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        // A signed payload whose branch is a git-rev injection attempt must be
        // refused before it reaches the queue (defense in depth).
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/--upload-pack=evil",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "an invalid branch must not enqueue");
    }

    #[tokio::test]
    async fn webhook_default_branch_resolved_from_mirror_when_payload_omits_it() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        // A prior sync leaves a bare mirror whose HEAD names the default branch.
        // Recreate that so the handler can resolve the default without a payload
        // `default_branch` field.
        let mirror = state
            .repo_root
            .join(RepoId::github("acme/widget").mirror_dir_name());
        gix::init_bare(&mirror).unwrap();
        std::fs::write(mirror.join("HEAD"), b"ref: refs/heads/trunk\n").unwrap();
        let app = build_app(state);
        // Push to the default branch ("trunk") with no default_branch in payload.
        let body = gh_push_body_no_default("acme", "widget", "refs/heads/trunk", &"1".repeat(40));
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let job = rx
            .try_recv()
            .expect("default branch resolved from the mirror is warmed");
        assert_eq!(job.branch, "trunk");
    }

    #[tokio::test]
    async fn webhook_no_default_no_mirror_untracked_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        // No payload default_branch, no mirror, branch not tracked → can't prove
        // it's the default, so it is not warmed (fail-safe).
        let body =
            gh_push_body_no_default("acme", "widget", "refs/heads/whatever", &"1".repeat(40));
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            rx.try_recv().is_err(),
            "unknowable default + untracked → ignored"
        );
    }

    #[tokio::test]
    async fn webhook_delete_outside_allowlist_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let (queue, _rx, depth) = crate::queue::LocalJobQueue::new(16);
        state.build_queue = Arc::new(queue);
        state.build_queue_depth = depth;
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                .with_allowlist(["acme/allowed".to_string()]),
        );
        // This repo is NOT on the allowlist but happens to have a stored ref.
        let repo = RepoId::github("acme/widget");
        let info = RefInfo {
            commit: "deadbeef".to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&repo, "feature", &info)
            .await
            .unwrap();
        let ref_store = state.ref_store.clone();
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/feature",
            &"0".repeat(40),
            "main",
            true,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // Delete is gated by the allowlist just like push: out-of-scope repo is
        // left untouched.
        assert!(
            ref_store
                .load_branch(&repo, "feature")
                .await
                .unwrap()
                .is_some(),
            "out-of-scope delete must not mutate refs"
        );
    }
}
