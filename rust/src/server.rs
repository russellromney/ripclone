use crate::archive::ArchiveBuilder;
use crate::auth::broker::{CredentialBroker, broker_from_env};
use crate::backends;
use crate::cas::Cas;
use crate::clonepack::{
    ChunkRef, ClonepackManifest, collect_manifest_hashes, hash_from_hex, hash_to_hex,
    manifest_chunk_refs,
};
use crate::control::{BuildRow, ControlStore, Out};
use crate::git;
use crate::metrics::{Metrics, SyncPhaseMetrics};
use crate::pack::PackBuilder;
use crate::provider::{ProviderInstance, ProviderRegistry, RepoId};
use crate::storage::StorageRef;
use crate::validation;
use crate::webhook::{EventKind, WebhookConfig};
use crate::{ExactResultKind, RefInfo};
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, DefaultBodyLimit, OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use futures::{SinkExt, StreamExt, TryStreamExt};
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
use std::time::{Duration, Instant, SystemTime};
use tracing::{error, info, warn};

mod worker;

#[derive(Clone)]
pub struct ServerState {
    pub cas: Cas,
    pub storage: StorageRef,
    pub repo_root: PathBuf,
    /// Builds, added repositories and repository config. Also holds the
    /// server's ownership lock on the control database.
    pub control: Arc<dyn ControlStore>,
    pub provider_registry: ProviderRegistry,
    pub broker: Arc<dyn CredentialBroker>,
    pub token_hash: Option<String>,
    pub metrics: Arc<Metrics>,
    pub rate_limiter: RateLimiter,
    /// Wakes a Head slot after an admission that left Head pending.
    pub head_wake: Arc<tokio::sync::Notify>,
    /// Wakes a Background slot after Head publishes.
    pub background_wake: Arc<tokio::sync::Notify>,
    /// Webhook receiver config: per-provider HMAC secret + optional repo
    /// allowlist. A provider with no configured secret returns 503. Reads
    /// `RIPCLONE_WEBHOOK_SECRET_<provider>`.
    pub webhook_config: Arc<WebhookConfig>,
    /// Per-repo mutexes so concurrent syncs for the same repo cannot corrupt
    /// the bare mirror directory.
    pub sync_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Cached `/readyz` result `(checked_at, ready)`. Bounds backend probe cost
    /// (S3 round-trips) and damps load-balancer flapping on a transient blip.
    pub readyz_cache: Arc<std::sync::Mutex<Option<(Instant, bool)>>>,
}

/// Simple in-memory token-bucket rate limiter keyed by real client IP.
/// The map is bounded and pruned periodically to avoid unbounded memory growth.
#[derive(Clone)]
pub struct RateLimiter {
    buckets: Arc<StdMutex<HashMap<String, (Instant, f64)>>>,
    max_burst: u32,
    restore_rate_per_sec: f64,
    max_entries: usize,
}

impl RateLimiter {
    pub fn new(max_burst: u32, restore_rate_per_sec: f64) -> Self {
        let restore_rate_per_sec = if restore_rate_per_sec.is_finite() {
            restore_rate_per_sec.max(0.0)
        } else {
            0.0
        };
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
            .or_insert_with(|| (now, f64::from(self.max_burst)));
        let elapsed = now.duration_since(entry.0).as_secs_f64();
        entry.1 =
            (entry.1 + elapsed * self.restore_rate_per_sec).clamp(0.0, f64::from(self.max_burst));
        entry.0 = now;
        let allowed = if entry.1 < 1.0 {
            false
        } else {
            entry.1 -= 1.0;
            true
        };
        drop(buckets);
        allowed
    }
}

fn parse_rate_limit_settings(
    burst: Option<&str>,
    restore_rate_per_sec: Option<&str>,
) -> Result<(u32, f64)> {
    let burst = burst
        .map(|value| {
            value
                .parse()
                .context("RIPCLONE_RATE_LIMIT_BURST must be an unsigned integer")
        })
        .transpose()?
        .unwrap_or(60);
    let restore_rate_per_sec: f64 = restore_rate_per_sec
        .map(|value| {
            value
                .parse()
                .context("RIPCLONE_RATE_LIMIT_PER_SEC must be a number")
        })
        .transpose()?
        .unwrap_or(10.0);
    anyhow::ensure!(
        restore_rate_per_sec.is_finite() && restore_rate_per_sec >= 0.0,
        "RIPCLONE_RATE_LIMIT_PER_SEC must be finite and non-negative"
    );
    Ok((burst, restore_rate_per_sec))
}

#[derive(Deserialize)]
pub struct SyncRequest {
    #[serde(default = "default_branch_value")]
    pub branch: String,
    /// Optional git rev to resolve instead of the branch tip (e.g. `HEAD~5` or
    /// a SHA). The resolved commit is the build and result identity.
    #[serde(default)]
    pub rev: Option<String>,
}

#[derive(Deserialize)]
pub struct RefQuery {
    pub result: ExactResultKind,
    /// Optional git rev to resolve instead of the branch tip (e.g. "HEAD~5").
    /// Pairs with `sync?rev=...`: clone the artifacts built for that commit.
    #[serde(default)]
    pub rev: Option<String>,
    /// Exact commit learned from an earlier response in this clone operation.
    /// Unlike `rev`, this is metadata-only and never contacts the upstream or
    /// schedules work.
    #[serde(default)]
    pub pinned: Option<String>,
    /// Opt in to a one-commit Full-clone top-up plan on a pending pinned lookup.
    /// Ignored on initial selector, rev-targeted, and non-Full requests.
    #[serde(default)]
    pub top_up: bool,
}

#[derive(Serialize)]
pub struct BuildResponse {
    pub status: String,
    pub queue_depth: usize,
    pub commit: String,
    pub branch: String,
}

#[derive(Serialize)]
pub struct ArtifactPendingResponse {
    pub code: &'static str,
    pub commit: String,
    pub branch: String,
    pub status: &'static str,
    pub queue_depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_up_supported: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_up_base: Option<RefResponse>,
}

#[derive(Serialize)]
pub struct ExactRevisionUnavailableResponse {
    pub error: String,
    pub commit: String,
    pub branch: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncPhases {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mirror_fetch_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_graph_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_packs_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skeleton_build_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_table_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prebuilt_index_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_head_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_publish_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_head_ms: Option<u64>,
}

impl From<&SyncPhases> for SyncPhaseMetrics {
    fn from(phases: &SyncPhases) -> Self {
        Self {
            mirror_fetch_ms: phases.mirror_fetch_ms,
            commit_graph_ms: phases.commit_graph_ms,
            head_packs_ms: phases.head_packs_ms,
            skeleton_build_ms: phases.skeleton_build_ms,
            files_table_ms: phases.files_table_ms,
            prebuilt_index_ms: phases.prebuilt_index_ms,
            upload_head_ms: phases.upload_head_ms,
            ref_publish_ms: phases.ref_publish_ms,
            publish_head_ms: phases.publish_head_ms,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    #[serde(flatten)]
    pub ref_info: RefResponse,
    pub status: String,
    pub phases: SyncPhases,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_bytes: Option<u64>,
}

fn default_branch_value() -> String {
    "HEAD".to_string()
}

/// Resolve a `{*rest}` path segment from `/v1/repos/{*rest}/...` into a
/// `(RepoId, ProviderInstance)` pair.
///
/// The first path segment MUST be a registered provider instance id; the
/// remainder is the opaque repo path. Callers
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

fn unknown_provider_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "unknown provider".to_string(),
        }),
    )
        .into_response()
}

fn repo_not_added_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": "repo not added; run `ripclone add <repo>`",
            "code": "repo_not_added",
        })),
    )
        .into_response()
}

fn ref_not_found_response(name: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": format!("upstream ref not found: {name}"),
            "code": "ref_not_found",
        })),
    )
        .into_response()
}

async fn repo_is_added(state: &ServerState, repo_id: &RepoId) -> Result<bool, Response> {
    state
        .control
        .get_repo(repo_id)
        .await
        .map(|repo| repo.is_some())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("added repo lookup failed: {e}"),
                }),
            )
                .into_response()
        })
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

#[derive(Clone, Serialize, Deserialize)]
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
    pub commit: String,
    pub parent_commit: Option<String>,
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
    /// Signed URL for the pre-built multi-pack-index (`manifest.midx`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub midx_url: Option<String>,
    /// Signed URL for the concatenated idx bundle (`manifest.idx_bundle`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idx_bundle_url: Option<String>,
    pub result: ExactResultKind,
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Serialize, Deserialize)]
pub struct RepoStatusResponse {
    pub owner: String,
    pub repo: String,
    pub added: bool,
    pub refs: Vec<ExactStatusEntry>,
    pub total_bytes: u64,
    pub total_unique_bytes: u64,
    pub regions: Vec<RegionStorageEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct ExactStatusEntry {
    pub commit: String,
    pub bytes: u64,
    pub unique_bytes: u64,
    pub head: bool,
    pub full: bool,
    pub files: bool,
    pub job: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct RegionStorageEntry {
    pub region: String,
    pub unique_bytes: u64,
}

pub fn build_app(state: ServerState) -> Router {
    let protected = Router::new()
        .route("/v1/repos", get(list_added_repos))
        // Single catch-all route for all provider-qualified repo endpoints.
        .route("/v1/repos/{*path}", get(dispatch_repos_get))
        .route("/v1/repos/{*path}", post(dispatch_repos_post))
        .route("/v1/repos/{*path}", delete(dispatch_repos_delete))
        .route("/v1/artifacts/{hash}", get(get_artifact))
        // Repository build settings. Legacy `?branch=` queries are parsed only
        // to return a clear fail-closed error.
        .route(
            "/v1/admin/config/{owner}/{repo}",
            get(admin_get_config).post(admin_put_config),
        )
        // Clone metrics sink. The cloud consumes these for analytics; the OSS
        // server accepts and drops them so a self-hosted CLI never spams 404s.
        .route(
            "/v1/clones/{clone_id}/metrics",
            post(clone_metrics_drop_handler),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .route_layer(middleware::from_fn(protocol_guard))
        .with_state(state.clone());

    let rate_limited = Router::new()
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        // Provider-agnostic push-webhook receiver: authenticated by the provider
        // HMAC over the raw body (not the ripclone bearer token), so it lives
        // outside the `protected` layer.
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

/// Maximum request body size accepted by the server.
const MAX_REQUEST_BODY_BYTES: usize = 256 * 1024 * 1024;
/// Cap for a webhook request body. The HMAC can only be verified after the whole
/// body is buffered, so this bounds what an unauthenticated caller can make the
/// server hold before the signature is checked. GitHub caps webhook payloads at
/// ~25 MiB; this is comfortably above that and far below the global limit.
const MAX_WEBHOOK_BODY_BYTES: usize = 25 * 1024 * 1024;

/// Reject explicit declarations that do not match the sole current wire
/// protocol. Ordinary HTTP integrations do not declare this private header and
/// use the same implementation.
async fn protocol_guard(
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(header) = headers.get("x-ripclone-protocol") else {
        return next.run(request).await;
    };
    let client_proto = match header
        .to_str()
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
    {
        Some(protocol) => protocol,
        None => {
            return (
                StatusCode::UPGRADE_REQUIRED,
                Json(ErrorResponse {
                    error: format!(
                        "invalid client protocol; this server requires {}",
                        crate::PROTOCOL_VERSION
                    ),
                }),
            )
                .into_response();
        }
    };
    if client_proto != crate::PROTOCOL_VERSION {
        return (
            StatusCode::UPGRADE_REQUIRED,
            Json(ErrorResponse {
                error: format!(
                    "client protocol {client_proto} does not match this server's {}; use matching ripclone client and server versions",
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
    if let Some(expected) = &state.token_hash {
        let authorized = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|v| check_auth_header(v, expected))
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
    next.run(request).await
}

fn constant_time_eq_str(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

fn check_auth_header(header: &str, expected: &str) -> bool {
    header
        .strip_prefix("Ripclone ")
        .is_some_and(|token| constant_time_eq_str(token, expected))
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
    // One logical clone fans out across content-addressed manifests, indexes,
    // and chunks. Charging those authenticated GETs to the control-plane
    // request bucket makes a sufficiently large repository rate-limit its own
    // clone. Authentication still runs in the protected router, and anonymous
    // artifact traffic remains subject to the normal per-IP limiter.
    if is_authenticated_artifact_get(&state, &request) {
        return next.run(request).await;
    }
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

fn is_authenticated_artifact_get(
    state: &ServerState,
    request: &axum::http::Request<axum::body::Body>,
) -> bool {
    if request.method() != axum::http::Method::GET {
        return false;
    }
    let path = request.uri().path();
    if !path.starts_with("/v1/artifacts/") {
        return false;
    }
    let Some(expected) = state.token_hash.as_deref() else {
        return false;
    };
    request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| check_auth_header(value, expected))
        .unwrap_or(false)
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

/// Accept-and-drop sink for the CLI's post-clone metrics report. The payload is
/// advertising-grade telemetry, not operator metrics, so the OSS server has no
/// use for it; rejecting it would only make self-hosted clients see 404s.
async fn clone_metrics_drop_handler() -> impl IntoResponse {
    StatusCode::ACCEPTED
}

/// Readiness probe: 200 only when storage and the control store are reachable,
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

    if let Err(e) = state.control.health().await {
        problems.push(format!("control: {e:#}"));
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

async fn dispatch_repos_get(
    Path(path): Path<String>,
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    if let Some((repo_path, branch)) = path.rsplit_once("/refs/") {
        let params = match Query::<RefQuery>::try_from_uri(&uri) {
            Ok(query) => query.0,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: error.to_string(),
                    }),
                )
                    .into_response();
            }
        };
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return get_ref_inner(repo_id, provider.clone(), branch.to_string(), params, state).await;
    }

    if let Some(repo_path) = path.strip_suffix("/status") {
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
        return repo_status_inner(repo_id, query, state).await;
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
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    if let Some(repo_path) = path.strip_suffix("/add") {
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return add_repo_inner(repo_id, provider.clone(), state).await;
    }

    if let Some(repo_path) = path.strip_suffix("/sync") {
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
        return sync_repo_inner(repo_id, provider.clone(), query, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn dispatch_repos_delete(
    Path(path): Path<String>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(repo_path) = path.strip_suffix("/add") {
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return remove_added_repo_inner(repo_id, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn list_added_repos(State(state): State<ServerState>) -> Response {
    match state.control.list_repos().await {
        Ok(repos) => Json(
            repos
                .into_iter()
                .map(|added| added.repo_id)
                .collect::<Vec<RepoId>>(),
        )
        .into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("list added repos failed: {e}"),
                }),
            )
                .into_response()
        }
    }
}

async fn ensure_mirror(
    mirror_dir: &std::path::Path,
    provider: &ProviderInstance,
    repo_id: &RepoId,
    branch: &str,
    rev: Option<&str>,
    credential: Option<&secrecy::SecretString>,
) -> Result<()> {
    let mirror_dir = mirror_dir.to_path_buf();
    let provider = provider.clone();
    let repo_id = repo_id.clone();
    let branch = branch.to_string();
    let rev = rev.map(str::to_string);
    let credential = credential.cloned();
    // Same process-global fetch cap as the build path.
    let _fetch_permit = fetch_semaphore()
        .acquire()
        .await
        .expect("fetch semaphore never closed");
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror(
            &mirror_dir,
            &provider,
            &repo_id,
            &branch,
            rev.as_deref(),
            credential.as_ref(),
        )
    })
    .await
    .context("ensure mirror task")?
}

fn exact_result_clonepack(
    info: &RefInfo,
    result: ExactResultKind,
) -> Option<&crate::ClonepackArtifacts> {
    match result {
        ExactResultKind::Head => info.head.as_ref().map(|head| &head.clonepack),
        ExactResultKind::Full => info.full.as_ref().map(|full| &full.clonepack),
        ExactResultKind::Files => info.files.as_ref().map(|files| &files.clonepack),
    }
}

fn exact_result_ready(info: &RefInfo, result: ExactResultKind, commit: &str) -> bool {
    crate::exact_output_ready(info, result, commit)
}

/// The ready outputs of a build row, in the shape responses are built from.
fn row_info(row: &BuildRow) -> RefInfo {
    RefInfo {
        commit: row.commit.clone(),
        head: row.head.ready().cloned(),
        full: row.full.ready().cloned(),
        files: row.files.ready().cloned(),
    }
}

/// The state of one output kind of a row, without its result.
fn kind_state(row: &BuildRow, kind: ExactResultKind) -> Out<()> {
    fn erase<T>(out: &Out<T>) -> Out<()> {
        match out {
            Out::Pending { error } => Out::Pending {
                error: error.clone(),
            },
            Out::Ready(_) => Out::Ready(()),
            Out::Failed(error) => Out::Failed(error.clone()),
        }
    }
    match kind {
        ExactResultKind::Head => erase(&row.head),
        ExactResultKind::Full => erase(&row.full),
        ExactResultKind::Files => erase(&row.files),
    }
}

fn row_complete(row: &BuildRow) -> bool {
    row.head.ready().is_some() && row.full.ready().is_some() && row.files.ready().is_some()
}

/// The full ref an admission came from, if the selector named a branch tip.
fn branch_ref_name(branch: &str) -> Option<String> {
    if branch.is_empty() || branch == "HEAD" {
        None
    } else if branch.starts_with("refs/") {
        Some(branch.to_string())
    } else {
        Some(format!("refs/heads/{branch}"))
    }
}

async fn artifact_pending_response(commit: &str, branch: &str, queue_depth: usize) -> Response {
    artifact_pending_response_with_top_up(commit, branch, queue_depth, None, None).await
}

async fn artifact_pending_response_with_top_up(
    commit: &str,
    branch: &str,
    queue_depth: usize,
    top_up_supported: Option<bool>,
    top_up_base: Option<RefResponse>,
) -> Response {
    let mut response = (
        StatusCode::ACCEPTED,
        Json(ArtifactPendingResponse {
            code: "artifact_pending",
            commit: commit.to_string(),
            branch: branch.to_string(),
            status: "building",
            queue_depth,
            top_up_supported,
            top_up_base,
        }),
    )
        .into_response();
    if let Ok(value) = urlencoding::encode(branch).parse() {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_LOCATION, value);
    }
    response
}

/// The Full result of B's parent, as the base of a one-commit top-up while
/// Full(B) builds. Read from the control store only; the client checks that
/// the base is B's parent.
async fn top_up_base_response(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: &str,
    row: &BuildRow,
) -> Option<RefResponse> {
    let parent = row.parent.as_deref()?;
    let parent_row = state.control.status(repo_id, parent).await.ok()??;
    parent_row.full.ready()?;
    Some(ref_response(
        repo_id,
        provider,
        branch.to_string(),
        &row_info(&parent_row),
        &state.storage,
        ExactResultKind::Full,
    ))
}

async fn get_ref_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    requested_checkout: String,
    params: RefQuery,
    state: ServerState,
) -> Response {
    if let Some(response) = validation::reject_if_invalid(|| {
        if requested_checkout == "HEAD" {
            Ok(())
        } else {
            validation::validate_checkout_name(&requested_checkout)
        }
    }) {
        return response;
    }
    if let Some(rev) = params.rev.as_deref()
        && let Some(response) = validation::reject_if_invalid(|| validation::validate_git_rev(rev))
    {
        return response;
    }
    if let Some(pinned) = params.pinned.as_deref()
        && let Some(response) =
            validation::reject_if_invalid(|| validation::validate_object_id(pinned))
    {
        return response;
    }
    match repo_is_added(&state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return repo_not_added_response(),
        Err(response) => return response,
    }

    let credential = match state.broker.fetch_credential(&repo_id) {
        Ok(credential) => credential,
        Err(error) => return credential_error_response(error),
    };

    state.metrics.record_ref_lookup();
    let (commit, checkout_name, already_pinned) = if let Some(pinned) = params.pinned.as_deref() {
        let checkout_name = if requested_checkout == "HEAD"
            && params.rev.as_deref() == Some(pinned)
            && validation::validate_object_id(pinned).is_ok()
        {
            String::new()
        } else {
            requested_checkout.clone()
        };
        (pinned.to_string(), checkout_name, true)
    } else if let Some(rev) = params.rev.as_deref() {
        if validation::validate_object_id(rev).is_ok() {
            let checkout_name = if requested_checkout == "HEAD" {
                String::new()
            } else {
                requested_checkout.clone()
            };
            (rev.to_string(), checkout_name, false)
        } else {
            let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
            let lock = repo_lock(&state.sync_locks, &repo_id).await;
            let _guard = lock.lock().await;
            if let Err(error) = ensure_mirror(
                &mirror_dir,
                &provider,
                &repo_id,
                &requested_checkout,
                Some(rev),
                credential.as_ref(),
            )
            .await
            {
                warn!(
                    revision = %rev,
                    error = %error,
                    "failed to resolve requested revision"
                );
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(ErrorResponse {
                        error: format!(
                            "cannot resolve requested revision `{rev}`; check the revision and upstream access"
                        ),
                    }),
                )
                    .into_response();
            }
            let checkout_name = if requested_checkout == "HEAD" {
                match git::default_branch(&mirror_dir)
                    .ok()
                    .filter(|name| !name.is_empty() && name != "HEAD")
                {
                    Some(name) => name,
                    None => {
                        return (
                            StatusCode::UNPROCESSABLE_ENTITY,
                            Json(ErrorResponse {
                                error: "cannot determine checkout name for HEAD".to_string(),
                            }),
                        )
                            .into_response();
                    }
                }
            } else {
                requested_checkout.clone()
            };
            let commit = match git::resolve_commit(&mirror_dir, rev) {
                Ok(commit) => commit,
                Err(error) => {
                    warn!(
                        revision = %rev,
                        error = %error,
                        "failed to resolve requested revision"
                    );
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(ErrorResponse {
                            error: format!(
                                "cannot resolve requested revision `{rev}`; check the revision and upstream access"
                            ),
                        }),
                    )
                        .into_response();
                }
            };
            (commit, checkout_name, false)
        }
    } else {
        let tip = {
            let _permit = fetch_semaphore()
                .acquire()
                .await
                .expect("fetch semaphore never closed");
            git::ls_remote_tip_async(
                &provider,
                &repo_id,
                &requested_checkout,
                credential.as_ref(),
            )
            .await
        };
        match tip {
            Ok(Some(tip)) => {
                let checkout_name = if requested_checkout == "HEAD" {
                    match tip.default_branch.filter(|name| !name.is_empty()) {
                        Some(name) => name,
                        None => {
                            return (
                                StatusCode::UNPROCESSABLE_ENTITY,
                                Json(ErrorResponse {
                                    error: "upstream HEAD did not advertise a checkout name"
                                        .to_string(),
                                }),
                            )
                                .into_response();
                        }
                    }
                } else {
                    requested_checkout.clone()
                };
                (tip.commit, checkout_name, false)
            }
            Ok(None) => return ref_not_found_response(&requested_checkout),
            Err(error) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse {
                        error: format!("upstream tip probe failed: {error:#}"),
                    }),
                )
                    .into_response();
            }
        }
    };

    let existing = match state.control.status(&repo_id, &commit).await {
        Ok(row) => row,
        Err(error) => {
            state.metrics.record_error();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("exact result lookup failed: {error:#}"),
                }),
            )
                .into_response();
        }
    };
    let ready_response = |row: &BuildRow, checkout_name: String| {
        (
            StatusCode::OK,
            Json(ref_response(
                &repo_id,
                &provider,
                checkout_name,
                &row_info(row),
                &state.storage,
                params.result,
            )),
        )
            .into_response()
    };
    if let Some(row) = existing.as_ref()
        && exact_result_ready(&row_info(row), params.result, &commit)
    {
        return ready_response(row, checkout_name);
    }
    if already_pinned {
        let Some(row) = existing else {
            return (
                StatusCode::CONFLICT,
                Json(ExactRevisionUnavailableResponse {
                    error: format!(
                        "{} result is missing for exact commit and no build exists",
                        params.result
                    ),
                    commit,
                    branch: checkout_name,
                }),
            )
                .into_response();
        };
        if let Out::Failed(error) = kind_state(&row, params.result) {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ExactRevisionUnavailableResponse {
                    error: format!("{} result failed: {error}", params.result),
                    commit,
                    branch: checkout_name,
                }),
            )
                .into_response();
        }
        if params.top_up && params.result == ExactResultKind::Full {
            let base =
                top_up_base_response(&state, &repo_id, &provider, &checkout_name, &row).await;
            return artifact_pending_response_with_top_up(
                &commit,
                &checkout_name,
                0,
                Some(true),
                base,
            )
            .await;
        }
        return artifact_pending_response(&commit, &checkout_name, 0).await;
    }

    // A rev names a commit, not a branch tip, so it records no ref.
    let ref_name = if params.rev.is_some() {
        None
    } else {
        branch_ref_name(&checkout_name)
    };
    match admit_build(&state, &repo_id, &commit, ref_name.as_deref(), true).await {
        Ok((row, _)) if exact_result_ready(&row_info(&row), params.result, &commit) => {
            ready_response(&row, checkout_name)
        }
        Ok(_) => artifact_pending_response(&commit, &checkout_name, 0).await,
        Err(error) => exact_unavailable_response(
            &state,
            format!("durable admission failed: {error:#}"),
            commit,
            checkout_name,
        ),
    }
}

/// Admission failure for an exact-revision request: the control store is unavailable.
fn exact_unavailable_response(
    state: &ServerState,
    error: String,
    commit: String,
    branch: String,
) -> Response {
    state.metrics.record_error();
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ExactRevisionUnavailableResponse {
            error,
            commit,
            branch,
        }),
    )
        .into_response()
}

const REF_SIGNED_URL_TTL_SECS: u64 = 1200;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn ref_signed_url_ttl() -> Duration {
    Duration::from_secs(env_u64(
        "RIPCLONE_SIGNED_URL_TTL_SECS",
        REF_SIGNED_URL_TTL_SECS,
    ))
}

/// Artifact hashes needed to build one [`RefResponse`], independent of
/// whether they came from a loaded `RefInfo` or from decoded manifest bytes
/// (the top-up path).
struct ResponseArtifacts {
    commit: String,
    parent_commit: Option<String>,
    manifest: String,
    metadata_chunk: String,
    archive_chunks: Vec<String>,
    head_blobs_chunks: Vec<String>,
    head_blobs_idx: Option<String>,
    packs: Vec<String>,
    midx: String,
    idx_bundle: String,
}

/// Sign and order the URLs for one ref response. Shared by every builder:
/// each caller extracts a [`ResponseArtifacts`] from its own source (a
/// `RefInfo`, or a decoded manifest) and hands it here.
fn build_ref_response(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    artifacts: &ResponseArtifacts,
    storage: &crate::storage::StorageRef,
    result: ExactResultKind,
) -> RefResponse {
    let ttl = ref_signed_url_ttl();
    let signed = |hash: &str| signed_url(storage, ttl, hash);
    // `None` entries (e.g. local backend) fall back to the gateway. Ordered
    // to match the manifest's own chunk/pack lists.
    let signed_list = |hashes: &[String]| -> Option<Vec<Option<String>>> {
        if hashes.is_empty() {
            return None;
        }
        let urls: Vec<Option<String>> = hashes.iter().map(|hash| signed(hash)).collect();
        (!urls.iter().all(Option::is_none)).then_some(urls)
    };
    let (owner, repo) = repo_id
        .github_owner_repo()
        .map(|(o, r)| (o.to_string(), r.to_string()))
        .unwrap_or_else(|| (repo_id.provider.as_str().to_string(), repo_id.path.clone()));
    RefResponse {
        owner,
        repo,
        provider: provider.id.as_str().to_string(),
        host: provider.host.clone(),
        origin_url: provider.clone_url(&repo_id.path),
        branch,
        commit: artifacts.commit.clone(),
        parent_commit: artifacts.parent_commit.clone(),
        clonepack_manifest: artifacts.manifest.clone(),
        clonepack_manifest_url: signed(&artifacts.manifest),
        metadata_chunk: artifacts.metadata_chunk.clone(),
        metadata_chunk_url: signed(&artifacts.metadata_chunk),
        archive_chunk_urls: signed_list(&artifacts.archive_chunks),
        head_blobs_chunk_urls: signed_list(&artifacts.head_blobs_chunks),
        head_blobs_idx_url: artifacts.head_blobs_idx.as_deref().and_then(signed),
        pack_chunk_urls: signed_list(&artifacts.packs),
        // Sign the pre-built MIDX for the selected variant so the client
        // installs it directly instead of running `git multi-pack-index
        // write`, and the idx bundle so the client fetches all idx in one GET.
        midx_url: signed(&artifacts.midx),
        idx_bundle_url: signed(&artifacts.idx_bundle),
        result,
    }
}

fn ref_response(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    info: &RefInfo,
    storage: &crate::storage::StorageRef,
    result: ExactResultKind,
) -> RefResponse {
    let artifacts = exact_result_clonepack(info, result)
        .expect("requested exact result is ready before response construction");
    let archive_chunks = info
        .files
        .as_ref()
        .filter(|_| result == ExactResultKind::Files)
        .map(|files| files.archive_chunks.clone())
        .unwrap_or_default();
    let packs = match result {
        ExactResultKind::Head => info.head.as_ref().map(|head| head.packs.as_slice()),
        ExactResultKind::Full => info.full.as_ref().map(|full| full.packs.as_slice()),
        ExactResultKind::Files => None,
    }
    .unwrap_or_default()
    .iter()
    .map(|p| p.pack.clone())
    .collect();
    let response_artifacts = ResponseArtifacts {
        commit: artifacts.commit.clone(),
        parent_commit: info
            .head
            .as_ref()
            .and_then(|head| head.parent_commit.clone()),
        manifest: artifacts.manifest.clone(),
        metadata_chunk: artifacts.metadata_chunk.clone(),
        archive_chunks,
        head_blobs_chunks: Vec::new(),
        head_blobs_idx: None,
        packs,
        midx: artifacts.midx.clone(),
        idx_bundle: artifacts.idx_bundle.clone(),
    };
    build_ref_response(
        repo_id,
        provider,
        branch,
        &response_artifacts,
        storage,
        result,
    )
}

/// Ready admission response. Signing already-known object URLs does not read
/// artifact bytes; keeping the byte totals absent preserves the no-op proof's
/// zero artifact-read boundary.
fn sync_response_without_storage_read(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    info: &RefInfo,
    storage: &crate::storage::StorageRef,
    result: ExactResultKind,
    status: impl Into<String>,
) -> SyncResponse {
    SyncResponse {
        ref_info: ref_response(repo_id, provider, branch, info, storage, result),
        status: status.into(),
        phases: SyncPhases::default(),
        bytes: None,
        unique_bytes: None,
    }
}

fn signed_url(storage: &crate::storage::StorageRef, ttl: Duration, hash: &str) -> Option<String> {
    if hash.is_empty() {
        return None;
    }
    storage.signed_url(hash, ttl)
}

fn credential_error_response(e: anyhow::Error) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: format!("credential fetch failed: {e}"),
        }),
    )
        .into_response()
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
    query: RepoStatusQuery,
    state: ServerState,
) -> Response {
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

fn record_chunk(unique_chunks: &mut HashMap<String, u64>, hash: &str, len: u64) {
    if hash.is_empty() || len == 0 {
        return;
    }
    unique_chunks.insert(hash.to_string(), len);
}

async fn build_repo_status(
    state: &ServerState,
    repo_id: &RepoId,
    public: bool,
    fork_of: Option<&str>,
) -> Result<RepoStatusResponse> {
    let rows = state.control.list_builds(repo_id).await?;
    let mut refs = Vec::new();
    let mut unique_chunks: HashMap<String, u64> = HashMap::new();
    for row in rows {
        let info = row_info(&row);
        let manifest_hashes = collect_manifest_hashes(&info);
        let mut ref_bytes = 0u64;
        for manifest_hash in manifest_hashes {
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
        if exact_result_ready(&info, ExactResultKind::Full, &info.commit)
            && let Some(full) = &info.full
        {
            for level in &full.history_levels {
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
        }

        let (job, job_error) = build_summary(&row);
        let is_public_fork = public && fork_of.is_some();
        let branch_unique_bytes = if is_public_fork { 0 } else { ref_bytes };
        let head_ready = exact_result_ready(&info, ExactResultKind::Head, &info.commit);
        let full_ready = exact_result_ready(&info, ExactResultKind::Full, &info.commit);
        let files_ready = exact_result_ready(&info, ExactResultKind::Files, &info.commit);

        refs.push(ExactStatusEntry {
            commit: info.commit,
            bytes: ref_bytes,
            unique_bytes: branch_unique_bytes,
            head: head_ready,
            full: full_ready,
            files: files_ready,
            job,
            job_error,
        });
    }

    refs.sort_by(|a, b| a.commit.cmp(&b.commit));
    let total_bytes = unique_chunks.values().sum();
    // TODO: cross-repo fork-network dedup for private repos. For now, public
    // forks receive zero unique-byte allocation and every other repository
    // reports its deduplicated logical bytes.
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
        added: state.control.get_repo(repo_id).await?.is_some(),
        refs,
        total_bytes,
        total_unique_bytes,
        regions,
    })
}

/// `/status` job summary of one build row: "pending" while any output is
/// pending, else "failed" with the first error if any output failed, else
/// "done".
fn build_summary(row: &BuildRow) -> (String, Option<String>) {
    let states = [
        kind_state(row, ExactResultKind::Head),
        kind_state(row, ExactResultKind::Full),
        kind_state(row, ExactResultKind::Files),
    ];
    if states
        .iter()
        .any(|state| matches!(state, Out::Pending { .. }))
    {
        return ("pending".to_string(), None);
    }
    match states.into_iter().find_map(|state| match state {
        Out::Failed(error) => Some(error),
        _ => None,
    }) {
        Some(error) => ("failed".to_string(), Some(error)),
        None => ("done".to_string(), None),
    }
}

/// Ordinary branch-tip sync: resolve one exact upstream commit, then perform a
/// read-only ready check or admit that immutable target. The response never
/// waits for the builder.
async fn sync_repo_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    params: SyncRequest,
    state: ServerState,
) -> Response {
    if let Some(resp) = validation::reject_if_invalid(|| {
        if params.branch == "HEAD" {
            Ok(())
        } else {
            validation::validate_checkout_name(&params.branch)
        }
    }) {
        return resp;
    }
    if params.rev.is_some() {
        return sync_repo_at_revision(repo_id, provider, params, state).await;
    }
    match repo_is_added(&state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return repo_not_added_response(),
        Err(resp) => return resp,
    }

    let credential = match state.broker.fetch_credential(&repo_id) {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };

    let start = Instant::now();
    let requested_branch = params.branch;
    let tip = {
        let _permit = fetch_semaphore()
            .acquire()
            .await
            .expect("fetch semaphore never closed");
        git::ls_remote_tip_async(&provider, &repo_id, &requested_branch, credential.as_ref()).await
    };
    let tip = match tip {
        Ok(Some(tip)) => tip,
        Ok(None) => return ref_not_found_response(&requested_branch),
        Err(e) => {
            state.metrics.record_error();
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse {
                    error: format!("upstream tip probe failed: {e:#}"),
                }),
            )
                .into_response();
        }
    };
    // The single bounded advertisement asks for symbolic HEAD alongside a
    // concrete requested branch. Preserve that identity for every exact-only
    // mirror, not only requests whose selector was HEAD, so later historical
    // HEAD~N resolution never falls back to Git's platform init default.
    let commit = tip.commit;
    let effective_branch = if requested_branch == "HEAD" {
        tip.default_branch
            .filter(|branch| !branch.is_empty())
            .unwrap_or_else(|| requested_branch.clone())
    } else {
        requested_branch.clone()
    };

    // Explicit sync owns every missing result. It is a no-op only when Head,
    // Full, and Files are all already present; failed outputs are retried.
    match admit_build(
        &state,
        &repo_id,
        &commit,
        branch_ref_name(&effective_branch).as_deref(),
        true,
    )
    .await
    {
        Ok((row, _)) if row_complete(&row) => {
            state.metrics.record_sync(start.elapsed());
            let resp = sync_response_without_storage_read(
                &repo_id,
                &provider,
                effective_branch,
                &row_info(&row),
                &state.storage,
                ExactResultKind::Full,
                "no-op",
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Ok((_, inserted)) => (
            StatusCode::ACCEPTED,
            Json(BuildResponse {
                status: if inserted { "queued" } else { "coalesced" }.to_string(),
                // The wire keeps the field; the server no longer tracks a
                // queue depth.
                queue_depth: 0,
                commit,
                branch: effective_branch,
            }),
        )
            .into_response(),
        Err(error) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("durable admission failed: {error:#}"),
                }),
            )
                .into_response()
        }
    }
}

/// First-class exact-revision sync used by `sync --at REV`.
async fn sync_repo_at_revision(
    repo_id: RepoId,
    provider: ProviderInstance,
    params: SyncRequest,
    state: ServerState,
) -> Response {
    if let Some(resp) = validation::reject_if_invalid(|| {
        if params.branch == "HEAD" {
            Ok(())
        } else {
            validation::validate_checkout_name(&params.branch)
        }
    }) {
        return resp;
    }
    let Some(at_rev) = params.rev else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "exact revision is required".to_string(),
            }),
        )
            .into_response();
    };
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&at_rev)) {
        return resp;
    }
    match repo_is_added(&state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return repo_not_added_response(),
        Err(resp) => return resp,
    }
    let start = Instant::now();
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let mut branch = params.branch;

    let credential = match state.broker.fetch_credential(&repo_id) {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };

    // Resolve a symbolic historical selector exactly once. Subsequent work and
    // polls use only the selected object id. A concrete object id never needs
    // upstream resolution: an explicit branch remains the checkout name, while
    // the default HEAD selector is represented by an empty detached marker.
    let selector_is_exact = crate::validation::validate_object_id(&at_rev).is_ok();
    let needs_resolution_fetch = !selector_is_exact;
    let selected_commit = if needs_resolution_fetch {
        let lock = repo_lock(&state.sync_locks, &repo_id).await;
        let _guard = lock.lock().await;
        let mirror = mirror_dir.clone();
        let provider = provider.clone();
        let repo = repo_id.clone();
        let fetch_branch = branch.clone();
        let selector = at_rev.clone();
        let fetch_credential = credential.clone();
        let resolved = tokio::task::spawn_blocking(move || {
            git::sync_bare_mirror(
                &mirror,
                &provider,
                &repo,
                &fetch_branch,
                Some(&selector),
                fetch_credential.as_ref(),
            )?;
            let commit = git::resolve_commit(&mirror, &selector)?;
            let default_branch = git::default_branch(&mirror)
                .ok()
                .filter(|candidate| !candidate.is_empty() && candidate != "HEAD");
            Ok::<_, anyhow::Error>((commit, default_branch))
        })
        .await;
        match resolved {
            Ok(Ok((commit, default_branch))) => {
                if branch == "HEAD" {
                    branch = default_branch.unwrap_or_else(|| branch.clone());
                }
                commit
            }
            Ok(Err(error)) => {
                state.metrics.record_error();
                warn!(
                    revision = %at_rev,
                    error = %error,
                    "failed to resolve requested revision"
                );
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(ErrorResponse {
                        error: format!(
                            "cannot resolve requested revision `{at_rev}`; check the revision and upstream access"
                        ),
                    }),
                )
                    .into_response();
            }
            Err(error) => {
                state.metrics.record_error();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("exact revision resolution task failed: {error}"),
                    }),
                )
                    .into_response();
            }
        }
    } else {
        if branch == "HEAD" {
            branch.clear();
        }
        at_rev.clone()
    };
    let at_rev = selected_commit;

    // Exact revisions use the same immutable `(repository, commit)` row as
    // ordinary requests; checkout names are never build identity. An explicit
    // sync is a no-op only when every output is ready.
    match admit_build(&state, &repo_id, &at_rev, None, true).await {
        Ok((row, _)) if row_complete(&row) => {
            state.metrics.record_sync(start.elapsed());
            let response = sync_response_without_storage_read(
                &repo_id,
                &provider,
                branch,
                &row_info(&row),
                &state.storage,
                ExactResultKind::Full,
                "no-op",
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(_) => artifact_pending_response(&at_rev, &branch, 0).await,
        Err(error) => exact_unavailable_response(
            &state,
            format!("durable admission failed: {error:#}"),
            at_rev,
            branch,
        ),
    }
}

async fn add_repo_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    state: ServerState,
) -> Response {
    if let Err(e) = state.control.add_repo(&repo_id).await {
        state.metrics.record_error();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("add failed: {e:#}"),
            }),
        )
            .into_response();
    }

    sync_repo_inner(
        repo_id,
        provider,
        SyncRequest {
            branch: "HEAD".to_string(),
            rev: None,
        },
        state,
    )
    .await
}

async fn remove_added_repo_inner(repo_id: RepoId, state: ServerState) -> Response {
    match state.control.remove_repo(&repo_id).await {
        Ok(true) => (StatusCode::NO_CONTENT, Body::empty()).into_response(),
        Ok(false) => repo_not_added_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("remove added repo failed: {e:#}"),
                }),
            )
                .into_response()
        }
    }
}

/// Admit `commit` and wake the pool that has work for it. Returns the row and
/// whether this call created it. With `retry`, failed outputs go back to
/// pending: an explicit request (`/sync`, `/add`, a clone lookup) asks for
/// them again. Webhook and poller admissions leave failures alone.
async fn admit_build(
    state: &ServerState,
    repo_id: &RepoId,
    commit: &str,
    ref_name: Option<&str>,
    retry: bool,
) -> Result<(BuildRow, bool)> {
    let (mut row, inserted) = state.control.admit(repo_id, commit, ref_name).await?;
    if inserted {
        state.metrics.record_build_accepted();
    }
    let failed = [
        ExactResultKind::Head,
        ExactResultKind::Full,
        ExactResultKind::Files,
    ]
    .into_iter()
    .any(|kind| matches!(kind_state(&row, kind), Out::Failed(_)));
    if retry
        && failed
        && let Some(retried) = state.control.retry(repo_id, commit).await?
    {
        row = retried;
    }
    match (&row.head, &row.files, &row.full) {
        (Out::Pending { .. }, _, _) => state.head_wake.notify_one(),
        (Out::Ready(_), Out::Pending { .. }, _) | (Out::Ready(_), _, Out::Pending { .. }) => {
            state.background_wake.notify_one();
        }
        _ => {}
    }
    Ok((row, inserted))
}

/// Legacy branch selectors are parsed only so they can fail closed without
/// changing repository configuration or admitting work.
#[derive(Deserialize)]
struct AdminConfigQuery {
    #[serde(default)]
    branch: Option<String>,
}

/// `GET /v1/admin/config/{owner}/{repo}` — return the stored repository config
/// (404 if none is stored).
async fn admin_get_config(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<AdminConfigQuery>,
    State(state): State<ServerState>,
) -> Response {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if query.branch.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "branch-level repository configuration is no longer supported".to_string(),
            }),
        )
            .into_response();
    }
    let repo_id = RepoId::github(format!("{owner}/{repo}"));
    match state.control.get_config(&repo_id).await {
        Ok(Some(config)) => (StatusCode::OK, Json(config)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "no config stored for this repository".to_string(),
            }),
        )
            .into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("load config: {e:#}"),
                }),
            )
                .into_response()
        }
    }
}

/// `POST /v1/admin/config/{owner}/{repo}` — store the repository config. The
/// body is validated before the control database is written; builds read it
/// when they start.
async fn admin_put_config(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<AdminConfigQuery>,
    State(state): State<ServerState>,
    Json(config): Json<crate::repo_config::RepoConfig>,
) -> Response {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if query.branch.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "branch-level repository configuration is no longer supported".to_string(),
            }),
        )
            .into_response();
    }
    if let Err(e) = config.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("invalid config: {e:#}"),
            }),
        )
            .into_response();
    }
    let repo_id = RepoId::github(format!("{owner}/{repo}"));
    match state.control.set_config(&repo_id, &config).await {
        Ok(()) => (StatusCode::OK, Json(config)).into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("store config: {e:#}"),
                }),
            )
                .into_response()
        }
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
async fn webhook_handler(
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    body: Body,
) -> Response {
    handle_webhook(state, provider_id, headers, body).await
}

/// verify (HMAC over the RAW body) → normalize → admit a build via the shared
/// `admit_build` path (so the build runs ahead of any clone, coalescing with
/// `/sync`). Responds 2xx fast. Fail-closed: no configured secret ⇒ 503; bad
/// signature ⇒ 401. The payload is trusted only for routing (which repo / ref),
/// never to choose a credential or escalate.
async fn handle_webhook(
    state: ServerState,
    provider_id: String,
    headers: HeaderMap,
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
    // Fail closed: no configured secret for this provider ⇒ 503.
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
    // bytes. Cap the buffer well below the global request limit: the signature
    // can only be checked after the whole body is buffered, so an unauthenticated
    // caller must not be able to make us hold a huge request before the 401.
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
        EventKind::BranchDelete => webhook_ignored("exact results outlive branch deletion"),
    }
}

/// Whether the webhook allowlist admits this repo. Matches the operator-facing
/// natural key (`owner/repo` for github, `provider/path` otherwise); for the
/// github default it ALSO accepts the explicit `github/owner/repo` form, so an
/// operator generalizing from the `gitlab/...` examples isn't silently bitten by
/// github's bare-key asymmetry.
fn webhook_repo_allowed(state: &ServerState, repo_id: &RepoId) -> bool {
    let cfg = &state.webhook_config;
    cfg.allows(&repo_id.natural_key())
        || (repo_id.is_github_default() && cfg.allows(&format!("github/{}", repo_id.path)))
}

/// Default-branch push → exact admission. Applies the allowlist and added-repo
/// gates, then triggers the shared fire-and-forget path and returns immediately.
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
    if !webhook_repo_allowed(state, &repo_id) {
        return webhook_ignored("repo not in webhook allowlist");
    }
    match repo_is_added(state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return webhook_ignored("repo not added"),
        Err(resp) => return resp,
    }
    // Only branch pushes can identify the admitted default-branch commit; tags
    // and other refs are ignored.
    let Some(branch) = event
        .ref_
        .strip_prefix("refs/heads/")
        .filter(|b| !b.is_empty())
    else {
        return webhook_ignored("non-branch ref");
    };
    let branch = branch.to_string();
    // Validate the payload-derived branch before it reaches the queue / git.
    if validation::validate_checkout_name(&branch).is_err() {
        return webhook_ignored("invalid branch");
    }
    let Some(after) = event.after.as_deref() else {
        return webhook_ignored("push event has no after commit");
    };
    if validation::validate_object_id(after).is_err() {
        return webhook_ignored("push event has invalid after commit");
    }
    // Only a signed event that names its default branch may admit a commit.
    // Missing identity and non-default pushes are acknowledged without work.
    if event.default_branch.as_deref() != Some(branch.as_str()) {
        return webhook_ignored("push is not identified as the default branch");
    }
    // The signed, validated `after` is the admission target. No second
    // upstream probe is permitted on this path.
    let ref_name = format!("refs/heads/{branch}");
    match admit_build(state, &repo_id, after, Some(&ref_name), false).await {
        Ok(_) => {
            info!(
                "webhook: triggered build for {}@{branch}",
                repo_id.storage_key()
            );
            (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response()
        }
        Err(error) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("durable admission failed: {error:#}"),
                }),
            )
                .into_response()
        }
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

async fn get_artifact(
    Path(hash): Path<String>,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = validate_artifact_hash(&hash) {
        return resp;
    }
    serve_artifact(hash, state, Some(headers))
        .await
        .into_response()
}

const ARTIFACT_STREAM_CHUNK_BYTES: usize = 64 * 1024;

enum ArtifactBodySource {
    File(tokio::fs::File),
    Ranges {
        storage: crate::storage::StorageRef,
        hash: String,
        offset: u64,
    },
}

/// Stream an artifact through a two-chunk bounded channel. Local storage uses
/// one open file; uncommon non-file backends without signed URLs use fixed-size
/// range reads. Neither path allocates according to the artifact's total size.
fn artifact_body(mut source: ArtifactBodySource, len: u64) -> Body {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;

        let mut sent = 0u64;
        while sent < len {
            let wanted = usize::try_from(
                (len - sent).min(u64::try_from(ARTIFACT_STREAM_CHUNK_BYTES).unwrap_or(u64::MAX)),
            )
            .unwrap_or(ARTIFACT_STREAM_CHUNK_BYTES);
            let read = match &mut source {
                ArtifactBodySource::File(file) => {
                    let mut buffer = vec![0u8; wanted];
                    match file.read(&mut buffer).await {
                        Ok(0) => Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "artifact file ended before its recorded length",
                        )),
                        Ok(read) => {
                            buffer.truncate(read);
                            Ok(buffer)
                        }
                        Err(error) => Err(error),
                    }
                }
                ArtifactBodySource::Ranges {
                    storage,
                    hash,
                    offset,
                } => {
                    let storage = Arc::clone(storage);
                    let hash = hash.clone();
                    let start = *offset;
                    match tokio::task::spawn_blocking(move || {
                        storage.get_range(&hash, start, wanted as u64)
                    })
                    .await
                    {
                        Ok(Ok(bytes)) if !bytes.is_empty() => {
                            *offset += bytes.len() as u64;
                            Ok(bytes)
                        }
                        Ok(Ok(_)) => Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "artifact range ended before its recorded length",
                        )),
                        Ok(Err(error)) => Err(std::io::Error::other(error)),
                        Err(error) => Err(std::io::Error::other(error)),
                    }
                }
            };
            match read {
                Ok(bytes) => {
                    sent += bytes.len() as u64;
                    if tx.send(Ok(Bytes::from(bytes))).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
        }
    });
    Body::from_stream(rx)
}

async fn serve_artifact(
    hash: String,
    state: ServerState,
    headers: Option<axum::http::HeaderMap>,
) -> impl IntoResponse {
    // If the backend can hand out a signed URL, redirect the client there.
    // The client can then use its own Range requests against the CDN/object store.
    // Use the same TTL as the ref path.
    if let Some(url) = state.storage.signed_url(&hash, ref_signed_url_ttl()) {
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
        let value = h
            .get(axum::http::header::RANGE)
            .and_then(|v| v.to_str().ok())?;
        parse_byte_range(value, total_size)
    });

    if total_size == 0 {
        state.metrics.record_artifact_request(0);
        return (StatusCode::OK, [("content-length", "0")], Body::empty()).into_response();
    }

    let (start, end, status) = range
        .map(|(start, end)| (start, end, StatusCode::PARTIAL_CONTENT))
        .unwrap_or((0, total_size.saturating_sub(1), StatusCode::OK));
    let len = end - start + 1;
    let source = match tokio::task::spawn_blocking({
        let storage = Arc::clone(&state.storage);
        let hash = hash.clone();
        move || -> anyhow::Result<ArtifactBodySource> {
            if let Some(mut file) = storage.open_file(&hash)? {
                use std::io::{Seek, SeekFrom};
                file.seek(SeekFrom::Start(start))?;
                Ok(ArtifactBodySource::File(tokio::fs::File::from_std(file)))
            } else {
                Ok(ArtifactBodySource::Ranges {
                    storage,
                    hash,
                    offset: start,
                })
            }
        }
    })
    .await
    {
        Ok(Ok(source)) => source,
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
    state.metrics.record_artifact_request(len);
    let body = artifact_body(source, len);
    if status == StatusCode::PARTIAL_CONTENT {
        let content_range = format!("bytes {start}-{end}/{total_size}");
        (
            status,
            [
                ("content-range", content_range),
                ("content-length", len.to_string()),
            ],
            body,
        )
            .into_response()
    } else {
        (status, [("content-length", len.to_string())], body).into_response()
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

/// Compact history down to at most this many levels (merging the smallest
/// adjacent pair) so the level count stays bounded under seal-every-sync.
fn lsm_max_levels() -> usize {
    std::env::var("RIPCLONE_LSM_MAX_LEVELS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16usize)
}

/// Given the prior sealed levels and a freshly built tail, decide whether to
/// seal the tail into a new level, then compact the level set back under
/// `max_levels`. Returns `(history_packs, new_pack_tuples, new_levels)` where
/// `history_packs` is every history pack this manifest references (all levels
/// flattened — prior levels reused by hash), `new_pack_tuples` is the packs
/// freshly built this sync (the tail plus any compaction output to upload), and
/// `new_levels` is the levels to persist for the next sync.
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
    max_levels: usize,
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
    if levels.len() > max_levels {
        let before = levels.len();
        let (md, c, levels_in, max, tgt) = (
            mirror_dir.to_path_buf(),
            cas.clone(),
            levels.clone(),
            max_levels,
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
/// Bytes come from local CAS or object storage (the disposable local cache may
/// have been cleared after a prior upload). Returns `None` on any failure — the
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
    let md = crate::clonepack::MetadataChunk::decode_and_validate(bytes.as_slice()).ok()?;
    Some(md.files)
}

/// Build one result's PackEntry list and concatenated idx bundle.
fn assemble_variant(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    tagged: &[(&(String, u64, String, u64), bool)],
) -> Result<(Vec<crate::clonepack::PackEntry>, Option<ChunkRef>, String)> {
    if tagged.is_empty() {
        return Ok((Vec::new(), None, String::new()));
    }
    use std::io::Write;

    let mut tmp = tempfile::Builder::new()
        .suffix(".idx-bundle")
        .tempfile_in(cas.root())
        .context("create idx bundle temp file")?;
    let mut entries = Vec::with_capacity(tagged.len());
    let mut len = 0u64;
    for &(pack, history_only) in tagged {
        let idx_bytes = cas.get(&pack.2).or_else(|_| storage.get(&pack.2))?;
        let offset = len;
        tmp.write_all(&idx_bytes)
            .context("write idx bytes to bundle")?;
        len += idx_bytes.len() as u64;
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
    tmp.flush().context("flush idx bundle temp file")?;
    let (hash, stored_len) = cas.put_file(tmp.path())?;
    anyhow::ensure!(
        stored_len == len,
        "idx bundle length changed while storing: expected {len}, got {stored_len}"
    );
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
/// set with reused packs absent from the local cache, omit the MIDX and let the client
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
    let lengths = crate::clonepack::archive_chunk_lengths(metadata_chunk)?;
    if lengths.len() != archive_chunk_hashes.len() {
        anyhow::bail!(
            "archive chunk hash/length mismatch: hashes={} lengths={}",
            archive_chunk_hashes.len(),
            lengths.len()
        );
    }
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

/// Concurrency for artifact uploads. Defaults to 2x CPU cores.
fn upload_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(8)
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
            let read_hash = hash.clone();
            let (path, len) = tokio::task::spawn_blocking(move || {
                let len = cas
                    .verify_object(&read_hash)
                    .with_context(|| format!("verify artifact {} before upload", read_hash))?;
                Ok::<_, anyhow::Error>((cas.path(&read_hash), len))
            })
            .await
            .context("verify artifact task")??;
            let upload_start = std::time::Instant::now();
            storage
                .put_file_async(&hash, &path)
                .await
                .with_context(|| format!("upload artifact {}", hash))?;
            crate::perf::record_storage_upload(upload_start.elapsed(), len);
            Ok(())
        }
    }))
    .buffer_unordered(conc.max(1))
    .try_collect::<Vec<()>>()
    .await
    .map(|_| ())
}

fn archive_publish_upload_hashes(
    metadata_hash: &str,
    clonepack_hash: &str,
    download_bundle_hashes: &[String],
    new_reuse_frame_hashes: &[String],
) -> Vec<String> {
    let mut uploads: Vec<String> = vec![metadata_hash.to_string(), clonepack_hash.to_string()];
    uploads.extend(download_bundle_hashes.iter().cloned());
    uploads.extend(new_reuse_frame_hashes.iter().cloned());
    uploads.retain(|h| !h.is_empty());
    uploads.sort();
    uploads.dedup();
    uploads
}

/// After upload, a remote backend may drop local pack copies while keeping the
/// tiny idx files used by later bundle and MIDX builds. Local storage owns the
/// durable copy and keeps every uploaded artifact.
async fn settle_storage(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    uploaded: Vec<String>,
    keep_idx: std::collections::HashSet<String>,
) {
    if storage.is_remote() {
        for h in uploaded.iter().filter(|h| !keep_idx.contains(*h)) {
            let _ = cas.remove(h);
        }
    }
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

/// Result of the Head-closure build: a small delta pack against the
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
    elapsed_ms: u64,
}

/// Build and upload the Head result for `commit`. Head packs are a delta
/// against `base`, an immutable Head pack set of an ancestor; without a base
/// the full closure is packed and becomes the base. `parent_head`, the
/// parent's ready Head, lets the files table hash only changed paths.
#[allow(clippy::too_many_arguments)]
async fn build_head_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    commit: &str,
    parent: Option<String>,
    parent_head: Option<&crate::HeadResult>,
    base: Option<crate::HeadBase>,
    storage: &crate::storage::StorageRef,
    phases: &mut SyncPhases,
) -> Result<crate::HeadResult> {
    let upload_conc = upload_concurrency();

    // Build the Head closure and shallow skeleton.
    let t = Instant::now();
    let sk_start = Instant::now();
    let (md1, c1, cm1) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let shallow_skeleton_handle = tokio::task::spawn_blocking(move || {
        let s = Instant::now();
        let r = PackBuilder::new(&md1, &c1).build_shallow_skeleton_pack(&cm1);
        info!("Head skeleton {:?}", s.elapsed());
        r
    });
    // Head-closure packs, incremental by delta against an immutable base: keep the
    // base packs (closure of `head_base_commit`) and pack only the depth-1 objects
    // new since that base (`closure(HEAD) − closure(base)`) into a delta pack. The
    // base and delta are disjoint by construction, so no object is ever in two HEAD
    // packs (which would double-materialize a worktree file). A cold sync (no base)
    // packs the full closure as the base. The cumulative delta grows as HEAD moves
    // from the base; once it reaches RIPCLONE_HEAD_REBASE_BYTES, the Full build
    // compacts a new base for later Heads, off the depth=1 critical path.
    let head_target = 4 * 1024 * 1024;
    let base = base.filter(|base| !base.commit.is_empty() && !base.packs.is_empty());
    let (md2, c2, cm2) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let head_handle = tokio::task::spawn_blocking(move || -> Result<HeadBuild> {
        let s = Instant::now();
        let b = PackBuilder::new(&md2, &c2);
        match base {
            // Delta path: a base exists; pack only what is new since the base.
            Some(base) => {
                let delta = b.build_head_delta_pack(&cm2, &base.commit, head_target)?;
                let mut all_packs: Vec<(String, u64, String, u64)> =
                    base.packs.iter().map(sized_to_tuple).collect();
                all_packs.extend(delta.iter().cloned());
                let elapsed = s.elapsed();
                info!(
                    "Head packs (delta vs base {}: {} new pack(s), {} total) {:?}",
                    &base.commit[..7.min(base.commit.len())],
                    delta.len(),
                    all_packs.len(),
                    elapsed
                );
                Ok(HeadBuild {
                    all_packs,
                    new_built: delta,
                    base_commit: base.commit,
                    base_packs: base.packs,
                    all_local: false,
                    elapsed_ms: duration_ms(elapsed),
                })
            }
            // Cold path: no base yet → pack the full closure as the base.
            None => {
                let base = b.build_head_packs(&cm2, head_target)?;
                let base_packs = base.iter().map(tuple_to_sized).collect();
                let elapsed = s.elapsed();
                info!("Head packs (full base, {} packs) {:?}", base.len(), elapsed);
                Ok(HeadBuild {
                    all_packs: base.clone(),
                    new_built: base,
                    base_commit: cm2,
                    base_packs,
                    all_local: true,
                    elapsed_ms: duration_ms(elapsed),
                })
            }
        }
    });
    // Head builds only the cheap files table (no zstd frames): editable
    // depth=1 materializes the worktree from the HEAD-closure packs, so it does
    // not need the archive. The full zstd archive (for files mode) is built in
    // concurrently afterward, off the time-to-Head critical path.
    //
    // Files-table by-diff: when the parent's Head is ready, reuse its content
    // hashes for unchanged paths and read+hash only the blobs that changed since
    // the parent (O(changed) instead of O(worktree)). Falls back to a full table
    // when there is no parent table.
    let prev_files: Option<Vec<crate::clonepack::FileEntry>> = parent_head
        .and_then(|head| load_metadata_files(cas, storage, &head.clonepack.metadata_chunk));
    let prev_commit_for_diff = parent_head.and(parent.clone());
    // Carry the prior files table + commit so the bounded archive can hash only
    // changed files and reuse frames for the unchanged prefix/suffix.
    let (md3, cm3) = (mirror_dir.to_path_buf(), commit.to_string());
    let ft_start = Instant::now();
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
                            "Head files table (incremental, {} changed) {:?}",
                            changed.len(),
                            s.elapsed()
                        );
                        r
                    }
                    Err(e) => {
                        warn!("files-table diff failed ({e:#}); full rebuild");
                        let r = ArchiveBuilder::new(&md).build_files_table(&cm);
                        info!("Head files table (full, diff fallback) {:?}", s.elapsed());
                        r
                    }
                }
            })
        }
        _ => tokio::task::spawn_blocking(move || {
            let s = Instant::now();
            let r = ArchiveBuilder::new(&md3).build_files_table(&cm3);
            info!("Head files table (full) {:?}", s.elapsed());
            r
        }),
    };
    let (shallow_skeleton_pack, shallow_skeleton_idx) = shallow_skeleton_handle
        .await
        .context("shallow skeleton")??;
    phases.skeleton_build_ms = Some(duration_ms(sk_start.elapsed()));
    let head_built = head_handle.await.context("head packs")??;
    phases.head_packs_ms = Some(head_built.elapsed_ms);
    let head_packs = head_built.all_packs.clone();
    let metadata_base = files_table_handle.await.context("files table")??;
    phases.files_table_ms = Some(duration_ms(ft_start.elapsed()));
    info!("Head result: packs+skeleton+files-table {:?}", t.elapsed());

    let (md4, c4, cm4, skp) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        commit.to_string(),
        shallow_skeleton_pack.clone(),
    );
    let idx_start = Instant::now();
    let shallow_prebuilt_index = tokio::task::spawn_blocking(move || {
        PackBuilder::new(&md4, &c4).build_prebuilt_index(&cm4, &skp)
    })
    .await
    .context("shallow prebuilt index")??;
    phases.prebuilt_index_ms = Some(duration_ms(idx_start.elapsed()));

    let mut shallow_meta = metadata_base.clone();
    shallow_meta.skeleton_pack = cas.get(&shallow_skeleton_pack)?;
    shallow_meta.skeleton_idx = cas.get(&shallow_skeleton_idx)?;
    shallow_meta.prebuilt_index = cas.get(&shallow_prebuilt_index)?;
    let shallow_meta_data = shallow_meta.encode_to_vec();
    let shallow_metadata_hash = cas.put(&shallow_meta_data)?;

    // Head has no archive frames. Files publishes its own archive result.
    let archive_chunks = archive_chunk_refs(&[], &metadata_base)?;
    let head_tagged: Vec<(&(String, u64, String, u64), bool)> =
        head_packs.iter().map(|p| (p, false)).collect();
    let (head_entries, head_idx_bundle_ref, head_idx_bundle_hash) =
        assemble_variant(cas, storage, &head_tagged)?;
    // Ship the head MIDX only on a cold full base (all pack bytes still local).
    // On a delta re-sync the base packs may be absent from local cache, so omit it — the
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
        &archive_chunks,
        &shallow_metadata_hash,
        shallow_meta_data.len() as u64,
        head_entries,
        head_midx_ref,
        head_idx_bundle_ref,
    )?;
    let shallow_clonepack_hash = cas.put(&shallow_manifest.encode_to_vec())?;

    let head = crate::HeadResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: shallow_clonepack_hash.clone(),
            metadata_chunk: shallow_metadata_hash.clone(),
            skeleton_pack: shallow_skeleton_pack.clone(),
            skeleton_idx: shallow_skeleton_idx.clone(),
            prebuilt_index: shallow_prebuilt_index.clone(),
            midx: head_midx_hash.clone(),
            idx_bundle: head_idx_bundle_hash.clone(),
            commit: commit.to_string(),
        },
        parent_commit: parent.clone(),
        packs: pack_artifacts_of(&head_packs),
        base_commit: head_built.base_commit.clone(),
        base_packs: head_built.base_packs.clone(),
    };

    // Upload Head artifacts (shallow skeleton/index/metadata, head idx-bundle
    // + midx, shallow manifest, and only the FRESHLY BUILT head packs+idx).
    // Reused bucket packs are already durable in storage from a prior sync.
    let mut head_uploads: Vec<String> = vec![
        shallow_skeleton_pack.clone(),
        shallow_skeleton_idx.clone(),
        shallow_prebuilt_index.clone(),
        shallow_metadata_hash.clone(),
        shallow_clonepack_hash.clone(),
        head_idx_bundle_hash.clone(),
        head_midx_hash.clone(),
    ];
    for (p, _, i, _) in &head_built.new_built {
        head_uploads.push(p.clone());
        head_uploads.push(i.clone());
    }
    head_uploads.retain(|h| !h.is_empty());
    let head_idx_keep: std::collections::HashSet<String> =
        head_packs.iter().map(|(_, _, ih, _)| ih.clone()).collect();
    let upload_start = Instant::now();
    upload_artifacts(cas, storage, head_uploads.clone(), upload_conc).await?;
    settle_storage(cas, storage, head_uploads, head_idx_keep).await;
    phases.upload_head_ms = Some(duration_ms(upload_start.elapsed()));

    Ok(head)
}

/// Storage amplification for one ref: durable bytes in object storage divided by
/// the upstream bare-mirror size, split by artifact class. Content-addressed
/// storage may be shared across refs, so this attributes every hash reachable
/// from the given ref to that ref.
#[derive(Debug, Clone, serde::Serialize)]
struct StorageAmplification {
    repo_size_bytes: u64,
    head_pack_bytes: u64,
    history_pack_bytes: u64,
    archive_chunk_bytes: u64,
    metadata_bytes: u64,
    total_storage_bytes: u64,
    amplification: f64,
}

/// Recursively sum file sizes under `dir`.
fn dir_size(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    total += dir_size(&path);
                }
            }
        }
    }
    total
}

/// Classify every hash reachable from `info` by artifact class and sum its
/// bytes in `storage`.
fn measure_storage_amplification(
    storage: &crate::storage::StorageRef,
    info: &crate::RefInfo,
    mirror_dir: &std::path::Path,
) -> Option<StorageAmplification> {
    let repo_size = dir_size(mirror_dir);
    let mut head_pack_bytes = 0u64;
    let mut history_pack_bytes = 0u64;
    let mut archive_chunk_bytes = 0u64;
    let mut metadata_bytes = 0u64;

    let add = |hash: &str, bucket: &mut u64| {
        if hash.is_empty() {
            return;
        }
        if let Ok(size) = storage.size(hash) {
            *bucket += size;
        }
    };

    // Head closure packs (base + delta) and their idx files.
    if let Some(head) = &info.head {
        for pack in &head.packs {
            add(&pack.pack, &mut head_pack_bytes);
            add(&pack.idx, &mut head_pack_bytes);
        }
    }

    // Full-history / LSM sealed levels.
    if let Some(full) = &info.full {
        for level in &full.history_levels {
            for pack in &level.packs {
                add(&pack.pack, &mut history_pack_bytes);
                add(&pack.idx, &mut history_pack_bytes);
            }
        }
    }

    // Archive chunks referenced directly from the ref.
    if let Some(files) = &info.files {
        for hash in &files.archive_chunks {
            add(hash, &mut archive_chunk_bytes);
        }
    }

    // Metadata: manifests, metadata chunks, skeleton/index, prebuilt index,
    // idx bundle, and MIDX.
    for artifacts in [
        info.head.as_ref().map(|result| &result.clonepack),
        info.full.as_ref().map(|result| &result.clonepack),
        info.files.as_ref().map(|result| &result.clonepack),
    ]
    .into_iter()
    .flatten()
    {
        for hash in [
            &artifacts.manifest,
            &artifacts.metadata_chunk,
            &artifacts.skeleton_pack,
            &artifacts.skeleton_idx,
            &artifacts.prebuilt_index,
            &artifacts.idx_bundle,
            &artifacts.midx,
        ] {
            add(hash, &mut metadata_bytes);
        }
    }

    let total_storage_bytes = head_pack_bytes
        .saturating_add(history_pack_bytes)
        .saturating_add(archive_chunk_bytes)
        .saturating_add(metadata_bytes);
    let amplification = if repo_size == 0 {
        0.0
    } else {
        total_storage_bytes as f64 / repo_size as f64
    };

    Some(StorageAmplification {
        repo_size_bytes: repo_size,
        head_pack_bytes,
        history_pack_bytes,
        archive_chunk_bytes,
        metadata_bytes,
        total_storage_bytes,
        amplification,
    })
}

fn report_sync_phases(repo_id: &RepoId, commit: &str, phases: &SyncPhases) {
    let report = serde_json::json!({
        "kind": "sync-phases",
        "repo": repo_id.storage_key(),
        "commit": &commit[..7.min(commit.len())],
        "phases": phases,
    });
    info!("{}", report.to_string());
}

/// Print a JSON benchmark report when `RIPCLONE_BENCH` is set. Mirrors the
/// client-side `--bench` report style: one structured object per completed
/// build, emitted at INFO so it can be scraped from logs. Phase timings are in
/// the `sync-phases` report written when Head publishes.
fn report_sync_bench(
    repo_id: &RepoId,
    commit: &str,
    storage: &crate::storage::StorageRef,
    info: &crate::RefInfo,
    mirror_dir: &std::path::Path,
) {
    let amplification = measure_storage_amplification(storage, info, mirror_dir);
    let report = serde_json::json!({
        "kind": "sync-bench",
        "repo": repo_id.storage_key(),
        "commit": &commit[..7.min(commit.len())],
        "storage_amplification": amplification,
    });
    info!("{}", report.to_string());
}

fn manifest_pack_tuples(manifest: &ClonepackManifest) -> Result<Vec<(String, u64, String, u64)>> {
    manifest
        .packs
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let pack = entry
                .pack
                .as_ref()
                .with_context(|| format!("Head manifest pack {index} missing pack"))?;
            let idx = entry
                .idx
                .as_ref()
                .with_context(|| format!("Head manifest pack {index} missing idx"))?;
            Ok((
                hash_to_hex(&pack.hash),
                pack.len,
                hash_to_hex(&idx.hash),
                idx.len,
            ))
        })
        .collect()
}

/// Build and upload the Full result: the Head packs plus history levels.
/// When the Head delta has reached `RIPCLONE_HEAD_REBASE_BYTES`, the Head
/// packs are first compacted into a fresh base for `commit`. Full uses the
/// compact packs and records them as `head_base`, so later Heads delta against
/// it. The published Head result never changes.
#[allow(clippy::too_many_arguments)]
async fn build_full_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    commit: &str,
    head: &crate::HeadResult,
    previous_levels: Vec<crate::HistoryLevel>,
    storage: &crate::storage::StorageRef,
) -> Result<crate::FullResult> {
    let head_manifest_bytes = storage
        .get(&head.clonepack.manifest)
        .or_else(|_| cas.get(&head.clonepack.manifest))
        .context("read Head manifest for Full")?;
    let head_manifest = ClonepackManifest::decode(head_manifest_bytes.as_slice())
        .context("decode Head manifest for Full")?;
    anyhow::ensure!(
        head_manifest.commit == commit && head.clonepack.commit == commit,
        "Head result identity mismatch for {}@{commit}",
        repo_id.storage_key()
    );
    let mut head_packs = manifest_pack_tuples(&head_manifest)?;
    let parent = head.parent_commit.clone();

    let history_target = 512 * 1024 * 1024;
    let sealed_tip = previous_levels.last().map(|level| level.tip_commit.clone());
    let history_mirror = mirror_dir.to_path_buf();
    let history_cas = cas.clone();
    let history_commit = commit.to_string();
    let history_sealed_tip = sealed_tip.clone();
    let built_history = tokio::task::spawn_blocking(move || {
        PackBuilder::new(&history_mirror, &history_cas).build_history_tail(
            &history_commit,
            history_sealed_tip.as_deref(),
            history_target,
        )
    })
    .await
    .context("Full history task")??;

    let (history_packs, new_history, levels) = seal_and_compact(
        mirror_dir,
        cas,
        commit,
        previous_levels,
        sealed_tip,
        built_history,
        history_target,
        lsm_max_levels(),
    )
    .await?;

    let rebase_bytes = env_u64("RIPCLONE_HEAD_REBASE_BYTES", 128 * 1024 * 1024);
    let delta_bytes: u64 = head_packs
        .iter()
        .skip(head.base_packs.len())
        .map(|(_, pack_len, _, _)| *pack_len)
        .sum();
    let mut head_base = None;
    if delta_bytes > 0 && delta_bytes >= rebase_bytes {
        let compact_mirror = mirror_dir.to_path_buf();
        let compact_cas = cas.clone();
        let compact_commit = commit.to_string();
        let compact_packs = tokio::task::spawn_blocking(move || {
            PackBuilder::new(&compact_mirror, &compact_cas)
                .build_head_packs(&compact_commit, 4 * 1024 * 1024)
        })
        .await
        .context("compact Head packs")??;
        let mut compact_uploads = Vec::new();
        for (pack, _, idx, _) in &compact_packs {
            compact_uploads.push(pack.clone());
            compact_uploads.push(idx.clone());
        }
        let compact_idx_keep = compact_packs
            .iter()
            .map(|(_, _, idx, _)| idx.clone())
            .collect();
        upload_artifacts(cas, storage, compact_uploads.clone(), upload_concurrency()).await?;
        settle_storage(cas, storage, compact_uploads, compact_idx_keep).await;
        info!(
            "compacted Head packs for {}@{} ({} MiB delta -> {} packs)",
            repo_id.storage_key(),
            &commit[..7.min(commit.len())],
            delta_bytes / (1024 * 1024),
            compact_packs.len()
        );
        head_base = Some(crate::HeadBase {
            commit: commit.to_string(),
            packs: compact_packs.iter().map(tuple_to_sized).collect(),
        });
        head_packs = compact_packs;
    }

    let tagged: Vec<(&(String, u64, String, u64), bool)> = head_packs
        .iter()
        .map(|pack| (pack, false))
        .chain(history_packs.iter().map(|pack| (pack, true)))
        .collect();
    let (entries, idx_bundle_ref, idx_bundle_hash) = assemble_variant(cas, storage, &tagged)?;
    let manifest = make_manifest(
        commit,
        &parent,
        &[],
        &head.clonepack.metadata_chunk,
        storage
            .size(&head.clonepack.metadata_chunk)
            .context("size Head metadata for Full")?,
        entries,
        None,
        idx_bundle_ref,
    )?;
    let manifest_hash = cas.put(&manifest.encode_to_vec())?;
    let mut uploads = vec![manifest_hash.clone(), idx_bundle_hash.clone()];
    for (pack, _, idx, _) in &new_history {
        uploads.push(pack.clone());
        uploads.push(idx.clone());
    }
    uploads.retain(|hash| !hash.is_empty());
    let keep_idx = new_history
        .iter()
        .map(|(_, _, idx, _)| idx.clone())
        .collect();
    upload_artifacts(cas, storage, uploads.clone(), upload_concurrency()).await?;
    settle_storage(cas, storage, uploads, keep_idx).await;

    let mut all_packs = head_packs;
    all_packs.extend(history_packs);
    Ok(crate::FullResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: manifest_hash,
            metadata_chunk: head.clonepack.metadata_chunk.clone(),
            skeleton_pack: head.clonepack.skeleton_pack.clone(),
            skeleton_idx: head.clonepack.skeleton_idx.clone(),
            prebuilt_index: head.clonepack.prebuilt_index.clone(),
            midx: String::new(),
            idx_bundle: idx_bundle_hash,
            commit: commit.to_string(),
        },
        packs: pack_artifacts_of(&all_packs),
        history_levels: levels,
        head_base,
    })
}

/// Build and upload the Files result. `parent_files`, the parent's ready
/// Files result and commit, lets the archive reuse unchanged frames.
async fn build_files_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    commit: &str,
    head: &crate::HeadResult,
    parent_files: Option<(&str, &crate::FilesResult)>,
    storage: &crate::storage::StorageRef,
    compression_level: i32,
) -> Result<crate::FilesResult> {
    let previous_frames = parent_files
        .map(|(_, files)| files.archive_frames.clone())
        .unwrap_or_default();
    let previous_files = match parent_files {
        Some((_, files)) => {
            load_metadata_files(cas, storage, &files.clonepack.metadata_chunk).unwrap_or_default()
        }
        None => Vec::new(),
    };
    let previous_commit = parent_files
        .map(|(commit, _)| commit.to_string())
        .unwrap_or_default();
    let archive_mirror = mirror_dir.to_path_buf();
    let archive_cas = cas.clone();
    let archive_storage = storage.clone();
    let archive_commit = commit.to_string();
    let archive_output = tokio::task::spawn_blocking(move || {
        ArchiveBuilder::new(&archive_mirror).build_into_cas_bounded(
            &archive_commit,
            &archive_cas,
            Some(&archive_storage),
            compression_level,
            &previous_frames,
            &previous_files,
            &previous_commit,
            crate::archive::DEFAULT_ARCHIVE_CHUNK_SIZE,
        )
    })
    .await
    .context("Files archive task")??;

    let archive_chunk_hashes = archive_output.download_bundle_hashes;
    let mut metadata = archive_output.metadata;
    let fetch = |hash: &str| cas.get(hash).or_else(|_| storage.get(hash));
    metadata.skeleton_pack = fetch(&head.clonepack.skeleton_pack)?;
    metadata.skeleton_idx = fetch(&head.clonepack.skeleton_idx)?;
    metadata.prebuilt_index = fetch(&head.clonepack.prebuilt_index)?;
    let metadata_bytes = metadata.encode_to_vec();
    let metadata_hash = cas.put(&metadata_bytes)?;
    let archive_chunks = archive_chunk_refs(&archive_chunk_hashes, &metadata)?;
    let manifest = make_manifest(
        commit,
        &head.parent_commit,
        &archive_chunks,
        &metadata_hash,
        metadata_bytes.len() as u64,
        Vec::new(),
        None,
        None,
    )?;
    let manifest_hash = cas.put(&manifest.encode_to_vec())?;
    let uploads = archive_publish_upload_hashes(
        &metadata_hash,
        &manifest_hash,
        &archive_chunk_hashes,
        &archive_output.new_reuse_frame_hashes,
    );
    upload_artifacts(cas, storage, uploads.clone(), upload_concurrency()).await?;
    settle_storage(cas, storage, uploads, std::collections::HashSet::new()).await;

    Ok(crate::FilesResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: manifest_hash,
            metadata_chunk: metadata_hash,
            skeleton_pack: head.clonepack.skeleton_pack.clone(),
            skeleton_idx: head.clonepack.skeleton_idx.clone(),
            prebuilt_index: head.clonepack.prebuilt_index.clone(),
            midx: String::new(),
            idx_bundle: String::new(),
            commit: commit.to_string(),
        },
        archive_chunks: archive_chunk_hashes,
        archive_frames: archive_output.archive_frames,
    })
}

/// Process-global cap on concurrent upstream fetches/clones. Separate from the
/// build slots: a fetch is network/upstream bound, a build is CPU bound, so
/// they throttle independently.
fn fetch_semaphore() -> &'static tokio::sync::Semaphore {
    static SEM: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    SEM.get_or_init(|| tokio::sync::Semaphore::new(4))
}

/// One polling pass: for every added repository, cheaply resolve HEAD once
/// (`ls-remote`, under the fetch cap) and admit a build for that commit if it
/// is new. Catches pushes that arrived without a webhook/Actions trigger,
/// so build-before-clone still holds. Best-effort: per-repo errors are logged and
/// skipped. Returns the number of builds admitted. Exposed for tests.
pub async fn poll_once(state: &ServerState) -> usize {
    let repos = match state.control.list_repos().await {
        Ok(r) => r,
        Err(e) => {
            warn!("poll: list repos failed: {e}");
            return 0;
        }
    };
    let mut triggered = 0;
    let mut seen_repos = std::collections::HashSet::new();
    for added in repos {
        let repo_id = added.repo_id;
        if !seen_repos.insert(repo_id.storage_key()) {
            continue;
        }
        let Some(provider) = state
            .provider_registry
            .get(repo_id.provider.as_str())
            .cloned()
        else {
            continue; // unknown provider; skip
        };
        // Cheap tip probe, under the same fetch cap as a real fetch so a sweep
        // can't become uncapped upstream chatter. Best-effort.
        let credential = match state.broker.fetch_credential(&repo_id) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "poll: credential fetch for {} failed: {e:#}",
                    repo_id.storage_key()
                );
                continue;
            }
        };
        let tip = {
            let _permit = fetch_semaphore()
                .acquire()
                .await
                .expect("fetch semaphore never closed");
            git::ls_remote_tip_async(&provider, &repo_id, "HEAD", credential.as_ref()).await
        };
        let Ok(Some(tip)) = tip else {
            continue; // unknown HEAD / probe failed
        };
        let ref_name = tip
            .default_branch
            .as_deref()
            .map(|branch| format!("refs/heads/{branch}"));
        match admit_build(state, &repo_id, &tip.commit, ref_name.as_deref(), false).await {
            Ok((_, true)) => {
                triggered += 1;
                info!(
                    "poll: admitted HEAD build for {} at {}",
                    repo_id.storage_key(),
                    &tip.commit[..7.min(tip.commit.len())]
                );
            }
            Ok((_, false)) => {}
            Err(e) => warn!("poll: admit {}@HEAD failed: {e:#}", repo_id.storage_key()),
        }
    }
    triggered
}

/// Spawn the polling-fallback loop. `interval == 0` disables it.
fn spawn_poll_loop(state: ServerState, interval: Duration) {
    if interval.is_zero() {
        info!("poll fallback disabled (RIPCLONE_POLL_INTERVAL_SECS=0)");
        return;
    }
    info!("poll fallback enabled every {:?}", interval);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let n = poll_once(&state).await;
            if n > 0 {
                info!("poll fallback: triggered {n} build(s)");
            }
        }
    });
}

/// The server auth token hash.
///
/// Precedence:
///   1. RIPCLONE_SERVER_TOKEN_HASH (already hashed)
///   2. RIPCLONE_SERVER_TOKEN (raw, hashed here)
///
/// Fails when neither is set and non-empty.
fn server_auth_token_hash(hash: Option<String>, raw: Option<String>) -> Result<String> {
    if let Some(hash) = hash.filter(|t| !t.is_empty()) {
        return Ok(hash);
    }
    raw.filter(|t| !t.is_empty())
        .map(|t| hex::encode(Sha256::digest(t.as_bytes())))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "RIPCLONE_SERVER_TOKEN is not set. Refusing to start an unauthenticated server."
            )
        })
}

/// [`server_auth_token_hash`] from the environment.
fn read_server_auth_token() -> Result<String> {
    server_auth_token_hash(
        env::var("RIPCLONE_SERVER_TOKEN_HASH").ok(),
        env::var("RIPCLONE_SERVER_TOKEN").ok(),
    )
}

async fn serve(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    explicit_control_path: Option<&std::path::Path>,
    host: &str,
    port: u16,
) -> Result<()> {
    let default_control_path = repo_root.parent().unwrap_or(repo_root).join("control.db");
    let control_settings = crate::control::ControlSettings::from_sources(
        explicit_control_path,
        &default_control_path,
    )?;

    let token_hash = read_server_auth_token()?;
    info!("server auth token configured; auth middleware enabled");

    let provider_registry = ProviderRegistry::load().context("load provider registry")?;
    info!(
        "provider registry loaded with {} instance(s)",
        provider_registry.iter().count()
    );
    let broker = broker_from_env(provider_registry.clone())?;

    let rate_burst_raw = env::var("RIPCLONE_RATE_LIMIT_BURST").ok();
    let rate_per_sec_raw = env::var("RIPCLONE_RATE_LIMIT_PER_SEC").ok();
    let (rate_burst, rate_per_sec) =
        parse_rate_limit_settings(rate_burst_raw.as_deref(), rate_per_sec_raw.as_deref())?;
    let rate_limiter = RateLimiter::new(rate_burst, rate_per_sec);
    info!(
        "rate limiter enabled: burst={}, restore={}/s",
        rate_burst, rate_per_sec
    );

    backends::Backends::validate_environment()?;
    let worker_settings = worker::WorkerSettings::from_env()?;

    let metrics = Metrics::new();
    let control: Arc<dyn ControlStore> = Arc::new(
        crate::control::SqliteControlStore::open(
            &control_settings.path,
            worker_settings.max_attempts,
            Arc::new(crate::control::SystemClock),
        )
        .await?,
    );
    // This process holds the control database's ownership lock, so no other
    // worker holds a lease: every lease is from a previous process.
    let reset = control.reset_leases().await?;
    info!(
        path = %control_settings.path.display(),
        reset_leases = reset,
        "server owns control database"
    );
    std::fs::create_dir_all(cas_dir)?;
    std::fs::create_dir_all(repo_root)?;
    let b = backends::Backends::from_env(cas_dir, &metrics)?;
    b.cache_retention.clone().spawn_from_env();

    // Webhook receiver config: per-provider secret + optional allowlist (built
    // before the registry is moved into the state). A push to a configured
    // webhook triggers a build before any clone — no per-repo Actions workflow.
    let webhook_config = Arc::new(WebhookConfig::from_env(&provider_registry));

    let state = ServerState {
        cas: b.cas,
        storage: b.storage,
        repo_root: repo_root.to_path_buf(),
        control,
        head_wake: Arc::new(tokio::sync::Notify::new()),
        background_wake: Arc::new(tokio::sync::Notify::new()),
        provider_registry,
        broker,
        token_hash: Some(token_hash),
        metrics,
        rate_limiter,
        webhook_config,
        sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        readyz_cache: Arc::new(std::sync::Mutex::new(None)),
    };

    worker::spawn_workers(&state, &worker_settings);

    // Polling fallback: catches pushes that arrived without a webhook/Actions
    // trigger so build-before-clone still holds. Defaults to 5 minutes so
    // webhook-less self-hosts still self-heal missed or stuck builds.
    const DEFAULT_POLL_INTERVAL_SECS: u64 = 300;
    let poll_interval = Duration::from_secs(
        env::var("RIPCLONE_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_POLL_INTERVAL_SECS),
    );
    spawn_poll_loop(state.clone(), poll_interval);

    let app = build_app(state);

    // Bind only after config and control ownership are checked, so a rejected
    // server never takes the port.
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    // Log the bound address, not the requested one: with `--port 0` this line
    // is how a caller learns the port.
    info!(
        "ripclone server listening on http://{}",
        listener.local_addr()?
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

pub async fn run_server(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    host: &str,
    port: u16,
) -> Result<()> {
    serve(cas_dir, repo_root, None, host, port).await
}

pub async fn run_server_with_control(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    control_path: &std::path::Path,
    host: &str,
    port: u16,
) -> Result<()> {
    serve(cas_dir, repo_root, Some(control_path), host, port).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::util::ServiceExt;

    fn ready_artifacts(commit: &str, label: &str) -> crate::ClonepackArtifacts {
        let hash = |suffix: &str| crate::cas::hash(format!("{label}-{suffix}").as_bytes());
        crate::ClonepackArtifacts {
            manifest: hash("manifest"),
            metadata_chunk: hash("metadata"),
            skeleton_pack: hash("skeleton-pack"),
            skeleton_idx: hash("skeleton-idx"),
            prebuilt_index: hash("index"),
            idx_bundle: hash("idx-bundle"),
            commit: commit.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn exact_result_requires_the_requested_stored_result() {
        let commit = "a".repeat(40);
        let info = RefInfo {
            commit: commit.clone(),
            full: Some(crate::FullResult {
                clonepack: ready_artifacts(&commit, "full"),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!exact_result_ready(&info, ExactResultKind::Head, &commit));
        assert!(exact_result_ready(&info, ExactResultKind::Full, &commit));
        assert!(!exact_result_ready(&info, ExactResultKind::Files, &commit));
        assert!(!exact_result_ready(
            &info,
            ExactResultKind::Full,
            &"b".repeat(40)
        ));
    }
    #[tokio::test]
    async fn pending_body_includes_the_selected_branch() {
        let response = artifact_pending_response(&"a".repeat(40), "rélease/東京", 3).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some("r%C3%A9lease%2F%E6%9D%B1%E4%BA%AC")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read pending body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("pending JSON");
        let keys = body.as_object().expect("pending object");
        assert_eq!(keys.len(), 5);
        assert_eq!(body["code"], "artifact_pending");
        assert_eq!(body["commit"], "a".repeat(40));
        assert_eq!(body["branch"], "rélease/東京");
        assert_eq!(body["status"], "building");
        assert_eq!(body["queue_depth"], 3);
    }

    /// A `ServerState` on a real control store in `tmp`, with no workers.
    async fn test_state(tmp: &tempfile::TempDir) -> ServerState {
        let cas_root = tmp.path().join("cas");
        let cas = Cas::new(&cas_root).unwrap();
        let storage = crate::storage::local(&cas_root).unwrap();
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&repo_root).unwrap();
        let control: Arc<dyn ControlStore> = Arc::new(
            crate::control::SqliteControlStore::open(
                &tmp.path().join("control.db"),
                5,
                Arc::new(crate::control::SystemClock),
            )
            .await
            .unwrap(),
        );
        let provider_registry = ProviderRegistry::new();
        let broker: Arc<dyn CredentialBroker> = Arc::new(crate::auth::broker::StaticBroker::new(
            provider_registry.clone(),
        ));
        ServerState {
            cas,
            storage,
            repo_root,
            control,
            head_wake: Arc::new(tokio::sync::Notify::new()),
            background_wake: Arc::new(tokio::sync::Notify::new()),
            provider_registry,
            broker,
            token_hash: Some(hex::encode(Sha256::digest("secret"))),
            metrics: Metrics::new(),
            rate_limiter: RateLimiter::new(100, 100.0),
            // No webhook secret here; webhook tests install their own.
            webhook_config: Arc::new(WebhookConfig::default()),
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            readyz_cache: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    fn auth_header() -> String {
        format!("Ripclone {}", hex::encode(Sha256::digest("secret")))
    }

    #[tokio::test]
    async fn repository_list_is_authenticated_and_returns_repo_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let github = RepoId::github("zeta/repo");
        let gitlab = RepoId {
            provider: crate::provider::ProviderInstanceId::new("gitlab"),
            path: "group/sub/repo".to_string(),
        };
        mark_added(&state, github.clone()).await;
        mark_added(&state, gitlab.clone()).await;
        let control = Arc::clone(&state.control);
        let app = build_app(state);

        let response = app
            .clone()
            .oneshot(test_request("GET", "/v1/repos"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let repos: Vec<RepoId> = serde_json::from_slice(&body).unwrap();
        assert_eq!(repos.len(), 2);
        assert!(repos.contains(&github));
        assert!(repos.contains(&gitlab));

        for auth in [None, Some("Ripclone wrong")] {
            let rejected = app
                .clone()
                .oneshot(request_with_auth("GET", "/v1/repos", auth))
                .await
                .unwrap();
            assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        }

        for auth in [None, Some("Ripclone wrong")] {
            let rejected_remove = app
                .clone()
                .oneshot(request_with_auth(
                    "DELETE",
                    "/v1/repos/github/zeta/repo/add",
                    auth,
                ))
                .await
                .unwrap();
            assert_eq!(rejected_remove.status(), StatusCode::UNAUTHORIZED);
            assert!(
                control.get_repo(&github).await.unwrap().is_some(),
                "rejected removal must preserve the registration"
            );
        }
    }

    #[tokio::test]
    async fn repository_remove_rejects_missing_and_invalid_without_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let store = Arc::clone(&state.control);
        let repo_id = RepoId::github("acme/kept");
        mark_added(&state, repo_id.clone()).await;
        let app = build_app(state);

        let missing = app
            .clone()
            .oneshot(test_request("DELETE", "/v1/repos/github/acme/missing/add"))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let missing_body = axum::body::to_bytes(missing.into_body(), usize::MAX)
            .await
            .unwrap();
        let missing_body: serde_json::Value = serde_json::from_slice(&missing_body).unwrap();
        assert_eq!(missing_body["code"], "repo_not_added");
        assert!(store.get_repo(&repo_id).await.unwrap().is_some());

        let invalid = app
            .clone()
            .oneshot(test_request(
                "DELETE",
                "/v1/repos/github/acme/bad%7Frepo/add",
            ))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert!(
            store.get_repo(&repo_id).await.unwrap().is_some(),
            "invalid removal must leave other registrations intact"
        );
    }

    #[tokio::test]
    async fn repository_remove_leaves_pending_and_claimed_builds_active() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let repo_id = RepoId::github("acme/pending");
        mark_added(&state, repo_id.clone()).await;
        let (first, second) = ("a".repeat(40), "b".repeat(40));
        state.control.admit(&repo_id, &first, None).await.unwrap();
        state.control.admit(&repo_id, &second, None).await.unwrap();
        let lease = Duration::from_secs(60);
        let claimed = state
            .control
            .claim("worker-1", crate::control::Pool::Head, lease)
            .await
            .unwrap()
            .expect("first build is claimable");
        assert_eq!(claimed.build.commit, first);

        let response = remove_added_repo_inner(repo_id.clone(), state.clone()).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(state.control.get_repo(&repo_id).await.unwrap().is_none());
        assert!(
            state.control.renew(&claimed, lease).await.unwrap(),
            "removal must not take a running build's lease"
        );
        let next = state
            .control
            .claim("worker-2", crate::control::Pool::Head, lease)
            .await
            .unwrap()
            .expect("the pending build stays claimable after removal");
        assert_eq!(next.build.commit, second);
    }

    #[test]
    fn archive_publish_uploads_download_bundles_and_reuse_frames() {
        let uploads = archive_publish_upload_hashes(
            "metadata",
            "clonepack",
            &["bundle-b".to_string(), "bundle-a".to_string()],
            &["frame-a".to_string(), "bundle-a".to_string(), String::new()],
        );
        assert_eq!(
            uploads,
            vec![
                "bundle-a".to_string(),
                "bundle-b".to_string(),
                "clonepack".to_string(),
                "frame-a".to_string(),
                "metadata".to_string(),
            ]
        );
    }

    #[test]
    fn archive_chunk_refs_rejects_hash_length_mismatch() {
        let mut metadata = crate::clonepack::MetadataChunk::new();
        metadata.frames.push(crate::clonepack::FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 4,
            raw_len: 10,
        });
        metadata.frames.push(crate::clonepack::FrameInfo {
            chunk_index: 1,
            chunk_offset: 0,
            compressed_len: 5,
            raw_len: 11,
        });

        assert!(archive_chunk_refs(&["a".repeat(64)], &metadata).is_err());
    }

    #[test]
    fn validate_repo_id_accepts_names_and_rejects_paths() {
        for (id, ok) in [
            ("ripclone", true),
            ("ripclone-rs", true),
            ("ripclone.rs", true),
            ("rip_clone", true),
            ("..", false),
            ("foo/bar", false),
            ("foo\\bar", false),
            ("foo\0bar", false),
            ("", false),
        ] {
            assert_eq!(validate_repo_id(id).is_ok(), ok, "{id:?}");
        }
    }

    #[test]
    fn server_auth_token_hash_prefers_the_hash_and_requires_a_token() {
        let s = |v: &str| Some(v.to_string());
        let secret = Some(hex::encode(Sha256::digest("secret")));
        for (hash, raw, want) in [
            (None, None, None),
            (s(""), s(""), None),
            (None, s("secret"), secret.clone()),
            (s(""), s("secret"), secret),
            (s("given-hash"), s("secret"), s("given-hash")),
            (s("given-hash"), None, s("given-hash")),
        ] {
            let case = format!("{hash:?} {raw:?}");
            assert_eq!(server_auth_token_hash(hash, raw).ok(), want, "{case}");
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

    #[tokio::test]
    async fn authenticated_artifact_fanout_does_not_consume_control_plane_rate_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp).await;
        state.rate_limiter = RateLimiter::new(1, 0.0);

        let data = b"immutable artifact";
        let hash = hex::encode(Sha256::digest(data));
        state.storage.put(&hash, data).unwrap();
        let app = build_app(state);
        let artifact_path = format!("/v1/artifacts/{hash}");

        // A logical clone can request many authenticated immutable objects from
        // one IP. Even a one-token control-plane bucket must not reject them.
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(request_with_auth(
                    "GET",
                    &artifact_path,
                    Some(&auth_header()),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), data);
        }

        // The exemption is narrow: ordinary control-plane requests from the
        // same IP still consume the configured bucket and then receive 429.
        let first = app
            .clone()
            .oneshot(request_with_auth("GET", "/readyz", None))
            .await
            .unwrap();
        assert_ne!(first.status(), StatusCode::TOO_MANY_REQUESTS);
        let second = app
            .oneshot(request_with_auth("GET", "/readyz", None))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn local_storage_does_not_produce_signed_urls() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = crate::storage::local(tmp.path()).unwrap();
        let info = RefInfo {
            commit: "abc".to_string(),
            full: Some(crate::FullResult {
                clonepack: crate::ClonepackArtifacts {
                    manifest: "exact-manifest".to_string(),
                    metadata_chunk: "exact-metadata".to_string(),
                    commit: "exact-commit".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let provider = ProviderRegistry::new().get("github").unwrap().clone();
        let repo_id = RepoId::github("o/r");
        let resp = ref_response(
            &repo_id,
            &provider,
            "main".to_string(),
            &info,
            &storage,
            ExactResultKind::Full,
        );
        assert_eq!(resp.commit, "exact-commit");
        assert_eq!(resp.clonepack_manifest, "exact-manifest");
        assert_eq!(resp.metadata_chunk, "exact-metadata");
        assert!(resp.clonepack_manifest_url.is_none());
        assert!(resp.metadata_chunk_url.is_none());
        assert!(resp.archive_chunk_urls.is_none());
    }

    /// Signs every hash as `signed:<ttl-secs>:<hash>` so a response's URL
    /// fields pin which hash was signed, in what order, and with which TTL.
    struct SigningStorage;

    #[async_trait::async_trait]
    impl crate::storage::StorageBackend for SigningStorage {
        fn get(&self, _hash: &str) -> Result<Vec<u8>> {
            anyhow::bail!("unsupported")
        }

        fn get_range(&self, _hash: &str, _start: u64, _len: u64) -> Result<Vec<u8>> {
            anyhow::bail!("unsupported")
        }

        fn put(&self, _hash: &str, _data: &[u8]) -> Result<()> {
            anyhow::bail!("unsupported")
        }

        fn size(&self, _hash: &str) -> Result<u64> {
            anyhow::bail!("unsupported")
        }

        fn verify_durable_copy(&self, _hash: &str) -> Result<()> {
            anyhow::bail!("unsupported")
        }

        fn signed_url(&self, hash: &str, expires_in: Duration) -> Option<String> {
            Some(format!("signed:{}:{hash}", expires_in.as_secs()))
        }
    }

    #[test]
    fn ref_response_signs_every_artifact_in_manifest_order() {
        let storage: StorageRef = Arc::new(SigningStorage);
        let commit = "c".repeat(40);
        let parent = "p".repeat(40);
        let pack = |name: &str| crate::PackArtifact {
            pack: format!("{name}-pack"),
            idx: format!("{name}-idx"),
        };
        let info = RefInfo {
            commit: commit.clone(),
            head: Some(crate::HeadResult {
                clonepack: ready_artifacts(&commit, "head"),
                parent_commit: Some(parent.clone()),
                packs: vec![pack("h1")],
                ..Default::default()
            }),
            full: Some(crate::FullResult {
                clonepack: crate::ClonepackArtifacts {
                    midx: "full-midx".to_string(),
                    ..ready_artifacts(&commit, "full")
                },
                packs: vec![pack("f1"), pack("f2"), pack("f3")],
                ..Default::default()
            }),
            files: Some(crate::FilesResult {
                clonepack: ready_artifacts(&commit, "files"),
                archive_chunks: vec!["a1".to_string(), "a2".to_string()],
                ..Default::default()
            }),
        };
        let provider = ProviderRegistry::new().get("github").unwrap().clone();
        let repo_id = RepoId::github("o/r");
        let signed = |ttl: u64, hash: &str| Some(format!("signed:{ttl}:{hash}"));

        let full = ref_response(
            &repo_id,
            &provider,
            "main".to_string(),
            &info,
            &storage,
            ExactResultKind::Full,
        );
        let full_artifacts = info.full.as_ref().unwrap();
        assert_eq!(full.commit, commit);
        assert_eq!(full.parent_commit.as_deref(), Some(parent.as_str()));
        assert_eq!(full.clonepack_manifest, full_artifacts.clonepack.manifest);
        assert_eq!(
            full.clonepack_manifest_url,
            signed(1200, &full_artifacts.clonepack.manifest)
        );
        assert_eq!(
            full.metadata_chunk_url,
            signed(1200, &full_artifacts.clonepack.metadata_chunk)
        );
        assert_eq!(
            full.pack_chunk_urls,
            Some(vec![
                signed(1200, "f1-pack"),
                signed(1200, "f2-pack"),
                signed(1200, "f3-pack"),
            ])
        );
        assert_eq!(full.midx_url, signed(1200, "full-midx"));
        assert_eq!(
            full.idx_bundle_url,
            signed(1200, &full_artifacts.clonepack.idx_bundle)
        );
        assert!(full.archive_chunk_urls.is_none());
        assert!(full.head_blobs_chunk_urls.is_none());
        assert!(full.head_blobs_idx_url.is_none());

        let files = ref_response(
            &repo_id,
            &provider,
            "main".to_string(),
            &info,
            &storage,
            ExactResultKind::Files,
        );
        assert_eq!(
            files.archive_chunk_urls,
            Some(vec![signed(1200, "a1"), signed(1200, "a2")])
        );
        assert!(files.pack_chunk_urls.is_none());
        assert_eq!(
            files.clonepack_manifest_url,
            signed(1200, &info.files.as_ref().unwrap().clonepack.manifest)
        );
        // No MIDX on the Files variant: an empty hash is never signed.
        assert!(files.midx_url.is_none());
        assert_eq!(files.parent_commit.as_deref(), Some(parent.as_str()));

        let head = ref_response(
            &repo_id,
            &provider,
            "main".to_string(),
            &info,
            &storage,
            ExactResultKind::Head,
        );
        assert_eq!(head.pack_chunk_urls, Some(vec![signed(1200, "h1-pack")]));
    }

    #[test]
    fn rate_limiter_sanitizes_invalid_restore_rates() {
        for rate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            let limiter = RateLimiter::new(1, rate);
            assert!(limiter.check("client"));
            assert!(!limiter.check("client"));
        }
    }

    #[test]
    fn rate_limit_config_rejects_invalid_values() {
        for rate in ["NaN", "inf", "-inf", "-1"] {
            let error = parse_rate_limit_settings(None, Some(rate)).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("RIPCLONE_RATE_LIMIT_PER_SEC must be finite and non-negative"),
                "got: {error:#}"
            );
        }

        let error = parse_rate_limit_settings(Some("not-a-number"), None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("RIPCLONE_RATE_LIMIT_BURST must be an unsigned integer"),
            "got: {error:#}"
        );
        assert_eq!(
            parse_rate_limit_settings(Some("0"), Some("0")).unwrap(),
            (0, 0.0)
        );
    }

    #[test]
    fn rate_limiter_retains_fractional_refill_credit() {
        let limiter = RateLimiter::new(1, 10.0);
        assert!(limiter.check("client"));

        for expected in [false, true] {
            limiter.buckets.lock().unwrap().get_mut("client").unwrap().0 =
                Instant::now() - Duration::from_millis(50);
            assert_eq!(limiter.check("client"), expected);
        }
    }

    fn test_request(method: &str, uri: &str) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("Authorization", auth_header())
            .header("x-ripclone-protocol", crate::PROTOCOL_VERSION)
            .body(Body::empty())
            .unwrap()
    }

    /// Like `test_request` but with an explicit (or absent) `Authorization`
    /// header, for exercising the auth middleware's reject path.
    fn request_with_auth(method: &str, uri: &str, auth: Option<&str>) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("x-ripclone-protocol", crate::PROTOCOL_VERSION);
        if let Some(a) = auth {
            b = b.header("Authorization", a);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn public_endpoints_require_no_auth() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
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

    fn gh_sign(secret: &str, body: &[u8]) -> String {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    const WEBHOOK_SECRET: &str = "shhh-very-secret";

    fn webhook_request(
        provider: &str,
        event: &str,
        signature: Option<&str>,
        body: Vec<u8>,
    ) -> axum::http::Request<axum::body::Body> {
        let mut b = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/webhooks/{provider}"))
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-GitHub-Event", event);
        if let Some(sig) = signature {
            b = b.header("X-Hub-Signature-256", sig);
        }
        b.body(axum::body::Body::from(body)).unwrap()
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

    /// A push payload that omits `repository.default_branch`, to exercise the
    /// mirror fallback.
    fn gh_push_body_no_default(owner: &str, repo: &str, ref_: &str, after: &str) -> Vec<u8> {
        serde_json::json!({
            "ref": ref_,
            "after": after,
            "deleted": false,
            "repository": {"name": repo, "owner": {"login": owner}, "private": false}
        })
        .to_string()
        .into_bytes()
    }

    /// A test state with a configured `github`
    /// webhook secret. Use `claim_admitted_job` to see what a trigger admitted.
    async fn webhook_state(tmp: &tempfile::TempDir) -> ServerState {
        let mut state = test_state(tmp).await;
        state.webhook_config = Arc::new(WebhookConfig::with_secret("github", WEBHOOK_SECRET));
        state
    }

    async fn mark_added(state: &ServerState, repo_id: RepoId) {
        state.control.add_repo(&repo_id).await.unwrap();
    }

    /// Claims the Head of the one build a trigger admitted. `None` when
    /// nothing was admitted.
    async fn claim_admitted_job(state: &ServerState) -> Option<crate::control::Claim> {
        state
            .control
            .claim(
                "test-worker",
                crate::control::Pool::Head,
                Duration::from_secs(60),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn webhook_without_secret_returns_503() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await; // no webhook secret configured
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
    async fn webhook_ping_is_acknowledged_without_build() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
        let body = br#"{"zen":"keep it simple"}"#.to_vec();
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "ping", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "ping must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_non_default_branch_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
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
            claim_admitted_job(&state).await.is_none(),
            "non-default push must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_allowlist_blocks_unlisted_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp).await;
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                .with_allowlist(["acme/allowed".to_string()]),
        );
        let app = build_app(state.clone());
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
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "unlisted repo must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_github_allowlist_accepts_bare_and_prefixed() {
        // Both the canonical `owner/repo` and the forgiving `github/owner/repo`
        // forms admit a github repo, so github's bare-key asymmetry vs the
        // `gitlab/...` form isn't a silent footgun.
        for entry in ["acme/widget", "github/acme/widget"] {
            let tmp = tempfile::tempdir().unwrap();
            let mut state = test_state(&tmp).await;
            state.webhook_config = Arc::new(
                WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                    .with_allowlist([entry.to_string()]),
            );
            mark_added(&state, RepoId::github("acme/widget")).await;
            let app = build_app(state.clone());
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
            assert_eq!(resp.status(), StatusCode::OK, "allowlist entry {entry}");
            assert!(
                claim_admitted_job(&state).await.is_some(),
                "entry {entry} must admit the repo"
            );
        }
    }

    #[tokio::test]
    async fn webhook_tag_push_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
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
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "a tag push must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_hostile_branch_name_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
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
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "an invalid branch must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_branch_delete_preserves_exact_result() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let repo = RepoId::github("acme/widget");
        let commit = "d".repeat(40);
        state.control.admit(&repo, &commit, None).await.unwrap();
        let control = Arc::clone(&state.control);
        let app = build_app(state.clone());
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
            control.status(&repo, &commit).await.unwrap().is_some(),
            "branch deletion must not delete an exact commit result"
        );
        assert_eq!(
            control.list_builds(&repo).await.unwrap().len(),
            1,
            "a delete must not admit a build"
        );
    }

    #[tokio::test]
    async fn webhook_unknown_provider_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state);
        let body = br#"{}"#.to_vec();
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("nope", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// Build state with a single non-default provider instance configured plus
    /// its webhook secret.
    async fn provider_webhook_state(
        tmp: &tempfile::TempDir,
        id: &str,
        kind: &str,
        host: &str,
    ) -> ServerState {
        let mut state = test_state(tmp).await;
        let mut registry = ProviderRegistry::new();
        registry
            .merge_one(crate::provider::ProviderConfig {
                id: id.to_string(),
                kind: Some(kind.to_string()),
                host: Some(host.to_string()),
                auth_template: (kind == "generic").then(|| "token {token}".to_string()),
                ..Default::default()
            })
            .unwrap();
        state.provider_registry = registry;
        state.webhook_config = Arc::new(WebhookConfig::with_secret(id, WEBHOOK_SECRET));
        state
    }

    #[tokio::test]
    async fn webhook_provider_without_adapter_returns_501() {
        let tmp = tempfile::tempdir().unwrap();
        let state = provider_webhook_state(&tmp, "generic", "generic", "git.example.com").await;
        let app = build_app(state);
        let resp = app
            .oneshot(webhook_request(
                "generic",
                "push",
                Some("whatever"),
                br#"{}"#.to_vec(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn webhook_gitlab_allowlist_matches_natural_key() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = provider_webhook_state(&tmp, "gitlab", "gitlab", "gitlab.com").await;
        // The allowlist is written in the operator-facing natural form
        // (provider-prefixed, unescaped) — not the escaped storage key.
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("gitlab", WEBHOOK_SECRET)
                .with_allowlist(["gitlab/group/sub/proj".to_string()]),
        );
        mark_added(
            &state,
            RepoId {
                provider: crate::provider::ProviderInstanceId::new("gitlab"),
                path: "group/sub/proj".to_string(),
            },
        )
        .await;
        let app = build_app(state.clone());
        let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","project":{"path_with_namespace":"group/sub/proj","default_branch":"main"}}"#.to_vec();
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/webhooks/gitlab")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-Gitlab-Event", "Push Hook")
            .header("X-Gitlab-Token", WEBHOOK_SECRET)
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            claim_admitted_job(&state)
                .await
                .expect("allowlisted gitlab repo enqueues")
                .repo
                .path,
            "group/sub/proj"
        );
    }

    #[tokio::test]
    async fn webhook_missing_default_identity_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let state = webhook_state(&tmp).await;
        let app = build_app(state.clone());
        let body =
            gh_push_body_no_default("acme", "widget", "refs/heads/whatever", &"1".repeat(40));
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            claim_admitted_job(&state).await.is_none(),
            "missing default-branch identity is ignored"
        );
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
    async fn protocol_guard_rejects_only_explicit_wrong_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let app = build_app(state);
        for protocol in [Some("1"), Some("999"), Some("invalid")] {
            let response = app
                .clone()
                .oneshot(protocol_request("/v1/repos/acme/secret/status", protocol))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
        }
        let response = app
            .clone()
            .oneshot(protocol_request(
                "/v1/repos/github/acme/secret/status",
                None,
            ))
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UPGRADE_REQUIRED);
        let current = crate::PROTOCOL_VERSION.to_string();
        let response = app
            .oneshot(protocol_request(
                "/v1/repos/github/acme/secret/status",
                Some(&current),
            ))
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UPGRADE_REQUIRED);
    }

    #[tokio::test]
    async fn repo_status_returns_empty_for_cold_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
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
        let state = test_state(&tmp).await;
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_not_ready_when_storage_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        // Simulate the data volume being unmounted/removed under the server.
        std::fs::remove_dir_all(tmp.path().join("cas")).unwrap();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_endpoint_is_prometheus_text() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
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
        let state = test_state(&tmp).await;
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
    async fn repo_status_reports_each_stored_result_and_current_job() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let repo_id = RepoId::github("acme/status-results");
        let commit = "a".repeat(40);
        let metadata = state.cas.put(b"status metadata").unwrap();
        let manifest =
            make_manifest(&commit, &None, &[], &metadata, 15, Vec::new(), None, None).unwrap();
        let manifest = state.cas.put(&manifest.encode_to_vec()).unwrap();
        let mut head_artifacts = ready_artifacts(&commit, "status-head");
        head_artifacts.manifest = manifest.clone();
        head_artifacts.metadata_chunk = metadata.clone();
        let mut full_artifacts = ready_artifacts(&commit, "status-full");
        full_artifacts.manifest = manifest;
        full_artifacts.metadata_chunk = metadata;
        let lease = Duration::from_secs(60);
        state.control.admit(&repo_id, &commit, None).await.unwrap();
        let head_claim = state
            .control
            .claim("status-head", crate::control::Pool::Head, lease)
            .await
            .unwrap()
            .unwrap();
        let head = crate::control::Output::Head {
            result: crate::HeadResult {
                clonepack: head_artifacts,
                ..Default::default()
            },
            parent: None,
        };
        assert!(
            state
                .control
                .publish(&head_claim, head)
                .await
                .unwrap()
                .applied
        );
        state.control.release(&head_claim).await.unwrap();
        let background = state
            .control
            .claim("status-background", crate::control::Pool::Background, lease)
            .await
            .unwrap()
            .unwrap();
        let full = crate::control::Output::Full(crate::FullResult {
            clonepack: full_artifacts,
            ..Default::default()
        });
        assert!(
            state
                .control
                .publish(&background, full)
                .await
                .unwrap()
                .applied
        );

        let status = build_repo_status(&state, &repo_id, false, None)
            .await
            .unwrap();
        assert_eq!(status.refs.len(), 1);
        let exact = &status.refs[0];
        assert!(exact.head);
        assert!(exact.full);
        assert!(!exact.files);
        assert_eq!(exact.job, "pending");
        assert!(exact.job_error.is_none());

        state
            .control
            .fail(&background, ExactResultKind::Files, "archive broke", None)
            .await
            .unwrap();
        let status = build_repo_status(&state, &repo_id, false, None)
            .await
            .unwrap();
        let exact = &status.refs[0];
        assert!(!exact.files);
        assert_eq!(exact.job, "failed");
        assert_eq!(exact.job_error.as_deref(), Some("archive broke"));
    }

    #[tokio::test]
    async fn sync_rejects_invalid_branch_name() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
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
}
