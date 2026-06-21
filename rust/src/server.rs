use crate::RefInfo;
use crate::archive::ArchiveBuilder;
use crate::cas::Cas;
use crate::clonepack::{ChunkRef, ClonepackManifest, hash_from_hex, hash_to_hex};
use crate::git;
use crate::metrics::Metrics;
use crate::oidc::OidcVerifier;
use crate::pack::PackBuilder;
use crate::ref_store::{CachingRefStore, FileRefStore, RefStore, S3RefStore, migrate_legacy_refs};
use crate::retention::Retention;
use crate::snapshot::SnapshotBuilder;
use crate::storage::{S3Storage, StorageRef, local};
use crate::validation;
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, State},
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
use std::net::SocketAddr;
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
    pub token_hash: Option<String>,
    pub github_token: Option<String>,
    pub metrics: Arc<Metrics>,
    pub rate_limiter: RateLimiter,
    pub retention: Arc<Retention>,
    pub build_queue: tokio::sync::mpsc::Sender<BuildJob>,
    pub build_queue_depth: Arc<AtomicUsize>,
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
}

#[derive(Deserialize)]
pub struct RefQuery {
    /// Clonepack variant to return: "full" (all reachable history) or
    /// "shallow" (depth=1). Defaults to "full" for backward compatibility.
    #[serde(default = "default_clonepack_kind")]
    pub clonepack: String,
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

#[derive(Clone)]
pub struct BuildJob {
    pub owner: String,
    pub repo: String,
    pub github_token: Option<secrecy::SecretString>,
}

fn default_branch_value() -> String {
    "HEAD".to_string()
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
    owner: &str,
    repo: &str,
) -> Arc<tokio::sync::Mutex<()>> {
    let key = format!("{}/{}", owner, repo);
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

#[derive(Serialize)]
pub struct RefResponse {
    pub owner: String,
    pub repo: String,
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
}

#[derive(Serialize, Deserialize)]
pub struct BranchStatusEntry {
    pub branch: String,
    pub commit: String,
    pub manifest: String,
    pub bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub built_at: Option<String>,
}

pub fn build_app(state: ServerState) -> Router {
    let protected = Router::new()
        .route("/v1/repos/{owner}/{repo}/refs/{branch}", get(get_ref))
        .route("/v1/repos/{owner}/{repo}/sync", post(sync_repo))
        .route("/v1/repos/{owner}/{repo}/status", get(repo_status))
        .route("/v1/repos/{owner}/{repo}/cat", get(cat_file))
        .route("/v1/repos/{owner}/{repo}/sizes", get(file_sizes))
        .route("/v1/repos/{owner}/{repo}/snapshot", post(create_snapshot))
        .route("/v1/repos/{owner}/{repo}/hotfiles", get(get_hotfiles))
        .route("/v1/repos/{owner}/{repo}/batch", post(batch_files))
        .route("/v1/packs/{hash}", get(get_pack))
        .route("/v1/objects/{sha}", get(get_object))
        .route("/v1/artifacts/{hash}", get(get_artifact))
        .route("/v1/archives/{hash}", get(get_artifact))
        .route("/v1/manifests/{hash}", get(get_artifact))
        .route("/v1/git/{owner}/{repo}/info/refs", get(git_info_refs))
        .route(
            "/v1/git/{owner}/{repo}/git-upload-pack",
            post(git_upload_pack),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state.clone());

    let rate_limited = Router::new()
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .route("/v1/build", post(build_handler))
        .merge(protected)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ))
        .with_state(state.clone());

    Router::new()
        .route("/healthz", get(healthz))
        .merge(rate_limited)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

/// Maximum request body size accepted by the server. This bounds the
/// `git-upload-pack` body and any other large POST payload.
const MAX_REQUEST_BODY_BYTES: usize = 256 * 1024 * 1024;
const MAX_UPLOAD_PACK_BODY_BYTES: usize = 256 * 1024 * 1024;

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

async fn rate_limit_middleware(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let key = addr.ip().to_string();
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

async fn readyz() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

async fn metrics_handler(State(state): State<ServerState>) -> impl IntoResponse {
    Json(state.metrics.snapshot())
}

#[derive(Deserialize)]
struct GitServiceQuery {
    service: String,
}

/// Smart-HTTP `info/refs` fallback. Advertises refs so a vanilla git client can
/// talk to ripclone when the archive-first path is not available.
async fn git_info_refs(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<GitServiceQuery>,
    State(state): State<ServerState>,
) -> Response {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if query.service != "git-upload-pack" {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "only git-upload-pack is supported".to_string(),
            }),
        )
            .into_response();
    }

    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
    let github_token = state
        .github_token
        .clone()
        .map(|s| secrecy::SecretString::new(s.into()));
    let lock = repo_lock(&state.sync_locks, &owner, &repo).await;
    let _guard = lock.lock().await;
    if let Err(e) = ensure_mirror(&mirror_dir, &owner, &repo, "HEAD", github_token.as_ref()).await {
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
async fn git_upload_pack(
    Path((owner, repo)): Path<(String, String)>,
    State(state): State<ServerState>,
    body: Body,
) -> Response {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
    let github_token = state
        .github_token
        .clone()
        .map(|s| secrecy::SecretString::new(s.into()));
    let lock = repo_lock(&state.sync_locks, &owner, &repo).await;
    let _guard = lock.lock().await;
    if let Err(e) = ensure_mirror(&mirror_dir, &owner, &repo, "HEAD", github_token.as_ref()).await {
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
    owner: &str,
    repo: &str,
    branch: &str,
    github_token: Option<&secrecy::SecretString>,
) -> Result<()> {
    let mirror_dir = mirror_dir.to_path_buf();
    let owner = owner.to_string();
    let repo = repo.to_string();
    let branch = branch.to_string();
    let github_token = github_token.map(|s| s.expose_secret().to_string());
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror(&mirror_dir, &owner, &repo, &branch, github_token.as_deref())
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

async fn get_ref(
    Path((owner, repo, branch)): Path<(String, String, String)>,
    Query(params): Query<RefQuery>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&branch)) {
        return resp;
    }
    state.metrics.record_ref_lookup();
    let key = format!("{}/{}/{}", owner, repo, branch);

    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
    let github_token = state
        .github_token
        .clone()
        .map(|s| secrecy::SecretString::new(s.into()));

    // Serialize syncs for this repo so concurrent fetches do not corrupt the
    // bare mirror directory. Acquiring the lock also means any in-progress sync
    // for this repo has finished by the time we proceed.
    let fresh_key = format!("{}/{}/{}", owner, repo, branch);
    let lock = repo_lock(&state.sync_locks, &owner, &repo).await;
    let _guard = lock.lock().await;
    // Skip the `git fetch` when the mirror was refreshed within the TTL — by a
    // recent resolve, or by the sync we just waited on while holding the lock.
    if !mirror_is_fresh(&state, &fresh_key) {
        if let Err(e) =
            ensure_mirror(&mirror_dir, &owner, &repo, &branch, github_token.as_ref()).await
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

    // Load the stored info for this branch, if any.
    let fallback = state
        .ref_store
        .load_branch(&owner, &repo, &branch)
        .await
        .ok()
        .flatten();

    let branch2 = branch.clone();
    let mirror_dir2 = mirror_dir.clone();
    match tokio::task::spawn_blocking(move || git::resolve_commit(&mirror_dir2, &branch2)).await {
        Ok(Ok(commit)) => {
            let default_branch =
                git::default_branch(&mirror_dir).unwrap_or_else(|_| "HEAD".to_string());
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
                build_status: None,
                synced_at: None,
            });
            let resp = ref_response(
                owner,
                repo,
                branch,
                &info,
                &state.storage,
                &params.clonepack,
            );
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

/// TTL for signed chunk URLs returned in ref responses. 20 minutes gives slow
/// clones and large archives enough time to finish without keeping the window
/// open too long.
const REF_SIGNED_URL_TTL: Duration = Duration::from_secs(1200);

fn ref_response(
    owner: String,
    repo: String,
    branch: String,
    info: &RefInfo,
    storage: &crate::storage::StorageRef,
    clonepack_kind: &str,
) -> RefResponse {
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

    let clonepack_manifest_url = signed_url(storage, &clonepack_manifest);
    let metadata_chunk_url = signed_url(storage, &metadata_chunk);
    let archive_chunk_urls = if info.archive_chunks.is_empty() {
        None
    } else {
        let urls: Vec<Option<String>> = info
            .archive_chunks
            .iter()
            .map(|h| signed_url(storage, h))
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
            .map(|h| signed_url(storage, h))
            .collect();
        if urls.iter().all(|u| u.is_none()) {
            None
        } else {
            Some(urls)
        }
    };
    let head_blobs_idx_url = signed_url(storage, &info.head_blobs_idx);

    // Sign each editable pack + idx so the client fetches them straight from
    // object storage. `None` entries (e.g. local backend) fall back to the
    // gateway. Ordered to match the manifest's `packs` list.
    let (pack_chunk_urls, pack_idx_urls) = if info.packs.is_empty() {
        (None, None)
    } else {
        let packs: Vec<Option<String>> = info
            .packs
            .iter()
            .map(|p| signed_url(storage, &p.pack))
            .collect();
        let idxs: Vec<Option<String>> = info
            .packs
            .iter()
            .map(|p| signed_url(storage, &p.idx))
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
    let midx_url = signed_url(storage, &artifacts.midx);
    // Sign the idx bundle so the client fetches all idx in one GET.
    let idx_bundle_url = signed_url(storage, &artifacts.idx_bundle);

    // The served commit is the selected variant's commit — which may differ from
    // RefInfo.commit during two-phase publish (depth=0 serves the previous commit
    // until the new full history is built). The client writes HEAD to this, so it
    // must match the installed objects.
    let served_commit = if artifacts.commit.is_empty() {
        info.commit.clone()
    } else {
        artifacts.commit.clone()
    };

    RefResponse {
        owner,
        repo,
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
    }
}

fn signed_url(storage: &crate::storage::StorageRef, hash: &str) -> Option<String> {
    if hash.is_empty() {
        return None;
    }
    storage.signed_url(hash, REF_SIGNED_URL_TTL)
}

async fn repo_status(
    Path((owner, repo)): Path<(String, String)>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    match build_repo_status(&state, &owner, &repo).await {
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

async fn build_repo_status(
    state: &ServerState,
    owner: &str,
    repo: &str,
) -> Result<RepoStatusResponse> {
    let branches = state.ref_store.list_branches(owner, repo).await?;
    let mut refs = Vec::new();
    let mut unique_chunks: HashMap<String, u64> = HashMap::new();

    for branch in branches {
        let Some(info) = state.ref_store.load_branch(owner, repo, &branch).await? else {
            continue;
        };
        let manifest_hash = if info.full_clonepack.manifest.is_empty() {
            info.clonepack_manifest.clone()
        } else {
            info.full_clonepack.manifest.clone()
        };
        if manifest_hash.is_empty() {
            continue;
        }

        let manifest_bytes = state.cas.get(&manifest_hash)?;
        let manifest = ClonepackManifest::decode(manifest_bytes.as_slice())
            .context("decode clonepack manifest for status")?;

        let mut ref_bytes = 0u64;
        if let Some(meta) = manifest.metadata_chunk {
            ref_bytes += meta.len;
            unique_chunks.insert(hash_to_hex(&meta.hash), meta.len);
        }
        for chunk in manifest.archive_chunks {
            ref_bytes += chunk.len;
            unique_chunks.insert(hash_to_hex(&chunk.hash), chunk.len);
        }

        let built_at = info.synced_at.and_then(|secs| {
            chrono::DateTime::from_timestamp(secs as i64, 0).map(|dt| dt.to_rfc3339())
        });

        refs.push(BranchStatusEntry {
            branch,
            commit: info.commit,
            manifest: manifest_hash,
            bytes: ref_bytes,
            built_at,
        });
    }

    refs.sort_by(|a, b| a.branch.cmp(&b.branch));
    let total_bytes = unique_chunks.values().sum();

    Ok(RepoStatusResponse {
        owner: owner.to_string(),
        repo: repo.to_string(),
        refs,
        total_bytes,
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

async fn sync_repo(
    Path((owner, repo)): Path<(String, String)>,
    Query(params): Query<SyncRequest>,
    headers: HeaderMap,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&params.branch))
    {
        return resp;
    }
    let start = Instant::now();
    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
    let branch = params.branch;
    let github_token = headers
        .get("X-GitHub-Token")
        .and_then(|v| v.to_str().ok())
        .map(|t| secrecy::SecretString::new(t.into()))
        .or_else(|| {
            state
                .github_token
                .clone()
                .map(|s| secrecy::SecretString::new(s.into()))
        });
    let lock = repo_lock(&state.sync_locks, &owner, &repo).await;
    let _guard = lock.lock().await;
    match do_sync(
        &state.cas,
        &mirror_dir,
        &owner,
        &repo,
        &branch,
        &state.ref_store,
        false,
        &state.storage,
        &state.retention,
        github_token.as_ref(),
    )
    .await
    {
        Ok(info) => {
            state.metrics.record_sync(start.elapsed());
            // The mirror was just fetched; let the immediately-following resolve
            // skip its own fetch.
            stamp_mirror_fresh(&state, &format!("{}/{}/{}", owner, repo, branch));
            let resp = ref_response(owner, repo, branch.clone(), &info, &state.storage, "full");
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

    let job = BuildJob {
        owner: body.owner,
        repo: body.repo,
        github_token: state
            .github_token
            .clone()
            .map(|s| secrecy::SecretString::new(s.into())),
    };

    // Increment counters before enqueueing so a fast worker cannot decrement
    // before we account for the job.
    state.metrics.record_build_queued();
    let queue_depth = state.build_queue_depth.fetch_add(1, Ordering::Relaxed) + 1;

    if let Err(e) = state.build_queue.try_send(job) {
        // The job never entered the queue; roll back the increments.
        state.metrics.record_build_rejected();
        state.build_queue_depth.fetch_sub(1, Ordering::Relaxed);
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("build queue full: {}", e),
            }),
        )
            .into_response();
    }
    state.metrics.record_build_accepted();

    (
        StatusCode::ACCEPTED,
        Json(BuildResponse {
            status: "queued".to_string(),
            queue_depth,
        }),
    )
        .into_response()
}

async fn cat_file(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<CatRequest>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&query.branch))
    {
        return resp;
    }
    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
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

async fn file_sizes(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<SizesRequest>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&query.branch))
    {
        return resp;
    }
    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
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

async fn create_snapshot(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<SnapshotRequest>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&query.branch))
    {
        return resp;
    }
    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
    let github_token = state
        .github_token
        .clone()
        .map(|s| secrecy::SecretString::new(s.into()));
    let branch = query.branch.clone();

    let lock = repo_lock(&state.sync_locks, &owner, &repo).await;
    let _guard = lock.lock().await;
    let info = match do_sync(
        &state.cas,
        &mirror_dir,
        &owner,
        &repo,
        &branch,
        &state.ref_store,
        false,
        &state.storage,
        &state.retention,
        github_token.as_ref(),
    )
    .await
    {
        Ok(info) => {
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
        Ok(Ok(snap)) => (
            StatusCode::OK,
            Json(SnapshotResponse {
                owner,
                repo,
                branch: query.branch,
                commit: snap.commit,
                snapshot_hash: snap.hash,
                size: snap.size,
                hot_files: snap.hot_files,
            }),
        )
            .into_response(),
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

async fn get_hotfiles(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<HotfilesRequest>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
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

async fn batch_files(
    Path((owner, repo)): Path<(String, String)>,
    State(state): State<ServerState>,
    Json(body): Json<BatchRequest>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&body.branch))
    {
        return resp;
    }
    if let Some(commit) = &body.commit
        && let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(commit))
    {
        return resp;
    }
    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
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
/// history in the background). Off by default.
fn two_phase_enabled() -> bool {
    std::env::var("RIPCLONE_TWO_PHASE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn env_bytes(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
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
}

/// Build a multi-pack-index over `packs` from local CAS. Free-function form.
fn assemble_midx(
    cas: &Cas,
    packs: &[(String, u64, String, u64)],
) -> Result<(Option<ChunkRef>, String)> {
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
            tokio::task::spawn_blocking(move || {
                let data = cas
                    .get(&hash)
                    .with_context(|| format!("read artifact {} for upload", hash))?;
                storage
                    .put(&hash, &data)
                    .with_context(|| format!("upload artifact {}", hash))
            })
            .await
            .context("upload task")?
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

async fn do_sync(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    owner: &str,
    repo: &str,
    branch: &str,
    ref_store: &Arc<dyn RefStore>,
    build_full_pack: bool,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    github_token: Option<&secrecy::SecretString>,
) -> Result<RefInfo> {
    info!("syncing {}/{}@{}", owner, repo, branch);

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
    let owner_sync = owner.to_string();
    let repo_sync = repo.to_string();
    let branch_sync = branch.to_string();
    let github_token = github_token.map(|s| s.expose_secret().to_string());
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror(
            &mirror_dir_sync,
            &owner_sync,
            &repo_sync,
            &branch_sync,
            github_token.as_deref(),
        )
    })
    .await
    .context("sync task")??;
    info!("sync phase: mirror fetch {:?}", t.elapsed());
    t = Instant::now();

    let commit = git::resolve_commit(&mirror_dir, branch)?;
    let parent = git::parent_commit(&mirror_dir, &commit).ok().flatten();
    let default_branch = git::default_branch(&mirror_dir).unwrap_or_else(|_| "HEAD".to_string());

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
            owner,
            repo,
            branch,
            &commit,
            parent,
            &default_branch,
            ref_store,
            storage,
            retention,
            t_total,
        )
        .await;
    }

    // Full-history skeleton pack + idx.
    let mirror_dir2 = mirror_dir.clone();
    let cas2 = cas.clone();
    let commit2 = commit.clone();
    let skeleton_handle = tokio::task::spawn_blocking(move || {
        let s = Instant::now();
        let builder = PackBuilder::new(&mirror_dir2, &cas2);
        let r = builder.build_skeleton_pack(&commit2);
        info!("build task: full skeleton {:?}", s.elapsed());
        r
    });

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

    // LSM incremental history build (opt-in via RIPCLONE_LSM). When on, only the
    // tail past the last sealed level is built; prior levels are reused by hash
    // from object storage. See ROADMAP "LSM incremental history build".
    let lsm = std::env::var("RIPCLONE_LSM")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let seal_threshold_raw: u64 = std::env::var("RIPCLONE_LSM_SEAL_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024 * 1024 * 1024);
    let prev_levels: Vec<crate::HistoryLevel> = if lsm {
        ref_store
            .load_branch(owner, repo, branch)
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

    let (skeleton_pack, skeleton_idx) =
        skeleton_handle.await.context("full skeleton pack task")??;
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
                // Manifest history = prior sealed levels (by hash) + the new tail.
                let mut history_packs: Vec<(String, u64, String, u64)> = prev_levels
                    .iter()
                    .flat_map(|l| l.packs.iter().map(sized_to_tuple))
                    .collect();
                history_packs.extend(inc.tail_packs.iter().cloned());

                // Newly built this sync = HEAD closure + tail (the only packs to
                // upload/evict; prior levels are already in object storage).
                let mut new_tuples = inc.head_packs.clone();
                new_tuples.extend(inc.tail_packs.iter().cloned());

                // Seal the tail into a new immutable level once it is large enough
                // and actually advances past the last sealed tip.
                let advances = sealed_tip.as_deref() != Some(commit.as_str());
                let seal = advances
                    && !inc.tail_packs.is_empty()
                    && inc.tail_raw_bytes >= seal_threshold_raw;
                let mut new_levels = prev_levels.clone();
                if seal {
                    new_levels.push(crate::HistoryLevel {
                        tip_commit: commit.clone(),
                        packs: inc.tail_packs.iter().map(tuple_to_sized).collect(),
                    });
                    info!(
                        "LSM: sealed level {} at {} ({} packs, {} MiB raw tail)",
                        new_levels.len() - 1,
                        &commit[..7.min(commit.len())],
                        inc.tail_packs.len(),
                        inc.tail_raw_bytes / (1024 * 1024)
                    );
                }
                (inc.head_packs, history_packs, new_tuples, new_levels, false)
            }
        };

    // Prebuilt indexes for both skeletons.
    let mirror_dir5 = mirror_dir.clone();
    let cas5 = cas.clone();
    let commit5 = commit.clone();
    let skeleton_pack_for_index = skeleton_pack.clone();
    let prebuilt_index_handle = tokio::task::spawn_blocking(move || {
        let builder = PackBuilder::new(&mirror_dir5, &cas5);
        builder.build_prebuilt_index(&commit5, &skeleton_pack_for_index)
    });
    let mirror_dir5s = mirror_dir.clone();
    let cas5s = cas.clone();
    let commit5s = commit.clone();
    let shallow_skeleton_pack_for_index = shallow_skeleton_pack.clone();
    let shallow_prebuilt_index_handle = tokio::task::spawn_blocking(move || {
        let builder = PackBuilder::new(&mirror_dir5s, &cas5s);
        builder.build_prebuilt_index(&commit5s, &shallow_skeleton_pack_for_index)
    });
    let prebuilt_index = prebuilt_index_handle
        .await
        .context("full prebuilt index task")??;
    let shallow_prebuilt_index = shallow_prebuilt_index_handle
        .await
        .context("shallow prebuilt index task")??;
    info!("sync phase: prebuilt indexes {:?}", t.elapsed());
    t = Instant::now();

    // Assemble the full-history metadata chunk with the small .git artifacts and
    // the frame/file tables. The head-blobs pack is kept as its own artifact
    // because it is ~65 MB of file contents and is not needed for archive
    // extraction.
    metadata_chunk.skeleton_pack = cas.get(&skeleton_pack)?;
    metadata_chunk.skeleton_idx = cas.get(&skeleton_idx)?;
    metadata_chunk.prebuilt_index = cas.get(&prebuilt_index)?;
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
        skeleton_pack: skeleton_pack.clone(),
        skeleton_idx: skeleton_idx.clone(),
        head_blobs_pack: String::new(),
        head_blobs_idx: String::new(),
        head_blobs_chunks: Vec::new(),
        packs: pack_artifacts.clone(),
        prebuilt_index: prebuilt_index.clone(),
        archive: archive_chunk_hashes.first().cloned().unwrap_or_default(),
        manifest: metadata_hash.clone(),
        full_pack,
        clonepack_manifest: full_clonepack_hash.clone(),
        metadata_chunk: metadata_hash.clone(),
        archive_chunks: archive_chunk_hashes.clone(),
        full_clonepack: crate::ClonepackArtifacts {
            manifest: full_clonepack_hash.clone(),
            metadata_chunk: metadata_hash.clone(),
            skeleton_pack: skeleton_pack.clone(),
            skeleton_idx: skeleton_idx.clone(),
            prebuilt_index: prebuilt_index.clone(),
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
    let upload_conc: usize = std::env::var("RIPCLONE_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16)
        .max(1);
    futures::stream::iter(upload_hashes.into_iter().map(|hash| {
        let cas = cas.clone();
        let storage = storage.clone();
        async move {
            tokio::task::spawn_blocking(move || {
                let data = cas
                    .get(&hash)
                    .with_context(|| format!("read artifact {} from CAS for upload", hash))?;
                storage
                    .put(&hash, &data)
                    .with_context(|| format!("upload artifact {} to storage", hash))
            })
            .await
            .context("upload task")?
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
        .save_branch(owner, repo, branch, &info)
        .await
        .with_context(|| format!("persist ref store for {owner}/{repo}@{branch}"))?;

    info!(
        "synced {}/{} at {} (total build {:?})",
        owner,
        repo,
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
#[allow(clippy::too_many_arguments)]
async fn build_and_publish_two_phase(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    owner: &str,
    repo: &str,
    branch: &str,
    commit: &str,
    parent: Option<String>,
    default_branch: &str,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    t_total: Instant,
) -> Result<RefInfo> {
    let head_target = env_bytes("RIPCLONE_PACK_BYTES", 12 * 1024 * 1024);
    let history_target = env_bytes("RIPCLONE_HISTORY_PACK_BYTES", 512 * 1024 * 1024);
    let upload_conc = env_bytes("RIPCLONE_UPLOAD_CONCURRENCY", 16) as usize;

    // ---- PHASE 1: HEAD closure + archive + shallow skeleton -> publish depth=1 ----
    let mut t = Instant::now();
    let (md1, c1, cm1) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let shallow_skeleton_handle = tokio::task::spawn_blocking(move || {
        PackBuilder::new(&md1, &c1).build_shallow_skeleton_pack(&cm1)
    });
    let (md2, c2, cm2) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let head_handle = tokio::task::spawn_blocking(move || {
        PackBuilder::new(&md2, &c2).build_head_packs(&cm2, head_target)
    });
    let (md3, c3, cm3) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let archive_handle = tokio::task::spawn_blocking(move || {
        ArchiveBuilder::new(&md3).build_into_cas(&cm3, &c3, 6, None)
    });
    let (shallow_skeleton_pack, shallow_skeleton_idx) = shallow_skeleton_handle
        .await
        .context("shallow skeleton")??;
    let head_packs = head_handle.await.context("head packs")??;
    let (archive_chunk_hashes, metadata_base) = archive_handle.await.context("archive")??;
    info!(
        "two-phase p1: head+shallow-skeleton+archive {:?}",
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

    let archive_chunks = archive_chunk_refs(&archive_chunk_hashes, &metadata_base)?;
    let head_tagged: Vec<(&(String, u64, String, u64), bool)> =
        head_packs.iter().map(|p| (p, false)).collect();
    let (head_entries, head_idx_bundle_ref, head_idx_bundle_hash) =
        assemble_variant(cas, storage, &head_tagged)?;
    let (head_midx_ref, head_midx_hash) = assemble_midx(cas, &head_packs)?;

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
    // working (one commit stale) until phase 2 publishes the new full.
    let prev = ref_store
        .load_branch(owner, repo, branch)
        .await
        .ok()
        .flatten();
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
        archive: archive_chunk_hashes.first().cloned().unwrap_or_default(),
        manifest: shallow_metadata_hash.clone(),
        full_pack: carried_full_pack,
        clonepack_manifest: carried_full_manifest,
        metadata_chunk: shallow_metadata_hash.clone(),
        archive_chunks: archive_chunk_hashes.clone(),
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
        build_status: Some("full history building".to_string()),
        synced_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs()),
    };

    // Upload phase-1 artifacts (head packs+idx, shallow skeleton/index/metadata,
    // archive chunks, head idx-bundle + midx, shallow manifest).
    let mut p1: Vec<String> = vec![
        shallow_skeleton_pack.clone(),
        shallow_skeleton_idx.clone(),
        shallow_prebuilt_index.clone(),
        shallow_metadata_hash.clone(),
        shallow_clonepack_hash.clone(),
        head_idx_bundle_hash.clone(),
        head_midx_hash.clone(),
    ];
    p1.extend(archive_chunk_hashes.iter().cloned());
    for (p, _, i, _) in &head_packs {
        p1.push(p.clone());
        p1.push(i.clone());
    }
    p1.retain(|h| !h.is_empty());
    let head_idx_keep: std::collections::HashSet<String> =
        head_packs.iter().map(|(_, _, ih, _)| ih.clone()).collect();
    upload_artifacts(cas, storage, p1.clone(), upload_conc).await?;
    settle_storage(cas, storage, retention, p1, head_idx_keep).await;

    ref_store
        .save_branch(owner, repo, branch, &info)
        .await
        .with_context(|| format!("persist depth=1 ref for {owner}/{repo}@{branch}"))?;
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
    let (owner2, repo2, branch2) = (owner.to_string(), repo.to_string(), branch.to_string());
    let commit2 = commit.to_string();
    let parent2 = parent.clone();
    let default_branch2 = default_branch.to_string();
    tokio::spawn(async move {
        let started = Instant::now();
        let res = build_full_in_background(
            &cas2,
            &mirror2,
            &owner2,
            &repo2,
            &branch2,
            &commit2,
            parent2,
            &default_branch2,
            &ref_store2,
            &storage2,
            &retention2,
            head_packs,
            archive_chunk_hashes,
            metadata_base,
            head_idx_bundle_hash,
            head_midx_hash,
            history_target,
            upload_conc,
        )
        .await;
        match res {
            Ok(()) => info!(
                "two-phase p2: published full history for {} in {:?}",
                &commit2[..7.min(commit2.len())],
                started.elapsed()
            ),
            Err(e) => error!("two-phase p2: full history build failed for {owner2}/{repo2}: {e:#}"),
        }
    });

    Ok(info)
}

/// Phase 2 of two-phase publish: build the full-history artifacts and upgrade
/// the ref's full clonepack. The depth=1 clonepack is already live.
#[allow(clippy::too_many_arguments)]
async fn build_full_in_background(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    owner: &str,
    repo: &str,
    branch: &str,
    commit: &str,
    parent: Option<String>,
    default_branch: &str,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    head_packs: Vec<(String, u64, String, u64)>,
    archive_chunk_hashes: Vec<String>,
    metadata_base: crate::clonepack::MetadataChunk,
    _head_idx_bundle_hash: String,
    _head_midx_hash: String,
    history_target: u64,
    upload_conc: usize,
) -> Result<()> {
    // Full skeleton + history packs (the expensive part), concurrently.
    let (md1, c1, cm1) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let skeleton_handle =
        tokio::task::spawn_blocking(move || PackBuilder::new(&md1, &c1).build_skeleton_pack(&cm1));
    let (md2, c2, cm2) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let history_handle = tokio::task::spawn_blocking(move || {
        PackBuilder::new(&md2, &c2).build_history_packs(&cm2, history_target)
    });
    let (skeleton_pack, skeleton_idx) = skeleton_handle.await.context("full skeleton")??;
    let history_packs = history_handle.await.context("history packs")??;

    let (md3, c3, cm3, skp) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        commit.to_string(),
        skeleton_pack.clone(),
    );
    let prebuilt_index = tokio::task::spawn_blocking(move || {
        PackBuilder::new(&md3, &c3).build_prebuilt_index(&cm3, &skp)
    })
    .await
    .context("full prebuilt index")??;

    let mut full_meta = metadata_base;
    full_meta.skeleton_pack = cas.get(&skeleton_pack)?;
    full_meta.skeleton_idx = cas.get(&skeleton_idx)?;
    full_meta.prebuilt_index = cas.get(&prebuilt_index)?;
    let full_meta_data = full_meta.encode_to_vec();
    let metadata_hash = cas.put(&full_meta_data)?;

    let archive_chunks = archive_chunk_refs(&archive_chunk_hashes, &full_meta)?;
    // Full variant entries + idx-bundle (head idx is kept local, history is fresh).
    let full_tagged: Vec<(&(String, u64, String, u64), bool)> = head_packs
        .iter()
        .map(|p| (p, false))
        .chain(history_packs.iter().map(|p| (p, true)))
        .collect();
    let (full_entries, full_idx_bundle_ref, full_idx_bundle_hash) =
        assemble_variant(cas, storage, &full_tagged)?;
    // Full MIDX is omitted: head packs were evicted after phase 1, so it can't be
    // built without re-fetching them. The client builds the full MIDX itself.
    let full_manifest = make_manifest(
        commit,
        &parent,
        default_branch,
        &archive_chunks,
        &metadata_hash,
        full_meta_data.len() as u64,
        full_entries,
        None,
        full_idx_bundle_ref,
    )?;
    let full_clonepack_hash = cas.put(&full_manifest.encode_to_vec())?;

    // Upload phase-2 artifacts (history packs+idx, full skeleton/index/metadata,
    // full idx-bundle, full manifest). Head packs are already in storage.
    let mut p2: Vec<String> = vec![
        skeleton_pack.clone(),
        skeleton_idx.clone(),
        prebuilt_index.clone(),
        metadata_hash.clone(),
        full_clonepack_hash.clone(),
        full_idx_bundle_hash.clone(),
    ];
    for (p, _, i, _) in &history_packs {
        p2.push(p.clone());
        p2.push(i.clone());
    }
    p2.retain(|h| !h.is_empty());
    let hist_idx_keep: std::collections::HashSet<String> = history_packs
        .iter()
        .map(|(_, _, ih, _)| ih.clone())
        .collect();
    upload_artifacts(cas, storage, p2.clone(), upload_conc).await?;

    // Upgrade the ref's full clonepack (preserve the live depth=1 clonepack).
    let mut info = ref_store
        .load_branch(owner, repo, branch)
        .await?
        .ok_or_else(|| anyhow::anyhow!("ref vanished before phase 2"))?;
    let mut all_packs = head_packs.clone();
    all_packs.extend(history_packs.iter().cloned());
    info.packs = pack_artifacts_of(&all_packs);
    info.skeleton_pack = skeleton_pack.clone();
    info.skeleton_idx = skeleton_idx.clone();
    info.prebuilt_index = prebuilt_index.clone();
    info.metadata_chunk = metadata_hash.clone();
    info.manifest = metadata_hash.clone();
    info.clonepack_manifest = full_clonepack_hash.clone();
    info.full_clonepack = crate::ClonepackArtifacts {
        manifest: full_clonepack_hash.clone(),
        metadata_chunk: metadata_hash.clone(),
        skeleton_pack: skeleton_pack.clone(),
        skeleton_idx: skeleton_idx.clone(),
        prebuilt_index: prebuilt_index.clone(),
        midx: String::new(),
        idx_bundle: full_idx_bundle_hash.clone(),
        commit: commit.to_string(),
    };
    info.build_status = None;
    ref_store
        .save_branch(owner, repo, branch, &info)
        .await
        .with_context(|| format!("persist full ref for {owner}/{repo}@{branch}"))?;

    settle_storage(cas, storage, retention, p2, hist_idx_keep).await;
    Ok(())
}

fn spawn_build_worker(state: ServerState) -> tokio::sync::mpsc::Sender<BuildJob> {
    const QUEUE_SIZE: usize = 1024;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<BuildJob>(QUEUE_SIZE);

    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let owner = job.owner.clone();
            let repo = job.repo.clone();
            let state = state.clone();

            // Mark as building in the shared ref store.
            let _ = update_build_status(&state, &owner, &repo, "building").await;

            let start = std::time::Instant::now();
            let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
            let lock = repo_lock(&state.sync_locks, &owner, &repo).await;
            let _guard = lock.lock().await;
            let result = do_sync(
                &state.cas,
                &mirror_dir,
                &owner,
                &repo,
                "HEAD",
                &state.ref_store,
                false,
                &state.storage,
                &state.retention,
                job.github_token.as_ref(),
            )
            .await;
            drop(_guard);

            state.build_queue_depth.fetch_sub(1, Ordering::Relaxed);
            match result {
                Ok(_) => {
                    state.metrics.record_build_completed(start.elapsed());
                    let _ = update_build_status(&state, &owner, &repo, "done").await;
                    info!("background build completed for {owner}/{repo}");
                }
                Err(e) => {
                    state.metrics.record_build_failed();
                    let _ =
                        update_build_status(&state, &owner, &repo, &format!("failed: {e}")).await;
                    warn!("background build failed for {owner}/{repo}: {e}");
                }
            }
        }
    });

    tx
}

async fn update_build_status(
    state: &ServerState,
    owner: &str,
    repo: &str,
    status: &str,
) -> Result<()> {
    let mut info = match state.ref_store.load(owner, repo).await? {
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
            build_status: None,
            synced_at: None,
        },
    };
    info.build_status = Some(status.to_string());
    state.ref_store.save(owner, repo, &info).await?;
    Ok(())
}

pub async fn run_server(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    host: &str,
    port: u16,
) -> Result<()> {
    std::fs::create_dir_all(cas_dir)?;
    std::fs::create_dir_all(repo_root)?;

    let token_hash = env::var("RIPCLONE_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .map(|t| format!("{:x}", Sha256::digest(t.as_bytes())));
    if token_hash.is_none() {
        anyhow::bail!("RIPCLONE_TOKEN is not set. Refusing to start an unauthenticated server.");
    }
    info!("RIPCLONE_TOKEN configured; auth middleware enabled");

    let github_token = env::var("RIPCLONE_GITHUB_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    if github_token.is_some() {
        info!("RIPCLONE_GITHUB_TOKEN configured; private repo sync enabled");
    }

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

    let cas = Cas::new(cas_dir)?;
    let s3_storage = S3Storage::from_env().context("initialize S3 storage from environment")?;
    let (storage, ref_store): (StorageRef, Arc<dyn RefStore>) = if let Some(s3) = s3_storage {
        info!(
            "using S3-compatible storage with local cache at {}",
            cas_dir.display()
        );
        let s3 = Arc::new(s3);
        let store = CachingRefStore::new(S3RefStore::new(s3.clone()));
        (s3 as StorageRef, Arc::new(store))
    } else {
        info!("using local storage at {}", cas_dir.display());
        let store = CachingRefStore::new(FileRefStore::new(repo_root));
        (local(cas_dir)?, Arc::new(store))
    };

    let metrics = Metrics::new();
    let retention = Arc::new(Retention::with_config_and_storage(
        cas.clone(),
        metrics.clone(),
        Retention::parse_age(),
        Retention::parse_size(),
        Some(storage.clone()),
    )?);
    let retention_interval: Duration = env::var("RIPCLONE_RETENTION_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(300));
    Retention::clone(&retention).spawn(retention_interval);

    let refs_path = repo_root.join(".ripclone-refs.json");
    if let Err(e) = migrate_legacy_refs(ref_store.as_ref(), &refs_path).await {
        warn!("failed to migrate legacy refs: {}", e);
    }

    let oidc_audience = env::var("RIPCLONE_OIDC_AUDIENCE")
        .ok()
        .filter(|t| !t.is_empty());
    let oidc_verifier = oidc_audience.map(OidcVerifier::new);
    if oidc_verifier.is_some() {
        info!("OIDC verification enabled for audience configured via RIPCLONE_OIDC_AUDIENCE");
    }

    let mut state = ServerState {
        cas,
        storage,
        repo_root: repo_root.to_path_buf(),
        ref_store,
        token_hash,
        github_token,
        metrics,
        rate_limiter,
        retention,
        build_queue: tokio::sync::mpsc::channel(1).0, // placeholder
        build_queue_depth: Arc::new(AtomicUsize::new(0)),
        oidc_verifier,
        sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        mirror_freshness: Arc::new(std::sync::Mutex::new(HashMap::new())),
        mirror_fresh_ttl: Duration::from_secs(
            env::var("RIPCLONE_MIRROR_FRESH_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(60),
        ),
    };
    let build_queue = spawn_build_worker(state.clone());
    state.build_queue = build_queue;

    let state = state;

    let app = build_app(state);
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

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
        let ref_store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&repo_root));
        let token_hash = format!("{:x}", Sha256::digest("secret"));
        let metrics = Metrics::new();
        let retention = Arc::new(Retention::new(cas.clone(), metrics.clone()).unwrap());
        let (build_queue, _build_rx) = tokio::sync::mpsc::channel::<BuildJob>(16);
        ServerState {
            cas,
            storage,
            repo_root,
            ref_store,
            token_hash: Some(token_hash),
            github_token: None,
            metrics,
            rate_limiter: RateLimiter::new(100, 100.0),
            retention,
            build_queue,
            build_queue_depth: Arc::new(AtomicUsize::new(0)),
            oidc_verifier: None,
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            mirror_freshness: Arc::new(std::sync::Mutex::new(HashMap::new())),
            mirror_fresh_ttl: Duration::from_secs(60),
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

    #[tokio::test]
    async fn run_server_refuses_to_start_without_token() {
        let tmp = tempfile::tempdir().unwrap();
        let result = run_server(
            tmp.path().join("cas").as_path(),
            tmp.path().join("repos").as_path(),
            "127.0.0.1",
            0,
        )
        .await;
        assert!(
            result.is_err(),
            "server must refuse to start without RIPCLONE_TOKEN"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("RIPCLONE_TOKEN"),
            "error should mention missing token: {}",
            err
        );
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
            build_status: None,
            synced_at: None,
        };
        let resp = ref_response(
            "o".to_string(),
            "r".to_string(),
            "main".to_string(),
            &info,
            &storage,
            "full",
        );
        assert!(resp.clonepack_manifest_url.is_none());
        assert!(resp.metadata_chunk_url.is_none());
        assert!(resp.archive_chunk_urls.is_none());
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

    #[tokio::test]
    async fn repo_status_returns_empty_for_cold_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/acme/secret/status"))
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
            build_status: None,
            synced_at: Some(1_718_812_800),
        };
        state
            .ref_store
            .save_branch("acme", "secret", "main", &info)
            .await
            .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/acme/secret/status"))
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
        assert_eq!(branch.bytes, 300);
        assert!(branch.built_at.is_some());
        assert_eq!(status.total_bytes, 300);
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
            build_status: None,
            synced_at: None,
        };
        state
            .ref_store
            .save_branch("acme", "secret", "main", &info)
            .await
            .unwrap();
        state
            .ref_store
            .save_branch("acme", "secret", "develop", &info)
            .await
            .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/acme/secret/status"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.refs.len(), 2);
        assert_eq!(status.total_bytes, 300);
    }

    #[tokio::test]
    async fn sync_rejects_invalid_branch_name() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app
            .oneshot(test_request(
                "POST",
                "/v1/repos/acme/secret/sync?branch=../evil",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
