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
use tracing::{info, warn};

#[derive(Clone)]
pub struct ServerState {
    pub cas: Cas,
    pub storage: StorageRef,
    pub repo_root: PathBuf,
    pub ref_store: Arc<dyn RefStore>,
    pub default_depth: usize,
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
    pub depth: Option<usize>,
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

pub fn build_app(state: ServerState) -> Router {
    let protected = Router::new()
        .route("/v1/repos/{owner}/{repo}/refs/{branch}", get(get_ref))
        .route("/v1/repos/{owner}/{repo}/sync", post(sync_repo))
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
    if let Some(credentials) = header.strip_prefix("Basic ") {
        if let Ok(decoded) =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, credentials)
        {
            if let Ok(decoded) = String::from_utf8(decoded) {
                // Accept "<username>:<password>"; compare the password to the
                // expected hash so vanilla git can use
                // http://user:<hash>@host/... URLs.
                if let Some((_, password)) = decoded.split_once(':') {
                    return constant_time_eq_str(password, expected);
                }
            }
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
    if let Err(e) = ensure_mirror(
        &mirror_dir,
        &owner,
        &repo,
        "HEAD",
        state.default_depth,
        github_token.as_ref(),
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
    if let Err(e) = ensure_mirror(
        &mirror_dir,
        &owner,
        &repo,
        "HEAD",
        state.default_depth,
        github_token.as_ref(),
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
    depth: usize,
    github_token: Option<&secrecy::SecretString>,
) -> Result<()> {
    let mirror_dir = mirror_dir.to_path_buf();
    let owner = owner.to_string();
    let repo = repo.to_string();
    let branch = branch.to_string();
    let github_token = github_token.map(|s| s.expose_secret().to_string());
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror(
            &mirror_dir,
            &owner,
            &repo,
            &branch,
            depth,
            github_token.as_deref(),
        )
    })
    .await
    .context("ensure mirror task")?
}

async fn get_ref(
    Path((owner, repo, branch)): Path<(String, String, String)>,
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
    // bare mirror directory.
    let lock = repo_lock(&state.sync_locks, &owner, &repo).await;
    let _guard = lock.lock().await;
    if let Err(e) = ensure_mirror(
        &mirror_dir,
        &owner,
        &repo,
        &branch,
        state.default_depth,
        github_token.as_ref(),
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

    // Load the stored HEAD info for this repo, if any.
    let fallback = state.ref_store.load(&owner, &repo).await.ok().flatten();

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
                prebuilt_index: String::new(),
                archive: String::new(),
                manifest: String::new(),
                full_pack: String::new(),
                clonepack_manifest: String::new(),
                metadata_chunk: String::new(),
                archive_chunks: Vec::new(),
                build_status: None,
                synced_at: None,
            });
            let resp = ref_response(owner, repo, branch, &info, &state.storage);
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
) -> RefResponse {
    let clonepack_manifest_url = signed_url(storage, &info.clonepack_manifest);
    let metadata_chunk_url = signed_url(storage, &info.metadata_chunk);
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

    RefResponse {
        owner,
        repo,
        branch,
        default_branch: info.default_branch.clone(),
        commit: info.commit.clone(),
        parent_commit: info.parent_commit.clone(),
        full_pack: info.full_pack.clone(),
        clonepack_manifest: info.clonepack_manifest.clone(),
        clonepack_manifest_url,
        metadata_chunk: info.metadata_chunk.clone(),
        metadata_chunk_url,
        archive_chunk_urls,
        head_blobs_chunk_urls,
        head_blobs_idx_url,
    }
}

fn signed_url(storage: &crate::storage::StorageRef, hash: &str) -> Option<String> {
    if hash.is_empty() {
        return None;
    }
    storage.signed_url(hash, REF_SIGNED_URL_TTL)
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
    let start = Instant::now();
    let mirror_dir = state.repo_root.join(format!("{}_{}.git", owner, repo));
    let depth = params.depth.unwrap_or(state.default_depth);
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
        "HEAD",
        &state.ref_store,
        depth,
        false,
        &state.storage,
        &state.retention,
        github_token.as_ref(),
    )
    .await
    {
        Ok(info) => {
            state.metrics.record_sync(start.elapsed());
            let resp = ref_response(owner, repo, "HEAD".to_string(), &info, &state.storage);
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
        state.default_depth,
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
    if let Some(commit) = &body.commit {
        if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(commit)) {
            return resp;
        }
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

async fn do_sync(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    owner: &str,
    repo: &str,
    branch: &str,
    ref_store: &Arc<dyn RefStore>,
    depth: usize,
    build_full_pack: bool,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    github_token: Option<&secrecy::SecretString>,
) -> Result<RefInfo> {
    info!("syncing {}/{}@{}", owner, repo, branch);

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
            depth,
            github_token.as_deref(),
        )
    })
    .await
    .context("sync task")??;

    let commit = git::resolve_commit(&mirror_dir, branch)?;
    let parent = git::parent_commit(&mirror_dir, &commit).ok().flatten();
    let default_branch = git::default_branch(&mirror_dir).unwrap_or_else(|_| "HEAD".to_string());

    info!("building artifacts for {}", &commit[..7]);

    // Skeleton pack + idx.
    let mirror_dir2 = mirror_dir.clone();
    let cas2 = cas.clone();
    let commit2 = commit.clone();
    let skeleton_handle = tokio::task::spawn_blocking(move || {
        let builder = PackBuilder::new(&mirror_dir2, &cas2);
        builder.build_skeleton_pack(&commit2)
    });

    // Head-blobs pack + idx.
    let mirror_dir3 = mirror_dir.clone();
    let cas3 = cas.clone();
    let commit3 = commit.clone();
    let head_blobs_handle = tokio::task::spawn_blocking(move || {
        let builder = PackBuilder::new(&mirror_dir3, &cas3);
        builder.build_head_blobs_pack(&commit3)
    });

    // Working-tree archive + manifest.
    let mirror_dir4 = mirror_dir.clone();
    let cas4 = cas.clone();
    let commit4 = commit.clone();
    let archive_handle = tokio::task::spawn_blocking(move || {
        let builder = ArchiveBuilder::new(&mirror_dir4);
        builder.build_into_cas(&commit4, &cas4, 6, None)
    });

    let (skeleton_pack, skeleton_idx) = skeleton_handle.await.context("skeleton pack task")??;
    let (head_blobs_pack, head_blobs_idx) =
        head_blobs_handle.await.context("head blobs pack task")??;
    let (archive_chunk_hashes, mut metadata_chunk) =
        archive_handle.await.context("archive task")??;

    // Prebuilt index depends on the skeleton pack being in the CAS.
    let mirror_dir5 = mirror_dir.clone();
    let cas5 = cas.clone();
    let commit5 = commit.clone();
    let skeleton_pack_for_index = skeleton_pack.clone();
    let prebuilt_index = tokio::task::spawn_blocking(move || {
        let builder = PackBuilder::new(&mirror_dir5, &cas5);
        builder.build_prebuilt_index(&commit5, &skeleton_pack_for_index)
    })
    .await
    .context("prebuilt index task")??;

    // Assemble the metadata chunk with the small .git artifacts and the
    // frame/file tables. The head-blobs pack is kept as its own artifact
    // because it is ~65 MB of file contents and is not needed for archive
    // extraction.
    metadata_chunk.skeleton_pack = cas.get(&skeleton_pack)?;
    metadata_chunk.skeleton_idx = cas.get(&skeleton_idx)?;
    metadata_chunk.prebuilt_index = cas.get(&prebuilt_index)?;
    let metadata_data = metadata_chunk.encode_to_vec();
    let metadata_hash = cas.put(&metadata_data)?;

    // Split the head-blobs pack into fixed-size chunks so the client can fetch
    // it in parallel. The idx stays small and is fetched as a single object.
    let head_blobs_pack_data = cas.get(&head_blobs_pack)?;
    let head_blobs_idx_data = cas.get(&head_blobs_idx)?;
    let head_blobs_chunk_refs = split_and_store_pack(&cas, &head_blobs_pack_data)?;

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
    let clonepack_manifest = ClonepackManifest {
        commit: commit.clone(),
        parent_commit: parent.clone(),
        default_branch: default_branch.clone(),
        metadata_chunk: Some(ChunkRef {
            hash: hash_from_hex(&metadata_hash)?,
            len: metadata_data.len() as u64,
        }),
        archive_chunks,
        head_blobs_chunks: head_blobs_chunk_refs.clone(),
        head_blobs_idx: Some(ChunkRef {
            hash: hash_from_hex(&head_blobs_idx)?,
            len: head_blobs_idx_data.len() as u64,
        }),
        ..Default::default()
    };
    let clonepack_data = clonepack_manifest.encode_to_vec();
    let clonepack_hash = cas.put(&clonepack_data)?;

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

    let head_blobs_chunk_hashes: Vec<String> = head_blobs_chunk_refs
        .iter()
        .map(|r| hash_to_hex(&r.hash))
        .collect();

    let info = RefInfo {
        commit: commit.clone(),
        parent_commit: parent.clone(),
        default_branch: default_branch.clone(),
        skeleton_pack,
        skeleton_idx,
        head_blobs_pack: String::new(),
        head_blobs_idx,
        head_blobs_chunks: head_blobs_chunk_hashes,
        prebuilt_index,
        archive: archive_chunk_hashes.first().cloned().unwrap_or_default(),
        manifest: metadata_hash.clone(),
        full_pack,
        clonepack_manifest: clonepack_hash,
        metadata_chunk: metadata_hash,
        archive_chunks: archive_chunk_hashes,
        build_status: None,
        synced_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs()),
    };

    // Push every built artifact to the configured storage backend. For a local
    // backend this is a no-op (CAS already holds it); for S3/R2/Tigris this
    // makes the artifact durable and available via signed URL.
    let mut artifact_hashes: Vec<&str> = vec![
        &info.skeleton_pack,
        &info.skeleton_idx,
        &info.head_blobs_idx,
        &info.prebuilt_index,
        &info.manifest,
        &info.clonepack_manifest,
    ];
    artifact_hashes.extend(info.head_blobs_chunks.iter().map(|s| s.as_str()));
    artifact_hashes.extend(info.archive_chunks.iter().map(|s| s.as_str()));
    for hash in artifact_hashes.iter().filter(|h| !h.is_empty()) {
        let data = cas
            .get(hash)
            .with_context(|| format!("read artifact {} from CAS for upload", hash))?;
        storage
            .put(hash, &data)
            .with_context(|| format!("upload artifact {} to storage", hash))?;
    }

    // Protect the current HEAD's artifacts from retention eviction.
    let protect_hashes: Vec<String> = artifact_hashes
        .iter()
        .filter(|h| !h.is_empty())
        .map(|h| h.to_string())
        .chain(std::iter::once(info.full_pack.clone()).filter(|h| !h.is_empty()))
        .collect();
    retention.protect(protect_hashes).await;

    let mut info = info;
    info.build_status = None;
    ref_store
        .save(owner, repo, &info)
        .await
        .with_context(|| format!("persist ref store for {owner}/{repo}"))?;

    info!("synced {}/{} at {}", owner, repo, &commit[..7]);
    Ok(info)
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
                state.default_depth,
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
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            full_pack: String::new(),
            clonepack_manifest: String::new(),
            metadata_chunk: String::new(),
            archive_chunks: Vec::new(),
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
    default_depth: usize,
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
        default_depth,
        token_hash,
        github_token,
        metrics,
        rate_limiter,
        retention,
        build_queue: tokio::sync::mpsc::channel(1).0, // placeholder
        build_queue_depth: Arc::new(AtomicUsize::new(0)),
        oidc_verifier,
        sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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
            50,
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
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            full_pack: String::new(),
            clonepack_manifest: "manifest".to_string(),
            metadata_chunk: "metadata".to_string(),
            archive_chunks: vec!["chunk1".to_string(), "chunk2".to_string()],
            build_status: None,
            synced_at: None,
        };
        let resp = ref_response(
            "o".to_string(),
            "r".to_string(),
            "main".to_string(),
            &info,
            &storage,
        );
        assert!(resp.clonepack_manifest_url.is_none());
        assert!(resp.metadata_chunk_url.is_none());
        assert!(resp.archive_chunk_urls.is_none());
    }
}
