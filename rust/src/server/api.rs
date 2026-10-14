//! The `/v1/repos` and `/v1/admin/config` routes: add and remove
//! repositories, admit builds, read commits and repository status.

use super::{ErrorResponse, ServerState, fetch_semaphore};
use crate::clonepack::{
    ClonepackManifest, collect_manifest_hashes, hash_to_hex, manifest_chunk_refs,
};
use crate::control::{BuildRow, Out};
use crate::git;
use crate::provider::{ProviderInstance, ProviderRegistry, RepoId};
use crate::validation::{self, Selector};
use crate::{ExactResultKind, RefInfo};
use anyhow::{Context, Result};
use axum::{
    Json,
    body::{Body, Bytes},
    extract::{OriginalUri, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// `POST .../builds` body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildRequest {
    /// `HEAD`, a full commit SHA, or a branch or tag name.
    pub selector: String,
    /// Put failed outputs of the commit back to pending.
    #[serde(default)]
    pub retry: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitQuery {
    #[serde(default)]
    kind: Option<ExactResultKind>,
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

/// An API error: `{error, code}` with a stable code clients can match.
#[derive(Debug)]
pub(super) struct ApiError {
    status: StatusCode,
    code: &'static str,
    pub(super) error: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, error: impl Into<String>) -> Self {
        Self {
            status,
            code,
            error: error.into(),
        }
    }

    fn invalid_request(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            error.to_string(),
        )
    }

    /// The control store could not be read or written.
    fn control(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "control_unavailable",
            format!("control store unavailable: {error:#}"),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({"error": self.error, "code": self.code})),
        )
            .into_response()
    }
}

pub(super) fn unknown_provider() -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "unknown_provider",
        "unknown provider",
    )
}

fn repo_not_added() -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "repo_not_added",
        "repo not added; run `ripclone add <repo>`",
    )
}

pub(super) async fn repo_is_added(state: &ServerState, repo_id: &RepoId) -> Result<bool, ApiError> {
    state
        .control
        .get_repo(repo_id)
        .await
        .map(|repo| repo.is_some())
        .map_err(|e| {
            state.metrics.record_error();
            ApiError::control(e)
        })
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
    /// Signed URL for each editable pack, ordered to match `manifest.packs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pack_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for the pre-built multi-pack-index (`manifest.midx`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub midx_url: Option<String>,
    /// Signed URL for the concatenated idx bundle (`manifest.idx_bundle`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idx_bundle_url: Option<String>,
    pub kind: ExactResultKind,
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

/// Resolve and validate the repository part of a `/v1/repos/...` path.
fn repo_from_path<'a>(
    state: &'a ServerState,
    repo_path: &str,
) -> Result<(RepoId, &'a ProviderInstance), ApiError> {
    let (repo_id, provider) =
        resolve_repo_id(&state.provider_registry, repo_path).ok_or_else(unknown_provider)?;
    validation::validate_repo_path(provider, &repo_id).map_err(ApiError::invalid_request)?;
    Ok((repo_id, provider))
}

fn not_found_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

pub(super) async fn dispatch_repos_get(
    Path(path): Path<String>,
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    if let Some((repo_path, commit)) = path.rsplit_once("/commits/") {
        let mut response = match repo_from_path(&state, repo_path) {
            Ok((repo_id, provider)) => {
                let provider = provider.clone();
                match Query::<CommitQuery>::try_from_uri(&uri) {
                    Ok(Query(query)) => get_commit(&state, &repo_id, &provider, commit, query.kind)
                        .await
                        .unwrap_or_else(IntoResponse::into_response),
                    Err(error) => ApiError::invalid_request(error).into_response(),
                }
            }
            Err(error) => error.into_response(),
        };
        // Every answer is a snapshot of a build that may change on the next
        // read, and ready bodies carry signed URLs.
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
        );
        return response;
    }

    if let Some(repo_path) = path.strip_suffix("/status") {
        let repo_id = match repo_from_path(&state, repo_path) {
            Ok((repo_id, _)) => repo_id,
            Err(error) => return error.into_response(),
        };
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

    not_found_response()
}

pub(super) async fn dispatch_repos_post(
    Path(path): Path<String>,
    State(state): State<ServerState>,
    body: Bytes,
) -> Response {
    if let Some(repo_path) = path.strip_suffix("/add") {
        let (repo_id, provider) = match repo_from_path(&state, repo_path) {
            Ok((repo_id, provider)) => (repo_id, provider.clone()),
            Err(error) => return error.into_response(),
        };
        return add_repo(&state, &repo_id, &provider)
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }

    if let Some(repo_path) = path.strip_suffix("/builds") {
        let (repo_id, provider) = match repo_from_path(&state, repo_path) {
            Ok((repo_id, provider)) => (repo_id, provider.clone()),
            Err(error) => return error.into_response(),
        };
        let request: BuildRequest = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(error) => {
                return ApiError::invalid_request(format!("invalid build request: {error}"))
                    .into_response();
            }
        };
        return post_build(&state, &repo_id, &provider, request)
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }

    not_found_response()
}

pub(super) async fn dispatch_repos_delete(
    Path(path): Path<String>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(repo_path) = path.strip_suffix("/add") {
        return match repo_from_path(&state, repo_path) {
            Ok((repo_id, _)) => remove_added_repo_inner(repo_id, state).await,
            Err(error) => error.into_response(),
        };
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

pub(super) async fn list_added_repos(State(state): State<ServerState>) -> Response {
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
pub(super) fn row_info(row: &BuildRow) -> RefInfo {
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

/// What a selector resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Resolved {
    pub(super) commit: String,
    /// The branch a clone checks out. `None` gives a detached checkout: a
    /// commit SHA, a tag, or an upstream without a symbolic HEAD.
    checkout_name: Option<String>,
    /// The upstream ref the commit was read from, recorded on admission.
    pub(super) ref_name: Option<String>,
}

/// Turn a selector into one commit. A full SHA is trusted as given; `HEAD`
/// and names cost one `ls-remote` under the fetch cap. Nothing is fetched.
pub(super) async fn resolve(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    selector: &Selector,
) -> Result<Resolved, ApiError> {
    let name = match selector {
        Selector::Commit(commit) => {
            return Ok(Resolved {
                commit: commit.clone(),
                checkout_name: None,
                ref_name: None,
            });
        }
        Selector::Head => "HEAD",
        Selector::Name(name) => name.as_str(),
    };
    let credential = state
        .broker
        .fetch_credential(repo_id)
        .map_err(credential_error)?;
    let tip = {
        let _permit = fetch_semaphore()
            .acquire()
            .await
            .expect("fetch semaphore never closed");
        git::ls_remote_tip_async(provider, repo_id, name, credential.as_ref()).await
    };
    let tip = match tip {
        Ok(Some(tip)) => tip,
        Ok(None) => {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "ref_not_found",
                format!("upstream ref not found: {name}"),
            ));
        }
        Err(error) => {
            state.metrics.record_error();
            // A missing repository, or one the server credential cannot see,
            // fails the same way every time.
            let permanent = error
                .downcast_ref::<git::UpstreamGitError>()
                .is_some_and(|error| !error.is_retryable());
            return Err(if permanent {
                ApiError::new(
                    StatusCode::NOT_FOUND,
                    "repo_not_found",
                    format!("upstream repository not found or not accessible: {error:#}"),
                )
            } else {
                ApiError::new(
                    StatusCode::BAD_GATEWAY,
                    "upstream_unavailable",
                    format!("upstream unavailable: {error:#}"),
                )
            });
        }
    };
    let (checkout_name, ref_name) = match selector {
        Selector::Head => {
            let branch = tip
                .default_branch
                .filter(|branch| validation::validate_checkout_name(branch).is_ok());
            let ref_name = branch.as_ref().map(|branch| format!("refs/heads/{branch}"));
            (branch, ref_name)
        }
        _ => (
            tip.ref_name.strip_prefix("refs/heads/").map(str::to_string),
            Some(tip.ref_name),
        ),
    };
    Ok(Resolved {
        commit: tip.commit,
        checkout_name,
        ref_name,
    })
}

/// `POST .../builds`: resolve the selector, admit its commit, and answer with
/// the state of each output. Never waits for a build.
async fn post_build(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    request: BuildRequest,
) -> Result<Response, ApiError> {
    let start = Instant::now();
    let selector = Selector::parse(&request.selector).map_err(|error| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_selector",
            error.to_string(),
        )
    })?;
    if !repo_is_added(state, repo_id).await? {
        return Err(repo_not_added());
    }
    let resolved = resolve(state, repo_id, provider, &selector).await?;
    admit_resolved(state, repo_id, resolved, request.retry, start).await
}

/// `POST .../add`: resolve HEAD first, so a repository the upstream does not
/// have is never added, then add it and admit HEAD.
async fn add_repo(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
) -> Result<Response, ApiError> {
    let start = Instant::now();
    let resolved = resolve(state, repo_id, provider, &Selector::Head).await?;
    state.control.add_repo(repo_id).await.map_err(|error| {
        state.metrics.record_error();
        ApiError::control(error)
    })?;
    admit_resolved(state, repo_id, resolved, true, start).await
}

async fn admit_resolved(
    state: &ServerState,
    repo_id: &RepoId,
    resolved: Resolved,
    retry: bool,
    start: Instant,
) -> Result<Response, ApiError> {
    let (row, _) = admit_build(
        state,
        repo_id,
        &resolved.commit,
        resolved.ref_name.as_deref(),
        retry,
    )
    .await
    .map_err(|error| {
        state.metrics.record_error();
        ApiError::control(error)
    })?;
    state.metrics.record_sync(start.elapsed());
    let mut body = build_summary_body(&row);
    body["checkout_name"] = serde_json::json!(resolved.checkout_name);
    Ok(Json(body).into_response())
}

fn output_state(out: Out<()>) -> serde_json::Value {
    match out {
        Out::Pending { error: None } => serde_json::json!({"state": "pending"}),
        Out::Pending { error: Some(error) } => {
            serde_json::json!({"state": "pending", "error": error})
        }
        Out::Ready(()) => serde_json::json!({"state": "ready"}),
        Out::Failed(error) => serde_json::json!({"state": "failed", "error": error}),
    }
}

/// `{commit, head, files, full}`, each output as `{state, error?}`.
fn build_summary_body(row: &BuildRow) -> serde_json::Value {
    serde_json::json!({
        "commit": row.commit,
        "head": output_state(kind_state(row, ExactResultKind::Head)),
        "files": output_state(kind_state(row, ExactResultKind::Files)),
        "full": output_state(kind_state(row, ExactResultKind::Full)),
    })
}

#[derive(Serialize)]
struct ReadyBody {
    state: &'static str,
    #[serde(flatten)]
    result: RefResponse,
}

fn ready_body(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    row: &BuildRow,
    kind: ExactResultKind,
) -> serde_json::Value {
    serde_json::json!(ReadyBody {
        state: "ready",
        result: ref_response(
            repo_id,
            provider,
            row,
            &state.storage,
            state.signed_url_ttl,
            kind
        ),
    })
}

/// `GET .../commits/{sha}[?kind=]`: a pure read of one build. With a kind,
/// one body per state of that output; without, the summary.
async fn get_commit(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    commit: &str,
    kind: Option<ExactResultKind>,
) -> Result<Response, ApiError> {
    validation::validate_object_id(commit).map_err(|error| {
        ApiError::new(StatusCode::BAD_REQUEST, "invalid_commit", error.to_string())
    })?;
    let row = state
        .control
        .status(repo_id, commit)
        .await
        .map_err(|error| {
            state.metrics.record_error();
            ApiError::control(error)
        })?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "build_not_found",
                format!("no build for commit {commit}"),
            )
        })?;
    state.metrics.record_ref_lookup();
    let Some(kind) = kind else {
        return Ok(Json(build_summary_body(&row)).into_response());
    };
    let body = match kind_state(&row, kind) {
        Out::Ready(()) => ready_body(state, repo_id, provider, &row, kind),
        Out::Failed(error) => serde_json::json!({
            "state": "failed",
            "commit": row.commit,
            "kind": kind,
            "error": error,
        }),
        Out::Pending { error } => {
            let mut body = serde_json::json!({
                "state": "pending",
                "commit": row.commit,
                "kind": kind,
                "attempts": row.attempts,
            });
            if let Some(error) = error {
                body["error"] = serde_json::json!(error);
            }
            if kind == ExactResultKind::Full
                && let Some(top_up) = top_up_plan(state, repo_id, provider, &row).await?
            {
                body["top_up"] = top_up;
            }
            body
        }
    };
    Ok(Json(body).into_response())
}

/// While Full(B) is pending, a full clone can install a ready Full of an
/// older commit and fetch the rest from the upstream. The base is the newest
/// ready Full admitted from B's ref, else B's parent's Full. Head(B) need not
/// be ready: a full clone never waits on the server.
async fn top_up_plan(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    row: &BuildRow,
) -> Result<Option<serde_json::Value>, ApiError> {
    let control = |error: anyhow::Error| {
        state.metrics.record_error();
        ApiError::control(error)
    };
    let mut base_commit = None;
    if let Some(ref_name) = row.ref_name.as_deref() {
        base_commit = state
            .control
            .latest_ready_full(repo_id, ref_name)
            .await
            .map_err(control)?
            .map(|(commit, _)| commit);
    }
    let Some(base_commit) = base_commit.or_else(|| row.parent.clone()) else {
        return Ok(None);
    };
    let Some(base) = state
        .control
        .status(repo_id, &base_commit)
        .await
        .map_err(control)?
        .filter(|base| base.full.ready().is_some())
    else {
        return Ok(None);
    };
    Ok(Some(serde_json::json!({
        "base_commit": base_commit,
        "base": ready_body(state, repo_id, provider, &base, ExactResultKind::Full),
    })))
}

pub(super) const DEFAULT_SIGNED_URL_TTL: Duration = Duration::from_secs(1200);

/// `RIPCLONE_SIGNED_URL_TTL_SECS`, read once at startup.
pub(super) fn signed_url_ttl_from_env() -> Result<Duration> {
    crate::env::number(
        "RIPCLONE_SIGNED_URL_TTL_SECS",
        DEFAULT_SIGNED_URL_TTL.as_secs(),
    )
    .map(Duration::from_secs)
}

/// The ready `kind` output of `row`, with a signed URL for every artifact the
/// backend can sign. `None` entries fall back to the gateway's
/// `/v1/artifacts/{hash}`.
fn ref_response(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    row: &BuildRow,
    storage: &crate::storage::StorageRef,
    ttl: Duration,
    kind: ExactResultKind,
) -> RefResponse {
    let info = row_info(row);
    let artifacts = exact_result_clonepack(&info, kind)
        .expect("requested exact result is ready before response construction");
    let archive_chunks: Vec<String> = info
        .files
        .as_ref()
        .filter(|_| kind == ExactResultKind::Files)
        .map(|files| files.archive_chunks.clone())
        .unwrap_or_default();
    let packs: Vec<String> = match kind {
        ExactResultKind::Head => info.head.as_ref().map(|head| head.packs.as_slice()),
        ExactResultKind::Full => info.full.as_ref().map(|full| full.packs.as_slice()),
        ExactResultKind::Files => None,
    }
    .unwrap_or_default()
    .iter()
    .map(|p| p.pack.clone())
    .collect();
    let signed = |hash: &str| signed_url(storage, ttl, hash);
    // Ordered to match the manifest's own chunk and pack lists.
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
        commit: artifacts.commit.clone(),
        parent_commit: row.parent.clone(),
        clonepack_manifest: artifacts.manifest.clone(),
        clonepack_manifest_url: signed(&artifacts.manifest),
        metadata_chunk: artifacts.metadata_chunk.clone(),
        metadata_chunk_url: signed(&artifacts.metadata_chunk),
        archive_chunk_urls: signed_list(&archive_chunks),
        pack_chunk_urls: signed_list(&packs),
        // The pre-built MIDX lets the client skip `git multi-pack-index
        // write`; the idx bundle fetches every idx in one GET.
        midx_url: signed(&artifacts.midx),
        idx_bundle_url: signed(&artifacts.idx_bundle),
        kind,
    }
}

fn signed_url(storage: &crate::storage::StorageRef, ttl: Duration, hash: &str) -> Option<String> {
    if hash.is_empty() {
        return None;
    }
    storage.signed_url(hash, ttl)
}

fn credential_error(e: anyhow::Error) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "credential_unavailable",
        format!("credential fetch failed: {e}"),
    )
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

async fn remove_added_repo_inner(repo_id: RepoId, state: ServerState) -> Response {
    match state.control.remove_repo(&repo_id).await {
        Ok(true) => (StatusCode::NO_CONTENT, Body::empty()).into_response(),
        Ok(false) => repo_not_added().into_response(),
        Err(e) => {
            state.metrics.record_error();
            ApiError::control(e).into_response()
        }
    }
}

/// Admit `commit` and wake the pool that has work for it. Returns the row and
/// whether this call created it. With `retry`, failed outputs go back to
/// pending: an explicit request (`POST .../builds` with `retry`, `/add`) asks
/// for them again. Webhook and poller admissions leave failures alone.
pub(super) async fn admit_build(
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

/// `GET /v1/admin/config/{provider}/{path}`: the stored repository config,
/// or 404 `config_not_found`.
pub(super) async fn admin_get_config(
    Path(path): Path<String>,
    State(state): State<ServerState>,
) -> Response {
    let repo_id = match repo_from_path(&state, &path) {
        Ok((repo_id, _)) => repo_id,
        Err(error) => return error.into_response(),
    };
    match state.control.get_config(&repo_id).await {
        Ok(Some(config)) => (StatusCode::OK, Json(config)).into_response(),
        Ok(None) => ApiError::new(
            StatusCode::NOT_FOUND,
            "config_not_found",
            "no config stored for this repository",
        )
        .into_response(),
        Err(e) => {
            state.metrics.record_error();
            ApiError::control(e).into_response()
        }
    }
}

/// `POST /v1/admin/config/{provider}/{path}`: store the repository config.
/// The body is validated before the control database is written; builds read
/// it when they start.
pub(super) async fn admin_put_config(
    Path(path): Path<String>,
    State(state): State<ServerState>,
    Json(config): Json<crate::repo_config::RepoConfig>,
) -> Response {
    let repo_id = match repo_from_path(&state, &path) {
        Ok((repo_id, _)) => repo_id,
        Err(error) => return error.into_response(),
    };
    if let Err(e) = config.validate() {
        return ApiError::invalid_request(format!("invalid config: {e:#}")).into_response();
    }
    match state.control.set_config(&repo_id, &config).await {
        Ok(()) => (StatusCode::OK, Json(config)).into_response(),
        Err(e) => {
            state.metrics.record_error();
            ApiError::control(e).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::build_app;
    use crate::server::builder::make_manifest;
    use crate::server::tests::{mark_added, request_with_auth, test_request, test_state};
    use crate::storage::StorageRef;
    use axum::Router;
    use axum::http::HeaderMap;
    use std::sync::Arc;
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

    /// A row whose ready outputs are those of `info`; the rest are pending.
    fn row_of(info: RefInfo, parent: Option<String>) -> BuildRow {
        fn out<T>(result: Option<T>) -> Out<T> {
            result.map_or(Out::Pending { error: None }, Out::Ready)
        }
        BuildRow {
            commit: info.commit,
            ref_name: None,
            parent,
            created_at: 0,
            head: out(info.head),
            files: out(info.files),
            full: out(info.full),
            attempts: 0,
            lease_until: None,
        }
    }

    /// Admit `commit`, publish its Head (with `parent`) and Full, and fail
    /// its Files for good.
    async fn publish_head_and_full(
        state: &ServerState,
        repo_id: &RepoId,
        commit: &str,
        ref_name: Option<&str>,
        parent: Option<&str>,
    ) {
        let metadata = state
            .cas
            .put(format!("metadata {commit}").as_bytes())
            .unwrap();
        let manifest =
            make_manifest(commit, &None, &[], &metadata, 15, Vec::new(), None, None).unwrap();
        let manifest = state.cas.put(&manifest.encode_to_vec()).unwrap();
        let artifacts = |label: &str| crate::ClonepackArtifacts {
            manifest: manifest.clone(),
            metadata_chunk: metadata.clone(),
            ..ready_artifacts(commit, label)
        };
        let lease = Duration::from_secs(60);
        state
            .control
            .admit(repo_id, commit, ref_name)
            .await
            .unwrap();
        let head = state
            .control
            .claim("test-head", crate::control::Pool::Head, lease)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.build.commit, commit);
        let output = crate::control::Output::Head {
            result: crate::HeadResult {
                clonepack: artifacts("head"),
                ..Default::default()
            },
            parent: parent.map(str::to_string),
        };
        assert!(state.control.publish(&head, output).await.unwrap().applied);
        state.control.release(&head).await.unwrap();
        let background = state
            .control
            .claim("test-background", crate::control::Pool::Background, lease)
            .await
            .unwrap()
            .unwrap();
        let output = crate::control::Output::Full(crate::FullResult {
            clonepack: artifacts("full"),
            ..Default::default()
        });
        assert!(
            state
                .control
                .publish(&background, output)
                .await
                .unwrap()
                .applied
        );
        state
            .control
            .fail(&background, ExactResultKind::Files, "archive broke", None)
            .await
            .unwrap();
        state.control.release(&background).await.unwrap();
    }

    async fn call(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        let mut request = test_request(method, uri);
        if let Some(body) = body {
            *request.body_mut() = Body::from(body.to_string());
            request.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
        }
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = if body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };
        (status, headers, body)
    }

    #[tokio::test]
    async fn commits_answer_each_output_state_without_caching() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let repo_id = RepoId::github("acme/api");
        mark_added(&state, repo_id.clone()).await;
        let base = "b".repeat(40);
        let tip = "c".repeat(40);
        publish_head_and_full(&state, &repo_id, &base, Some("refs/heads/main"), None).await;
        state
            .control
            .admit(&repo_id, &tip, Some("refs/heads/main"))
            .await
            .unwrap();
        let control = Arc::clone(&state.control);
        let app = build_app(state);
        let commits = "/v1/repos/github/acme/api/commits";

        let (status, headers, ready) =
            call(&app, "GET", &format!("{commits}/{base}?kind=full"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(ready["state"], "ready");
        assert_eq!(ready["kind"], "full");
        assert_eq!(ready["commit"], base);
        assert!(ready.get("branch").is_none());
        assert!(ready["clonepack_manifest"].is_string());

        let (_, _, failed) = call(&app, "GET", &format!("{commits}/{base}?kind=files"), None).await;
        assert_eq!(
            failed,
            serde_json::json!({"state": "failed", "commit": base, "kind": "files", "error": "archive broke"})
        );

        // Full(tip) pending: the newest ready Full of the same ref is the
        // top-up base, although Head(tip) is not built.
        let (status, headers, pending) =
            call(&app, "GET", &format!("{commits}/{tip}?kind=full"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(pending["state"], "pending");
        assert_eq!(pending["attempts"], 0);
        assert_eq!(pending["top_up"]["base_commit"], base);
        assert_eq!(pending["top_up"]["base"], ready);
        let (_, _, head) = call(&app, "GET", &format!("{commits}/{tip}?kind=head"), None).await;
        assert_eq!(
            head,
            serde_json::json!({"state": "pending", "commit": tip, "kind": "head", "attempts": 0})
        );

        let (_, _, summary) = call(&app, "GET", &format!("{commits}/{base}"), None).await;
        assert_eq!(
            summary,
            serde_json::json!({
                "commit": base,
                "head": {"state": "ready"},
                "files": {"state": "failed", "error": "archive broke"},
                "full": {"state": "ready"},
            })
        );

        let unknown = "d".repeat(40);
        let (status, headers, missing) =
            call(&app, "GET", &format!("{commits}/{unknown}?kind=full"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(missing["code"], "build_not_found");
        assert!(control.status(&repo_id, &unknown).await.unwrap().is_none());

        let (status, _, invalid) =
            call(&app, "GET", &format!("{commits}/main?kind=full"), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid["code"], "invalid_commit");
    }

    #[tokio::test]
    async fn top_up_falls_back_to_the_parent_full() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let repo_id = RepoId::github("acme/api");
        let parent = "b".repeat(40);
        let child = "c".repeat(40);
        publish_head_and_full(&state, &repo_id, &parent, None, None).await;
        // The child's Head names the parent; its Full is still pending.
        state.control.admit(&repo_id, &child, None).await.unwrap();
        let head = state
            .control
            .claim(
                "test-head",
                crate::control::Pool::Head,
                Duration::from_secs(60),
            )
            .await
            .unwrap()
            .unwrap();
        let mut artifacts = state
            .control
            .status(&repo_id, &parent)
            .await
            .unwrap()
            .unwrap();
        let Out::Ready(mut result) =
            std::mem::replace(&mut artifacts.head, Out::Failed(String::new()))
        else {
            panic!("parent Head is ready");
        };
        result.clonepack.commit = child.clone();
        let output = crate::control::Output::Head {
            result,
            parent: Some(parent.clone()),
        };
        assert!(state.control.publish(&head, output).await.unwrap().applied);
        let app = build_app(state);

        let (_, _, pending) = call(
            &app,
            "GET",
            &format!("/v1/repos/github/acme/api/commits/{child}?kind=full"),
            None,
        )
        .await;
        assert_eq!(pending["state"], "pending");
        assert_eq!(pending["top_up"]["base_commit"], parent);
        assert_eq!(
            pending["top_up"]["base"]["parent_commit"],
            serde_json::Value::Null
        );
        let (_, _, head) = call(
            &app,
            "GET",
            &format!("/v1/repos/github/acme/api/commits/{child}?kind=head"),
            None,
        )
        .await;
        assert_eq!(head["state"], "ready");
        assert_eq!(head["parent_commit"], parent);
    }

    #[tokio::test]
    async fn builds_admit_a_sha_without_the_upstream() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let repo_id = RepoId::github("acme/api");
        let builds = "/v1/repos/github/acme/api/builds";
        let commit = "e".repeat(40);
        let control = Arc::clone(&state.control);
        let app = build_app(state);

        let (status, _, body) = call(
            &app,
            "POST",
            builds,
            Some(serde_json::json!({"selector": commit})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "repo_not_added");
        control.add_repo(&repo_id).await.unwrap();

        // A short SHA is a valid branch name, so it is not tested here: it
        // would ask the upstream.
        for selector in ["HEAD~1", "main^", "main@{1}", ""] {
            let (status, _, body) = call(
                &app,
                "POST",
                builds,
                Some(serde_json::json!({"selector": selector})),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{selector:?}");
            assert_eq!(body["code"], "invalid_selector");
        }
        for request in [
            serde_json::json!({}),
            serde_json::json!({"selector": "HEAD", "branch": "main"}),
        ] {
            let (status, _, body) = call(&app, "POST", builds, Some(request)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], "invalid_request");
        }
        assert!(control.list_builds(&repo_id).await.unwrap().is_empty());

        let (status, _, body) = call(
            &app,
            "POST",
            builds,
            Some(serde_json::json!({"selector": commit, "retry": true})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            serde_json::json!({
                "commit": commit,
                "checkout_name": null,
                "head": {"state": "pending"},
                "files": {"state": "pending"},
                "full": {"state": "pending"},
            })
        );
        let row = control.status(&repo_id, &commit).await.unwrap().unwrap();
        assert_eq!(row.ref_name, None);

        let (status, _, body) = call(
            &app,
            "POST",
            "/v1/repos/nowhere/acme/api/builds",
            Some(serde_json::json!({"selector": commit})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "unknown_provider");
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
            &row_of(info, None),
            &storage,
            DEFAULT_SIGNED_URL_TTL,
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
        // `parent_commit` comes from the row, not from the Head result.
        let row = row_of(info.clone(), Some(parent.clone()));

        let full = ref_response(
            &repo_id,
            &provider,
            &row,
            &storage,
            DEFAULT_SIGNED_URL_TTL,
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

        let files = ref_response(
            &repo_id,
            &provider,
            &row,
            &storage,
            DEFAULT_SIGNED_URL_TTL,
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
            &row,
            &storage,
            DEFAULT_SIGNED_URL_TTL,
            ExactResultKind::Head,
        );
        assert_eq!(head.pack_chunk_urls, Some(vec![signed(1200, "h1-pack")]));
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
    async fn admin_config_is_addressed_by_provider_and_path() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let control = Arc::clone(&state.control);
        let app = build_app(state);
        let uri = "/v1/admin/config/github/acme/configured";

        let (status, _, body) = call(&app, "GET", uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "config_not_found");
        let (status, _, _) = call(&app, "POST", uri, Some(serde_json::json!({}))).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            control
                .get_config(&RepoId::github("acme/configured"))
                .await
                .unwrap()
                .is_some()
        );
        let (status, _, _) = call(&app, "GET", uri, None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, body) = call(&app, "GET", "/v1/admin/config/nowhere/acme/x", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "unknown_provider");
    }
}
