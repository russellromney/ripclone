//! The `/v1/repos` routes: add and remove repositories, admit builds, read commits and repository status.

use super::{ErrorResponse, ServerState, ls_remote_semaphore};
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
use std::collections::{BTreeMap, HashMap};
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
    /// The output the caller reads next: the answer's `output` is its body,
    /// as `GET .../commits/{sha}?kind=` answers.
    #[serde(default)]
    pub kind: Option<ExactResultKind>,
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
    ApiError::new(StatusCode::NOT_FOUND, "repo_not_added", "repo not added")
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
    /// Hash of the delta on `metadata_chunk`; the files table is the chunk
    /// with it applied. Absent when the chunk is the whole table.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub metadata_delta: String,
    /// Signed URL for the metadata delta, if there is one and the backend
    /// supports it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_delta_url: Option<String>,
    /// Signed URL for each archive chunk. `None` entries fall back to the
    /// gateway's `/v1/artifacts/{hash}` endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for each editable pack, ordered to match `manifest.packs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pack_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for each history pack's idx, ordered to match
    /// `manifest.packs`. HEAD-closure packs read theirs from the idx bundle
    /// and have `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pack_idx_urls: Option<Vec<Option<String>>>,
    /// Signed URL for the pre-built multi-pack-index (`manifest.midx`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub midx_url: Option<String>,
    /// Signed URL for the concatenated idx bundle (`manifest.idx_bundle`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idx_bundle_url: Option<String>,
    /// CAS hash of the repository's tags artifact, absent when it has no
    /// tags or none is published yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags_artifact: Option<String>,
    /// Signed URL for `tags_artifact`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags_artifact_url: Option<String>,
    /// A Full's branches artifact and side pack, absent before the
    /// repository's first refresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branches: Option<crate::branches::BranchesRef>,
    /// The ready Full of the commit the side pack is built against, the
    /// default branch's tip, when it is another commit. A full clone
    /// installs its history with the side pack to hold every ref, as `git
    /// clone --branch` downloads every ref before its checkout.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs_base: Option<Box<RefResponse>>,
    /// With `refs_base`, what the clone installs beyond its history and
    /// the side pack, when the server built it: then the clone downloads
    /// none of this Full's history packs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub own_history: Option<crate::branches::OwnHistoryRef>,
    pub kind: ExactResultKind,
    /// The manifest and files table when they are small and the server holds
    /// them: base64 bytes by hash, used in place of a download.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub inline: BTreeMap<String, String>,
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
    match repo_from_path(&state, &path) {
        Ok((repo_id, _)) => match super::purge::purge(&state, &repo_id).await {
            Ok(_) => (StatusCode::NO_CONTENT, Body::empty()).into_response(),
            Err(e) => {
                state.metrics.record_error();
                ApiError::control(e).into_response()
            }
        },
        Err(error) => error.into_response(),
    }
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
/// and names cost one `ls-remote` under the ls-remote cap. Nothing is
/// fetched.
pub(super) async fn resolve(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    selector: &Selector,
) -> Result<Resolved, ApiError> {
    Ok(resolve_listing(state, repo_id, provider, selector, false)
        .await?
        .0)
}

/// [`resolve`] of `HEAD`, with upstream's branches and tags from the same
/// `ls-remote`.
pub(super) async fn resolve_head_and_refs(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
) -> Result<(Resolved, git::UpstreamRefs), ApiError> {
    resolve_listing(state, repo_id, provider, &Selector::Head, true).await
}

async fn resolve_listing(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    selector: &Selector,
    refs: bool,
) -> Result<(Resolved, git::UpstreamRefs), ApiError> {
    let name = match selector {
        Selector::Commit(commit) => {
            let resolved = Resolved {
                commit: commit.clone(),
                checkout_name: None,
                ref_name: None,
            };
            return Ok((resolved, git::UpstreamRefs::default()));
        }
        Selector::Head => "HEAD",
        Selector::Name(name) => name.as_str(),
    };
    let credential = state
        .credentials
        .get(repo_id)
        .await
        .map_err(credential_error)?;
    let tip = {
        let _permit = ls_remote_semaphore()
            .acquire()
            .await
            .expect("ls-remote semaphore never closed");
        if refs && name == "HEAD" {
            git::ls_remote_refs(provider, repo_id, credential.as_ref()).await
        } else {
            git::ls_remote_tip_async(provider, repo_id, name, credential.as_ref())
                .await
                .map(|tip| (tip, git::UpstreamRefs::default()))
        }
    };
    let (tip, upstream_refs) = match tip {
        Ok((Some(tip), upstream_refs)) => (tip, upstream_refs),
        Ok((None, _)) => {
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
    let resolved = Resolved {
        commit: tip.commit,
        checkout_name,
        ref_name,
    };
    Ok((resolved, upstream_refs))
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
    let output = request.kind.map(|kind| (provider, kind));
    admit_resolved(state, repo_id, resolved, request.retry, start, output).await
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
    admit_resolved(state, repo_id, resolved, true, start, None).await
}

async fn admit_resolved(
    state: &ServerState,
    repo_id: &RepoId,
    resolved: Resolved,
    retry: bool,
    start: Instant,
    output: Option<(&ProviderInstance, ExactResultKind)>,
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
    if let Some(tag) =
        (resolved.ref_name.as_deref()).and_then(|name| name.strip_prefix("refs/tags/"))
    {
        body["tag"] = serde_json::json!(tag);
    }
    if let Some((provider, kind)) = output {
        body["output"] = output_body(state, repo_id, provider, &row, kind).await?;
    }
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

/// A ready output's body, with the repository's refs as of now. Refs that
/// cannot be read are left out, as before the first publish: the clone
/// still comes, and fetches its refs from upstream.
///
/// A Full gets the newest published branches it can complete every ref
/// from: those whose base is in its own history, or else whose base has a
/// ready Full, named as `refs_base` for the clone to install. With none, it
/// gets the newest branches and no `refs_base`, and the clone fetches the
/// refs from the upstream.
async fn ready_body(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    row: &BuildRow,
    kind: ExactResultKind,
) -> Result<serde_json::Value, ApiError> {
    let control = |error: anyhow::Error| {
        state.metrics.record_error();
        ApiError::control(error)
    };
    let refs = state
        .control
        .get_refs(repo_id)
        .await
        .unwrap_or_else(|error| {
            state.metrics.record_error();
            tracing::warn!(
                "read refs of {}: {error:#}; the response has none",
                repo_id.storage_key()
            );
            None
        });
    let respond = |row: &BuildRow, refs: Option<&crate::control::RepoRefs>| {
        ref_response(
            repo_id,
            provider,
            row,
            refs,
            &state.storage,
            state.signed_url_ttl,
            kind,
        )
    };
    let mut result = respond(row, refs.as_ref());
    if let Some(stored) = refs.as_ref().filter(|_| kind == ExactResultKind::Full) {
        // The ready branches with the tags of their own refresh.
        let ready = stored
            .ready
            .clone()
            .map(|branches| crate::control::RepoRefs {
                tags_artifact: branches.tags.artifact.clone(),
                tags_len: branches.tags.len,
                tags_count: branches.tags.count,
                tags_hash: branches.tags.hash.clone(),
                branches: Some(branches),
                ready: None,
            });
        for served in [Some(stored), ready.as_ref()].into_iter().flatten() {
            let Some(base) = (served.branches.as_ref()).and_then(|branches| branches.base.clone())
            else {
                continue;
            };
            if base == row.commit || in_history(state, repo_id, &base, &row.commit).await {
                result = respond(row, Some(served));
                break;
            }
            let base = state
                .control
                .status(repo_id, &base)
                .await
                .map_err(control)?;
            // A later admit of the base's branch that does not descend from
            // it was force-pushed: these refs are behind the upstream, so
            // the clone, lacking the base, fetches them from there.
            let newer = |base: &BuildRow| {
                row.ref_name.is_some() && row.ref_name == base.ref_name && row.seq > base.seq
            };
            if base.as_ref().is_some_and(newer) {
                break;
            }
            if let Some(base) = base.filter(|base| base.full.ready().is_some()) {
                result = respond(row, Some(served));
                result.own_history = own_history(state, repo_id, row, &base.commit).await;
                result.refs_base = Some(Box::new(respond(&base, Some(served))));
                break;
            }
        }
    }
    inline_small(&state.cas, &mut result).await;
    if let Some(base) = result.refs_base.as_deref_mut() {
        inline_small(&state.cas, base).await;
    }
    Ok(serde_json::json!(ReadyBody {
        state: "ready",
        result,
    }))
}

/// The own history of `row`'s Full, signed, when a clone that installs
/// `base`'s history holds every object but these: built against `base` or a
/// commit in its history.
async fn own_history(
    state: &ServerState,
    repo_id: &RepoId,
    row: &BuildRow,
    base: &str,
) -> Option<crate::branches::OwnHistoryRef> {
    let own = row.full.ready()?.own_history.as_ref()?;
    if own.base != base && !in_history(state, repo_id, &own.base, base).await {
        return None;
    }
    let signed = |hash: &str| signed_url(&state.storage, state.signed_url_ttl, hash);
    Some(crate::branches::OwnHistoryRef {
        packs: (own.packs.iter())
            .map(|pack| crate::branches::SidePackRef {
                pack: pack.pack.clone(),
                pack_len: pack.pack_len,
                pack_url: signed(&pack.pack),
                idx: pack.idx.clone(),
                idx_len: pack.idx_len,
                idx_url: signed(&pack.idx),
            })
            .collect(),
        head_packs: own.head_packs.clone(),
    })
}

/// The most bytes of one artifact, and of all of a response's, it carries
/// inline.
const INLINE_MAX: u64 = 32 << 10;
const INLINE_TOTAL: u64 = 64 << 10;

/// Put `response`'s manifest and files table in its `inline` while they are
/// small and in the local store, so a clone starts its pack downloads
/// without first downloading them. Anything else it downloads as before.
async fn inline_small(cas: &crate::cas::Cas, response: &mut RefResponse) {
    let hashes = [
        &response.clonepack_manifest,
        &response.metadata_chunk,
        &response.metadata_delta,
    ]
    .into_iter()
    .filter(|hash| !hash.is_empty())
    .cloned()
    .collect::<Vec<_>>();
    let cas = cas.clone();
    let Ok(found) = tokio::task::spawn_blocking(move || {
        let mut total = 0;
        let mut found = Vec::new();
        for hash in hashes {
            let Some(bytes) = cas.get_small(&hash, INLINE_MAX) else {
                continue;
            };
            total += bytes.len() as u64;
            if total > INLINE_TOTAL {
                break;
            }
            found.push((hash, bytes));
        }
        found
    })
    .await
    else {
        return;
    };
    for (hash, bytes) in found {
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
        response.inline.insert(hash, encoded);
    }
}

/// Whether `base` is in `commit`'s history in the mirror: then a Full of
/// `commit` holds all of the base's. A mirror that cannot answer says no,
/// and the clone downloads the base's history too. Asked once per pair.
async fn in_history(state: &ServerState, repo_id: &RepoId, base: &str, commit: &str) -> bool {
    let cell = (state.ancestry).cell(format!("{} {base} {commit}", repo_id.storage_key()));
    let answer = cell
        .get_or_try_init(|| async {
            let dir = state.repo_root.join(repo_id.mirror_dir_name());
            let (base, commit) = (base.to_string(), commit.to_string());
            let key = format!(
                "{} in {}",
                &base[..7.min(base.len())],
                &commit[..7.min(commit.len())]
            );
            let answer = crate::cancel::spawn_blocking(move || {
                crate::gix_util::is_ancestor(&dir, &base, &commit)
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(|answer| answer)?;
            tracing::info!("ancestry of {key} in {}: {answer}", repo_id.storage_key());
            anyhow::Ok(answer)
        })
        .await;
    match answer {
        Ok(answer) => *answer,
        Err(error) => {
            tracing::debug!("ancestry in {}: {error:#}", repo_id.storage_key());
            false
        }
    }
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
    let body = output_body(state, repo_id, provider, &row, kind).await?;
    Ok(Json(body).into_response())
}

/// One body per state of `row`'s `kind` output.
async fn output_body(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    row: &BuildRow,
    kind: ExactResultKind,
) -> Result<serde_json::Value, ApiError> {
    Ok(match kind_state(row, kind) {
        Out::Ready(()) => ready_body(state, repo_id, provider, row, kind).await?,
        Out::Failed(error) => serde_json::json!({
            "state": "failed",
            "commit": row.commit,
            "kind": kind,
            "error": error,
        }),
        Out::Pending { error } => pending_body(state, repo_id, provider, row, kind, error).await?,
    })
}

/// The answer for `row`'s pending `kind` output. A pending Full names its
/// top-up base, read after `row`: when that base is the commit itself, its
/// Full was published in between, and the answer is that ready Full.
async fn pending_body(
    state: &ServerState,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    row: &BuildRow,
    kind: ExactResultKind,
    error: Option<String>,
) -> Result<serde_json::Value, ApiError> {
    let base = match kind {
        ExactResultKind::Full => top_up_base(state, repo_id, row).await?,
        _ => None,
    };
    if let Some(base) = base.as_ref().filter(|base| base.commit == row.commit) {
        return ready_body(state, repo_id, provider, base, kind).await;
    }
    let mut body = serde_json::json!({
        "state": "pending",
        "commit": row.commit,
        "kind": kind,
        "attempts": row.lease(kind).attempts,
    });
    if let Some(error) = error {
        body["error"] = serde_json::json!(error);
    }
    if let Some(base) = base {
        body["top_up"] = serde_json::json!({
            "base_commit": base.commit,
            "base": ready_body(state, repo_id, provider, &base, ExactResultKind::Full).await?,
        });
    }
    Ok(body)
}

/// While Full(B) is pending, a full clone can install a ready Full of an
/// older commit and fetch the rest from the upstream. The base is the newest
/// ready Full admitted from B's ref, else B's parent's Full. Head(B) need not
/// be ready: a full clone never waits on the server.
async fn top_up_base(
    state: &ServerState,
    repo_id: &RepoId,
    row: &BuildRow,
) -> Result<Option<BuildRow>, ApiError> {
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
    Ok(state
        .control
        .status(repo_id, &base_commit)
        .await
        .map_err(control)?
        .filter(|base| base.full.ready().is_some()))
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
    refs: Option<&crate::control::RepoRefs>,
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
    let pack_artifacts = match kind {
        ExactResultKind::Head => info.head.as_ref().map(|head| head.packs.as_slice()),
        ExactResultKind::Full => info.full.as_ref().map(|full| full.packs.as_slice()),
        ExactResultKind::Files => None,
    }
    .unwrap_or_default();
    let packs: Vec<String> = pack_artifacts.iter().map(|p| p.pack.clone()).collect();
    // The idx files fetched on their own: a Full's history packs' and a
    // Head's levels'. The Head base's are in the idx bundle.
    // A Full's Head packs a clone installs with its own history, too.
    let own_idx: std::collections::HashSet<&str> = match kind {
        ExactResultKind::Full => (info.full.as_ref())
            .map(|full| {
                let installed = (full.own_history.iter())
                    .flat_map(|own| &own.head_packs)
                    .filter_map(|&i| full.packs.get(i).map(|p| p.idx.as_str()));
                (full.history_levels.iter())
                    .flat_map(|level| level.packs.iter().map(|p| p.idx.as_str()))
                    .chain(installed)
                    .collect()
            })
            .unwrap_or_default(),
        ExactResultKind::Head => (info.head.as_ref())
            .map(|head| head.levels.iter().map(|p| p.idx.as_str()).collect())
            .unwrap_or_default(),
        ExactResultKind::Files => Default::default(),
    };
    let signed = |hash: &str| signed_url(storage, ttl, hash);
    let pack_idx_urls: Option<Vec<Option<String>>> = (!own_idx.is_empty()).then(|| {
        pack_artifacts
            .iter()
            .map(|p| {
                own_idx
                    .contains(p.idx.as_str())
                    .then(|| signed(&p.idx))
                    .flatten()
            })
            .collect()
    });
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
        metadata_delta: artifacts.metadata_delta.clone(),
        metadata_delta_url: signed(&artifacts.metadata_delta),
        archive_chunk_urls: signed_list(&archive_chunks),
        pack_chunk_urls: signed_list(&packs),
        pack_idx_urls: pack_idx_urls.filter(|urls| urls.iter().any(Option::is_some)),
        // The pre-built MIDX lets the client skip `git multi-pack-index
        // write`; the idx bundle fetches every idx in one GET.
        midx_url: signed(&artifacts.midx),
        idx_bundle_url: signed(&artifacts.idx_bundle),
        // No tags, no download.
        tags_artifact: refs
            .filter(|refs| refs.tags_count > 0)
            .map(|refs| refs.tags_artifact.clone()),
        tags_artifact_url: refs
            .filter(|refs| refs.tags_count > 0)
            .and_then(|refs| signed(&refs.tags_artifact)),
        // Only a Full gets every branch, as `git clone --depth` gets one.
        branches: refs
            .and_then(|refs| refs.branches.as_ref())
            .filter(|_| kind == ExactResultKind::Full)
            .map(|branches| crate::branches::BranchesRef {
                artifact: branches.artifact.clone(),
                artifact_url: signed(&branches.artifact),
                side_packs: (branches.side_packs.iter())
                    .map(|side| crate::branches::SidePackRef {
                        pack: side.pack.clone(),
                        pack_len: side.pack_len,
                        pack_url: signed(&side.pack),
                        idx: side.idx.clone(),
                        idx_len: side.idx_len,
                        idx_url: signed(&side.idx),
                    })
                    .collect(),
            }),
        refs_base: None,
        own_history: None,
        kind,
        inline: BTreeMap::new(),
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
    // Storage reads block (remote storage makes a request each), so they run
    // off the async workers.
    let wanted: Vec<String> = rows
        .iter()
        .flat_map(|row| collect_manifest_hashes(&row_info(row)))
        .collect();
    let storage = state.storage.clone();
    let manifests: HashMap<String, Vec<u8>> = tokio::task::spawn_blocking(move || {
        wanted
            .into_iter()
            .map(|hash| {
                let bytes = storage.get(&hash)?;
                Ok((hash, bytes))
            })
            .collect::<Result<_>>()
    })
    .await
    .context("read manifests for status")??;
    let mut refs = Vec::new();
    let mut unique_chunks: HashMap<String, u64> = HashMap::new();
    for row in rows {
        let info = row_info(&row);
        let manifest_hashes = collect_manifest_hashes(&info);
        let mut ref_bytes = 0u64;
        for manifest_hash in manifest_hashes {
            let manifest_bytes = manifests
                .get(&manifest_hash)
                .context("manifest read for status is missing")?;
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
    // Head and Files build from the commit alone; Full once Head is ready.
    if matches!(row.head, Out::Pending { .. }) {
        state.head_wake.notify_one();
    }
    if matches!(row.files, Out::Pending { .. }) {
        state.files_wake.notify_one();
    }
    if matches!(row.head, Out::Ready(_)) && matches!(row.full, Out::Pending { .. }) {
        state.full_wake.notify_one();
    }
    Ok((row, inserted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::build_app;
    use crate::server::builder::{chunk_ref, make_manifest};
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
            seq: 0,
            head: out(info.head),
            files: out(info.files),
            full: out(info.full),
            head_lease: Default::default(),
            files_lease: Default::default(),
            full_lease: Default::default(),
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
        let manifest = make_manifest(
            commit,
            &None,
            &[],
            &(chunk_ref(&metadata, 15).unwrap(), None),
            Vec::new(),
            None,
            None,
        )
        .unwrap();
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
        let files = state
            .control
            .claim("test-files", crate::control::Pool::Files, lease)
            .await
            .unwrap()
            .unwrap();
        state
            .control
            .fail(&files, "archive broke", None)
            .await
            .unwrap();
        state.control.release(&files).await.unwrap();
        let full = state
            .control
            .claim("test-full", crate::control::Pool::Full, lease)
            .await
            .unwrap()
            .unwrap();
        let output = crate::control::Output::Full(crate::FullResult {
            clonepack: artifacts("full"),
            ..Default::default()
        });
        assert!(state.control.publish(&full, output).await.unwrap().applied);
        state.control.release(&full).await.unwrap();
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
    async fn a_full_built_after_its_row_was_read_answers_ready() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let repo_id = RepoId::github("acme/api");
        let commit = "c".repeat(40);
        state
            .control
            .admit(&repo_id, &commit, Some("refs/tags/v1"))
            .await
            .unwrap();
        let stale = state
            .control
            .status(&repo_id, &commit)
            .await
            .unwrap()
            .unwrap();
        assert!(stale.full.ready().is_none());
        publish_head_and_full(&state, &repo_id, &commit, Some("refs/tags/v1"), None).await;
        let (_, provider) = repo_from_path(&state, "github/acme/api").unwrap();
        let provider = provider.clone();

        let body = pending_body(
            &state,
            &repo_id,
            &provider,
            &stale,
            ExactResultKind::Full,
            None,
        )
        .await
        .unwrap();
        assert_eq!(body["state"], "ready");
        assert_eq!(body["commit"], commit);
        assert!(body.get("top_up").is_none());
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
        // A repo holds one Head lease at a time, so the second build waits
        // for the first to finish.
        assert!(
            state
                .control
                .claim("worker-2", crate::control::Pool::Head, lease)
                .await
                .unwrap()
                .is_none()
        );
        state
            .control
            .fail(&claimed, "retry later", Some(Duration::from_secs(600)))
            .await
            .unwrap();
        state.control.release(&claimed).await.unwrap();
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
            None,
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

        async fn delete(&self, _hash: &str) -> Result<()> {
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
                // f2 and f3 are history packs; f1 is the Head closure.
                history_levels: ["f2", "f3"]
                    .map(|name| crate::HistoryLevel {
                        tip_commit: commit.clone(),
                        packs: vec![crate::SizedPack {
                            pack: format!("{name}-pack"),
                            idx: format!("{name}-idx"),
                            ..Default::default()
                        }],
                    })
                    .to_vec(),
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
            None,
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
        assert_eq!(
            full.pack_idx_urls,
            Some(vec![None, signed(1200, "f2-idx"), signed(1200, "f3-idx")])
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
            None,
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
            None,
            &storage,
            DEFAULT_SIGNED_URL_TTL,
            ExactResultKind::Head,
        );
        assert_eq!(head.pack_chunk_urls, Some(vec![signed(1200, "h1-pack")]));
        assert!(head.pack_idx_urls.is_none());
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
        let manifest = make_manifest(
            &commit,
            &None,
            &[],
            &(chunk_ref(&metadata, 15).unwrap(), None),
            Vec::new(),
            None,
            None,
        )
        .unwrap();
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
        // Full ready while Files is still building.
        let background = state
            .control
            .claim("status-files", crate::control::Pool::Files, lease)
            .await
            .unwrap()
            .unwrap();
        let full_claim = state
            .control
            .claim("status-full", crate::control::Pool::Full, lease)
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
                .publish(&full_claim, full)
                .await
                .unwrap()
                .applied
        );
        state.control.release(&full_claim).await.unwrap();

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
            .fail(&background, "archive broke", None)
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
}
