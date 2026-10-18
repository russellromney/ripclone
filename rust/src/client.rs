use crate::ExactResultKind;
use crate::cas::Cas;
use crate::provider::{ProviderInstance, RepoId};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

mod download;
mod install;
mod progress;
mod staging;
mod tuning;

pub use install::CloneRequest;
pub use progress::Stderr;
pub use staging::wait_for_cancelled_cleanup;

/// Sent on every request so the server can attribute usage and nudge upgrades.
const USER_AGENT: &str = concat!("ripclone/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Deserialize)]
struct ServerError {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    code: Option<String>,
}

/// The state of one output of a build.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OutputState {
    /// `pending`, `ready` or `failed`.
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
}

/// The answer to `POST .../builds` and `POST .../add`, and the summary from
/// `GET .../commits/{sha}`.
#[derive(Debug, Clone, Deserialize)]
pub struct BuildSummary {
    pub commit: String,
    /// The branch a clone of this build checks out; `None` is detached. Only
    /// a POST answer carries it.
    #[serde(default)]
    pub checkout_name: Option<String>,
    pub head: OutputState,
    pub files: OutputState,
    pub full: OutputState,
}

impl BuildSummary {
    fn outputs(&self) -> [(ExactResultKind, &OutputState); 3] {
        [
            (ExactResultKind::Head, &self.head),
            (ExactResultKind::Files, &self.files),
            (ExactResultKind::Full, &self.full),
        ]
    }

    /// Whether any output is still pending.
    pub fn pending(&self) -> bool {
        self.outputs()
            .iter()
            .any(|(_, output)| output.state == "pending")
    }

    /// Whether every output is ready.
    pub fn ready(&self) -> bool {
        self.outputs()
            .iter()
            .all(|(_, output)| output.state == "ready")
    }

    /// Each output's state, e.g. `Head ready, Files pending, Full pending`.
    fn states(&self) -> String {
        let states = self.outputs().map(|(kind, output)| {
            let name = match kind {
                ExactResultKind::Head => "Head",
                ExactResultKind::Files => "Files",
                ExactResultKind::Full => "Full",
            };
            format!("{name} {}", output.state)
        });
        states.join(", ")
    }

    /// The first failed output and its error.
    pub fn failed(&self) -> Option<(ExactResultKind, &str)> {
        self.outputs().into_iter().find_map(|(kind, output)| {
            (output.state == "failed").then(|| (kind, output.error.as_deref().unwrap_or("")))
        })
    }
}

/// `GET .../commits/{sha}?kind=`: one body per state of that output.
#[derive(Debug, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
enum CommitOutput {
    Ready(Box<RefResponse>),
    Pending {
        commit: String,
        #[serde(default)]
        top_up: Option<TopUpPlan>,
    },
    Failed {
        commit: String,
        error: String,
    },
}

/// A ready Full of an older commit that a full clone can install and then
/// fetch the pending commit on top of.
#[derive(Debug, Deserialize)]
struct TopUpPlan {
    base_commit: String,
    base: Box<RefResponse>,
}

fn validate_summary(summary: &BuildSummary, repo_path: &str) -> Result<()> {
    crate::validation::validate_object_id(&summary.commit)
        .with_context(|| format!("invalid build commit for {repo_path}"))?;
    if let Some(name) = summary.checkout_name.as_deref() {
        crate::validation::validate_checkout_name(name)
            .with_context(|| format!("invalid checkout name for {repo_path}"))?;
    }
    Ok(())
}

/// Check that a ready body is the `kind` output of `commit`.
fn validate_output(
    info: &RefResponse,
    commit: &str,
    kind: ExactResultKind,
    repo_path: &str,
) -> Result<()> {
    anyhow::ensure!(
        info.commit == commit,
        "ref integrity error: expected commit {commit} changed to {} in a ready response for {repo_path}",
        info.commit
    );
    anyhow::ensure!(
        info.result == kind,
        "ref result mismatch for {repo_path}: requested {kind}, server returned {}",
        info.result
    );
    anyhow::ensure!(
        !info.clonepack_manifest.is_empty(),
        "{kind} output of {commit} has no clonepack manifest"
    );
    Ok(())
}

/// A failed output. `sync --at` asks for it again.
fn failed_output_error(commit: &str, kind: ExactResultKind, error: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "{kind} output of {commit} failed: {error}\n  → run `ripclone sync <repo> --at {commit}` to build it again"
    )
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

/// A failed exchange with the ripclone server or an artifact URL: an HTTP
/// error status, or no answer at all. The CLI maps `status` to its exit code.
#[derive(Debug)]
pub struct HttpFailure {
    /// The HTTP status, or `None` when the server could not be reached.
    pub status: Option<u16>,
    message: String,
}

impl HttpFailure {
    fn status(status: u16, message: String) -> anyhow::Error {
        anyhow::Error::new(Self {
            status: Some(status),
            message,
        })
    }

    fn unreachable(message: String) -> anyhow::Error {
        anyhow::Error::new(Self {
            status: None,
            message,
        })
    }
}

impl std::fmt::Display for HttpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HttpFailure {}

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
async fn server_error(
    progress: &progress::Progress,
    context: &str,
    resp: reqwest::Response,
) -> anyhow::Error {
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
        progress.note(&u);
    }
    HttpFailure::status(status.as_u16(), format!("{context}: {msg}{hint}"))
}

/// Next-step hint for a failed server response, keyed on HTTP status, the
/// server's `code`, and whether we are talking to the default hosted server.
/// Pure so the access-error paths stay unit-testable: a failure carries the
/// machine-parseable `code` so an agent fleet can detect and route it without
/// scraping prose.
fn error_hint(status: u16, code: Option<&str>, is_cloud: bool) -> &'static str {
    match (status, code, is_cloud) {
        (401, _, true) => "\n  → run `ripclone login`",
        (401, _, false) => "\n  → set RIPCLONE_SERVER_TOKEN to the server's token",
        (402, _, _) => "\n  → the server returned 402 (payment required) for this repo",
        (403, Some("no_access"), _) => "\n  → you don't have access to this repo",
        (403, _, _) => "\n  → access denied by the configured server",
        (429, _, _) => "\n  → rate limited; wait a moment and retry",
        (404, Some("repo_not_added"), _) => "\n  → run `ripclone add <repo>`",
        (404, Some("repo_not_found"), _) => {
            "\n  → check the repository name and the server's access to it"
        }
        (404, Some("ref_not_found"), _) => "\n  → check the branch or tag name",
        (400, Some("invalid_selector"), _) => {
            "\n  → use HEAD, a full commit SHA, or a branch or tag name"
        }
        (426, _, _) => {
            "\n  → this CLI and the server speak different protocols; run `ripclone version` and update the older one"
        }
        (502, Some("upstream_unavailable"), _) => {
            "\n  → the server could not reach the upstream git host; check the host, then retry"
        }
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
    /// Signed URL for each editable pack, ordered to match `manifest.packs`.
    #[serde(default)]
    pub pack_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for each history pack's idx, ordered to match `manifest.packs`.
    #[serde(default)]
    pub pack_idx_urls: Option<Vec<Option<String>>>,
    /// Signed URL for the pre-built multi-pack-index (`manifest.midx`).
    #[serde(default)]
    pub midx_url: Option<String>,
    /// Signed URL for the concatenated idx bundle (`manifest.idx_bundle`).
    #[serde(default)]
    pub idx_bundle_url: Option<String>,
    #[serde(rename = "kind")]
    pub result: ExactResultKind,
}

/// Wait before poll `attempt` (0-based) while an output is pending. Starts at
/// 100 ms and doubles up to a 2 s cap, so a short build is seen at once and a
/// long one is not polled hard. `RIPCLONE_POLL_MS` overrides the cap.
fn poll_delay(attempt: usize) -> std::time::Duration {
    let cap = std::env::var("RIPCLONE_POLL_MS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(2_000u64);
    std::time::Duration::from_millis((100u64 << attempt.min(5)).min(cap))
}

/// How many times a command polls a pending build before giving up.
const MAX_POLLS: usize = 40;

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
    pub mode: ExactResultKind,
    /// True when the resolve saw its output pending before it was ready.
    pub cold: bool,
    /// The cloud's `X-Ripclone-Clone-Id`. `None` means no metrics report.
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
    cache: Option<Cas>,
    /// Upstream git provider instance id (e.g. "github", "gitlab").
    provider: String,
    /// The locally configured provider a top-up fetches from. Its URL comes
    /// from the client's own registry, never from the server.
    provider_instance: Option<ProviderInstance>,
    /// What a clone writes to stderr, and the running clone's counts.
    progress: progress::Progress,
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
    /// local artifact cache; otherwise no cache is used.
    pub fn new_with_token(server: String, token: Option<String>) -> Self {
        let cache_dir = std::env::var_os("RIPCLONE_CACHE_DIR")
            .filter(|dir| !dir.is_empty())
            .map(PathBuf::from);
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
        let cache = cache_dir.and_then(|dir| Cas::cache(dir).ok());
        Self {
            server,
            http,
            raw_http: build_http_client(reqwest::header::HeaderMap::new()),
            cache,
            provider: "github".to_string(),
            provider_instance: None,
            progress: progress::Progress::new(Stderr::default()),
        }
    }

    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self.provider_instance = None;
        self
    }

    /// Use a locally configured provider, which also enables top-up.
    pub fn with_provider_instance(mut self, provider: ProviderInstance) -> Self {
        self.provider = provider.id.as_str().to_string();
        self.provider_instance = Some(provider);
        self
    }

    /// Choose what clones write to stderr; see [`Stderr`].
    pub fn with_stderr(mut self, mode: Stderr) -> Self {
        self.progress = progress::Progress::new(mode);
        self
    }

    /// Build a provider-qualified request URL for `repo_path`.
    fn repo_url(&self, repo_path: &str, suffix: &str) -> String {
        format!(
            "{}/v1/repos/{}/{repo_path}{suffix}",
            self.server, self.provider
        )
    }

    /// Send a request, turning a transport-level failure — server unreachable,
    /// connection refused, DNS failure, timeout — into an actionable message that
    /// names the server. The most common first-run mistake is pointing at a
    /// server that isn't running (or a wrong `RIPCLONE_SERVER`), and
    /// the bare reqwest chain ("connection refused (os error 61)") hides that.
    async fn send(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        req.send().await.map_err(|e| {
            if e.is_connect() || e.is_timeout() {
                HttpFailure::unreachable(format!(
                    "could not reach ripclone server at {}: {}\n  → is the server running? check RIPCLONE_SERVER",
                    self.server,
                    transport_cause(&e),
                ))
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
        let resp = self
            .send(self.http.request(reqwest::Method::GET, &url))
            .await?;
        if !resp.status().is_success() {
            return Err(server_error(&self.progress, "list failed", resp).await);
        }
        resp.json()
            .await
            .context("invalid repository list response")
    }

    /// Remove one repository registration without contacting its upstream.
    pub async fn remove_repo(&self, repo_path: &str) -> Result<()> {
        let url = self.repo_url(repo_path, "/add");
        let resp = self
            .send(self.http.request(reqwest::Method::DELETE, &url))
            .await?;
        if !resp.status().is_success() {
            return Err(server_error(&self.progress, "rm failed", resp).await);
        }
        Ok(())
    }

    /// Remove one repository and everything the server holds for it: its
    /// builds, its mirror, and the artifacts no other repository uses.
    pub async fn purge_repo(&self, repo_path: &str) -> Result<()> {
        let url = self.repo_url(repo_path, "");
        let resp = self
            .send(self.http.request(reqwest::Method::DELETE, &url))
            .await?;
        if !resp.status().is_success() {
            return Err(server_error(&self.progress, "rm --purge failed", resp).await);
        }
        Ok(())
    }

    /// `POST .../builds`: resolve `selector` on the server and admit its
    /// commit. `retry` puts failed outputs back to pending.
    pub async fn post_build(
        &self,
        repo_path: &str,
        selector: &str,
        retry: bool,
    ) -> Result<BuildSummary> {
        Ok(self.request_build(repo_path, selector, retry).await?.0)
    }

    /// [`Self::post_build`], plus the hosted server's per-clone id.
    async fn request_build(
        &self,
        repo_path: &str,
        selector: &str,
        retry: bool,
    ) -> Result<(BuildSummary, Option<String>)> {
        crate::validation::Selector::parse(selector)?;
        let url = self.repo_url(repo_path, "/builds");
        let request = self
            .http
            .request(reqwest::Method::POST, &url)
            .json(&serde_json::json!({"selector": selector, "retry": retry}));
        let resp = self.send(request).await?;
        if !resp.status().is_success() {
            return Err(server_error(&self.progress, "build request failed", resp).await);
        }
        let clone_id = resp
            .headers()
            .get("x-ripclone-clone-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let summary: BuildSummary = resp
            .json()
            .await
            .with_context(|| format!("invalid build response for {repo_path}"))?;
        validate_summary(&summary, repo_path)?;
        if crate::validation::validate_object_id(selector).is_ok() {
            anyhow::ensure!(
                summary.commit == selector,
                "ref integrity error: build of {selector} answered for {}",
                summary.commit
            );
        }
        Ok((summary, clone_id))
    }

    /// `GET .../commits/{sha}`: the state of each output of one build.
    pub async fn build_summary(&self, repo_path: &str, commit: &str) -> Result<BuildSummary> {
        let url = self.repo_url(repo_path, &format!("/commits/{commit}"));
        let resp = self
            .send(self.http.request(reqwest::Method::GET, &url))
            .await?;
        if !resp.status().is_success() {
            return Err(server_error(&self.progress, "build lookup failed", resp).await);
        }
        let summary: BuildSummary = resp
            .json()
            .await
            .with_context(|| format!("invalid build summary for {repo_path}"))?;
        validate_summary(&summary, repo_path)?;
        anyhow::ensure!(
            summary.commit == commit,
            "ref integrity error: build summary of {commit} answered for {}",
            summary.commit
        );
        Ok(summary)
    }

    /// `GET .../commits/{sha}?kind=`.
    async fn commit_output(
        &self,
        repo_path: &str,
        commit: &str,
        kind: ExactResultKind,
    ) -> Result<CommitOutput> {
        let url = self.repo_url(repo_path, &format!("/commits/{commit}?kind={kind}"));
        let resp = self
            .send(self.http.request(reqwest::Method::GET, &url))
            .await?;
        if !resp.status().is_success() {
            return Err(server_error(&self.progress, "build lookup failed", resp).await);
        }
        resp.json()
            .await
            .with_context(|| format!("invalid {kind} build response for {repo_path}"))
    }

    /// Request a build of `selector`, failed outputs included, and wait until
    /// no output is pending.
    pub async fn build_and_wait(&self, repo_path: &str, selector: &str) -> Result<BuildSummary> {
        let summary = self.post_build(repo_path, selector, true).await?;
        self.wait_for_build(repo_path, summary).await
    }

    /// Poll a build until no output is pending. Fails when an output failed
    /// or the poll budget runs out.
    pub async fn wait_for_build(
        &self,
        repo_path: &str,
        summary: BuildSummary,
    ) -> Result<BuildSummary> {
        let client = Self {
            progress: self.progress.fresh(),
            ..self.clone()
        };
        let _drawing = client.progress.draw();
        client.poll_build(repo_path, summary).await
    }

    async fn poll_build(&self, repo_path: &str, mut summary: BuildSummary) -> Result<BuildSummary> {
        self.progress
            .phase(format!("waiting for {}", &summary.commit[..7]));
        let mut attempt = 0;
        while summary.pending() {
            self.progress.detail(summary.states());
            if attempt == MAX_POLLS {
                anyhow::bail!(
                    "{repo_path} is still building {} after {MAX_POLLS} polls; run the command again to keep waiting",
                    summary.commit
                );
            }
            tokio::time::sleep(poll_delay(attempt)).await;
            attempt += 1;
            let checkout_name = summary.checkout_name.take();
            summary = BuildSummary {
                checkout_name,
                ..self.build_summary(repo_path, &summary.commit).await?
            };
        }
        if let Some((kind, error)) = summary.failed() {
            return Err(failed_output_error(&summary.commit, kind, error));
        }
        Ok(summary)
    }

    /// `POST .../add`: register the repository and admit its HEAD.
    pub async fn add_repo(&self, repo_path: &str) -> Result<BuildSummary> {
        let url = self.repo_url(repo_path, "/add");
        let resp = self
            .send(self.http.request(reqwest::Method::POST, &url))
            .await?;
        if !resp.status().is_success() {
            return Err(server_error(&self.progress, "add failed", resp).await);
        }
        let summary: BuildSummary = resp
            .json()
            .await
            .with_context(|| format!("invalid add response for {repo_path}"))?;
        validate_summary(&summary, repo_path)?;
        Ok(summary)
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
        if opted_out() {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_body(commit: &str, kind: &str) -> serde_json::Value {
        serde_json::json!({
            "state": "ready",
            "kind": kind,
            "owner": "acme",
            "repo": "app",
            "provider": "github",
            "host": "github.com",
            "origin_url": "https://github.com/acme/app.git",
            "commit": commit,
            "parent_commit": null,
            "clonepack_manifest": "m",
            "metadata_chunk": "d",
        })
    }

    /// The client reads each state of `GET .../commits/{sha}?kind=`.
    #[test]
    fn commit_output_parses_each_state() {
        let (a, b) = ("a".repeat(40), "b".repeat(40));
        let ready: CommitOutput = serde_json::from_value(ready_body(&a, "full")).unwrap();
        let CommitOutput::Ready(info) = ready else {
            panic!("ready body")
        };
        assert_eq!(info.commit, a);
        assert_eq!(info.result, ExactResultKind::Full);
        validate_output(&info, &a, ExactResultKind::Full, "acme/app").unwrap();
        assert!(validate_output(&info, &b, ExactResultKind::Full, "acme/app").is_err());
        assert!(validate_output(&info, &a, ExactResultKind::Head, "acme/app").is_err());

        let pending: CommitOutput = serde_json::from_value(serde_json::json!({
            "state": "pending",
            "commit": b,
            "kind": "full",
            "attempts": 1,
            "error": "upstream timed out",
            "top_up": {
                "base_commit": a,
                "base": ready_body(&a, "full"),
            },
        }))
        .unwrap();
        let CommitOutput::Pending { commit, top_up } = pending else {
            panic!("pending body")
        };
        assert_eq!(commit, b);
        let plan = top_up.unwrap();
        assert_eq!(plan.base_commit, a);
        assert_eq!(plan.base.commit, a);

        let failed: CommitOutput = serde_json::from_value(serde_json::json!({
            "state": "failed", "commit": b, "kind": "files", "error": "archive broke",
        }))
        .unwrap();
        assert!(matches!(failed, CommitOutput::Failed { error, .. } if error == "archive broke"));
        let message = failed_output_error(&b, ExactResultKind::Files, "archive broke").to_string();
        assert!(message.contains("archive broke"), "{message}");
        assert!(message.contains("ripclone sync"), "{message}");
    }

    #[test]
    fn build_summary_reports_pending_ready_and_failed() {
        let summary = |head: &str, files: &str, full: &str| -> BuildSummary {
            let out = |state: &str| {
                if state == "failed" {
                    serde_json::json!({"state": state, "error": "boom"})
                } else {
                    serde_json::json!({"state": state})
                }
            };
            serde_json::from_value(serde_json::json!({
                "commit": "a".repeat(40),
                "checkout_name": "main",
                "head": out(head),
                "files": out(files),
                "full": out(full),
            }))
            .unwrap()
        };
        let pending = summary("ready", "pending", "failed");
        assert!(pending.pending());
        assert!(!pending.ready());
        assert_eq!(pending.failed(), Some((ExactResultKind::Full, "boom")));
        assert_eq!(pending.checkout_name.as_deref(), Some("main"));
        let ready = summary("ready", "ready", "ready");
        assert!(!ready.pending());
        assert!(ready.ready());
        assert_eq!(ready.failed(), None);
    }
}
