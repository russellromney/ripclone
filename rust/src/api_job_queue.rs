//! `WorkerQueue` that claims/acks/heartbeats over the server's HTTP API.
//!
//! Selected with `RIPCLONE_QUEUE=api`. A farmed-out worker holds only a base URL
//! and a signed, expiring bearer token — never database credentials. It reaches
//! the queue entirely through the server's `/v1/jobs/*` endpoints; the server
//! holds the one queue database and performs every state change after checking
//! the token. This is the queue-side twin of [`ApiRefStore`](crate::api_ref_store)
//! (metadata over `POST /v1/refs`): together they let a worker run on untrusted
//! infra with a single token as its whole credential.
//!
//! Wire shapes:
//! - `POST /v1/jobs/claim` — body `{worker_id, max_size_class?}` → `{job?}`
//!   where `job = {id, provider, path, branch, credential?}`. Exactly one job
//!   (or none), scoped to the caller; `credential` is this job's upstream token.
//! - `POST /v1/jobs/{id}/ack` — body `{worker_id, result:{ok, retryable, error?}}`
//!   → `{settled, state, error?}`.
//! - `POST /v1/jobs/heartbeat` — body `{worker_id, current_job?}` → 200.
//!
//! A failed claim/ack/heartbeat is never swallowed: network / 5xx / 429 map to a
//! retryable [`ApiReportError`] (the worker polls again / the job stays queued),
//! and a 401/403 maps to an *unauthorized* error so the worker exits cleanly and
//! the dispatcher respawns it with a fresh token.

use crate::api_ref_store::ApiReportError;
use crate::queue::{
    BuildError, BuildJob, ClaimedJob, DEFAULT_HEARTBEAT_TIMEOUT_SECS, Enqueued, JobId, JobQueue,
    JobState, WorkerQueue,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::info;

/// Claim request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub worker_id: String,
    /// The worker's size-class ceiling name (`--max-size-class`), applied by the
    /// server per claim. `None` claims anything.
    #[serde(default)]
    pub max_size_class: Option<String>,
}

/// One claimed job on the wire. `credential` is the per-job upstream token the
/// enqueuer persisted; the worker needs it to fetch a private repo it has no
/// standing credential for. Sent only to the worker that just claimed this job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedJobWire {
    pub id: JobId,
    pub provider: String,
    pub path: String,
    pub branch: String,
    #[serde(default)]
    pub credential: Option<String>,
    #[serde(default)]
    pub initialization_attempt_id: Option<String>,
}

/// Claim response: exactly the one claimed job, or `None` when the queue is empty.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClaimResponse {
    #[serde(default)]
    pub job: Option<ClaimedJobWire>,
}

/// The worker's build outcome, sent to the ack endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckResult {
    pub ok: bool,
    /// Meaningful only when `!ok`: whether the failure is retryable (requeue) or
    /// permanent (terminal fail).
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub error: Option<String>,
}

/// Ack request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckRequest {
    pub worker_id: String,
    pub result: AckResult,
}

/// Ack response. `state` is the job's lifecycle after settling (`done` /
/// `failed` / `pending` / `unknown`) so the worker can detect a dead-letter
/// without a second round-trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckResponse {
    pub settled: bool,
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
}

/// Heartbeat request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub worker_id: String,
    #[serde(default)]
    pub current_job: Option<JobId>,
}

/// Serialize a [`JobState`] to its wire tag.
pub fn job_state_tag(state: &JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Done => "done",
        JobState::Failed(_) => "failed",
        JobState::Unknown => "unknown",
    }
}

/// Parse a wire tag (+ optional error) back into a [`JobState`].
pub fn job_state_from_tag(tag: &str, error: Option<String>) -> JobState {
    match tag {
        "done" => JobState::Done,
        "failed" => JobState::Failed(error.unwrap_or_else(|| "build failed".to_string())),
        "pending" => JobState::Pending,
        _ => JobState::Unknown,
    }
}

/// Worker-side queue that talks HTTP to the server. Holds a base URL + bearer
/// token and no DB credentials.
pub struct ApiJobQueue {
    base_url: String,
    job_token: String,
    client: reqwest::Client,
    heartbeat_timeout_secs: i64,
    /// Last known lifecycle per job id, populated on `ack` so a follow-up
    /// `job_status` (the dead-letter check) needs no extra round-trip.
    last_status: Mutex<HashMap<JobId, JobState>>,
}

/// Manual `Debug` that REDACTS the bearer token — it is the worker's whole
/// credential and must never land in logs. Do not `#[derive(Debug)]` (it would
/// print the token).
impl std::fmt::Debug for ApiJobQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiJobQueue")
            .field("base_url", &self.base_url)
            .field("job_token", &"<redacted>")
            .field("heartbeat_timeout_secs", &self.heartbeat_timeout_secs)
            .finish_non_exhaustive()
    }
}

impl ApiJobQueue {
    /// Build from env: requires `RIPCLONE_QUEUE_API_URL` (the server base URL)
    /// and `RIPCLONE_METADATA_JOB_TOKEN` (the one worker bearer token, shared
    /// with the `api` metadata path). Fails loudly if either is missing.
    pub fn from_env() -> Result<Self> {
        let base_url = std::env::var("RIPCLONE_QUEUE_API_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .context(
                "RIPCLONE_QUEUE=api requires RIPCLONE_QUEUE_API_URL \
                 (the server base URL that serves POST /v1/jobs/*)",
            )?;
        let job_token = std::env::var("RIPCLONE_METADATA_JOB_TOKEN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .context(
                "RIPCLONE_QUEUE=api requires RIPCLONE_METADATA_JOB_TOKEN \
                 (the one signed, expiring bearer token for all worker endpoints)",
            )?;
        let timeout = std::env::var("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT_SECS);
        Self::new(base_url, job_token, timeout)
    }

    pub fn new(
        base_url: impl Into<String>,
        job_token: impl Into<String>,
        heartbeat_timeout_secs: i64,
    ) -> Result<Self> {
        let base_url = base_url.into();
        let job_token = job_token.into();
        // Store the base without a trailing slash so path joins are clean.
        let base_url = base_url.trim_end_matches('/').to_string();
        if base_url.trim().is_empty() {
            bail!("RIPCLONE_QUEUE_API_URL must not be empty");
        }
        if job_token.trim().is_empty() {
            bail!("RIPCLONE_METADATA_JOB_TOKEN must not be empty");
        }
        if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
            bail!("RIPCLONE_QUEUE_API_URL must be http(s), got {base_url:?}");
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("build HTTP client for ApiJobQueue")?;
        info!(url = %base_url, "queue: api (claim/ack/heartbeat over HTTP)");
        Ok(Self {
            base_url,
            job_token,
            client,
            heartbeat_timeout_secs: heartbeat_timeout_secs.max(1),
            last_status: Mutex::new(HashMap::new()),
        })
    }

    /// POST `body` to `path` and deserialize the JSON response. Maps transport /
    /// status failures to a retryable / unauthorized / permanent
    /// [`ApiReportError`] so nothing is silently dropped.
    async fn post<B: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.job_token))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|e| ApiReportError::retryable(format!("queue POST {url}: {e}")))?;
        let status = resp.status();
        if status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            if text.trim().is_empty() {
                // A 200 with no body: let the caller's `R` default in via serde
                // by parsing `null`. Callers that expect a body use non-empty.
                return serde_json::from_str::<R>("null").map_err(|e| {
                    ApiReportError::permanent(format!(
                        "queue {url}: empty body, expected JSON: {e}"
                    ))
                    .into()
                });
            }
            serde_json::from_str::<R>(&text).map_err(|e| {
                ApiReportError::permanent(format!("queue {url}: bad JSON response: {e}")).into()
            })
        } else if status.as_u16() == 401 || status.as_u16() == 403 {
            let body = resp.text().await.unwrap_or_default();
            Err(ApiReportError::unauthorized(format!(
                "queue {url} unauthorized ({status}): {body}"
            ))
            .into())
        } else if status.is_server_error() || status.as_u16() == 429 {
            let body = resp.text().await.unwrap_or_default();
            Err(
                ApiReportError::retryable(format!("queue {url} retryable HTTP {status}: {body}"))
                    .into(),
            )
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(
                ApiReportError::permanent(format!("queue {url} rejected ({status}): {body}"))
                    .into(),
            )
        }
    }
}

#[async_trait]
impl WorkerQueue for ApiJobQueue {
    async fn claim(&self, worker_id: &str) -> Result<Option<ClaimedJob>> {
        // The server applies this worker's ceiling per claim. We don't carry a
        // resolved rank here; the name is enough and the server owns the classes.
        let max_size_class = std::env::var("RIPCLONE_MAX_SIZE_CLASS")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let req = ClaimRequest {
            worker_id: worker_id.to_string(),
            max_size_class,
        };
        let resp: ClaimResponse = self.post("/v1/jobs/claim", &req).await?;
        Ok(resp.job.map(|j| ClaimedJob {
            id: j.id,
            provider: j.provider,
            path: j.path,
            branch: j.branch,
            credential: j.credential.map(|c| SecretString::new(c.into())),
            initialization_attempt_id: j.initialization_attempt_id,
        }))
    }

    async fn ack(
        &self,
        id: JobId,
        worker_id: &str,
        result: Result<(), BuildError>,
    ) -> Result<bool> {
        let ack = match &result {
            Ok(()) => AckResult {
                ok: true,
                retryable: false,
                error: None,
            },
            Err(e) => AckResult {
                ok: false,
                retryable: e.is_retryable(),
                error: Some(e.message().to_string()),
            },
        };
        let req = AckRequest {
            worker_id: worker_id.to_string(),
            result: ack,
        };
        let resp: AckResponse = self.post(&format!("/v1/jobs/{id}/ack"), &req).await?;
        // Cache the post-ack lifecycle so job_status (the dead-letter check that
        // runs right after ack) needs no second round-trip.
        let state = job_state_from_tag(&resp.state, resp.error);
        self.last_status.lock().await.insert(id, state);
        Ok(resp.settled)
    }

    async fn heartbeat(&self, worker_id: &str, current_job: Option<JobId>) -> Result<()> {
        let req = HeartbeatRequest {
            worker_id: worker_id.to_string(),
            current_job,
        };
        // 200 with an empty body is fine; deserialize into unit via `null`.
        let _: Option<serde_json::Value> = self.post("/v1/jobs/heartbeat", &req).await?;
        Ok(())
    }

    async fn prune_failed(&self) -> Result<u64> {
        // Pruning is a server-side concern (the server owns the DB and prunes on
        // its own timers). A farm-out worker has no DB and no prune endpoint.
        Ok(0)
    }

    // `job_status` is provided by the `JobQueue` supertrait impl below (reads the
    // post-ack cache), so it is not redeclared here.

    fn supports_worker_registry(&self) -> bool {
        // The server decides whether its queue backend has a registry; the api
        // worker optimistically heartbeats and the endpoint fails loudly if not.
        true
    }

    fn heartbeat_timeout_secs(&self) -> i64 {
        self.heartbeat_timeout_secs
    }
}

/// The api worker's `ServerState.build_queue`. All durable enqueue/coalesce
/// happens on the server; the only enqueue a worker would attempt is the
/// post-build freshness re-check, and the server's poll loop is that backstop
/// for cross-process queues. So `enqueue` fails loudly rather than pretend.
#[async_trait]
impl JobQueue for ApiJobQueue {
    async fn enqueue(&self, _job: BuildJob) -> Result<Enqueued> {
        bail!(
            "RIPCLONE_QUEUE=api workers do not enqueue; the server enqueues and its \
             periodic poll loop is the freshness backstop for cross-process queues"
        )
    }

    async fn job_status(&self, id: JobId) -> Result<JobState> {
        // The post-ack cache (populated by `WorkerQueue::ack`) is the worker's
        // dead-letter check; there is no job-status endpoint.
        Ok(self
            .last_status
            .lock()
            .await
            .get(&id)
            .cloned()
            .unwrap_or(JobState::Unknown))
    }

    async fn depth(&self) -> usize {
        0
    }

    fn inproc_wait(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn claimed_job_wire_round_trips_admission_attempt_and_reads_legacy_none() {
        let wire = ClaimedJobWire {
            id: 1,
            provider: "github".into(),
            path: "o/r".into(),
            branch: "main".into(),
            credential: None,
            initialization_attempt_id: Some("attempt".into()),
        };
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["initialization_attempt_id"], "attempt");
        let legacy: ClaimedJobWire = serde_json::from_value(serde_json::json!({
            "id": 2, "provider": "github", "path": "o/r", "branch": "main"
        }))
        .unwrap();
        assert_eq!(legacy.initialization_attempt_id, None);
    }

    #[test]
    fn from_env_needs_url_and_token() {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev_url = std::env::var("RIPCLONE_QUEUE_API_URL").ok();
        let prev_tok = std::env::var("RIPCLONE_METADATA_JOB_TOKEN").ok();

        unsafe {
            std::env::remove_var("RIPCLONE_QUEUE_API_URL");
            std::env::set_var("RIPCLONE_METADATA_JOB_TOKEN", "tok");
        }
        let err = ApiJobQueue::from_env().unwrap_err();
        assert!(
            err.to_string().contains("RIPCLONE_QUEUE_API_URL"),
            "got: {err}"
        );

        unsafe {
            std::env::set_var("RIPCLONE_QUEUE_API_URL", "http://127.0.0.1:9");
            std::env::remove_var("RIPCLONE_METADATA_JOB_TOKEN");
        }
        let err = ApiJobQueue::from_env().unwrap_err();
        assert!(
            err.to_string().contains("RIPCLONE_METADATA_JOB_TOKEN"),
            "got: {err}"
        );

        unsafe {
            match prev_url {
                Some(v) => std::env::set_var("RIPCLONE_QUEUE_API_URL", v),
                None => std::env::remove_var("RIPCLONE_QUEUE_API_URL"),
            }
            match prev_tok {
                Some(v) => std::env::set_var("RIPCLONE_METADATA_JOB_TOKEN", v),
                None => std::env::remove_var("RIPCLONE_METADATA_JOB_TOKEN"),
            }
        }
    }

    #[test]
    fn rejects_non_http_url() {
        let err = ApiJobQueue::new("ftp://x", "tok", 60).unwrap_err();
        assert!(err.to_string().contains("http(s)"), "got: {err}");
    }

    #[tokio::test]
    async fn dead_url_claim_is_retryable() {
        let q = ApiJobQueue::new("http://127.0.0.1:1", "tok", 60).unwrap();
        let err = q.claim("w1").await.unwrap_err();
        let api = err
            .downcast_ref::<ApiReportError>()
            .expect("ApiReportError in chain");
        assert!(api.is_retryable(), "network failure must be retryable");
        assert!(!api.is_unauthorized());
    }

    #[tokio::test]
    async fn job_status_defaults_unknown_then_reads_cache() {
        let q = ApiJobQueue::new("http://127.0.0.1:9", "tok", 60).unwrap();
        assert!(matches!(
            JobQueue::job_status(&q, 1).await.unwrap(),
            JobState::Unknown
        ));
        q.last_status.lock().await.insert(1, JobState::Done);
        assert!(matches!(
            JobQueue::job_status(&q, 1).await.unwrap(),
            JobState::Done
        ));
    }

    #[test]
    fn usable_as_both_trait_objects() {
        let q = Arc::new(ApiJobQueue::new("http://127.0.0.1:9", "t", 60).unwrap());
        let _wq: Arc<dyn WorkerQueue> = q.clone();
        let _jq: Arc<dyn JobQueue> = q;
    }
}
