//! Durable sync-task queue stored in the server-owned control database.
//!
//! The embedded worker claims this table directly through the server's shared
//! database handle. Standalone workers claim through the authenticated API and
//! never receive a database credential.

use crate::provider::RepoId;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;

pub mod libsql_db;
pub mod size_class;
pub mod sql;

pub use libsql_db::LibsqlDb;
pub use size_class::{
    SizeClass, classify_rank, default_size_classes, load_size_classes, prior_clonepack_bytes,
    resolve_job_size_bytes,
};
pub use sql::{
    ClaimedJob, DEFAULT_HEARTBEAT_TIMEOUT_SECS, SqlJobQueue, make_worker_id, make_worker_id_parts,
    validate_heartbeat_timing, worker_heartbeat_enabled, worker_heartbeat_enabled_from_env,
    worker_heartbeat_interval_secs, worker_heartbeat_interval_secs_from,
};

/// A request to build one repository at one admitted commit.
#[derive(Clone)]
pub struct BuildJob {
    pub repo_id: RepoId,
    /// Exact commit admitted for this build. Every selector is resolved before
    /// enqueue, so ordinary and explicit requests for the same result coalesce.
    pub admitted_commit: String,
    /// Optional source ref used only as a fetch hint. Workers still verify and
    /// build `admitted_commit`; this value is never result or job identity.
    pub source_ref: Option<String>,
    /// Validated repository build settings captured by the server at
    /// admission. Workers use this immutable snapshot and never read live
    /// repository configuration.
    pub repo_config: crate::repo_config::RepoConfig,
    /// Upstream credential (Tier-B passthrough) for the mirror fetch. The jobs
    /// table stores an obfuscated copy until claim or finish.
    pub credential: Option<secrecy::SecretString>,
    /// Byte size used to classify into a [`size_class`](size_class) rank at
    /// enqueue on the SQL queue. First build → repo size from the tiered-add
    /// preflight; re-sync → prior clonepack byte total. `None` maps to the
    /// largest configured class so a first build is never under-sized.
    pub size_bytes: Option<u64>,
}

/// Error returned by a build worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildError {
    message: String,
    retryable: bool,
}

impl BuildError {
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for BuildError {}

impl BuildJob {
    /// Coalescing key for one immutable exact result.
    pub fn key(&self) -> String {
        format!("{}\x1f{}", self.repo_id.storage_key(), self.admitted_commit)
    }
}

/// Disposition of an enqueue attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// A new job was queued (or dispatched).
    Enqueued,
    /// An equivalent job for this key is already active; folded into it.
    Coalesced,
    /// The queue is at capacity; the caller should back off (HTTP 503).
    Full,
}

/// Identifier for a queued job, used to poll its completion across processes.
pub type JobId = i64;

/// Result of [`JobQueue::enqueue`].
#[derive(Debug, Clone, Copy)]
pub struct Enqueued {
    pub outcome: EnqueueOutcome,
    /// Durable handle to poll via [`JobQueue::job_status`].
    pub job_id: Option<JobId>,
}

/// Lifecycle of a specific job, as seen by a poller.
#[derive(Debug, Clone)]
pub enum JobState {
    /// Queued or running — not finished yet.
    Pending,
    /// Built successfully; the metadata store now has the fresh ref.
    Done,
    /// Build failed, with the error message.
    Failed(String),
    /// The database no longer has this retained id.
    Unknown,
}

/// Abstract sync-task queue.
#[async_trait]
pub trait JobQueue: Send + Sync {
    /// Durably enqueue (or dispatch) a build job, coalescing by [`BuildJob::key`]
    /// so concurrent `/sync` for the same key produce a single build.
    async fn enqueue(&self, job: BuildJob) -> Result<Enqueued>;

    /// Poll a durable job's lifecycle.
    async fn job_status(&self, _job_id: JobId) -> Result<JobState> {
        Ok(JobState::Unknown)
    }

    /// Best-effort count of queued (not-yet-running) jobs, for metrics and
    /// backpressure reporting.
    async fn depth(&self) -> usize;
}

pub type JobQueueRef = Arc<dyn JobQueue>;

/// The worker-facing side of the queue: claim a job, settle it, heartbeat.
///
/// A `ripclone-worker` drives its loop through this trait so it is generic over
/// *how* it reaches the queue. Two impls exist:
/// - [`SqlJobQueue`] — a direct SQL connection. The trusted single-box server
///   and its co-located workers use this (no HTTP hop forced).
/// - [`ApiJobQueue`](crate::api_job_queue::ApiJobQueue) — HTTP to the server's
///   `/v1/jobs/*` endpoints with a bearer token and **no** DB credentials. This
///   is the farm-out path: workers run on untrusted infra holding only a token.
///
/// A failed `claim`/`ack`/`heartbeat` returns an error the worker must not
/// swallow (a silent success would drop the build result). For the API impl an
/// expired-token (401) error is flagged via
/// [`ApiReportError`](crate::api_ref_store::ApiReportError) so the worker exits
/// cleanly without attempting to refresh credentials locally.
///
/// [`JobQueue`] is a supertrait, so `job_status` (used after `ack` to detect a
/// dead-letter) is inherited from it — not redeclared here. Declaring it on both
/// traits would make `q.job_status(..)` ambiguous once both are in scope.
#[async_trait]
pub trait WorkerQueue: JobQueue {
    /// Claim the oldest eligible queued job for this worker, or `None` when the
    /// queue is empty. Returns **exactly one** job, scoped to this caller.
    async fn claim(&self, worker_id: &str) -> Result<Option<ClaimedJob>>;

    /// Settle a claimed job. `Ok(true)` when it settled/requeued, `Ok(false)`
    /// when the claim was reclaimed out from under this worker.
    async fn ack(&self, id: JobId, worker_id: &str, result: Result<(), BuildError>)
    -> Result<bool>;

    /// Refresh this worker's registry row. `current_job` is the claimed job id
    /// (or `None` when idle) so an autoscaler can count live workers.
    async fn heartbeat(&self, worker_id: &str, current_job: Option<JobId>) -> Result<()>;

    /// Prune expired `failed` jobs. Returns rows removed.
    async fn prune_failed(&self) -> Result<u64>;

    /// Whether the backing queue has a workers registry (heartbeat support).
    fn supports_worker_registry(&self) -> bool;

    /// Soft age-out window for the live-worker count.
    fn heartbeat_timeout_secs(&self) -> i64;
}

pub type WorkerQueueRef = Arc<dyn WorkerQueue>;
