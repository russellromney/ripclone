//! Pluggable sync-task queue.
//!
//! The server enqueues a [`BuildJob`] when a `/sync` arrives; a worker dequeues
//! it and runs the build. The default [`LocalJobQueue`] is an in-process channel
//! (builder and waiter share a process). [`SqlJobQueue`] is a cross-process
//! queue (a jobs table) so the build can run in a *separate* `ripclone-worker`
//! process — on another machine, a Fly Machine, a Lambda, etc.
//!
//! A worker is stateless: all durable state lives in the `StorageBackend`
//! (artifacts) and the `RefStore` (metadata). The local bare git mirror is
//! rebuildable scratch. That is what makes the queue safe to farm out.

use crate::provider::RepoId;
use anyhow::Result;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;

pub mod libsql_db;
pub mod local;
pub mod mysql_db;
pub mod postgres_db;
pub mod size_class;
pub mod sql;
pub mod sqlite_db;

pub use libsql_db::LibsqlDb;
pub use local::LocalJobQueue;
pub use mysql_db::MysqlDb;
pub use postgres_db::PostgresDb;
pub use size_class::{
    SizeClass, classify_rank, default_size_classes, load_size_classes, prior_clonepack_bytes,
    resolve_job_size_bytes,
};
pub use sql::{
    ClaimedJob, DEFAULT_HEARTBEAT_TIMEOUT_SECS, DeadLetteredInitialization, SqlJobQueue,
    make_worker_id, make_worker_id_parts, validate_heartbeat_timing, worker_heartbeat_enabled,
    worker_heartbeat_enabled_from_env, worker_heartbeat_interval_secs,
    worker_heartbeat_interval_secs_from,
};
pub use sqlite_db::SqliteDb;

/// A request to build (sync) one repo's branch.
#[derive(Clone)]
pub struct BuildJob {
    pub repo_id: RepoId,
    pub branch: String,
    /// Immutable repo-admission attempt that authorized this initialization
    /// build. Ordinary syncs carry `None` and may never mutate admission state.
    pub initialization_attempt_id: Option<String>,
    /// Optional build-commit override (see `SyncRequest.rev`). Only honored on
    /// the in-process [`LocalJobQueue`]; the cross-process [`SqlJobQueue`] builds
    /// the branch tip (rev is not persisted).
    pub rev: Option<String>,
    /// Upstream credential (Tier-B passthrough) for the mirror fetch. The
    /// in-process [`LocalJobQueue`] carries it directly; [`SqlJobQueue`] stores
    /// an obfuscated copy long enough for a cross-process worker to claim the
    /// job, then clears it on claim or finish.
    pub credential: Option<secrecy::SecretString>,
    /// How many consecutive post-build freshness re-checks led to this job. The
    /// post-build re-check stops once this reaches `RIPCLONE_RECHECK_MAX`, so on a
    /// single box one repo pushing faster than it builds can't pin the worker.
    /// Only carried in-process; the cross-process [`SqlJobQueue`] does not persist
    /// it (like `rev`), so there the chain is not capped — but it is
    /// bounded by the real push rate (each re-trigger builds a genuinely newer tip,
    /// not a spin) and spread across the worker pool, with the poller as backstop.
    pub recheck: u32,
    /// Byte size used to classify into a [`size_class`](size_class) rank at
    /// enqueue on the SQL queue. First build → repo size from the tiered-add
    /// preflight; re-sync → prior clonepack byte total. `None` maps to the
    /// largest configured class so a first build is never under-sized.
    /// The in-process queue ignores this (single-worker, no claim filter).
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
    /// Coalescing key: concurrent syncs for the same key collapse to one build.
    /// Every variable component is byte-length-prefixed in the hash input, so
    /// legal branch names cannot impersonate an admission suffix. Hashing keeps
    /// the SQL key fixed-size even at maximum legal component lengths. The
    /// version prefix permits future encoding changes.
    pub fn key(&self) -> String {
        let repo = self.repo_id.storage_key();
        let mut digest = Sha256::new();
        for component in [repo.as_bytes(), self.branch.as_bytes()] {
            digest.update((component.len() as u64).to_be_bytes());
            digest.update(component);
        }
        match self.initialization_attempt_id.as_deref() {
            Some(attempt_id) => {
                digest.update([1]);
                digest.update((attempt_id.len() as u64).to_be_bytes());
                digest.update(attempt_id.as_bytes());
            }
            None => digest.update([0]),
        }
        format!("v2:{}", hex::encode(digest.finalize()))
    }
}

#[cfg(test)]
mod build_job_key_tests {
    use super::*;

    fn job(branch: &str, attempt: Option<&str>) -> BuildJob {
        BuildJob {
            repo_id: RepoId::github("owner/repo"),
            branch: branch.to_string(),
            initialization_attempt_id: attempt.map(str::to_string),
            rev: None,
            credential: None,
            recheck: 0,
            size_bytes: None,
        }
    }

    #[test]
    fn admission_key_cannot_collide_with_legal_ordinary_branch() {
        let admission = job("main", Some("X"));
        let ordinary = job("main/admission-attempt/X", None);
        assert_ne!(admission.key(), ordinary.key());
        assert_ne!(admission.key(), job("main", Some("X:a0")).key());
        assert_eq!(admission.key(), job("main", Some("X")).key());
        let maximal = job(&"b".repeat(255), Some(&"a".repeat(1024))).key();
        assert_eq!(maximal.len(), 67);
        assert!(maximal.is_ascii());
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
    /// Handle to poll via [`JobQueue::job_status`]. `None` for the in-process
    /// queue, where `/sync` waits on an in-process oneshot instead.
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
    /// The queue can't report on this id (e.g. the in-process queue).
    Unknown,
}

/// Abstract sync-task queue.
#[async_trait]
pub trait JobQueue: Send + Sync {
    /// Durably enqueue (or dispatch) a build job, coalescing by [`BuildJob::key`]
    /// so concurrent `/sync` for the same key produce a single build.
    async fn enqueue(&self, job: BuildJob) -> Result<Enqueued>;

    /// Poll a job's lifecycle. Cross-process queues use this so `/sync` can
    /// observe completion of a build running in another process. The default
    /// (used by the in-process queue) reports [`JobState::Unknown`].
    async fn job_status(&self, _job_id: JobId) -> Result<JobState> {
        Ok(JobState::Unknown)
    }

    /// Best-effort count of queued (not-yet-running) jobs, for metrics and
    /// backpressure reporting.
    async fn depth(&self) -> usize;

    /// True when build completion is signalled to waiters *in this process*
    /// (the in-process [`LocalJobQueue`]). When false, `/sync` must observe
    /// completion by polling [`job_status`](JobQueue::job_status), because the
    /// build runs in another process.
    fn inproc_wait(&self) -> bool {
        false
    }
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
/// cleanly and the dispatcher respawns it with a fresh token.
///
/// [`JobQueue`] is a supertrait, so `job_status` (used after `ack` to detect a
/// dead-letter) is inherited from it — not redeclared here. Declaring it on both
/// traits would make `q.job_status(..)` ambiguous once both are in scope.
#[async_trait]
pub trait WorkerQueue: JobQueue {
    async fn reclaim_stale_initializations(&self) -> Result<Vec<DeadLetteredInitialization>> {
        Ok(Vec::new())
    }

    async fn acknowledge_dead_lettered_initialization(
        &self,
        _id: JobId,
        _attempt_id: &str,
    ) -> Result<()> {
        Ok(())
    }

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
