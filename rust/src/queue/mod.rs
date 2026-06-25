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
use std::sync::Arc;

pub mod libsql_db;
pub mod local;
pub mod mysql_db;
pub mod postgres_db;
pub mod sql;
pub mod sqlite_db;

pub use libsql_db::LibsqlDb;
pub use local::LocalJobQueue;
pub use mysql_db::MysqlDb;
pub use postgres_db::PostgresDb;
pub use sql::SqlJobQueue;
pub use sqlite_db::SqliteDb;

/// A request to build (sync) one repo's branch.
#[derive(Clone)]
pub struct BuildJob {
    pub repo_id: RepoId,
    pub branch: String,
    /// Optional build-commit override (see `SyncRequest.rev`). Only honored on
    /// the in-process [`LocalJobQueue`]; the cross-process [`SqlJobQueue`] builds
    /// the branch tip (rev is not persisted).
    pub rev: Option<String>,
    /// Upstream credential (Tier-B passthrough) for the mirror fetch. Only
    /// carried in-process via [`LocalJobQueue`]; the cross-process
    /// [`SqlJobQueue`] never persists credentials — its worker resolves its own
    /// via the credential broker.
    pub credential: Option<secrecy::SecretString>,
}

impl BuildJob {
    /// Coalescing key: concurrent syncs for the same key collapse to one build.
    /// Uses the repo's storage key (back-compat `owner/repo` for GitHub) plus the
    /// branch, so it is stable across processes. Slash-joined (not NUL-joined):
    /// some SQL engines (Postgres) reject `\0` in TEXT columns.
    pub fn key(&self) -> String {
        format!("{}/{}", self.repo_id.storage_key(), self.branch)
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
