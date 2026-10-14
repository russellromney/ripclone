//! Durable sync-task queue stored in the server-owned control database.
//!
//! The embedded worker claims this table directly through the server's shared
//! database handle.

use crate::provider::RepoId;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;

pub mod libsql_db;
pub mod sql;

pub use libsql_db::LibsqlDb;
pub use sql::{ClaimedJob, SqlJobQueue};

/// A request to build one repository at one admitted commit.
#[derive(Clone)]
pub struct BuildJob {
    pub repo_id: RepoId,
    /// Exact commit admitted for this build. Every selector is resolved before
    /// enqueue, so ordinary and explicit requests for the same result coalesce.
    pub admitted_commit: String,
    /// Validated repository build settings captured by the server at
    /// admission. Workers use this immutable snapshot and never read live
    /// repository configuration.
    pub repo_config: crate::repo_config::RepoConfig,
    /// Upstream credential (Tier-B passthrough) for the mirror fetch. The jobs
    /// table stores an obfuscated copy until claim or finish.
    pub credential: Option<secrecy::SecretString>,
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

    /// Lifecycle of the newest job for one exact-result key.
    async fn job_state_for_key(&self, _key: &str) -> Result<JobState> {
        Ok(JobState::Unknown)
    }

    /// Best-effort count of queued (not-yet-running) jobs, for metrics and
    /// backpressure reporting.
    async fn depth(&self) -> usize;
}

pub type JobQueueRef = Arc<dyn JobQueue>;
