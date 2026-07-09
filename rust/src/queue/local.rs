//! In-process queue: today's bounded `tokio::mpsc` channel behind the
//! [`JobQueue`] trait. Builder and waiter share a process, so `/sync` can be
//! signalled directly via an in-process oneshot (`inproc_wait() == true`).

use super::{BuildJob, EnqueueOutcome, Enqueued, JobQueue};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

pub struct LocalJobQueue {
    tx: mpsc::Sender<BuildJob>,
    /// Shared with `ServerState.build_queue_depth`: incremented here on enqueue,
    /// decremented by the worker loop when a job finishes.
    depth: Arc<AtomicUsize>,
}

impl LocalJobQueue {
    /// Create the queue and return its receiver (handed to the in-process worker
    /// loop) plus the shared depth counter (also stored on `ServerState`).
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<BuildJob>, Arc<AtomicUsize>) {
        let (tx, rx) = mpsc::channel(capacity);
        let depth = Arc::new(AtomicUsize::new(0));
        (
            Self {
                tx,
                depth: depth.clone(),
            },
            rx,
            depth,
        )
    }
}

#[async_trait]
impl JobQueue for LocalJobQueue {
    async fn enqueue(&self, job: BuildJob) -> Result<Enqueued> {
        // Coalescing for the local path is handled by the caller's
        // `build_waiters` (only the first waiter enqueues), so a plain
        // non-blocking send is correct here. Completion is signalled in-process
        // via the oneshot, so there is no job id to poll.
        self.depth.fetch_add(1, Ordering::Relaxed);
        let outcome = match self.tx.try_send(job) {
            Ok(()) => EnqueueOutcome::Enqueued,
            Err(_) => {
                self.depth.fetch_sub(1, Ordering::Relaxed);
                EnqueueOutcome::Full
            }
        };
        Ok(Enqueued {
            outcome,
            job_id: None,
        })
    }

    async fn depth(&self) -> usize {
        self.depth.load(Ordering::Relaxed)
    }

    fn inproc_wait(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> BuildJob {
        BuildJob {
            repo_id: crate::provider::RepoId::github("o/r"),
            branch: "b".into(),
            rev: None,
            credential: None,
            recheck: 0,
            size_bytes: None,
        }
    }

    #[tokio::test]
    async fn enqueue_reports_full_when_capacity_exhausted() {
        // Hold the receiver so nothing drains; capacity 1 buffers exactly one job.
        let (q, _rx, _depth) = LocalJobQueue::new(1);
        assert_eq!(
            q.enqueue(job()).await.unwrap().outcome,
            EnqueueOutcome::Enqueued
        );
        // Second send has nowhere to go → Full, with depth rolled back.
        assert_eq!(
            q.enqueue(job()).await.unwrap().outcome,
            EnqueueOutcome::Full
        );
        assert_eq!(q.depth().await, 1, "Full enqueue must not inflate depth");
    }
}
