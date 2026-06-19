use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Lightweight in-memory metrics for the ripclone server.
///
/// These are intentionally simple (counters and time sums) so the server can
/// expose them without pulling in a full metrics stack. A production deployment
/// can scrape `/metrics` and forward to Prometheus/etc.
#[derive(Default)]
pub struct Metrics {
    ref_lookups: AtomicU64,
    syncs: AtomicU64,
    sync_duration_ms_total: AtomicU64,
    artifact_requests: AtomicU64,
    artifact_bytes_served: AtomicU64,
    errors: AtomicU64,
    retention_runs: AtomicU64,
    retention_evicted_bytes: AtomicU64,
    retention_evicted_objects: AtomicU64,
    retention_errors: AtomicU64,
    builds_queued: AtomicU64,
    builds_completed: AtomicU64,
    builds_failed: AtomicU64,
    build_duration_ms_total: AtomicU64,
    build_queue_depth: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_ref_lookup(&self) {
        self.ref_lookups.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sync(&self, duration: std::time::Duration) {
        self.syncs.fetch_add(1, Ordering::Relaxed);
        self.sync_duration_ms_total
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_artifact_request(&self, bytes: u64) {
        self.artifact_requests.fetch_add(1, Ordering::Relaxed);
        self.artifact_bytes_served
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retention_run(&self, evicted_bytes: u64, evicted_objects: u64) {
        self.retention_runs.fetch_add(1, Ordering::Relaxed);
        self.retention_evicted_bytes
            .fetch_add(evicted_bytes, Ordering::Relaxed);
        self.retention_evicted_objects
            .fetch_add(evicted_objects, Ordering::Relaxed);
    }

    pub fn record_retention_error(&self) {
        self.retention_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the queue-depth gauge when a job is accepted into (or about
    /// to enter) the queue.
    pub fn record_build_queued(&self) {
        self.build_queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    /// Roll back a queue-depth increment when the queue is full and the job is
    /// rejected.
    pub fn record_build_rejected(&self) {
        self.build_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record that a job was actually enqueued successfully.
    pub fn record_build_accepted(&self) {
        self.builds_queued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_build_completed(&self, duration: std::time::Duration) {
        self.builds_completed.fetch_add(1, Ordering::Relaxed);
        self.build_duration_ms_total
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
        self.build_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_build_failed(&self) {
        self.builds_failed.fetch_add(1, Ordering::Relaxed);
        self.build_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let syncs = self.syncs.load(Ordering::Relaxed);
        let sync_ms = self.sync_duration_ms_total.load(Ordering::Relaxed);
        let builds_completed = self.builds_completed.load(Ordering::Relaxed);
        let build_ms = self.build_duration_ms_total.load(Ordering::Relaxed);
        MetricsSnapshot {
            ref_lookups: self.ref_lookups.load(Ordering::Relaxed),
            syncs,
            sync_avg_ms: if syncs == 0 { 0 } else { sync_ms / syncs },
            sync_duration_ms_total: sync_ms,
            artifact_requests: self.artifact_requests.load(Ordering::Relaxed),
            artifact_bytes_served: self.artifact_bytes_served.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            retention_runs: self.retention_runs.load(Ordering::Relaxed),
            retention_evicted_bytes: self.retention_evicted_bytes.load(Ordering::Relaxed),
            retention_evicted_objects: self.retention_evicted_objects.load(Ordering::Relaxed),
            retention_errors: self.retention_errors.load(Ordering::Relaxed),
            builds_queued: self.builds_queued.load(Ordering::Relaxed),
            builds_completed,
            build_avg_ms: if builds_completed == 0 {
                0
            } else {
                build_ms / builds_completed
            },
            build_duration_ms_total: build_ms,
            builds_failed: self.builds_failed.load(Ordering::Relaxed),
            build_queue_depth: self.build_queue_depth.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
pub struct MetricsSnapshot {
    pub ref_lookups: u64,
    pub syncs: u64,
    pub sync_avg_ms: u64,
    pub sync_duration_ms_total: u64,
    pub artifact_requests: u64,
    pub artifact_bytes_served: u64,
    pub errors: u64,
    pub retention_runs: u64,
    pub retention_evicted_bytes: u64,
    pub retention_evicted_objects: u64,
    pub retention_errors: u64,
    pub builds_queued: u64,
    pub builds_completed: u64,
    pub build_avg_ms: u64,
    pub build_duration_ms_total: u64,
    pub builds_failed: u64,
    pub build_queue_depth: u64,
}
