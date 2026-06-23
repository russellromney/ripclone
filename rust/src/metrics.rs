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
        Self::dec_saturating(&self.build_queue_depth);
    }

    /// Record that a job was actually enqueued successfully.
    pub fn record_build_accepted(&self) {
        self.builds_queued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_build_completed(&self, duration: std::time::Duration) {
        self.builds_completed.fetch_add(1, Ordering::Relaxed);
        self.build_duration_ms_total
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
        Self::dec_saturating(&self.build_queue_depth);
    }

    pub fn record_build_failed(&self) {
        self.builds_failed.fetch_add(1, Ordering::Relaxed);
        Self::dec_saturating(&self.build_queue_depth);
    }

    /// Decrement a gauge but never wrap below zero — a gauge that underflowed to
    /// `u64::MAX` would render as a garbage Prometheus value.
    fn dec_saturating(g: &AtomicU64) {
        let _ = g.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        });
    }

    /// Render metrics in the Prometheus text exposition format (v0.0.4).
    /// Counters use the `_total` suffix; `build_queue_depth` is a gauge.
    pub fn prometheus(&self) -> String {
        use std::fmt::Write as _;
        let s = self.snapshot();
        let mut out = String::with_capacity(2048);

        let counters: [(&str, &str, u64); 14] = [
            (
                "ripclone_ref_lookups_total",
                "Ref lookups served",
                s.ref_lookups,
            ),
            ("ripclone_syncs_total", "Repo syncs performed", s.syncs),
            (
                "ripclone_sync_duration_ms_total",
                "Total sync wall time in milliseconds",
                s.sync_duration_ms_total,
            ),
            (
                "ripclone_artifact_requests_total",
                "Artifact requests served",
                s.artifact_requests,
            ),
            (
                "ripclone_artifact_bytes_served_total",
                "Artifact bytes served",
                s.artifact_bytes_served,
            ),
            ("ripclone_errors_total", "Request errors", s.errors),
            (
                "ripclone_retention_runs_total",
                "Retention runs",
                s.retention_runs,
            ),
            (
                "ripclone_retention_evicted_bytes_total",
                "Bytes evicted by retention",
                s.retention_evicted_bytes,
            ),
            (
                "ripclone_retention_evicted_objects_total",
                "Objects evicted by retention",
                s.retention_evicted_objects,
            ),
            (
                "ripclone_retention_errors_total",
                "Retention errors",
                s.retention_errors,
            ),
            (
                "ripclone_builds_queued_total",
                "Builds enqueued",
                s.builds_queued,
            ),
            (
                "ripclone_builds_completed_total",
                "Builds completed",
                s.builds_completed,
            ),
            (
                "ripclone_build_duration_ms_total",
                "Total build wall time in milliseconds",
                s.build_duration_ms_total,
            ),
            (
                "ripclone_builds_failed_total",
                "Builds failed",
                s.builds_failed,
            ),
        ];
        for (name, help, value) in counters {
            let _ = writeln!(out, "# HELP {name} {}", escape_help(help));
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {value}");
        }

        let _ = writeln!(
            out,
            "# HELP ripclone_build_queue_depth {}",
            escape_help("Builds currently queued or in flight")
        );
        let _ = writeln!(out, "# TYPE ripclone_build_queue_depth gauge");
        let _ = writeln!(out, "ripclone_build_queue_depth {}", s.build_queue_depth);

        out
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

/// Escape HELP text per the Prometheus exposition format (backslash and
/// newline). All current strings are plain, but this keeps the renderer correct
/// if a future HELP string contains a special character.
fn escape_help(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_format_has_help_type_and_values() {
        let m = Metrics::new();
        m.record_ref_lookup();
        m.record_ref_lookup();
        m.record_sync(std::time::Duration::from_millis(10));
        m.record_artifact_request(1234);
        m.record_build_queued();

        let out = m.prometheus();

        // Counter: HELP + TYPE + value.
        assert!(out.contains("# HELP ripclone_ref_lookups_total Ref lookups served"));
        assert!(out.contains("# TYPE ripclone_ref_lookups_total counter"));
        assert!(out.contains("\nripclone_ref_lookups_total 2\n"));
        assert!(out.contains("\nripclone_artifact_bytes_served_total 1234\n"));

        // Gauge.
        assert!(out.contains("# TYPE ripclone_build_queue_depth gauge"));
        assert!(out.contains("\nripclone_build_queue_depth 1\n"));

        // Every metric (14 counters + 1 gauge) carries a TYPE line.
        assert_eq!(out.matches("# TYPE ").count(), 15);
        // Output is a complete set of lines (no dangling partial line).
        assert!(out.ends_with('\n'));
    }

    #[test]
    fn build_queue_depth_gauge_is_balanced_and_saturating() {
        let m = Metrics::new();
        m.record_build_queued();
        m.record_build_completed(std::time::Duration::from_millis(1));
        assert!(
            m.prometheus().contains("\nripclone_build_queue_depth 0\n"),
            "balanced queue/complete should return the gauge to 0"
        );
        // Underflow guard: a stray decrement must clamp at 0, never wrap to
        // u64::MAX (which would render as a garbage gauge value).
        m.record_build_failed();
        assert!(
            m.prometheus().contains("\nripclone_build_queue_depth 0\n"),
            "decrement below zero must saturate at 0"
        );
    }
}
