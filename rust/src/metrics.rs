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
    sync_mirror_fetch_ms_total: AtomicU64,
    sync_head_packs_ms_total: AtomicU64,
    sync_files_table_ms_total: AtomicU64,
    sync_prebuilt_index_ms_total: AtomicU64,
    sync_upload_head_ms_total: AtomicU64,
    sync_ref_publish_ms_total: AtomicU64,
    sync_publish_head_ms_total: AtomicU64,
    artifact_requests: AtomicU64,
    artifact_bytes_served: AtomicU64,
    errors: AtomicU64,
    local_cache_cleanup_runs: AtomicU64,
    local_cache_removed_bytes: AtomicU64,
    local_cache_removed_objects: AtomicU64,
    local_cache_cleanup_errors: AtomicU64,
    artifacts_deleted: AtomicU64,
    builds_queued: AtomicU64,
    builds_completed: AtomicU64,
    builds_failed: AtomicU64,
    build_duration_ms_total: AtomicU64,
    outputs_published_head: AtomicU64,
    outputs_published_files: AtomicU64,
    outputs_published_full: AtomicU64,
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
        self.sync_duration_ms_total.fetch_add(
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn record_sync_phases(&self, phases: SyncPhaseMetrics) {
        Self::add_optional(&self.sync_mirror_fetch_ms_total, phases.mirror_fetch_ms);
        Self::add_optional(&self.sync_head_packs_ms_total, phases.head_packs_ms);
        Self::add_optional(&self.sync_files_table_ms_total, phases.files_table_ms);
        Self::add_optional(&self.sync_prebuilt_index_ms_total, phases.prebuilt_index_ms);
        Self::add_optional(&self.sync_upload_head_ms_total, phases.upload_head_ms);
        Self::add_optional(&self.sync_ref_publish_ms_total, phases.ref_publish_ms);
        Self::add_optional(&self.sync_publish_head_ms_total, phases.publish_head_ms);
    }

    pub fn record_artifact_request(&self, bytes: u64) {
        self.artifact_requests.fetch_add(1, Ordering::Relaxed);
        self.artifact_bytes_served
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_local_cache_cleanup(&self, removed_bytes: u64, removed_objects: u64) {
        self.local_cache_cleanup_runs
            .fetch_add(1, Ordering::Relaxed);
        self.local_cache_removed_bytes
            .fetch_add(removed_bytes, Ordering::Relaxed);
        self.local_cache_removed_objects
            .fetch_add(removed_objects, Ordering::Relaxed);
    }

    pub fn record_local_cache_cleanup_error(&self) {
        self.local_cache_cleanup_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// An artifact no output names was deleted from storage.
    pub fn record_artifact_deleted(&self) {
        self.artifacts_deleted.fetch_add(1, Ordering::Relaxed);
    }

    /// A new build row was admitted.
    pub fn record_build_accepted(&self) {
        self.builds_queued.fetch_add(1, Ordering::Relaxed);
    }

    /// Every output of a build row is ready. `duration` runs from admission.
    pub fn record_build_completed(&self, duration: std::time::Duration) {
        self.builds_completed.fetch_add(1, Ordering::Relaxed);
        self.build_duration_ms_total.fetch_add(
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// An output failed for good (permanent error or attempts exhausted).
    pub fn record_build_failed(&self) {
        self.builds_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_output_published(&self, kind: crate::ExactResultKind) {
        let counter = match kind {
            crate::ExactResultKind::Head => &self.outputs_published_head,
            crate::ExactResultKind::Files => &self.outputs_published_files,
            crate::ExactResultKind::Full => &self.outputs_published_full,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn add_optional(counter: &AtomicU64, value: Option<u64>) {
        if let Some(value) = value {
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Render metrics in the Prometheus text exposition format (v0.0.4).
    /// Every metric is a counter with the `_total` suffix.
    pub fn prometheus(&self) -> String {
        use std::fmt::Write as _;
        let s = self.snapshot();
        let mut out = String::with_capacity(2048);

        let counters: [(&str, &str, u64); 22] = [
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
                "ripclone_sync_mirror_fetch_ms_total",
                "Total mirror-fetch phase time in milliseconds",
                s.sync_mirror_fetch_ms_total,
            ),
            (
                "ripclone_sync_head_packs_ms_total",
                "Total head-pack phase time in milliseconds",
                s.sync_head_packs_ms_total,
            ),
            (
                "ripclone_sync_files_table_ms_total",
                "Total files-table phase time in milliseconds",
                s.sync_files_table_ms_total,
            ),
            (
                "ripclone_sync_prebuilt_index_ms_total",
                "Total prebuilt-index phase time in milliseconds",
                s.sync_prebuilt_index_ms_total,
            ),
            (
                "ripclone_sync_upload_head_ms_total",
                "Total Head upload time in milliseconds",
                s.sync_upload_head_ms_total,
            ),
            (
                "ripclone_sync_ref_publish_ms_total",
                "Total ref-publish phase time in milliseconds",
                s.sync_ref_publish_ms_total,
            ),
            (
                "ripclone_sync_publish_head_ms_total",
                "Total Head publish wall time in milliseconds",
                s.sync_publish_head_ms_total,
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
                "ripclone_local_cache_cleanup_runs_total",
                "Local cache cleanup runs",
                s.local_cache_cleanup_runs,
            ),
            (
                "ripclone_local_cache_removed_bytes_total",
                "Bytes removed from the local build cache",
                s.local_cache_removed_bytes,
            ),
            (
                "ripclone_local_cache_removed_objects_total",
                "Objects removed from the local build cache",
                s.local_cache_removed_objects,
            ),
            (
                "ripclone_local_cache_cleanup_errors_total",
                "Local cache cleanup errors",
                s.local_cache_cleanup_errors,
            ),
            (
                "ripclone_artifacts_deleted_total",
                "Artifacts no output names, deleted from storage",
                s.artifacts_deleted,
            ),
            (
                "ripclone_builds_queued_total",
                "Build rows admitted",
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
            "# HELP ripclone_outputs_published_total {}",
            escape_help("Build outputs published, by kind")
        );
        let _ = writeln!(out, "# TYPE ripclone_outputs_published_total counter");
        for (kind, value) in [
            ("head", s.outputs_published_head),
            ("files", s.outputs_published_files),
            ("full", s.outputs_published_full),
        ] {
            let _ = writeln!(
                out,
                "ripclone_outputs_published_total{{kind=\"{kind}\"}} {value}"
            );
        }

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
            sync_mirror_fetch_ms_total: self.sync_mirror_fetch_ms_total.load(Ordering::Relaxed),
            sync_head_packs_ms_total: self.sync_head_packs_ms_total.load(Ordering::Relaxed),
            sync_files_table_ms_total: self.sync_files_table_ms_total.load(Ordering::Relaxed),
            sync_prebuilt_index_ms_total: self.sync_prebuilt_index_ms_total.load(Ordering::Relaxed),
            sync_upload_head_ms_total: self.sync_upload_head_ms_total.load(Ordering::Relaxed),
            sync_ref_publish_ms_total: self.sync_ref_publish_ms_total.load(Ordering::Relaxed),
            sync_publish_head_ms_total: self.sync_publish_head_ms_total.load(Ordering::Relaxed),
            artifact_requests: self.artifact_requests.load(Ordering::Relaxed),
            artifact_bytes_served: self.artifact_bytes_served.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            local_cache_cleanup_runs: self.local_cache_cleanup_runs.load(Ordering::Relaxed),
            local_cache_removed_bytes: self.local_cache_removed_bytes.load(Ordering::Relaxed),
            local_cache_removed_objects: self.local_cache_removed_objects.load(Ordering::Relaxed),
            local_cache_cleanup_errors: self.local_cache_cleanup_errors.load(Ordering::Relaxed),
            artifacts_deleted: self.artifacts_deleted.load(Ordering::Relaxed),
            builds_queued: self.builds_queued.load(Ordering::Relaxed),
            builds_completed,
            build_avg_ms: if builds_completed == 0 {
                0
            } else {
                build_ms / builds_completed
            },
            build_duration_ms_total: build_ms,
            builds_failed: self.builds_failed.load(Ordering::Relaxed),
            outputs_published_head: self.outputs_published_head.load(Ordering::Relaxed),
            outputs_published_files: self.outputs_published_files.load(Ordering::Relaxed),
            outputs_published_full: self.outputs_published_full.load(Ordering::Relaxed),
        }
    }
}

/// Escape HELP text per the Prometheus exposition format (backslash and
/// newline). All current strings are plain, but this keeps the renderer correct
/// if a future HELP string contains a special character.
fn escape_help(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n")
}

#[derive(Default)]
pub struct SyncPhaseMetrics {
    pub mirror_fetch_ms: Option<u64>,
    pub head_packs_ms: Option<u64>,
    pub files_table_ms: Option<u64>,
    pub prebuilt_index_ms: Option<u64>,
    pub upload_head_ms: Option<u64>,
    pub ref_publish_ms: Option<u64>,
    pub publish_head_ms: Option<u64>,
}

#[derive(Serialize)]
pub struct MetricsSnapshot {
    pub ref_lookups: u64,
    pub syncs: u64,
    pub sync_avg_ms: u64,
    pub sync_duration_ms_total: u64,
    pub sync_mirror_fetch_ms_total: u64,
    pub sync_head_packs_ms_total: u64,
    pub sync_files_table_ms_total: u64,
    pub sync_prebuilt_index_ms_total: u64,
    pub sync_upload_head_ms_total: u64,
    pub sync_ref_publish_ms_total: u64,
    pub sync_publish_head_ms_total: u64,
    pub artifact_requests: u64,
    pub artifact_bytes_served: u64,
    pub errors: u64,
    pub local_cache_cleanup_runs: u64,
    pub local_cache_removed_bytes: u64,
    pub local_cache_removed_objects: u64,
    pub local_cache_cleanup_errors: u64,
    pub artifacts_deleted: u64,
    pub builds_queued: u64,
    pub builds_completed: u64,
    pub build_avg_ms: u64,
    pub build_duration_ms_total: u64,
    pub builds_failed: u64,
    pub outputs_published_head: u64,
    pub outputs_published_files: u64,
    pub outputs_published_full: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_outputs_render_one_series_per_kind() {
        let m = Metrics::new();
        m.record_output_published(crate::ExactResultKind::Head);
        m.record_output_published(crate::ExactResultKind::Head);
        m.record_output_published(crate::ExactResultKind::Full);
        let text = m.prometheus();
        for line in [
            "\nripclone_outputs_published_total{kind=\"head\"} 2\n",
            "\nripclone_outputs_published_total{kind=\"files\"} 0\n",
            "\nripclone_outputs_published_total{kind=\"full\"} 1\n",
        ] {
            assert!(text.contains(line), "{line:?} missing from {text}");
        }
    }
}
