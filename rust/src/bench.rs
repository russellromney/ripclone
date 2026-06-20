use serde::Serialize;
use std::time::Instant;

/// Per-phase benchmark report for a single clone.
///
/// Phase definitions match `ROADMAP.md`:
///
/// * `resolve_ms` — ref request sent to ref response received.
/// * `manifest_ms` — manifest downloaded + decoded.
/// * `metadata_ms` — metadata chunk downloaded + decoded + skeleton/index written.
/// * `head_blobs_download_ms` — first head-blobs chunk request sent to last byte received.
/// * `archive_download_ms` — first archive chunk request sent to last byte received.
/// * `write_ms` — first working-tree byte written to last file closed.
/// * `checkout_ms` — `git checkout-index` duration, or extractor worker duration.
/// * `total_ms` — wall clock from CLI start to exit.
#[derive(Default, Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    pub resolve_ms: u64,
    pub manifest_ms: u64,
    pub metadata_ms: u64,
    pub head_blobs_download_ms: u64,
    pub archive_download_ms: u64,
    pub write_ms: u64,
    pub checkout_ms: u64,
    pub total_ms: u64,

    pub metadata_bytes: u64,
    pub head_blobs_bytes: u64,
    pub archive_bytes: u64,
}

impl BenchmarkReport {
    pub fn total_bytes(&self) -> u64 {
        self.metadata_bytes + self.head_blobs_bytes + self.archive_bytes
    }

    pub fn throughput_mbps(&self, bytes: u64, ms: u64) -> f64 {
        if ms == 0 || bytes == 0 {
            0.0
        } else {
            // bytes * 8 / ms / 1000 -> Mbps
            (bytes as f64 * 8.0) / (ms as f64) / 1000.0
        }
    }

    pub fn head_blobs_throughput_mbps(&self) -> f64 {
        self.throughput_mbps(self.head_blobs_bytes, self.head_blobs_download_ms)
    }

    pub fn archive_throughput_mbps(&self) -> f64 {
        self.throughput_mbps(self.archive_bytes, self.archive_download_ms)
    }
}

pub struct Benchmark {
    start: Instant,
    last: Instant,
    head_blobs_download_start: Option<Instant>,
    archive_download_start: Option<Instant>,
    report: BenchmarkReport,
}

impl Benchmark {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            start: now,
            last: now,
            head_blobs_download_start: None,
            archive_download_start: None,
            report: BenchmarkReport::default(),
        }
    }

    fn mark_phase(&mut self, setter: impl FnOnce(&mut BenchmarkReport, u64)) -> Instant {
        let elapsed = self.last.elapsed();
        setter(&mut self.report, elapsed.as_millis() as u64);
        let now = Instant::now();
        self.last = now;
        now
    }

    pub fn mark_resolve(&mut self) {
        self.mark_phase(|r, ms| r.resolve_ms = ms);
    }

    pub fn mark_manifest(&mut self) {
        self.mark_phase(|r, ms| r.manifest_ms = ms);
    }

    pub fn mark_metadata(&mut self) {
        self.mark_phase(|r, ms| r.metadata_ms = ms);
    }

    pub fn start_head_blobs_download(&mut self) {
        self.head_blobs_download_start = Some(Instant::now());
    }

    pub fn start_archive_download(&mut self) {
        self.archive_download_start = Some(Instant::now());
    }

    pub fn mark_head_blobs_download(&mut self, bytes: u64) {
        let start = self.head_blobs_download_start.unwrap_or(self.last);
        self.report.head_blobs_download_ms = start.elapsed().as_millis() as u64;
        self.report.head_blobs_bytes = bytes;
        self.last = Instant::now();
    }

    pub fn mark_archive_download(&mut self, bytes: u64) {
        let start = self.archive_download_start.unwrap_or(self.last);
        self.report.archive_download_ms = start.elapsed().as_millis() as u64;
        self.report.archive_bytes = bytes;
        self.last = Instant::now();
    }

    pub fn mark_write(&mut self) {
        self.mark_phase(|r, ms| r.write_ms = ms);
    }

    pub fn mark_checkout(&mut self) {
        self.mark_phase(|r, ms| r.checkout_ms = ms);
    }

    pub fn add_bytes(&mut self, metadata: u64, head_blobs: u64, archive: u64) {
        self.report.metadata_bytes += metadata;
        self.report.head_blobs_bytes += head_blobs;
        self.report.archive_bytes += archive;
    }

    pub fn finish(&mut self) -> BenchmarkReport {
        self.report.total_ms = self.start.elapsed().as_millis() as u64;
        self.report.clone()
    }
}
