use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static ARCHIVE_SEND_WAIT_NS: AtomicU64 = AtomicU64::new(0);
static ARCHIVE_DOWNLOAD_NS: AtomicU64 = AtomicU64::new(0);
static ARCHIVE_DOWNLOAD_BYTES: AtomicU64 = AtomicU64::new(0);
static ZSTD_INFLATE_NS: AtomicU64 = AtomicU64::new(0);
static ZSTD_INFLATE_IN_BYTES: AtomicU64 = AtomicU64::new(0);
static ZSTD_INFLATE_OUT_BYTES: AtomicU64 = AtomicU64::new(0);
static ZLIB_INFLATE_NS: AtomicU64 = AtomicU64::new(0);
static ZLIB_INFLATE_IN_BYTES: AtomicU64 = AtomicU64::new(0);
static ZLIB_INFLATE_OUT_BYTES: AtomicU64 = AtomicU64::new(0);
static SHA1_NS: AtomicU64 = AtomicU64::new(0);
static SHA1_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Default, Debug, Clone, Serialize)]
pub struct PerfCounters {
    pub archive_send_wait_ns: u64,
    pub archive_download_ns: u64,
    pub archive_download_bytes: u64,
    pub zstd_inflate_ns: u64,
    pub zstd_inflate_in_bytes: u64,
    pub zstd_inflate_out_bytes: u64,
    pub zlib_inflate_ns: u64,
    pub zlib_inflate_in_bytes: u64,
    pub zlib_inflate_out_bytes: u64,
    pub sha1_ns: u64,
    pub sha1_bytes: u64,
}

pub fn reset_perf_counters() {
    let _ = take_perf_counters();
}

pub fn take_perf_counters() -> PerfCounters {
    PerfCounters {
        archive_send_wait_ns: ARCHIVE_SEND_WAIT_NS.swap(0, Ordering::Relaxed),
        archive_download_ns: ARCHIVE_DOWNLOAD_NS.swap(0, Ordering::Relaxed),
        archive_download_bytes: ARCHIVE_DOWNLOAD_BYTES.swap(0, Ordering::Relaxed),
        zstd_inflate_ns: ZSTD_INFLATE_NS.swap(0, Ordering::Relaxed),
        zstd_inflate_in_bytes: ZSTD_INFLATE_IN_BYTES.swap(0, Ordering::Relaxed),
        zstd_inflate_out_bytes: ZSTD_INFLATE_OUT_BYTES.swap(0, Ordering::Relaxed),
        zlib_inflate_ns: ZLIB_INFLATE_NS.swap(0, Ordering::Relaxed),
        zlib_inflate_in_bytes: ZLIB_INFLATE_IN_BYTES.swap(0, Ordering::Relaxed),
        zlib_inflate_out_bytes: ZLIB_INFLATE_OUT_BYTES.swap(0, Ordering::Relaxed),
        sha1_ns: SHA1_NS.swap(0, Ordering::Relaxed),
        sha1_bytes: SHA1_BYTES.swap(0, Ordering::Relaxed),
    }
}

fn add_ns(counter: &AtomicU64, duration: Duration) {
    counter.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
}

pub fn record_archive_send_wait(duration: Duration) {
    add_ns(&ARCHIVE_SEND_WAIT_NS, duration);
}

pub fn record_archive_download(duration: Duration, bytes: u64) {
    add_ns(&ARCHIVE_DOWNLOAD_NS, duration);
    ARCHIVE_DOWNLOAD_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

pub fn record_zstd_inflate(duration: Duration, input: usize, output: usize) {
    add_ns(&ZSTD_INFLATE_NS, duration);
    ZSTD_INFLATE_IN_BYTES.fetch_add(input as u64, Ordering::Relaxed);
    ZSTD_INFLATE_OUT_BYTES.fetch_add(output as u64, Ordering::Relaxed);
}

pub fn record_zlib_inflate(duration: Duration, input: usize, output: usize) {
    add_ns(&ZLIB_INFLATE_NS, duration);
    ZLIB_INFLATE_IN_BYTES.fetch_add(input as u64, Ordering::Relaxed);
    ZLIB_INFLATE_OUT_BYTES.fetch_add(output as u64, Ordering::Relaxed);
}

pub fn record_sha1(duration: Duration, bytes: usize) {
    add_ns(&SHA1_NS, duration);
    SHA1_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}
