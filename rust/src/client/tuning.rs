#[derive(Debug, Clone, Copy)]
pub(super) struct ClientTuning {
    pub(super) fetch_concurrency: usize,
    pub(super) archive_fetch_concurrency: usize,
    pub(super) editable_download_concurrency: usize,
    pub(super) pack_parse_threads: usize,
}

impl ClientTuning {
    pub(super) fn load() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        let fetch_concurrency = 6;
        let archive_fetch_concurrency = 16;
        // Test hook: force serial downloads so a barrier-based test can pin the
        // race window deterministically. Never set in production (the default is
        // one download per core).
        let editable_download_concurrency = std::env::var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(cores);
        let pack_parse_threads = cores;
        tracing::debug!(
            fetch_concurrency,
            archive_fetch_concurrency,
            editable_download_concurrency,
            pack_parse_threads,
            "ripclone client tuning"
        );
        Self {
            fetch_concurrency,
            archive_fetch_concurrency,
            editable_download_concurrency,
            pack_parse_threads,
        }
    }
}
