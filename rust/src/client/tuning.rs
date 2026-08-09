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
        // Test hook: set both pack pipeline stages so barrier-based tests can
        // pin their race windows deterministically. Never set in production
        // (the default is one worker per core).
        let test_pack_concurrency = std::env::var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0);
        let editable_download_concurrency = test_pack_concurrency.unwrap_or(cores);
        let requested_pack_threads = test_pack_concurrency.unwrap_or(cores);
        #[cfg(target_os = "linux")]
        let pack_parse_threads =
            crate::worktree_writer::linux_fd_safe_writer_concurrency(requested_pack_threads);
        #[cfg(not(target_os = "linux"))]
        let pack_parse_threads = requested_pack_threads;
        if pack_parse_threads < requested_pack_threads {
            tracing::debug!(
                requested_pack_threads,
                pack_parse_threads,
                "capping pack parser concurrency to the process file-descriptor budget"
            );
        }
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
