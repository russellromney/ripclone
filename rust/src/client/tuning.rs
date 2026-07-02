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
        let fetch_concurrency = env_usize("RIPCLONE_FETCH_CONCURRENCY", 6);
        let archive_fetch_concurrency = env_usize_any(
            &[
                "RIPCLONE_ARCHIVE_FETCH_CONCURRENCY",
                "RIPCLONE_FETCH_CONCURRENCY",
            ],
            16,
        );
        let editable_download_concurrency = env_usize_any(
            &[
                "RIPCLONE_EDITABLE_DOWNLOAD_CONCURRENCY",
                "RIPCLONE_FETCH_CONCURRENCY",
            ],
            cores,
        );
        let pack_parse_threads = env_usize("RIPCLONE_PACK_PARSE_THREADS", cores);
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

fn env_usize(key: &str, default: usize) -> usize {
    env_usize_any(&[key], default)
}

fn env_usize_any(keys: &[&str], default: usize) -> usize {
    keys.iter()
        .find_map(|key| {
            std::env::var(key)
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|&n| n > 0)
        })
        .unwrap_or(default)
        .max(1)
}
