use std::sync::{Arc, OnceLock};

const EDITABLE_PACK_MEMORY_UNIT_BYTES: u64 = 64 * 1024;
const EDITABLE_PACK_MEMORY_BUDGET_BYTES: u64 = 128 * 1024 * 1024;
const EDITABLE_PACK_MEMORY_PERMITS: u32 =
    (EDITABLE_PACK_MEMORY_BUDGET_BYTES / EDITABLE_PACK_MEMORY_UNIT_BYTES) as u32;

pub(super) struct EditablePackMemoryPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct ProcessEditablePackMemoryBudget {
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl ProcessEditablePackMemoryBudget {
    fn new() -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(
                EDITABLE_PACK_MEMORY_PERMITS as usize,
            )),
        }
    }

    fn permits_for_len(len: u64) -> u32 {
        let units = len.saturating_add(EDITABLE_PACK_MEMORY_UNIT_BYTES - 1)
            / EDITABLE_PACK_MEMORY_UNIT_BYTES;
        u32::try_from(units.min(u64::from(EDITABLE_PACK_MEMORY_PERMITS)))
            .unwrap_or(EDITABLE_PACK_MEMORY_PERMITS)
            .max(1)
    }

    async fn acquire(&self, len: u64) -> EditablePackMemoryPermit {
        let permits = Self::permits_for_len(len);
        let permit = Arc::clone(&self.semaphore)
            .acquire_many_owned(permits)
            .await
            .expect("editable pack memory semaphore is never closed");
        EditablePackMemoryPermit { _permit: permit }
    }
}

pub(super) async fn acquire_editable_pack_memory(len: u64) -> EditablePackMemoryPermit {
    static BUDGET: OnceLock<ProcessEditablePackMemoryBudget> = OnceLock::new();
    BUDGET
        .get_or_init(ProcessEditablePackMemoryBudget::new)
        .acquire(len)
        .await
}

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
        // Test hook: set archive downloads and both pack pipeline stages so
        // barrier-based tests can pin their race windows deterministically.
        // Never set in production (the defaults remain 16 archive/editable
        // requests and one pack worker per core).
        let test_download_concurrency = std::env::var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0);
        let archive_fetch_concurrency = test_download_concurrency.unwrap_or(16);
        let editable_download_concurrency = test_download_concurrency.unwrap_or(16);
        let requested_pack_threads = test_download_concurrency.unwrap_or(cores);
        #[cfg(target_os = "linux")]
        let pack_parse_threads = requested_pack_threads
            .min(process_pack_fd_budget().capacity)
            .max(1);
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

/// One permit represents the maximum direct descriptors a pack parser's
/// io_uring writer can keep live. The owned permit moves into the blocking
/// parser task and is released only when extraction (including deferred-window
/// draining) exits, so concurrent clones cannot each spend the full process
/// `RLIMIT_NOFILE` independently.
#[cfg(target_os = "linux")]
pub(super) struct PackParseFdPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(not(target_os = "linux"))]
pub(super) struct PackParseFdPermit;

#[cfg(target_os = "linux")]
struct ProcessPackFdBudget {
    semaphore: Arc<tokio::sync::Semaphore>,
    capacity: usize,
}

#[cfg(target_os = "linux")]
impl ProcessPackFdBudget {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(capacity)),
            capacity,
        }
    }

    async fn acquire(&self) -> PackParseFdPermit {
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("process pack FD semaphore is never closed");
        PackParseFdPermit { _permit: permit }
    }
}

#[cfg(target_os = "linux")]
fn process_pack_fd_budget() -> &'static ProcessPackFdBudget {
    static BUDGET: OnceLock<ProcessPackFdBudget> = OnceLock::new();
    BUDGET.get_or_init(|| {
        let capacity = crate::worktree_writer::linux_fd_safe_writer_concurrency(usize::MAX);
        tracing::debug!(
            capacity,
            "initialized process-wide pack parser file-descriptor budget"
        );
        ProcessPackFdBudget::new(capacity)
    })
}

#[cfg(target_os = "linux")]
pub(super) async fn acquire_pack_parse_fd_permit() -> PackParseFdPermit {
    process_pack_fd_budget().acquire().await
}

#[cfg(not(target_os = "linux"))]
pub(super) async fn acquire_pack_parse_fd_permit() -> PackParseFdPermit {
    PackParseFdPermit
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        EDITABLE_PACK_MEMORY_BUDGET_BYTES, ProcessEditablePackMemoryBudget, ProcessPackFdBudget,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    #[tokio::test]
    async fn process_fd_budget_is_shared_across_concurrent_callers() {
        let budget = Arc::new(ProcessPackFdBudget::new(2));
        let start = Arc::new(tokio::sync::Barrier::new(9));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let budget = Arc::clone(&budget);
            let start = Arc::clone(&start);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                let _permit = budget.acquire().await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(15)).await;
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        start.wait().await;
        for task in tasks {
            task.await.expect("budget caller");
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn editable_pack_memory_budget_is_byte_weighted_and_process_shared() {
        let budget = Arc::new(ProcessEditablePackMemoryBudget::new());
        let first = budget.acquire(EDITABLE_PACK_MEMORY_BUDGET_BYTES / 2).await;
        let second = budget.acquire(EDITABLE_PACK_MEMORY_BUDGET_BYTES / 2).await;
        let waiting_budget = Arc::clone(&budget);
        let waiting = tokio::spawn(async move { waiting_budget.acquire(1).await });
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(
            !waiting.is_finished(),
            "full byte budget must apply backpressure"
        );
        drop(first);
        let third = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("released byte budget admits waiting pack")
            .expect("memory permit task");
        drop((second, third));

        assert_eq!(
            ProcessEditablePackMemoryBudget::permits_for_len(u64::MAX),
            super::EDITABLE_PACK_MEMORY_PERMITS,
            "one oversized pack monopolizes, but cannot exceed, semaphore permits"
        );
    }
}
