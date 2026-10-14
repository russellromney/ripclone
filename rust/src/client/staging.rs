//! The staging directory a clone builds in, its rename onto the target, and
//! the cleanup that removes it when the clone fails or is cancelled.

use anyhow::{Context, Result};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

const INSTALL_STAGING_MARKER_SUFFIX: &str = ".ripclone-install-staging";

fn install_staging_marker_path(staging_dir: &Path) -> PathBuf {
    let mut marker_name = staging_dir
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("ripclone.tmp"))
        .to_os_string();
    marker_name.push(INSTALL_STAGING_MARKER_SUFFIX);
    staging_dir.with_file_name(marker_name)
}

#[cfg(unix)]
fn lock_staging_marker(marker: &std::fs::File, nonblocking: bool) -> Result<()> {
    use std::os::fd::AsRawFd;

    let mut operation = libc::LOCK_EX;
    if nonblocking {
        operation |= libc::LOCK_NB;
    }
    // SAFETY: `marker` owns this valid descriptor for the whole call. `flock`
    // changes only the advisory lock associated with that open file.
    if unsafe { libc::flock(marker.as_raw_fd(), operation) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("lock clone staging marker")
    }
}

pub(super) struct StagedTempDir {
    dir: Option<tempfile::TempDir>,
    marker_path: PathBuf,
    #[cfg(unix)]
    _marker_lock: std::fs::File,
}

impl StagedTempDir {
    pub(super) fn path(&self) -> &Path {
        self.dir
            .as_ref()
            .expect("staged temp directory exists until drop")
            .path()
    }
}

impl Drop for StagedTempDir {
    fn drop(&mut self) {
        let staging_path = self.path().to_path_buf();
        // TempDir performs the recursive cleanup. Preserve the sidecar marker
        // if that cleanup fails so a later clone can still identify and remove
        // the abandoned staging directory safely.
        drop(self.dir.take());
        if staging_path.exists() {
            return;
        }
        if let Err(error) = std::fs::remove_file(&self.marker_path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                "cannot remove clone staging marker {}: {error}",
                self.marker_path.display()
            );
        }
    }
}

#[cfg(unix)]
fn cleanup_stale_install_dirs(target: &Path, parent: &Path) {
    use std::io::Read;

    let target_name = target
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("ripclone"))
        .to_string_lossy();
    let prefix = format!("{target_name}.ripclone-");
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(rest) = name.strip_prefix(&prefix) else {
            continue;
        };
        let Some(rest) = rest.strip_suffix(".tmp") else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        let path = entry.path();
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let marker_path = install_staging_marker_path(&path);
        let Ok(mut marker) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&marker_path)
        else {
            continue;
        };
        // A live clone holds this lock even across PID namespaces. Failure to
        // acquire it is therefore a preserve decision, including unexpected
        // filesystem/locking errors.
        if lock_staging_marker(&marker, true).is_err() {
            continue;
        }
        let mut contents = String::new();
        if marker.read_to_string(&mut contents).is_err() || contents != "ripclone\n" {
            continue;
        }
        if let Err(error) = std::fs::remove_dir_all(&path) {
            warn!(
                "cannot remove stale clone staging directory {}: {error}",
                path.display()
            );
        } else {
            if let Err(error) = std::fs::remove_file(&marker_path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(
                    "cannot remove stale clone staging marker {}: {error}",
                    marker_path.display()
                );
            }
            info!("removed stale clone staging directory {}", path.display());
        }
    }
}

#[cfg(not(unix))]
fn cleanup_stale_install_dirs(_target: &Path, _parent: &Path) {}

/// The directory `target` lives in; a bare relative target lives in `.`.
fn container(target: &Path) -> &Path {
    target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Create a temp install directory next to `target`. Returns its owning handle
/// (not a bare path): the caller must keep it alive so that on *any* failure
/// before the final rename, the partial install is removed on drop and other
/// processes can see that its marker remains locked. On success the dir is
/// renamed onto `target`, after which the handle's drop is a harmless no-op
/// (the path no longer exists).
pub(super) fn temp_install_dir(target: &Path) -> Result<StagedTempDir> {
    let parent = container(target);
    cleanup_stale_install_dirs(target, parent);
    let target_name = target
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("ripclone"))
        .to_string_lossy();
    let tmp = tempfile::Builder::new()
        .prefix(&format!("{target_name}.ripclone-"))
        .suffix(".tmp")
        .tempdir_in(parent)
        .context("create temp install directory")?;
    let marker_path = install_staging_marker_path(tmp.path());
    #[cfg(unix)]
    {
        use std::io::Write;

        let mut marker_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&marker_path)
            .context("create clone staging marker")?;
        lock_staging_marker(&marker_lock, false)?;
        marker_lock
            .write_all(b"ripclone\n")
            .context("mark temp install directory")?;
        Ok(StagedTempDir {
            dir: Some(tmp),
            marker_path,
            _marker_lock: marker_lock,
        })
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&marker_path, b"ripclone\n").context("mark temp install directory")?;
        Ok(StagedTempDir {
            dir: Some(tmp),
            marker_path,
        })
    }
}

/// Rename the finished staging tree onto `target`. With `RIPCLONE_FSYNC` set,
/// flush the tree first and the parent directory after, so a crash cannot
/// leave a torn tree that `git status` calls clean. Off by default: the
/// rename alone is already crash-consistent.
pub(super) fn publish(root: &Path, target: &Path) -> Result<()> {
    let fsync = fsync_requested();
    if fsync {
        fsync_tree(root).context("fsync staged clone before publish")?;
    }
    std::fs::rename(root, target)
        .with_context(|| format!("rename {} to {}", root.display(), target.display()))?;
    if fsync {
        fsync_dir(container(target)).context("fsync parent directory after publish")?;
    }
    Ok(())
}

fn fsync_requested() -> bool {
    std::env::var("RIPCLONE_FSYNC")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// fsync one directory so its newly created entries survive a crash. On Unix
/// this opens the directory and syncs it; elsewhere it is a best-effort no-op.
#[cfg(unix)]
fn fsync_dir(path: &Path) -> Result<()> {
    let dir = std::fs::File::open(path)
        .with_context(|| format!("open dir for fsync {}", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("fsync dir {}", path.display()))
}

#[cfg(not(unix))]
fn fsync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

/// Walk `root` and collect every regular file into `files` and every directory
/// into `dirs`. A symlink is persisted by syncing its parent directory, not by
/// following the link, so it needs no entry of its own. The `.git/index` written
/// during extraction is a regular file under `root`, so this picks it up — the
/// index stat cache is flushed alongside the working-tree files it describes.
fn collect_fsync_targets(
    root: &Path,
    files: &mut Vec<PathBuf>,
    dirs: &mut Vec<PathBuf>,
) -> Result<()> {
    let meta = std::fs::symlink_metadata(root)
        .with_context(|| format!("stat for fsync {}", root.display()))?;
    if meta.is_dir() {
        for entry in
            std::fs::read_dir(root).with_context(|| format!("read dir {}", root.display()))?
        {
            collect_fsync_targets(&entry?.path(), files, dirs)?;
        }
        dirs.push(root.to_path_buf());
    } else if meta.is_file() {
        files.push(root.to_path_buf());
    }
    Ok(())
}

/// Flush the whole materialized tree under `root`: every file (including the
/// `.git/index` stat cache) and every directory that holds one.
fn fsync_tree(root: &Path) -> Result<()> {
    let mut files = Vec::new();
    let mut dirs = Vec::new();
    collect_fsync_targets(root, &mut files, &mut dirs)?;
    crate::worktree_writer::fsync_paths_durable(&files, &dirs)
}

type CleanupFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Default)]
struct CleanupInner {
    pending: Mutex<Vec<CleanupFuture>>,
    active_guards: AtomicUsize,
    closed: AtomicBool,
    changed: tokio::sync::Notify,
}

/// Work a clone must finish before its staging can be removed: the tasks and
/// child processes it started. Each is registered through an [`AbortOnDrop`]
/// or [`GitChild`] guard.
#[derive(Clone, Default)]
pub(super) struct Cleanup(Arc<CleanupInner>);

/// The staging directory of one clone, owned by its reaper until it is
/// published or removed.
pub(super) type Staging = Arc<Mutex<Option<StagedTempDir>>>;

// This cleanup ownership deliberately covers arbitrary cancellation of the
// public install future. Tokio cannot stop spawn_blocking work that has
// begun, so detached cancellation must retain the staging tree until every
// registered writer has exited.
pub(super) fn spawn_reaper(cleanup: Cleanup, staging: Staging) -> tokio::task::JoinHandle<()> {
    let running = RunningReaper::start();
    tokio::spawn(async move {
        cleanup.drain_closed().await;
        let staging = staging.lock().unwrap_or_else(|e| e.into_inner()).take();
        drop(staging);
        drop(running);
    })
}

/// Reapers whose staging is not yet removed. A cancelled clone's reaper keeps
/// running after the clone future is dropped; see [`wait_for_cancelled_cleanup`].
static RUNNING_REAPERS: AtomicUsize = AtomicUsize::new(0);

struct RunningReaper;

impl RunningReaper {
    fn start() -> Self {
        RUNNING_REAPERS.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for RunningReaper {
    fn drop(&mut self) {
        RUNNING_REAPERS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// After dropping an install future (for example on Ctrl-C), wait up to
/// `limit` for its workers to stop and its staging to be removed. Returns
/// false if cleanup was still running at the limit; the next clone into the
/// same parent removes what is left.
pub async fn wait_for_cancelled_cleanup(limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while RUNNING_REAPERS.load(Ordering::SeqCst) > 0 {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    true
}

impl Cleanup {
    fn guard_started(&self) {
        self.0.active_guards.fetch_add(1, Ordering::SeqCst);
    }

    fn guard_finished(&self) {
        let previous = self.0.active_guards.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0, "cleanup guard underflow");
        self.0.changed.notify_one();
    }

    fn push(&self, future: CleanupFuture) {
        self.0
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(future);
        self.0.changed.notify_one();
    }

    fn close(&self) {
        self.0.closed.store(true, Ordering::SeqCst);
        self.0.changed.notify_one();
    }

    async fn drain(&self) {
        loop {
            let pending = {
                let mut pending = self.0.pending.lock().unwrap_or_else(|e| e.into_inner());
                std::mem::take(&mut *pending)
            };
            if pending.is_empty() {
                break;
            }
            // Awaiting one cancelled parent can drop child guards that append
            // more handles, so drain to a fixed point before removing staging.
            futures::future::join_all(pending).await;
        }
    }

    async fn drain_closed(&self) {
        loop {
            let changed = self.0.changed.notified();
            self.drain().await;
            if self.0.closed.load(Ordering::SeqCst)
                && self.0.active_guards.load(Ordering::SeqCst) == 0
            {
                // No guard can enqueue after the clone is closed and all
                // registered guards have finished. One last drain closes the
                // race with a guard that enqueued immediately before decrement.
                self.drain().await;
                return;
            }
            changed.await;
        }
    }
}

/// Closes the clone's [`Cleanup`] when the clone future finishes or is
/// dropped, so its reaper can remove staging once every guard is done.
pub(super) struct CloseOnDrop(pub(super) Cleanup);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// A spawned task that is aborted, and then awaited by the reaper, if its
/// owner is dropped before joining it.
pub(super) struct AbortOnDrop<T: Send + 'static> {
    handle: Option<tokio::task::JoinHandle<T>>,
    cleanup: Cleanup,
}

impl<T: Send + 'static> AbortOnDrop<T> {
    pub(super) fn new(handle: tokio::task::JoinHandle<T>, cleanup: Cleanup) -> Self {
        cleanup.guard_started();
        Self {
            handle: Some(handle),
            cleanup,
        }
    }

    pub(super) async fn join(mut self) -> std::result::Result<T, tokio::task::JoinError> {
        let result = self.handle.as_mut().expect("join handle present").await;
        self.handle.take();
        self.cleanup.guard_finished();
        result
    }
}

impl<T: Send + 'static> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            self.cleanup.push(Box::pin(async move {
                // `abort` cancels async tasks immediately. A blocking task
                // already running cannot be cancelled, so awaiting its handle
                // is what keeps it from writing into removed staging.
                let _ = handle.await;
            }));
            self.cleanup.guard_finished();
        }
    }
}

/// A git child process working in staging. Dropping it before it exits
/// kills it and leaves the wait to the reaper.
pub(super) struct GitChild {
    child: Option<tokio::process::Child>,
    cleanup: Cleanup,
}

impl GitChild {
    pub(super) fn new(child: tokio::process::Child, cleanup: Cleanup) -> Self {
        Self {
            child: Some(child),
            cleanup,
        }
    }

    pub(super) async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let child = self.child.as_mut().expect("git child present until exit");
        let status = child.wait().await?;
        self.child = None;
        Ok(status)
    }
}

impl Drop for GitChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            self.cleanup.push(Box::pin(async move {
                let _ = child.wait().await;
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn next_clone_removes_only_marked_unlocked_staging() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("linux");

        let stale = dir.path().join("linux.ripclone-old.tmp");
        std::fs::create_dir(&stale).unwrap();
        std::fs::write(install_staging_marker_path(&stale), b"ripclone\n").unwrap();
        std::fs::write(stale.join("partial"), b"partial clone").unwrap();

        let live = temp_install_dir(&target).unwrap();
        let live_path = live.path().to_path_buf();

        let unmarked = dir.path().join("linux.ripclone-unmarked.tmp");
        std::fs::create_dir(&unmarked).unwrap();
        std::fs::write(unmarked.join("user-data"), b"preserve").unwrap();

        let created = temp_install_dir(&target).unwrap();
        assert!(!stale.exists());
        assert!(live_path.exists());
        assert!(unmarked.exists());
        assert!(install_staging_marker_path(created.path()).is_file());
        assert!(!created.path().join(INSTALL_STAGING_MARKER_SUFFIX).exists());
    }

    #[test]
    fn staging_marker_name_in_repository_survives_publication() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("collision");
        let staged = temp_install_dir(&target).unwrap();
        let staging_path = staged.path().to_path_buf();
        let sidecar = install_staging_marker_path(&staging_path);
        std::fs::write(
            staging_path.join(INSTALL_STAGING_MARKER_SUFFIX),
            b"tracked repository content\n",
        )
        .unwrap();

        publish(&staging_path, &target).unwrap();
        drop(staged);

        assert_eq!(
            std::fs::read(target.join(INSTALL_STAGING_MARKER_SUFFIX)).unwrap(),
            b"tracked repository content\n"
        );
        assert!(!sidecar.exists());
    }

    #[test]
    fn collect_fsync_targets_covers_files_dirs_and_index() {
        // The durability barrier must flush the working-tree files, every
        // directory that holds one, AND the `.git/index` stat cache that git
        // consults to decide clean vs dirty. Missing the index is the exact way
        // a crash leaves a torn tree that `git status` calls clean.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join(".git/index"), b"INDEX").unwrap();
        std::fs::write(root.join("src/main.rs"), b"fn main() {}").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("src/main.rs", root.join("link")).unwrap();

        let mut files = Vec::new();
        let mut dirs = Vec::new();
        collect_fsync_targets(root, &mut files, &mut dirs).expect("collect targets");

        assert!(
            files.iter().any(|p| p.ends_with(".git/index")),
            "index stat cache must be in the fsync set: {files:?}"
        );
        assert!(files.iter().any(|p| p.ends_with("src/main.rs")));
        // The symlink is persisted via its parent dir, so it is not fsynced
        // directly.
        assert!(!files.iter().any(|p| p.ends_with("link")));
        // Every directory that holds a materialized entry is flushed.
        assert!(dirs.iter().any(|p| p.ends_with(".git")));
        assert!(dirs.iter().any(|p| p.ends_with("src")));
        assert!(dirs.iter().any(|p| p == root));

        // The batched barrier itself must run cleanly over the collected set
        // (POSIX path here; the io_uring path is exercised on Linux CI).
        crate::worktree_writer::fsync_paths_durable(&files, &dirs)
            .expect("durable fsync over collected tree");
    }

    #[test]
    fn container_of_a_relative_target_is_the_cwd() {
        // The post-rename fsync and the staging directory both use the
        // target's container. A bare relative target (`bun`) has an empty
        // parent; it must resolve to `.`, never be skipped.
        for (target, parent) in [
            ("bun", "."),
            ("bun/", "."),
            ("out/bun", "out"),
            ("a/b/c", "a/b"),
            ("/work/bun", "/work"),
        ] {
            assert_eq!(container(Path::new(target)), Path::new(parent), "{target}");
        }
    }

    #[tokio::test]
    async fn cancelled_join_keeps_blocking_sibling_armed_until_cleanup() {
        let cleanup = Cleanup::default();
        let fixture = tempfile::tempdir().unwrap();
        let target = fixture.path().join("target");
        let staging_owner = temp_install_dir(&target).unwrap();
        let staging = staging_owner.path().to_path_buf();
        let staging_worker = staging.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let sibling = AbortOnDrop::new(
            tokio::task::spawn_blocking(move || {
                std::fs::write(staging_worker.join("partial"), b"started").unwrap();
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                std::fs::write(staging_worker.join("late-write"), b"finished").unwrap();
                Ok::<(), anyhow::Error>(())
            }),
            cleanup.clone(),
        );
        let failed = AbortOnDrop::new(
            tokio::spawn(async move {
                started_rx.await.unwrap();
                Err::<(), anyhow::Error>(anyhow::anyhow!("download failed"))
            }),
            cleanup.clone(),
        );

        let failed_join = async {
            failed.join().await.context("join failed task")??;
            Ok::<(), anyhow::Error>(())
        };
        let sibling_join = async {
            sibling.join().await.context("join blocking sibling")??;
            Ok::<(), anyhow::Error>(())
        };
        tokio::try_join!(failed_join, sibling_join)
            .expect_err("failed task cancels the sibling join future");

        let cleanup_wait = cleanup.clone();
        let drained = Arc::new(AtomicBool::new(false));
        let drained_flag = Arc::clone(&drained);
        let staging_after_cleanup = staging.clone();
        let mut drain_then_remove = Box::pin(async move {
            cleanup_wait.drain().await;
            drop(staging_owner);
            assert!(!staging_after_cleanup.exists());
            drained_flag.store(true, Ordering::SeqCst);
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut drain_then_remove)
                .await
                .is_err(),
            "cleanup must wait for the cancelled join's blocking sibling"
        );
        assert!(!drained.load(Ordering::SeqCst));
        assert!(staging.exists());
        assert!(!target.exists());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), drain_then_remove)
            .await
            .expect("blocking sibling joined before cleanup");
        assert!(drained.load(Ordering::SeqCst));
        assert!(!staging.exists());
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn cancelled_clone_reaper_joins_worker_before_removing_staging() {
        let cleanup = Cleanup::default();
        let fixture = tempfile::tempdir().unwrap();
        let target = fixture.path().join("target");
        let staging_owner = temp_install_dir(&target).unwrap();
        let staging_path = staging_owner.path().to_path_buf();
        let staging = Arc::new(Mutex::new(Some(staging_owner)));
        let mut reaper = spawn_reaper(cleanup.clone(), Arc::clone(&staging));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_path = staging_path.clone();
        let operation_cleanup = cleanup.clone();
        let mut operation = tokio::spawn(async move {
            let _close_on_drop = CloseOnDrop(operation_cleanup.clone());
            let _worker = AbortOnDrop::new(
                tokio::task::spawn_blocking(move || {
                    std::fs::write(worker_path.join("partial"), b"started").unwrap();
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    std::fs::write(worker_path.join("late-write"), b"finished").unwrap();
                }),
                operation_cleanup,
            );
            std::future::pending::<()>().await;
        });

        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("blocking worker started")
            .expect("worker start signal");
        operation.abort();
        tokio::time::timeout(Duration::from_secs(1), &mut operation)
            .await
            .expect("cancelled clone task joined")
            .expect_err("clone task was cancelled");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut reaper)
                .await
                .is_err(),
            "cancellation must not remove staging before blocking work exits"
        );
        assert!(staging_path.exists());
        assert!(!target.exists());

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut reaper)
            .await
            .expect("cancelled clone reaper completed")
            .expect("reaper task joined");
        assert!(!staging_path.exists());
        assert!(!target.exists());
    }
}
