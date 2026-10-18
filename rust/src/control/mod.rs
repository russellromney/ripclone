//! The server-owned SQLite control database.
//!
//! One `builds` row per (repo, commit) with a state and a lease per output
//! kind, leases instead of jobs, and added repositories with their config.
//! [`sqlite::SqliteControlStore`] implements [`ControlStore`].

use crate::provider::RepoId;
use crate::{ExactResultKind, FilesResult, FullResult, HeadResult};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(test)]
pub mod conformance;
pub mod sqlite;

pub use sqlite::SqliteControlStore;

/// Which claimers a build row is offered to. Head and Files are offered
/// while pending; they need only the commit. Full is offered once Head is
/// ready, since it reuses Head's packs. Each output kind has its own lease,
/// attempts and retry time, so a row's Full never waits for its Files. A Full is offered only when no older row
/// of the repo has Full pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pool {
    Head,
    Files,
    Full,
}

impl Pool {
    /// The output kind this pool builds.
    pub fn kind(self) -> ExactResultKind {
        match self {
            Self::Head => ExactResultKind::Head,
            Self::Files => ExactResultKind::Files,
            Self::Full => ExactResultKind::Full,
        }
    }
}

/// One output kind's state.
#[derive(Debug, Clone, PartialEq)]
pub enum Out<T> {
    /// Not built yet. `error` is the last retryable failure, if any.
    Pending {
        error: Option<String>,
    },
    Ready(T),
    Failed(String),
}

impl<T> Out<T> {
    pub fn ready(&self) -> Option<&T> {
        match self {
            Self::Ready(output) => Some(output),
            _ => None,
        }
    }
}

/// One (repo, commit) build as stored.
#[derive(Debug, Clone)]
pub struct BuildRow {
    pub commit: String,
    /// Full ref name (`refs/heads/main`) of the first admit that named one.
    pub ref_name: Option<String>,
    /// Parent commit, written when Head is published.
    pub parent: Option<String>,
    /// Admission time, epoch seconds.
    pub created_at: i64,
    pub head: Out<HeadResult>,
    pub files: Out<FilesResult>,
    pub full: Out<FullResult>,
    pub head_lease: Lease,
    pub files_lease: Lease,
    pub full_lease: Lease,
}

impl BuildRow {
    /// The lease and attempts of one output kind.
    pub fn lease(&self, kind: ExactResultKind) -> Lease {
        match kind {
            ExactResultKind::Head => self.head_lease,
            ExactResultKind::Files => self.files_lease,
            ExactResultKind::Full => self.full_lease,
        }
    }
}

/// One output kind's lease and attempts. Each kind of a row has its own.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Lease {
    /// Claims of this kind since admission or the last retry.
    pub attempts: i64,
    /// Lease expiry, epoch seconds. The kind is building while this is in
    /// the future.
    pub until: Option<i64>,
}

/// A leased output of a build row. `build` is the row as it was when
/// claimed. The claim holds the lease of `pool`'s kind only.
#[derive(Debug, Clone)]
pub struct Claim {
    pub repo: RepoId,
    pub owner: String,
    pub pool: Pool,
    pub build: BuildRow,
}

impl Claim {
    /// The output kind this claim builds.
    pub fn kind(&self) -> ExactResultKind {
        self.pool.kind()
    }

    /// Claims of this kind so far, this one included.
    pub fn attempts(&self) -> i64 {
        self.build.lease(self.kind()).attempts
    }
}

/// A finished output to publish for a claim.
#[derive(Debug, Clone)]
pub enum Output {
    Head {
        result: HeadResult,
        parent: Option<String>,
    },
    Files(FilesResult),
    Full(FullResult),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Published {
    /// The output was written. False when the claim no longer owns its
    /// kind's lease, the output is not the claim's kind, or the output is no
    /// longer pending.
    pub applied: bool,
    /// This publish made every output of the row ready.
    pub row_complete: bool,
}

/// A repository added to the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddedRepo {
    pub repo_id: RepoId,
    /// Epoch seconds.
    pub added_at: u64,
}

/// Epoch-seconds time source. Tests use a manual clock.
pub trait Clock: Send + Sync {
    fn now(&self) -> i64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

/// Builds, leases, repositories and repository config. Every method is one
/// statement or one short transaction; callers never see a transaction.
#[async_trait]
pub trait ControlStore: Send + Sync {
    /// Create the row for `commit` if it is new; returns the row and whether
    /// it was inserted. A later admit with a ref fills a missing `ref_name`
    /// and never replaces one.
    async fn admit(
        &self,
        repo: &RepoId,
        commit: &str,
        ref_name: Option<&str>,
    ) -> Result<(BuildRow, bool)>;
    /// Return failed outputs to pending with their attempts and backoff
    /// cleared. Ready and pending outputs are untouched. `None` if the row
    /// is unknown.
    async fn retry(&self, repo: &RepoId, commit: &str) -> Result<Option<BuildRow>>;
    /// Lease `pool`'s output of the oldest claimable row, by admission
    /// order (a strictly increasing sequence, so same-second admits keep
    /// their order). Head and Files are claimable while pending, Full once
    /// Head is ready. The kind must have no
    /// held lease, no retry time in the future and attempts left. A lease is
    /// held until it expires, and a lease this store claimed is held until
    /// it is released, even past expiry: a process that stalled longer than
    /// a lease still runs those builds and must not start them again. A
    /// repo that already holds a lease of `pool`'s kind is skipped, so one
    /// repo cannot fill a pool. A Full is claimable only when no older row
    /// of its repo has Full pending, so a repo's Fulls publish in admission
    /// order. First gives up on outputs of `pool`'s kind whose last allowed
    /// attempt is no longer held: that output fails, and a failed Head
    /// takes Full with it. The returned claim's owner is `owner` prefixed
    /// with this store's instance.
    async fn claim(&self, owner: &str, pool: Pool, lease: Duration) -> Result<Option<Claim>>;
    /// Extend the claim's lease. False when the claim no longer owns it: it
    /// expired and another store claimed the output. An expired lease nobody
    /// took renews.
    async fn renew(&self, claim: &Claim, lease: Duration) -> Result<bool>;
    /// Write a ready output of the claim's kind. Nothing is written for a
    /// claim whose lease was lost, or for another kind's output.
    async fn publish(&self, claim: &Claim, output: Output) -> Result<Published>;
    /// Record a failed build of the claim's kind. With `retry_after` and
    /// attempts left the output stays pending and is not claimable until
    /// the delay passes; otherwise it fails. A failed Head fails Files and
    /// Full too; a failed Files or Full fails alone. Like `publish`, a lost
    /// lease records nothing.
    async fn fail(&self, claim: &Claim, error: &str, retry_after: Option<Duration>) -> Result<()>;
    /// Give up the claim's lease. Other kinds' leases of the row stay.
    async fn release(&self, claim: &Claim) -> Result<()>;
    /// Clear every lease of every kind. Returns the number of rows that
    /// held one. Only safe while this process is the single owner.
    async fn reset_leases(&self) -> Result<u64>;
    async fn status(&self, repo: &RepoId, commit: &str) -> Result<Option<BuildRow>>;
    /// Every row of `repo`, oldest admission first.
    async fn list_builds(&self, repo: &RepoId) -> Result<Vec<BuildRow>>;
    /// Newest row admitted from `ref_name` whose Full output is ready.
    async fn latest_ready_full(
        &self,
        repo: &RepoId,
        ref_name: &str,
    ) -> Result<Option<(String, FullResult)>>;
    /// Up to `limit` commits of `repo` other than `except` whose `kind`
    /// output is ready: rows admitted from `prefer_ref` first, then the
    /// rest, each newest admission first.
    async fn ready_commits(
        &self,
        repo: &RepoId,
        kind: ExactResultKind,
        prefer_ref: Option<&str>,
        except: &str,
        limit: usize,
    ) -> Result<Vec<String>>;
    /// False when the repository was already added.
    async fn add_repo(&self, repo: &RepoId) -> Result<bool>;
    /// False when the repository was not added.
    async fn remove_repo(&self, repo: &RepoId) -> Result<bool>;
    async fn get_repo(&self, repo: &RepoId) -> Result<Option<AddedRepo>>;
    async fn list_repos(&self) -> Result<Vec<AddedRepo>>;
    async fn health(&self) -> Result<()>;
}

#[cfg(unix)]
struct ControlPathLock {
    _file: std::fs::File,
}

#[cfg(unix)]
impl ControlPathLock {
    fn acquire(path: &Path) -> Result<Self> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;

        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".owner");
        let lock_path = PathBuf::from(lock_path);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create control lock directory {}", parent.display()))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .with_context(|| format!("open control ownership lock {}", lock_path.display()))?;
        // SAFETY: `file` owns a live descriptor for the duration of the call;
        // `flock` neither retains the descriptor nor dereferences Rust memory.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            bail!(
                "control database {} is already owned by another server: {}",
                path.display(),
                error
            );
        }
        Ok(Self { _file: file })
    }
}

#[cfg(unix)]
impl Drop for ControlPathLock {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;

        // A concurrently spawned test/helper process can briefly inherit this
        // descriptor between fork and exec. Explicitly unlock the shared file
        // description so ownership ends with the store even if that child has
        // not exited yet; closing our descriptor alone would leave the inherited
        // duplicate holding the lock.
        // SAFETY: `_file` owns a live descriptor for this lock file, and
        // `flock(LOCK_UN)` neither dereferences memory nor transfers ownership.
        unsafe {
            libc::flock(self._file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[cfg(not(unix))]
struct ControlPathLock;

#[cfg(not(unix))]
impl ControlPathLock {
    fn acquire(path: &Path) -> Result<Self> {
        bail!(
            "exclusive control database ownership is unsupported on this platform: {}",
            path.display()
        )
    }
}
