//! The server-owned SQLite control database.
//!
//! One `builds` row per (repo, commit) with a state per output kind, leases
//! instead of jobs, and added repositories with their config.
//! [`sqlite::SqliteControlStore`] implements [`ControlStore`].

use crate::provider::RepoId;
use crate::repo_config::RepoConfig;
use crate::{ExactResultKind, FilesResult, FullResult, HeadResult};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(test)]
pub mod conformance;
pub mod sqlite;

pub use sqlite::SqliteControlStore;

/// Which claimers a build row is offered to. Head builds are latency-critical;
/// Files and Full run in the background once Head is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pool {
    Head,
    Background,
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
    /// Claims in the current phase (Head, then Background).
    pub attempts: i64,
    /// Lease expiry, epoch seconds. The row is building while this is in the
    /// future.
    pub lease_until: Option<i64>,
}

/// A leased build row. `build` is the row as it was when claimed.
#[derive(Debug, Clone)]
pub struct Claim {
    pub repo: RepoId,
    pub owner: String,
    pub build: BuildRow,
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
    /// The output was written. False when the claim no longer owns the row
    /// or the output was already ready.
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
    /// Return failed outputs to pending with attempts and backoff cleared.
    /// Ready and pending outputs are untouched. `None` if the row is unknown.
    async fn retry(&self, repo: &RepoId, commit: &str) -> Result<Option<BuildRow>>;
    /// Lease the oldest claimable row of `pool`. First fails the pending
    /// outputs of rows whose lease expired after the last allowed attempt.
    async fn claim(&self, owner: &str, pool: Pool, lease: Duration) -> Result<Option<Claim>>;
    /// Extend the lease. False when the claim no longer owns the row.
    async fn renew(&self, claim: &Claim, lease: Duration) -> Result<bool>;
    async fn publish(&self, claim: &Claim, output: Output) -> Result<Published>;
    /// Record a failed build of `kind`. With `retry_after` and attempts left
    /// the output stays pending and the row is not claimable until the delay
    /// passes; otherwise it fails. A failed Head fails Files and Full too.
    async fn fail(
        &self,
        claim: &Claim,
        kind: ExactResultKind,
        error: &str,
        retry_after: Option<Duration>,
    ) -> Result<()>;
    async fn release(&self, claim: &Claim) -> Result<()>;
    /// Clear every lease. Only safe while this process is the single owner.
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
    /// False when the repository was already added.
    async fn add_repo(&self, repo: &RepoId) -> Result<bool>;
    /// False when the repository was not added.
    async fn remove_repo(&self, repo: &RepoId) -> Result<bool>;
    async fn get_repo(&self, repo: &RepoId) -> Result<Option<AddedRepo>>;
    async fn list_repos(&self) -> Result<Vec<AddedRepo>>;
    /// The stored config; `None` when none is stored (builds use the default).
    async fn get_config(&self, repo: &RepoId) -> Result<Option<RepoConfig>>;
    async fn set_config(&self, repo: &RepoId, config: &RepoConfig) -> Result<()>;
    async fn health(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct ControlSettings {
    pub path: PathBuf,
}

impl ControlSettings {
    /// Resolve and validate every control selector before any runtime component
    /// creates a directory, opens storage, binds a listener, or starts work.
    pub fn from_sources(explicit_path: Option<&Path>, default_path: &Path) -> Result<Self> {
        validate_removed_environment()?;
        let config = crate::config::load_global();
        validate_removed_config(&config)?;
        let environment_path = std::env::var_os("RIPCLONE_CONTROL_DB_PATH");
        if environment_path
            .as_ref()
            .is_some_and(|value| value.to_string_lossy().trim().is_empty())
        {
            bail!("RIPCLONE_CONTROL_DB_PATH must not be empty");
        }
        validate_nonempty_config("control.path", config.control.path.as_deref())?;
        let path = explicit_path
            .map(Path::to_path_buf)
            .or_else(|| environment_path.map(PathBuf::from))
            .or_else(|| config.control.path.as_deref().map(PathBuf::from))
            .unwrap_or_else(|| default_path.to_path_buf());
        if path.as_os_str().to_string_lossy().trim().is_empty() {
            bail!("RIPCLONE_CONTROL_DB_PATH must not be empty");
        }
        Ok(Self { path })
    }
}

fn validate_nonempty_config(key: &str, value: Option<&str>) -> Result<()> {
    if value.is_some_and(|value| value.trim().is_empty()) {
        bail!("{key} must not be empty");
    }
    Ok(())
}

const REMOVED_ENVIRONMENT: &[&str] = &[
    "RIPCLONE_METADATA",
    "RIPCLONE_METADATA_DB_URL",
    "RIPCLONE_METADATA_DB_TOKEN",
    "RIPCLONE_QUEUE",
    "RIPCLONE_QUEUE_DB_URL",
    "RIPCLONE_QUEUE_DB_TOKEN",
    "RIPCLONE_DISPATCH",
    "RIPCLONE_DISPATCH_CMD",
    "RIPCLONE_DISPATCH_CMD_ARGS",
    "RIPCLONE_DISPATCH_INTERVAL_SECS",
    "RIPCLONE_DISPATCH_MAX_WORKERS",
    "RIPCLONE_DISPATCH_TOKEN",
    "RIPCLONE_DISPATCH_URL",
    "RIPCLONE_HEARTBEAT_URL",
    "RIPCLONE_RECHECK_MAX",
    "RIPCLONE_REF_CACHE_TTL_SECS",
];

pub fn validate_removed_environment() -> Result<()> {
    let configured: Vec<&str> = REMOVED_ENVIRONMENT
        .iter()
        .copied()
        .filter(|key| std::env::var_os(key).is_some())
        .collect();
    if configured.is_empty() {
        return Ok(());
    }
    bail!(
        "removed control configuration is set: {}; use RIPCLONE_CONTROL_DB_PATH",
        configured.join(", ")
    )
}

fn validate_removed_config(config: &crate::config::Config) -> Result<()> {
    if config.removed_metadata.is_some() {
        bail!(
            "removed [metadata] configuration is present; refs now live in the server-owned [control] database"
        );
    }
    if config.removed_queue.is_some() {
        bail!(
            "removed [queue] backend/url/token configuration is present; jobs now live in the server-owned [control] database"
        );
    }
    Ok(())
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
