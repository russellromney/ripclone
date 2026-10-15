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

/// Which claimers a build row is offered to. A row moves through the pools
/// in order: Head while Head is pending, then Files once Head is ready, then
/// Full once Files is ready or failed and no older row of the repo has Full
/// pending. Files is fast, so a new push's Files never waits behind another
/// commit's Full, and one row has one lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pool {
    Head,
    Files,
    Full,
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
    /// Claims in the current phase (Head, Files, then Full).
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
    /// Lease the oldest claimable row of `pool`, by admission order (a
    /// strictly increasing sequence, so same-second admits keep their
    /// order). A repo that already holds a live lease of `pool` is skipped,
    /// so one repo cannot fill a pool. A Full row is claimable only when no
    /// older row of its repo has Full pending, so a repo's Fulls publish in
    /// admission order. First gives up on rows whose
    /// lease expired after the last allowed attempt: the current phase's
    /// output fails (a pending Head takes Files and Full with it), and a
    /// failed Files hands the row to the Full phase.
    async fn claim(&self, owner: &str, pool: Pool, lease: Duration) -> Result<Option<Claim>>;
    /// Extend the lease. False when the claim no longer owns the row or the
    /// lease already expired: an expired lease is lost even if nobody has
    /// claimed the row since.
    async fn renew(&self, claim: &Claim, lease: Duration) -> Result<bool>;
    /// Write a ready output. Publishing Head or Files ends that phase and
    /// clears attempts for the next one. Nothing is written for a claim
    /// whose lease was lost or expired.
    async fn publish(&self, claim: &Claim, output: Output) -> Result<Published>;
    /// Record a failed build of `kind`. With `retry_after` and attempts left
    /// the output stays pending and the row is not claimable until the delay
    /// passes; otherwise it fails. A failed Head fails Files and Full too; a
    /// failed Files starts the Full phase. Like `publish`, a lost or
    /// expired lease records nothing.
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

/// How a removed environment variable is treated when it is set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Removal {
    /// A removed control setting: the server refuses to start.
    Refuse,
    /// No effect: the server and the CLI warn once at startup.
    Warn,
}

/// Environment variables removed in the rebuild (from `git log -S`). A
/// trailing `*` matches a prefix; the first matching row wins.
const REMOVED_ENVIRONMENT: &[(&str, Removal)] = &[
    ("RIPCLONE_METADATA", Removal::Refuse),
    ("RIPCLONE_METADATA_DB_URL", Removal::Refuse),
    ("RIPCLONE_METADATA_DB_TOKEN", Removal::Refuse),
    ("RIPCLONE_QUEUE", Removal::Refuse),
    ("RIPCLONE_QUEUE_DB_URL", Removal::Refuse),
    ("RIPCLONE_QUEUE_DB_TOKEN", Removal::Refuse),
    ("RIPCLONE_DISPATCH", Removal::Refuse),
    ("RIPCLONE_DISPATCH_CMD", Removal::Refuse),
    ("RIPCLONE_DISPATCH_CMD_ARGS", Removal::Refuse),
    ("RIPCLONE_DISPATCH_INTERVAL_SECS", Removal::Refuse),
    ("RIPCLONE_DISPATCH_MAX_WORKERS", Removal::Refuse),
    ("RIPCLONE_DISPATCH_TOKEN", Removal::Refuse),
    ("RIPCLONE_DISPATCH_URL", Removal::Refuse),
    ("RIPCLONE_HEARTBEAT_URL", Removal::Refuse),
    ("RIPCLONE_RECHECK_MAX", Removal::Refuse),
    ("RIPCLONE_REF_CACHE_TTL_SECS", Removal::Refuse),
    ("RIPCLONE_METADATA_*", Removal::Warn),
    ("RIPCLONE_QUEUE_*", Removal::Warn),
    ("RIPCLONE_DISPATCH_*", Removal::Warn),
    ("RIPCLONE_TEST_*", Removal::Warn),
    ("RIPCLONE_TESTING", Removal::Warn),
    ("RIPCLONE_WORKER_HEARTBEAT*", Removal::Warn),
    ("RIPCLONE_JWT_*", Removal::Warn),
    ("RIPCLONE_JOB_TOKEN_SECRET", Removal::Warn),
    ("RIPCLONE_OIDC_AUDIENCE", Removal::Warn),
    ("RIPCLONE_TRUST_GATEWAY", Removal::Warn),
    ("RIPCLONE_TURSO_*", Removal::Warn),
    ("RIPCLONE_LSM", Removal::Warn),
    ("RIPCLONE_SIZE_CLASSES", Removal::Warn),
    ("RIPCLONE_MAX_SIZE_CLASS", Removal::Warn),
    ("RIPCLONE_MAX_JOBS", Removal::Warn),
    ("RIPCLONE_IDLE_EXIT_SECS", Removal::Warn),
    ("RIPCLONE_BUILD_CONCURRENCY", Removal::Warn),
    ("RIPCLONE_ARCHIVE_BOUNDED", Removal::Warn),
    ("RIPCLONE_NO_OVERLAY", Removal::Warn),
    ("RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS", Removal::Warn),
    ("RIPCLONE_STAGING_DIR", Removal::Warn),
    ("RIPCLONE_TEMP", Removal::Warn),
    ("RIPCLONE_BACKGROUND_SLOTS", Removal::Warn),
    ("RIPCLONE_IO_URING", Removal::Warn),
];

fn removal(name: &str) -> Option<Removal> {
    REMOVED_ENVIRONMENT
        .iter()
        .find(|(pattern, _)| match pattern.strip_suffix('*') {
            Some(prefix) => name.starts_with(prefix),
            None => name == *pattern,
        })
        .map(|(_, removal)| *removal)
}

/// The removed variables set in this process's environment, sorted by name.
pub fn removed_environment() -> Vec<(String, Removal)> {
    let mut found: Vec<(String, Removal)> = std::env::vars_os()
        .filter_map(|(name, _)| {
            let name = name.into_string().ok()?;
            let removal = removal(&name)?;
            Some((name, removal))
        })
        .collect();
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}

pub fn validate_removed_environment() -> Result<()> {
    let configured: Vec<String> = removed_environment()
        .into_iter()
        .filter(|(_, removal)| *removal == Removal::Refuse)
        .map(|(name, _)| name)
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

#[cfg(test)]
mod removed_environment_tests {
    use super::*;

    #[test]
    fn removed_environment_rows_match_names_and_prefixes() {
        for (name, want) in [
            ("RIPCLONE_QUEUE", Some(Removal::Refuse)),
            ("RIPCLONE_QUEUE_DB_URL", Some(Removal::Refuse)),
            ("RIPCLONE_QUEUE_STALE_SECS", Some(Removal::Warn)),
            ("RIPCLONE_DISPATCH_SIZE_LARGE", Some(Removal::Warn)),
            ("RIPCLONE_TEST_REF_POLL_MS", Some(Removal::Warn)),
            ("RIPCLONE_WORKER_HEARTBEAT", Some(Removal::Warn)),
            ("RIPCLONE_LSM", Some(Removal::Warn)),
            ("RIPCLONE_BACKGROUND_SLOTS", Some(Removal::Warn)),
            ("RIPCLONE_FILES_SLOTS", None),
            ("RIPCLONE_LSM_MAX_LEVELS", None),
            ("RIPCLONE_TRUST_FORWARDED_FOR", None),
            ("RIPCLONE_CONTROL_DB_PATH", None),
            ("RIPCLONE_QUEUED", None),
        ] {
            assert_eq!(removal(name), want, "{name}");
        }
    }
}
