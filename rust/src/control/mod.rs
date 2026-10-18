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
    /// Admission order: a later admit has a larger one.
    pub seq: i64,
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

impl Output {
    /// Every artifact the output names.
    pub fn artifacts(&self) -> std::collections::BTreeSet<&str> {
        use crate::Artifacts;
        match self {
            Self::Head { result, .. } => result.artifacts(),
            Self::Files(result) => result.artifacts(),
            Self::Full(result) => result.artifacts(),
        }
    }
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

/// What a purge removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Purged {
    /// Build rows deleted.
    pub builds: u64,
    /// Artifacts queued for deletion: those no other repository names.
    pub queued: u64,
}

/// A repository added to the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddedRepo {
    pub repo_id: RepoId,
    /// Epoch seconds.
    pub added_at: u64,
}

/// A repository's published refs: the newest tags artifact, and the
/// branches artifact and side pack of the last refresh.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepoRefs {
    /// CAS id of the zstd-compressed `TagsArtifact`.
    pub tags_artifact: String,
    pub tags_len: u64,
    /// Tags in the artifact; a clone downloads nothing when there are none.
    pub tags_count: u64,
    /// Hash of the mirror's `refs/tags/` listing the tags artifact was
    /// built from.
    pub tags_hash: String,
    /// What the last refresh published; `None` before the first.
    pub branches: Option<RepoBranches>,
    /// The newest earlier `branches` whose base had a ready Full when a
    /// refresh replaced them. A full clone of another commit installs these
    /// while `branches`' base has no ready Full, rather than fetching every
    /// ref from the upstream.
    pub ready: Option<RepoBranches>,
}

impl RepoRefs {
    /// Every stored artifact the refs name.
    pub fn artifacts(&self) -> std::collections::BTreeSet<&str> {
        let mut hashes = std::collections::BTreeSet::from([self.tags_artifact.as_str()]);
        for branches in self.branches.iter().chain(&self.ready) {
            hashes.insert(&branches.artifact);
            hashes.insert(&branches.tags.artifact);
            if let Some(side) = &branches.side_pack {
                hashes.insert(&side.pack);
                hashes.insert(&side.idx);
            }
        }
        hashes.retain(|hash| !hash.is_empty());
        hashes
    }
}

/// The branches a refresh published, for full clones.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct RepoBranches {
    /// CAS id of the zstd-compressed `BranchesArtifact`.
    pub artifact: String,
    pub len: u64,
    /// `branches::refs_hash` of the mirror's refs it was built from, which
    /// the poller compares with upstream's.
    pub refs_hash: String,
    /// The branch the mirror's `HEAD` named; `None` when it named none.
    pub default_branch: Option<String>,
    /// The default branch's commit.
    pub base: Option<String>,
    /// The commit the side pack leaves out the history of: `base`, or an
    /// older default branch commit in its history when the side pack was
    /// kept from an earlier refresh.
    pub side_base: Option<String>,
    /// `branches::MirrorRefs::side_tips_hash` of the refs the side pack
    /// was built from.
    pub side_tips: String,
    /// `None` when every ref is in the default branch's history.
    pub side_pack: Option<SidePack>,
    /// The tags artifact built from the same listing, so these branches
    /// can be served with the tags of their time once newer ones replace
    /// them.
    pub tags: PublishedTags,
}

/// A tags artifact as a refresh published it.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct PublishedTags {
    pub artifact: String,
    pub len: u64,
    pub count: u64,
    /// Hash of the `refs/tags/` listing it was built from.
    pub hash: String,
}

/// The CAS ids and lengths of a side pack and its idx.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct SidePack {
    pub pack: String,
    pub pack_len: u64,
    pub idx: String,
    pub idx_len: u64,
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
    /// Write a ready output of the claim's kind, and record that the repo
    /// names each of its artifacts. Nothing is written for a claim whose
    /// lease was lost, or for another kind's output.
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
    /// Delete the repository, in one transaction: drop it from the added
    /// set, delete its build rows, refs and artifact references, and queue each
    /// artifact it named that no other repository names, due at `due_at`.
    /// A leased build of it can no longer renew, publish or fail.
    async fn purge_repo(&self, repo: &RepoId, due_at: i64) -> Result<Purged>;
    /// Queue artifacts for deletion at `due_at`. An artifact already queued
    /// keeps the later due time.
    async fn queue_deletions(&self, hashes: &[String], due_at: i64) -> Result<()>;
    /// Up to `limit` queued artifacts due before `now`, soonest first. An
    /// artifact queued during a sweep is due no earlier than the sweep's
    /// `now`, so the sweep's dequeue leaves it queued.
    async fn due_deletions(&self, now: i64, limit: usize) -> Result<Vec<String>>;
    /// Whether `hash` may be deleted now: queued, due before `now`, and
    /// named by no repository's published output.
    async fn deletable(&self, hash: &str, now: i64) -> Result<bool>;
    /// Take each of `hashes` off the queue unless it was queued again for
    /// `now` or later.
    async fn dequeue_deletions(&self, hashes: &[String], now: i64) -> Result<()>;
    /// Whether `repo` is added or has build rows.
    async fn knows_repo(&self, repo: &RepoId) -> Result<bool>;
    /// Every repository that is added or has build rows.
    async fn known_repos(&self) -> Result<Vec<RepoId>>;
    /// The repository's published refs; `None` before its first publish.
    async fn get_refs(&self, repo: &RepoId) -> Result<Option<RepoRefs>>;
    /// Replace the repository's published refs, and record that the repo
    /// names each of their artifacts. Nothing is written for a repo that is
    /// no longer known, such as one purged while its refs were built.
    /// Returns whether the refs were written.
    async fn set_refs(&self, repo: &RepoId, refs: &RepoRefs) -> Result<bool>;
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
