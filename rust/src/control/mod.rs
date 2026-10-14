//! The server-owned SQLite control database.
//!
//! Refs, added repositories, repository build settings, durable jobs, attempts,
//! and claims share one schema and one local SQLite path.
//!
//! [`ControlStore`] is the replacement control plane: one `builds` row per
//! (repo, commit) with a state per output kind, leases instead of jobs, and
//! repositories and their config. [`sqlite::SqliteControlStore`] implements
//! it. The server does not use it yet; [`ControlDb`] below is the current one.

use crate::meta::{LibsqlMeta, SqlRefStore};
use crate::provider::RepoId;
use crate::queue::{BuildJob, EnqueueOutcome, Enqueued, LibsqlDb, SqlJobQueue};
use crate::ref_store::RefStore;
use crate::repo_config::RepoConfig;
use crate::{ExactResultKind, FilesResult, FullResult, HeadResult};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use libsql::{Builder, Database, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
pub mod conformance;
pub mod sqlite;

pub use sqlite::SqliteControlStore;

const SCHEMA_VERSION: i64 = 3;

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
    /// The stored config, or the default when none is stored.
    async fn get_config(&self, repo: &RepoId) -> Result<RepoConfig>;
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

/// One concrete control database and the two existing domain-facing views over
/// its shared connection handle.
pub struct ControlDb {
    database: Arc<Database>,
    refs: Arc<SqlRefStore>,
    queue: Arc<SqlJobQueue>,
    path: PathBuf,
    /// Process-local wake hint for embedded claimers. The jobs table remains
    /// authoritative; notifications are emitted only after admission commits.
    admission_notify: Arc<tokio::sync::Notify>,
    _ownership: ControlPathLock,
}

impl ControlDb {
    /// Open the configured local path and fail closed on an incompatible
    /// pre-existing database. The ownership lock is held for this value's
    /// lifetime and is acquired before the database is opened.
    pub async fn open(path: &Path) -> Result<Self> {
        let ownership = ControlPathLock::acquire(path)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create control directory {}", parent.display()))?;
        }

        preflight_sqlite_schema(path).await?;
        let database = Arc::new(
            Builder::new_local(path)
                .build()
                .await
                .with_context(|| format!("open control database {}", path.display()))?,
        );
        database
            .connect()
            .context("connect to control database")?
            .execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")
            .await
            .context("configure local control database")?;
        validate_or_initialize_schema(&database, path).await?;
        let refs = Arc::new(SqlRefStore::new(LibsqlMeta::from_database(database.clone())).await?);
        let queue = Arc::new(SqlJobQueue::new(LibsqlDb::from_database(database.clone())).await?);
        Ok(Self {
            database,
            refs,
            queue,
            path: path.to_path_buf(),
            admission_notify: Arc::new(tokio::sync::Notify::new()),
            _ownership: ownership,
        })
    }

    pub fn ref_store(&self) -> Arc<dyn RefStore> {
        self.refs.clone()
    }

    pub fn queue(&self) -> Arc<SqlJobQueue> {
        self.queue.clone()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn admission_notifier(&self) -> Arc<tokio::sync::Notify> {
        self.admission_notify.clone()
    }

    pub async fn publish_head_for_claim(
        &self,
        job_id: i64,
        worker_id: &str,
        repo_id: &crate::provider::RepoId,
        commit: &str,
        head: crate::HeadResult,
    ) -> Result<bool> {
        crate::validation::validate_object_id(commit).context("validate claimed result commit")?;
        anyhow::ensure!(
            crate::exact_output_artifacts_ready(
                commit,
                crate::ExactResultKind::Head,
                &head.clonepack,
            ),
            "invalid claimed Head result for {commit}"
        );
        let connection = self
            .database
            .connect()
            .context("connect for claimed result write")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let tx = connection
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .context("begin claimed result write")?;
        if !claim_authorizes(&tx, job_id, worker_id, repo_id, commit).await? {
            tx.rollback().await.ok();
            return Ok(false);
        }
        let repo_key = repo_id.storage_key();
        let mut rows = tx
            .query(
                "SELECT data FROM results WHERE repo_key = ?1 AND commit_id = ?2",
                libsql::params![repo_key.as_str(), commit],
            )
            .await
            .context("read claimed exact result")?;
        let Some(row) = rows.next().await? else {
            tx.rollback().await.ok();
            return Ok(false);
        };
        let mut result: crate::RefInfo =
            serde_json::from_str(&row.get::<String>(0)?).context("decode claimed exact result")?;
        drop(rows);
        anyhow::ensure!(
            result.commit == commit,
            "stored exact result identity mismatch"
        );
        result.head = Some(head);
        let data = serde_json::to_string(&result).context("encode claimed Head result")?;
        tx.execute(
            "UPDATE results SET data = ?1 WHERE repo_key = ?2 AND commit_id = ?3",
            libsql::params![data, repo_key, commit],
        )
        .await
        .context("write claimed Head result")?;
        tx.commit().await.context("commit claimed Head result")?;
        Ok(true)
    }

    pub async fn publish_full_for_claim(
        &self,
        job_id: i64,
        worker_id: &str,
        repo_id: &crate::provider::RepoId,
        commit: &str,
        full: crate::FullResult,
    ) -> Result<bool> {
        crate::validation::validate_object_id(commit).context("validate claimed Full commit")?;
        anyhow::ensure!(
            crate::exact_output_artifacts_ready(
                commit,
                crate::ExactResultKind::Full,
                &full.clonepack,
            ),
            "invalid claimed Full result for {commit}"
        );
        let connection = self
            .database
            .connect()
            .context("connect for claimed Full write")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let tx = connection
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .context("begin claimed Full write")?;
        if !claim_authorizes(&tx, job_id, worker_id, repo_id, commit).await? {
            tx.rollback().await.ok();
            return Ok(false);
        }
        let repo_key = repo_id.storage_key();
        let mut rows = tx
            .query(
                "SELECT data FROM results WHERE repo_key = ?1 AND commit_id = ?2",
                libsql::params![repo_key.as_str(), commit],
            )
            .await
            .context("read exact result for claimed Full")?;
        let Some(row) = rows.next().await? else {
            tx.rollback().await.ok();
            return Ok(false);
        };
        let mut result: crate::RefInfo = serde_json::from_str(&row.get::<String>(0)?)
            .context("decode exact result for claimed Full")?;
        drop(rows);
        anyhow::ensure!(
            result.commit == commit,
            "stored exact result identity mismatch"
        );
        if !crate::exact_output_ready(&result, crate::ExactResultKind::Full, commit) {
            result.full = Some(full);
            let data = serde_json::to_string(&result).context("encode claimed Full")?;
            tx.execute(
                "UPDATE results SET data = ?1 WHERE repo_key = ?2 AND commit_id = ?3",
                libsql::params![data, repo_key, commit],
            )
            .await
            .context("write claimed Full")?;
        }
        tx.commit().await.context("commit claimed Full write")?;
        Ok(true)
    }

    pub async fn publish_files_for_claim(
        &self,
        job_id: i64,
        worker_id: &str,
        repo_id: &crate::provider::RepoId,
        commit: &str,
        files: crate::FilesResult,
    ) -> Result<bool> {
        crate::validation::validate_object_id(commit).context("validate claimed Files commit")?;
        anyhow::ensure!(
            crate::exact_output_artifacts_ready(
                commit,
                crate::ExactResultKind::Files,
                &files.clonepack,
            ),
            "invalid claimed Files result for {commit}"
        );
        let connection = self
            .database
            .connect()
            .context("connect for claimed Files write")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let tx = connection
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .context("begin claimed Files write")?;
        if !claim_authorizes(&tx, job_id, worker_id, repo_id, commit).await? {
            tx.rollback().await.ok();
            return Ok(false);
        }
        let repo_key = repo_id.storage_key();
        let mut rows = tx
            .query(
                "SELECT data FROM results WHERE repo_key = ?1 AND commit_id = ?2",
                libsql::params![repo_key.as_str(), commit],
            )
            .await
            .context("read exact result for claimed Files")?;
        let Some(row) = rows.next().await? else {
            tx.rollback().await.ok();
            return Ok(false);
        };
        let mut result: crate::RefInfo = serde_json::from_str(&row.get::<String>(0)?)
            .context("decode exact result for claimed Files")?;
        drop(rows);
        anyhow::ensure!(
            result.commit == commit,
            "stored exact result identity mismatch"
        );
        if !crate::exact_output_ready(&result, crate::ExactResultKind::Files, commit) {
            result.files = Some(files);
            let data = serde_json::to_string(&result).context("encode claimed Files")?;
            tx.execute(
                "UPDATE results SET data = ?1 WHERE repo_key = ?2 AND commit_id = ?3",
                libsql::params![data, repo_key, commit],
            )
            .await
            .context("write claimed Files")?;
        }
        tx.commit().await.context("commit claimed Files write")?;
        Ok(true)
    }

    /// Load the single repository-level build configuration record. Only a
    /// genuinely absent row returns `None`; database, decode, and validation
    /// failures are propagated so admission cannot silently select defaults.
    pub async fn repository_config(
        &self,
        repo_id: &crate::provider::RepoId,
    ) -> Result<Option<crate::repo_config::RepoConfig>> {
        let connection = self
            .database
            .connect()
            .context("connect to read repository config")?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .context("configure repository config busy timeout")?;
        let mut rows = connection
            .query(
                "SELECT data FROM repository_configs WHERE repo_key = ?1",
                [repo_id.storage_key()],
            )
            .await
            .with_context(|| format!("read repository config {}", repo_id.storage_key()))?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let config: crate::repo_config::RepoConfig =
            serde_json::from_str(&row.get::<String>(0)?)
                .with_context(|| format!("decode repository config {}", repo_id.storage_key()))?;
        config
            .validate()
            .with_context(|| format!("validate repository config {}", repo_id.storage_key()))?;
        Ok(Some(config))
    }

    /// Store one validated repository-level build configuration record.
    pub async fn put_repository_config(
        &self,
        repo_id: &crate::provider::RepoId,
        config: &crate::repo_config::RepoConfig,
    ) -> Result<()> {
        config
            .validate()
            .with_context(|| format!("validate repository config {}", repo_id.storage_key()))?;
        let data = serde_json::to_string(config).context("encode repository config")?;
        let connection = self
            .database
            .connect()
            .context("connect to write repository config")?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .context("configure repository config busy timeout")?;
        connection
            .execute(
                "INSERT INTO repository_configs(repo_key, data) VALUES (?1, ?2)
                 ON CONFLICT(repo_key) DO UPDATE SET data = excluded.data",
                libsql::params![repo_id.storage_key(), data],
            )
            .await
            .with_context(|| format!("write repository config {}", repo_id.storage_key()))?;
        Ok(())
    }

    /// Atomically create the exact pending result and enqueue or join its one
    /// durable job. No worker can observe a job without its exact result row.
    pub(crate) async fn admit_exact_and_job(
        &self,
        job: &BuildJob,
        pending: &crate::RefInfo,
    ) -> Result<Enqueued> {
        job.repo_config
            .validate()
            .context("validate admitted repository config")?;
        let repo_config =
            serde_json::to_string(&job.repo_config).context("encode admitted repository config")?;
        let result = self.admit(&self.database, job, &repo_config, pending).await;
        if result.is_ok() {
            self.admission_notify.notify_one();
        }
        result
    }

    async fn admit(
        &self,
        database: &Database,
        job: &BuildJob,
        repo_config: &str,
        pending: &crate::RefInfo,
    ) -> Result<Enqueued> {
        let connection = database
            .connect()
            .context("connect for durable admission")?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .context("configure durable admission busy timeout")?;
        let tx = connection
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .context("begin durable admission")?;
        let repo_key = job.repo_id.storage_key();
        let mut existing_rows = tx
            .query(
                "SELECT data FROM results WHERE repo_key = ?1 AND commit_id = ?2",
                libsql::params![repo_key.as_str(), job.admitted_commit.as_str()],
            )
            .await
            .context("read exact result during durable admission")?;
        let existing = match existing_rows.next().await? {
            Some(row) => Some(
                serde_json::from_str::<crate::RefInfo>(&row.get::<String>(0)?)
                    .context("decode exact result during durable admission")?,
            ),
            None => None,
        };
        drop(existing_rows);
        let mut rows = tx
            .query(
                "SELECT id FROM jobs WHERE key = ?1 AND status IN ('queued', 'claimed') LIMIT 1",
                [job.key().as_str()],
            )
            .await
            .context("load active job for exact result")?;
        let active_job_id = match rows.next().await? {
            Some(row) => Some(row.get::<i64>(0)?),
            None => None,
        };
        drop(rows);
        let all_results_ready = existing
            .as_ref()
            .is_some_and(|result| crate::exact_result_complete(result, &job.admitted_commit));
        if all_results_ready || active_job_id.is_some() {
            tx.commit()
                .await
                .context("commit coalesced exact admission")?;
            return Ok(Enqueued {
                outcome: EnqueueOutcome::Coalesced,
                job_id: active_job_id,
            });
        }
        insert_exact_result(&tx, job, pending).await?;
        let key = job.key();
        let credential = crate::queue::sql::encode_credential(job.credential.as_ref());
        let changed = tx
            .execute(
                "INSERT OR IGNORE INTO jobs
                 (key, provider, path, status, created_at, admitted_commit,
                  repo_config, credential, attempts)
                 VALUES (?1, ?2, ?3, 'queued', ?4, ?5, ?6, ?7, 0)",
                libsql::params![
                    key.clone(),
                    job.repo_id.provider.as_str(),
                    job.repo_id.path.as_str(),
                    crate::queue::sql::now_secs(),
                    job.admitted_commit.as_str(),
                    repo_config,
                    credential.as_deref()
                ],
            )
            .await
            .context("insert durable admitted job")?;
        let (outcome, id) = if changed == 1 {
            (EnqueueOutcome::Enqueued, tx.last_insert_rowid())
        } else {
            let mut rows = tx
                .query(
                    "SELECT id FROM jobs
                     WHERE key = ?1 AND status IN ('queued', 'claimed') LIMIT 1",
                    [key.as_str()],
                )
                .await
                .context("load coalesced durable job")?;
            let id = rows
                .next()
                .await?
                .context("coalesced durable job disappeared")?
                .get::<i64>(0)?;
            drop(rows);
            (EnqueueOutcome::Coalesced, id)
        };
        tx.commit().await.context("commit durable admission")?;
        Ok(Enqueued {
            outcome,
            job_id: Some(id),
        })
    }
}

async fn claim_authorizes(
    tx: &libsql::Transaction,
    job_id: i64,
    worker_id: &str,
    repo_id: &crate::provider::RepoId,
    commit: &str,
) -> Result<bool> {
    let mut rows = tx
        .query(
            "SELECT provider, path, admitted_commit, status, key FROM jobs
             WHERE id = ?1 AND worker_id = ?2",
            libsql::params![job_id, worker_id],
        )
        .await
        .context("verify claimed result owner")?;
    let Some(row) = rows.next().await? else {
        return Ok(false);
    };
    let provider = row.get::<String>(0)?;
    let path = row.get::<String>(1)?;
    let admitted_commit = row.get::<String>(2)?;
    let status = row.get::<String>(3)?;
    let _key = row.get::<String>(4)?;
    drop(rows);
    let identity_matches =
        provider == repo_id.provider.as_str() && path == repo_id.path && admitted_commit == commit;
    Ok(identity_matches && status == "claimed")
}

async fn insert_exact_result(
    tx: &libsql::Transaction,
    job: &BuildJob,
    info: &crate::RefInfo,
) -> Result<()> {
    let data = serde_json::to_string(info).context("serialize exact admission")?;
    tx.execute(
        "INSERT OR IGNORE INTO results(repo_key, commit_id, data)
             VALUES (?1, ?2, ?3)",
        libsql::params![
            job.repo_id.storage_key(),
            job.admitted_commit.as_str(),
            data
        ],
    )
    .await
    .context("create exact admission row")?;
    Ok(())
}

async fn preflight_sqlite_schema(path: &Path) -> Result<()> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect control database {}", path.display()));
        }
    };
    if metadata.len() == 0 {
        return Ok(());
    }

    let database = Builder::new_local(path)
        .flags(OpenFlags::SQLITE_OPEN_READ_ONLY)
        .build()
        .await
        .with_context(|| format!("inspect control database {} read-only", path.display()))?;
    let connection = database
        .connect()
        .context("connect to control database read-only")?;
    let mut rows = connection
        .query(
            "SELECT name FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            (),
        )
        .await
        .context("inspect existing control schema")?;
    let mut tables = Vec::new();
    while let Some(row) = rows.next().await? {
        tables.push(row.get::<String>(0)?);
    }
    drop(rows);
    if !tables.iter().any(|name| name == "control_schema") {
        bail!(
            "incompatible control database {}: missing schema marker (found tables: {}); automatic migration is not supported",
            path.display(),
            tables.join(", ")
        );
    }
    let mut rows = connection
        .query("SELECT version FROM control_schema WHERE id = 1", ())
        .await
        .context("read existing control schema version")?;
    let version = match rows.next().await? {
        Some(row) => Some(row.get::<i64>(0)?),
        None => None,
    };
    if version != Some(SCHEMA_VERSION) {
        bail!(
            "incompatible control database {}: expected schema version {}, found {:?}; automatic migration is not supported",
            path.display(),
            SCHEMA_VERSION,
            version
        );
    }
    Ok(())
}

async fn validate_or_initialize_schema(database: &Database, path: &Path) -> Result<()> {
    let connection = database.connect().context("connect to control database")?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .context("configure schema-check busy timeout")?;
    let tx = connection
        .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
        .await
        .context("begin control schema check")?;
    let mut rows = tx
        .query(
            "SELECT name FROM sqlite_master
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            (),
        )
        .await
        .context("inspect control schema")?;
    let mut tables = Vec::new();
    while let Some(row) = rows.next().await? {
        tables.push(row.get::<String>(0)?);
    }
    drop(rows);
    if tables.iter().any(|name| name == "control_schema") {
        let mut rows = tx
            .query("SELECT version FROM control_schema WHERE id = 1", ())
            .await
            .context("read control schema version")?;
        let version = match rows.next().await? {
            Some(row) => Some(row.get::<i64>(0)?),
            None => None,
        };
        drop(rows);
        if version != Some(SCHEMA_VERSION) {
            bail!(
                "incompatible control database {}: expected schema version {}, found {:?}; automatic migration is not supported",
                path.display(),
                SCHEMA_VERSION,
                version
            );
        }
    } else if !tables.is_empty() {
        bail!(
            "incompatible control database {}: missing schema marker (found tables: {}); automatic migration is not supported",
            path.display(),
            tables.join(", ")
        );
    } else {
        tx.execute(
            "CREATE TABLE control_schema (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 version INTEGER NOT NULL
             )",
            (),
        )
        .await
        .context("create control schema marker")?;
        tx.execute(
            "INSERT INTO control_schema(id, version) VALUES (1, ?1)",
            [SCHEMA_VERSION],
        )
        .await
        .context("write control schema version")?;
        tx.execute(
            "CREATE TABLE repository_configs (
                 repo_key TEXT PRIMARY KEY,
                 data TEXT NOT NULL
             )",
            (),
        )
        .await
        .context("create repository configs table")?;
    }
    tx.commit().await.context("commit control schema check")
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
        // description so ownership ends with ControlDb even if that child has
        // not exited yet; closing our descriptor alone would leave the inherited
        // duplicate holding the lock.
        // SAFETY: `_file` owns a live descriptor for this lock file, and
        // `flock(LOCK_UN)` neither dereferences memory nor transfers ownership.
        unsafe {
            libc::flock(self._file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{JobQueue, JobState};

    fn job(commit: &str, _checkout_name: &str) -> BuildJob {
        BuildJob {
            repo_id: crate::provider::RepoId::github("acme/control"),
            admitted_commit: commit.to_string(),
            repo_config: crate::repo_config::RepoConfig::default(),
            credential: None,
        }
    }

    fn pending(commit: &str) -> crate::RefInfo {
        crate::RefInfo {
            commit: commit.to_string(),
            ..Default::default()
        }
    }

    fn ready_artifacts(commit: &str, label: &str) -> crate::ClonepackArtifacts {
        let hash = |suffix: &str| crate::cas::hash(format!("{label}-{suffix}").as_bytes());
        crate::ClonepackArtifacts {
            manifest: hash("manifest"),
            metadata_chunk: hash("metadata"),
            skeleton_pack: hash("skeleton-pack"),
            skeleton_idx: hash("skeleton-idx"),
            prebuilt_index: hash("index"),
            idx_bundle: hash("idx-bundle"),
            commit: commit.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn repository_config_persists_and_absence_does_not_insert_defaults() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let repo_id = crate::provider::RepoId::github("acme/configured");
        let missing = crate::provider::RepoId::github("acme/missing");
        let config = crate::repo_config::RepoConfig {
            compression_level: Some(3),
            archive_chunk_size: Some(1024),
            ..Default::default()
        };
        let control = ControlDb::open(&path).await.unwrap();
        assert!(control.repository_config(&missing).await.unwrap().is_none());
        control
            .put_repository_config(&repo_id, &config)
            .await
            .unwrap();
        drop(control);
        let reopened = ControlDb::open(&path).await.unwrap();
        assert_eq!(
            reopened.repository_config(&repo_id).await.unwrap(),
            Some(config)
        );
        assert!(
            reopened
                .repository_config(&missing)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn corrupt_or_invalid_repository_config_fails_instead_of_defaulting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let control = ControlDb::open(&path).await.unwrap();
        let repo_id = crate::provider::RepoId::github("acme/corrupt");
        let connection = control.database.connect().unwrap();
        connection
            .execute(
                "INSERT INTO repository_configs(repo_key, data) VALUES (?1, ?2)",
                libsql::params![repo_id.storage_key(), "not-json"],
            )
            .await
            .unwrap();
        assert!(
            control
                .repository_config(&repo_id)
                .await
                .unwrap_err()
                .to_string()
                .contains("decode repository config")
        );
        connection
            .execute(
                "UPDATE repository_configs SET data = ?1 WHERE repo_key = ?2",
                libsql::params![r#"{"compression_level":99}"#, repo_id.storage_key()],
            )
            .await
            .unwrap();
        assert!(
            control
                .repository_config(&repo_id)
                .await
                .unwrap_err()
                .to_string()
                .contains("validate repository config")
        );
    }

    #[tokio::test]
    async fn admission_snapshots_repository_config() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let control = ControlDb::open(&path).await.unwrap();
        let repo_id = crate::provider::RepoId::github("acme/control");
        let admitted_config = crate::repo_config::RepoConfig {
            compression_level: Some(3),
            ..Default::default()
        };
        control
            .put_repository_config(&repo_id, &admitted_config)
            .await
            .unwrap();
        let commit = "dddddddddddddddddddddddddddddddddddddddd";
        let mut admitted_job = job(commit, "main");
        admitted_job.repo_config = control.repository_config(&repo_id).await.unwrap().unwrap();
        control
            .admit_exact_and_job(&admitted_job, &pending(commit))
            .await
            .unwrap();
        control
            .put_repository_config(
                &repo_id,
                &crate::repo_config::RepoConfig {
                    compression_level: Some(19),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let claimed = control
            .queue()
            .claim("snapshot-worker")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.repo_config, admitted_config);
    }

    #[tokio::test]
    async fn same_commit_different_names_share_one_result_and_job() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let control = Arc::new(ControlDb::open(&path).await.unwrap());
        let commit = "1111111111111111111111111111111111111111";
        let names = ["main", "release", "HEAD"];
        let mut admissions = Vec::new();
        for index in 0..32 {
            let control = control.clone();
            let admitted = job(commit, names[index % names.len()]);
            let pending = pending(commit);
            admissions.push(tokio::spawn(async move {
                control
                    .admit_exact_and_job(&admitted, &pending)
                    .await
                    .unwrap()
            }));
        }
        let mut enqueued = 0;
        let mut job_ids = std::collections::HashSet::new();
        for admission in admissions {
            let admission = admission.await.unwrap();
            enqueued += usize::from(admission.outcome == EnqueueOutcome::Enqueued);
            job_ids.insert(admission.job_id.unwrap());
        }
        assert_eq!(enqueued, 1);
        assert_eq!(job_ids.len(), 1);
        assert_eq!(control.queue().depth().await, 1);
        assert_eq!(
            control
                .ref_store()
                .list_commits(&job(commit, "main").repo_id)
                .await
                .unwrap(),
            vec![commit.to_string()]
        );

        let later = "2222222222222222222222222222222222222222";
        let admitted = control
            .admit_exact_and_job(&job(later, "main"), &pending(later))
            .await
            .unwrap();
        assert_eq!(admitted.outcome, EnqueueOutcome::Enqueued);
        assert_eq!(control.queue().depth().await, 2);
    }

    #[tokio::test]
    async fn accepted_job_and_exact_result_survive_restart() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let commit = "4444444444444444444444444444444444444444";
        let control = ControlDb::open(&path).await.unwrap();
        let admitted_id = control
            .admit_exact_and_job(&job(commit, "main"), &pending(commit))
            .await
            .unwrap()
            .job_id
            .unwrap();
        drop(control);
        let reopened = ControlDb::open(&path).await.unwrap();
        assert!(matches!(
            reopened.queue().job_status(admitted_id).await.unwrap(),
            JobState::Pending
        ));
        assert_eq!(
            reopened
                .ref_store()
                .load_result(&job(commit, "main").repo_id, commit)
                .await
                .unwrap()
                .unwrap()
                .commit,
            commit
        );
    }

    #[tokio::test]
    async fn stale_job_cannot_overwrite_any_ready_result() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let control = ControlDb::open(&path).await.unwrap();
        let commit = "5555555555555555555555555555555555555555";
        let repo_id = job(commit, "main").repo_id;
        let first_id = control
            .admit_exact_and_job(&job(commit, "main"), &pending(commit))
            .await
            .unwrap()
            .job_id
            .unwrap();
        let first = control.queue().claim("first-owner").await.unwrap().unwrap();
        assert_eq!(first.id, first_id);
        let head = crate::HeadResult {
            clonepack: ready_artifacts(commit, "ready-head"),
            ..Default::default()
        };
        assert!(
            control
                .publish_head_for_claim(first.id, "first-owner", &repo_id, commit, head.clone())
                .await
                .unwrap()
        );
        assert!(
            control
                .queue()
                .ack(
                    first.id,
                    "first-owner",
                    Err(crate::queue::BuildError::permanent("stopped")),
                )
                .await
                .unwrap()
        );

        let second_id = control
            .admit_exact_and_job(&job(commit, "main"), &pending(commit))
            .await
            .unwrap()
            .job_id
            .unwrap();
        let second = control
            .queue()
            .claim("second-owner")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.id, second_id);
        let full = crate::FullResult {
            clonepack: ready_artifacts(commit, "ready-full"),
            ..Default::default()
        };
        let files = crate::FilesResult {
            clonepack: ready_artifacts(commit, "ready-files"),
            ..Default::default()
        };
        assert!(
            control
                .publish_full_for_claim(second.id, "second-owner", &repo_id, commit, full.clone())
                .await
                .unwrap()
        );
        assert!(
            control
                .publish_files_for_claim(
                    second.id,
                    "second-owner",
                    &repo_id,
                    commit,
                    files.clone(),
                )
                .await
                .unwrap()
        );

        assert!(
            !control
                .publish_head_for_claim(
                    first.id,
                    "first-owner",
                    &repo_id,
                    commit,
                    crate::HeadResult {
                        clonepack: ready_artifacts(commit, "stale-head"),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
        );
        assert!(
            !control
                .publish_full_for_claim(
                    first.id,
                    "first-owner",
                    &repo_id,
                    commit,
                    crate::FullResult {
                        clonepack: ready_artifacts(commit, "stale-full"),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
        );
        assert!(
            !control
                .publish_files_for_claim(
                    first.id,
                    "first-owner",
                    &repo_id,
                    commit,
                    crate::FilesResult {
                        clonepack: ready_artifacts(commit, "stale-files"),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
        );
        let stored = control
            .ref_store()
            .load_result(&repo_id, commit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.head.unwrap().clonepack.manifest,
            head.clonepack.manifest
        );
        assert_eq!(
            stored.full.unwrap().clonepack.manifest,
            full.clonepack.manifest
        );
        assert_eq!(
            stored.files.unwrap().clonepack.manifest,
            files.clonepack.manifest
        );
    }

    #[tokio::test]
    async fn second_server_is_rejected_while_owner_is_alive() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let _owner = ControlDb::open(&path).await.unwrap();
        let error = ControlDb::open(&path).await.err().unwrap();
        assert!(
            error
                .to_string()
                .contains("already owned by another server")
        );
    }

    #[tokio::test]
    async fn incompatible_database_is_not_rewritten() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("old.db");
        let database = Builder::new_local(&path).build().await.unwrap();
        database
            .connect()
            .unwrap()
            .execute("CREATE TABLE legacy_refs (id INTEGER PRIMARY KEY)", ())
            .await
            .unwrap();
        drop(database);
        let before = std::fs::read(&path).unwrap();
        let error = ControlDb::open(&path).await.err().unwrap();
        assert!(
            error
                .to_string()
                .contains("automatic migration is not supported")
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
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
