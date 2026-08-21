//! The server-owned SQLite control database.
//!
//! Refs, added repositories, repository build settings, durable jobs, attempts,
//! claims, and worker heartbeats share one schema and one local path. Plain
//! SQLite is the default; the only replicated mode is a Turso embedded replica
//! at that same path.

use crate::meta::{LibsqlMeta, SqlRefStore};
use crate::queue::{BuildJob, EnqueueOutcome, Enqueued, LibsqlDb, SizeClass, SqlJobQueue};
use crate::ref_store::RefStore;
use anyhow::{Context, Result, bail};
use libsql::{Builder, Database, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const SCHEMA_VERSION: i64 = 3;

#[derive(Debug, Clone)]
pub struct TursoReplicaConfig {
    pub url: String,
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct ControlSettings {
    pub path: PathBuf,
    pub turso: Option<TursoReplicaConfig>,
    pub size_classes: Vec<SizeClass>,
}

impl ControlSettings {
    /// Resolve and validate every control selector before any runtime component
    /// creates a directory, opens storage, binds a listener, or starts work.
    pub fn from_env(default_path: &Path) -> Result<Self> {
        Self::from_sources(None, default_path)
    }

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
        validate_nonempty_config("control.turso_url", config.control.turso_url.as_deref())?;
        validate_nonempty_config("control.turso_token", config.control.turso_token.as_deref())?;
        let path = explicit_path
            .map(Path::to_path_buf)
            .or_else(|| environment_path.map(PathBuf::from))
            .or_else(|| config.control.path.as_deref().map(PathBuf::from))
            .unwrap_or_else(|| default_path.to_path_buf());
        if path.as_os_str().to_string_lossy().trim().is_empty() {
            bail!("RIPCLONE_CONTROL_DB_PATH must not be empty");
        }
        let url = strict_env_or(
            "RIPCLONE_TURSO_DATABASE_URL",
            config.control.turso_url.as_deref(),
        )?;
        let token = strict_env_or(
            "RIPCLONE_TURSO_AUTH_TOKEN",
            config.control.turso_token.as_deref(),
        )?;
        let turso = match (url, token) {
            (None, None) => None,
            (Some(url), Some(token)) => Some(TursoReplicaConfig { url, token }),
            (Some(_), None) => {
                bail!("RIPCLONE_TURSO_DATABASE_URL requires RIPCLONE_TURSO_AUTH_TOKEN")
            }
            (None, Some(_)) => {
                bail!("RIPCLONE_TURSO_AUTH_TOKEN requires RIPCLONE_TURSO_DATABASE_URL")
            }
        };
        let size_classes = crate::queue::load_size_classes(&config.control.size_classes)?;
        Ok(Self {
            path,
            turso,
            size_classes,
        })
    }
}

fn strict_env_or(key: &str, fallback: Option<&str>) -> Result<Option<String>> {
    match std::env::var(key) {
        Ok(value) if value.trim().is_empty() => bail!("{key} must not be empty"),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(fallback.map(str::to_owned)),
        Err(std::env::VarError::NotUnicode(_)) => bail!("{key} must be valid UTF-8"),
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
        "removed control configuration is set: {}; use RIPCLONE_CONTROL_DB_PATH and optional RIPCLONE_TURSO_DATABASE_URL/RIPCLONE_TURSO_AUTH_TOKEN",
        configured.join(", ")
    )
}

/// Standalone workers are API-only. Reject even otherwise-valid server control
/// credentials so a deployment cannot accidentally grant database authority.
pub fn validate_worker_environment() -> Result<()> {
    validate_removed_environment()?;
    let config = crate::config::load_global();
    validate_removed_config(&config)?;
    if config.control.path.is_some()
        || config.control.turso_url.is_some()
        || config.control.turso_token.is_some()
        || !config.control.size_classes.is_empty()
    {
        bail!("standalone workers are API-only; [control] configuration is forbidden");
    }
    const SERVER_ONLY: &[&str] = &[
        "RIPCLONE_CONTROL_DB_PATH",
        "RIPCLONE_TURSO_DATABASE_URL",
        "RIPCLONE_TURSO_AUTH_TOKEN",
    ];
    let configured: Vec<&str> = SERVER_ONLY
        .iter()
        .copied()
        .filter(|key| std::env::var_os(key).is_some())
        .collect();
    if !configured.is_empty() {
        bail!(
            "standalone workers are API-only; server control configuration is forbidden: {}",
            configured.join(", ")
        );
    }
    Ok(())
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
    turso_replica: bool,
    refs: Arc<SqlRefStore>,
    queue: Arc<SqlJobQueue>,
    path: PathBuf,
    size_classes: Vec<SizeClass>,
    /// Process-local wake hint for embedded claimers. The jobs table remains
    /// authoritative; notifications are emitted only after admission commits.
    admission_notify: Arc<tokio::sync::Notify>,
    _ownership: ControlPathLock,
}

impl ControlDb {
    /// Open the configured local path and fail closed on an incompatible
    /// pre-existing database. The ownership lock is held for this value's
    /// lifetime and is acquired before the database is opened.
    pub async fn open(
        path: &Path,
        turso: Option<TursoReplicaConfig>,
        size_classes: Vec<SizeClass>,
    ) -> Result<Self> {
        let ownership = ControlPathLock::acquire(path)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create control directory {}", parent.display()))?;
        }

        preflight_sqlite_schema(path).await?;
        let (database, turso_replica) = match turso {
            None => (
                Arc::new(
                    Builder::new_local(path)
                        .build()
                        .await
                        .with_context(|| format!("open control database {}", path.display()))?,
                ),
                false,
            ),
            Some(config) => {
                let database = Arc::new(
                    Builder::new_remote_replica(path, config.url, config.token)
                        .read_your_writes(true)
                        .sync_interval(Duration::from_secs(1))
                        .build()
                        .await
                        .with_context(|| {
                            format!("open Turso embedded replica {}", path.display())
                        })?,
                );
                database
                    .sync()
                    .await
                    .context("bootstrap Turso embedded replica from primary")?;
                (database, true)
            }
        };
        if !turso_replica {
            database
                .connect()
                .context("connect to control database")?
                .execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")
                .await
                .context("configure local control database")?;
        }
        validate_or_initialize_schema(&database, path).await?;
        let refs = Arc::new(
            SqlRefStore::new(Box::new(LibsqlMeta::from_database(database.clone()))).await?,
        );
        let queue = Arc::new(
            SqlJobQueue::new_with_classes(
                Box::new(LibsqlDb::from_database(database.clone())),
                size_classes.clone(),
            )
            .await?,
        );
        Ok(Self {
            database,
            turso_replica,
            refs,
            queue,
            path: path.to_path_buf(),
            size_classes,
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

    pub fn is_turso_replica(&self) -> bool {
        self.turso_replica
    }

    pub(crate) fn admission_notifier(&self) -> Arc<tokio::sync::Notify> {
        self.admission_notify.clone()
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
        crate::server::admission_test_inside_admission_tx(&job.admitted_commit).await;
        insert_exact_result(&tx, job, pending).await?;
        let key = job.key();
        let size_class = crate::queue::classify_rank(job.size_bytes, &self.size_classes);
        let credential = crate::queue::sql::encode_credential(job.credential.as_ref());
        let changed = tx
            .execute(
                "INSERT OR IGNORE INTO jobs
                 (key, provider, path, status, created_at, admitted_commit,
                  repo_config, credential, attempts, size_class)
                 VALUES (?1, ?2, ?3, 'queued', ?4, ?5, ?6, ?7, 0, ?8)",
                libsql::params![
                    key.clone(),
                    job.repo_id.provider.as_str(),
                    job.repo_id.path.as_str(),
                    crate::queue::sql::now_secs(),
                    job.admitted_commit.as_str(),
                    repo_config,
                    credential.as_deref(),
                    size_class
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
            tx.execute(
                "UPDATE jobs SET size_class = MAX(size_class, ?1)
                 WHERE id = ?2 AND status = 'queued'",
                libsql::params![size_class, id],
            )
            .await
            .context("raise coalesced job size class")?;
            (EnqueueOutcome::Coalesced, id)
        };
        tx.commit().await.context("commit durable admission")?;
        Ok(Enqueued {
            outcome,
            job_id: Some(id),
        })
    }
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
            size_bytes: None,
        }
    }

    fn pending(commit: &str) -> crate::RefInfo {
        crate::RefInfo {
            commit: commit.to_string(),
            build_status: Some("queued".to_string()),
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
        let control = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
        assert!(control.repository_config(&missing).await.unwrap().is_none());
        control
            .put_repository_config(&repo_id, &config)
            .await
            .unwrap();
        drop(control);
        let reopened = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
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
        let control = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
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
        let control = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
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
        let control = Arc::new(
            ControlDb::open(&path, None, crate::queue::default_size_classes())
                .await
                .unwrap(),
        );
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
        let control = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
        let admitted_id = control
            .admit_exact_and_job(&job(commit, "main"), &pending(commit))
            .await
            .unwrap()
            .job_id
            .unwrap();
        drop(control);
        let reopened = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
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
    async fn second_server_is_rejected_while_owner_is_alive() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let _owner = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
        let error = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .err()
            .unwrap();
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
        let error = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .err()
            .unwrap();
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
