//! The server-owned SQLite control database.
//!
//! Refs, added repositories, durable jobs, attempts, claims, and worker
//! heartbeats share one schema and one local path. Plain SQLite is the default;
//! the only replicated mode is a Turso embedded replica at that same path.

use crate::meta::{LibsqlMeta, SqlRefStore};
use crate::queue::{BuildJob, EnqueueOutcome, Enqueued, LibsqlDb, SizeClass, SqlJobQueue};
use crate::ref_store::RefStore;
use anyhow::{Context, Result, bail};
use libsql::{Builder, Database, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const SCHEMA_VERSION: i64 = 1;

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
        let path = explicit_path
            .map(Path::to_path_buf)
            .or_else(|| {
                std::env::var_os("RIPCLONE_CONTROL_DB_PATH")
                    .filter(|value| !value.is_empty())
                    .map(PathBuf::from)
            })
            .or_else(|| config.control.path.as_deref().map(PathBuf::from))
            .unwrap_or_else(|| default_path.to_path_buf());
        if path.as_os_str().is_empty() {
            bail!("RIPCLONE_CONTROL_DB_PATH must not be empty");
        }
        let url = nonempty_env_or(
            "RIPCLONE_TURSO_DATABASE_URL",
            config.control.turso_url.as_deref(),
        );
        let token = nonempty_env_or(
            "RIPCLONE_TURSO_AUTH_TOKEN",
            config.control.turso_token.as_deref(),
        );
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

fn nonempty_env_or(key: &str, fallback: Option<&str>) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            fallback
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
        })
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

    /// Atomically create/replace the exact pending row, link the temporary
    /// moving-publication fence when supplied, and enqueue or join the durable
    /// job. No worker can observe the job without its exact result row.
    pub(crate) async fn admit_exact_and_job(
        &self,
        job: &BuildJob,
        exact_branch: &str,
        pending: &crate::RefInfo,
        tail: Option<(&str, &crate::RefInfo)>,
    ) -> Result<Enqueued> {
        let result = self
            .admit(&self.database, job, exact_branch, pending, tail)
            .await;
        if result.is_ok() {
            self.admission_notify.notify_one();
        }
        result
    }

    async fn admit(
        &self,
        database: &Database,
        job: &BuildJob,
        exact_branch: &str,
        pending: &crate::RefInfo,
        tail: Option<(&str, &crate::RefInfo)>,
    ) -> Result<Enqueued> {
        let connection = database
            .connect()
            .context("connect for durable admission")?;
        connection
            .execute("PRAGMA busy_timeout = 5000", ())
            .await
            .context("configure durable admission busy timeout")?;
        let tx = connection
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .context("begin durable admission")?;
        upsert_exact(&tx, job, exact_branch, pending).await?;
        if let Some((tail_branch, tail_info)) = tail {
            update_tail(&tx, job, tail_branch, tail_info).await?;
        }
        let key = job.key();
        let size_class = crate::queue::classify_rank(job.size_bytes, &self.size_classes);
        let credential = crate::queue::sql::encode_credential(job.credential.as_ref());
        let changed = tx
            .execute(
                "INSERT OR IGNORE INTO jobs
                 (key, provider, path, branch, status, created_at, admitted_commit,
                  admitted_default_branch, credential, attempts, size_class)
                 VALUES (?1, ?2, ?3, ?4, 'queued', ?5, ?6, ?7, ?8, 0, ?9)",
                libsql::params![
                    key.clone(),
                    job.repo_id.provider.as_str(),
                    job.repo_id.path.as_str(),
                    job.branch.as_str(),
                    crate::queue::sql::now_secs(),
                    job.admitted_commit.as_str(),
                    job.admitted_default_branch.as_deref(),
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

fn ref_times(info: &crate::RefInfo) -> (Option<i64>, Option<i64>) {
    (
        info.synced_at.and_then(|value| i64::try_from(value).ok()),
        info.generation.and_then(|value| i64::try_from(value).ok()),
    )
}

async fn upsert_exact(
    tx: &libsql::Transaction,
    job: &BuildJob,
    exact_branch: &str,
    info: &crate::RefInfo,
) -> Result<()> {
    let data = serde_json::to_string(info).context("serialize exact admission")?;
    let (synced_at, generation) = ref_times(info);
    let changed = tx
        .execute(
            "INSERT INTO refs(repo_key, branch, commit_id, synced_at, generation, data)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(repo_key, branch) DO UPDATE SET
                 commit_id=excluded.commit_id, synced_at=excluded.synced_at,
                 generation=excluded.generation, data=excluded.data
             WHERE refs.commit_id = excluded.commit_id",
            libsql::params![
                job.repo_id.storage_key(),
                exact_branch,
                job.admitted_commit.as_str(),
                synced_at,
                generation,
                data
            ],
        )
        .await
        .context("create exact admission row")?;
    if changed != 1 {
        bail!("exact admission key contains a different commit");
    }
    Ok(())
}

async fn update_tail(
    tx: &libsql::Transaction,
    job: &BuildJob,
    tail_branch: &str,
    info: &crate::RefInfo,
) -> Result<()> {
    let data = serde_json::to_string(info).context("serialize admission tail")?;
    let (synced_at, generation) = ref_times(info);
    let changed = tx
        .execute(
            "UPDATE refs SET synced_at=?1, generation=?2, data=?3
             WHERE repo_key=?4 AND branch=?5 AND commit_id=?6",
            libsql::params![
                synced_at,
                generation,
                data,
                job.repo_id.storage_key(),
                tail_branch,
                info.commit.as_str()
            ],
        )
        .await
        .context("link moving admission tail")?;
    if changed != 1 {
        bail!("ordinary admission tail disappeared");
    }
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
        .execute("PRAGMA busy_timeout = 5000", ())
        .await
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
mod tests {
    use super::*;
    use crate::queue::{JobQueue, JobState};

    fn job(commit: &str) -> BuildJob {
        BuildJob {
            repo_id: crate::provider::RepoId::github("acme/control"),
            branch: "main".to_string(),
            admitted_commit: commit.to_string(),
            admitted_default_branch: Some("main".to_string()),
            credential: None,
            size_bytes: None,
        }
    }

    fn pending(commit: &str) -> crate::RefInfo {
        crate::RefInfo {
            commit: commit.to_string(),
            default_branch: "main".to_string(),
            internal_exact_result: true,
            build_status: Some("queued".to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn atomic_admission_coalesces_duplicates_and_keeps_later_commits_distinct() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let control = Arc::new(
            ControlDb::open(&path, None, crate::queue::default_size_classes())
                .await
                .unwrap(),
        );
        let first_commit = "1111111111111111111111111111111111111111";
        let first_job = job(first_commit);
        let first_pending = pending(first_commit);
        let exact = format!("main@{first_commit}");

        let mut admissions = Vec::new();
        for _ in 0..32 {
            let control = control.clone();
            let job = first_job.clone();
            let pending = first_pending.clone();
            let exact = exact.clone();
            admissions.push(tokio::spawn(async move {
                control
                    .admit_exact_and_job(&job, &exact, &pending, None)
                    .await
                    .unwrap()
            }));
        }
        let mut enqueued = 0;
        let mut coalesced = 0;
        let mut first_id = None;
        for admission in admissions {
            let admission = admission.await.unwrap();
            first_id.get_or_insert(admission.job_id.unwrap());
            match admission.outcome {
                EnqueueOutcome::Enqueued => enqueued += 1,
                EnqueueOutcome::Coalesced => coalesced += 1,
                EnqueueOutcome::Full => panic!("durable admission is not capacity bounded"),
            }
        }
        assert_eq!(enqueued, 1);
        assert_eq!(coalesced, 31);
        assert_eq!(control.queue().depth().await, 1);
        assert_eq!(
            control
                .ref_store()
                .load_branch(&first_job.repo_id, &exact)
                .await
                .unwrap()
                .unwrap()
                .commit,
            first_commit
        );

        let second_commit = "2222222222222222222222222222222222222222";
        let second = control
            .admit_exact_and_job(
                &job(second_commit),
                &format!("main@{second_commit}"),
                &pending(second_commit),
                None,
            )
            .await
            .unwrap();
        assert_eq!(second.outcome, EnqueueOutcome::Enqueued);
        assert_ne!(second.job_id, first_id);
        assert_eq!(control.queue().depth().await, 2);
    }

    #[tokio::test]
    async fn failed_admission_rolls_back_exact_result_and_job() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let control = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
        let commit = "3333333333333333333333333333333333333333";
        let job = job(commit);
        let exact = format!("main@{commit}");
        let missing_tail = pending(commit);
        let error = control
            .admit_exact_and_job(
                &job,
                &exact,
                &pending(commit),
                Some(("main", &missing_tail)),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("tail disappeared"));
        assert!(
            control
                .ref_store()
                .load_branch(&job.repo_id, &exact)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(control.queue().depth().await, 0);
    }

    #[tokio::test]
    async fn accepted_job_survives_server_restart() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let commit = "4444444444444444444444444444444444444444";
        let admitted_id = {
            let control = ControlDb::open(&path, None, crate::queue::default_size_classes())
                .await
                .unwrap();
            control
                .admit_exact_and_job(
                    &job(commit),
                    &format!("main@{commit}"),
                    &pending(commit),
                    None,
                )
                .await
                .unwrap()
                .job_id
                .unwrap()
        };
        let reopened = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
        assert!(matches!(
            reopened.queue().job_status(admitted_id).await.unwrap(),
            JobState::Pending
        ));
        assert_eq!(reopened.queue().depth().await, 1);
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
            .expect("second server must fail before opening the database");
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
            .expect("old database must fail");

        assert!(
            error
                .to_string()
                .contains("automatic migration is not supported")
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}
