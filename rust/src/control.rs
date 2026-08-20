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

    /// Atomically create/replace the exact pending row, link the temporary
    /// moving-publication fence when supplied, and enqueue or join the durable
    /// job. No worker can observe the job without its exact result row.
    pub(crate) async fn admit_exact_and_job(
        &self,
        job: &BuildJob,
        exact_branch: &str,
        pending: &crate::RefInfo,
        moving_authorized: bool,
    ) -> Result<Enqueued> {
        let result = self
            .admit(
                &self.database,
                job,
                exact_branch,
                pending,
                moving_authorized,
            )
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
        moving_authorized: bool,
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
        let mut pending = pending.clone();
        let tail = if moving_authorized {
            discover_moving_admission_tail(&tx, job, &mut pending).await?
        } else {
            None
        };
        upsert_exact(&tx, job, exact_branch, &pending).await?;
        if let Some((tail_branch, tail_info)) = tail.as_ref() {
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

/// Discover and extend the ordinary-publication chain under the same immediate
/// transaction that creates the exact result and durable job. Request-side
/// snapshots are advisory only: concurrent admissions serialize here and the
/// later transaction follows every successor committed by the earlier one.
async fn discover_moving_admission_tail(
    tx: &libsql::Transaction,
    job: &BuildJob,
    pending: &mut crate::RefInfo,
) -> Result<Option<(String, crate::RefInfo)>> {
    let result_branch = if job.branch == "HEAD" {
        job.admitted_default_branch
            .as_deref()
            .unwrap_or(job.branch.as_str())
    } else {
        job.branch.as_str()
    };
    let moving = load_transaction_ref(tx, job, result_branch).await?;
    if let Some(moving) = moving {
        if moving.commit == job.admitted_commit {
            pending.moving_publication_predecessors = moving.moving_publication_predecessors;
            return Ok(None);
        }
        let mut predecessors = vec![moving.commit.clone()];
        let mut tail_commit = moving.commit;
        let mut seen = std::collections::HashSet::new();
        loop {
            anyhow::ensure!(
                seen.insert(tail_commit.clone()),
                "ordinary admission chain contains a cycle"
            );
            let tail_branch = crate::ref_store::exact_ref_key(result_branch, &tail_commit);
            let mut tail = load_transaction_ref(tx, job, &tail_branch)
                .await?
                .with_context(|| {
                    format!("ordinary admission chain is missing exact {tail_commit}")
                })?;
            let Some(next) = tail.moving_admission_successors.last().cloned() else {
                pending.moving_publication_predecessors = predecessors;
                tail.require_matching_commit = false;
                tail.moving_admission_successors
                    .push(job.admitted_commit.clone());
                return Ok(Some((tail_branch, tail)));
            };
            crate::validation::validate_object_id(&next)
                .context("ordinary admission chain has invalid successor")?;
            if next == job.admitted_commit {
                pending.moving_publication_predecessors = predecessors;
                return Ok(None);
            }
            predecessors.push(next.clone());
            tail_commit = next;
        }
    }

    // Before the first moving projection publishes, concurrent initial
    // admissions are rooted at the explicit initial marker. Find the sole
    // outstanding tail from rows committed by earlier admission transactions.
    pending.moving_publication_predecessors =
        vec![crate::ref_store::INITIAL_MOVING_PROJECTION_PREDECESSOR.to_string()];
    let prefix = crate::ref_store::exact_ref_key(result_branch, "");
    let mut rows = tx
        .query(
            "SELECT branch, data FROM refs
             WHERE repo_key = ?1 AND substr(branch, 1, length(?2)) = ?2",
            libsql::params![job.repo_id.storage_key(), prefix],
        )
        .await
        .context("load initial ordinary-admission chain")?;
    let mut tails = Vec::new();
    while let Some(row) = rows.next().await? {
        let branch = row.get::<String>(0)?;
        let info: crate::RefInfo =
            serde_json::from_str(&row.get::<String>(1)?).context("parse admission-chain ref")?;
        if info.commit != job.admitted_commit
            && info.internal_exact_result
            && info
                .moving_publication_predecessors
                .iter()
                .any(|commit| commit == crate::ref_store::INITIAL_MOVING_PROJECTION_PREDECESSOR)
            && info.moving_admission_successors.is_empty()
        {
            tails.push((branch, info));
        }
    }
    drop(rows);
    let Some((branch, mut tail)) = tails.pop() else {
        return Ok(None);
    };
    anyhow::ensure!(
        tails.is_empty(),
        "ordinary admission has multiple initial chain tails"
    );
    for predecessor in tail
        .moving_publication_predecessors
        .iter()
        .chain(std::iter::once(&tail.commit))
    {
        if !pending
            .moving_publication_predecessors
            .contains(predecessor)
        {
            pending
                .moving_publication_predecessors
                .push(predecessor.clone());
        }
    }
    tail.require_matching_commit = false;
    tail.moving_admission_successors
        .push(job.admitted_commit.clone());
    Ok(Some((branch, tail)))
}

async fn load_transaction_ref(
    tx: &libsql::Transaction,
    job: &BuildJob,
    branch: &str,
) -> Result<Option<crate::RefInfo>> {
    let mut rows = tx
        .query(
            "SELECT data FROM refs WHERE repo_key = ?1 AND branch = ?2",
            libsql::params![job.repo_id.storage_key(), branch],
        )
        .await
        .with_context(|| format!("load transactional ref {branch}"))?;
    let info = match rows.next().await? {
        Some(row) => Some(
            serde_json::from_str(&row.get::<String>(0)?)
                .with_context(|| format!("parse transactional ref {branch}"))?,
        ),
        None => None,
    };
    Ok(info)
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
                    .admit_exact_and_job(&job, &exact, &pending, false)
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
                false,
            )
            .await
            .unwrap();
        assert_eq!(second.outcome, EnqueueOutcome::Enqueued);
        assert_ne!(second.job_id, first_id);
        assert_eq!(control.queue().depth().await, 2);
    }

    #[tokio::test]
    async fn first_admissions_form_one_fenced_publication_chain() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let control = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
        let first_commit = "1111111111111111111111111111111111111111";
        let second_commit = "2222222222222222222222222222222222222222";
        let mut first_pending = pending(first_commit);
        first_pending.require_matching_commit = true;
        first_pending.moving_publication_predecessors =
            vec![crate::ref_store::INITIAL_MOVING_PROJECTION_PREDECESSOR.to_string()];
        let mut second_pending = pending(second_commit);
        second_pending.require_matching_commit = true;
        second_pending.moving_publication_predecessors =
            vec![crate::ref_store::INITIAL_MOVING_PROJECTION_PREDECESSOR.to_string()];
        let first_exact = crate::ref_store::exact_ref_key("main", first_commit);
        let second_exact = crate::ref_store::exact_ref_key("main", second_commit);

        control
            .admit_exact_and_job(&job(first_commit), &first_exact, &first_pending, true)
            .await
            .unwrap();
        control
            .admit_exact_and_job(&job(second_commit), &second_exact, &second_pending, true)
            .await
            .unwrap();

        let store = control.ref_store();
        let first = store
            .load_branch(&job(first_commit).repo_id, &first_exact)
            .await
            .unwrap()
            .unwrap();
        let second = store
            .load_branch(&job(second_commit).repo_id, &second_exact)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            first.moving_admission_successors,
            vec![second_commit.to_string()]
        );
        assert_eq!(
            second.moving_publication_predecessors,
            vec![
                crate::ref_store::INITIAL_MOVING_PROJECTION_PREDECESSOR.to_string(),
                first_commit.to_string(),
            ]
        );

        let mut first_projection = first;
        first_projection.internal_exact_result = false;
        first_projection.require_matching_commit = true;
        let mut second_projection = second;
        second_projection.internal_exact_result = false;
        second_projection.require_matching_commit = true;
        store
            .save_branch(&job(first_commit).repo_id, "main", &first_projection)
            .await
            .unwrap();
        store
            .save_branch(&job(second_commit).repo_id, "main", &second_projection)
            .await
            .unwrap();
        store
            .save_branch(&job(first_commit).repo_id, "main", &first_projection)
            .await
            .unwrap();
        assert_eq!(
            store
                .load_branch(&job(first_commit).repo_id, "main")
                .await
                .unwrap()
                .unwrap()
                .commit,
            second_commit,
            "the older first admission must not replace its admitted successor"
        );
    }

    #[tokio::test]
    async fn established_tail_is_extended_transactionally_for_later_admissions() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        let control = ControlDb::open(&path, None, crate::queue::default_size_classes())
            .await
            .unwrap();
        let a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let c = "cccccccccccccccccccccccccccccccccccccccc";
        let store = control.ref_store();
        let moving_a = crate::RefInfo {
            commit: a.to_string(),
            default_branch: "main".to_string(),
            require_matching_commit: true,
            ..Default::default()
        };
        let mut exact_a = moving_a.clone();
        exact_a.internal_exact_result = true;
        store
            .save_branch(&job(a).repo_id, "main", &moving_a)
            .await
            .unwrap();
        store
            .save_branch(
                &job(a).repo_id,
                &crate::ref_store::exact_ref_key("main", a),
                &exact_a,
            )
            .await
            .unwrap();

        // Both request-side preparations may have observed A. The database is
        // authoritative: serialized immediate transactions must extend the
        // committed tail to A -> B -> C instead of overwriting A twice.
        control
            .admit_exact_and_job(
                &job(b),
                &crate::ref_store::exact_ref_key("main", b),
                &pending(b),
                true,
            )
            .await
            .unwrap();
        control
            .admit_exact_and_job(
                &job(c),
                &crate::ref_store::exact_ref_key("main", c),
                &pending(c),
                true,
            )
            .await
            .unwrap();

        let mut projection_b = store
            .load_branch(&job(b).repo_id, &crate::ref_store::exact_ref_key("main", b))
            .await
            .unwrap()
            .unwrap();
        projection_b.internal_exact_result = false;
        projection_b.require_matching_commit = true;
        store
            .save_branch(&job(b).repo_id, "main", &projection_b)
            .await
            .unwrap();
        let mut projection_c = store
            .load_branch(&job(c).repo_id, &crate::ref_store::exact_ref_key("main", c))
            .await
            .unwrap()
            .unwrap();
        projection_c.internal_exact_result = false;
        projection_c.require_matching_commit = true;
        store
            .save_branch(&job(c).repo_id, "main", &projection_c)
            .await
            .unwrap();

        assert_eq!(
            store
                .load_branch(&job(c).repo_id, "main")
                .await
                .unwrap()
                .unwrap()
                .commit,
            c,
            "completing B then C must leave the established moving branch at C"
        );
        let exact_b = store
            .load_branch(&job(b).repo_id, &crate::ref_store::exact_ref_key("main", b))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exact_b.moving_admission_successors, vec![c.to_string()]);
        assert_eq!(
            projection_c.moving_publication_predecessors,
            vec![a.to_string(), b.to_string()]
        );
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
        let missing_commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let moving = crate::RefInfo {
            commit: missing_commit.to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        control
            .ref_store()
            .save_branch(&job.repo_id, "main", &moving)
            .await
            .unwrap();
        let error = control
            .admit_exact_and_job(&job, &exact, &pending(commit), true)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ordinary admission chain is missing exact")
        );
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
                    false,
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
