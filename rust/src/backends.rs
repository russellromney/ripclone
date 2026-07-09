//! Env-driven construction of the pluggable backends, shared by the
//! `ripclone-server` and the standalone `ripclone-worker` so both wire up
//! identically: same storage, same metadata store, same queue.
//!
//! A build needs only durable state — blob `storage` (artifacts) and the
//! `ref_store` (metadata) — plus the local CAS cache and a scratch mirror root.
//! That is why the worker can run anywhere: it owns no durable state.

use crate::cas::Cas;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::queue::sql::QueueDb;
use crate::queue::{
    BuildJob, JobQueueRef, LibsqlDb, LocalJobQueue, MysqlDb, PostgresDb, SqlJobQueue, SqliteDb,
};
use crate::ref_store::{CachingRefStore, FileRefStore, RefStore, S3RefStore};
use crate::retention::Retention;
use crate::storage::{S3Storage, StorageRef, local};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Capacity of the in-process queue channel (only used by the local backend).
pub const LOCAL_QUEUE_CAPACITY: usize = 1024;

/// The **global** `config.toml`, loaded once. Backend selectors consult this as
/// a fallback for their `RIPCLONE_*` env vars (env always wins).
///
/// Deliberately `load_global` (not `load`): server backend config must not be
/// silently altered by a stray project `ripclone.toml` in the server's working
/// directory. `RIPCLONE_CONFIG` can point at an explicit file. Project configs
/// remain a client-side concept (clone defaults, server URL).
fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(crate::config::load_global)
}

/// Resolve a setting: the env var wins; otherwise fall back to the config value.
/// Empty env values are treated as unset.
fn env_or(key: &str, cfg_val: Option<&str>) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| cfg_val.map(str::to_string))
}

/// Durable + cache backends needed to run a build.
pub struct Backends {
    pub cas: Cas,
    pub storage: StorageRef,
    pub ref_store: Arc<dyn RefStore>,
    pub retention: Arc<Retention>,
    pub repo_root: PathBuf,
}

impl Backends {
    /// Build storage + metadata store + retention from the environment, factored
    /// out so the server and the worker share it. Does **not** start the
    /// retention sweep loop or migrate legacy refs — those are server-startup
    /// concerns.
    pub async fn from_env(
        cas_dir: &Path,
        repo_root: &Path,
        metrics: &Arc<Metrics>,
    ) -> Result<Self> {
        let cas = Cas::new(cas_dir)?;
        let s3_storage =
            S3Storage::from_env_or_config(&config().storage).context("initialize S3 storage")?;
        let (storage, s3): (StorageRef, Option<Arc<S3Storage>>) = if let Some(s3) = s3_storage {
            info!(
                "using S3-compatible storage with local cache at {}",
                cas_dir.display()
            );
            let s3 = Arc::new(s3);
            (s3.clone() as StorageRef, Some(s3))
        } else {
            info!("using local storage at {}", cas_dir.display());
            (local(cas_dir)?, None)
        };
        let ref_store = select_metadata(repo_root, s3.as_ref()).await?;
        let retention = Arc::new(
            Retention::with_config_and_storage(
                cas.clone(),
                metrics.clone(),
                Retention::parse_age(),
                Retention::parse_size(),
                Some(storage.clone()),
            )?
            // Protect the ref-reachable set each run so retention never deletes a
            // referenced artifact (critical on a local-only backend, where the
            // cache holds the only copy).
            .with_ref_store(ref_store.clone(), storage.clone()),
        );
        Ok(Self {
            cas,
            storage,
            ref_store,
            retention,
            repo_root: repo_root.to_path_buf(),
        })
    }
}

/// Select the metadata store from `RIPCLONE_METADATA`:
/// `file` | `s3` | `sqlite` | `postgres` | `mysql` | `libsql`. Unset preserves
/// the historical default: S3 when S3 storage is configured, else file. SQL
/// backends read `RIPCLONE_METADATA_DB_URL` (+ `RIPCLONE_METADATA_DB_TOKEN` for
/// libsql). The result is always wrapped in `CachingRefStore`.
async fn select_metadata(
    repo_root: &Path,
    s3: Option<&Arc<S3Storage>>,
) -> Result<Arc<dyn RefStore>> {
    use crate::meta::MetaDb;
    use crate::meta::{LibsqlMeta, MysqlMeta, PostgresMeta, SqlRefStore, SqliteMeta};

    let kind =
        env_or("RIPCLONE_METADATA", config().metadata.backend.as_deref()).unwrap_or_default();

    // Warn when a SQL queue is paired with per-host file metadata. If the
    // workers run on other hosts, each reads and writes its own local ref files
    // and a worker's build is invisible to the server. It's valid when the server
    // and workers share one filesystem (same box), which we can't tell apart
    // here, so warn loudly rather than refuse — the point is to break the silence.
    let resolves_to_file = kind == "file" || (kind.is_empty() && s3.is_none());
    if resolves_to_file {
        let queue = queue_kind();
        if matches!(queue.as_str(), "sqlite" | "postgres" | "mysql" | "libsql") {
            warn!(
                "RIPCLONE_QUEUE={queue} is SQL but the metadata store resolves to per-host \
                 files. If workers don't share this filesystem, set a shared metadata store \
                 (RIPCLONE_METADATA=s3|sqlite|postgres|mysql|libsql) so the server and workers \
                 share refs."
            );
        }
    }

    // Each arm wraps its concrete store in CachingRefStore (the read cache) and
    // coerces to Arc<dyn RefStore>.
    let store: Arc<dyn RefStore> = match kind.as_str() {
        "" => {
            // Backward compatible: metadata follows storage.
            if let Some(s3) = s3 {
                info!("metadata store: s3 (default, follows storage)");
                Arc::new(CachingRefStore::new(S3RefStore::new(s3.clone())))
            } else {
                info!("metadata store: file (default)");
                Arc::new(CachingRefStore::new(FileRefStore::new(repo_root)))
            }
        }
        "file" => {
            info!("metadata store: file");
            Arc::new(CachingRefStore::new(FileRefStore::new(repo_root)))
        }
        "s3" => {
            let s3 = s3.context("RIPCLONE_METADATA=s3 requires S3 storage env (RIPCLONE_S3_*)")?;
            info!("metadata store: s3");
            Arc::new(CachingRefStore::new(S3RefStore::new(s3.clone())))
        }
        "sqlite" | "postgres" | "mysql" | "libsql" => {
            let url = env_or("RIPCLONE_METADATA_DB_URL", config().metadata.url.as_deref()).context(
                "RIPCLONE_METADATA=sqlite|postgres|mysql|libsql requires RIPCLONE_METADATA_DB_URL (or [metadata].url)",
            )?;
            let db: Box<dyn MetaDb> = match kind.as_str() {
                "sqlite" => Box::new(SqliteMeta::connect(&url).await?),
                "postgres" => Box::new(PostgresMeta::connect(&url).await?),
                "mysql" => Box::new(MysqlMeta::connect(&url).await?),
                "libsql" => {
                    if !is_remote_url(&url) {
                        anyhow::bail!(
                            "RIPCLONE_METADATA=libsql is remote-only; RIPCLONE_METADATA_DB_URL \
                             must be a libsql:// or https:// url (local file → use sqlite)"
                        );
                    }
                    let token = env_or("RIPCLONE_METADATA_DB_TOKEN", config().metadata.token.as_deref())
                        .context("RIPCLONE_METADATA=libsql requires RIPCLONE_METADATA_DB_TOKEN (or [metadata].token)")?;
                    Box::new(LibsqlMeta::connect_remote(&url, &token).await?)
                }
                _ => unreachable!(),
            };
            info!("metadata store: {kind}");
            Arc::new(CachingRefStore::new(SqlRefStore::new(db).await?))
        }
        other => anyhow::bail!(
            "unknown RIPCLONE_METADATA backend: {other:?} \
             (expected 'file', 's3', 'sqlite', 'postgres', 'mysql', or 'libsql')"
        ),
    };
    Ok(store)
}

/// The selected queue backend (`RIPCLONE_QUEUE`, default `local`).
pub enum QueueBackend {
    /// In-process queue. `rx` drives the in-process worker; `depth` is the
    /// shared counter on `ServerState`.
    Local {
        queue: JobQueueRef,
        rx: mpsc::Receiver<BuildJob>,
        depth: Arc<AtomicUsize>,
    },
    /// SQL-backed queue. Builds run in separate `ripclone-worker` processes, so
    /// the server spawns no in-process worker.
    Sql { queue: JobQueueRef },
}

/// Read `RIPCLONE_QUEUE` (default `local`).
pub fn queue_kind() -> String {
    env_or("RIPCLONE_QUEUE", config().queue.backend.as_deref())
        .unwrap_or_else(|| "local".to_string())
}

/// Select the queue for the **server** from `RIPCLONE_QUEUE`:
/// `local` (in-process worker) | `sqlite` (local file, single-box farm-out) |
/// `postgres` / `mysql` (network db, multi-machine) | `libsql` (remote Turso
/// Cloud, multi-machine). The SQL backends run builds in separate
/// `ripclone-worker` processes.
pub async fn select_queue() -> Result<QueueBackend> {
    let kind = queue_kind();
    match kind.as_str() {
        "local" => {
            let (queue, rx, depth) = LocalJobQueue::new(LOCAL_QUEUE_CAPACITY);
            info!("using in-process build queue (RIPCLONE_QUEUE=local)");
            Ok(QueueBackend::Local {
                queue: Arc::new(queue),
                rx,
                depth,
            })
        }
        "sqlite" | "postgres" | "mysql" | "libsql" => {
            let queue = connect_sql_queue().await?;
            info!("using SQL build queue (RIPCLONE_QUEUE={kind}); builds run in ripclone-worker");
            Ok(QueueBackend::Sql {
                queue: Arc::new(queue),
            })
        }
        other => anyhow::bail!(
            "unknown RIPCLONE_QUEUE backend: {other:?} \
             (expected 'local', 'sqlite', 'postgres', 'mysql', or 'libsql')"
        ),
    }
}

/// Build the SQL-backed queue from env, shared by the server and the worker.
/// - `sqlite` → local file at `RIPCLONE_QUEUE_DB_URL` (mature, single-box).
/// - `postgres` → `postgres://…` at `RIPCLONE_QUEUE_DB_URL` (network, multi-machine).
/// - `mysql` → `mysql://…` at `RIPCLONE_QUEUE_DB_URL` (network, multi-machine).
/// - `libsql` → **remote** Turso Cloud at `RIPCLONE_QUEUE_DB_URL` (a `libsql://`
///   / `https://` url) with `RIPCLONE_QUEUE_DB_TOKEN`.
pub async fn connect_sql_queue() -> Result<SqlJobQueue> {
    let kind = queue_kind();
    let url = queue_db_url()?;
    let db: Box<dyn QueueDb> = match kind.as_str() {
        "sqlite" => Box::new(SqliteDb::connect(&url).await?),
        "postgres" => Box::new(PostgresDb::connect(&url).await?),
        "mysql" => Box::new(MysqlDb::connect(&url).await?),
        "libsql" => {
            if !is_remote_url(&url) {
                anyhow::bail!(
                    "RIPCLONE_QUEUE=libsql is remote-only; RIPCLONE_QUEUE_DB_URL must be a \
                     libsql:// or https:// url (for a local file use RIPCLONE_QUEUE=sqlite)"
                );
            }
            let token = env_or("RIPCLONE_QUEUE_DB_TOKEN", config().queue.token.as_deref()).context(
                "RIPCLONE_QUEUE=libsql requires RIPCLONE_QUEUE_DB_TOKEN (or [queue].token) for the remote database",
            )?;
            Box::new(LibsqlDb::connect_remote(&url, &token).await?)
        }
        other => anyhow::bail!(
            "RIPCLONE_QUEUE={other:?} is not a SQL queue backend \
             (expected 'sqlite', 'postgres', 'mysql', or 'libsql')"
        ),
    };
    let classes = crate::queue::load_size_classes(&config().queue.size_classes)?;
    SqlJobQueue::new_with_classes(db, classes).await
}

fn is_remote_url(url: &str) -> bool {
    ["libsql://", "http://", "https://", "ws://", "wss://"]
        .iter()
        .any(|p| url.starts_with(p))
}

/// The SQL queue connection URL, required by the SQL backends.
pub fn queue_db_url() -> Result<String> {
    env_or("RIPCLONE_QUEUE_DB_URL", config().queue.url.as_deref()).context(
        "RIPCLONE_QUEUE=sqlite|postgres|mysql|libsql requires RIPCLONE_QUEUE_DB_URL \
         (or [queue].url in config.toml) — sqlite: a local path; postgres: postgres://…; \
         mysql: mysql://…; libsql: a remote libsql:// url",
    )
}

#[cfg(test)]
mod tests {
    use super::env_or;

    #[test]
    fn env_or_prefers_env_then_config() {
        let key = "RIPCLONE_TEST_ENV_OR_PRECEDENCE";
        unsafe { std::env::remove_var(key) };
        // No env → fall back to config.
        assert_eq!(env_or(key, Some("cfg")), Some("cfg".to_string()));
        // Empty env counts as unset → still config.
        unsafe { std::env::set_var(key, "") };
        assert_eq!(env_or(key, Some("cfg")), Some("cfg".to_string()));
        // Env set → env wins over config.
        unsafe { std::env::set_var(key, "envval") };
        assert_eq!(env_or(key, Some("cfg")), Some("envval".to_string()));
        // Neither → None.
        unsafe { std::env::remove_var(key) };
        assert_eq!(env_or(key, None), None);
    }
}
