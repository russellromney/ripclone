//! Env-driven construction of the pluggable backends, shared by the
//! `ripclone-server` and the standalone `ripclone-worker` so both wire up
//! identically: same storage, same metadata store, same queue.
//!
//! A build needs only durable state — blob `storage` (artifacts) and the
//! `ref_store` (metadata) — plus the local CAS cache and a scratch mirror root.
//! That is why the worker can run anywhere: it owns no durable state.

use crate::api_job_queue::ApiJobQueue;
use crate::api_ref_store::ApiRefStore;
use crate::artifact_manifest::CasCompletionVerifier;
use crate::artifact_scheduler::{CompletionVerifier, SchedulerLimits};
use crate::artifact_scheduler_backend::ArtifactSchedulerPersistence;
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
    /// Normalized artifact scheduler. Present only when the caller explicitly
    /// supplies a production completion verifier.
    pub artifact_scheduler: Option<Arc<dyn ArtifactSchedulerPersistence>>,
    /// The exact strict verifier installed in the normalized scheduler. Typed
    /// builders, admission, clone transport, scrub, and GC must clone this Arc;
    /// reconstructing policy independently can split format/proof authority.
    pub artifact_verifier: Option<Arc<CasCompletionVerifier>>,
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
        Self::from_env_inner(cas_dir, repo_root, metrics, SchedulerRequest::Disabled).await
    }

    /// Production server/worker backend selection. The normalized scheduler is
    /// opt-in until typed builders replace the legacy build worker. Selecting
    /// it constructs the real CAS verifier and fails startup on any unsupported
    /// metadata/configuration; there is no fallback to the legacy scheduler.
    pub async fn from_env_for_runtime(
        cas_dir: &Path,
        repo_root: &Path,
        metrics: &Arc<Metrics>,
    ) -> Result<Self> {
        Self::from_env_inner(cas_dir, repo_root, metrics, artifact_scheduler_request()?).await
    }

    /// Build the regular backends and the normalized artifact scheduler on the
    /// same SQL metadata connection. Callers must inject the production CAS
    /// verifier; this API never substitutes the scheduler's structural test
    /// verifier. Non-SQL metadata configurations fail closed.
    pub async fn from_env_with_artifact_scheduler(
        cas_dir: &Path,
        repo_root: &Path,
        metrics: &Arc<Metrics>,
        limits: SchedulerLimits,
        verifier: Arc<dyn CompletionVerifier>,
    ) -> Result<Self> {
        Self::from_env_inner(
            cas_dir,
            repo_root,
            metrics,
            SchedulerRequest::Injected(limits, verifier),
        )
        .await
    }

    async fn from_env_inner(
        cas_dir: &Path,
        repo_root: &Path,
        metrics: &Arc<Metrics>,
        scheduler: SchedulerRequest,
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
        let (scheduler, artifact_verifier) = match scheduler {
            SchedulerRequest::Disabled => (None, None),
            SchedulerRequest::Production => {
                let verifier = Arc::new(CasCompletionVerifier::from_env_with_limits(
                    cas.clone(),
                    storage.clone(),
                    crate::artifact_manifest::ArtifactVerificationLimits::default(),
                )?);
                (
                    Some((
                        SchedulerLimits::default(),
                        verifier.clone() as Arc<dyn CompletionVerifier>,
                    )),
                    Some(verifier),
                )
            }
            SchedulerRequest::Injected(limits, verifier) => (Some((limits, verifier)), None),
        };
        let (ref_store, artifact_scheduler) =
            select_metadata(repo_root, s3.as_ref(), scheduler).await?;
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
            artifact_scheduler,
            artifact_verifier,
        })
    }
}

enum SchedulerRequest {
    Disabled,
    Production,
    Injected(SchedulerLimits, Arc<dyn CompletionVerifier>),
}

fn artifact_scheduler_request() -> Result<SchedulerRequest> {
    let mode = std::env::var("RIPCLONE_ARTIFACT_SCHEDULER").ok();
    parse_artifact_scheduler_request(mode.as_deref())
}

fn parse_artifact_scheduler_request(mode: Option<&str>) -> Result<SchedulerRequest> {
    let mode = mode.unwrap_or("legacy");
    match mode {
        "legacy" => Ok(SchedulerRequest::Disabled),
        "normalized" => Ok(SchedulerRequest::Production),
        other => anyhow::bail!(
            "unknown RIPCLONE_ARTIFACT_SCHEDULER mode: {other:?} \
             (expected 'legacy' or 'normalized')"
        ),
    }
}

/// Select the metadata store from `RIPCLONE_METADATA`:
/// `file` | `s3` | `sqlite` | `postgres` | `mysql` | `libsql` | `api`. Unset
/// preserves the historical default: S3 when S3 storage is configured, else
/// file. SQL backends read `RIPCLONE_METADATA_DB_URL` (+
/// `RIPCLONE_METADATA_DB_TOKEN` for libsql). `api` is worker-only: it POSTs
/// ref-writes to `RIPCLONE_METADATA_REPORT_URL` with
/// `RIPCLONE_METADATA_JOB_TOKEN` and holds no DB credentials. The result is
/// always wrapped in `CachingRefStore`.
async fn select_metadata(
    repo_root: &Path,
    s3: Option<&Arc<S3Storage>>,
    scheduler: Option<(SchedulerLimits, Arc<dyn CompletionVerifier>)>,
) -> Result<(
    Arc<dyn RefStore>,
    Option<Arc<dyn ArtifactSchedulerPersistence>>,
)> {
    use crate::meta::MetaDb;
    use crate::meta::{LibsqlMeta, MysqlMeta, PostgresMeta, SqlRefStore, SqliteMeta};

    let kind =
        env_or("RIPCLONE_METADATA", config().metadata.backend.as_deref()).unwrap_or_default();
    if scheduler.is_some() {
        validate_scheduler_metadata_selection(&kind)?;
    }

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
                 (RIPCLONE_METADATA=s3|sqlite|postgres|mysql|libsql|api) so the server and \
                 workers share refs."
            );
        }
    }

    // Each arm wraps its concrete store in CachingRefStore (the read cache) and
    // coerces to Arc<dyn RefStore>.
    let (store, artifact_scheduler): (
        Arc<dyn RefStore>,
        Option<Arc<dyn ArtifactSchedulerPersistence>>,
    ) = match kind.as_str() {
        "" => {
            if scheduler.is_some() {
                anyhow::bail!(
                    "artifact scheduler requires an explicit SQL metadata backend \
                     (RIPCLONE_METADATA=sqlite|postgres|mysql|libsql)"
                )
            }
            // Backward compatible: metadata follows storage.
            if let Some(s3) = s3 {
                info!("metadata store: s3 (default, follows storage)");
                (
                    Arc::new(CachingRefStore::new(S3RefStore::new(s3.clone()))),
                    None,
                )
            } else {
                info!("metadata store: file (default)");
                (
                    Arc::new(CachingRefStore::new(FileRefStore::new(repo_root))),
                    None,
                )
            }
        }
        "file" => {
            if scheduler.is_some() {
                anyhow::bail!("artifact scheduler is unavailable with RIPCLONE_METADATA=file")
            }
            info!("metadata store: file");
            (
                Arc::new(CachingRefStore::new(FileRefStore::new(repo_root))),
                None,
            )
        }
        "s3" => {
            if scheduler.is_some() {
                anyhow::bail!("artifact scheduler is unavailable with RIPCLONE_METADATA=s3")
            }
            let s3 = s3.context("RIPCLONE_METADATA=s3 requires S3 storage env (RIPCLONE_S3_*)")?;
            info!("metadata store: s3");
            (
                Arc::new(CachingRefStore::new(S3RefStore::new(s3.clone()))),
                None,
            )
        }
        "sqlite" | "postgres" | "mysql" | "libsql" => {
            let url = env_or("RIPCLONE_METADATA_DB_URL", config().metadata.url.as_deref()).context(
                "RIPCLONE_METADATA=sqlite|postgres|mysql|libsql requires RIPCLONE_METADATA_DB_URL (or [metadata].url)",
            )?;
            let (db, artifact_scheduler): (
                Box<dyn MetaDb>,
                Option<Arc<dyn ArtifactSchedulerPersistence>>,
            ) = match kind.as_str() {
                "sqlite" => {
                    let db = SqliteMeta::connect(&url).await?;
                    let scheduler = match scheduler.as_ref() {
                        Some((limits, verifier)) => Some(Arc::new(
                            db.artifact_scheduler(limits.clone(), verifier.clone())
                                .await?,
                        )
                            as Arc<dyn ArtifactSchedulerPersistence>),
                        None => None,
                    };
                    (Box::new(db), scheduler)
                }
                "postgres" => {
                    let db = PostgresMeta::connect(&url).await?;
                    let scheduler = match scheduler.as_ref() {
                        Some((limits, verifier)) => Some(Arc::new(
                            db.artifact_scheduler(limits.clone(), verifier.clone())
                                .await?,
                        )
                            as Arc<dyn ArtifactSchedulerPersistence>),
                        None => None,
                    };
                    (Box::new(db), scheduler)
                }
                "mysql" => {
                    let db = MysqlMeta::connect(&url).await?;
                    let scheduler = match scheduler.as_ref() {
                        Some((limits, verifier)) => Some(Arc::new(
                            db.artifact_scheduler(limits.clone(), verifier.clone())
                                .await?,
                        )
                            as Arc<dyn ArtifactSchedulerPersistence>),
                        None => None,
                    };
                    (Box::new(db), scheduler)
                }
                "libsql" => {
                    if !is_remote_url(&url) {
                        anyhow::bail!(
                            "RIPCLONE_METADATA=libsql is remote-only; RIPCLONE_METADATA_DB_URL \
                             must be a libsql:// or https:// url (local file → use sqlite)"
                        );
                    }
                    let token = env_or("RIPCLONE_METADATA_DB_TOKEN", config().metadata.token.as_deref())
                        .context("RIPCLONE_METADATA=libsql requires RIPCLONE_METADATA_DB_TOKEN (or [metadata].token)")?;
                    let db = LibsqlMeta::connect_remote(&url, &token).await?;
                    let scheduler = match scheduler.as_ref() {
                        Some((limits, verifier)) => Some(Arc::new(
                            db.artifact_scheduler(limits.clone(), verifier.clone())
                                .await?,
                        )
                            as Arc<dyn ArtifactSchedulerPersistence>),
                        None => None,
                    };
                    (Box::new(db), scheduler)
                }
                _ => unreachable!(),
            };
            info!("metadata store: {kind}");
            (
                Arc::new(CachingRefStore::new(SqlRefStore::new(db).await?)),
                artifact_scheduler,
            )
        }
        "api" => {
            if scheduler.is_some() {
                anyhow::bail!(
                    "artifact scheduler cannot use RIPCLONE_METADATA=api; workers need a \
                     scheduler API or direct SQL metadata connection"
                )
            }
            // Worker-only: POSTs ref-writes to the server. No DB URL, no DB token.
            // Fail loudly if the report URL or job token is missing (same style as
            // the libsql arm without its token).
            let store = ApiRefStore::from_env()?;
            (Arc::new(CachingRefStore::new(store)), None)
        }
        other => anyhow::bail!(
            "unknown RIPCLONE_METADATA backend: {other:?} \
             (expected 'file', 's3', 'sqlite', 'postgres', 'mysql', 'libsql', or 'api')"
        ),
    };
    Ok((store, artifact_scheduler))
}

fn validate_scheduler_metadata_selection(kind: &str) -> Result<()> {
    match kind {
        "sqlite" | "postgres" | "mysql" | "libsql" => Ok(()),
        "" => anyhow::bail!(
            "artifact scheduler requires an explicit SQL metadata backend \
             (RIPCLONE_METADATA=sqlite|postgres|mysql|libsql)"
        ),
        "file" | "s3" | "api" => {
            anyhow::bail!("artifact scheduler is unavailable with RIPCLONE_METADATA={kind}")
        }
        other => anyhow::bail!(
            "unknown RIPCLONE_METADATA backend for artifact scheduler: {other:?} \
             (expected 'sqlite', 'postgres', 'mysql', or 'libsql')"
        ),
    }
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
    /// the server spawns no in-process worker. The concrete `Arc<SqlJobQueue>`
    /// is kept (not just `JobQueueRef`) so the server can also serve the
    /// worker-facing `/v1/jobs/*` endpoints (claim/ack/heartbeat) from it.
    Sql { queue: Arc<SqlJobQueue> },
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
        "api" => anyhow::bail!(
            "RIPCLONE_QUEUE=api is worker-only (a farm-out worker claims over HTTP). \
             The server holds the real queue — set RIPCLONE_QUEUE=sqlite|postgres|mysql|libsql \
             on the server."
        ),
        other => anyhow::bail!(
            "unknown RIPCLONE_QUEUE backend: {other:?} \
             (expected 'local', 'sqlite', 'postgres', 'mysql', or 'libsql')"
        ),
    }
}

/// The queue a `ripclone-worker` claims from. Either a direct SQL connection
/// (trusted single-box) or the HTTP [`ApiJobQueue`] (farm-out, token-only).
pub enum WorkerQueueBackend {
    Sql(Arc<SqlJobQueue>),
    Api(Arc<ApiJobQueue>),
}

/// Select the worker's queue from `RIPCLONE_QUEUE`. `api` builds an
/// [`ApiJobQueue`] (base URL + bearer token, no DB creds); the SQL kinds build a
/// direct [`SqlJobQueue`] with `max_size_class` applied. Unlike [`select_queue`]
/// (the server side), `api` is valid here — this is the farm-out path.
pub async fn connect_worker_queue(max_size_class: Option<&str>) -> Result<WorkerQueueBackend> {
    let kind = queue_kind();
    match kind.as_str() {
        "api" => {
            let queue = ApiJobQueue::from_env()?;
            info!("using API build queue (RIPCLONE_QUEUE=api); no DB credentials on this worker");
            Ok(WorkerQueueBackend::Api(Arc::new(queue)))
        }
        "sqlite" | "postgres" | "mysql" | "libsql" => {
            let queue = connect_sql_queue()
                .await?
                .with_max_size_class(max_size_class)
                .context("resolve --max-size-class")?;
            Ok(WorkerQueueBackend::Sql(Arc::new(queue)))
        }
        "local" => anyhow::bail!(
            "RIPCLONE_QUEUE=local is the in-process server queue; a standalone worker \
             needs RIPCLONE_QUEUE=api (farm-out) or sqlite|postgres|mysql|libsql (direct)"
        ),
        other => anyhow::bail!(
            "unknown RIPCLONE_QUEUE backend: {other:?} \
             (expected 'api', 'sqlite', 'postgres', 'mysql', or 'libsql')"
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
    use super::{
        SchedulerRequest, env_or, parse_artifact_scheduler_request,
        validate_scheduler_metadata_selection,
    };

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

    #[test]
    fn artifact_scheduler_selection_is_explicit_sql_only() {
        for kind in ["sqlite", "postgres", "mysql", "libsql"] {
            assert!(
                validate_scheduler_metadata_selection(kind).is_ok(),
                "{kind}"
            );
        }
        for kind in [
            "",
            "file",
            "s3",
            "api",
            "LOCAL",
            " sqlite",
            "postgres ",
            "bogus",
        ] {
            let error = validate_scheduler_metadata_selection(kind)
                .expect_err("unsafe or malformed selection must fail closed")
                .to_string();
            assert!(error.contains("artifact scheduler"), "{kind:?}: {error}");
        }
    }

    #[test]
    fn runtime_scheduler_cutover_selector_fails_closed() {
        assert!(matches!(
            parse_artifact_scheduler_request(None).unwrap(),
            SchedulerRequest::Disabled
        ));
        assert!(matches!(
            parse_artifact_scheduler_request(Some("legacy")).unwrap(),
            SchedulerRequest::Disabled
        ));
        assert!(matches!(
            parse_artifact_scheduler_request(Some("normalized")).unwrap(),
            SchedulerRequest::Production
        ));
        for invalid in ["", "NORMALIZED", " normalized", "auto", "sqlite"] {
            assert!(
                parse_artifact_scheduler_request(Some(invalid)).is_err(),
                "{invalid:?}"
            );
        }
    }
}
