//! SQL-backed cross-process queue, shared across two SQLite-compatible engines:
//! `sqlite` (local file, via `sqlx`) and `libsql` (remote Turso Cloud). The
//! server `enqueue`s; a separate `ripclone-worker` process `claim`s, builds, and
//! `ack`s.
//!
//! [`QueueDb`] is a tiny per-engine adapter that returns plain Rust types (no
//! engine types leak); [`SqlJobQueue`] holds one and contains all the queue
//! orchestration, so the logic is written once and runs on either engine.
//!
//! ## Portability / correctness
//!
//! The orchestration uses only the common SQLite subset so the same SQL runs
//! unchanged on every engine — it does not lean on `BEGIN IMMEDIATE`, MVCC, or
//! `RETURNING`. Concretely:
//! - **Claim exclusivity** comes from an atomic conditional `UPDATE ... WHERE
//!   id = (oldest queued) AND status = 'queued'`, checking rows-affected — only
//!   one worker can flip a given row out of `queued` (SQLite serialises
//!   writers), so no job is double-claimed. Lost races retry.
//! - Ids come from `last_insert_rowid()`, not `RETURNING`.
//! - **Coalescing** (one build per repo/branch) is best-effort:
//!   `SELECT active`-then-`INSERT`, with a partial unique index attempted as a
//!   backstop. A rare duplicate job is wasted compute, not a wrong result — the
//!   poller watches its own job id and builds are idempotent into the CAS.

use super::{BuildJob, Enqueued, EnqueueOutcome, JobId, JobQueue, JobState};
use crate::provider::{ProviderInstanceId, RepoId};
use anyhow::Result;
use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH};

/// Default age (seconds) after which a `claimed` job is treated as abandoned (a
/// crashed worker) and returned to the queue. Override with
/// `RIPCLONE_QUEUE_STALE_SECS` — set it above your longest build so a slow build
/// is never reclaimed and double-run.
const DEFAULT_STALE_CLAIM_SECS: i64 = 1800;

/// Bound on claim retries under contention before giving up for this poll (the
/// caller polls again). Prevents an unbounded spin if many workers collide.
const MAX_CLAIM_ATTEMPTS: usize = 64;

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A job claimed by a worker. Tokens are never stored, so the worker supplies
/// its own (from `RIPCLONE_GITHUB_TOKEN` / its credential broker) when turning
/// this into a [`BuildJob`].
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub id: i64,
    /// Provider instance id (e.g. `github`), persisted so the worker can rebuild
    /// the full [`RepoId`] and resolve provider-specific credentials.
    pub provider: String,
    /// Opaque repo path (`owner/repo` for GitHub).
    pub path: String,
    pub branch: String,
}

impl ClaimedJob {
    /// Reconstruct the repo identity for the build.
    pub fn repo_id(&self) -> RepoId {
        RepoId {
            provider: ProviderInstanceId::new(self.provider.clone()),
            path: self.path.clone(),
        }
    }
}

/// Per-engine adapter. Each method runs one or two statements on a fresh
/// connection and returns plain Rust types. Implemented by `SqliteDb` and
/// `LibsqlDb`.
#[async_trait]
pub trait QueueDb: Send + Sync {
    /// Create the `jobs` table and indexes (best-effort on the partial unique
    /// index, which not every engine enforces).
    async fn init(&self) -> Result<()>;

    /// Id of the active (queued or claimed) job for `key`, if any.
    async fn active_job_id(&self, key: &str) -> Result<Option<i64>>;

    /// Insert a new queued job and return its id. Errors if a unique constraint
    /// rejects a duplicate active key (the caller treats that as coalesced).
    async fn insert_job(
        &self,
        key: &str,
        provider: &str,
        path: &str,
        branch: &str,
        created_at: i64,
    ) -> Result<i64>;

    /// Return any `claimed` job whose `claimed_at <= cutoff` to the queue.
    async fn reclaim_stale(&self, cutoff: i64) -> Result<()>;

    /// Id of the oldest queued job, if any.
    async fn next_queued_id(&self) -> Result<Option<i64>>;

    /// Atomically claim `id` if it is still `queued`. Returns true iff this call
    /// won the row.
    async fn try_claim(&self, id: i64, worker_id: &str, now: i64) -> Result<bool>;

    /// `(provider, path, branch)` for a job id.
    async fn job_fields(&self, id: i64) -> Result<Option<(String, String, String)>>;

    /// Mark a job finished: `status` is `done` or `failed`, with optional error.
    async fn finish(&self, id: i64, status: &str, finished_at: i64, error: Option<&str>)
    -> Result<()>;

    /// `(status, error)` for a job id.
    async fn status(&self, id: i64) -> Result<Option<(String, Option<String>)>>;

    /// Count of `queued` jobs.
    async fn count_queued(&self) -> Result<i64>;

    /// Delete `failed` jobs finished before `cutoff` (epoch secs). Returns the
    /// number removed. `done` jobs are intentionally kept (they are the build /
    /// version-live-at-time-T history and stay small at real commit rates).
    async fn prune_failed(&self, cutoff: i64) -> Result<u64>;
}

/// Default retention for `failed` jobs (seconds) before they are pruned. `done`
/// jobs are never pruned. Override with `RIPCLONE_QUEUE_FAILED_RETENTION_SECS`.
const DEFAULT_FAILED_RETENTION_SECS: i64 = 7 * 24 * 3600;

/// Cross-process queue over a [`QueueDb`].
pub struct SqlJobQueue {
    db: Box<dyn QueueDb>,
    stale_claim_secs: i64,
    failed_retention_secs: i64,
}

impl SqlJobQueue {
    /// Wrap an engine adapter and run schema setup.
    pub async fn new(db: Box<dyn QueueDb>) -> Result<Self> {
        db.init().await?;
        let stale_claim_secs = std::env::var("RIPCLONE_QUEUE_STALE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_STALE_CLAIM_SECS);
        let failed_retention_secs = std::env::var("RIPCLONE_QUEUE_FAILED_RETENTION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_FAILED_RETENTION_SECS);
        Ok(Self {
            db,
            stale_claim_secs,
            failed_retention_secs,
        })
    }

    /// Prune `failed` jobs older than the configured retention. Idempotent and
    /// safe to call from any worker; `done` jobs are kept. Returns rows removed.
    pub async fn prune_failed(&self) -> Result<u64> {
        self.db
            .prune_failed(now_secs() - self.failed_retention_secs)
            .await
    }

    /// Claim the oldest queued job for this worker, reclaiming abandoned claims
    /// first. Returns `None` when the queue is empty (or contention exhausted
    /// the retry budget — the caller polls again).
    pub async fn claim(&self, worker_id: &str) -> Result<Option<ClaimedJob>> {
        self.db
            .reclaim_stale(now_secs() - self.stale_claim_secs)
            .await?;
        for attempt in 0..MAX_CLAIM_ATTEMPTS {
            let Some(id) = self.db.next_queued_id().await? else {
                return Ok(None);
            };
            if self.db.try_claim(id, worker_id, now_secs()).await? {
                let Some((provider, path, branch)) = self.db.job_fields(id).await? else {
                    continue;
                };
                return Ok(Some(ClaimedJob {
                    id,
                    provider,
                    path,
                    branch,
                }));
            }
            // Lost the race for this row. Back off briefly before retrying so N
            // contending workers don't hammer the DB in lockstep (matters on a
            // network DB). Jitter by worker id keeps them out of phase.
            let jitter = (worker_id.len() as u64 % 4) + 1;
            tokio::time::sleep(std::time::Duration::from_millis(attempt as u64 + jitter)).await;
        }
        Ok(None)
    }

    /// Mark a claimed job finished. `Ok` → `done`; `Err(msg)` → `failed`.
    pub async fn ack(&self, id: i64, result: Result<(), String>) -> Result<()> {
        let (status, error) = match result {
            Ok(()) => ("done", None),
            Err(e) => ("failed", Some(e)),
        };
        self.db
            .finish(id, status, now_secs(), error.as_deref())
            .await
    }
}

#[async_trait]
impl JobQueue for SqlJobQueue {
    async fn enqueue(&self, job: BuildJob) -> Result<Enqueued> {
        let key = job.key();
        // Best-effort coalesce: fold into an already-active job for this key.
        if let Some(id) = self.db.active_job_id(&key).await? {
            return Ok(Enqueued {
                outcome: EnqueueOutcome::Coalesced,
                job_id: Some(id),
            });
        }
        match self
            .db
            .insert_job(
                &key,
                job.repo_id.provider.as_str(),
                &job.repo_id.path,
                &job.branch,
                now_secs(),
            )
            .await
        {
            Ok(id) => Ok(Enqueued {
                outcome: EnqueueOutcome::Enqueued,
                job_id: Some(id),
            }),
            Err(e) => {
                // A concurrent enqueue may have inserted first and tripped the
                // unique backstop; if an active job now exists, treat as coalesced.
                if let Some(id) = self.db.active_job_id(&key).await? {
                    Ok(Enqueued {
                        outcome: EnqueueOutcome::Coalesced,
                        job_id: Some(id),
                    })
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn job_status(&self, job_id: JobId) -> Result<JobState> {
        match self.db.status(job_id).await? {
            None => Ok(JobState::Unknown),
            Some((status, error)) => Ok(match status.as_str() {
                "done" => JobState::Done,
                "failed" => JobState::Failed(error.unwrap_or_else(|| "build failed".to_string())),
                _ => JobState::Pending,
            }),
        }
    }

    async fn depth(&self) -> usize {
        self.db.count_queued().await.map(|n| n as usize).unwrap_or(0)
    }

    fn inproc_wait(&self) -> bool {
        false
    }
}

/// Shared DDL for both engines.
pub(crate) const CREATE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    provider TEXT NOT NULL,
    path TEXT NOT NULL,
    branch TEXT NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT,
    created_at INTEGER NOT NULL,
    claimed_at INTEGER,
    finished_at INTEGER,
    error TEXT
)";

pub(crate) const CREATE_STATUS_INDEX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at)";

/// Best-effort coalescing backstop; not every engine enforces partial indexes.
pub(crate) const CREATE_ACTIVE_KEY_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_key
     ON jobs(key) WHERE status IN ('queued', 'claimed')";

/// Index for the build/version history queries over retained `done` jobs
/// ("what was synced for this repo over time").
pub(crate) const CREATE_HISTORY_INDEX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS idx_jobs_provider_path_finished ON jobs(provider, path, finished_at)";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::sqlite_db::SqliteDb;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn job(owner: &str, repo: &str, branch: &str) -> BuildJob {
        BuildJob {
            repo_id: RepoId::github(format!("{owner}/{repo}")),
            branch: branch.into(),
            rev: None,
            credential: None,
        }
    }

    /// Build a fresh queue on each supported local engine, backed by a temp file
    /// (a per-op connection model needs a real file, not `:memory:`). The libsql
    /// backend is remote-only (Turso Cloud) and can't be exercised in CI; it
    /// shares this exact orchestration + SQL, so the logic is covered by sqlite.
    async fn queues() -> Vec<(&'static str, Arc<SqlJobQueue>, tempfile::TempDir)> {
        let mut out = Vec::new();
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let db = make_db(engine, &dir.path().join("q.db").to_string_lossy()).await;
            out.push((engine, Arc::new(SqlJobQueue::new(db).await.unwrap()), dir));
        }
        out
    }

    async fn make_db(engine: &str, path: &str) -> Box<dyn QueueDb> {
        match engine {
            "sqlite" => Box::new(SqliteDb::connect(path).await.unwrap()),
            other => panic!("unknown test engine {other}"),
        }
    }

    #[tokio::test]
    async fn enqueue_claim_ack_roundtrip() {
        for (engine, q, _dir) in queues().await {
            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            assert_eq!(enq.outcome, EnqueueOutcome::Enqueued, "{engine}");
            assert!(enq.job_id.is_some(), "{engine}");
            assert_eq!(q.depth().await, 1, "{engine}");
            assert!(
                matches!(
                    q.job_status(enq.job_id.unwrap()).await.unwrap(),
                    JobState::Pending
                ),
                "{engine}"
            );

            let claimed = q.claim("w1").await.unwrap().unwrap();
            assert_eq!(
                (
                    claimed.provider.as_str(),
                    claimed.path.as_str(),
                    claimed.branch.as_str()
                ),
                ("github", "o/r", "main"),
                "{engine}"
            );
            assert_eq!(q.depth().await, 0, "{engine}: claimed no longer queued");
            assert!(q.claim("w1").await.unwrap().is_none(), "{engine}");

            q.ack(claimed.id, Ok(())).await.unwrap();
            assert!(
                matches!(q.job_status(claimed.id).await.unwrap(), JobState::Done),
                "{engine}"
            );
        }
    }

    #[tokio::test]
    async fn ack_failure_reports_error() {
        for (engine, q, _dir) in queues().await {
            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            let claimed = q.claim("w").await.unwrap().unwrap();
            q.ack(claimed.id, Err("boom".to_string())).await.unwrap();
            match q.job_status(enq.job_id.unwrap()).await.unwrap() {
                JobState::Failed(e) => assert_eq!(e, "boom", "{engine}"),
                other => panic!("{engine}: expected Failed, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn enqueue_coalesces_by_key() {
        for (engine, q, _dir) in queues().await {
            let first = q.enqueue(job("o", "r", "main")).await.unwrap();
            assert_eq!(first.outcome, EnqueueOutcome::Enqueued, "{engine}");
            let second = q.enqueue(job("o", "r", "main")).await.unwrap();
            assert_eq!(second.outcome, EnqueueOutcome::Coalesced, "{engine}");
            assert_eq!(first.job_id, second.job_id, "{engine}");
            assert_eq!(
                q.enqueue(job("o", "r", "dev")).await.unwrap().outcome,
                EnqueueOutcome::Enqueued,
                "{engine}"
            );
            assert_eq!(q.depth().await, 2, "{engine}");
        }
    }

    #[tokio::test]
    async fn coalesces_against_a_claimed_job() {
        for (engine, q, _dir) in queues().await {
            q.enqueue(job("o", "r", "main")).await.unwrap();
            let _claimed = q.claim("w").await.unwrap().unwrap();
            assert_eq!(
                q.enqueue(job("o", "r", "main")).await.unwrap().outcome,
                EnqueueOutcome::Coalesced,
                "{engine}"
            );
        }
    }

    #[tokio::test]
    async fn coalesces_to_fresh_job_after_completion() {
        for (engine, q, _dir) in queues().await {
            let first = q.enqueue(job("o", "r", "main")).await.unwrap();
            let claimed = q.claim("w").await.unwrap().unwrap();
            q.ack(claimed.id, Ok(())).await.unwrap();
            let second = q.enqueue(job("o", "r", "main")).await.unwrap();
            assert_eq!(second.outcome, EnqueueOutcome::Enqueued, "{engine}");
            assert_ne!(first.job_id, second.job_id, "{engine}");
        }
    }

    #[tokio::test]
    async fn stale_claim_is_reclaimed() {
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("q.db").to_string_lossy().to_string();
            let db = make_db(engine, &path).await;
            db.init().await.unwrap();
            // Zero tolerance: any claim is immediately reclaimable.
            let q = SqlJobQueue {
                db,
                stale_claim_secs: 0,
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
            };
            q.enqueue(job("o", "r", "main")).await.unwrap();
            let first = q.claim("w1").await.unwrap().unwrap();
            let second = q.claim("w2").await.unwrap().unwrap();
            assert_eq!(first.id, second.id, "{engine}");
        }
    }

    #[tokio::test]
    async fn fresh_claim_not_reclaimed_within_window() {
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("q.db").to_string_lossy().to_string();
            let db = make_db(engine, &path).await;
            db.init().await.unwrap();
            // Generous window: a just-claimed job must not be stolen.
            let q = SqlJobQueue {
                db,
                stale_claim_secs: 3600,
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
            };
            q.enqueue(job("o", "r", "main")).await.unwrap();
            let _first = q.claim("w1").await.unwrap().unwrap();
            assert!(
                q.claim("w2").await.unwrap().is_none(),
                "{engine}: a fresh claim must not be reclaimed within the window"
            );
        }
    }

    #[tokio::test]
    async fn prune_failed_removes_failed_keeps_done() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let db = make_db("sqlite", &path).await;
        db.init().await.unwrap();
        // Negative retention → cutoff is in the future, so any finished job is
        // eligible; only `failed` should actually be removed.
        let q = SqlJobQueue {
            db,
            stale_claim_secs: DEFAULT_STALE_CLAIM_SECS,
            failed_retention_secs: -1,
        };

        let failed = q.enqueue(job("o", "r", "fail")).await.unwrap();
        let c = q.claim("w").await.unwrap().unwrap();
        q.ack(c.id, Err("boom".to_string())).await.unwrap();

        let done = q.enqueue(job("o", "r", "ok")).await.unwrap();
        let c = q.claim("w").await.unwrap().unwrap();
        q.ack(c.id, Ok(())).await.unwrap();

        let removed = q.prune_failed().await.unwrap();
        assert_eq!(removed, 1, "only the failed job is pruned");
        assert!(matches!(
            q.job_status(failed.job_id.unwrap()).await.unwrap(),
            JobState::Unknown
        ));
        assert!(matches!(
            q.job_status(done.job_id.unwrap()).await.unwrap(),
            JobState::Done
        ));
    }

    /// Concurrent enqueues for the same key coalesce; concurrent claims are
    /// exclusive. Run on SQLite (the mature local engine).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_coalesce_and_claim_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::connect(&dir.path().join("q.db").to_string_lossy())
            .await
            .unwrap();
        let q = Arc::new(SqlJobQueue::new(Box::new(db)).await.unwrap());

        // 24 concurrent enqueues of one key → exactly one active job.
        let mut hs = Vec::new();
        for _ in 0..24 {
            let q = q.clone();
            hs.push(tokio::spawn(
                async move { q.enqueue(job("o", "r", "main")).await },
            ));
        }
        for h in hs {
            h.await.unwrap().expect("enqueue must not error under contention");
        }
        assert_eq!(q.depth().await, 1, "concurrent enqueues coalesced");

        // Enqueue distinct jobs, drain with 4 workers — none double-claimed.
        for i in 0..20 {
            q.enqueue(job("o", "r", &format!("b{i}"))).await.unwrap();
        }
        let seen = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let mut hs = Vec::new();
        for w in 0..4 {
            let (q, seen) = (q.clone(), seen.clone());
            hs.push(tokio::spawn(async move {
                let wid = format!("w{w}");
                while let Some(c) = q.claim(&wid).await.unwrap() {
                    assert!(seen.lock().await.insert(c.id), "job {} double-claimed", c.id);
                }
            }));
        }
        for h in hs {
            h.await.unwrap();
        }
        // 20 distinct branches + the 1 coalesced "main".
        assert_eq!(seen.lock().await.len(), 21, "every job claimed exactly once");
    }

    // ---- Postgres / MySQL: exercised against a real server (env-gated) --------
    //
    // These need a live network DB, so they run only when RIPCLONE_TEST_PG_URL /
    // RIPCLONE_TEST_MYSQL_URL is set (e.g. by scripts/test-queue-sql.sh against
    // docker). They cover the dialect-sensitive paths: DDL,
    // `$N` vs `?` placeholders, RETURNING vs last_insert_id, coalescing (partial
    // index on pg, best-effort on mysql), the conditional-UPDATE claim, and
    // status/error reads. Single test per engine → no intra-engine concurrency.

    /// Full queue lifecycle on a fresh queue: enqueue, coalesce, distinct key,
    /// claim ordering, ack done/failed, drain, and a fresh job after completion.
    async fn exercise_core(q: &SqlJobQueue) {
        let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
        assert_eq!(enq.outcome, EnqueueOutcome::Enqueued);
        let id = enq.job_id.unwrap();
        assert_eq!(q.depth().await, 1);
        assert!(matches!(q.job_status(id).await.unwrap(), JobState::Pending));

        let coalesced = q.enqueue(job("o", "r", "main")).await.unwrap();
        assert_eq!(coalesced.outcome, EnqueueOutcome::Coalesced);
        assert_eq!(coalesced.job_id, Some(id));

        let other = q.enqueue(job("o", "r", "dev")).await.unwrap();
        assert_eq!(other.outcome, EnqueueOutcome::Enqueued);
        assert_eq!(q.depth().await, 2);

        let first = q.claim("w1").await.unwrap().unwrap();
        assert_eq!(first.branch, "main", "oldest queued claimed first");
        q.ack(first.id, Ok(())).await.unwrap();
        assert!(matches!(q.job_status(first.id).await.unwrap(), JobState::Done));

        let second = q.claim("w1").await.unwrap().unwrap();
        assert_eq!(second.branch, "dev");
        q.ack(second.id, Err("boom".to_string())).await.unwrap();
        match q.job_status(second.id).await.unwrap() {
            JobState::Failed(e) => assert_eq!(e, "boom"),
            o => panic!("expected Failed, got {o:?}"),
        }

        assert_eq!(q.depth().await, 0);
        assert!(q.claim("w1").await.unwrap().is_none());

        // A completed key gets a brand new job, not the old id.
        let fresh = q.enqueue(job("o", "r", "main")).await.unwrap();
        assert_eq!(fresh.outcome, EnqueueOutcome::Enqueued);
        assert_ne!(fresh.job_id, Some(id));
    }

    #[tokio::test]
    async fn postgres_queue_lifecycle() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_PG_URL") else {
            eprintln!("SKIP postgres_queue_lifecycle: RIPCLONE_TEST_PG_URL unset");
            return;
        };
        let pool = sqlx::postgres::PgPool::connect(&url).await.expect("connect pg");
        sqlx::query("DROP TABLE IF EXISTS jobs")
            .execute(&pool)
            .await
            .expect("drop jobs");
        pool.close().await;
        let q = SqlJobQueue::new(Box::new(
            crate::queue::postgres_db::PostgresDb::connect(&url).await.unwrap(),
        ))
        .await
        .unwrap();
        exercise_core(&q).await;
    }

    #[tokio::test]
    async fn mysql_queue_lifecycle() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL") else {
            eprintln!("SKIP mysql_queue_lifecycle: RIPCLONE_TEST_MYSQL_URL unset");
            return;
        };
        let pool = sqlx::mysql::MySqlPool::connect(&url).await.expect("connect mysql");
        sqlx::query("DROP TABLE IF EXISTS jobs")
            .execute(&pool)
            .await
            .expect("drop jobs");
        pool.close().await;
        let q = SqlJobQueue::new(Box::new(
            crate::queue::mysql_db::MysqlDb::connect(&url).await.unwrap(),
        ))
        .await
        .unwrap();
        exercise_core(&q).await;
    }
}
