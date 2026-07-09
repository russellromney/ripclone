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

use super::{BuildError, BuildJob, EnqueueOutcome, Enqueued, JobId, JobQueue, JobState};
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

/// Default cap on how many times a job may be claimed before it is dead-lettered
/// to terminal `failed` instead of being requeued. A SIGKILL/OOM crash leaves
/// the row `claimed` with no ack; the stale-reclaim would otherwise requeue it
/// forever (a crash-loop). Override with `RIPCLONE_QUEUE_MAX_ATTEMPTS`.
const DEFAULT_MAX_BUILD_ATTEMPTS: i64 = 5;

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A job claimed by a worker.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub id: i64,
    /// Provider instance id (e.g. `github`), persisted so the worker can rebuild
    /// the full [`RepoId`] and resolve provider-specific credentials.
    pub provider: String,
    /// Opaque repo path (`owner/repo` for GitHub).
    pub path: String,
    pub branch: String,
    /// Per-job upstream credential the enqueuer passed (the cloud's per-request
    /// `X-Upstream-Token`), so a cross-process worker can read a private repo it
    /// has no standing credential for. `None` falls back to the worker's broker.
    /// SECURITY: stored only base64-obfuscated in the queue DB — treat that DB as
    /// sensitive and access-controlled. (Tokens are short-lived; encryption-at-
    /// rest with a worker-shared key is a noted follow-up.)
    pub credential: Option<secrecy::SecretString>,
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

/// Base64-encode a per-job credential for storage (obfuscation, not encryption —
/// see [`ClaimedJob::credential`]).
pub(crate) fn encode_credential(cred: Option<&secrecy::SecretString>) -> Option<String> {
    use base64::Engine;
    use secrecy::ExposeSecret;
    cred.map(|c| base64::engine::general_purpose::STANDARD.encode(c.expose_secret()))
}

/// Decode a stored credential back into a secret. A malformed value decodes to
/// `None` (the worker then falls back to its broker) rather than erroring.
pub(crate) fn decode_credential(enc: Option<String>) -> Option<secrecy::SecretString> {
    use base64::Engine;
    enc.and_then(|e| base64::engine::general_purpose::STANDARD.decode(e).ok())
        .and_then(|b| String::from_utf8(b).ok())
        .map(|s| secrecy::SecretString::new(s.into()))
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
        credential: Option<&str>,
        created_at: i64,
    ) -> Result<i64>;

    /// Resolve `claimed` jobs whose `claimed_at <= cutoff` (a crashed or
    /// timed-out worker). A job that has already been claimed `max_attempts` or
    /// more times is dead-lettered to terminal `failed` (with `dead_letter_error`
    /// and `now` as its finished time) so a hard-killed build can't crash-loop;
    /// anything under the cap is returned to `queued` for another attempt, with
    /// `size_class` bumped one rung so a larger worker can claim it next
    /// (right-sizing / O2). Dead-letter does not bump.
    async fn reclaim_stale(
        &self,
        cutoff: i64,
        max_attempts: i64,
        now: i64,
        dead_letter_error: &str,
    ) -> Result<()>;

    /// Current `size_class` for a job id (`None` if the row is missing).
    async fn job_size_class(&self, id: i64) -> Result<Option<i64>>;

    /// Id of the oldest queued job, if any.
    async fn next_queued_id(&self) -> Result<Option<i64>>;

    /// Atomically claim `id` if it is still `queued`, incrementing its
    /// `attempts` counter. Returns true iff this call won the row.
    async fn try_claim(&self, id: i64, worker_id: &str, now: i64) -> Result<bool>;

    /// `(provider, path, branch, credential)` for a job id. `credential` is the
    /// stored base64 blob (or `None`).
    async fn job_fields(&self, id: i64)
    -> Result<Option<(String, String, String, Option<String>)>>;

    /// Settle a claimed job: `status` is `done` or `failed`, with optional
    /// error. Conditional on the caller still owning the claim — the UPDATE
    /// matches only `id = ? AND worker_id = ? AND status = 'claimed'`. Returns
    /// true iff a row was settled; false means the claim was reclaimed and
    /// re-owned (or dead-lettered) while this worker was building, so its result
    /// must be discarded — the row belongs to whoever holds the claim now.
    async fn finish(
        &self,
        id: i64,
        worker_id: &str,
        status: &str,
        finished_at: i64,
        error: Option<&str>,
    ) -> Result<bool>;

    /// Current attempt count for a claim owned by `worker_id`.
    async fn claimed_attempts(&self, id: i64, worker_id: &str) -> Result<Option<i64>>;

    /// Requeue a retryable build failure while the caller still owns the claim.
    /// Returns false if the claim was reclaimed or otherwise settled first.
    async fn requeue_claim(&self, id: i64, worker_id: &str, error: &str) -> Result<bool>;

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
    max_build_attempts: i64,
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
        let max_build_attempts = std::env::var("RIPCLONE_QUEUE_MAX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(DEFAULT_MAX_BUILD_ATTEMPTS);
        Ok(Self {
            db,
            stale_claim_secs,
            failed_retention_secs,
            max_build_attempts,
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
        let now = now_secs();
        self.db
            .reclaim_stale(
                now - self.stale_claim_secs,
                self.max_build_attempts,
                now,
                &format!(
                    "dead-lettered after {} build attempts (worker crashed or timed out)",
                    self.max_build_attempts
                ),
            )
            .await?;
        for attempt in 0..MAX_CLAIM_ATTEMPTS {
            let Some(id) = self.db.next_queued_id().await? else {
                return Ok(None);
            };
            if self.db.try_claim(id, worker_id, now_secs()).await? {
                let Some((provider, path, branch, credential)) = self.db.job_fields(id).await?
                else {
                    continue;
                };
                return Ok(Some(ClaimedJob {
                    id,
                    provider,
                    path,
                    branch,
                    credential: decode_credential(credential),
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

    /// Settle a claimed job.
    ///
    /// - `Ok(())` → terminal `done`
    /// - `Err(permanent)` → terminal `failed` immediately
    /// - `Err(retryable)` under the attempts cap → requeue with capped backoff
    /// - `Err(retryable)` at/over the attempts cap → terminal `failed` (dead-letter)
    ///
    /// Conditional on `worker_id` still owning the claim; returns `Ok(true)` if
    /// it settled (or requeued), `Ok(false)` if the claim had been
    /// reclaimed/dead-lettered out from under this worker (its result is stale
    /// and must be discarded — see [`QueueDb::finish`]).
    pub async fn ack(
        &self,
        id: i64,
        worker_id: &str,
        result: Result<(), BuildError>,
    ) -> Result<bool> {
        let (status, error) = match result {
            Ok(()) => ("done", None),
            Err(e) if e.is_retryable() => {
                let message = e.message().to_string();
                let attempts = self.db.claimed_attempts(id, worker_id).await?;
                let Some(attempts) = attempts else {
                    return Ok(false);
                };
                if attempts >= self.max_build_attempts {
                    let error = self.dead_letter_error(&message);
                    return self
                        .db
                        .finish(id, worker_id, "failed", now_secs(), Some(&error))
                        .await;
                }
                tokio::time::sleep(retry_backoff(attempts)).await;
                return self.db.requeue_claim(id, worker_id, &message).await;
            }
            Err(e) => ("failed", Some(e.message().to_string())),
        };
        self.db
            .finish(id, worker_id, status, now_secs(), error.as_deref())
            .await
    }

    fn dead_letter_error(&self, error: &str) -> String {
        format!(
            "dead-lettered after {} build attempts: {error}",
            self.max_build_attempts
        )
    }
}

fn retry_backoff(attempts: i64) -> std::time::Duration {
    let base_ms = std::env::var("RIPCLONE_QUEUE_RETRY_BACKOFF_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(250);
    let shift = attempts.saturating_sub(1).min(5) as u32;
    std::time::Duration::from_millis(base_ms * 2_u64.saturating_pow(shift))
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
        let credential = encode_credential(job.credential.as_ref());
        match self
            .db
            .insert_job(
                &key,
                job.repo_id.provider.as_str(),
                &job.repo_id.path,
                &job.branch,
                credential.as_deref(),
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
        self.db
            .count_queued()
            .await
            .map(|n| n as usize)
            .unwrap_or(0)
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
    error TEXT,
    credential TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    size_class INTEGER NOT NULL DEFAULT 0
)";

pub(crate) const CREATE_STATUS_INDEX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at)";

/// Drop the older index that made (queued OR claimed) unique per key. That
/// uniqueness blocked queuing a fresh build for a key whose previous build was
/// already claimed (and had already fetched), so a push arriving mid-build was
/// dropped until the next push. Best-effort.
pub(crate) const DROP_LEGACY_ACTIVE_KEY_INDEX_SQL: &str =
    "DROP INDEX IF EXISTS idx_jobs_active_key";

/// Best-effort coalescing backstop: at most one *queued* build per key, so
/// concurrent pushes collapse into one. A claimed build can coexist with a
/// queued one, so a push that lands while a build is in flight gets its own
/// queued job and builds the newer commit next.
pub(crate) const CREATE_ACTIVE_KEY_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_queued_key
     ON jobs(key) WHERE status = 'queued'";

/// Index for the build/version history queries over retained `done` jobs
/// ("what was synced for this repo over time").
pub(crate) const CREATE_HISTORY_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS idx_jobs_provider_path_finished ON jobs(provider, path, finished_at)";

/// Migration for a `jobs` table created before the `credential` column existed.
/// `CREATE TABLE IF NOT EXISTS` is a no-op on an existing table, so it never adds
/// the column — run this ALTER best-effort (it errors "duplicate column" on a
/// fresh table, which is ignored). SQLite/libsql have no `ADD COLUMN IF NOT
/// EXISTS`, hence best-effort; Postgres uses its own `IF NOT EXISTS` form.
pub(crate) const ADD_CREDENTIAL_COLUMN_SQL: &str = "ALTER TABLE jobs ADD COLUMN credential TEXT";

/// Migration for a `jobs` table created before the `attempts` column existed.
/// Best-effort like [`ADD_CREDENTIAL_COLUMN_SQL`]: errors "duplicate column" on
/// a fresh/up-to-date table, which is ignored.
pub(crate) const ADD_ATTEMPTS_COLUMN_SQL: &str =
    "ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0";

/// Migration for a `jobs` table created before the `size_class` column existed.
/// Best-effort like [`ADD_ATTEMPTS_COLUMN_SQL`]. Stale-reclaim bumps this rung so
/// a larger worker can pick the job up next (claim filter lands in O2).
pub(crate) const ADD_SIZE_CLASS_COLUMN_SQL: &str =
    "ALTER TABLE jobs ADD COLUMN size_class INTEGER NOT NULL DEFAULT 0";

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
            recheck: 0,
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

            assert!(
                q.ack(claimed.id, "w1", Ok(())).await.unwrap(),
                "{engine}: the owning worker settles its own claim"
            );
            assert!(
                matches!(q.job_status(claimed.id).await.unwrap(), JobState::Done),
                "{engine}"
            );
        }
    }

    #[tokio::test]
    async fn per_job_credential_round_trips_through_the_queue() {
        use secrecy::ExposeSecret;
        for (engine, q, _dir) in queues().await {
            let mut j = job("o", "r", "main");
            j.credential = Some(secrecy::SecretString::new(
                "ghs_secret123".to_string().into(),
            ));
            q.enqueue(j).await.unwrap();
            let claimed = q.claim("w1").await.unwrap().unwrap();
            let cred = claimed.credential.expect("credential persisted");
            assert_eq!(cred.expose_secret(), "ghs_secret123", "{engine}");
        }
    }

    #[tokio::test]
    async fn absent_credential_stays_none() {
        for (engine, q, _dir) in queues().await {
            q.enqueue(job("o", "r", "main")).await.unwrap();
            let claimed = q.claim("w1").await.unwrap().unwrap();
            assert!(claimed.credential.is_none(), "{engine}");
        }
    }

    #[tokio::test]
    async fn finish_clears_the_stored_credential() {
        // A short-lived upstream token must not linger in the kept-forever
        // done-job history. (Adapter-level: SqliteDb directly.)
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::connect(&dir.path().join("q.db").to_string_lossy())
            .await
            .unwrap();
        db.init().await.unwrap();
        let id = db
            .insert_job("k", "github", "o/r", "main", Some("dG9rZW4="), 1)
            .await
            .unwrap();
        let (_, _, _, before) = db.job_fields(id).await.unwrap().unwrap();
        assert_eq!(before.as_deref(), Some("dG9rZW4="));
        // finish is conditional on owning the claim: claim it as "w" first.
        assert!(db.try_claim(id, "w", 2).await.unwrap());
        assert!(db.finish(id, "w", "done", 3, None).await.unwrap());
        let (_, _, _, after) = db.job_fields(id).await.unwrap().unwrap();
        assert!(after.is_none(), "credential must be cleared on finish");
    }

    #[tokio::test]
    async fn init_migrates_a_legacy_jobs_table_and_is_idempotent() {
        use sqlx::sqlite::SqlitePoolOptions;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        // A pre-credential `jobs` table, created by hand (no credential column).
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let pool = SqlitePoolOptions::new().connect(&url).await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL, \
             provider TEXT NOT NULL, path TEXT NOT NULL, branch TEXT NOT NULL, \
             status TEXT NOT NULL, worker_id TEXT, created_at INTEGER NOT NULL, \
             claimed_at INTEGER, finished_at INTEGER, error TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let db = SqliteDb::connect(&path.to_string_lossy()).await.unwrap();
        db.init().await.unwrap(); // adds credential / attempts / size_class columns
        db.init().await.unwrap(); // idempotent: best-effort ALTER ignores duplicate
        // Inserting a credential now works because the column exists.
        let id = db
            .insert_job("k", "github", "o/r", "main", Some("Y3JlZA=="), 1)
            .await
            .unwrap();
        let (_, _, _, cred) = db.job_fields(id).await.unwrap().unwrap();
        assert_eq!(cred.as_deref(), Some("Y3JlZA=="));
        // size_class migration is load-bearing for stale-reclaim escalation.
        assert_eq!(
            db.job_size_class(id).await.unwrap(),
            Some(0),
            "legacy table must gain size_class DEFAULT 0"
        );
    }

    #[tokio::test]
    async fn ack_failure_reports_error() {
        for (engine, q, _dir) in queues().await {
            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            let claimed = q.claim("w").await.unwrap().unwrap();
            q.ack(claimed.id, "w", Err(BuildError::permanent("boom")))
                .await
                .unwrap();
            match q.job_status(enq.job_id.unwrap()).await.unwrap() {
                JobState::Failed(e) => assert_eq!(e, "boom", "{engine}"),
                other => panic!("{engine}: expected Failed, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn retryable_ack_requeues_and_later_attempt_succeeds() {
        for (engine, q, _dir) in queues().await {
            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            let id = enq.job_id.unwrap();
            let first = q.claim("w1").await.unwrap().unwrap();
            assert_eq!(first.id, id, "{engine}");

            assert!(
                q.ack(first.id, "w1", Err(BuildError::retryable("storage 503")))
                    .await
                    .unwrap(),
                "{engine}: retryable ack should requeue the owned claim"
            );
            assert!(matches!(q.job_status(id).await.unwrap(), JobState::Pending));

            let second = q.claim("w2").await.unwrap().unwrap();
            assert_eq!(second.id, id, "{engine}");
            assert!(q.ack(second.id, "w2", Ok(())).await.unwrap(), "{engine}");
            assert!(matches!(q.job_status(id).await.unwrap(), JobState::Done));
        }
    }

    /// Transient requeue (error with retryable bit) must NOT escalate size_class
    /// — only crash/OOM stale-reclaim does. A storage 5xx is not fixed by a
    /// bigger box; bumping on every retry would starve small workers.
    #[tokio::test]
    async fn retryable_ack_does_not_bump_size_class() {
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("q.db").to_string_lossy().to_string();
            let db = make_db(engine, &path).await;
            db.init().await.unwrap();
            let q = SqlJobQueue {
                db,
                stale_claim_secs: DEFAULT_STALE_CLAIM_SECS,
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
            };
            let reader = make_db(engine, &path).await;

            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            let id = enq.job_id.unwrap();
            let claimed = q.claim("w1").await.unwrap().unwrap();
            assert!(
                q.ack(claimed.id, "w1", Err(BuildError::retryable("storage 503")))
                    .await
                    .unwrap()
            );
            assert_eq!(
                reader.job_size_class(id).await.unwrap(),
                Some(0),
                "{engine}: retryable error requeue must leave size_class at 0"
            );
            // Still claimable (requeued), not terminal.
            assert!(q.claim("w2").await.unwrap().is_some(), "{engine}");
        }
    }

    #[tokio::test]
    async fn permanent_ack_is_terminal_with_no_retry() {
        for (engine, q, _dir) in queues().await {
            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            let id = enq.job_id.unwrap();
            let claimed = q.claim("w").await.unwrap().unwrap();

            assert!(
                q.ack(claimed.id, "w", Err(BuildError::permanent("bad repo")))
                    .await
                    .unwrap(),
                "{engine}: permanent ack should terminally fail"
            );
            match q.job_status(id).await.unwrap() {
                JobState::Failed(e) => assert_eq!(e, "bad repo", "{engine}"),
                other => panic!("{engine}: expected Failed, got {other:?}"),
            }
            assert!(
                q.claim("w2").await.unwrap().is_none(),
                "{engine}: permanent failure must not be retried"
            );
        }
    }

    #[tokio::test]
    async fn retryable_ack_dead_letters_at_attempt_cap() {
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("q.db").to_string_lossy().to_string();
            let db = make_db(engine, &path).await;
            db.init().await.unwrap();
            let q = SqlJobQueue {
                db,
                stale_claim_secs: DEFAULT_STALE_CLAIM_SECS,
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: 1,
            };
            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            let id = enq.job_id.unwrap();
            let claimed = q.claim("w").await.unwrap().unwrap();

            assert!(
                q.ack(claimed.id, "w", Err(BuildError::retryable("storage 503")))
                    .await
                    .unwrap(),
                "{engine}: over-cap retryable ack should dead-letter"
            );
            match q.job_status(id).await.unwrap() {
                JobState::Failed(e) => assert!(
                    e.contains("dead-lettered"),
                    "{engine}: expected dead-letter error, got {e:?}"
                ),
                other => panic!("{engine}: expected Failed, got {other:?}"),
            }
            assert!(
                q.claim("w2").await.unwrap().is_none(),
                "{engine}: dead-lettered retryable failure must not loop"
            );
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

    /// A push that arrives while the prior build for the same key is already
    /// claimed (and has already fetched) must get its own queued job, so the
    /// newer commit is built next — not coalesced onto the in-flight build and
    /// dropped. A second push while that fresh job is still queued does coalesce.
    #[tokio::test]
    async fn enqueues_fresh_job_when_prior_is_claimed() {
        for (engine, q, _dir) in queues().await {
            q.enqueue(job("o", "r", "main")).await.unwrap();
            let _claimed = q.claim("w").await.unwrap().unwrap();
            assert_eq!(
                q.enqueue(job("o", "r", "main")).await.unwrap().outcome,
                EnqueueOutcome::Enqueued,
                "{engine}: a push during an in-flight build gets its own queued job"
            );
            assert_eq!(q.depth().await, 1, "{engine}: one fresh queued job");
            // A further push while that job is queued coalesces onto it.
            assert_eq!(
                q.enqueue(job("o", "r", "main")).await.unwrap().outcome,
                EnqueueOutcome::Coalesced,
                "{engine}: further pushes collapse into the queued job"
            );
            assert_eq!(q.depth().await, 1, "{engine}: still one queued job");
        }
    }

    #[tokio::test]
    async fn coalesces_to_fresh_job_after_completion() {
        for (engine, q, _dir) in queues().await {
            let first = q.enqueue(job("o", "r", "main")).await.unwrap();
            let claimed = q.claim("w").await.unwrap().unwrap();
            q.ack(claimed.id, "w", Ok(())).await.unwrap();
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
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
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
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
            };
            q.enqueue(job("o", "r", "main")).await.unwrap();
            let _first = q.claim("w1").await.unwrap().unwrap();
            assert!(
                q.claim("w2").await.unwrap().is_none(),
                "{engine}: a fresh claim must not be reclaimed within the window"
            );
        }
    }

    /// A2: after a time-based reclaim re-owns a job, the original (slow but
    /// alive) worker's late ack must be rejected — not double-settle the row.
    #[tokio::test]
    async fn late_ack_from_reclaimed_worker_is_rejected() {
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("q.db").to_string_lossy().to_string();
            let db = make_db(engine, &path).await;
            db.init().await.unwrap();
            // Zero tolerance: the first claim is immediately reclaimable.
            let q = SqlJobQueue {
                db,
                stale_claim_secs: 0,
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
            };
            q.enqueue(job("o", "r", "main")).await.unwrap();
            let slow = q.claim("w1").await.unwrap().unwrap();
            // w2 reclaims the stale claim and now owns the row.
            let owner = q.claim("w2").await.unwrap().unwrap();
            assert_eq!(slow.id, owner.id, "{engine}");

            // The slow worker finally finishes and acks — must be rejected.
            assert!(
                !q.ack(slow.id, "w1", Ok(())).await.unwrap(),
                "{engine}: a reclaimed worker's late ack must not settle the job"
            );
            assert!(
                matches!(q.job_status(slow.id).await.unwrap(), JobState::Pending),
                "{engine}: the job is still owned by the new worker, not done"
            );

            // The current owner's ack settles it.
            assert!(
                q.ack(owner.id, "w2", Ok(())).await.unwrap(),
                "{engine}: the owning worker settles the job"
            );
            assert!(matches!(
                q.job_status(owner.id).await.unwrap(),
                JobState::Done
            ));
        }
    }

    /// A1: a build that is hard-killed (SIGKILL/OOM) never acks, so its claim
    /// goes stale and is reclaimed; after `max_build_attempts` it must reach a
    /// terminal `failed` (dead-letter) instead of crash-looping forever.
    #[tokio::test]
    async fn hard_killed_build_dead_letters_after_max_attempts() {
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("q.db").to_string_lossy().to_string();
            let db = make_db(engine, &path).await;
            db.init().await.unwrap();
            let max = 3;
            // Zero tolerance so each claim's predecessor is immediately stale.
            let q = SqlJobQueue {
                db,
                stale_claim_secs: 0,
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: max,
            };
            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            let id = enq.job_id.unwrap();

            // Each claim simulates a worker that gets SIGKILLed mid-build: it
            // never acks. The next claim reclaims the stale row.
            for attempt in 1..=max {
                let c = q.claim("w").await.unwrap();
                assert!(
                    c.is_some(),
                    "{engine}: attempt {attempt} should still be retryable"
                );
                assert!(matches!(q.job_status(id).await.unwrap(), JobState::Pending));
            }

            // The next claim finds the row over the attempt cap: it dead-letters
            // it to `failed` and there is nothing left to hand out.
            assert!(
                q.claim("w").await.unwrap().is_none(),
                "{engine}: an over-cap job is dead-lettered, not re-handed-out"
            );
            match q.job_status(id).await.unwrap() {
                JobState::Failed(e) => assert!(
                    e.contains("dead-lettered"),
                    "{engine}: dead-letter error, got {e:?}"
                ),
                other => panic!("{engine}: expected Failed (dead-letter), got {other:?}"),
            }
        }
    }

    /// P1: a crash/OOM (no ack) is reclaimed by `reclaim_stale`, and each
    /// under-cap stale-reclaim bumps `size_class` one rung so a larger worker
    /// can take the job next (claim filter lands in O2).
    #[tokio::test]
    async fn reclaim_stale_bumps_size_class() {
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("q.db").to_string_lossy().to_string();
            let db = make_db(engine, &path).await;
            db.init().await.unwrap();
            // Zero tolerance: a claim is immediately reclaimable.
            let q = SqlJobQueue {
                db,
                stale_claim_secs: 0,
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
            };
            // Second adapter on the same file for size_class reads.
            let reader = make_db(engine, &path).await;

            let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
            let id = enq.job_id.unwrap();
            assert_eq!(
                reader.job_size_class(id).await.unwrap(),
                Some(0),
                "{engine}: fresh job starts at size_class 0"
            );

            // Claim, then abandon (no ack). Next claim reclaims and bumps.
            let _first = q.claim("w1").await.unwrap().unwrap();
            let second = q.claim("w2").await.unwrap().unwrap();
            assert_eq!(second.id, id, "{engine}");
            assert_eq!(
                reader.job_size_class(id).await.unwrap(),
                Some(1),
                "{engine}: first stale-reclaim bumps size_class to 1"
            );

            // Second abandon → another bump.
            let third = q.claim("w3").await.unwrap().unwrap();
            assert_eq!(third.id, id, "{engine}");
            assert_eq!(
                reader.job_size_class(id).await.unwrap(),
                Some(2),
                "{engine}: second stale-reclaim bumps size_class to 2"
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
            max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
        };

        let failed = q.enqueue(job("o", "r", "fail")).await.unwrap();
        let c = q.claim("w").await.unwrap().unwrap();
        q.ack(c.id, "w", Err(BuildError::permanent("boom")))
            .await
            .unwrap();

        let done = q.enqueue(job("o", "r", "ok")).await.unwrap();
        let c = q.claim("w").await.unwrap().unwrap();
        q.ack(c.id, "w", Ok(())).await.unwrap();

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
            hs.push(tokio::spawn(async move {
                q.enqueue(job("o", "r", "main")).await
            }));
        }
        for h in hs {
            h.await
                .unwrap()
                .expect("enqueue must not error under contention");
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
                    assert!(
                        seen.lock().await.insert(c.id),
                        "job {} double-claimed",
                        c.id
                    );
                }
            }));
        }
        for h in hs {
            h.await.unwrap();
        }
        // 20 distinct branches + the 1 coalesced "main".
        assert_eq!(
            seen.lock().await.len(),
            21,
            "every job claimed exactly once"
        );
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
        // Per-job credential: round-trips through this engine's INSERT + SELECT
        // decode, and the ack runs the finish UPDATE that clears it (the cleared
        // *value* is asserted on sqlite in finish_clears_the_stored_credential).
        {
            use secrecy::ExposeSecret;
            let mut j = job("o", "r", "cred");
            j.credential = Some(secrecy::SecretString::new("dG9rZW4=".to_string().into()));
            q.enqueue(j).await.unwrap();
            let c = q.claim("wc").await.unwrap().unwrap();
            assert_eq!(
                c.credential.as_ref().map(|s| s.expose_secret().to_string()),
                Some("dG9rZW4=".to_string()),
                "credential round-trips through the queue DB"
            );
            q.ack(c.id, "wc", Ok(())).await.unwrap();
        }

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
        q.ack(first.id, "w1", Ok(())).await.unwrap();
        assert!(matches!(
            q.job_status(first.id).await.unwrap(),
            JobState::Done
        ));

        let second = q.claim("w1").await.unwrap().unwrap();
        assert_eq!(second.branch, "dev");
        q.ack(second.id, "w1", Err(BuildError::permanent("boom")))
            .await
            .unwrap();
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
        let pool = sqlx::postgres::PgPool::connect(&url)
            .await
            .expect("connect pg");
        sqlx::query("DROP TABLE IF EXISTS jobs")
            .execute(&pool)
            .await
            .expect("drop jobs");
        pool.close().await;
        let q = SqlJobQueue::new(Box::new(
            crate::queue::postgres_db::PostgresDb::connect(&url)
                .await
                .unwrap(),
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
        let pool = sqlx::mysql::MySqlPool::connect(&url)
            .await
            .expect("connect mysql");
        sqlx::query("DROP TABLE IF EXISTS jobs")
            .execute(&pool)
            .await
            .expect("drop jobs");
        pool.close().await;
        let q = SqlJobQueue::new(Box::new(
            crate::queue::mysql_db::MysqlDb::connect(&url)
                .await
                .unwrap(),
        ))
        .await
        .unwrap();
        exercise_core(&q).await;
    }
}
