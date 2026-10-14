//! Durable jobs in the server-owned SQLite control database. The server admits
//! work and its embedded workers claim it locally.
//!
//! `LibsqlDb` is a tiny adapter that returns plain Rust types (no driver types
//! leak); [`SqlJobQueue`] holds one and contains all the queue orchestration.
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
//! - **Coalescing** is keyed by repository and exact admitted commit. The
//!   enqueue transaction first finds an active exact key and the partial unique
//!   index covers both `queued` and `claimed` rows as a database backstop. A
//!   later exact commit is intentionally a distinct job; a duplicate is not.

use super::libsql_db::LibsqlDb;
use super::{BuildError, BuildJob, EnqueueOutcome, Enqueued, JobId, JobQueue, JobState};
use crate::provider::{ProviderInstanceId, RepoId};
use anyhow::{Context, Result};
use async_trait::async_trait;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
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
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
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
    /// Exact commit admitted by the server before enqueue.
    pub admitted_commit: String,
    /// Validated repository build settings snapshotted at admission.
    pub repo_config: crate::repo_config::RepoConfig,
    /// Per-job upstream credential the enqueuer passed (the cloud's per-request
    /// `X-Upstream-Token`), so the worker can read a private repo it has no
    /// standing credential for. `None` falls back to the server's broker.
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

/// Durable queue over a `LibsqlDb`.
pub struct SqlJobQueue {
    db: LibsqlDb,
    stale_claim_secs: i64,
    max_build_attempts: i64,
    /// Shared by every clone of the server queue. Empty claim attempts consult
    /// this coarse deadline before running the write-heavy stale sweep.
    next_stale_reclaim_at: AtomicI64,
    #[cfg(test)]
    stale_reclaim_sweeps: AtomicU64,
}

impl SqlJobQueue {
    /// Wrap an engine adapter and run schema setup.
    pub async fn new(db: LibsqlDb) -> Result<Self> {
        db.init().await?;
        let stale_claim_secs = std::env::var("RIPCLONE_QUEUE_STALE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_STALE_CLAIM_SECS);
        let max_build_attempts = std::env::var("RIPCLONE_QUEUE_MAX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(DEFAULT_MAX_BUILD_ATTEMPTS);
        Ok(Self {
            db,
            stale_claim_secs,
            max_build_attempts,
            next_stale_reclaim_at: AtomicI64::new(i64::MIN),
            #[cfg(test)]
            stale_reclaim_sweeps: AtomicU64::new(0),
        })
    }

    /// Override the stale-claim window (seconds) used by
    /// [`reclaim_stale`](Self::reclaim_stale) / [`claim`](Self::claim).
    /// Used by tests that need a deterministic (short) window instead of the
    /// `RIPCLONE_QUEUE_STALE_SECS` env default.
    pub fn with_stale_claim_secs(mut self, secs: i64) -> Self {
        self.stale_claim_secs = secs.max(0);
        self
    }

    /// How often a building worker renews its claim: a third of the lease
    /// window, capped at 20 seconds.
    pub fn renew_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            (self.stale_claim_secs.clamp(1, 60) / 3)
                .max(1)
                .unsigned_abs(),
        )
    }

    /// Renew a claimed job's lease. Fails if `worker_id` no longer owns it.
    pub async fn renew_claim(&self, id: JobId, worker_id: &str) -> Result<()> {
        self.renew_claim_at(id, worker_id, now_secs()).await
    }

    /// [`renew_claim`](Self::renew_claim) with an explicit clock (tests).
    pub async fn renew_claim_at(&self, id: JobId, worker_id: &str, now: i64) -> Result<()> {
        anyhow::ensure!(
            self.db.renew_claim(id, worker_id, now).await?,
            "worker {worker_id} no longer owns claimed job {id}"
        );
        Ok(())
    }

    /// Claim the oldest queued job for this worker, reclaiming abandoned claims
    /// first. Returns `None` when the queue is empty (or contention exhausted
    /// the retry budget — the caller polls again).
    pub async fn claim(&self, worker_id: &str) -> Result<Option<ClaimedJob>> {
        self.claim_at(worker_id, now_secs()).await
    }

    /// Reclaim claims abandoned by dead/stuck workers, independent of a claim.
    ///
    /// [`claim`](Self::claim) already reclaims before claiming, so
    /// a queue with active claim traffic self-heals on its own. But a job stuck
    /// `claimed` on an otherwise-idle queue has no claimer to trigger that path.
    /// Explicit recovery can flip it back to `queued` before reading depth.
    ///
    /// Same reclaim semantics as `claim` — same stale window, same
    /// max-attempts cap, same dead-letter behavior. Only *when* this runs
    /// changes, never *what* it does.
    pub async fn reclaim_stale(&self) -> Result<()> {
        self.reclaim_stale_at(now_secs()).await
    }

    async fn reclaim_stale_at(&self, now: i64) -> Result<()> {
        #[cfg(test)]
        self.stale_reclaim_sweeps
            .fetch_add(1, AtomicOrdering::Relaxed);
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
            .await
    }

    /// Run at most one stale sweep per shared coarse interval. A zero-second
    /// test window remains immediate; production intervals are capped at 30s.
    async fn maybe_reclaim_stale_at(&self, now: i64) -> Result<()> {
        let next = self.next_stale_reclaim_at.load(AtomicOrdering::Acquire);
        if now < next {
            return Ok(());
        }
        let interval = if self.stale_claim_secs == 0 {
            0
        } else {
            self.stale_claim_secs.clamp(1, 30)
        };
        if self
            .next_stale_reclaim_at
            .compare_exchange(
                next,
                now.saturating_add(interval),
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            )
            .is_err()
        {
            return Ok(());
        }
        if let Err(error) = self.reclaim_stale_at(now).await {
            self.next_stale_reclaim_at
                .store(now, AtomicOrdering::Release);
            return Err(error);
        }
        Ok(())
    }

    #[cfg(test)]
    fn stale_reclaim_sweep_count(&self) -> u64 {
        self.stale_reclaim_sweeps.load(AtomicOrdering::Relaxed)
    }

    async fn claim_at(&self, worker_id: &str, now: i64) -> Result<Option<ClaimedJob>> {
        self.maybe_reclaim_stale_at(now).await?;
        for attempt in 0..MAX_CLAIM_ATTEMPTS {
            let Some(id) = self.db.next_queued_id().await? else {
                return Ok(None);
            };
            if self.db.try_claim(id, worker_id, now).await? {
                let Some((provider, path, admitted_commit, repo_config, credential)) =
                    self.db.job_fields(id).await?
                else {
                    continue;
                };
                let repo_config: crate::repo_config::RepoConfig =
                    serde_json::from_str(&repo_config).context("decode admitted repo config")?;
                repo_config
                    .validate()
                    .context("validate admitted repo config")?;
                return Ok(Some(ClaimedJob {
                    id,
                    provider,
                    path,
                    admitted_commit,
                    repo_config,
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
    /// and must be discarded — see `LibsqlDb::finish`).
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
    retry_backoff_with_base(base_ms, attempts)
}

fn retry_backoff_with_base(base_ms: u64, attempts: i64) -> std::time::Duration {
    let shift = u32::try_from(attempts.saturating_sub(1).clamp(0, 5)).unwrap_or(0);
    std::time::Duration::from_millis(base_ms.saturating_mul(2_u64.saturating_pow(shift)))
}

#[async_trait]
impl JobQueue for SqlJobQueue {
    async fn enqueue(&self, job: BuildJob) -> Result<Enqueued> {
        crate::validation::validate_object_id(&job.admitted_commit)
            .context("validate admitted build commit")?;
        job.repo_config
            .validate()
            .context("validate admitted repo config")?;
        let repo_config =
            serde_json::to_string(&job.repo_config).context("encode admitted repo config")?;
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
                &job.admitted_commit,
                &repo_config,
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

    async fn job_state_for_key(&self, key: &str) -> Result<JobState> {
        match self.db.latest_job_id(key).await? {
            Some(id) => self.job_status(id).await,
            None => Ok(JobState::Unknown),
        }
    }

    async fn depth(&self) -> usize {
        self.db
            .count_queued()
            .await
            .ok()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0)
    }
}

/// Jobs table DDL.
pub(crate) const CREATE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    provider TEXT NOT NULL,
    path TEXT NOT NULL,
    status TEXT NOT NULL,
    worker_id TEXT,
    created_at INTEGER NOT NULL,
    claimed_at INTEGER,
    finished_at INTEGER,
    error TEXT,
    admitted_commit TEXT NOT NULL,
    repo_config TEXT NOT NULL,
    credential TEXT,
    attempts INTEGER NOT NULL DEFAULT 0
)";

pub(crate) const CREATE_STATUS_INDEX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at)";

/// Database-enforced coalescing backstop: at most one queued or claimed build
/// per immutable key. A later admitted commit has a different key and remains
/// a distinct job.
pub(crate) const CREATE_ACTIVE_KEY_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_identity_v3
     ON jobs(key) WHERE status IN ('queued', 'claimed')";

/// Index for the build/version history queries over retained `done` jobs
/// ("what was synced for this repo over time").
pub(crate) const CREATE_HISTORY_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS idx_jobs_provider_path_finished ON jobs(provider, path, finished_at)";

/// Fail-closed error for a corrupted queue state where a claimed row encounters
/// a queued sibling with the same key.
pub(crate) const SUPERSEDED_BY_NEWER_QUEUED: &str =
    "superseded by newer queued job for the same active key";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::libsql_db::LibsqlDb;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn job(owner: &str, repo: &str, branch: &str) -> BuildJob {
        job_at(
            owner,
            repo,
            branch,
            "1111111111111111111111111111111111111111",
        )
    }

    fn job_at(owner: &str, repo: &str, _checkout_name: &str, commit: &str) -> BuildJob {
        BuildJob {
            repo_id: RepoId::github(format!("{owner}/{repo}")),
            admitted_commit: commit.into(),
            repo_config: crate::repo_config::RepoConfig::default(),
            credential: None,
        }
    }

    /// Build a fresh queue backed by a temp file (a per-op connection model
    /// needs a real file, not `:memory:`).
    async fn queues() -> Vec<(&'static str, Arc<SqlJobQueue>, tempfile::TempDir)> {
        let mut out = Vec::new();
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let db = make_db(engine, &dir.path().join("q.db").to_string_lossy()).await;
            out.push((engine, Arc::new(SqlJobQueue::new(db).await.unwrap()), dir));
        }
        out
    }

    async fn make_db(engine: &str, path: &str) -> LibsqlDb {
        match engine {
            "sqlite" => LibsqlDb::connect(path).await.unwrap(),
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
                (claimed.provider.as_str(), claimed.path.as_str()),
                ("github", "o/r"),
                "{engine}"
            );
            assert_eq!(
                claimed.admitted_commit, "1111111111111111111111111111111111111111",
                "{engine}: exact admission must survive claim"
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
    async fn first_exact_job_credential_survives_duplicate_coalescing() {
        use secrecy::ExposeSecret;
        for (engine, q, _dir) in queues().await {
            let mut j = job("o", "r", "main");
            j.credential = Some(secrecy::SecretString::new(
                "first-credential".to_string().into(),
            ));
            let first = q.enqueue(j).await.unwrap();
            let mut before_claim_duplicate = job("o", "r", "main");
            before_claim_duplicate.credential = Some(secrecy::SecretString::new(
                "before-claim-decoy".to_string().into(),
            ));
            let duplicate = q.enqueue(before_claim_duplicate).await.unwrap();
            assert_eq!(duplicate.outcome, EnqueueOutcome::Coalesced, "{engine}");
            assert_eq!(duplicate.job_id, first.job_id, "{engine}");
            let claimed = q.claim("w1").await.unwrap().unwrap();
            let mut after_claim_duplicate = job("o", "r", "main");
            after_claim_duplicate.credential = Some(secrecy::SecretString::new(
                "after-claim-decoy".to_string().into(),
            ));
            let claimed_duplicate = q.enqueue(after_claim_duplicate).await.unwrap();
            assert_eq!(
                claimed_duplicate.outcome,
                EnqueueOutcome::Coalesced,
                "{engine}"
            );
            assert_eq!(claimed_duplicate.job_id, Some(claimed.id), "{engine}");
            let cred = claimed.credential.expect("first credential persisted");
            assert_eq!(cred.expose_secret(), "first-credential", "{engine}");
            eprintln!("{engine}: active_rows_for_exact_key=1 credential_owner=first-accepted");
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
        // done-job history. (Adapter-level: LibsqlDb directly.)
        let dir = tempfile::tempdir().unwrap();
        let db = LibsqlDb::connect(&dir.path().join("q.db").to_string_lossy())
            .await
            .unwrap();
        db.init().await.unwrap();
        let id = db
            .insert_job(
                "k",
                "github",
                "o/r",
                "1111111111111111111111111111111111111111",
                "{}",
                Some("dG9rZW4="),
                1,
            )
            .await
            .unwrap();
        let (_, _, _, _, before) = db.job_fields(id).await.unwrap().unwrap();
        assert_eq!(before.as_deref(), Some("dG9rZW4="));
        // finish is conditional on owning the claim: claim it as "w" first.
        assert!(db.try_claim(id, "w", 2).await.unwrap());
        assert!(db.finish(id, "w", "done", 3, None).await.unwrap());
        let (_, _, _, _, after) = db.job_fields(id).await.unwrap().unwrap();
        assert!(after.is_none(), "credential must be cleared on finish");
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

    /// A later immutable commit gets a distinct queued job. Retrying the older
    /// claim remains valid because its exact key is distinct from the newer one.
    #[tokio::test]
    async fn retryable_ack_requeues_older_commit_when_later_commit_is_queued() {
        for (engine, q, _dir) in queues().await {
            let first = q
                .enqueue(job_at(
                    "o",
                    "r",
                    "main",
                    "1111111111111111111111111111111111111111",
                ))
                .await
                .unwrap();
            let old_id = first.job_id.unwrap();
            let claimed = q.claim("w1").await.unwrap().unwrap();
            assert_eq!(claimed.id, old_id, "{engine}");

            // Push while build is in flight → a fresh queued job for commit C.
            let second = q
                .enqueue(job_at(
                    "o",
                    "r",
                    "main",
                    "2222222222222222222222222222222222222222",
                ))
                .await
                .unwrap();
            assert_eq!(second.outcome, EnqueueOutcome::Enqueued, "{engine}");
            let new_id = second.job_id.unwrap();
            assert_ne!(old_id, new_id, "{engine}");

            // The older immutable job can be retried independently.
            assert!(
                q.ack(claimed.id, "w1", Err(BuildError::retryable("storage 503")))
                    .await
                    .unwrap(),
                "{engine}: retryable ack should requeue the old exact job"
            );
            assert!(matches!(
                q.job_status(old_id).await.unwrap(),
                JobState::Pending
            ));
            assert!(matches!(
                q.job_status(new_id).await.unwrap(),
                JobState::Pending
            ));
        }
    }

    /// A hard-killed claim for commit B is reclaimed without disturbing a
    /// separately queued commit C for the same repo and branch.
    #[tokio::test]
    async fn stale_reclaim_preserves_later_commit_job() {
        for engine in ["sqlite"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("q.db").to_string_lossy().to_string();
            let db = make_db(engine, &path).await;
            db.init().await.unwrap();
            let q = SqlJobQueue {
                db,
                stale_claim_secs: 0,
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
                next_stale_reclaim_at: AtomicI64::new(i64::MIN),
                stale_reclaim_sweeps: AtomicU64::new(0),
            };

            let first = q
                .enqueue(job_at(
                    "o",
                    "r",
                    "main",
                    "1111111111111111111111111111111111111111",
                ))
                .await
                .unwrap();
            let old_id = first.job_id.unwrap();
            let _claimed = q.claim("w1").await.unwrap().unwrap();
            let second = q
                .enqueue(job_at(
                    "o",
                    "r",
                    "main",
                    "2222222222222222222222222222222222222222",
                ))
                .await
                .unwrap();
            let new_id = second.job_id.unwrap();

            // Next claim reclaims the stale older row and returns it to queued;
            // the newer exact job remains queued too.
            let next = q.claim("w2").await.unwrap().unwrap();
            assert_eq!(
                next.id, old_id,
                "{engine}: stale B should be requeued first"
            );
            assert!(matches!(
                q.job_status(new_id).await.unwrap(),
                JobState::Pending
            ));
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
                max_build_attempts: 1,
                next_stale_reclaim_at: AtomicI64::new(i64::MIN),
                stale_reclaim_sweeps: AtomicU64::new(0),
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
    async fn enqueue_coalesces_same_commit_across_source_names() {
        for (engine, q, _dir) in queues().await {
            let first = q.enqueue(job("o", "r", "main")).await.unwrap();
            assert_eq!(first.outcome, EnqueueOutcome::Enqueued, "{engine}");
            let second = q.enqueue(job("o", "r", "main")).await.unwrap();
            assert_eq!(second.outcome, EnqueueOutcome::Coalesced, "{engine}");
            assert_eq!(first.job_id, second.job_id, "{engine}");
            assert_eq!(
                q.enqueue(job("o", "r", "dev")).await.unwrap().outcome,
                EnqueueOutcome::Coalesced,
                "{engine}"
            );
            assert_eq!(q.depth().await, 1, "{engine}");
        }
    }

    /// A duplicate exact admission coalesces even while the first job is
    /// claimed. A later exact commit remains a distinct active job, and a
    /// duplicate admission of that later commit coalesces onto it.
    #[tokio::test]
    async fn coalesces_claimed_exact_job_but_admits_later_commit() {
        for (engine, q, _dir) in queues().await {
            let first = q
                .enqueue(job_at(
                    "o",
                    "r",
                    "main",
                    "1111111111111111111111111111111111111111",
                ))
                .await
                .unwrap();
            let _claimed = q.claim("w").await.unwrap().unwrap();
            assert_eq!(
                q.enqueue(job("o", "r", "main")).await.unwrap().outcome,
                EnqueueOutcome::Coalesced,
                "{engine}: duplicate exact admission coalesces onto claimed work"
            );
            assert_eq!(q.depth().await, 0, "{engine}: the exact B job is claimed");
            let later = q
                .enqueue(job_at(
                    "o",
                    "r",
                    "main",
                    "2222222222222222222222222222222222222222",
                ))
                .await
                .unwrap();
            assert_eq!(later.outcome, EnqueueOutcome::Enqueued, "{engine}");
            assert_ne!(first.job_id, later.job_id, "{engine}");
            assert_eq!(
                q.enqueue(job_at(
                    "o",
                    "r",
                    "main",
                    "2222222222222222222222222222222222222222",
                ))
                .await
                .unwrap()
                .outcome,
                EnqueueOutcome::Coalesced,
                "{engine}: duplicate C admission coalesces onto the queued job"
            );
            assert_eq!(q.depth().await, 1, "{engine}: still one queued job");
            eprintln!("{engine}: active_rows_B=1 active_rows_C=1 queued_rows=1 claimed_rows=1");
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
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
                next_stale_reclaim_at: AtomicI64::new(i64::MIN),
                stale_reclaim_sweeps: AtomicU64::new(0),
            };
            let mut admitted = job("o", "r", "main");
            admitted.repo_config.compression_level = Some(3);
            q.enqueue(admitted).await.unwrap();
            let first = q.claim("w1").await.unwrap().unwrap();
            let second = q.claim("w2").await.unwrap().unwrap();
            assert_eq!(first.id, second.id, "{engine}");
            assert_eq!(first.repo_config.compression_level, Some(3), "{engine}");
            assert_eq!(second.repo_config, first.repo_config, "{engine}");
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
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
                next_stale_reclaim_at: AtomicI64::new(i64::MIN),
                stale_reclaim_sweeps: AtomicU64::new(0),
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
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
                next_stale_reclaim_at: AtomicI64::new(i64::MIN),
                stale_reclaim_sweeps: AtomicU64::new(0),
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
                max_build_attempts: max,
                next_stale_reclaim_at: AtomicI64::new(i64::MIN),
                stale_reclaim_sweeps: AtomicU64::new(0),
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

    /// Concurrent enqueues for the same key coalesce; concurrent claims are
    /// exclusive. Run on SQLite (the mature local engine).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_coalesce_and_claim_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let db = LibsqlDb::connect(&dir.path().join("q.db").to_string_lossy())
            .await
            .unwrap();
        let q = Arc::new(SqlJobQueue::new(db).await.unwrap());

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

        // Enqueue distinct exact commits, drain with 4 workers — none double-claimed.
        for i in 0..20 {
            q.enqueue(job_at("o", "r", "main", &format!("{i:040x}")))
                .await
                .unwrap();
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
        // 20 distinct commits + the 1 coalesced initial commit.
        assert_eq!(
            seen.lock().await.len(),
            21,
            "every job claimed exactly once"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_sqlite_initialization_never_removes_active_uniqueness() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart-race.db");
        let path_text = path.to_string_lossy().to_string();
        let queue = Arc::new(
            SqlJobQueue::new(LibsqlDb::connect(&path_text).await.unwrap())
                .await
                .unwrap(),
        );
        let first = queue
            .enqueue(job_at(
                "o",
                "r",
                "restart-race",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ))
            .await
            .unwrap();
        assert_eq!(first.outcome, EnqueueOutcome::Enqueued);

        let mut tasks = Vec::new();
        for _ in 0..12 {
            let path = path_text.clone();
            tasks.push(tokio::spawn(async move {
                SqlJobQueue::new(LibsqlDb::connect(&path).await.unwrap())
                    .await
                    .unwrap();
            }));
            let queue = Arc::clone(&queue);
            tasks.push(tokio::spawn(async move {
                let outcome = queue
                    .enqueue(job_at(
                        "o",
                        "r",
                        "restart-race",
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    ))
                    .await
                    .unwrap()
                    .outcome;
                assert_eq!(outcome, EnqueueOutcome::Coalesced);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        drop(queue);
        let database = libsql::Builder::new_local(&path).build().await.unwrap();
        let connection = database.connect().unwrap();
        let active: i64 = connection
            .query(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'claimed')",
                (),
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(active, 1, "restart races preserve one active exact job");
        let index_count: i64 = connection
            .query(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_active_identity_v3'",
                (),
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(index_count, 1, "versioned active index remains installed");
    }

    /// Full queue lifecycle on a fresh queue: enqueue, coalesce, distinct key,
    /// claim ordering, ack done/failed, drain, and a fresh job after completion.
    async fn exercise_core(q: &SqlJobQueue) {
        // Per-job credential: round-trips through this engine's INSERT + SELECT
        // decode, and the ack runs the finish UPDATE that clears it (the cleared
        // *value* is asserted on sqlite in finish_clears_the_stored_credential).
        {
            use secrecy::ExposeSecret;
            let mut j = job("o", "r", "cred");
            j.credential = Some(secrecy::SecretString::new("first-token".to_string().into()));
            let first = q.enqueue(j).await.unwrap();
            let mut queued_duplicate = job("o", "r", "cred");
            queued_duplicate.credential = Some(secrecy::SecretString::new(
                "queued-decoy-token".to_string().into(),
            ));
            let duplicate = q.enqueue(queued_duplicate).await.unwrap();
            assert_eq!(duplicate.outcome, EnqueueOutcome::Coalesced);
            assert_eq!(duplicate.job_id, first.job_id);
            let c = q.claim("wc").await.unwrap().unwrap();
            let mut claimed_duplicate = job("o", "r", "cred");
            claimed_duplicate.credential = Some(secrecy::SecretString::new(
                "claimed-decoy-token".to_string().into(),
            ));
            let duplicate = q.enqueue(claimed_duplicate).await.unwrap();
            assert_eq!(duplicate.outcome, EnqueueOutcome::Coalesced);
            assert_eq!(duplicate.job_id, Some(c.id));
            assert_eq!(
                c.credential.as_ref().map(|s| s.expose_secret().to_string()),
                Some("first-token".to_string()),
                "the first accepted credential survives queued and claimed duplicates"
            );
            eprintln!("active_rows_for_exact_credential_key=1");
            q.ack(c.id, "wc", Ok(())).await.unwrap();
        }

        // Dialect-sensitive active uniqueness: B stays the sole active row
        // after claim, while C on the same repo/branch is a second immutable
        // key. Both exact identities must survive their claim transport.
        {
            let b_commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
            let c_commit = "cccccccccccccccccccccccccccccccccccccccc";
            let b = q
                .enqueue(job_at("o", "r", "immutable", b_commit))
                .await
                .unwrap();
            let claimed_b = q.claim("immutable-worker").await.unwrap().unwrap();
            assert_eq!(claimed_b.admitted_commit, b_commit);
            let duplicate_b = q
                .enqueue(job_at("o", "r", "immutable", b_commit))
                .await
                .unwrap();
            assert_eq!(duplicate_b.outcome, EnqueueOutcome::Coalesced);
            assert_eq!(duplicate_b.job_id, b.job_id);
            let c = q
                .enqueue(job_at("o", "r", "immutable", c_commit))
                .await
                .unwrap();
            assert_eq!(c.outcome, EnqueueOutcome::Enqueued);
            assert_ne!(c.job_id, b.job_id);
            assert_eq!(q.depth().await, 1, "only queued C contributes to depth");
            let duplicate_c = q
                .enqueue(job_at("o", "r", "immutable", c_commit))
                .await
                .unwrap();
            assert_eq!(duplicate_c.outcome, EnqueueOutcome::Coalesced);
            assert_eq!(duplicate_c.job_id, c.job_id);
            eprintln!("active_rows_B=1 active_rows_C=1 queued_rows=1 claimed_rows=1");
            q.ack(claimed_b.id, "immutable-worker", Ok(()))
                .await
                .unwrap();
            let claimed_c = q.claim("immutable-worker").await.unwrap().unwrap();
            assert_eq!(claimed_c.admitted_commit, c_commit);
            q.ack(claimed_c.id, "immutable-worker", Ok(()))
                .await
                .unwrap();
        }

        let enq = q.enqueue(job("o", "r", "main")).await.unwrap();
        assert_eq!(enq.outcome, EnqueueOutcome::Enqueued);
        let id = enq.job_id.unwrap();
        assert_eq!(q.depth().await, 1);
        assert!(matches!(q.job_status(id).await.unwrap(), JobState::Pending));

        let coalesced = q.enqueue(job("o", "r", "main")).await.unwrap();
        assert_eq!(coalesced.outcome, EnqueueOutcome::Coalesced);
        assert_eq!(coalesced.job_id, Some(id));

        let other = q
            .enqueue(job_at(
                "o",
                "r",
                "dev",
                "2222222222222222222222222222222222222222",
            ))
            .await
            .unwrap();
        assert_eq!(other.outcome, EnqueueOutcome::Enqueued);
        assert_eq!(q.depth().await, 2);

        let first = q.claim("w1").await.unwrap().unwrap();
        assert_eq!(
            first.admitted_commit, "1111111111111111111111111111111111111111",
            "oldest exact commit claimed first"
        );
        q.ack(first.id, "w1", Ok(())).await.unwrap();
        assert!(matches!(
            q.job_status(first.id).await.unwrap(),
            JobState::Done
        ));

        let second = q.claim("w1").await.unwrap().unwrap();
        assert_eq!(
            second.admitted_commit,
            "2222222222222222222222222222222222222222"
        );
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
    async fn sqlite_exercises_durable_queue_lifecycle() {
        let mut queues = queues().await;
        let (_, queue, _dir) = queues.pop().expect("SQLite queue");
        exercise_core(&queue).await;
    }

    async fn queue_with_stale_secs(stale_claim_secs: i64) -> (SqlJobQueue, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let q = SqlJobQueue::new(make_db("sqlite", &path).await)
            .await
            .unwrap()
            .with_stale_claim_secs(stale_claim_secs);
        (q, dir)
    }

    #[tokio::test]
    async fn empty_claimers_share_one_coarse_stale_sweep_deadline() {
        let (q, _dir) = queue_with_stale_secs(120).await;

        for slot in 0..8 {
            assert!(
                q.claim_at(&format!("idle-{slot}"), 1_000)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            q.stale_reclaim_sweep_count(),
            1,
            "all idle slots share the first stale sweep"
        );

        assert!(q.claim_at("idle-later", 1_029).await.unwrap().is_none());
        assert_eq!(q.stale_reclaim_sweep_count(), 1);
        assert!(q.claim_at("idle-next", 1_030).await.unwrap().is_none());
        assert_eq!(
            q.stale_reclaim_sweep_count(),
            2,
            "the next sweep runs only when the coarse deadline arrives"
        );
    }

    #[tokio::test]
    async fn renewal_keeps_owned_claim_then_stopped_renewal_allows_recovery() {
        let (q, _dir) = queue_with_stale_secs(60).await;
        let id = q
            .enqueue(job("o", "lease", "main"))
            .await
            .unwrap()
            .job_id
            .unwrap();
        let claimed = q.claim_at("full-owner", 1_000).await.unwrap().unwrap();
        assert_eq!(claimed.id, id);

        // The original lease is logically ancient, but the active Full owner
        // renews it.
        q.renew_claim_at(id, "full-owner", 2_000).await.unwrap();
        q.reclaim_stale_at(2_059).await.unwrap();
        assert!(
            q.db.next_queued_id().await.unwrap().is_none(),
            "continuing renewal keeps the claim owned"
        );

        // Once renewals stop, the same lease crosses the configured bound
        // and becomes queued for a new owner.
        q.reclaim_stale_at(2_060).await.unwrap();
        assert_eq!(q.db.next_queued_id().await.unwrap(), Some(id));
        let recovered = q.claim_at("replacement", 2_060).await.unwrap().unwrap();
        assert_eq!(recovered.id, id);
    }

    #[tokio::test]
    async fn renewal_fails_after_claim_ownership_changes() {
        let (q, _dir) = queue_with_stale_secs(0).await;
        let id = q
            .enqueue(job("o", "lost", "main"))
            .await
            .unwrap()
            .job_id
            .unwrap();
        q.claim_at("old", 1_000).await.unwrap().unwrap();
        q.reclaim_stale_at(1_001).await.unwrap();
        q.claim_at("new", 1_001).await.unwrap().unwrap();

        let error = q.renew_claim_at(id, "old", 1_002).await.unwrap_err();
        assert!(error.to_string().contains("no longer owns claimed job"));
    }

    #[test]
    fn retry_backoff_clamps_invalid_attempt_counts() {
        assert_eq!(
            retry_backoff(i64::MIN),
            std::time::Duration::from_millis(250)
        );
        assert_eq!(retry_backoff(0), std::time::Duration::from_millis(250));
        assert_eq!(retry_backoff(1), std::time::Duration::from_millis(250));
        assert_eq!(retry_backoff(6), std::time::Duration::from_millis(8_000));
        assert_eq!(
            retry_backoff(i64::MAX),
            std::time::Duration::from_millis(8_000)
        );
        assert_eq!(
            retry_backoff_with_base(u64::MAX, i64::MAX),
            std::time::Duration::from_millis(u64::MAX),
            "a configured base at the u64 boundary must saturate"
        );
    }
}
