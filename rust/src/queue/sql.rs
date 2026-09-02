//! Durable jobs in the server-owned control database. The official libsql
//! driver serves plain local SQLite and the Turso embedded replica. The server
//! admits work; embedded workers claim locally and standalone workers use its API.
//!
//! `LibsqlDb` is a tiny per-engine adapter that returns plain Rust types (no
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
//! - **Coalescing** is keyed by repository and exact admitted commit. The
//!   enqueue transaction first finds an active exact key and the partial unique
//!   index covers both `queued` and `claimed` rows as a database backstop. A
//!   later exact commit is intentionally a distinct job; a duplicate is not.

use super::libsql_db::LibsqlDb;
#[cfg(test)]
use super::size_class::default_size_classes;
use super::size_class::{SizeClass, classify_rank, load_size_classes, rank_ceiling};
use super::{
    BuildError, BuildJob, EnqueueOutcome, Enqueued, JobId, JobQueue, JobState, WorkerQueue,
};
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

/// Default retention for `failed` jobs (seconds) before they are pruned. `done`
/// jobs are never pruned. Override with `RIPCLONE_QUEUE_FAILED_RETENTION_SECS`.
const DEFAULT_FAILED_RETENTION_SECS: i64 = 7 * 24 * 3600;

/// Soft age-out for worker heartbeats (seconds). A worker is "live" when its
/// last heartbeat is newer than `now - this`. Override with
/// `RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS`. Must be longer than the worker's
/// heartbeat interval so a healthy worker is never counted dead between beats.
pub const DEFAULT_HEARTBEAT_TIMEOUT_SECS: i64 = 60;

/// Whether the worker should write heartbeat rows into the queue registry.
///
/// `RIPCLONE_WORKER_HEARTBEAT`:
/// - unset / empty → disabled (self-host default)
/// - `queue` / `1` / `true` / `yes` / `on` → write to the connected queue DB
/// - anything else → hard error (fail loudly; do not silently ignore)
pub fn worker_heartbeat_enabled_from_env() -> Result<bool> {
    worker_heartbeat_enabled(std::env::var("RIPCLONE_WORKER_HEARTBEAT").ok())
}

/// Pure form of [`worker_heartbeat_enabled_from_env`] for tests.
pub fn worker_heartbeat_enabled(heartbeat_env: Option<String>) -> Result<bool> {
    let Some(raw) = heartbeat_env else {
        return Ok(false);
    };
    let s = raw.trim();
    if s.is_empty() {
        return Ok(false);
    }
    let lower = s.to_ascii_lowercase();
    if matches!(lower.as_str(), "queue" | "1" | "true" | "yes" | "on") {
        return Ok(true);
    }
    anyhow::bail!(
        "RIPCLONE_WORKER_HEARTBEAT={s:?}: expected 'queue' or truthy 1|true to \
         write the workers registry, or leave unset to disable"
    )
}

/// How often the worker refreshes its registry row (seconds).
/// Default = timeout/3 (at least 1s). Override with
/// `RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS`.
pub fn worker_heartbeat_interval_secs() -> u64 {
    worker_heartbeat_interval_secs_from(
        std::env::var("RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS").ok(),
        std::env::var("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS").ok(),
    )
}

/// Pure form of [`worker_heartbeat_interval_secs`] for tests.
pub fn worker_heartbeat_interval_secs_from(
    interval_env: Option<String>,
    timeout_env: Option<String>,
) -> u64 {
    if let Some(n) = interval_env
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u64| n >= 1)
    {
        return n;
    }
    let timeout = timeout_env
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u64| n >= 1)
        .unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT_SECS as u64);
    (timeout / 3).max(1)
}

/// Build a fleet-unique worker id. PID alone collides across machines (two
/// containers with the same pid would overwrite one registry row and under-
/// count the live fleet). Prefer `FLY_MACHINE_ID` / `HOSTNAME`, then pid +
/// a start-time nanos suffix.
pub fn make_worker_id() -> String {
    make_worker_id_parts(
        std::env::var("FLY_MACHINE_ID")
            .ok()
            .or_else(|| std::env::var("HOSTNAME").ok()),
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    )
}

/// Pure form of [`make_worker_id`] for tests.
pub fn make_worker_id_parts(host: Option<String>, pid: u32, nanos: u128) -> String {
    let host = host
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "local".into());
    // Sanitize host so it stays a single token in logs / SQL keys.
    let host: String = host
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("{host}-{pid}-{nanos}")
}

/// Fail loudly when heartbeat is enabled but the interval is not strictly
/// less than the soft age-out timeout (otherwise a healthy worker looks dead
/// between beats and the autoscaler over-spawns).
pub fn validate_heartbeat_timing(interval_secs: u64, timeout_secs: i64) -> Result<()> {
    if timeout_secs < 1 {
        anyhow::bail!("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS must be >= 1, got {timeout_secs}");
    }
    if interval_secs < 1 {
        anyhow::bail!("RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS must be >= 1, got {interval_secs}");
    }
    let timeout_secs = u64::try_from(timeout_secs).context("heartbeat timeout must be positive")?;
    if interval_secs >= timeout_secs {
        anyhow::bail!(
            "RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS ({interval_secs}) must be < \
             RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS ({timeout_secs}) so a healthy \
             worker is never counted dead between beats"
        );
    }
    Ok(())
}

/// Cross-process queue over a `LibsqlDb`.
pub struct SqlJobQueue {
    db: LibsqlDb,
    stale_claim_secs: i64,
    failed_retention_secs: i64,
    max_build_attempts: i64,
    /// Ordered size classes from config. Classification + claim filter use ranks.
    size_classes: Vec<SizeClass>,
    /// Inclusive rank ceiling for this process (`--max-size-class`). `None` =
    /// no ceiling, claim everything (single-worker self-host unchanged).
    max_size_class: Option<i64>,
    /// How long a heartbeat stays "live" before aging out of
    /// [`Self::live_worker_count`].
    heartbeat_timeout_secs: i64,
    /// Shared by every clone of the server queue. Empty claim attempts consult
    /// this coarse deadline before running the write-heavy stale sweep.
    next_stale_reclaim_at: AtomicI64,
    #[cfg(test)]
    stale_reclaim_sweeps: AtomicU64,
}

impl SqlJobQueue {
    /// Wrap an engine adapter and run schema setup. Size classes load from
    /// config / `RIPCLONE_SIZE_CLASSES` / launch defaults. No claim ceiling
    /// (worker calls [`with_max_size_class`] to set one).
    pub async fn new(db: LibsqlDb) -> Result<Self> {
        Self::new_with_classes(db, load_size_classes(&[])?).await
    }

    /// Like [`new`] but with an explicit size-class list (tests, custom wiring).
    pub async fn new_with_classes(db: LibsqlDb, size_classes: Vec<SizeClass>) -> Result<Self> {
        super::size_class::validate_size_classes(&size_classes)?;
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
        let heartbeat_timeout_secs = std::env::var("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT_SECS);
        Ok(Self {
            db,
            stale_claim_secs,
            failed_retention_secs,
            max_build_attempts,
            size_classes,
            max_size_class: None,
            heartbeat_timeout_secs,
            next_stale_reclaim_at: AtomicI64::new(i64::MIN),
            #[cfg(test)]
            stale_reclaim_sweeps: AtomicU64::new(0),
        })
    }

    /// Set this worker's claim ceiling by class name. `None` clears the ceiling
    /// (claim everything). Unknown names fail loudly.
    pub fn with_max_size_class(mut self, name: Option<&str>) -> Result<Self> {
        self.max_size_class = match name {
            None => None,
            Some(n) => Some(rank_ceiling(n, &self.size_classes)?),
        };
        Ok(self)
    }

    /// Override the live-heartbeat timeout (seconds). Used by tests and by
    /// callers that want a tighter age-out than the env default.
    pub fn with_heartbeat_timeout_secs(mut self, secs: i64) -> Self {
        self.heartbeat_timeout_secs = secs.max(1);
        self
    }

    /// Override the stale-claim window (seconds) used by
    /// [`reclaim_stale`](Self::reclaim_stale) / [`claim_capped`](Self::claim_capped).
    /// Used by tests that need a deterministic (short) window instead of the
    /// `RIPCLONE_QUEUE_STALE_SECS` env default.
    pub fn with_stale_claim_secs(mut self, secs: i64) -> Self {
        self.stale_claim_secs = secs.max(0);
        self
    }

    /// Configured size classes (ordered, smallest first).
    pub fn size_classes(&self) -> &[SizeClass] {
        &self.size_classes
    }

    /// Soft age-out window for [`live_worker_count`](Self::live_worker_count).
    pub fn heartbeat_timeout_secs(&self) -> i64 {
        self.heartbeat_timeout_secs
    }

    /// Durable claim lease window. Active heartbeat intervals must remain
    /// comfortably below this value so healthy work cannot be reclaimed.
    pub fn stale_claim_secs(&self) -> i64 {
        self.stale_claim_secs
    }

    /// The one durable jobs table always includes the worker registry.
    pub fn supports_worker_registry(&self) -> bool {
        true
    }

    /// Write/update this worker's registry row and, for active work, renew the
    /// durable claim. Active renewal fails if this worker no longer owns it.
    pub async fn heartbeat(&self, worker_id: &str, current_job: Option<i64>) -> Result<()> {
        self.heartbeat_at(worker_id, current_job, now_secs()).await
    }

    /// Heartbeat with an explicit timestamp (epoch secs). Production uses
    /// [`heartbeat`](Self::heartbeat); tests pass a frozen clock.
    pub async fn heartbeat_at(
        &self,
        worker_id: &str,
        current_job: Option<i64>,
        now: i64,
    ) -> Result<()> {
        if worker_id.is_empty() {
            anyhow::bail!("worker_id must not be empty");
        }
        if let Some(job_id) = current_job {
            anyhow::ensure!(
                self.db.renew_claim(job_id, worker_id, now).await?,
                "worker {worker_id} no longer owns claimed job {job_id}"
            );
        }
        self.db
            .upsert_heartbeat(worker_id, self.max_size_class, current_job, now)
            .await?;
        Ok(())
    }

    /// Remove a no-longer-active embedded worker from the durable registry.
    pub async fn remove_worker(&self, worker_id: &str) -> Result<()> {
        self.db.delete_worker(worker_id).await?;
        Ok(())
    }

    /// How many workers have a fresh heartbeat within the timeout. The
    /// durable registry. Also hard-prunes aged-out rows.
    pub async fn live_worker_count(&self) -> Result<usize> {
        self.live_worker_count_at(now_secs()).await
    }

    /// Live-worker count as of `now` (epoch secs). Soft age-out: only rows with
    /// `last_heartbeat >= now - timeout` count; older rows are deleted then
    /// excluded.
    pub async fn live_worker_count_at(&self, now: i64) -> Result<usize> {
        let cutoff = now.saturating_sub(self.heartbeat_timeout_secs);
        // Hard age-out so the table does not grow with dead workers forever.
        // Fail loudly on prune errors — a partial view under-counts the fleet.
        self.db.prune_stale_workers(cutoff).await.map_err(|e| {
            tracing::error!("prune stale workers: {e:#}");
            e
        })?;
        usize::try_from(self.db.count_live_workers(cutoff).await?)
            .context("database returned a negative live-worker count")
    }

    /// Live workers that can claim jobs of at least `min_rank`.
    ///
    /// Soft age-out + prune, same as [`live_worker_count`]. A worker counts when
    /// `max_size_class` is NULL (no ceiling) or `>= min_rank`.
    pub async fn live_worker_count_capable(&self, min_rank: i64) -> Result<usize> {
        self.live_worker_count_capable_at(min_rank, now_secs())
            .await
    }

    /// [`live_worker_count_capable`] with an explicit clock (tests).
    pub async fn live_worker_count_capable_at(&self, min_rank: i64, now: i64) -> Result<usize> {
        let cutoff = now.saturating_sub(self.heartbeat_timeout_secs);
        self.db.prune_stale_workers(cutoff).await.map_err(|e| {
            tracing::error!("prune stale workers: {e:#}");
            e
        })?;
        usize::try_from(self.db.count_live_workers_capable(cutoff, min_rank).await?)
            .context("database returned a negative capable-worker count")
    }

    /// Pending (`queued`) job counts by size-class rank.
    ///
    /// Returns `(rank, count)` for ranks with depth > 0, ordered by rank.
    /// Ranks from the DB are clamped into the configured class range.
    pub async fn pending_by_class(&self) -> Result<Vec<(i64, usize)>> {
        let last = i64::try_from(self.size_classes.len().saturating_sub(1))
            .context("too many configured size classes")?;
        let rows = self.db.count_queued_by_size_class().await?;
        let mut out = Vec::with_capacity(rows.len());
        for (rank, count) in rows {
            if count <= 0 {
                continue;
            }
            let rank = rank.clamp(0, last);
            out.push((
                rank,
                usize::try_from(count).context("queued job count exceeds usize")?,
            ));
        }
        // Merge rows that collapsed onto the same clamped rank.
        out.sort_by_key(|(r, _)| *r);
        let mut merged: Vec<(i64, usize)> = Vec::with_capacity(out.len());
        for (rank, count) in out {
            match merged.last_mut() {
                Some((r, c)) if *r == rank => *c = c.saturating_add(count),
                _ => merged.push((rank, count)),
            }
        }
        Ok(merged)
    }

    /// Prune `failed` jobs older than the configured retention. Idempotent and
    /// safe to call from any worker; `done` jobs are kept. Returns rows removed.
    pub async fn prune_failed(&self) -> Result<u64> {
        self.db
            .prune_failed(now_secs() - self.failed_retention_secs)
            .await
    }

    /// Claim the oldest queued job for this worker, reclaiming abandoned claims
    /// first. Respects `--max-size-class` when set: only jobs at or below the
    /// ceiling are claimed. Returns `None` when the queue is empty (or no
    /// eligible job under the ceiling / contention exhausted the retry budget —
    /// the caller polls again).
    pub async fn claim(&self, worker_id: &str) -> Result<Option<ClaimedJob>> {
        self.claim_capped(worker_id, self.max_size_class).await
    }

    /// Reclaim claims abandoned by dead/stuck workers, independent of a claim.
    ///
    /// [`claim_capped`](Self::claim_capped) already reclaims before claiming, so
    /// a queue with active claim traffic self-heals on its own. But a job stuck
    /// `claimed` on an otherwise-idle queue has no claimer to trigger that path.
    /// Explicit recovery can flip it back to `queued` before reading depth.
    ///
    /// Same reclaim semantics as `claim_capped` — same stale window, same
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

    /// Resolve a size-class *name* to a rank ceiling using this queue's
    /// configured classes. `None` clears the ceiling (claim anything). Unknown
    /// names fail loudly. Used by the API claim endpoint to apply a farm-out
    /// worker's `--max-size-class` server-side, since the server's queue holds
    /// no per-worker ceiling of its own.
    pub fn resolve_ceiling(&self, name: Option<&str>) -> Result<Option<i64>> {
        match name {
            None => Ok(None),
            Some(n) => Ok(Some(rank_ceiling(n, &self.size_classes)?)),
        }
    }

    /// Claim honoring an explicit rank `ceiling`, overriding this queue's
    /// configured `max_size_class`. The inherent [`claim`](Self::claim) passes
    /// the configured ceiling; the API claim endpoint passes the caller's.
    pub async fn claim_capped(
        &self,
        worker_id: &str,
        ceiling: Option<i64>,
    ) -> Result<Option<ClaimedJob>> {
        self.claim_capped_at(worker_id, ceiling, now_secs()).await
    }

    async fn claim_capped_at(
        &self,
        worker_id: &str,
        ceiling: Option<i64>,
        now: i64,
    ) -> Result<Option<ClaimedJob>> {
        self.maybe_reclaim_stale_at(now).await?;
        for attempt in 0..MAX_CLAIM_ATTEMPTS {
            let Some(id) = self.db.next_queued_id(ceiling).await? else {
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
        let size_class = classify_rank(job.size_bytes, &self.size_classes);
        // Best-effort coalesce: fold into an already-active job for this key.
        // Raise size_class if this enqueue needs a bigger box — otherwise a
        // large push coalescing onto a small queued job under-sizes the lane.
        if let Some(id) = self.db.active_job_id(&key).await? {
            self.db.raise_size_class(id, size_class).await?;
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
                size_class,
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
                // unique backstop; if an active job now exists, treat as coalesced
                // and still raise size_class for the bigger of the two.
                if let Some(id) = self.db.active_job_id(&key).await? {
                    self.db.raise_size_class(id, size_class).await?;
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

/// Direct SQL is the trusted single-box worker path: each method forwards to the
/// inherent implementation above. The farm-out path uses
/// [`ApiJobQueue`](crate::api_job_queue::ApiJobQueue) instead.
#[async_trait]
impl WorkerQueue for SqlJobQueue {
    async fn claim(&self, worker_id: &str) -> Result<Option<ClaimedJob>> {
        SqlJobQueue::claim(self, worker_id).await
    }

    async fn ack(
        &self,
        id: JobId,
        worker_id: &str,
        result: Result<(), BuildError>,
    ) -> Result<bool> {
        SqlJobQueue::ack(self, id, worker_id, result).await
    }

    async fn heartbeat(&self, worker_id: &str, current_job: Option<JobId>) -> Result<()> {
        SqlJobQueue::heartbeat(self, worker_id, current_job).await
    }

    async fn prune_failed(&self) -> Result<u64> {
        SqlJobQueue::prune_failed(self).await
    }

    // `job_status` comes from the `JobQueue` supertrait (the real impl above).

    fn supports_worker_registry(&self) -> bool {
        SqlJobQueue::supports_worker_registry(self)
    }

    fn heartbeat_timeout_secs(&self) -> i64 {
        SqlJobQueue::heartbeat_timeout_secs(self)
    }
}

/// Shared DDL for both engines (blessed: sqlite + libsql).
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
    attempts INTEGER NOT NULL DEFAULT 0,
    size_class INTEGER NOT NULL DEFAULT 0
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

/// Durable worker heartbeat/registry table. One row per worker records its id,
/// size ceiling, optional
/// current job, last heartbeat. Stale rows age out of
/// [`SqlJobQueue::live_worker_count`] after the configured timeout.
pub(crate) const CREATE_WORKERS_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    max_size_class INTEGER,
    current_job INTEGER,
    last_heartbeat INTEGER NOT NULL
)";

/// Index for live-count / prune by heartbeat freshness.
pub(crate) const CREATE_WORKERS_HEARTBEAT_INDEX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS idx_workers_last_heartbeat ON workers(last_heartbeat)";

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
            size_bytes: None,
        }
    }

    fn job_sized(owner: &str, repo: &str, branch: &str, size_bytes: u64) -> BuildJob {
        let mut j = job(owner, repo, branch);
        j.size_bytes = Some(size_bytes);
        j
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
                0,
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
                size_classes: default_size_classes(),
                max_size_class: None,
                heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
                next_stale_reclaim_at: AtomicI64::new(i64::MIN),
                stale_reclaim_sweeps: AtomicU64::new(0),
            };
            let reader = make_db(engine, &path).await;

            // A known-small size classifies deterministically to rank 0; unknown
            // size would classify to the largest class instead (O2), which is
            // beside the point of this test (the ack path must not touch
            // size_class either way).
            let enq = q.enqueue(job_sized("o", "r", "main", 1024)).await.unwrap();
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
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
                size_classes: default_size_classes(),
                max_size_class: None,
                heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
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
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: 1,
                size_classes: default_size_classes(),
                max_size_class: None,
                heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
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
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
                size_classes: default_size_classes(),
                max_size_class: None,
                heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
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
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
                size_classes: default_size_classes(),
                max_size_class: None,
                heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
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
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: DEFAULT_MAX_BUILD_ATTEMPTS,
                size_classes: default_size_classes(),
                max_size_class: None,
                heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
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
                failed_retention_secs: DEFAULT_FAILED_RETENTION_SECS,
                max_build_attempts: max,
                size_classes: default_size_classes(),
                max_size_class: None,
                heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
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
                size_classes: default_size_classes(),
                max_size_class: None,
                heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
                next_stale_reclaim_at: AtomicI64::new(i64::MIN),
                stale_reclaim_sweeps: AtomicU64::new(0),
            };
            // Second adapter on the same file for size_class reads.
            let reader = make_db(engine, &path).await;

            // A known-small size classifies deterministically to rank 0, so the
            // bumps below land on 1, then 2 (unknown size would start at the
            // largest class instead — O2's classify_rank — which is beside the
            // point of this test).
            let enq = q.enqueue(job_sized("o", "r", "main", 1024)).await.unwrap();
            let id = enq.job_id.unwrap();
            assert_eq!(
                reader.job_size_class(id).await.unwrap(),
                Some(0),
                "{engine}: fresh small job starts at size_class 0"
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
            size_classes: default_size_classes(),
            max_size_class: None,
            heartbeat_timeout_secs: DEFAULT_HEARTBEAT_TIMEOUT_SECS,
            next_stale_reclaim_at: AtomicI64::new(i64::MIN),
            stale_reclaim_sweeps: AtomicU64::new(0),
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

    /// Two-class launch config: small ≤ 100 bytes, large catch-all.
    fn two_classes() -> Vec<SizeClass> {
        vec![
            SizeClass {
                name: "small".into(),
                max_bytes: 100,
                machine: "s".into(),
            },
            SizeClass {
                name: "large".into(),
                max_bytes: u64::MAX,
                machine: "l".into(),
            },
        ]
    }

    /// Three-class config: small ≤ 100, medium ≤ 1000, large catch-all.
    fn three_classes() -> Vec<SizeClass> {
        vec![
            SizeClass {
                name: "small".into(),
                max_bytes: 100,
                machine: "s".into(),
            },
            SizeClass {
                name: "medium".into(),
                max_bytes: 1_000,
                machine: "m".into(),
            },
            SizeClass {
                name: "large".into(),
                max_bytes: u64::MAX,
                machine: "l".into(),
            },
        ]
    }

    #[tokio::test]
    async fn sqlite_exercises_durable_queue_lifecycle() {
        let mut queues = queues().await;
        let (_, queue, _dir) = queues.pop().expect("SQLite queue");
        exercise_core(&queue).await;
    }

    async fn queue_classes(
        classes: Vec<SizeClass>,
        max_size_class: Option<&str>,
    ) -> (SqlJobQueue, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let db = make_db("sqlite", &path).await;
        let q = SqlJobQueue::new_with_classes(db, classes)
            .await
            .unwrap()
            .with_max_size_class(max_size_class)
            .unwrap();
        (q, dir)
    }

    #[tokio::test]
    async fn two_class_config_classifies_and_filters() {
        let (small_q, _dir) = queue_classes(two_classes(), Some("small")).await;
        small_q
            .enqueue(job_sized("o", "small-repo", "main", 50))
            .await
            .unwrap();
        small_q
            .enqueue(job_sized("o", "large-repo", "main", 10_000))
            .await
            .unwrap();
        // Small worker claims only the small job.
        let claimed = small_q.claim("small-w").await.unwrap().unwrap();
        assert_eq!(claimed.path, "o/small-repo");
        assert!(
            small_q.claim("small-w").await.unwrap().is_none(),
            "small worker must not claim a large job"
        );
        assert_eq!(small_q.depth().await, 1, "large job still queued");
    }

    #[tokio::test]
    async fn three_class_config_classifies_and_filters() {
        let (med_q, _dir) = queue_classes(three_classes(), Some("medium")).await;
        med_q
            .enqueue(job_sized("o", "s", "main", 50))
            .await
            .unwrap();
        med_q
            .enqueue(job_sized("o", "m", "main", 500))
            .await
            .unwrap();
        med_q
            .enqueue(job_sized("o", "l", "main", 50_000))
            .await
            .unwrap();
        // Medium ceiling drains small + medium, never large.
        let a = med_q.claim("m-w").await.unwrap().unwrap();
        let b = med_q.claim("m-w").await.unwrap().unwrap();
        let mut paths: Vec<_> = [a.path, b.path].into_iter().collect();
        paths.sort();
        assert_eq!(paths, vec!["o/m".to_string(), "o/s".to_string()]);
        assert!(
            med_q.claim("m-w").await.unwrap().is_none(),
            "medium worker must not claim a large job"
        );
        assert_eq!(med_q.depth().await, 1);
    }

    #[tokio::test]
    async fn large_worker_drains_both_classes() {
        let (large_q, _dir) = queue_classes(two_classes(), Some("large")).await;
        large_q
            .enqueue(job_sized("o", "small-repo", "main", 50))
            .await
            .unwrap();
        large_q
            .enqueue(job_sized("o", "large-repo", "main", 10_000))
            .await
            .unwrap();
        let a = large_q.claim("large-w").await.unwrap().unwrap();
        let b = large_q.claim("large-w").await.unwrap().unwrap();
        let mut paths: Vec<_> = [a.path, b.path].into_iter().collect();
        paths.sort();
        assert_eq!(
            paths,
            vec!["o/large-repo".to_string(), "o/small-repo".to_string()]
        );
        assert!(large_q.claim("large-w").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn no_ceiling_drains_all() {
        // No --max-size-class: single-worker self-host claims everything.
        let (q, _dir) = queue_classes(two_classes(), None).await;
        q.enqueue(job_sized("o", "small-repo", "main", 50))
            .await
            .unwrap();
        q.enqueue(job_sized("o", "large-repo", "main", 10_000))
            .await
            .unwrap();
        assert!(q.claim("w").await.unwrap().is_some());
        assert!(q.claim("w").await.unwrap().is_some());
        assert!(q.claim("w").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn threshold_change_reclassifies_at_enqueue() {
        // Same byte size, different thresholds → different claim eligibility.
        let bytes = 500u64;
        let tight = three_classes(); // 500 → medium
        let (med_q, dir) = queue_classes(tight, Some("small")).await;
        med_q
            .enqueue(job_sized("o", "r", "main", bytes))
            .await
            .unwrap();
        assert!(
            med_q.claim("small-w").await.unwrap().is_none(),
            "500 bytes is medium under the tight config; small worker skips it"
        );
        drop(med_q);

        // Retune: raise small threshold so 500 fits small.
        let retuned = vec![
            SizeClass {
                name: "small".into(),
                max_bytes: 600,
                machine: "s".into(),
            },
            SizeClass {
                name: "medium".into(),
                max_bytes: 1_000,
                machine: "m".into(),
            },
            SizeClass {
                name: "large".into(),
                max_bytes: u64::MAX,
                machine: "l".into(),
            },
        ];
        let path = dir.path().join("q2.db").to_string_lossy().to_string();
        let db = make_db("sqlite", &path).await;
        let retuned_q = SqlJobQueue::new_with_classes(db, retuned)
            .await
            .unwrap()
            .with_max_size_class(Some("small"))
            .unwrap();
        retuned_q
            .enqueue(job_sized("o", "r", "main", bytes))
            .await
            .unwrap();
        let claimed = retuned_q.claim("small-w").await.unwrap().unwrap();
        assert_eq!(
            claimed.path, "o/r",
            "after threshold retune, 500 bytes is small and claimable"
        );
    }

    #[tokio::test]
    async fn unknown_max_size_class_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let db = make_db("sqlite", &path).await;
        let q = SqlJobQueue::new_with_classes(db, two_classes())
            .await
            .unwrap();
        let err = match q.with_max_size_class(Some("xlarge")) {
            Ok(_) => panic!("expected unknown size class to fail"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unknown size class"), "got: {err}");
    }

    #[tokio::test]
    async fn preflight_size_classifies_first_build_as_small() {
        // Plan: first build uses tiered-add preflight size (no prior clonepack).
        let (small_q, _dir) = queue_classes(two_classes(), Some("small")).await;
        // 50 bytes → small under the test 100-byte threshold.
        small_q
            .enqueue(job_sized("o", "tiny", "main", 50))
            .await
            .unwrap();
        let claimed = small_q.claim("s").await.unwrap().unwrap();
        assert_eq!(claimed.path, "o/tiny");
    }

    #[tokio::test]
    async fn unknown_size_first_build_is_large_only() {
        // Plan: no preflight / no prior → largest class (never under-size).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let small = SqlJobQueue::new_with_classes(make_db("sqlite", &path).await, two_classes())
            .await
            .unwrap()
            .with_max_size_class(Some("small"))
            .unwrap();
        small.enqueue(job("o", "unknown", "main")).await.unwrap();
        assert!(
            small.claim("s").await.unwrap().is_none(),
            "unknown-size first build must not land on a small worker"
        );
        drop(small);

        let large = SqlJobQueue::new_with_classes(make_db("sqlite", &path).await, two_classes())
            .await
            .unwrap()
            .with_max_size_class(Some("large"))
            .unwrap();
        assert_eq!(large.claim("l").await.unwrap().unwrap().path, "o/unknown");
    }

    #[test]
    fn config_driven_n_classes_not_hardcoded() {
        // Code must accept N classes from config — 2 and 3 both validate and classify.
        assert_eq!(two_classes().len(), 2);
        assert_eq!(three_classes().len(), 3);
        crate::queue::size_class::validate_size_classes(&two_classes()).unwrap();
        crate::queue::size_class::validate_size_classes(&three_classes()).unwrap();
        let defaults = default_size_classes();
        assert_eq!(defaults[0].name, "small");
        assert_eq!(defaults[1].name, "large");
        assert_eq!(defaults[0].max_bytes, 1 << 30);
    }

    #[tokio::test]
    async fn pending_by_class_groups_mixed_size_bytes() {
        // Per-class pending read: mixed
        // size_bytes → correct ranks, empty when nothing queued.
        let (q, _dir) = queue_classes(two_classes(), None).await;
        assert!(
            q.pending_by_class().await.unwrap().is_empty(),
            "empty queue → no pending classes"
        );

        // two_classes: small max_bytes=100, large = u64::MAX
        q.enqueue(job_sized("o", "s1", "main", 50)).await.unwrap();
        q.enqueue(job_sized("o", "s2", "main", 10)).await.unwrap();
        q.enqueue(job_sized("o", "big", "main", 10_000))
            .await
            .unwrap();
        // Unknown size → largest class (rank 1).
        q.enqueue(job("o", "unknown", "main")).await.unwrap();

        let pending = q.pending_by_class().await.unwrap();
        assert_eq!(
            pending,
            vec![(0, 2), (1, 2)],
            "two small (rank 0) + one large + one unknown→large (rank 1)"
        );
        assert_eq!(q.depth().await, 4, "total depth still sums all classes");
    }

    #[tokio::test]
    async fn coalesce_raises_size_class_so_small_worker_cannot_claim() {
        // Dangerous case: small job queued first, large enqueue coalesces onto
        // it. Without raise_size_class the row stays small and a small worker
        // claims a large build.
        let (small_q, dir) = queue_classes(two_classes(), Some("small")).await;
        small_q
            .enqueue(job_sized("o", "r", "main", 50))
            .await
            .unwrap();
        // Coalesce a large size onto the same key.
        let coalesced = small_q
            .enqueue(job_sized("o", "r", "main", 10_000))
            .await
            .unwrap();
        assert_eq!(coalesced.outcome, EnqueueOutcome::Coalesced);
        assert!(
            small_q.claim("s").await.unwrap().is_none(),
            "after coalesce raise, small worker must not claim the upgraded job"
        );
        drop(small_q);

        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let large = SqlJobQueue::new_with_classes(make_db("sqlite", &path).await, two_classes())
            .await
            .unwrap()
            .with_max_size_class(Some("large"))
            .unwrap();
        assert_eq!(
            large.claim("l").await.unwrap().unwrap().path,
            "o/r",
            "large worker drains the raised job"
        );
    }

    // --- Worker heartbeat / registry (D3) ---------------------------------

    async fn queue_with_timeout(timeout_secs: i64) -> (SqlJobQueue, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let q = SqlJobQueue::new(make_db("sqlite", &path).await)
            .await
            .unwrap()
            .with_heartbeat_timeout_secs(timeout_secs);
        (q, dir)
    }

    #[tokio::test]
    async fn heartbeat_insert_and_update_marks_worker_live() {
        let (q, _dir) = queue_with_timeout(60).await;
        assert_eq!(
            q.live_worker_count_at(1_000).await.unwrap(),
            0,
            "empty registry"
        );

        q.heartbeat_at("w1", None, 1_000).await.unwrap();
        assert_eq!(
            q.live_worker_count_at(1_000).await.unwrap(),
            1,
            "insert marks live"
        );

        // Update same idle worker — still one live row.
        q.heartbeat_at("w1", None, 1_010).await.unwrap();
        assert_eq!(
            q.live_worker_count_at(1_010).await.unwrap(),
            1,
            "update does not double-count"
        );
    }

    #[tokio::test]
    async fn empty_claimers_share_one_coarse_stale_sweep_deadline() {
        let (q, _dir) = queue_with_timeout(60).await;
        let q = q.with_stale_claim_secs(120);

        for slot in 0..8 {
            assert!(
                q.claim_capped_at(&format!("idle-{slot}"), None, 1_000)
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

        assert!(
            q.claim_capped_at("idle-later", None, 1_029)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(q.stale_reclaim_sweep_count(), 1);
        assert!(
            q.claim_capped_at("idle-next", None, 1_030)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            q.stale_reclaim_sweep_count(),
            2,
            "the next sweep runs only when the coarse deadline arrives"
        );
    }

    #[tokio::test]
    async fn heartbeat_renews_owned_claim_then_stopped_heartbeat_allows_recovery() {
        let (q, _dir) = queue_with_timeout(60).await;
        let q = q.with_stale_claim_secs(60);
        let id = q
            .enqueue(job("o", "lease", "main"))
            .await
            .unwrap()
            .job_id
            .unwrap();
        let claimed = q
            .claim_capped_at("full-owner", None, 1_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, id);

        // The original lease is logically ancient, but the active Full owner
        // renews it through the unchanged heartbeat call.
        q.heartbeat_at("full-owner", Some(id), 2_000).await.unwrap();
        q.reclaim_stale_at(2_059).await.unwrap();
        assert!(
            q.db.next_queued_id(None).await.unwrap().is_none(),
            "continuing heartbeat keeps the claim owned"
        );

        // Once heartbeats stop, the same lease crosses the configured bound
        // and becomes queued for a new owner.
        q.reclaim_stale_at(2_060).await.unwrap();
        assert_eq!(q.db.next_queued_id(None).await.unwrap(), Some(id));
        let recovered = q
            .claim_capped_at("replacement", None, 2_060)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.id, id);
    }

    #[tokio::test]
    async fn stale_heartbeat_ages_out_of_live_count() {
        // timeout = 60s: live if last_heartbeat >= now - 60
        let (q, _dir) = queue_with_timeout(60).await;
        q.heartbeat_at("w1", None, 1_000).await.unwrap();

        assert_eq!(
            q.live_worker_count_at(1_050).await.unwrap(),
            1,
            "within timeout still live"
        );
        assert_eq!(
            q.live_worker_count_at(1_061).await.unwrap(),
            0,
            "past timeout ages out (excluded + pruned)"
        );
        // A later fresh heartbeat can re-enter.
        q.heartbeat_at("w1", None, 1_100).await.unwrap();
        assert_eq!(q.live_worker_count_at(1_100).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn live_worker_count_capable_filters_by_max_size_class() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let uncapped = SqlJobQueue::new(make_db("sqlite", &path).await)
            .await
            .unwrap()
            .with_heartbeat_timeout_secs(60);
        let small = SqlJobQueue::new(make_db("sqlite", &path).await)
            .await
            .unwrap()
            .with_max_size_class(Some("small"))
            .unwrap()
            .with_heartbeat_timeout_secs(60);
        let large = SqlJobQueue::new(make_db("sqlite", &path).await)
            .await
            .unwrap()
            .with_max_size_class(Some("large"))
            .unwrap()
            .with_heartbeat_timeout_secs(60);

        uncapped.heartbeat_at("u", None, 1_000).await.unwrap();
        small.heartbeat_at("s", None, 1_000).await.unwrap();
        large.heartbeat_at("l", None, 1_000).await.unwrap();

        assert_eq!(
            uncapped.live_worker_count_at(1_000).await.unwrap(),
            3,
            "raw count is everyone"
        );
        // Rank 0 (small): every worker is capable (NULL / 0 / 1 all >= 0).
        assert_eq!(
            uncapped
                .live_worker_count_capable_at(0, 1_000)
                .await
                .unwrap(),
            3
        );
        // Rank 1 (large): small-only (max_size_class=0) excluded.
        assert_eq!(
            uncapped
                .live_worker_count_capable_at(1, 1_000)
                .await
                .unwrap(),
            2,
            "uncapped + large; not small-only"
        );
    }

    #[tokio::test]
    async fn live_worker_count_returns_n() {
        let (q, _dir) = queue_with_timeout(60).await;
        for i in 0..5 {
            q.heartbeat_at(&format!("w{i}"), None, 1_000).await.unwrap();
        }
        // One stale among them.
        q.heartbeat_at("stale", None, 900).await.unwrap();

        assert_eq!(
            q.live_worker_count_at(1_000).await.unwrap(),
            5,
            "N live workers → count N (stale excluded)"
        );
    }

    #[tokio::test]
    async fn concurrent_live_count_readers_agree() {
        // Two concurrent readers must see the same live fleet size so
        // they do not each over-spawn.
        let (q, _dir) = queue_with_timeout(60).await;
        let q = Arc::new(q);
        for i in 0..3 {
            q.heartbeat_at(&format!("w{i}"), None, 1_000).await.unwrap();
        }

        let q1 = q.clone();
        let q2 = q.clone();
        let (a, b) = tokio::join!(
            q1.live_worker_count_at(1_000),
            q2.live_worker_count_at(1_000)
        );
        assert_eq!(a.unwrap(), 3);
        assert_eq!(b.unwrap(), 3);
        // Repeat: still stable (no double-count from concurrent prune).
        let (c, d) = tokio::join!(q.live_worker_count_at(1_000), q.live_worker_count_at(1_000));
        assert_eq!(c.unwrap(), 3);
        assert_eq!(d.unwrap(), 3);
    }

    #[tokio::test]
    async fn claim_without_heartbeat_leaves_registry_empty() {
        // Worker with heartbeat disabled never writes the registry — default
        // path is claim/ack only, so live_worker_count stays 0.
        let (q, _dir) = queue_with_timeout(60).await;
        q.enqueue(job("o", "r", "main")).await.unwrap();
        let claimed = q.claim("w1").await.unwrap().unwrap();
        assert!(q.ack(claimed.id, "w1", Ok(())).await.unwrap());
        assert_eq!(
            q.live_worker_count_at(now_secs()).await.unwrap(),
            0,
            "no heartbeat → empty registry (self-host unchanged)"
        );
    }

    #[tokio::test]
    async fn heartbeat_records_max_size_class_ceiling() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let q = SqlJobQueue::new_with_classes(make_db("sqlite", &path).await, two_classes())
            .await
            .unwrap()
            .with_max_size_class(Some("small"))
            .unwrap()
            .with_heartbeat_timeout_secs(60);
        q.heartbeat_at("small-w", None, 1_000).await.unwrap();

        let database = libsql::Builder::new_local(dir.path().join("q.db"))
            .build()
            .await
            .unwrap();
        let connection = database.connect().unwrap();
        let rank: Option<i64> = connection
            .query(
                "SELECT max_size_class FROM workers WHERE worker_id = ?",
                ["small-w"],
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(rank, Some(0), "small is rank 0 in two_classes()");
    }

    #[tokio::test]
    async fn init_creates_workers_registry_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        // SqlJobQueue::new runs init → workers table.
        let q = SqlJobQueue::new(make_db("sqlite", &path).await)
            .await
            .unwrap();
        drop(q);
        let database = libsql::Builder::new_local(&path).build().await.unwrap();
        let connection = database.connect().unwrap();
        let name: String = connection
            .query(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'workers'",
                (),
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .expect("workers table must exist after init")
            .get(0)
            .unwrap();
        assert_eq!(name, "workers");
    }

    #[tokio::test]
    async fn sqlite_supports_worker_registry() {
        let (q, _dir) = queue_with_timeout(60).await;
        assert!(
            q.supports_worker_registry(),
            "sqlite is a blessed backend for the workers registry"
        );
    }

    #[tokio::test]
    async fn stale_row_is_hard_deleted_not_only_soft_excluded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let q = SqlJobQueue::new(make_db("sqlite", &path).await)
            .await
            .unwrap()
            .with_heartbeat_timeout_secs(60);
        q.heartbeat_at("w1", None, 1_000).await.unwrap();

        // Age out: live-count prunes rows older than cutoff.
        assert_eq!(q.live_worker_count_at(1_100).await.unwrap(), 0);

        drop(q);
        let database = libsql::Builder::new_local(&path).build().await.unwrap();
        let connection = database.connect().unwrap();
        let n: i64 = connection
            .query("SELECT count(*) FROM workers", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(
            n, 0,
            "stale row must be hard-deleted, not left as a soft ghost"
        );
    }

    #[tokio::test]
    async fn heartbeat_persists_current_job_and_clears_on_idle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let q = SqlJobQueue::new(make_db("sqlite", &path).await)
            .await
            .unwrap()
            .with_heartbeat_timeout_secs(60);

        let id = q
            .enqueue(job("o", "heartbeat", "main"))
            .await
            .unwrap()
            .job_id
            .unwrap();
        q.claim_capped_at("w1", None, 990).await.unwrap().unwrap();
        q.heartbeat_at("w1", Some(id), 1_000).await.unwrap();
        let database = libsql::Builder::new_local(&path).build().await.unwrap();
        let connection = database.connect().unwrap();
        let job: Option<i64> = connection
            .query(
                "SELECT current_job FROM workers WHERE worker_id = ?",
                ["w1"],
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(job, Some(id));

        q.heartbeat_at("w1", None, 1_010).await.unwrap();
        let job: Option<i64> = connection
            .query(
                "SELECT current_job FROM workers WHERE worker_id = ?",
                ["w1"],
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        assert!(job.is_none(), "idle heartbeat clears current_job");
    }

    #[tokio::test]
    async fn empty_worker_id_fails_loudly() {
        let (q, _dir) = queue_with_timeout(60).await;
        let err = q.heartbeat_at("", None, 1_000).await.unwrap_err();
        assert!(
            err.to_string().contains("worker_id must not be empty"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn active_heartbeat_fails_after_claim_ownership_changes() {
        let (q, _dir) = queue_with_timeout(60).await;
        let q = q.with_stale_claim_secs(0);
        let id = q
            .enqueue(job("o", "lost", "main"))
            .await
            .unwrap()
            .job_id
            .unwrap();
        q.claim_capped_at("old", None, 1_000)
            .await
            .unwrap()
            .unwrap();
        q.reclaim_stale_at(1_001).await.unwrap();
        q.claim_capped_at("new", None, 1_001)
            .await
            .unwrap()
            .unwrap();

        let error = q.heartbeat_at("old", Some(id), 1_002).await.unwrap_err();
        assert!(error.to_string().contains("no longer owns claimed job"));
    }

    #[tokio::test]
    async fn remove_worker_deletes_registry_row() {
        let (q, _dir) = queue_with_timeout(60).await;
        q.heartbeat_at("embedded-slot", None, 1_000).await.unwrap();
        assert_eq!(q.live_worker_count_at(1_000).await.unwrap(), 1);
        q.remove_worker("embedded-slot").await.unwrap();
        assert_eq!(q.live_worker_count_at(1_000).await.unwrap(), 0);
    }

    #[test]
    fn heartbeat_env_default_disabled() {
        assert!(!worker_heartbeat_enabled(None).unwrap());
        assert!(!worker_heartbeat_enabled(Some("".into())).unwrap());
        assert!(!worker_heartbeat_enabled(Some("  ".into())).unwrap());
    }

    #[test]
    fn heartbeat_env_truthy_and_queue_enable() {
        assert!(worker_heartbeat_enabled(Some("queue".into())).unwrap());
        assert!(worker_heartbeat_enabled(Some("1".into())).unwrap());
        assert!(worker_heartbeat_enabled(Some("TRUE".into())).unwrap());
        assert!(worker_heartbeat_enabled(Some("yes".into())).unwrap());
    }

    #[test]
    fn heartbeat_env_unknown_target_fails_loudly() {
        let err = worker_heartbeat_enabled(Some("redis://elsewhere".into())).unwrap_err();
        assert!(
            err.to_string().contains("RIPCLONE_WORKER_HEARTBEAT"),
            "got: {err}"
        );
    }

    #[test]
    fn heartbeat_interval_defaults_to_third_of_timeout() {
        assert_eq!(
            worker_heartbeat_interval_secs_from(None, Some("90".into())),
            30
        );
        assert_eq!(
            worker_heartbeat_interval_secs_from(None, None),
            (DEFAULT_HEARTBEAT_TIMEOUT_SECS as u64) / 3
        );
        assert_eq!(
            worker_heartbeat_interval_secs_from(Some("7".into()), Some("90".into())),
            7
        );
    }

    #[test]
    fn heartbeat_timing_rejects_interval_ge_timeout() {
        validate_heartbeat_timing(10, 60).unwrap();
        let err = validate_heartbeat_timing(60, 60).unwrap_err();
        assert!(err.to_string().contains("must be <"), "got: {err}");
        let err = validate_heartbeat_timing(90, 60).unwrap_err();
        assert!(err.to_string().contains("must be <"), "got: {err}");
        let err = validate_heartbeat_timing(u64::MAX, 60).unwrap_err();
        assert!(err.to_string().contains("must be <"), "got: {err}");
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

    #[test]
    fn worker_id_is_unique_across_hosts_and_pids() {
        let a = make_worker_id_parts(Some("host-a".into()), 1, 100);
        let b = make_worker_id_parts(Some("host-b".into()), 1, 100);
        let c = make_worker_id_parts(Some("host-a".into()), 2, 100);
        let d = make_worker_id_parts(Some("host-a".into()), 1, 101);
        assert_ne!(a, b, "same pid on different hosts must not collide");
        assert_ne!(a, c, "different pids must not collide");
        assert_ne!(a, d, "same host+pid different start times must not collide");
        assert_eq!(
            make_worker_id_parts(None, 7, 1),
            "local-7-1",
            "missing host falls back to local"
        );
        assert_eq!(
            make_worker_id_parts(Some("fly/abc".into()), 1, 2),
            "fly-abc-1-2",
            "non-alphanumeric host chars sanitized"
        );
    }

    #[tokio::test]
    async fn two_distinct_worker_ids_both_count_as_live() {
        // Regression for the pid-collision failure mode: two workers with
        // different ids must never collapse to one live row.
        let (q, _dir) = queue_with_timeout(60).await;
        let w1 = make_worker_id_parts(Some("m1".into()), 42, 1);
        let w2 = make_worker_id_parts(Some("m2".into()), 42, 1); // same pid, different host
        q.heartbeat_at(&w1, None, 1_000).await.unwrap();
        q.heartbeat_at(&w2, None, 1_000).await.unwrap();
        assert_eq!(q.live_worker_count_at(1_000).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn concurrent_heartbeats_and_live_counts_stay_consistent() {
        // Stress concurrent readers and workers writing: many
        // concurrent upserts + concurrent live-count readers must agree.
        let (q, _dir) = queue_with_timeout(60).await;
        let q = Arc::new(q);
        let mut writers = Vec::new();
        for i in 0..8 {
            let q = q.clone();
            writers.push(tokio::spawn(async move {
                let id = format!("w{i}");
                for t in 0..5 {
                    q.heartbeat_at(&id, None, 1_000 + t).await.unwrap();
                }
            }));
        }
        for h in writers {
            h.await.unwrap();
        }
        let q1 = q.clone();
        let q2 = q.clone();
        let (a, b) = tokio::join!(
            q1.live_worker_count_at(1_004),
            q2.live_worker_count_at(1_004)
        );
        assert_eq!(a.unwrap(), 8);
        assert_eq!(b.unwrap(), 8);
    }
}
