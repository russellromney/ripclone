//! Durable queue storage over the server's control database handle.

use super::sql::{
    CREATE_ACTIVE_KEY_INDEX_SQL, CREATE_HISTORY_INDEX_SQL, CREATE_STATUS_INDEX_SQL,
    CREATE_TABLE_SQL, SUPERSEDED_BY_NEWER_QUEUED, now_secs,
};
use anyhow::{Context, Result};
use libsql::{Connection, Database};
use std::sync::Arc;
use std::time::Duration;

pub struct LibsqlDb {
    db: Arc<Database>,
}

impl LibsqlDb {
    pub(crate) fn from_database(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub async fn connect(path: &str) -> Result<Self> {
        let db = libsql::Builder::new_local(path)
            .build()
            .await
            .context("open local queue database")?;
        Ok(Self { db: Arc::new(db) })
    }

    async fn conn(&self) -> Result<Connection> {
        let conn = self.db.connect().context("libsql connect")?;
        // Wait out lock contention rather than erroring (local files).
        conn.busy_timeout(Duration::from_secs(5))
            .context("configure queue busy timeout")?;
        Ok(conn)
    }

    /// Create the `jobs` table and indexes. Active-key
    /// uniqueness is required; initialization fails closed if the backend cannot
    /// enforce it.
    pub(crate) async fn init(&self) -> Result<()> {
        let conn = self.conn().await?;
        // WAL keeps readers from blocking the writer.
        let _ = conn.execute("PRAGMA journal_mode = WAL", ()).await;
        conn.execute(CREATE_TABLE_SQL, ())
            .await
            .context("create jobs table")?;
        conn.execute(CREATE_STATUS_INDEX_SQL, ())
            .await
            .context("create status index")?;
        conn.execute(CREATE_ACTIVE_KEY_INDEX_SQL, ())
            .await
            .context("create active-key uniqueness index")?;
        conn.execute(CREATE_HISTORY_INDEX_SQL, ())
            .await
            .context("create history index")?;
        Ok(())
    }

    /// Id of the active (queued or claimed) job for `key`, if any.
    pub(crate) async fn active_job_id(&self, key: &str) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT id FROM jobs WHERE key = ? AND status IN ('queued', 'claimed') LIMIT 1",
                [key],
            )
            .await
            .context("query active job")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    /// Newest retained job id for `key`, including settled jobs.
    pub(crate) async fn latest_job_id(&self, key: &str) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT id FROM jobs WHERE key = ? ORDER BY id DESC LIMIT 1",
                [key],
            )
            .await
            .context("query latest exact job")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    /// Insert a new queued job and return its id. Errors if a unique constraint
    /// rejects a duplicate active key (the caller treats that as coalesced).
    pub(crate) async fn insert_job(
        &self,
        key: &str,
        provider: &str,
        path: &str,
        admitted_commit: &str,
        repo_config: &str,
        credential: Option<&str>,
        created_at: i64,
    ) -> Result<i64> {
        let conn = self.conn().await?;
        let cred_val = match credential {
            Some(s) => libsql::Value::Text(s.to_string()),
            None => libsql::Value::Null,
        };
        conn.execute(
            "INSERT INTO jobs (key, provider, path, admitted_commit, repo_config, status, credential, created_at)
             VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)",
            libsql::params![
                key,
                provider,
                path,
                admitted_commit,
                repo_config,
                cred_val,
                created_at
            ],
        )
        .await
        .context("insert job")?;
        Ok(conn.last_insert_rowid())
    }

    /// Resolve `claimed` jobs whose `claimed_at <= cutoff` (a crashed or
    /// timed-out worker). A job that has already been claimed `max_attempts` or
    /// more times is dead-lettered to terminal `failed` (with `dead_letter_error`
    /// and `now` as its finished time) so a hard-killed build can't crash-loop;
    /// anything under the cap is returned to `queued` for another attempt.
    pub(crate) async fn reclaim_stale(
        &self,
        cutoff: i64,
        max_attempts: i64,
        now: i64,
        dead_letter_error: &str,
    ) -> Result<()> {
        let conn = self.conn().await?;
        // Dead-letter stale claims over the attempt cap.
        conn.execute(
            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?,
                 worker_id = NULL, credential = NULL
             WHERE status = 'claimed' AND claimed_at <= ? AND attempts >= ?",
            libsql::params![now, dead_letter_error, cutoff, max_attempts],
        )
        .await
        .context("dead-letter stale jobs")?;
        // Fail closed if external corruption creates a same-key sibling state.
        conn.execute(
            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?,
                 worker_id = NULL, credential = NULL
             WHERE id IN (
                 SELECT id FROM (
                     SELECT j1.id AS id FROM jobs j1
                     WHERE j1.status = 'claimed' AND j1.claimed_at <= ? AND j1.attempts < ?
                       AND EXISTS (
                           SELECT 1 FROM jobs j2
                           WHERE j2.key = j1.key AND j2.status = 'queued' AND j2.id != j1.id
                       )
                 )
             )",
            libsql::params![now, SUPERSEDED_BY_NEWER_QUEUED, cutoff, max_attempts],
        )
        .await
        .context("supersede stale jobs with a newer queued sibling")?;
        // Under-cap with no sibling: requeue.
        conn.execute(
            "UPDATE jobs SET status = 'queued', worker_id = NULL
             WHERE status = 'claimed' AND claimed_at <= ? AND attempts < ?",
            libsql::params![cutoff, max_attempts],
        )
        .await
        .context("reclaim stale jobs")?;
        Ok(())
    }

    /// Id of the oldest queued job.
    pub(crate) async fn next_queued_id(&self) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at, id LIMIT 1",
                (),
            )
            .await
            .context("select next queued")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    /// Atomically claim `id` if it is still `queued`, incrementing its
    /// `attempts` counter. Returns true iff this call won the row.
    pub(crate) async fn try_claim(&self, id: i64, worker_id: &str, now: i64) -> Result<bool> {
        let conn = self.conn().await?;
        let n = conn
            .execute(
                "UPDATE jobs SET status = 'claimed', worker_id = ?, claimed_at = ?,
                     attempts = attempts + 1
                 WHERE id = ? AND status = 'queued'",
                libsql::params![worker_id, now, id],
            )
            .await
            .context("claim job")?;
        Ok(n == 1)
    }

    /// Renew a live claim only while `worker_id` still owns it. Returns true
    /// iff the claimed row was refreshed.
    pub(crate) async fn renew_claim(&self, id: i64, worker_id: &str, now: i64) -> Result<bool> {
        let conn = self.conn().await?;
        let n = conn
            .execute(
                "UPDATE jobs SET claimed_at = ?
                 WHERE id = ? AND status = 'claimed' AND worker_id = ?",
                libsql::params![now, id, worker_id],
            )
            .await
            .context("renew job claim")?;
        Ok(n == 1)
    }

    /// `(provider, path, admitted_commit, repo_config, credential)` for a job
    /// id. `credential` is the stored base64 blob (or `None`).
    pub(crate) async fn job_fields(
        &self,
        id: i64,
    ) -> Result<Option<(String, String, String, String, Option<String>)>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT provider, path, admitted_commit, repo_config, credential FROM jobs WHERE id = ?",
                [id],
            )
            .await
            .context("fetch job fields")?;
        match rows.next().await? {
            Some(row) => Ok(Some((
                row.get::<String>(0)?,
                row.get::<String>(1)?,
                row.get::<String>(2)?,
                row.get::<String>(3)?,
                row.get::<Option<String>>(4)?,
            ))),
            None => Ok(None),
        }
    }

    /// Settle a claimed job: `status` is `done` or `failed`, with optional
    /// error. Conditional on the caller still owning the claim: the UPDATE
    /// matches only `id = ? AND worker_id = ? AND status = 'claimed'`. Returns
    /// true iff a row was settled; false means the claim was reclaimed and
    /// re-owned (or dead-lettered) while this worker was building, so its result
    /// must be discarded. The row belongs to whoever holds the claim now.
    pub(crate) async fn finish(
        &self,
        id: i64,
        worker_id: &str,
        status: &str,
        finished_at: i64,
        error: Option<&str>,
    ) -> Result<bool> {
        let conn = self.conn().await?;
        // Conditional on still owning the claim (no double-settle after a
        // reclaim). Clearing the credential keeps a token out of done-job history.
        let n = conn
            .execute(
                "UPDATE jobs SET status = ?, finished_at = ?, error = ?, credential = NULL
                 WHERE id = ? AND worker_id = ? AND status = 'claimed'",
                libsql::params![status, finished_at, error, id, worker_id],
            )
            .await
            .context("finish job")?;
        Ok(n == 1)
    }

    /// Current attempt count for a claim owned by `worker_id`.
    pub(crate) async fn claimed_attempts(&self, id: i64, worker_id: &str) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT attempts FROM jobs WHERE id = ? AND worker_id = ? AND status = 'claimed'",
                libsql::params![id, worker_id],
            )
            .await
            .context("fetch claimed attempts")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    /// Requeue a retryable build failure while the caller still owns the claim.
    /// Returns false if the claim was reclaimed or otherwise settled first.
    ///
    /// If external corruption creates a same-key queued sibling, requeue may
    /// violate the active-key constraint; settle the redundant claim with
    /// [`SUPERSEDED_BY_NEWER_QUEUED`]. New exact B/C admissions use different
    /// keys and therefore retry independently.
    pub(crate) async fn requeue_claim(
        &self,
        id: i64,
        worker_id: &str,
        error: &str,
    ) -> Result<bool> {
        let conn = self.conn().await?;
        let n = conn
            .execute(
                "UPDATE jobs SET status = 'queued', worker_id = NULL, error = ?
                 WHERE id = ? AND worker_id = ? AND status = 'claimed'
                   AND NOT EXISTS (
                       SELECT 1 FROM jobs AS j2
                       WHERE j2.key = (SELECT key FROM jobs WHERE id = ?)
                         AND j2.status = 'queued' AND j2.id != ?
                   )",
                libsql::params![error, id, worker_id, id, id],
            )
            .await
            .context("requeue retryable job")?;
        if n == 1 {
            return Ok(true);
        }
        let n = conn
            .execute(
                "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?, credential = NULL
                 WHERE id = ? AND worker_id = ? AND status = 'claimed'",
                libsql::params![now_secs(), SUPERSEDED_BY_NEWER_QUEUED, id, worker_id],
            )
            .await
            .context("supersede claim blocked by newer queued job")?;
        Ok(n == 1)
    }

    /// `(status, error)` for a job id.
    pub(crate) async fn status(&self, id: i64) -> Result<Option<(String, Option<String>)>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT status, error FROM jobs WHERE id = ?", [id])
            .await
            .context("query job status")?;
        match rows.next().await? {
            Some(row) => Ok(Some((row.get::<String>(0)?, row.get::<Option<String>>(1)?))),
            None => Ok(None),
        }
    }

    /// Count of `queued` jobs.
    pub(crate) async fn count_queued(&self) -> Result<i64> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT count(*) FROM jobs WHERE status = 'queued'", ())
            .await
            .context("count queued")?;
        match rows.next().await? {
            Some(row) => Ok(row.get::<i64>(0)?),
            None => Ok(0),
        }
    }
}
