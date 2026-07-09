//! [`QueueDb`] backed by plain SQLite via `sqlx` — the mature, default local
//! engine. Reliable multi-process access on one host (WAL + busy_timeout + the
//! atomic conditional claim). For multi-machine use the remote `libsql` backend.

use super::sql::{
    ADD_ATTEMPTS_COLUMN_SQL, ADD_CREDENTIAL_COLUMN_SQL, ADD_SIZE_CLASS_COLUMN_SQL,
    CREATE_ACTIVE_KEY_INDEX_SQL, CREATE_HISTORY_INDEX_SQL, CREATE_STATUS_INDEX_SQL,
    CREATE_TABLE_SQL, DROP_LEGACY_ACTIVE_KEY_INDEX_SQL, QueueDb, SUPERSEDED_BY_NEWER_QUEUED,
    now_secs,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;

pub struct SqliteDb {
    pool: SqlitePool,
}

impl SqliteDb {
    /// Open (creating if needed) a local SQLite database at `path`.
    pub async fn connect(path: &str) -> Result<Self> {
        // Accept either a bare path or a `sqlite:`/`file:` URL.
        let opts = SqliteConnectOptions::from_str(path)
            .with_context(|| format!("parse sqlite url {path}"))?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5))
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .with_context(|| format!("open sqlite db {path}"))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl QueueDb for SqliteDb {
    async fn init(&self) -> Result<()> {
        // DDL via raw_sql (unprepared simple-query protocol): no params needed,
        // and it avoids the prepared-statement path.
        sqlx::raw_sql(CREATE_TABLE_SQL)
            .execute(&self.pool)
            .await
            .context("create jobs table")?;
        // Migrate a legacy table to add the credential column (best-effort: errors
        // "duplicate column" on a fresh table, which is fine).
        let _ = sqlx::raw_sql(ADD_CREDENTIAL_COLUMN_SQL)
            .execute(&self.pool)
            .await;
        // Same best-effort migration for the attempts column (dead-letter bound).
        let _ = sqlx::raw_sql(ADD_ATTEMPTS_COLUMN_SQL)
            .execute(&self.pool)
            .await;
        // size_class rank: the claim filter (right-sizing) reads it, and
        // stale-reclaim bumps it as an escalation rung. Best-effort migration.
        let _ = sqlx::raw_sql(ADD_SIZE_CLASS_COLUMN_SQL)
            .execute(&self.pool)
            .await;
        sqlx::raw_sql(CREATE_STATUS_INDEX_SQL)
            .execute(&self.pool)
            .await
            .context("create status index")?;
        let _ = sqlx::raw_sql(DROP_LEGACY_ACTIVE_KEY_INDEX_SQL)
            .execute(&self.pool)
            .await;
        if let Err(e) = sqlx::raw_sql(CREATE_ACTIVE_KEY_INDEX_SQL)
            .execute(&self.pool)
            .await
        {
            tracing::warn!("sqlite: active-key index unsupported ({e}); coalescing best-effort");
        }
        sqlx::raw_sql(CREATE_HISTORY_INDEX_SQL)
            .execute(&self.pool)
            .await
            .context("create history index")?;
        Ok(())
    }

    async fn active_job_id(&self, key: &str) -> Result<Option<i64>> {
        sqlx::query_scalar("SELECT id FROM jobs WHERE key = ? AND status = 'queued' LIMIT 1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .context("query active job")
    }

    async fn insert_job(
        &self,
        key: &str,
        provider: &str,
        path: &str,
        branch: &str,
        credential: Option<&str>,
        size_class: i64,
        created_at: i64,
    ) -> Result<i64> {
        let res = sqlx::query(
            "INSERT INTO jobs (key, provider, path, branch, status, credential, size_class, created_at)
             VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)",
        )
        .bind(key)
        .bind(provider)
        .bind(path)
        .bind(branch)
        .bind(credential)
        .bind(size_class)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .context("insert job")?;
        Ok(res.last_insert_rowid())
    }

    async fn raise_size_class(&self, id: i64, rank: i64) -> Result<()> {
        sqlx::query(
            "UPDATE jobs SET size_class = MAX(size_class, ?)
             WHERE id = ? AND status = 'queued'",
        )
        .bind(rank)
        .bind(id)
        .execute(&self.pool)
        .await
        .context("raise size_class")?;
        Ok(())
    }

    async fn reclaim_stale(
        &self,
        cutoff: i64,
        max_attempts: i64,
        now: i64,
        dead_letter_error: &str,
    ) -> Result<()> {
        // Dead-letter first: any stale claim already at/over the attempt cap is
        // terminally failed so it can't crash-loop.
        sqlx::query(
            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?,
                 worker_id = NULL, credential = NULL
             WHERE status = 'claimed' AND claimed_at <= ? AND attempts >= ?",
        )
        .bind(now)
        .bind(dead_letter_error)
        .bind(cutoff)
        .bind(max_attempts)
        .execute(&self.pool)
        .await
        .context("dead-letter stale jobs")?;
        // Under-cap but a newer job is already queued for the same key: the
        // unique queued-key index forbids requeue. Terminalise as superseded
        // (the newer job builds the tip). Nested FROM avoids SQLite's
        // "table is locked" when updating from a same-table subquery.
        sqlx::query(
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
        )
        .bind(now)
        .bind(SUPERSEDED_BY_NEWER_QUEUED)
        .bind(cutoff)
        .bind(max_attempts)
        .execute(&self.pool)
        .await
        .context("supersede stale jobs with a newer queued sibling")?;
        // Under-cap with no sibling: requeue and bump size_class.
        sqlx::query(
            "UPDATE jobs SET status = 'queued', worker_id = NULL,
                 size_class = size_class + 1
             WHERE status = 'claimed' AND claimed_at <= ? AND attempts < ?",
        )
        .bind(cutoff)
        .bind(max_attempts)
        .execute(&self.pool)
        .await
        .context("reclaim stale jobs")?;
        Ok(())
    }

    async fn job_size_class(&self, id: i64) -> Result<Option<i64>> {
        sqlx::query_scalar("SELECT size_class FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .context("fetch job size_class")
    }

    async fn next_queued_id(&self, max_size_class: Option<i64>) -> Result<Option<i64>> {
        match max_size_class {
            None => sqlx::query_scalar(
                "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at, id LIMIT 1",
            )
            .fetch_optional(&self.pool)
            .await
            .context("select next queued"),
            Some(ceiling) => sqlx::query_scalar(
                "SELECT id FROM jobs WHERE status = 'queued' AND size_class <= ?
                 ORDER BY created_at, id LIMIT 1",
            )
            .bind(ceiling)
            .fetch_optional(&self.pool)
            .await
            .context("select next queued under size-class ceiling"),
        }
    }

    async fn try_claim(&self, id: i64, worker_id: &str, now: i64) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE jobs SET status = 'claimed', worker_id = ?, claimed_at = ?,
                 attempts = attempts + 1
             WHERE id = ? AND status = 'queued'",
        )
        .bind(worker_id)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await
        .context("claim job")?;
        Ok(res.rows_affected() == 1)
    }

    async fn job_fields(
        &self,
        id: i64,
    ) -> Result<Option<(String, String, String, Option<String>)>> {
        let row = sqlx::query("SELECT provider, path, branch, credential FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .context("fetch job fields")?;
        match row {
            Some(row) => Ok(Some((
                row.try_get(0)?,
                row.try_get(1)?,
                row.try_get(2)?,
                row.try_get(3)?,
            ))),
            None => Ok(None),
        }
    }

    async fn finish(
        &self,
        id: i64,
        worker_id: &str,
        status: &str,
        finished_at: i64,
        error: Option<&str>,
    ) -> Result<bool> {
        // Conditional on still owning the claim, so a slow worker whose claim was
        // reclaimed (or dead-lettered) can't double-settle the row. Clearing the
        // per-job credential keeps a short-lived token out of the done-job history.
        let res = sqlx::query(
            "UPDATE jobs SET status = ?, finished_at = ?, error = ?, credential = NULL
             WHERE id = ? AND worker_id = ? AND status = 'claimed'",
        )
        .bind(status)
        .bind(finished_at)
        .bind(error)
        .bind(id)
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .context("finish job")?;
        Ok(res.rows_affected() == 1)
    }

    async fn claimed_attempts(&self, id: i64, worker_id: &str) -> Result<Option<i64>> {
        sqlx::query_scalar(
            "SELECT attempts FROM jobs WHERE id = ? AND worker_id = ? AND status = 'claimed'",
        )
        .bind(id)
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await
        .context("fetch claimed attempts")
    }

    async fn requeue_claim(&self, id: i64, worker_id: &str, error: &str) -> Result<bool> {
        // Requeue only when no sibling is already queued for this key (the
        // unique idx_jobs_queued_key would reject a second queued row).
        let res = sqlx::query(
            "UPDATE jobs SET status = 'queued', worker_id = NULL, error = ?
             WHERE id = ? AND worker_id = ? AND status = 'claimed'
               AND NOT EXISTS (
                   SELECT 1 FROM jobs AS j2
                   WHERE j2.key = (SELECT key FROM jobs WHERE id = ?)
                     AND j2.status = 'queued' AND j2.id != ?
               )",
        )
        .bind(error)
        .bind(id)
        .bind(worker_id)
        .bind(id)
        .bind(id)
        .execute(&self.pool)
        .await
        .context("requeue retryable job")?;
        if res.rows_affected() == 1 {
            return Ok(true);
        }
        // Still owning the claim ⇒ a newer queued job blocked requeue. Settle
        // terminal so the row is not stuck claimed forever.
        let res = sqlx::query(
            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?, credential = NULL
             WHERE id = ? AND worker_id = ? AND status = 'claimed'",
        )
        .bind(now_secs())
        .bind(SUPERSEDED_BY_NEWER_QUEUED)
        .bind(id)
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .context("supersede claim blocked by newer queued job")?;
        Ok(res.rows_affected() == 1)
    }

    async fn status(&self, id: i64) -> Result<Option<(String, Option<String>)>> {
        let row = sqlx::query("SELECT status, error FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .context("query job status")?;
        match row {
            Some(row) => Ok(Some((row.try_get(0)?, row.try_get(1)?))),
            None => Ok(None),
        }
    }

    async fn count_queued(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status = 'queued'")
            .fetch_one(&self.pool)
            .await
            .context("count queued")
    }

    async fn prune_failed(&self, cutoff: i64) -> Result<u64> {
        let res = sqlx::query("DELETE FROM jobs WHERE status = 'failed' AND finished_at < ?")
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .context("prune failed jobs")?;
        Ok(res.rows_affected())
    }
}
