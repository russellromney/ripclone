//! [`QueueDb`] backed by MySQL via `sqlx`. A network database for multi-machine
//! farm-out; also works against any MySQL-wire-compatible server.
//!
//! Dialect notes vs sqlite: `key` is a reserved word so it is backticked; ids are
//! `AUTO_INCREMENT` read via `last_insert_id()`; indexed text columns are
//! `VARCHAR`; the status index is declared inline (MySQL has no
//! `CREATE INDEX IF NOT EXISTS`); and MySQL has **no partial indexes**, so the
//! coalescing backstop is omitted — coalescing is best-effort only (a rare
//! duplicate is wasted compute, not a wrong result). Orchestration is reused.

use super::sql::{QueueDb, SUPERSEDED_BY_NEWER_QUEUED, now_secs};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

/// Reject a value that wouldn't fit a VARCHAR column, so MySQL never silently
/// truncates a queue key (which would collide two distinct jobs onto one row).
fn check_len(field: &str, value: &str, max: usize) -> Result<()> {
    if value.len() > max {
        anyhow::bail!(
            "{field} is too long for MySQL ({} bytes, max {max}): {value:?}",
            value.len()
        );
    }
    Ok(())
}

pub struct MysqlDb {
    pool: MySqlPool,
}

impl MysqlDb {
    /// Connect to a MySQL server at `url` (`mysql://user:pass@host:port/db`).
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .with_context(|| format!("connect mysql {url}"))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl QueueDb for MysqlDb {
    async fn init(&self) -> Result<()> {
        // Index declared inline: MySQL lacks CREATE INDEX IF NOT EXISTS, and the
        // whole statement is a no-op when the table already exists.
        //
        // raw_sql (the unprepared simple-query protocol) is used for DDL: it
        // needs no parameters, and it avoids the prepared-statement path, which
        // some MySQL-wire servers don't route through their `IF NOT EXISTS`
        // handling.
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS jobs (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                `key` VARCHAR(512) NOT NULL,
                provider VARCHAR(255) NOT NULL,
                path VARCHAR(255) NOT NULL,
                branch VARCHAR(255) NOT NULL,
                status VARCHAR(32) NOT NULL,
                worker_id VARCHAR(255),
                created_at BIGINT NOT NULL,
                claimed_at BIGINT,
                finished_at BIGINT,
                error TEXT,
                credential TEXT,
                attempts BIGINT NOT NULL DEFAULT 0,
                size_class BIGINT NOT NULL DEFAULT 0,
                INDEX idx_jobs_status_created (status, created_at),
                INDEX idx_jobs_provider_path_finished (provider, path, finished_at)
            )",
        )
        .execute(&self.pool)
        .await
        .context("create jobs table")?;
        // Migrate a legacy table to add the credential column. MySQL 8 has no
        // ADD COLUMN IF NOT EXISTS, so this is best-effort: it errors with a
        // duplicate-column code on an up-to-date table, which we ignore.
        let _ = sqlx::raw_sql("ALTER TABLE jobs ADD COLUMN credential TEXT")
            .execute(&self.pool)
            .await;
        // Same best-effort migration for the attempts column (dead-letter bound).
        let _ = sqlx::raw_sql("ALTER TABLE jobs ADD COLUMN attempts BIGINT NOT NULL DEFAULT 0")
            .execute(&self.pool)
            .await;
        // Best-effort migration for size_class (stale-reclaim escalation rung).
        let _ = sqlx::raw_sql("ALTER TABLE jobs ADD COLUMN size_class BIGINT NOT NULL DEFAULT 0")
            .execute(&self.pool)
            .await;
        Ok(())
    }

    async fn active_job_id(&self, key: &str) -> Result<Option<i64>> {
        sqlx::query_scalar("SELECT id FROM jobs WHERE `key` = ? AND status = 'queued' LIMIT 1")
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
        _size_class: i64,
        created_at: i64,
    ) -> Result<i64> {
        // size_class is blessed-backend only (sqlite/libsql); mysql lags.
        // VARCHAR key columns: reject an over-long value instead of letting MySQL
        // silently truncate it (which would collide two jobs onto one key).
        check_len("key", key, 512)?;
        check_len("provider", provider, 255)?;
        check_len("path", path, 255)?;
        check_len("branch", branch, 255)?;
        let res = sqlx::query(
            "INSERT INTO jobs (`key`, provider, path, branch, status, credential, created_at)
             VALUES (?, ?, ?, ?, 'queued', ?, ?)",
        )
        .bind(key)
        .bind(provider)
        .bind(path)
        .bind(branch)
        .bind(credential)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .context("insert job")?;
        Ok(res.last_insert_id() as i64)
    }

    async fn raise_size_class(&self, _id: i64, _rank: i64) -> Result<()> {
        // size_class column lags on mysql.
        Ok(())
    }

    async fn reclaim_stale(
        &self,
        cutoff: i64,
        max_attempts: i64,
        now: i64,
        dead_letter_error: &str,
    ) -> Result<()> {
        // Dead-letter stale claims over the attempt cap.
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
        // Under-cap with a newer queued sibling → superseded. MySQL has no
        // partial unique index, but the semantic still holds: a newer queued
        // job will build the tip, so don't requeue the older claim.
        // Nested derived table: MySQL forbids updating a table while selecting
        // from it in the same statement without the double-wrap.
        sqlx::query(
            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?,
                 worker_id = NULL, credential = NULL
             WHERE id IN (
                 SELECT id FROM (
                     SELECT j1.id AS id FROM jobs j1
                     WHERE j1.status = 'claimed' AND j1.claimed_at <= ? AND j1.attempts < ?
                       AND EXISTS (
                           SELECT 1 FROM jobs j2
                           WHERE j2.`key` = j1.`key` AND j2.status = 'queued' AND j2.id != j1.id
                       )
                 ) AS superseded
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

    async fn next_queued_id(&self, _max_size_class: Option<i64>) -> Result<Option<i64>> {
        // Claim filter lags with the size_class column; claim everything.
        sqlx::query_scalar(
            "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at, id LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .context("select next queued")
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
        // Conditional on still owning the claim (no double-settle after a
        // reclaim). Clearing the credential keeps a token out of done-job history.
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
        let res = sqlx::query(
            "UPDATE jobs SET status = 'queued', worker_id = NULL, error = ?
             WHERE id = ? AND worker_id = ? AND status = 'claimed'
               AND NOT EXISTS (
                   SELECT 1 FROM (
                       SELECT j2.id FROM jobs j2
                       WHERE j2.`key` = (SELECT `key` FROM jobs WHERE id = ?)
                         AND j2.status = 'queued' AND j2.id != ?
                   ) AS siblings
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
        // CAST to SIGNED: MySQL COUNT(*) is BIGINT UNSIGNED, which sqlx decodes
        // as u64 and would not coerce into i64.
        sqlx::query_scalar("SELECT CAST(count(*) AS SIGNED) FROM jobs WHERE status = 'queued'")
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
