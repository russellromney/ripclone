//! [`QueueDb`] backed by MySQL via `sqlx`. A network database for multi-machine
//! farm-out; also works against any MySQL-wire-compatible server.
//!
//! Dialect notes vs sqlite: `key` is a reserved word so it is backticked; ids are
//! `AUTO_INCREMENT` read via `last_insert_id()`; indexed text columns are
//! `VARCHAR`; the status index is declared inline (MySQL has no
//! `CREATE INDEX IF NOT EXISTS`); and MySQL has **no partial indexes**, so the
//! coalescing backstop is omitted — coalescing is best-effort only (a rare
//! duplicate is wasted compute, not a wrong result). Orchestration is reused.

use super::sql::QueueDb;
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

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
                INDEX idx_jobs_status_created (status, created_at),
                INDEX idx_jobs_provider_path_finished (provider, path, finished_at)
            )",
        )
        .execute(&self.pool)
        .await
        .context("create jobs table")?;
        Ok(())
    }

    async fn active_job_id(&self, key: &str) -> Result<Option<i64>> {
        sqlx::query_scalar(
            "SELECT id FROM jobs WHERE `key` = ? AND status IN ('queued', 'claimed') LIMIT 1",
        )
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
        created_at: i64,
    ) -> Result<i64> {
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

    async fn reclaim_stale(&self, cutoff: i64) -> Result<()> {
        sqlx::query(
            "UPDATE jobs SET status = 'queued', worker_id = NULL
             WHERE status = 'claimed' AND claimed_at <= ?",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .context("reclaim stale jobs")?;
        Ok(())
    }

    async fn next_queued_id(&self) -> Result<Option<i64>> {
        sqlx::query_scalar(
            "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at, id LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .context("select next queued")
    }

    async fn try_claim(&self, id: i64, worker_id: &str, now: i64) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE jobs SET status = 'claimed', worker_id = ?, claimed_at = ?
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
        status: &str,
        finished_at: i64,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query("UPDATE jobs SET status = ?, finished_at = ?, error = ? WHERE id = ?")
            .bind(status)
            .bind(finished_at)
            .bind(error)
            .bind(id)
            .execute(&self.pool)
            .await
            .context("finish job")?;
        Ok(())
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
