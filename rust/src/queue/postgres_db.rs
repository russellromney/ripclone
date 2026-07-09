//! [`QueueDb`] backed by PostgreSQL via `sqlx`. A network database, so it serves
//! multi-machine farm-out (workers on many hosts share one queue). Also works
//! against any Postgres-wire-compatible server.
//!
//! Differs from the sqlite adapter only in dialect: `$N` placeholders,
//! `GENERATED … AS IDENTITY` ids fetched via `RETURNING`, and Postgres-native
//! partial unique index. The claim/coalesce orchestration in `SqlJobQueue` is
//! reused unchanged.

use super::sql::{QueueDb, SUPERSEDED_BY_NEWER_QUEUED, now_secs};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub struct PostgresDb {
    pool: PgPool,
}

impl PostgresDb {
    /// Connect to a Postgres server at `url` (`postgres://user:pass@host:port/db`).
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .with_context(|| format!("connect postgres {url}"))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl QueueDb for PostgresDb {
    async fn init(&self) -> Result<()> {
        // BIGSERIAL (not GENERATED … AS IDENTITY): valid Postgres and far more
        // widely supported by Postgres-wire compatibility layers, which don't all
        // parse the newer IDENTITY syntax. DDL runs via raw_sql (unprepared
        // simple-query protocol) — no params, and it avoids the prepared-statement
        // path.
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS jobs (
                id BIGSERIAL PRIMARY KEY,
                key TEXT NOT NULL,
                provider TEXT NOT NULL,
                path TEXT NOT NULL,
                branch TEXT NOT NULL,
                status TEXT NOT NULL,
                worker_id TEXT,
                created_at BIGINT NOT NULL,
                claimed_at BIGINT,
                finished_at BIGINT,
                error TEXT,
                credential TEXT,
                attempts BIGINT NOT NULL DEFAULT 0,
                size_class BIGINT NOT NULL DEFAULT 0
            )",
        )
        .execute(&self.pool)
        .await
        .context("create jobs table")?;
        // Migrate a legacy table (created before the credential column).
        sqlx::raw_sql("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS credential TEXT")
            .execute(&self.pool)
            .await
            .context("add credential column")?;
        // Migrate a legacy table for the attempts column (dead-letter bound).
        sqlx::raw_sql(
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS attempts BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await
        .context("add attempts column")?;
        // Stale-reclaim escalation rung (right-sizing / O2 claim filter).
        sqlx::raw_sql(
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS size_class BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await
        .context("add size_class column")?;
        sqlx::raw_sql(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at)",
        )
        .execute(&self.pool)
        .await
        .context("create status index")?;
        // Coalescing backstop: at most one *queued* job per key (a claimed build
        // can coexist with a queued one, so a push mid-build still gets queued).
        let _ = sqlx::raw_sql("DROP INDEX IF EXISTS idx_jobs_active_key")
            .execute(&self.pool)
            .await;
        if let Err(e) = sqlx::raw_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_queued_key
             ON jobs(key) WHERE status = 'queued'",
        )
        .execute(&self.pool)
        .await
        {
            tracing::warn!("postgres: active-key index unsupported ({e}); coalescing best-effort");
        }
        sqlx::raw_sql(
            "CREATE INDEX IF NOT EXISTS idx_jobs_provider_path_finished
             ON jobs(provider, path, finished_at)",
        )
        .execute(&self.pool)
        .await
        .context("create history index")?;
        Ok(())
    }

    async fn active_job_id(&self, key: &str) -> Result<Option<i64>> {
        sqlx::query_scalar("SELECT id FROM jobs WHERE key = $1 AND status = 'queued' LIMIT 1")
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
        sqlx::query_scalar(
            "INSERT INTO jobs (key, provider, path, branch, status, credential, created_at)
             VALUES ($1, $2, $3, $4, 'queued', $5, $6) RETURNING id",
        )
        .bind(key)
        .bind(provider)
        .bind(path)
        .bind(branch)
        .bind(credential)
        .bind(created_at)
        .fetch_one(&self.pool)
        .await
        .context("insert job")
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
            "UPDATE jobs SET status = 'failed', finished_at = $1, error = $2,
                 worker_id = NULL, credential = NULL
             WHERE status = 'claimed' AND claimed_at <= $3 AND attempts >= $4",
        )
        .bind(now)
        .bind(dead_letter_error)
        .bind(cutoff)
        .bind(max_attempts)
        .execute(&self.pool)
        .await
        .context("dead-letter stale jobs")?;
        // Under-cap with a newer queued sibling → superseded (unique key).
        sqlx::query(
            "UPDATE jobs SET status = 'failed', finished_at = $1, error = $2,
                 worker_id = NULL, credential = NULL
             WHERE status = 'claimed' AND claimed_at <= $3 AND attempts < $4
               AND EXISTS (
                   SELECT 1 FROM jobs j2
                   WHERE j2.key = jobs.key AND j2.status = 'queued' AND j2.id != jobs.id
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
             WHERE status = 'claimed' AND claimed_at <= $1 AND attempts < $2",
        )
        .bind(cutoff)
        .bind(max_attempts)
        .execute(&self.pool)
        .await
        .context("reclaim stale jobs")?;
        Ok(())
    }

    async fn job_size_class(&self, id: i64) -> Result<Option<i64>> {
        sqlx::query_scalar("SELECT size_class FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .context("fetch job size_class")
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
            "UPDATE jobs SET status = 'claimed', worker_id = $1, claimed_at = $2,
                 attempts = attempts + 1
             WHERE id = $3 AND status = 'queued'",
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
        let row = sqlx::query("SELECT provider, path, branch, credential FROM jobs WHERE id = $1")
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
            "UPDATE jobs SET status = $1, finished_at = $2, error = $3, credential = NULL
             WHERE id = $4 AND worker_id = $5 AND status = 'claimed'",
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
            "SELECT attempts FROM jobs WHERE id = $1 AND worker_id = $2 AND status = 'claimed'",
        )
        .bind(id)
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await
        .context("fetch claimed attempts")
    }

    async fn requeue_claim(&self, id: i64, worker_id: &str, error: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE jobs SET status = 'queued', worker_id = NULL, error = $1
             WHERE id = $2 AND worker_id = $3 AND status = 'claimed'
               AND NOT EXISTS (
                   SELECT 1 FROM jobs AS j2
                   WHERE j2.key = jobs.key AND j2.status = 'queued' AND j2.id != jobs.id
               )",
        )
        .bind(error)
        .bind(id)
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .context("requeue retryable job")?;
        if res.rows_affected() == 1 {
            return Ok(true);
        }
        let res = sqlx::query(
            "UPDATE jobs SET status = 'failed', finished_at = $1, error = $2, credential = NULL
             WHERE id = $3 AND worker_id = $4 AND status = 'claimed'",
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
        let row = sqlx::query("SELECT status, error FROM jobs WHERE id = $1")
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
        let res = sqlx::query("DELETE FROM jobs WHERE status = 'failed' AND finished_at < $1")
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .context("prune failed jobs")?;
        Ok(res.rows_affected())
    }
}
