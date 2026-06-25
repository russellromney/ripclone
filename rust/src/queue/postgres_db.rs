//! [`QueueDb`] backed by PostgreSQL via `sqlx`. A network database, so it serves
//! multi-machine farm-out (workers on many hosts share one queue). Also works
//! against any Postgres-wire-compatible server.
//!
//! Differs from the sqlite adapter only in dialect: `$N` placeholders,
//! `GENERATED … AS IDENTITY` ids fetched via `RETURNING`, and Postgres-native
//! partial unique index. The claim/coalesce orchestration in `SqlJobQueue` is
//! reused unchanged.

use super::sql::QueueDb;
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
                error TEXT
            )",
        )
        .execute(&self.pool)
        .await
        .context("create jobs table")?;
        sqlx::raw_sql(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at)",
        )
        .execute(&self.pool)
        .await
        .context("create status index")?;
        // Coalescing backstop: at most one active job per key.
        if let Err(e) = sqlx::raw_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_key
             ON jobs(key) WHERE status IN ('queued', 'claimed')",
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
        sqlx::query_scalar(
            "SELECT id FROM jobs WHERE key = $1 AND status IN ('queued', 'claimed') LIMIT 1",
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
        created_at: i64,
    ) -> Result<i64> {
        sqlx::query_scalar(
            "INSERT INTO jobs (key, provider, path, branch, status, created_at)
             VALUES ($1, $2, $3, $4, 'queued', $5) RETURNING id",
        )
        .bind(key)
        .bind(provider)
        .bind(path)
        .bind(branch)
        .bind(created_at)
        .fetch_one(&self.pool)
        .await
        .context("insert job")
    }

    async fn reclaim_stale(&self, cutoff: i64) -> Result<()> {
        sqlx::query(
            "UPDATE jobs SET status = 'queued', worker_id = NULL
             WHERE status = 'claimed' AND claimed_at <= $1",
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
            "UPDATE jobs SET status = 'claimed', worker_id = $1, claimed_at = $2
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

    async fn job_fields(&self, id: i64) -> Result<Option<(String, String, String)>> {
        let row = sqlx::query("SELECT provider, path, branch FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .context("fetch job fields")?;
        match row {
            Some(row) => Ok(Some((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?))),
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
        sqlx::query("UPDATE jobs SET status = $1, finished_at = $2, error = $3 WHERE id = $4")
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
