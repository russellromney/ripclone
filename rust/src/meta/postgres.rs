//! [`MetaDb`] backed by PostgreSQL via `sqlx` (also works against any
//! Postgres-wire server). Differs from sqlite only in `$N` placeholders.

use super::{MetaDb, RefRow};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub struct PostgresMeta {
    pool: PgPool,
}

impl PostgresMeta {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .with_context(|| format!("connect postgres metadata {url}"))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl MetaDb for PostgresMeta {
    async fn init(&self) -> Result<()> {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS refs (
                repo_key TEXT NOT NULL,
                branch TEXT NOT NULL,
                commit_id TEXT NOT NULL,
                synced_at BIGINT,
                data TEXT NOT NULL,
                PRIMARY KEY (repo_key, branch)
            )",
        )
        .execute(&self.pool)
        .await
        .context("create refs table")?;
        Ok(())
    }

    async fn get(&self, repo_key: &str, branch: &str) -> Result<Option<RefRow>> {
        let row = sqlx::query(
            "SELECT data, commit_id, synced_at FROM refs
             WHERE repo_key = $1 AND branch = $2",
        )
        .bind(repo_key)
        .bind(branch)
        .fetch_optional(&self.pool)
        .await
        .context("get ref")?;
        match row {
            Some(row) => Ok(Some(RefRow {
                data: row.try_get(0)?,
                commit_id: row.try_get(1)?,
                synced_at: row.try_get(2)?,
            })),
            None => Ok(None),
        }
    }

    async fn get_by_commit(&self, repo_key: &str, commit: &str) -> Result<Vec<RefRow>> {
        let rows = sqlx::query(
            "SELECT data, commit_id, synced_at FROM refs
             WHERE repo_key = $1 AND commit_id = $2",
        )
        .bind(repo_key)
        .bind(commit)
        .fetch_all(&self.pool)
        .await
        .context("get refs by commit")?;
        rows.into_iter()
            .map(|row| -> Result<RefRow> {
                Ok(RefRow {
                    data: row.try_get(0)?,
                    commit_id: row.try_get(1)?,
                    synced_at: row.try_get(2)?,
                })
            })
            .collect()
    }

    async fn upsert(
        &self,
        repo_key: &str,
        branch: &str,
        data: &str,
        commit_id: &str,
        synced_at: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO refs (repo_key, branch, commit_id, synced_at, data)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (repo_key, branch) DO UPDATE SET
                 commit_id = excluded.commit_id,
                 synced_at = excluded.synced_at,
                 data = excluded.data",
        )
        .bind(repo_key)
        .bind(branch)
        .bind(commit_id)
        .bind(synced_at)
        .bind(data)
        .execute(&self.pool)
        .await
        .context("upsert ref")?;
        Ok(())
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT DISTINCT repo_key FROM refs")
            .fetch_all(&self.pool)
            .await
            .context("list repos")?;
        rows.iter().map(|r| Ok(r.try_get(0)?)).collect()
    }

    async fn list_branches(&self, repo_key: &str) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT branch FROM refs WHERE repo_key = $1")
            .bind(repo_key)
            .fetch_all(&self.pool)
            .await
            .context("list branches")?;
        rows.iter().map(|r| Ok(r.try_get(0)?)).collect()
    }

    async fn health(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .context("postgres metadata health")?;
        Ok(())
    }
}
