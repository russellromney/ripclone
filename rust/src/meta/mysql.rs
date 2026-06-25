//! [`MetaDb`] backed by MySQL via `sqlx` (also works against any MySQL-wire
//! server). Dialect notes: `VARCHAR` key columns (the composite PK can't be
//! `TEXT`), `LONGTEXT` for the JSON blob, `ON DUPLICATE KEY UPDATE` upsert. No
//! reserved-word columns here, so no backticks needed.

use super::{MetaDb, RefRow};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

pub struct MysqlMeta {
    pool: MySqlPool,
}

impl MysqlMeta {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .with_context(|| format!("connect mysql metadata {url}"))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl MetaDb for MysqlMeta {
    async fn init(&self) -> Result<()> {
        // VARCHAR sizes keep the composite (repo_key, branch) PK under MySQL's
        // 3072-byte InnoDB key limit: (512 + 255) * 4 bytes for utf8mb4 = 3068,
        // while comfortably fitting any real repo key / branch.
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS refs (
                repo_key VARCHAR(512) NOT NULL,
                branch VARCHAR(255) NOT NULL,
                commit_id VARCHAR(64) NOT NULL,
                synced_at BIGINT,
                data LONGTEXT NOT NULL,
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
             WHERE repo_key = ? AND branch = ?",
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

    async fn save_ordered(
        &self,
        repo_key: &str,
        branch: &str,
        data: &str,
        commit_id: &str,
        synced_at: Option<i64>,
    ) -> Result<()> {
        // MySQL's ON DUPLICATE KEY UPDATE has no WHERE clause, so the ordering
        // decision is computed once into the session variable `@ripl` in the
        // first (data) assignment — while `commit_id`/`synced_at` still hold
        // their original values — then reused for the remaining columns. The
        // assignments evaluate left-to-right, so `data` must come first or the
        // condition would read already-overwritten columns. `@ripl` is set and
        // read within this one statement, so the connection pool can't leak it
        // across calls. Policy is identical to the sqlite adapter's WHERE.
        sqlx::query(
            "INSERT INTO refs (repo_key, branch, commit_id, synced_at, data)
             VALUES (?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                 data = IF(@ripl := (VALUES(commit_id) = commit_id
                                     OR synced_at IS NULL
                                     OR VALUES(synced_at) IS NULL
                                     OR VALUES(synced_at) >= synced_at),
                           VALUES(data), data),
                 commit_id = IF(@ripl, VALUES(commit_id), commit_id),
                 synced_at = IF(@ripl, VALUES(synced_at), synced_at)",
        )
        .bind(repo_key)
        .bind(branch)
        .bind(commit_id)
        .bind(synced_at)
        .bind(data)
        .execute(&self.pool)
        .await
        .context("save_ordered ref")?;
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
        let rows = sqlx::query("SELECT branch FROM refs WHERE repo_key = ?")
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
            .context("mysql metadata health")?;
        Ok(())
    }
}
