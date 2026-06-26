//! [`MetaDb`] backed by local SQLite via `sqlx`.

use super::{MetaDb, RefRow};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;

pub struct SqliteMeta {
    pool: SqlitePool,
}

impl SqliteMeta {
    pub async fn connect(path: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(path)
            .with_context(|| format!("parse sqlite url {path}"))?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5))
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .with_context(|| format!("open sqlite metadata db {path}"))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl MetaDb for SqliteMeta {
    async fn init(&self) -> Result<()> {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS refs (
                repo_key TEXT NOT NULL,
                branch TEXT NOT NULL,
                commit_id TEXT NOT NULL,
                synced_at BIGINT,
                generation BIGINT,
                data TEXT NOT NULL,
                PRIMARY KEY (repo_key, branch)
            )",
        )
        .execute(&self.pool)
        .await
        .context("create refs table")?;
        // Index for commit-keyed reuse (get_by_commit). The PK is (repo_key,
        // branch), so without this a lookup by commit scans the repo's branches.
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_refs_commit ON refs (repo_key, commit_id)")
            .execute(&self.pool)
            .await
            .context("create refs commit index")?;
        // Add the generation column to a table created before it existed
        // (best-effort: errors "duplicate column" on an up-to-date table).
        let _ = sqlx::raw_sql("ALTER TABLE refs ADD COLUMN generation BIGINT")
            .execute(&self.pool)
            .await;
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

    async fn get_by_commit(&self, repo_key: &str, commit: &str) -> Result<Vec<RefRow>> {
        let rows = sqlx::query(
            "SELECT data, commit_id, synced_at FROM refs
             WHERE repo_key = ? AND commit_id = ?",
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

    async fn save_ordered(
        &self,
        repo_key: &str,
        branch: &str,
        data: &str,
        commit_id: &str,
        synced_at: Option<i64>,
        generation: Option<i64>,
    ) -> Result<()> {
        // The DO UPDATE ... WHERE makes the ordering check atomic with the write:
        // on conflict the row is overwritten only when the new write wins — same
        // commit, a higher-or-equal generation (commit history depth), or, when
        // either side has no generation, a newer-or-equal synced_at. A losing
        // write is a silent no-op, exactly like the file/S3 stores.
        sqlx::query(
            "INSERT INTO refs (repo_key, branch, commit_id, synced_at, generation, data)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT (repo_key, branch) DO UPDATE SET
                 commit_id = excluded.commit_id,
                 synced_at = excluded.synced_at,
                 generation = excluded.generation,
                 data = excluded.data
             WHERE excluded.commit_id = refs.commit_id
                OR (refs.generation IS NOT NULL AND excluded.generation IS NOT NULL
                    AND excluded.generation >= refs.generation)
                OR ((refs.generation IS NULL OR excluded.generation IS NULL)
                    AND (refs.synced_at IS NULL OR excluded.synced_at IS NULL
                         OR excluded.synced_at >= refs.synced_at))",
        )
        .bind(repo_key)
        .bind(branch)
        .bind(commit_id)
        .bind(synced_at)
        .bind(generation)
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
            .context("sqlite metadata health")?;
        Ok(())
    }
}
