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

    /// Reuse the already-authenticated metadata pool for normalized scheduler
    /// state; callers must not open a second DSN/credential path.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Construct the artifact scheduler on the same pool as ref metadata.
    pub async fn artifact_scheduler(
        &self,
        limits: crate::artifact_scheduler::SchedulerLimits,
        verifier: std::sync::Arc<dyn crate::artifact_scheduler::CompletionVerifier>,
    ) -> Result<crate::artifact_scheduler_postgres::PostgresArtifactScheduler> {
        crate::artifact_scheduler_postgres::PostgresArtifactScheduler::from_pool(
            self.pool.clone(),
            limits,
            verifier,
        )
        .await
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
                generation BIGINT,
                data TEXT NOT NULL,
                PRIMARY KEY (repo_key, branch)
            )",
        )
        .execute(&self.pool)
        .await
        .context("create refs table")?;
        // Index for commit-keyed reuse (get_by_commit); the PK is (repo_key,
        // branch), so a by-commit lookup would otherwise scan the repo's branches.
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_refs_commit ON refs (repo_key, commit_id)")
            .execute(&self.pool)
            .await
            .context("create refs commit index")?;
        sqlx::raw_sql("ALTER TABLE refs ADD COLUMN IF NOT EXISTS generation BIGINT")
            .execute(&self.pool)
            .await
            .context("add generation column")?;
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS added_repos (
                repo_key TEXT PRIMARY KEY NOT NULL,
                data TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await
        .context("create added_repos table")?;
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

    async fn save_ordered(
        &self,
        repo_key: &str,
        branch: &str,
        data: &str,
        commit_id: &str,
        synced_at: Option<i64>,
        generation: Option<i64>,
    ) -> Result<()> {
        // DO UPDATE ... WHERE makes the ordering check atomic with the write;
        // a losing write is a silent no-op. See the sqlite adapter for the
        // policy, which is identical.
        sqlx::query(
            "INSERT INTO refs (repo_key, branch, commit_id, synced_at, generation, data)
             VALUES ($1, $2, $3, $4, $5, $6)
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

    async fn compare_and_swap_data(
        &self,
        repo_key: &str,
        branch: &str,
        expected_commit: &str,
        expected_data: &str,
        new_data: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE refs SET data = $1
             WHERE repo_key = $2 AND branch = $3 AND commit_id = $4 AND data = $5",
        )
        .bind(new_data)
        .bind(repo_key)
        .bind(branch)
        .bind(expected_commit)
        .bind(expected_data)
        .execute(&self.pool)
        .await
        .context("compare-and-swap ref data")?;
        Ok(result.rows_affected() > 0)
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

    async fn add_repo(&self, repo_key: &str, data: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO added_repos (repo_key, data) VALUES ($1, $2)
             ON CONFLICT (repo_key) DO UPDATE SET data = excluded.data",
        )
        .bind(repo_key)
        .bind(data)
        .execute(&self.pool)
        .await
        .context("add repo")?;
        Ok(())
    }

    async fn insert_added_repo(&self, repo_key: &str, data: &str) -> Result<bool> {
        let result = sqlx::query("INSERT INTO added_repos (repo_key, data) VALUES ($1, $2) ON CONFLICT (repo_key) DO NOTHING")
            .bind(repo_key).bind(data).execute(&self.pool).await.context("insert added repo")?;
        Ok(result.rows_affected() == 1)
    }

    async fn compare_and_swap_added_repo(
        &self,
        repo_key: &str,
        expected_data: &str,
        new_data: &str,
    ) -> Result<bool> {
        let result =
            sqlx::query("UPDATE added_repos SET data = $1 WHERE repo_key = $2 AND data = $3")
                .bind(new_data)
                .bind(repo_key)
                .bind(expected_data)
                .execute(&self.pool)
                .await
                .context("CAS added repo")?;
        Ok(result.rows_affected() == 1)
    }

    async fn get_added_repo(&self, repo_key: &str) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT data FROM added_repos WHERE repo_key = $1")
            .bind(repo_key)
            .fetch_optional(&self.pool)
            .await
            .context("get added repo")
    }

    async fn remove_added_repo(&self, repo_key: &str) -> Result<()> {
        sqlx::query("DELETE FROM added_repos WHERE repo_key = $1")
            .bind(repo_key)
            .execute(&self.pool)
            .await
            .context("remove added repo")?;
        Ok(())
    }

    async fn list_added_repos(&self) -> Result<Vec<String>> {
        sqlx::query_scalar("SELECT data FROM added_repos ORDER BY repo_key")
            .fetch_all(&self.pool)
            .await
            .context("list added repos")
    }

    async fn health(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .context("postgres metadata health")?;
        Ok(())
    }
}
