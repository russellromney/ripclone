//! [`MetaDb`] backed by the `libsql` crate, connecting to a **remote** Turso
//! Cloud database. SQLite-flavored SQL (`?` placeholders, `ON CONFLICT` upsert).

use super::{MetaDb, RefRow};
use anyhow::{Context, Result};
use async_trait::async_trait;
use libsql::{Builder, Connection, Database};

pub struct LibsqlMeta {
    db: Database,
}

impl LibsqlMeta {
    pub async fn connect_remote(url: &str, token: &str) -> Result<Self> {
        let db = Builder::new_remote(url.to_string(), token.to_string())
            .build()
            .await
            .with_context(|| format!("open libsql remote metadata {url}"))?;
        Ok(Self { db })
    }

    async fn conn(&self) -> Result<Connection> {
        self.db.connect().context("libsql connect")
    }
}

#[async_trait]
impl MetaDb for LibsqlMeta {
    async fn init(&self) -> Result<()> {
        self.conn()
            .await?
            .execute(
                "CREATE TABLE IF NOT EXISTS refs (
                    repo_key TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    commit_id TEXT NOT NULL,
                    synced_at BIGINT,
                    generation BIGINT,
                    data TEXT NOT NULL,
                    PRIMARY KEY (repo_key, branch)
                )",
                (),
            )
            .await
            .context("create refs table")?;
        self.conn()
            .await?
            .execute(
                "CREATE TABLE IF NOT EXISTS added_repos (
                    repo_key TEXT PRIMARY KEY NOT NULL,
                    data TEXT NOT NULL
                )",
                (),
            )
            .await
            .context("create added_repos table")?;
        Ok(())
    }

    async fn get(&self, repo_key: &str, branch: &str) -> Result<Option<RefRow>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT data, commit_id, synced_at FROM refs
                 WHERE repo_key = ? AND branch = ?",
                libsql::params![repo_key, branch],
            )
            .await
            .context("get ref")?;
        match rows.next().await? {
            Some(row) => Ok(Some(RefRow {
                data: row.get::<String>(0)?,
                commit_id: row.get::<String>(1)?,
                synced_at: row.get::<Option<i64>>(2)?,
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
        generation: Option<i64>,
        require_matching_commit: bool,
        internal_exact_result: bool,
        moving_publication_fence: Option<&str>,
    ) -> Result<()> {
        let insert_only = i64::from(internal_exact_result && require_matching_commit);
        let require_match = i64::from(require_matching_commit);
        let expected = moving_publication_fence.unwrap_or(commit_id);
        // DO UPDATE ... WHERE makes the ordering check atomic with the write;
        // a losing write is a silent no-op. Same policy as the sqlite adapter.
        self.conn()
            .await?
            .execute(
                "INSERT INTO refs (repo_key, branch, commit_id, synced_at, generation, data)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT (repo_key, branch) DO UPDATE SET
                     commit_id = excluded.commit_id,
                     synced_at = excluded.synced_at,
                     generation = excluded.generation,
                     data = excluded.data
                 WHERE ? = 0 AND (
                    (? = 1 AND (excluded.commit_id = refs.commit_id OR refs.commit_id = ?))
                    OR (? = 0 AND (excluded.commit_id = refs.commit_id
                        OR (refs.generation IS NOT NULL AND excluded.generation IS NOT NULL
                            AND excluded.generation >= refs.generation)
                        OR ((refs.generation IS NULL OR excluded.generation IS NULL)
                            AND (refs.synced_at IS NULL OR excluded.synced_at IS NULL
                                 OR excluded.synced_at >= refs.synced_at)))))",
                libsql::params![
                    repo_key,
                    branch,
                    commit_id,
                    synced_at,
                    generation,
                    data,
                    insert_only,
                    require_match,
                    expected,
                    require_match
                ],
            )
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
        let changed = self
            .conn()
            .await?
            .execute(
                "UPDATE refs SET data = ?
                 WHERE repo_key = ? AND branch = ? AND commit_id = ? AND data = ?",
                libsql::params![new_data, repo_key, branch, expected_commit, expected_data],
            )
            .await
            .context("compare-and-swap ref data")?;
        Ok(changed > 0)
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT DISTINCT repo_key FROM refs", ())
            .await
            .context("list repos")?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row.get::<String>(0)?);
        }
        Ok(out)
    }

    async fn list_branches(&self, repo_key: &str) -> Result<Vec<String>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT branch FROM refs WHERE repo_key = ?",
                libsql::params![repo_key],
            )
            .await
            .context("list branches")?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row.get::<String>(0)?);
        }
        Ok(out)
    }

    async fn add_repo(&self, repo_key: &str, data: &str) -> Result<()> {
        self.conn()
            .await?
            .execute(
                "INSERT INTO added_repos (repo_key, data) VALUES (?, ?)
                 ON CONFLICT (repo_key) DO UPDATE SET data = excluded.data",
                libsql::params![repo_key, data],
            )
            .await
            .context("add repo")?;
        Ok(())
    }

    async fn get_added_repo(&self, repo_key: &str) -> Result<Option<String>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT data FROM added_repos WHERE repo_key = ?",
                libsql::params![repo_key],
            )
            .await
            .context("get added repo")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<String>(0)?)),
            None => Ok(None),
        }
    }

    async fn remove_added_repo(&self, repo_key: &str) -> Result<()> {
        self.conn()
            .await?
            .execute(
                "DELETE FROM added_repos WHERE repo_key = ?",
                libsql::params![repo_key],
            )
            .await
            .context("remove added repo")?;
        Ok(())
    }

    async fn list_added_repos(&self) -> Result<Vec<String>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT data FROM added_repos ORDER BY repo_key", ())
            .await
            .context("list added repos")?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row.get::<String>(0)?);
        }
        Ok(out)
    }

    async fn health(&self) -> Result<()> {
        let conn = self.conn().await?;
        conn.query("SELECT 1", ())
            .await
            .context("libsql metadata health")?;
        Ok(())
    }
}
