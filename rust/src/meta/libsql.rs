//! Exact-result metadata over the server's Turso embedded-replica handle.

use super::{MetaDb, ResultRow};
use anyhow::{Context, Result};
use async_trait::async_trait;
use libsql::{Connection, Database};
use std::sync::Arc;
use std::time::Duration;

pub struct LibsqlMeta {
    db: Arc<Database>,
}

impl LibsqlMeta {
    pub(crate) fn from_database(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub async fn connect(path: &str) -> Result<Self> {
        let db = libsql::Builder::new_local(path)
            .build()
            .await
            .context("open local control database")?;
        Ok(Self { db: Arc::new(db) })
    }

    async fn conn(&self) -> Result<Connection> {
        let conn = self.db.connect().context("libsql connect")?;
        conn.busy_timeout(Duration::from_secs(5))
            .context("configure metadata busy timeout")?;
        Ok(conn)
    }
}

#[async_trait]
impl MetaDb for LibsqlMeta {
    async fn init(&self) -> Result<()> {
        self.conn()
            .await?
            .execute(
                "CREATE TABLE IF NOT EXISTS results (
                    repo_key TEXT NOT NULL,
                    commit_id TEXT NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (repo_key, commit_id)
                )",
                (),
            )
            .await
            .context("create exact results table")?;
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

    async fn get_result(&self, repo_key: &str, commit: &str) -> Result<Option<ResultRow>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT data FROM results WHERE repo_key = ? AND commit_id = ?",
                libsql::params![repo_key, commit],
            )
            .await
            .context("get exact result")?;
        match rows.next().await? {
            Some(row) => Ok(Some(ResultRow {
                data: row.get::<String>(0)?,
            })),
            None => Ok(None),
        }
    }

    async fn insert_result(&self, repo_key: &str, commit: &str, data: &str) -> Result<bool> {
        let changed = self
            .conn()
            .await?
            .execute(
                "INSERT OR IGNORE INTO results(repo_key, commit_id, data) VALUES (?, ?, ?)",
                libsql::params![repo_key, commit, data],
            )
            .await
            .context("insert exact result")?;
        Ok(changed == 1)
    }

    async fn compare_and_swap_result(
        &self,
        repo_key: &str,
        commit: &str,
        expected_data: &str,
        new_data: &str,
    ) -> Result<bool> {
        let changed = self
            .conn()
            .await?
            .execute(
                "UPDATE results SET data = ? WHERE repo_key = ? AND commit_id = ? AND data = ?",
                libsql::params![new_data, repo_key, commit, expected_data],
            )
            .await
            .context("compare-and-swap exact result")?;
        Ok(changed == 1)
    }

    async fn compare_and_swap_result_if_job_inactive(
        &self,
        repo_key: &str,
        commit: &str,
        expected_data: &str,
        new_data: &str,
    ) -> Result<bool> {
        let conn = self.conn().await?;
        let tx = conn
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .context("begin inactive-result eviction")?;
        let mut table_rows = tx
            .query(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs' LIMIT 1",
                (),
            )
            .await
            .context("check job table for exact-result eviction")?;
        let jobs_exist = table_rows.next().await?.is_some();
        drop(table_rows);

        if jobs_exist {
            let job_key = format!("{repo_key}\x1f{commit}");
            let mut active_rows = tx
                .query(
                    "SELECT 1 FROM jobs
                     WHERE key = ?1 AND status IN ('queued', 'claimed') LIMIT 1",
                    [job_key.as_str()],
                )
                .await
                .context("check active job for exact-result eviction")?;
            let active = active_rows.next().await?.is_some();
            drop(active_rows);
            if active {
                tx.rollback().await.ok();
                return Ok(false);
            }
        }

        let changed = tx
            .execute(
                "UPDATE results SET data = ?1
                 WHERE repo_key = ?2 AND commit_id = ?3 AND data = ?4",
                libsql::params![new_data, repo_key, commit, expected_data],
            )
            .await
            .context("compare-and-swap inactive exact result")?;
        tx.commit()
            .await
            .context("commit inactive-result eviction")?;
        Ok(changed == 1)
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT DISTINCT repo_key FROM results", ())
            .await
            .context("list result repositories")?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row.get::<String>(0)?);
        }
        Ok(out)
    }

    async fn list_commits(&self, repo_key: &str) -> Result<Vec<String>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT commit_id FROM results WHERE repo_key = ? ORDER BY commit_id",
                libsql::params![repo_key],
            )
            .await
            .context("list exact result commits")?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row.get::<String>(0)?);
        }
        Ok(out)
    }

    async fn delete_result(&self, repo_key: &str, commit: &str) -> Result<()> {
        self.conn()
            .await?
            .execute(
                "DELETE FROM results WHERE repo_key = ? AND commit_id = ?",
                libsql::params![repo_key, commit],
            )
            .await
            .context("delete exact result")?;
        Ok(())
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
        self.conn()
            .await?
            .query("SELECT 1", ())
            .await
            .context("libsql metadata health")?;
        Ok(())
    }
}
