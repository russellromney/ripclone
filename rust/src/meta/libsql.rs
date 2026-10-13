//! Exact-result metadata over the server's Turso embedded-replica handle.

use super::ResultRow;
use anyhow::{Context, Result};
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

    pub(crate) async fn init(&self) -> Result<()> {
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

    pub(crate) async fn get_result(
        &self,
        repo_key: &str,
        commit: &str,
    ) -> Result<Option<ResultRow>> {
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

    pub(crate) async fn insert_result(
        &self,
        repo_key: &str,
        commit: &str,
        data: &str,
    ) -> Result<bool> {
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

    pub(crate) async fn compare_and_swap_result(
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

    pub(crate) async fn list_commits(&self, repo_key: &str) -> Result<Vec<String>> {
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

    pub(crate) async fn add_repo(&self, repo_key: &str, data: &str) -> Result<()> {
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

    pub(crate) async fn get_added_repo(&self, repo_key: &str) -> Result<Option<String>> {
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

    pub(crate) async fn remove_added_repo(&self, repo_key: &str) -> Result<()> {
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

    pub(crate) async fn list_added_repos(&self) -> Result<Vec<String>> {
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

    pub(crate) async fn health(&self) -> Result<()> {
        self.conn()
            .await?
            .query("SELECT 1", ())
            .await
            .context("libsql metadata health")?;
        Ok(())
    }
}
