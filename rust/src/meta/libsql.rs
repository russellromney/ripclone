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
        // Index for commit-keyed reuse (get_by_commit); the PK is (repo_key,
        // branch), so a by-commit lookup would otherwise scan the repo's branches.
        self.conn()
            .await?
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_refs_commit ON refs (repo_key, commit_id)",
                (),
            )
            .await
            .context("create refs commit index")?;
        // Add the generation column to a table created before it existed
        // (best-effort: errors on an up-to-date table, which is fine).
        let _ = self
            .conn()
            .await?
            .execute("ALTER TABLE refs ADD COLUMN generation BIGINT", ())
            .await;
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

    async fn get_by_commit(&self, repo_key: &str, commit: &str) -> Result<Vec<RefRow>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT data, commit_id, synced_at FROM refs
                 WHERE repo_key = ? AND commit_id = ?",
                libsql::params![repo_key, commit],
            )
            .await
            .context("get refs by commit")?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(RefRow {
                data: row.get::<String>(0)?,
                commit_id: row.get::<String>(1)?,
                synced_at: row.get::<Option<i64>>(2)?,
            });
        }
        Ok(out)
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
                 WHERE excluded.commit_id = refs.commit_id
                    OR (refs.generation IS NOT NULL AND excluded.generation IS NOT NULL
                        AND excluded.generation >= refs.generation)
                    OR ((refs.generation IS NULL OR excluded.generation IS NULL)
                        AND (refs.synced_at IS NULL OR excluded.synced_at IS NULL
                             OR excluded.synced_at >= refs.synced_at))",
                libsql::params![repo_key, branch, commit_id, synced_at, generation, data],
            )
            .await
            .context("save_ordered ref")?;
        Ok(())
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

    async fn health(&self) -> Result<()> {
        let conn = self.conn().await?;
        conn.query("SELECT 1", ())
            .await
            .context("libsql metadata health")?;
        Ok(())
    }
}
