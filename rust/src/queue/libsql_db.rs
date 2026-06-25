//! [`QueueDb`] backed by the `libsql` crate, connecting to a **remote** Turso
//! Cloud database over the network — the multi-machine farm-out backend. (Local
//! files are served by the `sqlite` backend; libsql is built remote-only here so
//! it doesn't bundle SQLite's C core and collide with sqlx.)

use super::sql::{
    CREATE_ACTIVE_KEY_INDEX_SQL, CREATE_HISTORY_INDEX_SQL, CREATE_STATUS_INDEX_SQL,
    CREATE_TABLE_SQL, QueueDb,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use libsql::{Builder, Connection, Database};

pub struct LibsqlDb {
    db: Database,
}

impl LibsqlDb {
    /// Connect to a remote Turso Cloud / libsql server.
    pub async fn connect_remote(url: &str, token: &str) -> Result<Self> {
        let db = Builder::new_remote(url.to_string(), token.to_string())
            .build()
            .await
            .with_context(|| format!("open libsql remote {url}"))?;
        Ok(Self { db })
    }

    async fn conn(&self) -> Result<Connection> {
        let conn = self.db.connect().context("libsql connect")?;
        // Wait out lock contention rather than erroring (local files).
        let _ = conn.execute("PRAGMA busy_timeout = 5000", ()).await;
        Ok(conn)
    }
}

#[async_trait]
impl QueueDb for LibsqlDb {
    async fn init(&self) -> Result<()> {
        let conn = self.conn().await?;
        // WAL keeps readers from blocking the writer on a local file (no-op on
        // remote, where the server manages concurrency).
        let _ = conn.execute("PRAGMA journal_mode = WAL", ()).await;
        conn.execute(CREATE_TABLE_SQL, ())
            .await
            .context("create jobs table")?;
        conn.execute(CREATE_STATUS_INDEX_SQL, ())
            .await
            .context("create status index")?;
        if let Err(e) = conn.execute(CREATE_ACTIVE_KEY_INDEX_SQL, ()).await {
            tracing::warn!(
                "libsql: partial unique index unsupported ({e}); coalescing is best-effort"
            );
        }
        conn.execute(CREATE_HISTORY_INDEX_SQL, ())
            .await
            .context("create history index")?;
        Ok(())
    }

    async fn active_job_id(&self, key: &str) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT id FROM jobs WHERE key = ? AND status IN ('queued', 'claimed') LIMIT 1",
                [key],
            )
            .await
            .context("query active job")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    async fn insert_job(
        &self,
        key: &str,
        provider: &str,
        path: &str,
        branch: &str,
        created_at: i64,
    ) -> Result<i64> {
        let conn = self.conn().await?;
        conn.execute(
            "INSERT INTO jobs (key, provider, path, branch, status, created_at)
             VALUES (?, ?, ?, ?, 'queued', ?)",
            libsql::params![key, provider, path, branch, created_at],
        )
        .await
        .context("insert job")?;
        Ok(conn.last_insert_rowid())
    }

    async fn reclaim_stale(&self, cutoff: i64) -> Result<()> {
        let conn = self.conn().await?;
        conn.execute(
            "UPDATE jobs SET status = 'queued', worker_id = NULL
             WHERE status = 'claimed' AND claimed_at <= ?",
            [cutoff],
        )
        .await
        .context("reclaim stale jobs")?;
        Ok(())
    }

    async fn next_queued_id(&self) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at, id LIMIT 1",
                (),
            )
            .await
            .context("select next queued")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    async fn try_claim(&self, id: i64, worker_id: &str, now: i64) -> Result<bool> {
        let conn = self.conn().await?;
        let n = conn
            .execute(
                "UPDATE jobs SET status = 'claimed', worker_id = ?, claimed_at = ?
                 WHERE id = ? AND status = 'queued'",
                libsql::params![worker_id, now, id],
            )
            .await
            .context("claim job")?;
        Ok(n == 1)
    }

    async fn job_fields(&self, id: i64) -> Result<Option<(String, String, String)>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT provider, path, branch FROM jobs WHERE id = ?", [id])
            .await
            .context("fetch job fields")?;
        match rows.next().await? {
            Some(row) => Ok(Some((
                row.get::<String>(0)?,
                row.get::<String>(1)?,
                row.get::<String>(2)?,
            ))),
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
        let conn = self.conn().await?;
        conn.execute(
            "UPDATE jobs SET status = ?, finished_at = ?, error = ? WHERE id = ?",
            libsql::params![status, finished_at, error, id],
        )
        .await
        .context("finish job")?;
        Ok(())
    }

    async fn status(&self, id: i64) -> Result<Option<(String, Option<String>)>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT status, error FROM jobs WHERE id = ?", [id])
            .await
            .context("query job status")?;
        match rows.next().await? {
            Some(row) => Ok(Some((row.get::<String>(0)?, row.get::<Option<String>>(1)?))),
            None => Ok(None),
        }
    }

    async fn count_queued(&self) -> Result<i64> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT count(*) FROM jobs WHERE status = 'queued'", ())
            .await
            .context("count queued")?;
        match rows.next().await? {
            Some(row) => Ok(row.get::<i64>(0)?),
            None => Ok(0),
        }
    }

    async fn prune_failed(&self, cutoff: i64) -> Result<u64> {
        self.conn()
            .await?
            .execute(
                "DELETE FROM jobs WHERE status = 'failed' AND finished_at < ?",
                libsql::params![cutoff],
            )
            .await
            .context("prune failed jobs")
    }
}
