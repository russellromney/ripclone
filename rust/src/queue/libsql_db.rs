//! [`QueueDb`] backed by the `libsql` crate, connecting to a **remote** Turso
//! Cloud database over the network — the multi-machine farm-out backend. (Local
//! files are served by the `sqlite` backend; libsql is built remote-only here so
//! it doesn't bundle SQLite's C core and collide with sqlx.)

use super::sql::{
    ADD_ATTEMPTS_COLUMN_SQL, ADD_CREDENTIAL_COLUMN_SQL, CREATE_ACTIVE_KEY_INDEX_SQL,
    CREATE_HISTORY_INDEX_SQL, CREATE_STATUS_INDEX_SQL, CREATE_TABLE_SQL,
    DROP_LEGACY_ACTIVE_KEY_INDEX_SQL, QueueDb,
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
        // Migrate a legacy table to add the credential column (best-effort: errors
        // "duplicate column" on a fresh table, which is fine).
        let _ = conn.execute(ADD_CREDENTIAL_COLUMN_SQL, ()).await;
        // Same best-effort migration for the attempts column (dead-letter bound).
        let _ = conn.execute(ADD_ATTEMPTS_COLUMN_SQL, ()).await;
        conn.execute(CREATE_STATUS_INDEX_SQL, ())
            .await
            .context("create status index")?;
        let _ = conn.execute(DROP_LEGACY_ACTIVE_KEY_INDEX_SQL, ()).await;
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
                "SELECT id FROM jobs WHERE key = ? AND status = 'queued' LIMIT 1",
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
        credential: Option<&str>,
        created_at: i64,
    ) -> Result<i64> {
        let conn = self.conn().await?;
        let cred_val = match credential {
            Some(s) => libsql::Value::Text(s.to_string()),
            None => libsql::Value::Null,
        };
        conn.execute(
            "INSERT INTO jobs (key, provider, path, branch, status, credential, created_at)
             VALUES (?, ?, ?, ?, 'queued', ?, ?)",
            libsql::params![key, provider, path, branch, cred_val, created_at],
        )
        .await
        .context("insert job")?;
        Ok(conn.last_insert_rowid())
    }

    async fn reclaim_stale(
        &self,
        cutoff: i64,
        max_attempts: i64,
        now: i64,
        dead_letter_error: &str,
    ) -> Result<()> {
        let conn = self.conn().await?;
        // Dead-letter stale claims over the attempt cap; requeue the rest.
        conn.execute(
            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?,
                 worker_id = NULL, credential = NULL
             WHERE status = 'claimed' AND claimed_at <= ? AND attempts >= ?",
            libsql::params![now, dead_letter_error, cutoff, max_attempts],
        )
        .await
        .context("dead-letter stale jobs")?;
        conn.execute(
            "UPDATE jobs SET status = 'queued', worker_id = NULL
             WHERE status = 'claimed' AND claimed_at <= ? AND attempts < ?",
            libsql::params![cutoff, max_attempts],
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
                "UPDATE jobs SET status = 'claimed', worker_id = ?, claimed_at = ?,
                     attempts = attempts + 1
                 WHERE id = ? AND status = 'queued'",
                libsql::params![worker_id, now, id],
            )
            .await
            .context("claim job")?;
        Ok(n == 1)
    }

    async fn job_fields(
        &self,
        id: i64,
    ) -> Result<Option<(String, String, String, Option<String>)>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT provider, path, branch, credential FROM jobs WHERE id = ?",
                [id],
            )
            .await
            .context("fetch job fields")?;
        match rows.next().await? {
            Some(row) => Ok(Some((
                row.get::<String>(0)?,
                row.get::<String>(1)?,
                row.get::<String>(2)?,
                row.get::<Option<String>>(3)?,
            ))),
            None => Ok(None),
        }
    }

    async fn finish(
        &self,
        id: i64,
        worker_id: &str,
        status: &str,
        finished_at: i64,
        error: Option<&str>,
    ) -> Result<bool> {
        let conn = self.conn().await?;
        // Conditional on still owning the claim (no double-settle after a
        // reclaim). Clearing the credential keeps a token out of done-job history.
        let n = conn
            .execute(
                "UPDATE jobs SET status = ?, finished_at = ?, error = ?, credential = NULL
                 WHERE id = ? AND worker_id = ? AND status = 'claimed'",
                libsql::params![status, finished_at, error, id, worker_id],
            )
            .await
            .context("finish job")?;
        Ok(n == 1)
    }

    async fn claimed_attempts(&self, id: i64, worker_id: &str) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT attempts FROM jobs WHERE id = ? AND worker_id = ? AND status = 'claimed'",
                libsql::params![id, worker_id],
            )
            .await
            .context("fetch claimed attempts")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    async fn requeue_claim(&self, id: i64, worker_id: &str, error: &str) -> Result<bool> {
        let conn = self.conn().await?;
        let n = conn
            .execute(
                "UPDATE jobs SET status = 'queued', worker_id = NULL, error = ?
                 WHERE id = ? AND worker_id = ? AND status = 'claimed'",
                libsql::params![error, id, worker_id],
            )
            .await
            .context("requeue retryable job")?;
        Ok(n == 1)
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
