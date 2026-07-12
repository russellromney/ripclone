//! [`QueueDb`] backed by the `libsql` crate, connecting to a **remote** Turso
//! Cloud database over the network — the multi-machine farm-out backend. (Local
//! files are served by the `sqlite` backend; libsql is built remote-only here so
//! it doesn't bundle SQLite's C core and collide with sqlx.)

use super::sql::{
    ADD_ATTEMPTS_COLUMN_SQL, ADD_CREDENTIAL_COLUMN_SQL, ADD_INITIALIZATION_ATTEMPT_COLUMN_SQL,
    ADD_SIZE_CLASS_COLUMN_SQL, CREATE_ACTIVE_KEY_INDEX_SQL, CREATE_HISTORY_INDEX_SQL,
    CREATE_STATUS_INDEX_SQL, CREATE_TABLE_SQL, CREATE_WORKERS_HEARTBEAT_INDEX_SQL,
    CREATE_WORKERS_TABLE_SQL, DROP_LEGACY_ACTIVE_KEY_INDEX_SQL, DeadLetteredInitialization,
    QueueDb, SUPERSEDED_BY_NEWER_QUEUED, now_secs,
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
        if let Err(error) = conn
            .execute(ADD_INITIALIZATION_ATTEMPT_COLUMN_SQL, ())
            .await
        {
            if !error
                .to_string()
                .to_ascii_lowercase()
                .contains("duplicate column")
            {
                return Err(error).context("add jobs.initialization_attempt_id");
            }
        }
        let mut rows = conn
            .query(
                "SELECT count(*) FROM pragma_table_info('jobs') WHERE name='initialization_attempt_id'",
                (),
            )
            .await
            .context("validate jobs.initialization_attempt_id")?;
        let attempt_column = rows
            .next()
            .await?
            .context("jobs.initialization_attempt_id validation returned no row")?
            .get::<i64>(0)?;
        anyhow::ensure!(
            attempt_column == 1,
            "queue schema is missing jobs.initialization_attempt_id"
        );
        // Same best-effort migration for the attempts column (dead-letter bound).
        let _ = conn.execute(ADD_ATTEMPTS_COLUMN_SQL, ()).await;
        // size_class rank: the claim filter (right-sizing) reads it, and
        // stale-reclaim bumps it as an escalation rung. Best-effort migration.
        let _ = conn.execute(ADD_SIZE_CLASS_COLUMN_SQL, ()).await;
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
        // Worker heartbeat/registry for dispatcher live-count (D3).
        conn.execute(CREATE_WORKERS_TABLE_SQL, ())
            .await
            .context("create workers table")?;
        conn.execute(CREATE_WORKERS_HEARTBEAT_INDEX_SQL, ())
            .await
            .context("create workers heartbeat index")?;
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
        initialization_attempt_id: Option<&str>,
        size_class: i64,
        created_at: i64,
    ) -> Result<i64> {
        let conn = self.conn().await?;
        let cred_val = match credential {
            Some(s) => libsql::Value::Text(s.to_string()),
            None => libsql::Value::Null,
        };
        let attempt_val = match initialization_attempt_id {
            Some(s) => libsql::Value::Text(s.to_string()),
            None => libsql::Value::Null,
        };
        conn.execute(
            "INSERT INTO jobs (key, provider, path, branch, status, credential, initialization_attempt_id, size_class, created_at)
             VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?)",
            libsql::params![key, provider, path, branch, cred_val, attempt_val, size_class, created_at],
        )
        .await
        .context("insert job")?;
        Ok(conn.last_insert_rowid())
    }

    async fn raise_size_class(&self, id: i64, rank: i64) -> Result<()> {
        let conn = self.conn().await?;
        conn.execute(
            "UPDATE jobs SET size_class = MAX(size_class, ?)
             WHERE id = ? AND status = 'queued'",
            libsql::params![rank, id],
        )
        .await
        .context("raise size_class")?;
        Ok(())
    }

    async fn reclaim_stale(
        &self,
        cutoff: i64,
        max_attempts: i64,
        now: i64,
        dead_letter_error: &str,
    ) -> Result<()> {
        let conn = self.conn().await?;
        // Dead-letter stale claims over the attempt cap.
        conn.execute(
            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?,
                 worker_id = NULL, credential = NULL
             WHERE status = 'claimed' AND claimed_at <= ? AND attempts >= ?",
            libsql::params![now, dead_letter_error, cutoff, max_attempts],
        )
        .await
        .context("dead-letter stale jobs")?;
        // Under-cap with a newer queued sibling → superseded (unique key).
        conn.execute(
            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?,
                 worker_id = NULL, credential = NULL
             WHERE id IN (
                 SELECT id FROM (
                     SELECT j1.id AS id FROM jobs j1
                     WHERE j1.status = 'claimed' AND j1.claimed_at <= ? AND j1.attempts < ?
                       AND EXISTS (
                           SELECT 1 FROM jobs j2
                           WHERE j2.key = j1.key AND j2.status = 'queued' AND j2.id != j1.id
                       )
                 )
             )",
            libsql::params![now, SUPERSEDED_BY_NEWER_QUEUED, cutoff, max_attempts],
        )
        .await
        .context("supersede stale jobs with a newer queued sibling")?;
        // Under-cap with no sibling: requeue and bump size_class.
        conn.execute(
            "UPDATE jobs SET status = 'queued', worker_id = NULL,
                 size_class = size_class + 1
             WHERE status = 'claimed' AND claimed_at <= ? AND attempts < ?",
            libsql::params![cutoff, max_attempts],
        )
        .await
        .context("reclaim stale jobs")?;
        Ok(())
    }

    async fn dead_lettered_initializations(&self) -> Result<Vec<DeadLetteredInitialization>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT id,provider,path,branch,initialization_attempt_id,error FROM jobs
             WHERE status='failed' AND initialization_attempt_id IS NOT NULL
               AND error LIKE 'dead-lettered after %'",
                (),
            )
            .await
            .context("list dead-lettered admission jobs")?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(DeadLetteredInitialization {
                id: row.get::<i64>(0)?,
                provider: row.get::<String>(1)?,
                path: row.get::<String>(2)?,
                branch: row.get::<String>(3)?,
                initialization_attempt_id: row.get::<String>(4)?,
                error: row.get::<String>(5)?,
            });
        }
        Ok(out)
    }

    async fn acknowledge_dead_lettered_initialization(
        &self,
        id: i64,
        attempt_id: &str,
    ) -> Result<()> {
        let conn = self.conn().await?;
        conn.execute(
            "UPDATE jobs SET initialization_attempt_id=NULL WHERE id=? AND status='failed' AND initialization_attempt_id=?",
            libsql::params![id, attempt_id],
        ).await.context("acknowledge dead-lettered admission")?;
        Ok(())
    }

    async fn job_size_class(&self, id: i64) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query("SELECT size_class FROM jobs WHERE id = ?", [id])
            .await
            .context("fetch job size_class")?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<i64>(0)?)),
            None => Ok(None),
        }
    }

    async fn next_queued_id(&self, max_size_class: Option<i64>) -> Result<Option<i64>> {
        let conn = self.conn().await?;
        let mut rows = match max_size_class {
            None => conn
                .query(
                    "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at, id LIMIT 1",
                    (),
                )
                .await
                .context("select next queued")?,
            Some(ceiling) => conn
                .query(
                    "SELECT id FROM jobs WHERE status = 'queued' AND size_class <= ?
                     ORDER BY created_at, id LIMIT 1",
                    libsql::params![ceiling],
                )
                .await
                .context("select next queued under size-class ceiling")?,
        };
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
    ) -> Result<Option<(String, String, String, Option<String>, Option<String>)>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT provider, path, branch, credential, initialization_attempt_id FROM jobs WHERE id = ?",
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
                row.get::<Option<String>>(4)?,
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
                 WHERE id = ? AND worker_id = ? AND status = 'claimed'
                   AND NOT EXISTS (
                       SELECT 1 FROM jobs AS j2
                       WHERE j2.key = (SELECT key FROM jobs WHERE id = ?)
                         AND j2.status = 'queued' AND j2.id != ?
                   )",
                libsql::params![error, id, worker_id, id, id],
            )
            .await
            .context("requeue retryable job")?;
        if n == 1 {
            return Ok(true);
        }
        let n = conn
            .execute(
                "UPDATE jobs SET status = 'failed', finished_at = ?, error = ?, credential = NULL
                 WHERE id = ? AND worker_id = ? AND status = 'claimed'",
                libsql::params![now_secs(), SUPERSEDED_BY_NEWER_QUEUED, id, worker_id],
            )
            .await
            .context("supersede claim blocked by newer queued job")?;
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

    async fn count_queued_by_size_class(&self) -> Result<Vec<(i64, i64)>> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT size_class, count(*) FROM jobs
                 WHERE status = 'queued'
                 GROUP BY size_class
                 ORDER BY size_class",
                (),
            )
            .await
            .context("count queued by size_class")?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let rank = row.get::<i64>(0)?;
            let count = row.get::<i64>(1)?;
            if count > 0 {
                out.push((rank, count));
            }
        }
        Ok(out)
    }

    async fn prune_failed(&self, cutoff: i64) -> Result<u64> {
        self.conn()
            .await?
            .execute(
                "DELETE FROM jobs WHERE status = 'failed' AND finished_at < ? AND (initialization_attempt_id IS NULL OR error NOT LIKE 'dead-lettered after %')",
                libsql::params![cutoff],
            )
            .await
            .context("prune failed jobs")
    }

    fn supports_worker_registry(&self) -> bool {
        true
    }

    async fn upsert_heartbeat(
        &self,
        worker_id: &str,
        max_size_class: Option<i64>,
        current_job: Option<i64>,
        now: i64,
    ) -> Result<()> {
        let max_sc = match max_size_class {
            Some(n) => libsql::Value::Integer(n),
            None => libsql::Value::Null,
        };
        let cur = match current_job {
            Some(n) => libsql::Value::Integer(n),
            None => libsql::Value::Null,
        };
        self.conn()
            .await?
            .execute(
                "INSERT INTO workers (worker_id, max_size_class, current_job, last_heartbeat)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(worker_id) DO UPDATE SET
                     max_size_class = excluded.max_size_class,
                     current_job = excluded.current_job,
                     last_heartbeat = excluded.last_heartbeat",
                libsql::params![worker_id, max_sc, cur, now],
            )
            .await
            .context("upsert worker heartbeat")?;
        Ok(())
    }

    async fn count_live_workers(&self, cutoff: i64) -> Result<i64> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT count(*) FROM workers WHERE last_heartbeat >= ?",
                [cutoff],
            )
            .await
            .context("count live workers")?;
        match rows.next().await? {
            Some(row) => Ok(row.get::<i64>(0)?),
            None => Ok(0),
        }
    }

    async fn count_live_workers_capable(&self, cutoff: i64, min_rank: i64) -> Result<i64> {
        let conn = self.conn().await?;
        let mut rows = conn
            .query(
                "SELECT count(*) FROM workers
                 WHERE last_heartbeat >= ?
                   AND (max_size_class IS NULL OR max_size_class >= ?)",
                libsql::params![cutoff, min_rank],
            )
            .await
            .context("count live workers capable of rank")?;
        match rows.next().await? {
            Some(row) => Ok(row.get::<i64>(0)?),
            None => Ok(0),
        }
    }

    async fn prune_stale_workers(&self, cutoff: i64) -> Result<u64> {
        self.conn()
            .await?
            .execute(
                "DELETE FROM workers WHERE last_heartbeat < ?",
                libsql::params![cutoff],
            )
            .await
            .context("prune stale workers")
    }
}
