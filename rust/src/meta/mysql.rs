//! [`MetaDb`] backed by MySQL via `sqlx` (also works against any MySQL-wire
//! server). Dialect notes: `VARCHAR` key columns (the composite PK can't be
//! `TEXT`), `LONGTEXT` for the JSON blob, `ON DUPLICATE KEY UPDATE` upsert. No
//! reserved-word columns here, so no backticks needed.

use super::{MetaDb, RefRow};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::Row;
use sqlx::mysql::{MySql, MySqlPool, MySqlPoolOptions};
use sqlx::pool::PoolConnection;

const ADMISSION_INSERT_LOCK_SQL: &str = "SELECT GET_LOCK(SHA2(?, 256), 10)";
const ADMISSION_RELEASE_LOCK_SQL: &str = "SELECT RELEASE_LOCK(SHA2(?, 256))";
const ADMISSION_CURRENT_BYTES_SQL: &str = "SELECT BINARY data FROM added_repos WHERE repo_key = ?";
const ADDED_REPOS_CREATE_SQL: &str = "CREATE TABLE IF NOT EXISTS added_repos (
    repo_key VARBINARY(512) NOT NULL,
    data LONGTEXT NOT NULL,
    PRIMARY KEY (repo_key)
)";
const ADDED_REPOS_BINARY_KEY_MIGRATION_SQL: &str =
    "ALTER TABLE added_repos MODIFY COLUMN repo_key VARBINARY(512) NOT NULL";

/// Reject a value that wouldn't fit a VARCHAR column, so MySQL never silently
/// truncates a key (which would merge two distinct repos/branches into one row).
fn check_len(field: &str, value: &str, max: usize) -> Result<()> {
    if value.len() > max {
        anyhow::bail!(
            "{field} is too long for MySQL ({} bytes, max {max}): {value:?}",
            value.len()
        );
    }
    Ok(())
}

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

    /// Acquire the connection used for a MySQL named admission lock.
    ///
    /// Named locks belong to the server session, not to a transaction. Arm the
    /// pooled connection for closing before the first query that can acquire
    /// the lock. From that point onward every return, query error, panic, or
    /// task cancellation closes the session instead of returning a potentially
    /// locked connection to the pool.
    async fn acquire_admission_lock(
        &self,
        repo_key: &str,
        operation: &str,
    ) -> Result<PoolConnection<MySql>> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .with_context(|| format!("acquire admission {operation} connection"))?;
        conn.close_on_drop();
        let locked: Option<i64> = sqlx::query_scalar(ADMISSION_INSERT_LOCK_SQL)
            .bind(repo_key)
            .fetch_one(&mut *conn)
            .await
            .with_context(|| format!("lock added repo key for {operation}"))?;
        anyhow::ensure!(
            locked == Some(1),
            "timed out locking added repo key for {operation}"
        );
        Ok(conn)
    }

    async fn release_admission_lock(
        conn: &mut PoolConnection<MySql>,
        repo_key: &str,
        operation: &str,
    ) -> Result<()> {
        let released: Option<i64> = sqlx::query_scalar(ADMISSION_RELEASE_LOCK_SQL)
            .bind(repo_key)
            .fetch_one(&mut **conn)
            .await
            .with_context(|| format!("unlock added repo key after {operation}"))?;
        anyhow::ensure!(
            released == Some(1),
            "admission {operation} session did not own the named lock"
        );
        Ok(())
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
                generation BIGINT,
                data LONGTEXT NOT NULL,
                PRIMARY KEY (repo_key, branch)
            )",
        )
        .execute(&self.pool)
        .await
        .context("create refs table")?;
        // Index for commit-keyed reuse (get_by_commit). MySQL has no
        // `CREATE INDEX IF NOT EXISTS`, so create it only when absent — keeping
        // init() idempotent.
        let index_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.statistics
             WHERE table_schema = DATABASE() AND table_name = 'refs'
               AND index_name = 'idx_refs_commit'",
        )
        .fetch_one(&self.pool)
        .await
        .context("check refs commit index")?;
        if index_exists == 0 {
            sqlx::query("CREATE INDEX idx_refs_commit ON refs (repo_key, commit_id)")
                .execute(&self.pool)
                .await
                .context("create refs commit index")?;
        }
        // Add the generation column to a table created before it existed. MySQL 8
        // has no ADD COLUMN IF NOT EXISTS, so this is best-effort: it errors with
        // a duplicate-column code on an up-to-date table, which we ignore.
        let _ = sqlx::raw_sql("ALTER TABLE refs ADD COLUMN generation BIGINT")
            .execute(&self.pool)
            .await;
        sqlx::raw_sql(ADDED_REPOS_CREATE_SQL)
            .execute(&self.pool)
            .await
            .context("create added_repos table")?;
        let binary_key_sql = "SELECT count(*) FROM information_schema.columns
             WHERE table_schema=DATABASE() AND table_name='added_repos'
               AND column_name='repo_key' AND data_type='varbinary'
               AND character_maximum_length=512 AND collation_name IS NULL";
        let mut binary_key: i64 = sqlx::query_scalar(binary_key_sql)
            .fetch_one(&self.pool)
            .await
            .context("inspect added repo key equality")?;
        if binary_key == 0 {
            // GET_LOCK hashes the raw repo-key bytes. The table key must use the
            // same equality relation; a case-insensitive or PAD SPACE VARCHAR
            // key lets DB-equal spellings acquire different named locks.
            let exact_collisions: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM (
                    SELECT CAST(repo_key AS BINARY) AS exact_key
                    FROM added_repos
                    GROUP BY exact_key
                    HAVING count(*) > 1
                ) AS collisions",
            )
            .fetch_one(&self.pool)
            .await
            .context("check byte-exact added repo key collisions")?;
            anyhow::ensure!(
                exact_collisions == 0,
                "added_repos contains byte-identical key collisions"
            );
            sqlx::raw_sql(ADDED_REPOS_BINARY_KEY_MIGRATION_SQL)
                .execute(&self.pool)
                .await
                .context("migrate added_repos.repo_key to byte-exact equality")?;
            binary_key = sqlx::query_scalar(binary_key_sql)
                .fetch_one(&self.pool)
                .await
                .context("validate byte-exact added repo key")?;
        }
        anyhow::ensure!(binary_key == 1, "added_repos.repo_key is not byte-exact");
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
        // The key columns are VARCHAR (the composite PK can't be TEXT). Reject an
        // over-long key instead of letting MySQL silently truncate it, which would
        // collide two distinct repos/branches onto one row.
        check_len("repo_key", repo_key, 512)?;
        check_len("branch", branch, 255)?;
        check_len("commit_id", commit_id, 64)?;
        // MySQL's ON DUPLICATE KEY UPDATE has no WHERE clause, so the ordering
        // decision is computed once into the session variable `@ripl` in the
        // first (data) assignment — while the other columns still hold their
        // original values — then reused for the remaining columns. The
        // assignments evaluate left-to-right, so `data` must come first or the
        // condition would read already-overwritten columns. `@ripl` is set and
        // read within this one statement, so the connection pool can't leak it
        // across calls. Policy is identical to the sqlite adapter's WHERE.
        sqlx::query(
            "INSERT INTO refs (repo_key, branch, commit_id, synced_at, generation, data)
             VALUES (?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                 data = IF(@ripl := (VALUES(commit_id) = commit_id
                                     OR (generation IS NOT NULL AND VALUES(generation) IS NOT NULL
                                         AND VALUES(generation) >= generation)
                                     OR ((generation IS NULL OR VALUES(generation) IS NULL)
                                         AND (synced_at IS NULL OR VALUES(synced_at) IS NULL
                                              OR VALUES(synced_at) >= synced_at))),
                           VALUES(data), data),
                 commit_id = IF(@ripl, VALUES(commit_id), commit_id),
                 synced_at = IF(@ripl, VALUES(synced_at), synced_at),
                 generation = IF(@ripl, VALUES(generation), generation)",
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
        check_len("repo_key", repo_key, 512)?;
        check_len("branch", branch, 255)?;
        check_len("commit_id", expected_commit, 64)?;
        let result = sqlx::query(
            "UPDATE refs SET data = ?
             WHERE repo_key = ? AND branch = ? AND commit_id = ? AND data = ?",
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
        let rows = sqlx::query("SELECT branch FROM refs WHERE repo_key = ?")
            .bind(repo_key)
            .fetch_all(&self.pool)
            .await
            .context("list branches")?;
        rows.iter().map(|r| Ok(r.try_get(0)?)).collect()
    }

    async fn add_repo(&self, repo_key: &str, data: &str) -> Result<()> {
        check_len("repo_key", repo_key, 512)?;
        sqlx::query(
            "INSERT INTO added_repos (repo_key, data) VALUES (?, ?)
             ON DUPLICATE KEY UPDATE data = VALUES(data)",
        )
        .bind(repo_key)
        .bind(data)
        .execute(&self.pool)
        .await
        .context("add repo")?;
        Ok(())
    }

    async fn insert_added_repo(&self, repo_key: &str, data: &str) -> Result<bool> {
        check_len("repo_key", repo_key, 512)?;
        // rows_affected is not a reliable insert discriminator when the client
        // enables CLIENT_FOUND_ROWS. Serialize this key explicitly and inspect
        // existence on the same connection instead.
        let mut conn = self.acquire_admission_lock(repo_key, "insert").await?;
        let exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM added_repos WHERE repo_key = ?")
                .bind(repo_key)
                .fetch_optional(&mut *conn)
                .await
                .context("check added repo existence")?;
        let inserted = if exists.is_none() {
            sqlx::query("INSERT INTO added_repos (repo_key, data) VALUES (?, ?)")
                .bind(repo_key)
                .bind(data)
                .execute(&mut *conn)
                .await
                .context("insert added repo")?;
            true
        } else {
            false
        };
        Self::release_admission_lock(&mut conn, repo_key, "insert").await?;
        Ok(inserted)
    }

    async fn compare_and_swap_added_repo(
        &self,
        repo_key: &str,
        expected_data: &str,
        new_data: &str,
    ) -> Result<bool> {
        check_len("repo_key", repo_key, 512)?;
        let mut conn = self.acquire_admission_lock(repo_key, "CAS").await?;
        let current: Option<Vec<u8>> = sqlx::query_scalar(ADMISSION_CURRENT_BYTES_SQL)
            .bind(repo_key)
            .fetch_optional(&mut *conn)
            .await
            .context("read byte-exact added repo value")?;
        let matches = current.as_deref() == Some(expected_data.as_bytes());
        if matches {
            sqlx::query("UPDATE added_repos SET data = ? WHERE repo_key = ?")
                .bind(new_data)
                .bind(repo_key)
                .execute(&mut *conn)
                .await
                .context("CAS added repo")?;
        }
        Self::release_admission_lock(&mut conn, repo_key, "CAS").await?;
        Ok(matches)
    }

    async fn get_added_repo(&self, repo_key: &str) -> Result<Option<String>> {
        check_len("repo_key", repo_key, 512)?;
        sqlx::query_scalar("SELECT data FROM added_repos WHERE repo_key = ?")
            .bind(repo_key)
            .fetch_optional(&self.pool)
            .await
            .context("get added repo")
    }

    async fn remove_added_repo(&self, repo_key: &str) -> Result<()> {
        check_len("repo_key", repo_key, 512)?;
        sqlx::query("DELETE FROM added_repos WHERE repo_key = ?")
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
            .context("mysql metadata health")?;
        Ok(())
    }
}

#[cfg(test)]
mod admission_sql_tests {
    use super::*;

    #[test]
    fn admission_insert_does_not_depend_on_affected_rows_semantics() {
        assert!(ADMISSION_INSERT_LOCK_SQL.contains("GET_LOCK"));
        assert!(!ADMISSION_INSERT_LOCK_SQL.contains("ON DUPLICATE KEY"));
        assert!(ADDED_REPOS_CREATE_SQL.contains("VARBINARY(512)"));
        assert!(ADDED_REPOS_BINARY_KEY_MIGRATION_SQL.contains("VARBINARY(512)"));
    }

    #[test]
    fn admission_cas_is_byte_exact_not_collation_equal() {
        assert!(ADMISSION_CURRENT_BYTES_SQL.contains("BINARY data"));
    }

    #[tokio::test]
    async fn mysql_case_variant_admissions_use_distinct_keys() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL") else {
            eprintln!(
                "SKIP mysql_case_variant_admissions_use_distinct_keys: RIPCLONE_TEST_MYSQL_URL unset"
            );
            return;
        };
        let meta = MysqlMeta::connect(&url).await.unwrap();
        meta.init().await.unwrap();
        let suffix = hex::encode(rand::random::<[u8; 8]>());
        let lower = format!("github:admission/case-{suffix}");
        let upper = format!("github:admission/CASE-{suffix}");
        let (a, b) = tokio::join!(
            meta.insert_added_repo(&lower, r#"{"attempt":"lower"}"#),
            meta.insert_added_repo(&upper, r#"{"attempt":"upper"}"#),
        );
        assert!(a.unwrap() && b.unwrap());
        assert_ne!(
            meta.get_added_repo(&lower).await.unwrap(),
            meta.get_added_repo(&upper).await.unwrap()
        );
        meta.remove_added_repo(&lower).await.unwrap();
        meta.remove_added_repo(&upper).await.unwrap();
    }

    #[tokio::test]
    async fn mysql_named_lock_session_closes_on_error_release_failure_and_cancellation() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL") else {
            eprintln!(
                "SKIP mysql_named_lock_session_closes_on_error_release_failure_and_cancellation: RIPCLONE_TEST_MYSQL_URL unset"
            );
            return;
        };
        let meta = MysqlMeta::connect(&url).await.unwrap();
        let key = format!(
            "github:admission/lock-{}",
            hex::encode(rand::random::<[u8; 8]>())
        );

        let mut failed = meta
            .acquire_admission_lock(&key, "error-test")
            .await
            .unwrap();
        sqlx::query("THIS IS NOT VALID SQL")
            .execute(&mut *failed)
            .await
            .unwrap_err();
        drop(failed);
        let mut recovered = meta
            .acquire_admission_lock(&key, "error-recovery")
            .await
            .unwrap();
        MysqlMeta::release_admission_lock(&mut recovered, &key, "error-recovery")
            .await
            .unwrap();

        let mut wrong_release = meta
            .acquire_admission_lock(&key, "release-failure-test")
            .await
            .unwrap();
        assert!(
            MysqlMeta::release_admission_lock(
                &mut wrong_release,
                "a-different-lock-name",
                "release-failure-test",
            )
            .await
            .is_err()
        );
        drop(wrong_release);
        let mut recovered = meta
            .acquire_admission_lock(&key, "release-failure-recovery")
            .await
            .unwrap();
        MysqlMeta::release_admission_lock(&mut recovered, &key, "release-failure-recovery")
            .await
            .unwrap();

        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
        let cancellation_meta = MysqlMeta::connect(&url).await.unwrap();
        let cancellation_key = key.clone();
        let task = tokio::spawn(async move {
            let _locked = cancellation_meta
                .acquire_admission_lock(&cancellation_key, "cancellation-test")
                .await
                .unwrap();
            let _ = acquired_tx.send(());
            std::future::pending::<()>().await;
        });
        acquired_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        let mut recovered = meta
            .acquire_admission_lock(&key, "cancellation-recovery")
            .await
            .unwrap();
        MysqlMeta::release_admission_lock(&mut recovered, &key, "cancellation-recovery")
            .await
            .unwrap();
    }
}
