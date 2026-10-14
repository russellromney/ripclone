//! [`ControlStore`] over one local SQLite file.
//!
//! SQL stays in the portable subset: `INSERT ... ON CONFLICT DO NOTHING`,
//! `UPDATE ... RETURNING`, integer epoch seconds, outputs as JSON text. Only
//! `open` (schema inspection, WAL) and `BEGIN IMMEDIATE` are SQLite-specific.
//! Claims rely on SQLite's single writer; another backend would add
//! `FOR UPDATE SKIP LOCKED` to the claim subquery.

use super::{
    AddedRepo, BuildRow, Claim, Clock, ControlPathLock, ControlStore, Out, Output, Pool, Published,
};
use crate::provider::RepoId;
use crate::repo_config::RepoConfig;
use crate::{ExactResultKind, FullResult};
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqlitePool, SqliteRow};
use sqlx::{Connection, Row};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const SCHEMA: &str = include_str!("schema/sqlite.sql");
const SCHEMA_VERSION: i64 = 1;

const COLUMNS: &str = "repo, commit_id, ref_name, parent, created_at, \
    head_state, head_output, head_error, files_state, files_output, files_error, \
    full_state, full_output, full_error, attempts, lease_until";

pub struct SqliteControlStore {
    pool: SqlitePool,
    max_attempts: i64,
    clock: Arc<dyn Clock>,
    /// The database path and the file it named at open. The pool keeps the
    /// opened file even after the path is deleted or replaced, so `health`
    /// compares the two.
    path: PathBuf,
    file: FileId,
    _ownership: ControlPathLock,
}

/// A file's device and inode.
type FileId = (u64, u64);

#[cfg(unix)]
fn file_id(path: &Path) -> std::io::Result<FileId> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path)?;
    Ok((metadata.dev(), metadata.ino()))
}

// `ControlPathLock::acquire` refuses to open a store off Unix.
#[cfg(not(unix))]
fn file_id(path: &Path) -> std::io::Result<FileId> {
    std::fs::metadata(path).map(|_| (0, 0))
}

impl SqliteControlStore {
    /// Open or create the control database at `path`. Holds the path's
    /// ownership lock for the store's lifetime and refuses a database that has
    /// tables but no matching `schema_version` row, without changing it.
    pub async fn open(path: &Path, max_attempts: i64, clock: Arc<dyn Clock>) -> Result<Self> {
        ensure!(max_attempts >= 1, "max build attempts must be at least 1");
        let ownership = ControlPathLock::acquire(path)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create control directory {}", parent.display()))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5));
        // Check or create the schema and switch to WAL on one connection
        // before the pool opens more: the WAL switch needs to be alone.
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .with_context(|| format!("open control database {}", path.display()))?;
        initialize(&mut connection, path).await?;
        let mode: String = sqlx::query_scalar("PRAGMA journal_mode = WAL")
            .fetch_one(&mut connection)
            .await
            .context("enable WAL on control database")?;
        ensure!(
            mode.eq_ignore_ascii_case("wal"),
            "control database {} stayed in journal mode {mode}",
            path.display()
        );
        connection
            .close()
            .await
            .context("close control database setup connection")?;
        let pool = SqlitePool::connect_with(options)
            .await
            .with_context(|| format!("open control database {}", path.display()))?;
        let file =
            file_id(path).with_context(|| format!("stat control database {}", path.display()))?;
        Ok(Self {
            pool,
            max_attempts,
            clock,
            path: path.to_path_buf(),
            file,
            _ownership: ownership,
        })
    }

    #[cfg(test)]
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

async fn initialize(connection: &mut SqliteConnection, path: &Path) -> Result<()> {
    let mut tx = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .context("begin control schema check")?;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(&mut *tx)
    .await
    .context("inspect control schema")?;
    if tables.is_empty() {
        // `Executor::execute` rather than `RawSql::execute`: the latter's
        // future is not provably `Send` (rustc higher-ranked lifetime bug).
        sqlx::Executor::execute(&mut *tx, sqlx::raw_sql(SCHEMA))
            .await
            .context("create control schema")?;
        sqlx::query("INSERT INTO schema_version (id, version) VALUES (1, $1)")
            .bind(SCHEMA_VERSION)
            .execute(&mut *tx)
            .await
            .context("write control schema version")?;
    } else {
        let version: Option<i64> = if tables.iter().any(|name| name == "schema_version") {
            sqlx::query_scalar("SELECT version FROM schema_version WHERE id = 1")
                .fetch_optional(&mut *tx)
                .await
                .context("read control schema version")?
        } else {
            None
        };
        if version != Some(SCHEMA_VERSION) {
            bail!(
                "incompatible control database {}: expected schema version {SCHEMA_VERSION}, \
                 found {version:?} (tables: {}); there is no migration, delete the control \
                 database to start fresh",
                path.display(),
                tables.join(", ")
            );
        }
    }
    tx.commit().await.context("commit control schema check")
}

fn column_prefix(kind: ExactResultKind) -> &'static str {
    match kind {
        ExactResultKind::Head => "head",
        ExactResultKind::Files => "files",
        ExactResultKind::Full => "full",
    }
}

fn duration_secs(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

fn decode_out<T: serde::de::DeserializeOwned>(
    row: &SqliteRow,
    kind: ExactResultKind,
) -> Result<Out<T>> {
    let prefix = column_prefix(kind);
    let state: String = row.try_get(format!("{prefix}_state").as_str())?;
    let output: Option<String> = row.try_get(format!("{prefix}_output").as_str())?;
    let error: Option<String> = row.try_get(format!("{prefix}_error").as_str())?;
    Ok(match state.as_str() {
        "pending" => Out::Pending { error },
        "ready" => {
            let output = output.with_context(|| format!("ready {kind} output is missing"))?;
            Out::Ready(
                serde_json::from_str(&output)
                    .with_context(|| format!("decode stored {kind} output"))?,
            )
        }
        "failed" => Out::Failed(error.unwrap_or_else(|| format!("{kind} build failed"))),
        other => bail!("unknown {kind} state {other:?}"),
    })
}

fn decode_build(row: &SqliteRow) -> Result<BuildRow> {
    Ok(BuildRow {
        commit: row.try_get("commit_id")?,
        ref_name: row.try_get("ref_name")?,
        parent: row.try_get("parent")?,
        created_at: row.try_get("created_at")?,
        head: decode_out(row, ExactResultKind::Head)?,
        files: decode_out(row, ExactResultKind::Files)?,
        full: decode_out(row, ExactResultKind::Full)?,
        attempts: row.try_get("attempts")?,
        lease_until: row.try_get("lease_until")?,
    })
}

fn decode_added_repo(row: &SqliteRow) -> Result<AddedRepo> {
    let added_at: i64 = row.try_get("added_at")?;
    Ok(AddedRepo {
        repo_id: RepoId::from_storage_key(&row.try_get::<String, _>("repo")?)?,
        added_at: u64::try_from(added_at).unwrap_or(0),
    })
}

impl SqliteControlStore {
    async fn select_build(&self, repo: &RepoId, commit: &str) -> Result<Option<BuildRow>> {
        sqlx::query(&format!(
            "SELECT {COLUMNS} FROM builds WHERE repo = $1 AND commit_id = $2"
        ))
        .bind(repo.storage_key())
        .bind(commit)
        .fetch_optional(&self.pool)
        .await
        .context("read build")?
        .as_ref()
        .map(decode_build)
        .transpose()
    }
}

#[async_trait]
impl ControlStore for SqliteControlStore {
    async fn admit(
        &self,
        repo: &RepoId,
        commit: &str,
        ref_name: Option<&str>,
    ) -> Result<(BuildRow, bool)> {
        crate::validation::validate_object_id(commit).context("validate admitted commit")?;
        if let Some(ref_name) = ref_name {
            ensure!(
                ref_name.starts_with("refs/"),
                "admitted ref {ref_name:?} is not a full ref name"
            );
        }
        let key = repo.storage_key();
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .context("begin admit")?;
        let inserted = sqlx::query(
            "INSERT INTO builds (repo, commit_id, ref_name, created_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT (repo, commit_id) DO NOTHING",
        )
        .bind(&key)
        .bind(commit)
        .bind(ref_name)
        .bind(self.clock.now())
        .execute(&mut *tx)
        .await
        .context("insert build")?
        .rows_affected()
            == 1;
        if !inserted && ref_name.is_some() {
            sqlx::query(
                "UPDATE builds SET ref_name = $1
                 WHERE repo = $2 AND commit_id = $3 AND ref_name IS NULL",
            )
            .bind(ref_name)
            .bind(&key)
            .bind(commit)
            .execute(&mut *tx)
            .await
            .context("fill build ref name")?;
        }
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM builds WHERE repo = $1 AND commit_id = $2"
        ))
        .bind(&key)
        .bind(commit)
        .fetch_one(&mut *tx)
        .await
        .context("read admitted build")?;
        tx.commit().await.context("commit admit")?;
        Ok((decode_build(&row)?, inserted))
    }

    async fn retry(&self, repo: &RepoId, commit: &str) -> Result<Option<BuildRow>> {
        let row = sqlx::query(&format!(
            "UPDATE builds SET
               head_state = CASE WHEN head_state = 'failed' THEN 'pending' ELSE head_state END,
               head_error = CASE WHEN head_state = 'failed' THEN NULL ELSE head_error END,
               files_state = CASE WHEN files_state = 'failed' THEN 'pending' ELSE files_state END,
               files_error = CASE WHEN files_state = 'failed' THEN NULL ELSE files_error END,
               full_state = CASE WHEN full_state = 'failed' THEN 'pending' ELSE full_state END,
               full_error = CASE WHEN full_state = 'failed' THEN NULL ELSE full_error END,
               attempts = 0,
               retry_after = 0
             WHERE repo = $1 AND commit_id = $2
               AND (head_state = 'failed' OR files_state = 'failed' OR full_state = 'failed')
             RETURNING {COLUMNS}"
        ))
        .bind(repo.storage_key())
        .bind(commit)
        .fetch_optional(&self.pool)
        .await
        .context("retry build")?;
        match row {
            Some(row) => Ok(Some(decode_build(&row)?)),
            None => self.select_build(repo, commit).await,
        }
    }

    async fn claim(&self, owner: &str, pool: Pool, lease: Duration) -> Result<Option<Claim>> {
        let now = self.clock.now();
        let predicate = match pool {
            Pool::Head => "head_state = 'pending'",
            Pool::Background => {
                "head_state = 'ready' AND (files_state = 'pending' OR full_state = 'pending')"
            }
        };
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .context("begin claim")?;
        // Give up on rows whose last allowed attempt ended without a result:
        // every pending output fails (a pending Head takes Files and Full with
        // it) and the lease is cleared so a late publish cannot land.
        sqlx::query(
            "UPDATE builds SET
               head_state = CASE WHEN head_state = 'pending' THEN 'failed' ELSE head_state END,
               head_error = CASE WHEN head_state = 'pending'
                 THEN $1 || COALESCE(': ' || head_error, '') ELSE head_error END,
               files_state = CASE WHEN files_state = 'pending' THEN 'failed' ELSE files_state END,
               files_error = CASE
                 WHEN files_state <> 'pending' THEN files_error
                 WHEN head_state = 'pending' THEN 'head failed: ' || $1 || COALESCE(': ' || head_error, '')
                 ELSE $1 || COALESCE(': ' || files_error, '') END,
               full_state = CASE WHEN full_state = 'pending' THEN 'failed' ELSE full_state END,
               full_error = CASE
                 WHEN full_state <> 'pending' THEN full_error
                 WHEN head_state = 'pending' THEN 'head failed: ' || $1 || COALESCE(': ' || head_error, '')
                 ELSE $1 || COALESCE(': ' || full_error, '') END,
               lease_owner = NULL,
               lease_until = NULL
             WHERE attempts >= $2
               AND (lease_until IS NULL OR lease_until <= $3)
               AND (head_state = 'pending' OR files_state = 'pending' OR full_state = 'pending')",
        )
        .bind(format!("gave up after {} attempts", self.max_attempts))
        .bind(self.max_attempts)
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("give up exhausted builds")?;
        let row = sqlx::query(&format!(
            "UPDATE builds SET lease_owner = $1, lease_until = $2, attempts = attempts + 1
             WHERE (repo, commit_id) = (
               SELECT repo, commit_id FROM builds
               WHERE {predicate}
                 AND (lease_until IS NULL OR lease_until <= $3)
                 AND retry_after <= $3
                 AND attempts < $4
               ORDER BY created_at, repo, commit_id
               LIMIT 1)
             RETURNING {COLUMNS}"
        ))
        .bind(owner)
        .bind(now.saturating_add(duration_secs(lease)))
        .bind(now)
        .bind(self.max_attempts)
        .fetch_optional(&mut *tx)
        .await
        .context("claim build")?;
        tx.commit().await.context("commit claim")?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(Claim {
            repo: RepoId::from_storage_key(&row.try_get::<String, _>("repo")?)?,
            owner: owner.to_string(),
            build: decode_build(&row)?,
        }))
    }

    async fn renew(&self, claim: &Claim, lease: Duration) -> Result<bool> {
        let renewed = sqlx::query(
            "UPDATE builds SET lease_until = $1
             WHERE repo = $2 AND commit_id = $3 AND lease_owner = $4",
        )
        .bind(self.clock.now().saturating_add(duration_secs(lease)))
        .bind(claim.repo.storage_key())
        .bind(&claim.build.commit)
        .bind(&claim.owner)
        .execute(&self.pool)
        .await
        .context("renew build lease")?
        .rows_affected();
        Ok(renewed == 1)
    }

    async fn publish(&self, claim: &Claim, output: Output) -> Result<Published> {
        let commit = &claim.build.commit;
        let (kind, clonepack, data, parent) = match &output {
            Output::Head { result, parent } => (
                ExactResultKind::Head,
                &result.clonepack,
                serde_json::to_string(result),
                parent.as_deref(),
            ),
            Output::Files(result) => (
                ExactResultKind::Files,
                &result.clonepack,
                serde_json::to_string(result),
                None,
            ),
            Output::Full(result) => (
                ExactResultKind::Full,
                &result.clonepack,
                serde_json::to_string(result),
                None,
            ),
        };
        ensure!(
            crate::exact_output_artifacts_ready(commit, kind, clonepack),
            "invalid {kind} result for {commit}"
        );
        let data = data.with_context(|| format!("encode {kind} output"))?;
        let prefix = column_prefix(kind);
        let head_only = if kind == ExactResultKind::Head {
            ", parent = $5, attempts = 0"
        } else {
            ""
        };
        let sql = format!(
            "UPDATE builds SET {prefix}_state = 'ready', {prefix}_output = $1,
               {prefix}_error = NULL{head_only}
             WHERE repo = $2 AND commit_id = $3 AND lease_owner = $4
               AND {prefix}_state <> 'ready'
             RETURNING head_state, files_state, full_state"
        );
        let mut query = sqlx::query_as::<_, (String, String, String)>(&sql)
            .bind(data)
            .bind(claim.repo.storage_key())
            .bind(commit)
            .bind(&claim.owner);
        if kind == ExactResultKind::Head {
            query = query.bind(parent);
        }
        let states = query
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("publish {kind} output"))?;
        Ok(match states {
            Some((head, files, full)) => Published {
                applied: true,
                row_complete: head == "ready" && files == "ready" && full == "ready",
            },
            None => Published {
                applied: false,
                row_complete: false,
            },
        })
    }

    async fn fail(
        &self,
        claim: &Claim,
        kind: ExactResultKind,
        error: &str,
        retry_after: Option<Duration>,
    ) -> Result<()> {
        let prefix = column_prefix(kind);
        // `$4` is the retry time, NULL for a permanent failure. The output
        // fails when there is no retry or no attempt left.
        let gives_up = "($4 IS NULL OR attempts >= $5)";
        // A Head that fails takes the pending Files and Full with it.
        let cascade = if kind == ExactResultKind::Head {
            format!(
                ", files_state = CASE WHEN {gives_up} AND files_state = 'pending'
                     THEN 'failed' ELSE files_state END,
                   files_error = CASE WHEN {gives_up} AND files_state = 'pending'
                     THEN $7 ELSE files_error END,
                   full_state = CASE WHEN {gives_up} AND full_state = 'pending'
                     THEN 'failed' ELSE full_state END,
                   full_error = CASE WHEN {gives_up} AND full_state = 'pending'
                     THEN $7 ELSE full_error END"
            )
        } else {
            String::new()
        };
        let sql = format!(
            "UPDATE builds SET
               {prefix}_state = CASE WHEN {gives_up} THEN 'failed' ELSE 'pending' END,
               {prefix}_error = $1,
               retry_after = CASE WHEN {gives_up} THEN retry_after ELSE $4 END{cascade}
             WHERE repo = $2 AND commit_id = $3 AND lease_owner = $6
               AND {prefix}_state = 'pending'"
        );
        let mut query = sqlx::query(&sql)
            .bind(error)
            .bind(claim.repo.storage_key())
            .bind(&claim.build.commit)
            .bind(retry_after.map(|delay| self.clock.now().saturating_add(duration_secs(delay))))
            .bind(self.max_attempts)
            .bind(&claim.owner);
        if kind == ExactResultKind::Head {
            query = query.bind(format!("head failed: {error}"));
        }
        query
            .execute(&self.pool)
            .await
            .with_context(|| format!("record {kind} failure"))?;
        Ok(())
    }

    async fn release(&self, claim: &Claim) -> Result<()> {
        sqlx::query(
            "UPDATE builds SET lease_owner = NULL, lease_until = NULL
             WHERE repo = $1 AND commit_id = $2 AND lease_owner = $3",
        )
        .bind(claim.repo.storage_key())
        .bind(&claim.build.commit)
        .bind(&claim.owner)
        .execute(&self.pool)
        .await
        .context("release build lease")?;
        Ok(())
    }

    async fn reset_leases(&self) -> Result<u64> {
        Ok(sqlx::query(
            "UPDATE builds SET lease_owner = NULL, lease_until = NULL
             WHERE lease_owner IS NOT NULL",
        )
        .execute(&self.pool)
        .await
        .context("reset build leases")?
        .rows_affected())
    }

    async fn status(&self, repo: &RepoId, commit: &str) -> Result<Option<BuildRow>> {
        self.select_build(repo, commit).await
    }

    async fn list_builds(&self, repo: &RepoId) -> Result<Vec<BuildRow>> {
        sqlx::query(&format!(
            "SELECT {COLUMNS} FROM builds WHERE repo = $1 ORDER BY created_at, commit_id"
        ))
        .bind(repo.storage_key())
        .fetch_all(&self.pool)
        .await
        .context("list builds")?
        .iter()
        .map(decode_build)
        .collect()
    }

    async fn latest_ready_full(
        &self,
        repo: &RepoId,
        ref_name: &str,
    ) -> Result<Option<(String, FullResult)>> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT commit_id, full_output FROM builds
             WHERE repo = $1 AND ref_name = $2 AND full_state = 'ready'
             ORDER BY created_at DESC, commit_id DESC
             LIMIT 1",
        )
        .bind(repo.storage_key())
        .bind(ref_name)
        .fetch_optional(&self.pool)
        .await
        .context("read latest ready Full")?;
        row.map(|(commit, output)| {
            let full = serde_json::from_str(&output).context("decode stored full output")?;
            Ok((commit, full))
        })
        .transpose()
    }

    async fn add_repo(&self, repo: &RepoId) -> Result<bool> {
        let added_at = self.clock.now();
        Ok(sqlx::query(
            "INSERT INTO added_repos (repo, added_at) VALUES ($1, $2)
             ON CONFLICT (repo) DO NOTHING",
        )
        .bind(repo.storage_key())
        .bind(added_at)
        .execute(&self.pool)
        .await
        .context("add repository")?
        .rows_affected()
            == 1)
    }

    async fn remove_repo(&self, repo: &RepoId) -> Result<bool> {
        Ok(sqlx::query("DELETE FROM added_repos WHERE repo = $1")
            .bind(repo.storage_key())
            .execute(&self.pool)
            .await
            .context("remove repository")?
            .rows_affected()
            == 1)
    }

    async fn get_repo(&self, repo: &RepoId) -> Result<Option<AddedRepo>> {
        sqlx::query("SELECT repo, added_at FROM added_repos WHERE repo = $1")
            .bind(repo.storage_key())
            .fetch_optional(&self.pool)
            .await
            .context("read repository")?
            .as_ref()
            .map(decode_added_repo)
            .transpose()
    }

    async fn list_repos(&self) -> Result<Vec<AddedRepo>> {
        sqlx::query("SELECT repo, added_at FROM added_repos ORDER BY repo")
            .fetch_all(&self.pool)
            .await
            .context("list repositories")?
            .iter()
            .map(decode_added_repo)
            .collect()
    }

    async fn get_config(&self, repo: &RepoId) -> Result<Option<RepoConfig>> {
        let key = repo.storage_key();
        let data: Option<String> =
            sqlx::query_scalar("SELECT data FROM repo_config WHERE repo = $1")
                .bind(&key)
                .fetch_optional(&self.pool)
                .await
                .with_context(|| format!("read repository config {key}"))?;
        let Some(data) = data else {
            return Ok(None);
        };
        let config: RepoConfig = serde_json::from_str(&data)
            .with_context(|| format!("decode repository config {key}"))?;
        config
            .validate()
            .with_context(|| format!("validate repository config {key}"))?;
        Ok(Some(config))
    }

    async fn set_config(&self, repo: &RepoId, config: &RepoConfig) -> Result<()> {
        let key = repo.storage_key();
        config
            .validate()
            .with_context(|| format!("validate repository config {key}"))?;
        let data = serde_json::to_string(config).context("encode repository config")?;
        sqlx::query(
            "INSERT INTO repo_config (repo, data) VALUES ($1, $2)
             ON CONFLICT (repo) DO UPDATE SET data = excluded.data",
        )
        .bind(&key)
        .bind(data)
        .execute(&self.pool)
        .await
        .with_context(|| format!("write repository config {key}"))?;
        Ok(())
    }

    async fn health(&self) -> Result<()> {
        match file_id(&self.path) {
            Ok(file) => ensure!(
                file == self.file,
                "control database {} was replaced after the server opened it",
                self.path.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                bail!(
                    "control database {} was deleted after the server opened it",
                    self.path.display()
                )
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("stat control database {}", self.path.display()));
            }
        }
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("control store health")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::conformance::{self, Backend, ManualClock};

    struct SqliteBackend {
        dir: tempfile::TempDir,
    }

    #[async_trait]
    impl Backend for SqliteBackend {
        async fn open(
            &self,
            clock: Arc<ManualClock>,
            max_attempts: i64,
        ) -> Result<Arc<dyn ControlStore>> {
            let path = self.dir.path().join("control.db");
            Ok(Arc::new(
                SqliteControlStore::open(&path, max_attempts, clock).await?,
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_passes_conformance() {
        conformance::run_all(|| SqliteBackend {
            dir: tempfile::tempdir().unwrap(),
        })
        .await;
    }

    async fn open(path: &Path) -> Result<SqliteControlStore> {
        SqliteControlStore::open(path, 3, Arc::new(crate::control::SystemClock)).await
    }

    #[tokio::test]
    async fn health_fails_when_the_database_file_is_deleted_or_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.db");
        let store = open(&path).await.unwrap();
        store.health().await.unwrap();

        let moved = dir.path().join("moved.db");
        std::fs::rename(&path, &moved).unwrap();
        let error = store.health().await.unwrap_err().to_string();
        assert!(error.contains("was deleted"), "{error}");

        std::fs::write(&path, b"").unwrap();
        let error = store.health().await.unwrap_err().to_string();
        assert!(error.contains("was replaced"), "{error}");

        std::fs::rename(&moved, &path).unwrap();
        store.health().await.unwrap();
    }

    #[tokio::test]
    async fn stored_config_with_removed_keys_still_loads() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(&dir.path().join("control.db")).await.unwrap();
        let repo = RepoId::github("acme/old");
        sqlx::query("INSERT INTO repo_config (repo, data) VALUES ($1, $2)")
            .bind(repo.storage_key())
            .bind(
                r#"{"compression_level":9,"clonepack_depths":[{"name":"full"}],"archive_chunk_size":1024,"head_blobs_chunk_size":4194304}"#,
            )
            .execute(store.pool())
            .await
            .unwrap();
        let config = store.get_config(&repo).await.unwrap().unwrap();
        assert_eq!(config.compression_level(), 9);
    }

    #[tokio::test]
    async fn second_owner_is_rejected_while_first_is_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.db");
        let first = open(&path).await.unwrap();
        let error = open(&path).await.err().unwrap();
        assert!(
            error
                .to_string()
                .contains("already owned by another server")
        );
        drop(first);
        open(&path).await.unwrap();
    }

    #[tokio::test]
    async fn incompatible_database_is_refused_and_left_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        for (name, setup) in [
            ("other-tables", "CREATE TABLE jobs (id INTEGER PRIMARY KEY)"),
            (
                "wrong-version",
                "CREATE TABLE schema_version (id INTEGER PRIMARY KEY, version INTEGER NOT NULL);
                 INSERT INTO schema_version VALUES (1, 999)",
            ),
            (
                "no-version-row",
                "CREATE TABLE schema_version (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)",
            ),
        ] {
            let path = dir.path().join(format!("{name}.db"));
            {
                use sqlx::Connection;
                let options = SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true);
                let mut connection = sqlx::SqliteConnection::connect_with(&options)
                    .await
                    .unwrap();
                sqlx::raw_sql(setup).execute(&mut connection).await.unwrap();
                connection.close().await.unwrap();
            }
            let before = std::fs::read(&path).unwrap();
            let error = open(&path).await.err().unwrap().to_string();
            assert!(
                error.contains("incompatible control database"),
                "{name}: {error}"
            );
            assert!(
                error.contains("delete the control database"),
                "{name}: {error}"
            );
            assert_eq!(std::fs::read(&path).unwrap(), before, "{name}");
        }
    }

    #[tokio::test]
    async fn fresh_database_uses_wal_and_current_version() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(&dir.path().join("control.db")).await.unwrap();
        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(mode, "wal");
        let version: i64 = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn corrupt_or_invalid_config_fails_instead_of_defaulting() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(&dir.path().join("control.db")).await.unwrap();
        let repo = RepoId::github("acme/corrupt");
        for (data, expected) in [
            ("not-json", "decode repository config"),
            (r#"{"compression_level":99}"#, "validate repository config"),
        ] {
            sqlx::query(
                "INSERT INTO repo_config (repo, data) VALUES ($1, $2)
                 ON CONFLICT (repo) DO UPDATE SET data = excluded.data",
            )
            .bind(repo.storage_key())
            .bind(data)
            .execute(store.pool())
            .await
            .unwrap();
            let error = store.get_config(&repo).await.unwrap_err().to_string();
            assert!(error.contains(expected), "{data}: {error}");
        }
    }
}
