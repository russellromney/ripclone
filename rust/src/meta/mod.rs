//! SQL-backed metadata store: makes the ref/metadata layer (`RefStore`)
//! pluggable onto SQLite / Postgres / MySQL / libsql (Turso Cloud), so operators
//! can keep ripclone's per-repo/branch `RefInfo` in a database they already run
//! instead of files or S3.
//!
//! [`MetaDb`] is a tiny per-engine adapter that returns plain Rust types (no
//! engine types leak); [`SqlRefStore`] holds one and implements the existing
//! [`RefStore`](crate::ref_store::RefStore) trait, owning the `RefInfo`↔JSON
//! serialization and the save-ordering policy. The metadata is small (one JSON
//! row per repo/branch), so this mirrors the file/S3 stores closely.

use crate::RefInfo;
use crate::provider::RepoId;
use crate::ref_store::RefStore;
use anyhow::{Context, Result};
use async_trait::async_trait;
use tracing::warn;

pub mod libsql;
pub mod mysql;
pub mod postgres;
pub mod sqlite;

pub use libsql::LibsqlMeta;
pub use mysql::MysqlMeta;
pub use postgres::PostgresMeta;
pub use sqlite::SqliteMeta;

/// One stored ref row, decoded to plain types.
#[derive(Debug, Clone)]
pub struct RefRow {
    /// The `RefInfo` serialized as JSON.
    pub data: String,
    /// The ref's commit, duplicated out of the JSON for the save-ordering check.
    pub commit_id: String,
    /// `RefInfo.synced_at` (epoch secs), `None` when the ref has no timestamp.
    pub synced_at: Option<i64>,
}

/// Per-engine adapter over a `refs(repo_key, branch, commit_id, synced_at,
/// data)` table. `repo_key` is the repo's [`RepoId::storage_key`] (the
/// back-compat `owner/repo` for GitHub). Implemented by `SqliteMeta`,
/// `PostgresMeta`, `MysqlMeta`, `LibsqlMeta`.
#[async_trait]
pub trait MetaDb: Send + Sync {
    /// Create the `refs` table if absent.
    async fn init(&self) -> Result<()>;

    /// Fetch the row for one ref, if present.
    async fn get(&self, repo_key: &str, branch: &str) -> Result<Option<RefRow>>;

    /// All ref rows for this repo whose `commit_id` equals `commit` (one per
    /// branch sitting at that commit). Powers commit-keyed reuse: a sync of one
    /// branch can reuse a build another branch already produced for the same
    /// commit. The `commit_id` column already exists, so this needs no new write.
    async fn get_by_commit(&self, repo_key: &str, commit: &str) -> Result<Vec<RefRow>>;

    /// Insert or replace the row for one ref (unconditional upsert; the
    /// save-ordering decision is made by [`SqlRefStore`]).
    async fn upsert(
        &self,
        repo_key: &str,
        branch: &str,
        data: &str,
        commit_id: &str,
        synced_at: Option<i64>,
    ) -> Result<()>;

    /// Distinct `repo_key`s that have at least one stored ref.
    async fn list_repos(&self) -> Result<Vec<String>>;

    /// Branches with a stored ref for this repo.
    async fn list_branches(&self, repo_key: &str) -> Result<Vec<String>>;

    /// Cheap reachability probe for `/readyz`.
    async fn health(&self) -> Result<()>;
}

/// `RefStore` over a [`MetaDb`]. Wrap in
/// [`CachingRefStore`](crate::ref_store::CachingRefStore) for the read cache,
/// exactly like the file/S3 stores.
pub struct SqlRefStore {
    db: Box<dyn MetaDb>,
}

impl SqlRefStore {
    /// Wrap an engine adapter and run schema setup.
    pub async fn new(db: Box<dyn MetaDb>) -> Result<Self> {
        db.init().await?;
        Ok(Self { db })
    }
}

#[async_trait]
impl RefStore for SqlRefStore {
    async fn load(&self, repo_id: &RepoId) -> Result<Option<RefInfo>> {
        self.load_branch(repo_id, "HEAD").await
    }

    async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()> {
        self.save_branch(repo_id, "HEAD", info).await
    }

    async fn list(&self) -> Result<Vec<RepoId>> {
        // Phase 0 (mirrors the file/S3 stores): every stored key is a GitHub
        // repo, so the back-compat `owner/repo` key reconstructs a GitHub RepoId.
        Ok(self
            .db
            .list_repos()
            .await?
            .into_iter()
            .map(RepoId::github)
            .collect())
    }

    async fn load_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RefInfo>> {
        match self.db.get(&repo_id.storage_key(), branch).await? {
            Some(row) => Ok(Some(
                serde_json::from_str(&row.data).context("parse stored RefInfo")?,
            )),
            None => Ok(None),
        }
    }

    async fn load_build(&self, repo_id: &RepoId, commit: &str) -> Result<Option<RefInfo>> {
        // The `commit_id` column narrows the scan to rows at this commit; we then
        // confirm a completed full build (some rows may be depth=1-only mid
        // two-phase). First complete match wins.
        for row in self
            .db
            .get_by_commit(&repo_id.storage_key(), commit)
            .await?
        {
            let info: RefInfo = serde_json::from_str(&row.data).context("parse stored RefInfo")?;
            if info.full_clonepack.commit == commit && !info.full_clonepack.manifest.is_empty() {
                return Ok(Some(info));
            }
        }
        Ok(None)
    }

    async fn save_branch(&self, repo_id: &RepoId, branch: &str, info: &RefInfo) -> Result<()> {
        let repo_key = repo_id.storage_key();
        let data = serde_json::to_string(info).context("serialize RefInfo")?;
        let new_synced = info.synced_at.map(|t| t as i64);

        // Same ordering policy as S3RefStore::save: never overwrite a newer sync
        // with an older one. Same commit → always write (build_status etc. can
        // change). Different commit + both timestamps present + existing newer →
        // skip. Otherwise write.
        if let Some(existing) = self.db.get(&repo_key, branch).await?
            && existing.commit_id != info.commit
            && let (Some(e), Some(n)) = (existing.synced_at, new_synced)
            && e > n
        {
            warn!(
                "metadata for {repo_key}@{branch} already has newer sync at {e}; \
                 skipping older {n}"
            );
            return Ok(());
        }

        self.db
            .upsert(&repo_key, branch, &data, &info.commit, new_synced)
            .await
    }

    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        self.db.list_branches(&repo_id.storage_key()).await
    }

    async fn health(&self) -> Result<()> {
        self.db.health().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ref_at(commit: &str, synced_at: Option<u64>) -> RefInfo {
        RefInfo {
            commit: commit.to_string(),
            synced_at,
            ..Default::default()
        }
    }

    /// A RefInfo with a *completed full* clonepack at `commit` — what commit-keyed
    /// reuse (`load_build`) requires.
    fn complete_build(commit: &str) -> RefInfo {
        let mut info = ref_at(commit, Some(100));
        info.full_clonepack = crate::ClonepackArtifacts {
            commit: commit.to_string(),
            manifest: "manifest-hash".to_string(),
            ..Default::default()
        };
        info
    }

    /// The full RefStore lifecycle on a `SqlRefStore`, engine-agnostic. Run
    /// against each available engine.
    async fn exercise(store: &SqlRefStore) {
        let rid = RepoId::github("o/r");

        // HEAD save/load.
        store.save(&rid, &ref_at("c1", Some(100))).await.unwrap();
        assert_eq!(store.load(&rid).await.unwrap().unwrap().commit, "c1");

        // Branch save/load, distinct from HEAD.
        store
            .save_branch(&rid, "dev", &ref_at("c2", Some(100)))
            .await
            .unwrap();
        assert_eq!(
            store
                .load_branch(&rid, "dev")
                .await
                .unwrap()
                .unwrap()
                .commit,
            "c2"
        );

        // list + list_branches.
        assert_eq!(store.list().await.unwrap(), vec![RepoId::github("o/r")]);
        let mut branches = store.list_branches(&rid).await.unwrap();
        branches.sort();
        assert_eq!(branches, vec!["HEAD", "dev"]);

        // Ordering guard: an older sync for a *different* commit is skipped.
        store.save(&rid, &ref_at("c0", Some(50))).await.unwrap();
        assert_eq!(
            store.load(&rid).await.unwrap().unwrap().commit,
            "c1",
            "older different-commit sync must not overwrite a newer one"
        );

        // Same commit always writes (e.g. build_status updates) even with no ts.
        let mut updated = ref_at("c1", None);
        updated.build_status = Some("done".to_string());
        store.save(&rid, &updated).await.unwrap();
        let loaded = store.load(&rid).await.unwrap().unwrap();
        assert_eq!(loaded.commit, "c1");
        assert_eq!(loaded.build_status.as_deref(), Some("done"));

        // A newer different-commit sync does overwrite.
        store.save(&rid, &ref_at("c3", Some(200))).await.unwrap();
        assert_eq!(store.load(&rid).await.unwrap().unwrap().commit, "c3");

        // Commit-keyed reuse (get_by_commit): a completed full build is found by
        // commit from any branch; an incomplete row and an unknown commit are not.
        // Runs for every engine `exercise` covers.
        store
            .save_branch(&rid, "release", &complete_build("cf"))
            .await
            .unwrap();
        assert_eq!(
            store
                .load_build(&rid, "cf")
                .await
                .unwrap()
                .expect("completed build reusable by commit")
                .full_clonepack
                .commit,
            "cf"
        );
        assert!(
            store
                .load_build(&rid, "no-such-commit")
                .await
                .unwrap()
                .is_none(),
            "unknown commit yields None"
        );
        // "dev" was saved at c2 with an empty full_clonepack — not a reusable build.
        assert!(
            store.load_build(&rid, "c2").await.unwrap().is_none(),
            "incomplete (depth=1-only) build must not be reused"
        );
    }

    #[tokio::test]
    async fn sqlite_refstore_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db").to_string_lossy().to_string();
        let store = SqlRefStore::new(Box::new(SqliteMeta::connect(&path).await.unwrap()))
            .await
            .unwrap();
        exercise(&store).await;
    }

    /// Commit-keyed reuse: a completed full build under one branch is found by
    /// commit, so another branch at the same commit reuses it. Depth=1-only and
    /// unknown commits return None.
    #[tokio::test]
    async fn sqlite_load_build_reuses_across_branches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db").to_string_lossy().to_string();
        let store = SqlRefStore::new(Box::new(SqliteMeta::connect(&path).await.unwrap()))
            .await
            .unwrap();
        let rid = RepoId::github("o/r");

        store
            .save_branch(&rid, "foo", &complete_build("X"))
            .await
            .unwrap();
        let reused = store.load_build(&rid, "X").await.unwrap();
        assert_eq!(
            reused.expect("reuse by commit").full_clonepack.commit,
            "X",
            "a completed full build is reusable across branches by commit"
        );

        assert!(
            store.load_build(&rid, "Y").await.unwrap().is_none(),
            "unknown commit yields None"
        );

        // A depth=1-only entry (empty full_clonepack) is not a reusable full build.
        store
            .save_branch(&rid, "bar", &ref_at("Z", Some(100)))
            .await
            .unwrap();
        assert!(
            store.load_build(&rid, "Z").await.unwrap().is_none(),
            "incomplete build must not be reused"
        );
    }

    #[tokio::test]
    async fn postgres_refstore_lifecycle() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_PG_URL") else {
            eprintln!("SKIP postgres_refstore_lifecycle: RIPCLONE_TEST_PG_URL unset");
            return;
        };
        let pool = sqlx::postgres::PgPool::connect(&url)
            .await
            .expect("connect pg");
        sqlx::query("DROP TABLE IF EXISTS refs")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let store = SqlRefStore::new(Box::new(PostgresMeta::connect(&url).await.unwrap()))
            .await
            .unwrap();
        exercise(&store).await;
    }

    #[tokio::test]
    async fn mysql_refstore_lifecycle() {
        let Ok(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL") else {
            eprintln!("SKIP mysql_refstore_lifecycle: RIPCLONE_TEST_MYSQL_URL unset");
            return;
        };
        let pool = sqlx::mysql::MySqlPool::connect(&url)
            .await
            .expect("connect mysql");
        sqlx::query("DROP TABLE IF EXISTS refs")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let store = SqlRefStore::new(Box::new(MysqlMeta::connect(&url).await.unwrap()))
            .await
            .unwrap();
        exercise(&store).await;
    }
}
