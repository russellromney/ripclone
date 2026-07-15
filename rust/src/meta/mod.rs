//! SQL-backed metadata store using SQLite. Legacy file and S3 ref stores remain
//! separate rollback paths until normalized cutover.
//!
//! [`MetaDb`] is a tiny per-engine adapter that returns plain Rust types (no
//! engine types leak); [`SqlRefStore`] holds one and implements the existing
//! [`RefStore`](crate::ref_store::RefStore) trait, owning the `RefInfo`↔JSON
//! serialization and the save-ordering policy. The metadata is small (one JSON
//! row per repo/branch), so this mirrors the file/S3 stores closely.

use crate::RefInfo;
use crate::provider::{RepoId, parse_storage_key};
use crate::ref_store::{AddedRepo, RefStore};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::time::SystemTime;

pub mod sqlite;

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
/// data)` table. `repo_key` is the repo's [`RepoId::storage_key`]. Implemented
/// by `SqliteMeta`. It intentionally exposes plain domain types so a future
/// adapter does not require changes to shared ref-store policy.
#[async_trait]
pub trait MetaDb: Send + Sync {
    /// Create the `refs` table if absent.
    async fn init(&self) -> Result<()>;

    /// Fetch the row for one ref, if present.
    async fn get(&self, repo_key: &str, branch: &str) -> Result<Option<RefRow>>;

    /// All ref rows for this repo whose `commit_id` equals `commit` (one per
    /// branch sitting at that commit). Powers commit-keyed reuse: a sync of one
    /// branch can reuse a build another branch already produced for the same
    /// commit. Indexed on `(repo_key, commit_id)`; no new write needed.
    async fn get_by_commit(&self, repo_key: &str, commit: &str) -> Result<Vec<RefRow>>;

    /// Insert-or-update the row for one ref, applying the save-ordering policy
    /// ("a newer sync never loses to an older one") in a **single atomic
    /// statement** — no read-then-write TOCTOU. The write lands when there is no
    /// existing row, the commit matches (metadata-only update), the new
    /// `generation` (commit history depth) is >= the stored one, or — for rows
    /// without a generation — the new `synced_at` is >= the stored one. This
    /// mirrors [`should_replace_ref`](crate::ref_store) but enforced in SQL so
    /// concurrent writers across processes can't reorder.
    ///
    /// Pairs with the fetch-time `synced_at` stamping in `do_sync`: the stamp
    /// makes "newer" mean "fetched later", the fallback used when neither side
    /// carries a generation.
    async fn save_ordered(
        &self,
        repo_key: &str,
        branch: &str,
        data: &str,
        commit_id: &str,
        synced_at: Option<i64>,
        generation: Option<i64>,
    ) -> Result<()>;

    /// Replace the JSON blob only if the row still has both the expected commit
    /// and the expected current JSON blob.
    async fn compare_and_swap_data(
        &self,
        repo_key: &str,
        branch: &str,
        expected_commit: &str,
        expected_data: &str,
        new_data: &str,
    ) -> Result<bool>;

    /// Distinct `repo_key`s that have at least one stored ref.
    async fn list_repos(&self) -> Result<Vec<String>>;

    /// Branches with a stored ref for this repo.
    async fn list_branches(&self, repo_key: &str) -> Result<Vec<String>>;

    /// Insert or update added-repo state, keyed by the unified storage key.
    async fn add_repo(&self, repo_key: &str, data: &str) -> Result<()>;

    /// Insert only when absent. Used for first admission without a read/write race.
    async fn insert_added_repo(&self, repo_key: &str, data: &str) -> Result<bool>;

    /// Replace only the exact JSON value previously observed.
    async fn compare_and_swap_added_repo(
        &self,
        repo_key: &str,
        expected_data: &str,
        new_data: &str,
    ) -> Result<bool>;

    /// Fetch added-repo state for one repo.
    async fn get_added_repo(&self, repo_key: &str) -> Result<Option<String>>;

    /// Remove added-repo state for one repo.
    async fn remove_added_repo(&self, repo_key: &str) -> Result<()>;

    /// List every added-repo record.
    async fn list_added_repos(&self) -> Result<Vec<String>>;

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

    async fn mutate_admission(
        &self,
        repo_id: &RepoId,
        mutate: impl Fn(&mut AddedRepo) -> bool,
    ) -> Result<bool> {
        let key = repo_id.storage_key();
        loop {
            let Some(old_data) = self.db.get_added_repo(&key).await? else {
                return Ok(false);
            };
            let mut repo: AddedRepo =
                serde_json::from_str(&old_data).context("parse stored added repo")?;
            if !mutate(&mut repo) {
                return Ok(false);
            }
            let new_data = serde_json::to_string(&repo).context("serialize added repo")?;
            if self
                .db
                .compare_and_swap_added_repo(&key, &old_data, &new_data)
                .await?
            {
                return Ok(true);
            }
        }
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
        Ok(self
            .db
            .list_repos()
            .await?
            .into_iter()
            .filter_map(|key| parse_storage_key(&key))
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
        // The `commit_id` index narrows to rows at this commit; we then confirm a
        // completed full build (some rows may be depth=1-only mid two-phase).
        // First complete match wins.
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
        let new_generation = info.generation.map(|g| g as i64);

        // Ordering ("a newer sync never loses to an older one") is enforced
        // atomically inside the single conditional upsert — no get-then-write
        // TOCTOU, so concurrent writers can't reorder. See `MetaDb::save_ordered`.
        self.db
            .save_ordered(
                &repo_key,
                branch,
                &data,
                &info.commit,
                new_synced,
                new_generation,
            )
            .await
    }

    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool> {
        let repo_key = repo_id.storage_key();
        for attempt in 0..64 {
            let Some(row) = self.db.get(&repo_key, branch).await? else {
                return Ok(false);
            };
            if row.commit_id != expected_commit {
                return Ok(false);
            }
            let mut info: RefInfo =
                serde_json::from_str(&row.data).context("parse stored RefInfo")?;
            if info.commit != expected_commit {
                return Ok(false);
            }
            info.build_status = Some(status.to_string());
            let data = serde_json::to_string(&info).context("serialize RefInfo")?;
            if data == row.data {
                return Ok(true);
            }
            if self
                .db
                .compare_and_swap_data(&repo_key, branch, expected_commit, &row.data, &data)
                .await?
            {
                return Ok(true);
            }
            tokio::time::sleep(std::time::Duration::from_millis(
                (attempt.min(10) + 1) as u64,
            ))
            .await;
        }
        anyhow::bail!(
            "SQL ref store {repo_key}@{branch}: gave up after repeated status-write conflicts"
        )
    }

    async fn touch_last_accessed_at(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
    ) -> Result<bool> {
        let repo_key = repo_id.storage_key();
        for attempt in 0..64 {
            let Some(row) = self.db.get(&repo_key, branch).await? else {
                return Ok(false);
            };
            if row.commit_id != expected_commit {
                return Ok(false);
            }
            let mut info: RefInfo =
                serde_json::from_str(&row.data).context("parse stored RefInfo")?;
            if info.commit != expected_commit {
                return Ok(false);
            }
            info.last_accessed_at = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|d| d.as_secs());
            let data = serde_json::to_string(&info).context("serialize RefInfo")?;
            if data == row.data {
                return Ok(true);
            }
            if self
                .db
                .compare_and_swap_data(&repo_key, branch, expected_commit, &row.data, &data)
                .await?
            {
                return Ok(true);
            }
            tokio::time::sleep(std::time::Duration::from_millis(
                (attempt.min(10) + 1) as u64,
            ))
            .await;
        }
        anyhow::bail!(
            "SQL ref store {repo_key}@{branch}: gave up after repeated last-accessed-write conflicts"
        )
    }

    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        self.db.list_branches(&repo_id.storage_key()).await
    }

    async fn add_repo(&self, repo: &AddedRepo) -> Result<()> {
        let data = serde_json::to_string(repo).context("serialize added repo")?;
        self.db.add_repo(&repo.repo_id.storage_key(), &data).await
    }

    async fn begin_repo_initialization(&self, repo: &AddedRepo) -> Result<bool> {
        anyhow::ensure!(
            repo.state == crate::ref_store::RepoLifecycleState::Initializing
                && repo.initialization_attempt_id.is_some(),
            "new repo admission requires an initializing row with an attempt id"
        );
        let key = repo.repo_id.storage_key();
        let new_data = serde_json::to_string(repo).context("serialize added repo")?;
        loop {
            let Some(old_data) = self.db.get_added_repo(&key).await? else {
                if self.db.insert_added_repo(&key, &new_data).await? {
                    return Ok(true);
                }
                continue;
            };
            let old: AddedRepo =
                serde_json::from_str(&old_data).context("parse stored added repo")?;
            if old.state != crate::ref_store::RepoLifecycleState::Failed
                || old.initialization_attempt_id == repo.initialization_attempt_id
            {
                return Ok(false);
            }
            if self
                .db
                .compare_and_swap_added_repo(&key, &old_data, &new_data)
                .await?
            {
                return Ok(true);
            }
        }
    }

    async fn repair_legacy_repo_initialization(
        &self,
        repo_id: &RepoId,
        attempt_id: &str,
    ) -> Result<bool> {
        self.mutate_admission(repo_id, |repo| {
            if repo.state != crate::ref_store::RepoLifecycleState::Initializing
                || repo.initialization_attempt_id.is_some()
            {
                return false;
            }
            repo.initialization_branch
                .get_or_insert_with(|| "HEAD".to_string());
            repo.initialization_target = None;
            repo.failure = None;
            repo.initialization_attempt_id = Some(attempt_id.to_string());
            true
        })
        .await
    }

    async fn pin_repo_initialization(
        &self,
        repo_id: &RepoId,
        branch: &str,
        commit: &str,
        attempt_id: Option<&str>,
    ) -> Result<bool> {
        self.mutate_admission(repo_id, |repo| {
            let branch_matches = repo.initialization_branch.as_deref() == Some(branch)
                || (repo.initialization_branch.as_deref() == Some("HEAD")
                    && repo.initialization_target.is_none());
            if repo.state != crate::ref_store::RepoLifecycleState::Initializing
                || !crate::ref_store::admission_attempt_matches(repo, attempt_id)
                || !branch_matches
                || repo
                    .initialization_target
                    .as_deref()
                    .is_some_and(|target| target != commit)
            {
                return false;
            }
            repo.initialization_branch = Some(branch.to_string());
            repo.initialization_target
                .get_or_insert_with(|| commit.to_string());
            true
        })
        .await
    }

    async fn activate_repo(
        &self,
        repo_id: &RepoId,
        _branch: &str,
        commit: &str,
        attempt_id: Option<&str>,
    ) -> Result<bool> {
        self.mutate_admission(repo_id, |repo| {
            if !matches!(
                repo.state,
                crate::ref_store::RepoLifecycleState::Initializing
                    | crate::ref_store::RepoLifecycleState::Failed
            ) || !crate::ref_store::admission_attempt_matches(repo, attempt_id)
                || repo.initialization_target.as_deref() != Some(commit)
            {
                return false;
            }
            repo.state = crate::ref_store::RepoLifecycleState::Active;
            repo.activated_at = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .ok()
                .map(|d| d.as_secs());
            repo.failure = None;
            true
        })
        .await
    }

    async fn fail_repo_initialization(
        &self,
        repo_id: &RepoId,
        _branch: &str,
        commit: Option<&str>,
        failure: &str,
        attempt_id: Option<&str>,
    ) -> Result<bool> {
        self.mutate_admission(repo_id, |repo| {
            if repo.state != crate::ref_store::RepoLifecycleState::Initializing
                || !crate::ref_store::admission_attempt_matches(repo, attempt_id)
                || commit
                    .is_some_and(|commit| repo.initialization_target.as_deref() != Some(commit))
            {
                return false;
            }
            repo.state = crate::ref_store::RepoLifecycleState::Failed;
            repo.failure = Some(failure.to_string());
            true
        })
        .await
    }

    async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>> {
        match self.db.get_added_repo(&repo_id.storage_key()).await? {
            Some(data) => Ok(Some(
                serde_json::from_str(&data).context("parse stored added repo")?,
            )),
            None => Ok(None),
        }
    }

    async fn remove_added_repo(&self, repo_id: &RepoId) -> Result<()> {
        self.db.remove_added_repo(&repo_id.storage_key()).await
    }

    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>> {
        self.db
            .list_added_repos()
            .await?
            .into_iter()
            .map(|data| serde_json::from_str(&data).context("parse stored added repo"))
            .collect()
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

        let added = AddedRepo {
            repo_id: rid.clone(),
            added_at: 123,
            history_enabled: true,
            source: crate::ref_store::AddedRepoSource::Api,
            repo_size_bytes: None,
            state: crate::ref_store::RepoLifecycleState::Active,
            initialization_branch: None,
            initialization_target: None,
            activated_at: Some(123),
            failure: None,
            initialization_attempt_id: None,
        };
        store.add_repo(&added).await.unwrap();
        assert_eq!(
            store.load_added_repo(&rid).await.unwrap(),
            Some(added.clone())
        );
        assert_eq!(store.list_added_repos().await.unwrap(), vec![added]);
        store.remove_added_repo(&rid).await.unwrap();
        assert!(store.load_added_repo(&rid).await.unwrap().is_none());

        let admission_id = RepoId::github("o/admission");
        let candidate = |attempt: &str| AddedRepo {
            repo_id: admission_id.clone(),
            added_at: 1,
            history_enabled: true,
            source: crate::ref_store::AddedRepoSource::Api,
            repo_size_bytes: None,
            state: crate::ref_store::RepoLifecycleState::Initializing,
            initialization_branch: Some("HEAD".into()),
            initialization_target: None,
            activated_at: None,
            failure: None,
            initialization_attempt_id: Some(attempt.into()),
        };
        assert!(
            store
                .begin_repo_initialization(&candidate("Attempt-A"))
                .await
                .unwrap()
        );
        assert!(
            !store
                .begin_repo_initialization(&candidate("attempt-a"))
                .await
                .unwrap()
        );
        assert!(
            store
                .fail_repo_initialization(&admission_id, "HEAD", None, "retry", Some("Attempt-A"),)
                .await
                .unwrap()
        );
        assert!(
            store
                .begin_repo_initialization(&candidate("attempt-a"))
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .load_added_repo(&admission_id)
                .await
                .unwrap()
                .unwrap()
                .initialization_attempt_id
                .as_deref(),
            Some("attempt-a"),
            "attempt IDs differing only by case must remain byte-distinct"
        );
        store.remove_added_repo(&admission_id).await.unwrap();

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

        // Generation (commit history depth) is the primary ordering signal.
        // Establish a baseline that has one (wins the synced_at fallback vs the
        // gen-less c3 above).
        let mut g10 = ref_at("g10", Some(300));
        g10.generation = Some(10);
        store.save(&rid, &g10).await.unwrap();
        assert_eq!(store.load(&rid).await.unwrap().unwrap().commit, "g10");

        // Deeper history wins even with an older wall clock.
        let mut g20 = ref_at("g20", Some(1));
        g20.generation = Some(20);
        store.save(&rid, &g20).await.unwrap();
        assert_eq!(
            store.load(&rid).await.unwrap().unwrap().commit,
            "g20",
            "higher generation wins over a newer wall clock"
        );

        // Shallower history loses even with a newer wall clock.
        let mut g15 = ref_at("g15", Some(99_999));
        g15.generation = Some(15);
        store.save(&rid, &g15).await.unwrap();
        assert_eq!(
            store.load(&rid).await.unwrap().unwrap().commit,
            "g20",
            "lower generation loses despite a newer wall clock"
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
}
