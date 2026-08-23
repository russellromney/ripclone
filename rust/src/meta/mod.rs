//! Exact results in the server-owned SQLite control database.

use crate::RefInfo;
use crate::provider::{RepoId, parse_storage_key};
use crate::ref_store::{AddedRepo, RefStore};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::time::SystemTime;

pub mod libsql;

pub use libsql::LibsqlMeta;

#[derive(Debug, Clone)]
pub struct ResultRow {
    pub data: String,
}

#[async_trait]
pub trait MetaDb: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn get_result(&self, repo_key: &str, commit: &str) -> Result<Option<ResultRow>>;
    async fn insert_result(&self, repo_key: &str, commit: &str, data: &str) -> Result<bool>;
    async fn compare_and_swap_result(
        &self,
        repo_key: &str,
        commit: &str,
        expected_data: &str,
        new_data: &str,
    ) -> Result<bool>;
    async fn list_repos(&self) -> Result<Vec<String>>;
    async fn list_commits(&self, repo_key: &str) -> Result<Vec<String>>;
    async fn delete_result(&self, repo_key: &str, commit: &str) -> Result<()>;
    async fn add_repo(&self, repo_key: &str, data: &str) -> Result<()>;
    async fn get_added_repo(&self, repo_key: &str) -> Result<Option<String>>;
    async fn remove_added_repo(&self, repo_key: &str) -> Result<()>;
    async fn list_added_repos(&self) -> Result<Vec<String>>;
    async fn health(&self) -> Result<()>;
}

pub struct SqlRefStore {
    db: Box<dyn MetaDb>,
}

impl SqlRefStore {
    pub async fn new(db: Box<dyn MetaDb>) -> Result<Self> {
        db.init().await?;
        Ok(Self { db })
    }

    async fn update_result(
        &self,
        repo_id: &RepoId,
        commit: &str,
        update: impl Fn(&mut RefInfo) -> bool,
    ) -> Result<bool> {
        let repo_key = repo_id.storage_key();
        for attempt in 0..64 {
            let Some(row) = self.db.get_result(&repo_key, commit).await? else {
                return Ok(false);
            };
            let mut info: RefInfo =
                serde_json::from_str(&row.data).context("parse stored exact result")?;
            ensure!(
                info.commit == commit,
                "stored exact result identity mismatch"
            );
            if !update(&mut info) {
                return Ok(true);
            }
            let data = serde_json::to_string(&info).context("serialize exact result")?;
            if self
                .db
                .compare_and_swap_result(&repo_key, commit, &row.data, &data)
                .await?
            {
                return Ok(true);
            }
            tokio::time::sleep(std::time::Duration::from_millis(
                (attempt.min(10) + 1) as u64,
            ))
            .await;
        }
        anyhow::bail!("exact result {repo_key}@{commit}: repeated write conflicts")
    }
}

fn variant_ready(variant: &crate::ClonepackArtifacts, commit: &str) -> bool {
    variant.commit == commit && !variant.manifest.is_empty()
}

/// Merge one worker publication without allowing a duplicate or stale report
/// to replace an already accepted ready variant. Ordered phase enrichment is
/// accepted only when it carries the same previously-published variant bytes.
pub(crate) fn merge_publication(existing: &RefInfo, incoming: &RefInfo) -> RefInfo {
    let commit = existing.commit.as_str();
    // GC deliberately retains artifact pointers on an evicted row so its
    // accounting remains inspectable after the objects are removed. Those
    // pointers are not ready variants and must not defeat a rebuilt exact
    // publication merely because their manifests differ from the new bytes.
    let existing_evicted =
        existing.build_status.as_deref() == Some(crate::remote_gc::EVICTED_BUILD_STATUS);
    let shallow_ready = !existing_evicted && variant_ready(&existing.shallow_clonepack, commit);
    let full_ready = !existing_evicted && variant_ready(&existing.full_clonepack, commit);
    let files_enrichment = full_ready
        && existing.archive_chunks.is_empty()
        && incoming.full_clonepack.commit == commit
        && !existing.full_clonepack.idx_bundle.is_empty()
        && incoming.full_clonepack.idx_bundle == existing.full_clonepack.idx_bundle
        && incoming.build_status.is_none();
    let exact_retry_completed = full_ready
        && incoming.full_clonepack == existing.full_clonepack
        && incoming.shallow_clonepack == existing.shallow_clonepack
        && existing
            .build_status
            .as_deref()
            .is_some_and(|status| status.starts_with("failed: "))
        && incoming.build_status.is_none();

    if (shallow_ready && existing.shallow_clonepack.manifest != incoming.shallow_clonepack.manifest)
        || (full_ready
            && existing.full_clonepack.manifest != incoming.full_clonepack.manifest
            && !files_enrichment)
    {
        let mut kept = existing.clone();
        kept.last_accessed_at = kept.last_accessed_at.max(incoming.last_accessed_at);
        kept.warm_pinned |= incoming.warm_pinned;
        return kept;
    }

    let mut merged = incoming.clone();
    if shallow_ready {
        merged.shallow_clonepack = existing.shallow_clonepack.clone();
    }
    if full_ready && !files_enrichment && !exact_retry_completed {
        merged = existing.clone();
    }
    merged.last_accessed_at = existing.last_accessed_at.max(incoming.last_accessed_at);
    merged.warm_pinned |= existing.warm_pinned;
    merged
}

#[async_trait]
impl RefStore for SqlRefStore {
    async fn load_result(&self, repo_id: &RepoId, commit: &str) -> Result<Option<RefInfo>> {
        match self.db.get_result(&repo_id.storage_key(), commit).await? {
            Some(row) => {
                let info: RefInfo =
                    serde_json::from_str(&row.data).context("parse stored exact result")?;
                ensure!(
                    info.commit == commit,
                    "stored exact result identity mismatch"
                );
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    async fn save_result(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()> {
        crate::validation::validate_object_id(&info.commit)
            .context("validate exact result commit")?;
        let repo_key = repo_id.storage_key();
        for attempt in 0..64 {
            let data = serde_json::to_string(info).context("serialize exact result")?;
            if self
                .db
                .insert_result(&repo_key, &info.commit, &data)
                .await?
            {
                return Ok(());
            }
            let row = self
                .db
                .get_result(&repo_key, &info.commit)
                .await?
                .context("exact result disappeared after insert conflict")?;
            let existing: RefInfo =
                serde_json::from_str(&row.data).context("parse stored exact result")?;
            ensure!(
                existing.commit == info.commit,
                "stored exact result identity mismatch"
            );
            let merged = merge_publication(&existing, info);
            let merged_data = serde_json::to_string(&merged).context("serialize exact result")?;
            if merged_data == row.data
                || self
                    .db
                    .compare_and_swap_result(&repo_key, &info.commit, &row.data, &merged_data)
                    .await?
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(
                (attempt.min(10) + 1) as u64,
            ))
            .await;
        }
        anyhow::bail!(
            "exact result {repo_key}@{}: repeated publication conflicts",
            info.commit
        )
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

    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        commit: &str,
        status: &str,
    ) -> Result<bool> {
        self.update_result(repo_id, commit, |info| {
            if info.build_status.as_deref() == Some(status) {
                false
            } else {
                info.build_status = Some(status.to_string());
                true
            }
        })
        .await
    }

    async fn touch_last_accessed_at(&self, repo_id: &RepoId, commit: &str) -> Result<bool> {
        self.update_result(repo_id, commit, |info| {
            info.last_accessed_at = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|duration| duration.as_secs());
            true
        })
        .await
    }

    async fn delete_result(&self, repo_id: &RepoId, commit: &str) -> Result<()> {
        self.db.delete_result(&repo_id.storage_key(), commit).await
    }

    async fn list_commits(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        self.db.list_commits(&repo_id.storage_key()).await
    }

    async fn add_repo(&self, repo: &AddedRepo) -> Result<()> {
        let data = serde_json::to_string(repo).context("serialize added repo")?;
        self.db.add_repo(&repo.repo_id.storage_key(), &data).await
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

    #[tokio::test]
    async fn stale_publication_cannot_regress_ready_result() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = LibsqlMeta::connect(tmp.path().join("control.db").to_str().unwrap())
            .await
            .unwrap();
        let store = SqlRefStore::new(Box::new(meta)).await.unwrap();
        let repo = RepoId::github("acme/widget");
        let commit = "a".repeat(40);
        let mut ready = RefInfo {
            commit: commit.clone(),
            build_status: Some("done".into()),
            ..Default::default()
        };
        ready.shallow_clonepack.commit = commit.clone();
        ready.shallow_clonepack.manifest = "ready".into();
        store.save_result(&repo, &ready).await.unwrap();

        let stale = RefInfo {
            commit: commit.clone(),
            build_status: Some("building".into()),
            ..Default::default()
        };
        store.save_result(&repo, &stale).await.unwrap();

        let stored = store.load_result(&repo, &commit).await.unwrap().unwrap();
        assert_eq!(stored.shallow_clonepack.manifest, "ready");
        assert_eq!(stored.build_status.as_deref(), Some("done"));
    }

    #[tokio::test]
    async fn files_enrichment_requires_the_accepted_full_build() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = LibsqlMeta::connect(tmp.path().join("control.db").to_str().unwrap())
            .await
            .unwrap();
        let store = SqlRefStore::new(Box::new(meta)).await.unwrap();
        let repo = RepoId::github("acme/files");
        let commit = "b".repeat(40);
        let mut editable = RefInfo {
            commit: commit.clone(),
            build_status: Some("archive building".into()),
            ..Default::default()
        };
        editable.full_clonepack.commit = commit.clone();
        editable.full_clonepack.manifest = "editable".into();
        editable.full_clonepack.idx_bundle = "accepted-bundle".into();
        store.save_result(&repo, &editable).await.unwrap();

        let mut wrong_attempt = editable.clone();
        wrong_attempt.full_clonepack.manifest = "wrong-files".into();
        wrong_attempt.full_clonepack.idx_bundle = "other-bundle".into();
        wrong_attempt.archive_chunks = vec!["wrong-archive".into()];
        wrong_attempt.build_status = None;
        store.save_result(&repo, &wrong_attempt).await.unwrap();
        let kept = store.load_result(&repo, &commit).await.unwrap().unwrap();
        assert_eq!(kept.full_clonepack.manifest, "editable");
        assert!(kept.archive_chunks.is_empty());

        let mut files = editable;
        files.full_clonepack.manifest = "files".into();
        files.archive_chunks = vec!["archive".into()];
        files.build_status = None;
        store.save_result(&repo, &files).await.unwrap();
        let ready = store.load_result(&repo, &commit).await.unwrap().unwrap();
        assert_eq!(ready.full_clonepack.manifest, "files");
        assert_eq!(ready.archive_chunks, vec!["archive"]);
        assert!(ready.build_status.is_none());
    }

    #[tokio::test]
    async fn empty_tree_files_enrichment_settles_without_archive_chunks() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = LibsqlMeta::connect(tmp.path().join("control.db").to_str().unwrap())
            .await
            .unwrap();
        let store = SqlRefStore::new(Box::new(meta)).await.unwrap();
        let repo = RepoId::github("acme/empty-files");
        let commit = "e".repeat(40);
        let mut editable = RefInfo {
            commit: commit.clone(),
            build_status: Some("archive building".into()),
            ..Default::default()
        };
        editable.full_clonepack.commit = commit.clone();
        editable.full_clonepack.manifest = "editable".into();
        editable.full_clonepack.idx_bundle = "accepted-bundle".into();
        store.save_result(&repo, &editable).await.unwrap();

        let mut settled = editable;
        settled.full_clonepack.manifest = "empty-files".into();
        settled.build_status = None;
        store.save_result(&repo, &settled).await.unwrap();

        let stored = store.load_result(&repo, &commit).await.unwrap().unwrap();
        assert_eq!(stored.full_clonepack.manifest, "empty-files");
        assert!(stored.archive_chunks.is_empty());
        assert!(stored.build_status.is_none());
    }

    #[tokio::test]
    async fn exact_retry_can_clear_failure_without_replacing_artifacts() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = LibsqlMeta::connect(tmp.path().join("control.db").to_str().unwrap())
            .await
            .unwrap();
        let store = SqlRefStore::new(Box::new(meta)).await.unwrap();
        let repo = RepoId::github("acme/retry");
        let commit = "c".repeat(40);
        let mut failed = RefInfo {
            commit: commit.clone(),
            build_status: Some("failed: transient source".into()),
            ..Default::default()
        };
        failed.full_clonepack.commit = commit.clone();
        failed.full_clonepack.manifest = "same-full".into();
        failed.full_clonepack.metadata_chunk = "same-metadata".into();
        failed.full_clonepack.idx_bundle = "same-bundle".into();
        failed.shallow_clonepack.commit = commit.clone();
        failed.shallow_clonepack.manifest = "same-shallow".into();
        store.save_result(&repo, &failed).await.unwrap();

        let mut recovered = failed.clone();
        recovered.build_status = None;
        store.save_result(&repo, &recovered).await.unwrap();
        let stored = store.load_result(&repo, &commit).await.unwrap().unwrap();
        assert!(stored.build_status.is_none());
        assert_eq!(stored.full_clonepack, failed.full_clonepack);
        assert_eq!(stored.shallow_clonepack, failed.shallow_clonepack);
    }
}
