//! Exact results in the server-owned SQLite control database.

use crate::provider::RepoId;
use crate::ref_store::{AddedRepo, RefStore};
use crate::{ExactResultKind, FilesResult, FullResult, HeadResult, RefInfo};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

pub mod libsql;

pub use libsql::LibsqlMeta;

#[derive(Debug, Clone)]
pub struct ResultRow {
    pub data: String,
}

pub struct SqlRefStore {
    db: LibsqlMeta,
}

impl SqlRefStore {
    pub async fn new(db: LibsqlMeta) -> Result<Self> {
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
                u64::try_from(attempt.min(10) + 1).unwrap_or(1),
            ))
            .await;
        }
        anyhow::bail!("exact result {repo_key}@{commit}: repeated write conflicts")
    }
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
            if data == row.data
                || self
                    .db
                    .compare_and_swap_result(&repo_key, &info.commit, &row.data, &data)
                    .await?
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(
                u64::try_from(attempt.min(10) + 1).unwrap_or(1),
            ))
            .await;
        }
        anyhow::bail!(
            "exact result {repo_key}@{}: repeated publication conflicts",
            info.commit
        )
    }

    async fn publish_head(&self, repo_id: &RepoId, commit: &str, head: HeadResult) -> Result<bool> {
        ensure!(
            crate::exact_output_artifacts_ready(commit, ExactResultKind::Head, &head.clonepack),
            "invalid Head result for {commit}"
        );
        self.update_result(repo_id, commit, move |info| {
            info.head = Some(head.clone());
            true
        })
        .await
    }

    async fn publish_full(&self, repo_id: &RepoId, commit: &str, full: FullResult) -> Result<bool> {
        ensure!(
            crate::exact_output_artifacts_ready(commit, ExactResultKind::Full, &full.clonepack),
            "invalid Full result for {commit}"
        );
        self.update_result(repo_id, commit, move |info| {
            if crate::exact_output_ready(info, ExactResultKind::Full, commit) {
                false
            } else {
                info.full = Some(full.clone());
                true
            }
        })
        .await
    }

    async fn publish_files(
        &self,
        repo_id: &RepoId,
        commit: &str,
        files: FilesResult,
    ) -> Result<bool> {
        ensure!(
            crate::exact_output_artifacts_ready(commit, ExactResultKind::Files, &files.clonepack),
            "invalid Files result for {commit}"
        );
        self.update_result(repo_id, commit, move |info| {
            if crate::exact_output_ready(info, ExactResultKind::Files, commit) {
                false
            } else {
                info.files = Some(files.clone());
                true
            }
        })
        .await
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

    fn artifacts(commit: &str, manifest: &str) -> crate::ClonepackArtifacts {
        let hash = |suffix: &str| crate::cas::hash(format!("{manifest}-{suffix}").as_bytes());
        crate::ClonepackArtifacts {
            commit: commit.to_string(),
            manifest: hash("manifest"),
            metadata_chunk: hash("metadata"),
            skeleton_pack: hash("skeleton-pack"),
            skeleton_idx: hash("skeleton-idx"),
            prebuilt_index: hash("index"),
            idx_bundle: hash("idx-bundle"),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn each_publication_updates_only_its_result() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = LibsqlMeta::connect(tmp.path().join("control.db").to_str().unwrap())
            .await
            .unwrap();
        let store = SqlRefStore::new(meta).await.unwrap();
        let repo = RepoId::github("acme/widget");
        let commit = "a".repeat(40);
        store
            .save_result(
                &repo,
                &RefInfo {
                    commit: commit.clone(),
                    head: Some(crate::HeadResult {
                        clonepack: artifacts(&commit, "head"),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        store
            .publish_full(
                &repo,
                &commit,
                crate::FullResult {
                    clonepack: artifacts(&commit, "full"),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let result = store.load_result(&repo, &commit).await.unwrap().unwrap();
        assert_eq!(
            result.head.unwrap().clonepack.manifest,
            crate::cas::hash(b"head-manifest")
        );
        assert_eq!(
            result.full.unwrap().clonepack.manifest,
            crate::cas::hash(b"full-manifest")
        );
        assert!(result.files.is_none());
    }

    #[tokio::test]
    async fn simultaneous_full_and_files_preserve_both_results() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = LibsqlMeta::connect(tmp.path().join("control.db").to_str().unwrap())
            .await
            .unwrap();
        let store = std::sync::Arc::new(SqlRefStore::new(meta).await.unwrap());
        let repo = RepoId::github("acme/concurrent");
        let commit = "b".repeat(40);
        store
            .save_result(
                &repo,
                &RefInfo {
                    commit: commit.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let full_store = store.clone();
        let files_store = store.clone();
        let full_repo = repo.clone();
        let files_repo = repo.clone();
        let full_commit = commit.clone();
        let files_commit = commit.clone();
        let (full, files) = tokio::join!(
            async move {
                full_store
                    .publish_full(
                        &full_repo,
                        &full_commit,
                        crate::FullResult {
                            clonepack: artifacts(&full_commit, "full"),
                            ..Default::default()
                        },
                    )
                    .await
            },
            async move {
                files_store
                    .publish_files(
                        &files_repo,
                        &files_commit,
                        crate::FilesResult {
                            clonepack: artifacts(&files_commit, "files"),
                            archive_chunks: Vec::new(),
                            archive_frames: Vec::new(),
                        },
                    )
                    .await
            }
        );
        assert!(full.unwrap());
        assert!(files.unwrap());
        let result = store.load_result(&repo, &commit).await.unwrap().unwrap();
        assert!(result.full.is_some());
        assert!(result.files.is_some());
        assert!(result.files.unwrap().archive_chunks.is_empty());
    }
}
