//! Ref and added-repository contracts backed by the server control database.

use crate::provider::RepoId;
use crate::{FilesResult, FullResult, HeadResult, RefInfo};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AddedRepo {
    pub repo_id: RepoId,
    pub added_at: u64,
    pub history_enabled: bool,
    pub source: AddedRepoSource,
    /// Upstream size used to classify the first build. `None` selects the
    /// largest configured class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AddedRepoSource {
    Cli,
    Cloud,
    Api,
}

#[async_trait]
pub trait RefStore: Send + Sync {
    async fn load_result(&self, repo_id: &RepoId, commit: &str) -> Result<Option<RefInfo>>;
    async fn save_result(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()>;
    async fn publish_head(&self, repo_id: &RepoId, commit: &str, head: HeadResult) -> Result<bool>;
    async fn publish_full(&self, repo_id: &RepoId, commit: &str, full: FullResult) -> Result<bool>;
    async fn publish_files(
        &self,
        repo_id: &RepoId,
        commit: &str,
        files: FilesResult,
    ) -> Result<bool>;
    async fn publish_claimed_head(
        &self,
        repo_id: &RepoId,
        commit: &str,
        head: HeadResult,
        _job_id: i64,
        _worker_id: &str,
    ) -> Result<bool> {
        self.publish_head(repo_id, commit, head).await
    }
    async fn publish_claimed_full(
        &self,
        repo_id: &RepoId,
        commit: &str,
        full: FullResult,
        _job_id: i64,
        _worker_id: &str,
    ) -> Result<bool> {
        self.publish_full(repo_id, commit, full).await
    }
    async fn publish_claimed_files(
        &self,
        repo_id: &RepoId,
        commit: &str,
        files: FilesResult,
        _job_id: i64,
        _worker_id: &str,
    ) -> Result<bool> {
        self.publish_files(repo_id, commit, files).await
    }
    /// Clear an idle exact result only if it has not changed since selection
    /// and no queued or claimed job owns that commit. Publication, access, and
    /// active work all win over eviction.
    async fn evict_if_unchanged(&self, repo_id: &RepoId, expected: &RefInfo) -> Result<bool>;
    /// Optional wrapper hooks around an atomically authorized claimed write.
    /// Production stores use the defaults; deterministic test stores use these
    /// without moving the authority check outside the control transaction.
    async fn before_claimed_result_write(&self, _repo_id: &RepoId, _info: &RefInfo) -> Result<()> {
        Ok(())
    }
    async fn after_claimed_result_write(&self, _repo_id: &RepoId, _info: &RefInfo) -> Result<()> {
        Ok(())
    }
    async fn list(&self) -> Result<Vec<RepoId>>;
    async fn touch_last_accessed_at(&self, repo_id: &RepoId, commit: &str) -> Result<bool>;
    async fn delete_result(&self, _repo_id: &RepoId, _commit: &str) -> Result<()> {
        Ok(())
    }
    async fn list_commits(&self, repo_id: &RepoId) -> Result<Vec<String>>;
    async fn add_repo(&self, repo: &AddedRepo) -> Result<()>;
    async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>>;
    async fn remove_added_repo(&self, _repo_id: &RepoId) -> Result<()> {
        Ok(())
    }
    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>>;
    async fn invalidate(&self, _repo_id: &RepoId, _commit: &str) {}
    async fn health(&self) -> Result<()> {
        Ok(())
    }
}
