//! Ref and added-repository contracts backed by the server control database.

use crate::RefInfo;
use crate::provider::RepoId;
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
    async fn save_claimed_result(
        &self,
        repo_id: &RepoId,
        info: &RefInfo,
        _job_id: i64,
        _worker_id: &str,
    ) -> Result<bool> {
        self.save_result(repo_id, info).await?;
        Ok(true)
    }
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
    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        commit: &str,
        status: &str,
    ) -> Result<bool>;
    async fn update_claimed_build_status(
        &self,
        repo_id: &RepoId,
        commit: &str,
        status: &str,
        _job_id: i64,
        _worker_id: &str,
    ) -> Result<bool> {
        self.update_build_status(repo_id, commit, status).await
    }
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
