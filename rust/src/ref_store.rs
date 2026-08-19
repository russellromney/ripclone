//! Ref and added-repository contracts backed by the server control database.

use crate::RefInfo;
use crate::provider::RepoId;
use anyhow::Result;
use async_trait::async_trait;

/// Admission-chain marker authorizing the first public projection for a branch.
/// `:` cannot be a Git object ID, so it cannot collide with a real commit.
pub(crate) const INITIAL_MOVING_PROJECTION_PREDECESSOR: &str = ":initial";

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

/// Metadata key for an internal exact-commit result. Git ref names cannot
/// contain `:`, so it cannot collide with a source branch.
pub fn exact_ref_key(branch: &str, commit: &str) -> String {
    format!(":{branch}#{commit}")
}

/// Decide whether a new ref may replace the stored row. SQL implementations
/// apply this policy inside their atomic update boundary.
pub(crate) fn should_replace_ref(existing: Option<&RefInfo>, new: &RefInfo) -> bool {
    if new.commit.is_empty() {
        return false;
    }
    if new.internal_exact_result && new.require_matching_commit {
        return existing.is_none();
    }
    let Some(existing) = existing else {
        return !new.require_matching_commit
            || new
                .moving_publication_predecessors
                .iter()
                .any(|commit| commit == INITIAL_MOVING_PROJECTION_PREDECESSOR);
    };
    if new.require_matching_commit {
        return existing.commit == new.commit
            || new
                .moving_publication_predecessors
                .iter()
                .any(|commit| commit == &existing.commit);
    }
    if existing.commit == new.commit {
        return true;
    }
    if let (Some(existing_gen), Some(new_gen)) = (existing.generation, new.generation) {
        return new_gen >= existing_gen;
    }
    match (existing.synced_at, new.synced_at) {
        (Some(existing_ts), Some(new_ts)) => new_ts >= existing_ts,
        _ => true,
    }
}

/// Preserve ordinary-admission authorization when a worker publishes newer
/// artifact metadata for the same exact commit.
pub(crate) fn merge_exact_admission(existing: Option<&RefInfo>, new: &RefInfo) -> RefInfo {
    let Some(existing) = existing.filter(|existing| {
        existing.internal_exact_result && new.internal_exact_result && existing.commit == new.commit
    }) else {
        return new.clone();
    };
    let identity_only = new
        .moving_publication_predecessors
        .iter()
        .any(|commit| !existing.moving_publication_predecessors.contains(commit))
        || new
            .moving_admission_successors
            .iter()
            .any(|commit| !existing.moving_admission_successors.contains(commit));
    let mut merged = if identity_only {
        existing.clone()
    } else {
        new.clone()
    };
    for predecessor in existing
        .moving_publication_predecessors
        .iter()
        .chain(&new.moving_publication_predecessors)
    {
        if !merged.moving_publication_predecessors.contains(predecessor) {
            merged
                .moving_publication_predecessors
                .push(predecessor.clone());
        }
    }
    for successor in existing
        .moving_admission_successors
        .iter()
        .chain(&new.moving_admission_successors)
    {
        if !merged.moving_admission_successors.contains(successor) {
            merged.moving_admission_successors.push(successor.clone());
        }
    }
    merged.require_matching_commit =
        existing.require_matching_commit && new.require_matching_commit;
    merged.warm_pinned |= existing.warm_pinned || new.warm_pinned;
    merged.last_accessed_at = existing.last_accessed_at.max(new.last_accessed_at);
    merged
}

#[async_trait]
pub trait RefStore: Send + Sync {
    async fn load(&self, repo_id: &RepoId) -> Result<Option<RefInfo>>;
    async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()>;
    async fn list(&self) -> Result<Vec<RepoId>>;
    async fn load_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RefInfo>>;
    async fn save_branch(&self, repo_id: &RepoId, branch: &str, info: &RefInfo) -> Result<()>;
    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool>;
    async fn touch_last_accessed_at(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
    ) -> Result<bool>;
    async fn delete_branch(&self, _repo_id: &RepoId, _branch: &str) -> Result<()> {
        Ok(())
    }
    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>>;
    async fn add_repo(&self, repo: &AddedRepo) -> Result<()>;
    async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>>;
    async fn remove_added_repo(&self, _repo_id: &RepoId) -> Result<()> {
        Ok(())
    }
    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>>;
    async fn invalidate(&self, _repo_id: &RepoId, _branch: &str) {}
    async fn health(&self) -> Result<()> {
        Ok(())
    }
}
