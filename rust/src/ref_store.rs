use crate::RefInfo;
use crate::provider::{RepoId, parse_storage_key};
use crate::storage::S3Storage;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AddedRepo {
    pub repo_id: RepoId,
    pub added_at: u64,
    pub history_enabled: bool,
    pub source: AddedRepoSource,
    /// Upstream repo size from the tiered-add preflight (GitHub `repo.size` in
    /// bytes, etc.). Used to classify the first build into a size class when no
    /// prior clonepack exists yet. `None` on legacy rows / providers with no
    /// size signal → first build maps to the largest class.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_size_bytes: Option<u64>,
    /// Admission lifecycle. Legacy rows predate admission gating and deserialize
    /// as initializing: startup reconciliation must verify their exact HEAD and
    /// full base before they become clone-visible under the new invariant.
    #[serde(default)]
    pub state: RepoLifecycleState,
    /// Branch whose first durable HEAD + full-history artifacts admit the repo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialization_branch: Option<String>,
    /// Commit pinned by the first successful mirror/build preparation. A stale
    /// or concurrent build for another commit may never activate the repo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialization_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activated_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

impl AddedRepo {
    pub fn is_active(&self) -> bool {
        self.state == RepoLifecycleState::Active
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RepoLifecycleState {
    #[default]
    Initializing,
    Active,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AddedRepoSource {
    Cli,
    Cloud,
    Api,
    Migration,
}

/// Encode a branch name so it is safe to use in a filesystem path or S3 key.
fn branch_slug(branch: &str) -> String {
    base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        branch.as_bytes(),
    )
}

/// Decode a branch slug back into the original branch name.
fn unbranch_slug(slug: &str) -> Option<String> {
    String::from_utf8(
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, slug).ok()?,
    )
    .ok()
}

/// The one place that decides whether a newly-synced `RefInfo` may replace an
/// existing stored one. The invariant is the README's "a newer sync never
/// loses to an older one"; every backend (file, S3, SQL) must apply the *same*
/// policy or the same race resolves differently depending on where the refs
/// happen to live.
///
/// Accept the write when:
/// - there is no existing ref, or
/// - the new ref points at the same commit (a metadata-only update — e.g.
///   `build_status` flipping to done — must always land), or
/// - the new commit is at least as deep in git history as the stored one
///   (`generation`), or
/// - (fallback, for refs without a generation) the new `synced_at` is
///   newer-than-or-equal to the stored one.
///
/// `generation` (the commit's history depth) is the primary signal: recency
/// follows the commit's place in history, not the builder's clock, so two
/// builders with skewed clocks still order correctly. `synced_at` is the
/// fallback for refs written before `generation` existed. A missing value on
/// either side defers to the backend's atomic tie-break (the SQL conditional
/// upsert, the S3 ETag CAS); the file store serializes writes in-process.
///
/// Force-push rewinds: an *older* commit has a lower generation, so this guard
/// would reject it. Both flavors are handled upstream in the sync path by
/// clearing generation so the fresh `synced_at` wins:
/// - a rewind to an *already-built* commit — `reuse_existing_build` re-points
///   authoritatively;
/// - a rewind to a commit *never built as a tip* — it is built fresh, and
///   `build_and_publish_two_phase` re-confirms via `ls-remote` that the freshly
///   built commit is still the branch tip before clearing generation, so the
///   confirmed-tip build wins regardless of history depth.
///
/// A build that is genuinely stale (upstream moved on during the build, so the
/// re-check no longer sees it as the tip) keeps its generation and correctly
/// loses here — recency by observation is only granted to a *confirmed* tip.
pub(crate) fn should_replace_ref(existing: Option<&RefInfo>, new: &RefInfo) -> bool {
    if new.commit.is_empty() {
        return false;
    }
    let Some(existing) = existing else {
        return true;
    };
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

/// Abstract store for repo → `RefInfo` mappings.
///
/// Implementations are expected to be shared across multiple ripclone backend
/// instances. Reads may be cached by the wrapping `CachingRefStore`; writes are
/// always durably persisted first.
#[async_trait]
pub trait RefStore: Send + Sync {
    /// Load the HEAD `RefInfo` for a repo, if one exists.
    async fn load(&self, repo_id: &RepoId) -> Result<Option<RefInfo>>;

    /// Save the HEAD `RefInfo` for a repo.
    async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()>;

    /// List all repos that have a stored `RefInfo`.
    ///
    /// Stored keys are workspace-qualified as `{workspace}/{escaped_path}`.
    async fn list(&self) -> Result<Vec<RepoId>>;

    /// Load the `RefInfo` for a specific branch.
    async fn load_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RefInfo>>;

    /// Load a *completed full build* for an exact commit, from any branch of this
    /// repo (commit-keyed reuse). Lets a sync of branch `bar` reuse a clonepack
    /// branch `foo` already built at the same commit, instead of rebuilding.
    ///
    /// Default is `Ok(None)` — stores without an efficient commit index (file,
    /// S3) simply fall back to the branch-scoped no-op, no regression. Returns a
    /// `RefInfo` only when its `full_clonepack` is present and matches `commit`.
    async fn load_build(&self, _repo_id: &RepoId, _commit: &str) -> Result<Option<RefInfo>> {
        Ok(None)
    }

    /// Save the `RefInfo` for a specific branch.
    async fn save_branch(&self, repo_id: &RepoId, branch: &str, info: &RefInfo) -> Result<()>;

    /// Update only `build_status` when the stored ref still points at
    /// `expected_commit`. Returns `false` when the row is absent or has moved.
    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool>;

    /// Update only `last_accessed_at` to the current time when the stored ref
    /// still points at `expected_commit`. Returns `false` when the row is absent
    /// or has moved. Implementations must be atomic (read-modify-write with a
    /// commit check) so a concurrent sync does not lose its newer metadata.
    async fn touch_last_accessed_at(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
    ) -> Result<bool>;

    /// Delete the stored `RefInfo` for a branch (e.g. on a webhook
    /// branch-delete). Idempotent: removing a branch that isn't stored is `Ok`.
    /// The default is a no-op for stores that don't support deletion.
    async fn delete_branch(&self, _repo_id: &RepoId, _branch: &str) -> Result<()> {
        Ok(())
    }

    /// List all branches that have a stored `RefInfo` for this repo.
    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>>;

    /// Persist that this repo is explicitly available to clone. This state is
    /// separate from refs/artifacts: an added repo may be cold or still building.
    async fn add_repo(&self, repo: &AddedRepo) -> Result<()>;

    /// Load added-repo state, if this repo was explicitly made available.
    async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>>;

    /// Remove added-repo state. Idempotent.
    async fn remove_added_repo(&self, _repo_id: &RepoId) -> Result<()> {
        Ok(())
    }

    /// List every repo explicitly made available.
    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>>;

    /// Pin the initialization target exactly once. Returns false for a missing,
    /// active/failed, different-branch, or already-pinned-to-another-commit row.
    async fn pin_repo_initialization(
        &self,
        repo_id: &RepoId,
        branch: &str,
        commit: &str,
    ) -> Result<bool> {
        let Some(mut repo) = self.load_added_repo(repo_id).await? else {
            return Ok(false);
        };
        let branch_matches = repo.initialization_branch.as_deref() == Some(branch)
            || (repo.initialization_branch.as_deref() == Some("HEAD")
                && repo.initialization_target.is_none());
        if repo.state != RepoLifecycleState::Initializing
            || !branch_matches
            || repo
                .initialization_target
                .as_deref()
                .is_some_and(|target| target != commit)
        {
            return Ok(false);
        }
        repo.initialization_branch = Some(branch.to_string());
        repo.initialization_target
            .get_or_insert_with(|| commit.to_string());
        self.add_repo(&repo).await?;
        Ok(true)
    }

    /// Admit an initializing repo only when branch and pinned commit match.
    async fn activate_repo(&self, repo_id: &RepoId, branch: &str, commit: &str) -> Result<bool> {
        let Some(mut repo) = self.load_added_repo(repo_id).await? else {
            return Ok(false);
        };
        if repo.state != RepoLifecycleState::Initializing
            || repo.initialization_branch.as_deref() != Some(branch)
            || repo.initialization_target.as_deref() != Some(commit)
        {
            return Ok(false);
        }
        repo.state = RepoLifecycleState::Active;
        repo.activated_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs());
        repo.failure = None;
        self.add_repo(&repo).await?;
        Ok(true)
    }

    /// Mark only the matching initialization attempt failed. Stale failures
    /// cannot demote an active repo or poison a newer attempt.
    async fn fail_repo_initialization(
        &self,
        repo_id: &RepoId,
        branch: &str,
        commit: Option<&str>,
        failure: &str,
    ) -> Result<bool> {
        let Some(mut repo) = self.load_added_repo(repo_id).await? else {
            return Ok(false);
        };
        if repo.state != RepoLifecycleState::Initializing
            || repo.initialization_branch.as_deref() != Some(branch)
            || commit.is_some_and(|commit| repo.initialization_target.as_deref() != Some(commit))
        {
            return Ok(false);
        }
        repo.state = RepoLifecycleState::Failed;
        repo.failure = Some(failure.to_string());
        self.add_repo(&repo).await?;
        Ok(true)
    }

    /// Drop any cached entry for this branch so the next load reads through to
    /// the backing store. Needed after a build completes in *another* process
    /// (the SQL queue / standalone worker path): this process's cache would
    /// otherwise keep serving a stale ref until its TTL expires. The default is
    /// a no-op for stores that don't cache.
    async fn invalidate(&self, _repo_id: &RepoId, _branch: &str) {}

    /// Cheap readiness probe used by `/readyz`. Should confirm the store is
    /// reachable without listing everything. Default assumes healthy; any new
    /// durable/remote backend MUST override this so readiness doesn't silently
    /// report "ready" while the store is unreachable.
    async fn health(&self) -> Result<()> {
        Ok(())
    }
}

/// Local filesystem-backed ref store. One JSON file per repo.
pub struct FileRefStore {
    root: PathBuf,
    added_root: PathBuf,
    /// Serializes the read-compare-then-rename below so concurrent in-process
    /// writers can't both read the old ref and then race their renames (which
    /// would let an older sync clobber a newer one, and made two writers fight
    /// over the shared `.json.tmp` path). The file store is per-host by design;
    /// in-process serialization is the right scope. Ref writes are off the
    /// clone hot path, so a single lock is fine.
    write_lock: tokio::sync::Mutex<()>,
}

impl FileRefStore {
    pub fn new(repo_root: &Path) -> Self {
        let root = repo_root.join(".ripclone-refs");
        let added_root = repo_root.join(".ripclone-added");
        // Best-effort creation of the root directory up front so `list()` works
        // on a fresh deployment.
        let _ = std::fs::create_dir_all(&root);
        let _ = std::fs::create_dir_all(&added_root);
        Self {
            root,
            added_root,
            write_lock: tokio::sync::Mutex::new(()),
        }
    }

    async fn mutate_added_repo(
        &self,
        repo_id: &RepoId,
        mutate: impl FnOnce(&mut AddedRepo) -> bool,
    ) -> Result<bool> {
        let _guard = self.write_lock.lock().await;
        let path = self.added_path(repo_id);
        let mut repo: AddedRepo = match tokio::fs::read(&path).await {
            Ok(data) => serde_json::from_slice(&data)
                .with_context(|| format!("parse added repo {}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(anyhow::anyhow!("read added repo {}: {}", path.display(), e)),
        };
        if !mutate(&mut repo) {
            return Ok(false);
        }
        let data = serde_json::to_vec_pretty(&repo).context("serialize added repo")?;
        let tmp_path = path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, data).await?;
        tokio::fs::rename(&tmp_path, &path).await?;
        Ok(true)
    }

    /// Read-compare-then-rename: apply [`should_replace_ref`] against whatever
    /// is currently on disk and only write when the new ref wins. Held under
    /// `write_lock` so the compare and the rename are atomic w.r.t. other
    /// in-process writers.
    async fn write_checked(&self, path: &Path, info: &RefInfo) -> Result<()> {
        let _guard = self.write_lock.lock().await;

        let existing: Option<RefInfo> = match tokio::fs::read(path).await {
            Ok(data) => Some(
                serde_json::from_slice(&data)
                    .with_context(|| format!("parse ref store {}", path.display()))?,
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(anyhow::anyhow!("read ref store {}: {}", path.display(), e)),
        };
        if !should_replace_ref(existing.as_ref(), info) {
            let key = path.display();
            warn!("ref store {key} already has a newer sync; skipping older write");
            return Ok(());
        }

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create ref store dir {}", parent.display()))?;
        }
        let data = serde_json::to_vec_pretty(info).context("serialize RefInfo")?;
        // Write to a temp file in the same directory and rename for atomicity.
        let tmp_path = path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, data)
            .await
            .with_context(|| format!("write temp ref store {}", tmp_path.display()))?;
        tokio::fs::rename(&tmp_path, path)
            .await
            .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
        Ok(())
    }

    async fn update_status_checked(
        &self,
        path: &Path,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool> {
        let _guard = self.write_lock.lock().await;

        let mut info: RefInfo = match tokio::fs::read(path).await {
            Ok(data) => serde_json::from_slice(&data)
                .with_context(|| format!("parse ref store {}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(anyhow::anyhow!("read ref store {}: {}", path.display(), e)),
        };
        if info.commit != expected_commit {
            return Ok(false);
        }

        info.build_status = Some(status.to_string());
        let data = serde_json::to_vec_pretty(&info).context("serialize RefInfo")?;
        let tmp_path = path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, data)
            .await
            .with_context(|| format!("write temp ref store {}", tmp_path.display()))?;
        tokio::fs::rename(&tmp_path, path)
            .await
            .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
        Ok(true)
    }

    async fn update_last_accessed_checked(
        &self,
        path: &Path,
        expected_commit: &str,
    ) -> Result<bool> {
        let _guard = self.write_lock.lock().await;

        let mut info: RefInfo = match tokio::fs::read(path).await {
            Ok(data) => serde_json::from_slice(&data)
                .with_context(|| format!("parse ref store {}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(anyhow::anyhow!("read ref store {}: {}", path.display(), e)),
        };
        if info.commit != expected_commit {
            return Ok(false);
        }

        info.last_accessed_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs());
        let data = serde_json::to_vec_pretty(&info).context("serialize RefInfo")?;
        let tmp_path = path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, data)
            .await
            .with_context(|| format!("write temp ref store {}", tmp_path.display()))?;
        tokio::fs::rename(&tmp_path, path)
            .await
            .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
        Ok(true)
    }

    /// Path to the legacy HEAD ref file. Kept for backward compatibility with
    /// refs created before branch-specific storage.
    fn path(&self, repo_id: &RepoId) -> PathBuf {
        let key = repo_id.storage_key();
        let (owner, repo) = key
            .split_once('/')
            .expect("RepoId storage_key always contains a slash");
        self.root.join(owner).join(format!("{}.json", repo))
    }

    fn branch_dir(&self, repo_id: &RepoId) -> PathBuf {
        let key = repo_id.storage_key();
        let (owner, repo) = key
            .split_once('/')
            .expect("RepoId storage_key always contains a slash");
        self.root.join(owner).join(repo)
    }

    fn branch_path(&self, repo_id: &RepoId, branch: &str) -> PathBuf {
        self.branch_dir(repo_id)
            .join(format!("{}.json", branch_slug(branch)))
    }

    fn added_path(&self, repo_id: &RepoId) -> PathBuf {
        let key = repo_id.storage_key();
        let (provider, repo) = key
            .split_once('/')
            .expect("RepoId storage_key always contains a slash");
        self.added_root.join(provider).join(format!("{repo}.json"))
    }
}

#[async_trait]
impl RefStore for FileRefStore {
    async fn load(&self, repo_id: &RepoId) -> Result<Option<RefInfo>> {
        let path = self.path(repo_id);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let info = serde_json::from_slice(&data)
                    .with_context(|| format!("parse ref store {}", path.display()))?;
                Ok(Some(info))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::anyhow!("read ref store {}: {}", path.display(), e)),
        }
    }

    async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()> {
        self.write_checked(&self.path(repo_id), info).await
    }

    async fn list(&self) -> Result<Vec<RepoId>> {
        let mut out = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.root).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let owner = entry.file_name().to_string_lossy().to_string();
            let mut repos = tokio::fs::read_dir(entry.path()).await?;
            while let Some(repo_entry) = repos.next_entry().await? {
                let ft = repo_entry.file_type().await?;
                let name = repo_entry.file_name().to_string_lossy().to_string();
                let key = if ft.is_file() {
                    let Some(repo) = name.strip_suffix(".json") else {
                        continue;
                    };
                    format!("{owner}/{repo}")
                } else if ft.is_dir() {
                    format!("{owner}/{name}")
                } else {
                    continue;
                };
                if let Some(repo_id) = parse_storage_key(&key) {
                    out.push(repo_id);
                }
            }
        }
        Ok(out)
    }

    async fn load_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RefInfo>> {
        if branch == "HEAD" {
            return self.load(repo_id).await;
        }
        let path = self.branch_path(repo_id, branch);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let info = serde_json::from_slice(&data)
                    .with_context(|| format!("parse ref store {}", path.display()))?;
                Ok(Some(info))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::anyhow!("read ref store {}: {}", path.display(), e)),
        }
    }

    async fn save_branch(&self, repo_id: &RepoId, branch: &str, info: &RefInfo) -> Result<()> {
        if branch == "HEAD" {
            return self.save(repo_id, info).await;
        }
        self.write_checked(&self.branch_path(repo_id, branch), info)
            .await
    }

    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool> {
        let path = if branch == "HEAD" {
            self.path(repo_id)
        } else {
            self.branch_path(repo_id, branch)
        };
        self.update_status_checked(&path, expected_commit, status)
            .await
    }

    async fn touch_last_accessed_at(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
    ) -> Result<bool> {
        let path = if branch == "HEAD" {
            self.path(repo_id)
        } else {
            self.branch_path(repo_id, branch)
        };
        self.update_last_accessed_checked(&path, expected_commit)
            .await
    }

    async fn delete_branch(&self, repo_id: &RepoId, branch: &str) -> Result<()> {
        // HEAD is the repo's default ref; a branch-delete webhook never targets
        // it, and removing it would orphan the repo. Refuse.
        if branch == "HEAD" {
            return Ok(());
        }
        let path = self.branch_path(repo_id, branch);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            // Already gone — deletion is idempotent.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(anyhow::anyhow!(
                "delete ref store {}: {}",
                path.display(),
                e
            )),
        }
    }

    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        let mut out = Vec::new();
        if self.path(repo_id).exists() {
            out.push("HEAD".to_string());
        }
        let dir = self.branch_dir(repo_id);
        if dir.exists() {
            let mut entries = tokio::fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                if !entry.file_type().await?.is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(slug) = name.strip_suffix(".json")
                    && let Some(branch) = unbranch_slug(slug)
                {
                    out.push(branch);
                }
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn add_repo(&self, repo: &AddedRepo) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let path = self.added_path(&repo.repo_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create added repo dir {}", parent.display()))?;
        }
        let data = serde_json::to_vec_pretty(repo).context("serialize added repo")?;
        let tmp_path = path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, data)
            .await
            .with_context(|| format!("write temp added repo {}", tmp_path.display()))?;
        tokio::fs::rename(&tmp_path, &path)
            .await
            .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
        Ok(())
    }

    async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>> {
        let path = self.added_path(repo_id);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let repo = serde_json::from_slice(&data)
                    .with_context(|| format!("parse added repo {}", path.display()))?;
                Ok(Some(repo))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::anyhow!("read added repo {}: {}", path.display(), e)),
        }
    }

    async fn remove_added_repo(&self, repo_id: &RepoId) -> Result<()> {
        let path = self.added_path(repo_id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(anyhow::anyhow!(
                "delete added repo {}: {}",
                path.display(),
                e
            )),
        }
    }

    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>> {
        let mut out = Vec::new();
        if !self.added_root.exists() {
            return Ok(out);
        }
        let mut providers = tokio::fs::read_dir(&self.added_root).await?;
        while let Some(provider) = providers.next_entry().await? {
            if !provider.file_type().await?.is_dir() {
                continue;
            }
            let mut repos = tokio::fs::read_dir(provider.path()).await?;
            while let Some(repo) = repos.next_entry().await? {
                if !repo.file_type().await?.is_file() {
                    continue;
                }
                let name = repo.file_name().to_string_lossy().to_string();
                if !name.ends_with(".json") {
                    continue;
                }
                let data = tokio::fs::read(repo.path()).await?;
                let added: AddedRepo = serde_json::from_slice(&data)
                    .with_context(|| format!("parse added repo {}", repo.path().display()))?;
                out.push(added);
            }
        }
        Ok(out)
    }

    async fn pin_repo_initialization(
        &self,
        repo_id: &RepoId,
        branch: &str,
        commit: &str,
    ) -> Result<bool> {
        self.mutate_added_repo(repo_id, |repo| {
            let branch_matches = repo.initialization_branch.as_deref() == Some(branch)
                || (repo.initialization_branch.as_deref() == Some("HEAD")
                    && repo.initialization_target.is_none());
            if repo.state != RepoLifecycleState::Initializing
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

    async fn activate_repo(&self, repo_id: &RepoId, branch: &str, commit: &str) -> Result<bool> {
        self.mutate_added_repo(repo_id, |repo| {
            if repo.state != RepoLifecycleState::Initializing
                || repo.initialization_branch.as_deref() != Some(branch)
                || repo.initialization_target.as_deref() != Some(commit)
            {
                return false;
            }
            repo.state = RepoLifecycleState::Active;
            repo.activated_at = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
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
        branch: &str,
        commit: Option<&str>,
        failure: &str,
    ) -> Result<bool> {
        self.mutate_added_repo(repo_id, |repo| {
            if repo.state != RepoLifecycleState::Initializing
                || repo.initialization_branch.as_deref() != Some(branch)
                || commit
                    .is_some_and(|commit| repo.initialization_target.as_deref() != Some(commit))
            {
                return false;
            }
            repo.state = RepoLifecycleState::Failed;
            repo.failure = Some(failure.to_string());
            true
        })
        .await
    }

    async fn load_build(&self, repo_id: &RepoId, commit: &str) -> Result<Option<RefInfo>> {
        // Commit-keyed reuse: scan branch refs for any completed full build at this
        // commit. This is a cold fallback (only invoked when the requesting branch
        // lacks a usable build), so a directory scan is acceptable.
        let branches = self.list_branches(repo_id).await?;
        for branch in branches {
            if let Some(info) = self.load_branch(repo_id, &branch).await?
                && info.full_clonepack.commit == commit
                && !info.full_clonepack.manifest.is_empty()
                && !info.archive_chunks.is_empty()
            {
                return Ok(Some(info));
            }
        }
        Ok(None)
    }

    async fn health(&self) -> Result<()> {
        // Write+read probe of the ref-store root, off the async worker. Catches
        // an unmounted/removed/read-only/full data volume — not just a missing
        // dir.
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            use std::io::Write;
            let mut f = tempfile::Builder::new()
                .prefix(".readyz-probe-")
                .tempfile_in(&root)
                .with_context(|| format!("ref-store root not writable: {}", root.display()))?;
            f.write_all(b"ok")
                .with_context(|| format!("ref-store root write failed: {}", root.display()))?;
            f.flush()
                .with_context(|| format!("ref-store root flush failed: {}", root.display()))?;
            Ok(())
        })
        .await
        .context("ref-store health probe failed to run")?
    }
}

/// S3-compatible ref store. Stores one object per repo under
/// `{prefix}refs/{workspace}/{escaped_path}.json`.
pub struct S3RefStore {
    storage: Arc<S3Storage>,
}

impl S3RefStore {
    pub fn new(storage: Arc<S3Storage>) -> Self {
        Self { storage }
    }

    fn key(&self, repo_id: &RepoId) -> String {
        format!("refs/{}.json", repo_id.storage_key())
    }

    fn branch_key(&self, repo_id: &RepoId, branch: &str) -> String {
        format!(
            "refs/{}/{}.json",
            repo_id.storage_key(),
            branch_slug(branch)
        )
    }

    fn branch_prefix(&self, repo_id: &RepoId) -> String {
        format!("refs/{}/", repo_id.storage_key())
    }

    fn added_key(&self, repo_id: &RepoId) -> String {
        format!("added_repos/{}.json", repo_id.storage_key())
    }

    /// Read-compare-CAS write shared by HEAD (`save`) and branch
    /// (`save_branch`) saves. Applies [`should_replace_ref`] and uses the
    /// object's ETag as the atomic tie-break against a concurrent writer:
    /// if another sync lands between our read and write, the conditional PUT
    /// fails and we re-read and re-decide. This is what makes branch refs
    /// "newer never loses" instead of last-writer-wins.
    async fn save_keyed(&self, key: &str, info: &RefInfo) -> Result<()> {
        let data = serde_json::to_vec_pretty(info).context("serialize RefInfo")?;
        // Bounded so a pathological hot key can't spin forever. Real S3-backed
        // metadata can see bursts of concurrent first writers, so allow enough
        // conflicts for every contender in the adversarial ordering test to
        // observe the winning ref and stand down.
        for attempt in 0..64 {
            let if_match = match self.storage.get_object(key).await {
                Ok(Some((etag, existing_bytes))) => {
                    let existing: RefInfo = serde_json::from_slice(&existing_bytes)
                        .with_context(|| format!("parse existing S3 ref store {key}"))?;
                    if !should_replace_ref(Some(&existing), info) {
                        warn!("S3 ref store {key} already has a newer sync; skipping older write");
                        return Ok(());
                    }
                    Some(etag)
                }
                Ok(None) => None,
                Err(e) => {
                    warn!(
                        "failed to read existing S3 ref store {key}: {e}; writing unconditionally"
                    );
                    None
                }
            };

            if self
                .storage
                .put_object_cas(key, &data, if_match.as_deref())
                .await
                .with_context(|| format!("write S3 ref store {key}"))?
            {
                return Ok(());
            }
            // Lost the CAS race against a concurrent writer; re-read and retry.
            tokio::time::sleep(Duration::from_millis((attempt.min(10) + 1) as u64)).await;
        }
        anyhow::bail!("S3 ref store {key}: gave up after repeated concurrent-write conflicts")
    }

    async fn update_status_keyed(
        &self,
        key: &str,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool> {
        for attempt in 0..64 {
            let (etag, mut info) = match self.storage.get_object(key).await {
                Ok(Some((etag, existing_bytes))) => {
                    let info: RefInfo = serde_json::from_slice(&existing_bytes)
                        .with_context(|| format!("parse existing S3 ref store {key}"))?;
                    (etag, info)
                }
                Ok(None) => return Ok(false),
                Err(e) => return Err(e).with_context(|| format!("load S3 ref store {key}")),
            };
            if info.commit != expected_commit {
                return Ok(false);
            }
            info.build_status = Some(status.to_string());
            let data = serde_json::to_vec_pretty(&info).context("serialize RefInfo")?;
            if self
                .storage
                .put_object_cas(key, &data, Some(&etag))
                .await
                .with_context(|| format!("write S3 ref store {key}"))?
            {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis((attempt.min(10) + 1) as u64)).await;
        }
        anyhow::bail!("S3 ref store {key}: gave up after repeated status-write conflicts")
    }

    async fn update_last_accessed_keyed(&self, key: &str, expected_commit: &str) -> Result<bool> {
        for attempt in 0..64 {
            let (etag, mut info) = match self.storage.get_object(key).await {
                Ok(Some((etag, existing_bytes))) => {
                    let info: RefInfo = serde_json::from_slice(&existing_bytes)
                        .with_context(|| format!("parse existing S3 ref store {key}"))?;
                    (etag, info)
                }
                Ok(None) => return Ok(false),
                Err(e) => return Err(e).with_context(|| format!("load S3 ref store {key}")),
            };
            if info.commit != expected_commit {
                return Ok(false);
            }
            info.last_accessed_at = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|d| d.as_secs());
            let data = serde_json::to_vec_pretty(&info).context("serialize RefInfo")?;
            if self
                .storage
                .put_object_cas(key, &data, Some(&etag))
                .await
                .with_context(|| format!("write S3 ref store {key}"))?
            {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis((attempt.min(10) + 1) as u64)).await;
        }
        anyhow::bail!("S3 ref store {key}: gave up after repeated last-accessed-write conflicts")
    }
}

#[async_trait]
impl RefStore for S3RefStore {
    async fn load(&self, repo_id: &RepoId) -> Result<Option<RefInfo>> {
        let key = self.key(repo_id);
        match self.storage.get_object(&key).await {
            Ok(Some((_, data))) => {
                let info = serde_json::from_slice(&data)
                    .with_context(|| format!("parse S3 ref store {key}"))?;
                Ok(Some(info))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("load S3 ref store {key}")),
        }
    }

    async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()> {
        self.save_keyed(&self.key(repo_id), info).await
    }

    async fn list(&self) -> Result<Vec<RepoId>> {
        let prefix = "refs/";
        let keys = self
            .storage
            .list_objects(prefix)
            .await
            .context("list S3 ref store")?;
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for key in keys {
            let Some(rest) = key.strip_prefix(prefix) else {
                continue;
            };
            let Some((provider, tail)) = rest.split_once('/') else {
                continue;
            };
            let repo_key = if let Some(repo_file) = tail.strip_suffix(".json") {
                format!("{provider}/{repo_file}")
            } else if let Some((repo, _branch_file)) = tail.split_once('/') {
                format!("{provider}/{repo}")
            } else {
                continue;
            };
            if seen.insert(repo_key.clone())
                && let Some(repo_id) = parse_storage_key(&repo_key)
            {
                out.push(repo_id);
            }
        }
        Ok(out)
    }

    async fn load_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RefInfo>> {
        if branch == "HEAD" {
            return self.load(repo_id).await;
        }
        let key = self.branch_key(repo_id, branch);
        match self.storage.get_object(&key).await {
            Ok(Some((_, data))) => {
                let info = serde_json::from_slice(&data)
                    .with_context(|| format!("parse S3 ref store {key}"))?;
                Ok(Some(info))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("load S3 ref store {key}")),
        }
    }

    async fn save_branch(&self, repo_id: &RepoId, branch: &str, info: &RefInfo) -> Result<()> {
        if branch == "HEAD" {
            return self.save(repo_id, info).await;
        }
        self.save_keyed(&self.branch_key(repo_id, branch), info)
            .await
    }

    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool> {
        let key = if branch == "HEAD" {
            self.key(repo_id)
        } else {
            self.branch_key(repo_id, branch)
        };
        self.update_status_keyed(&key, expected_commit, status)
            .await
    }

    async fn touch_last_accessed_at(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
    ) -> Result<bool> {
        let key = if branch == "HEAD" {
            self.key(repo_id)
        } else {
            self.branch_key(repo_id, branch)
        };
        self.update_last_accessed_keyed(&key, expected_commit).await
    }

    async fn delete_branch(&self, repo_id: &RepoId, branch: &str) -> Result<()> {
        // Never delete the HEAD ref (see FileRefStore::delete_branch).
        if branch == "HEAD" {
            return Ok(());
        }
        let key = self.branch_key(repo_id, branch);
        self.storage
            .delete_object(&key)
            .await
            .with_context(|| format!("delete S3 ref store {key}"))
    }

    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        let mut out = Vec::new();
        if self.load(repo_id).await?.is_some() {
            out.push("HEAD".to_string());
        }
        let prefix = self.branch_prefix(repo_id);
        let keys = self
            .storage
            .list_objects(&prefix)
            .await
            .context("list S3 ref store branches")?;
        for key in keys {
            let Some(file) = key.strip_prefix(&prefix) else {
                continue;
            };
            let Some(slug) = file.strip_suffix(".json") else {
                continue;
            };
            if let Some(branch) = unbranch_slug(slug) {
                out.push(branch);
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn add_repo(&self, repo: &AddedRepo) -> Result<()> {
        let data = serde_json::to_vec_pretty(repo).context("serialize added repo")?;
        self.storage
            .put_object(&self.added_key(&repo.repo_id), &data, None)
            .await
            .with_context(|| format!("write S3 added repo {}", repo.repo_id.storage_key()))?;
        Ok(())
    }

    async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>> {
        let key = self.added_key(repo_id);
        match self.storage.get_object(&key).await {
            Ok(Some((_, data))) => {
                let repo = serde_json::from_slice(&data)
                    .with_context(|| format!("parse S3 added repo {key}"))?;
                Ok(Some(repo))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("load S3 added repo {key}")),
        }
    }

    async fn remove_added_repo(&self, repo_id: &RepoId) -> Result<()> {
        let key = self.added_key(repo_id);
        self.storage
            .delete_object(&key)
            .await
            .with_context(|| format!("delete S3 added repo {key}"))
    }

    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>> {
        let keys = self
            .storage
            .list_objects("added_repos/")
            .await
            .context("list S3 added repos")?;
        let mut out = Vec::new();
        for key in keys {
            if !key.ends_with(".json") {
                continue;
            }
            if let Ok(Some((_, data))) = self.storage.get_object(&key).await {
                out.push(
                    serde_json::from_slice(&data)
                        .with_context(|| format!("parse S3 added repo {key}"))?,
                );
            }
        }
        Ok(out)
    }

    async fn load_build(&self, repo_id: &RepoId, commit: &str) -> Result<Option<RefInfo>> {
        // Commit-keyed reuse: scan branch refs for any completed full build at this
        // commit. This is a cold fallback (only invoked when the requesting branch
        // lacks a usable build), so an S3 list + per-key GET is acceptable.
        let prefix = self.branch_prefix(repo_id);
        let keys = self
            .storage
            .list_objects(&prefix)
            .await
            .context("list S3 ref store for commit-keyed reuse")?;
        for key in keys {
            if !key.ends_with(".json") || key == format!("{}HEAD.json", prefix) {
                continue;
            }
            if let Ok(Some((_, data))) = self.storage.get_object(&key).await
                && let Ok(info) = serde_json::from_slice::<RefInfo>(&data)
                && info.full_clonepack.commit == commit
                && !info.full_clonepack.manifest.is_empty()
                && !info.archive_chunks.is_empty()
            {
                return Ok(Some(info));
            }
        }
        Ok(None)
    }

    async fn health(&self) -> Result<()> {
        // Reachability probe with a prefix that matches nothing, bounded by a
        // short timeout. The readiness handler caches the result.
        match tokio::time::timeout(
            Duration::from_secs(3),
            self.storage.list_objects("__ripclone_readyz_probe__/none/"),
        )
        .await
        {
            Ok(r) => r.map(|_| ()).context("S3 ref-store unreachable"),
            Err(_) => anyhow::bail!("S3 ref-store health check timed out"),
        }
    }
}

/// How long a branch-delete tombstone suppresses a stale build's publish. Must
/// exceed the longest realistic build so an in-flight build that fetched before
/// the delete cannot outlive its tombstone and resurrect the branch. A genuine
/// re-create (a new push fetched *after* the delete) is admitted regardless of
/// this window because its `synced_at` beats the tombstone.
const DELETE_TOMBSTONE_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// In-memory caching wrapper around a `RefStore`.
pub struct CachingRefStore<T: RefStore> {
    inner: T,
    ttl: Duration,
    cache: RwLock<HashMap<(String, String), (Instant, RefInfo)>>,
    /// Monotonic generation counter. Incremented before every write so a read
    /// that overlaps with a write can detect the overlap and avoid caching a
    /// value that may be stale by the time it acquires the write lock.
    write_gen: AtomicU64,
    /// Branch-delete tombstones: `(repo, branch) -> (deleted-at unix secs,
    /// recorded Instant)`. A webhook branch-delete records one here so a build
    /// that was already in flight when the delete landed cannot re-create the
    /// ref by publishing after it. `save_branch` drops any write whose ordering
    /// timestamp is at-or-before the delete; a later legitimate re-create clears
    /// the tombstone. The `Instant` bounds the map so tombstones for branches
    /// that never come back are pruned after [`DELETE_TOMBSTONE_TTL`].
    tombstones: RwLock<HashMap<(String, String), (u64, Instant)>>,
}

impl<T: RefStore> CachingRefStore<T> {
    pub fn new(inner: T) -> Self {
        let ttl = std::env::var("RIPCLONE_REF_CACHE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30));
        Self {
            inner,
            ttl,
            cache: RwLock::new(HashMap::new()),
            write_gen: AtomicU64::new(0),
            tombstones: RwLock::new(HashMap::new()),
        }
    }

    fn cache_key(repo_id: &RepoId, branch: &str) -> (String, String) {
        (repo_id.storage_key(), branch.to_string())
    }

    /// Whether a stale build's write for `key` must be dropped to avoid
    /// resurrecting a just-deleted branch. True when a live tombstone exists and
    /// the write's ordering timestamp (`synced_at`, stamped at fetch time) is
    /// at-or-before the delete — i.e. this build fetched before the branch was
    /// removed. A re-create fetched after the delete has a newer `synced_at` and
    /// is admitted. Expired tombstones are pruned here.
    async fn write_blocked_by_tombstone(&self, key: &(String, String), info: &RefInfo) -> bool {
        let mut tombstones = self.tombstones.write().await;
        tombstones.retain(|_, (_, seen)| seen.elapsed() < DELETE_TOMBSTONE_TTL);
        match tombstones.get(key) {
            Some((deleted_at, _)) => info.synced_at.unwrap_or(0) <= *deleted_at,
            None => false,
        }
    }
}

#[async_trait]
impl<T: RefStore> RefStore for CachingRefStore<T> {
    async fn load(&self, repo_id: &RepoId) -> Result<Option<RefInfo>> {
        self.load_branch(repo_id, "HEAD").await
    }

    async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()> {
        self.save_branch(repo_id, "HEAD", info).await
    }

    async fn list(&self) -> Result<Vec<RepoId>> {
        self.inner.list().await
    }

    async fn load_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RefInfo>> {
        let key = Self::cache_key(repo_id, branch);

        // Fast read-locked path: hot ref lookups are the common case and must
        // not serialize behind writers.
        {
            let cache = self.cache.read().await;
            if let Some((ts, info)) = cache.get(&key)
                && ts.elapsed() < self.ttl
            {
                return Ok(Some(info.clone()));
            }
        }

        // Snapshot the generation before doing I/O. If any write starts or
        // completes while we are reading, the generation will advance and we
        // must not cache what we loaded — it may be stale by the time we insert.
        let write_gen_before = self.write_gen.load(Ordering::SeqCst);
        let info = self.inner.load_branch(repo_id, branch).await?;
        if let Some(info) = &info {
            let mut cache = self.cache.write().await;
            // Double-checked insert: another writer may have filled the cache
            // while we were reading through to the inner store.
            if let Some((ts, existing)) = cache.get(&key)
                && ts.elapsed() < self.ttl
            {
                return Ok(Some(existing.clone()));
            }
            // Only cache if no write happened between our read and this lock.
            // This prevents a slow reader that loaded an old value from
            // overwriting the cache with it after a concurrent writer committed
            // a newer ref (and removed the cache entry).
            if self.write_gen.load(Ordering::SeqCst) == write_gen_before {
                cache.insert(key, (Instant::now(), info.clone()));
            }
        }
        Ok(info)
    }

    async fn load_build(&self, repo_id: &RepoId, commit: &str) -> Result<Option<RefInfo>> {
        // Commit-keyed reuse is a cold fallback path; read through to the inner
        // store rather than maintaining a separate commit cache.
        self.inner.load_build(repo_id, commit).await
    }

    async fn save_branch(&self, repo_id: &RepoId, branch: &str, info: &RefInfo) -> Result<()> {
        let key = Self::cache_key(repo_id, branch);
        // A build that was already in flight when this branch was deleted must
        // not resurrect it by publishing afterward. Drop the write when a live
        // delete tombstone shadows it; a genuine re-create (fetched after the
        // delete) clears the tombstone and lands.
        if self.write_blocked_by_tombstone(&key, info).await {
            warn!(
                "ref store {}/{branch}: dropping stale build write; branch was deleted after this build fetched",
                repo_id.storage_key()
            );
            return Ok(());
        }
        // Do not hold the cache write lock across the inner store's I/O (e.g.
        // S3 CAS retries); one contended ref write must not stall every /refs
        // lookup on the node. The cache eviction happens after the durable
        // write returns. Advance the generation first so any read already in
        // flight knows not to cache its result.
        self.write_gen.fetch_add(1, Ordering::SeqCst);
        self.inner.save_branch(repo_id, branch, info).await?;
        let mut cache = self.cache.write().await;
        // The inner store may skip this write when it already holds a newer ref.
        // Caching `info` here would then serve the older one until the entry
        // expires. Drop the cache entry instead; the next load reads the kept
        // value through and refills the cache.
        cache.remove(&key);
        Ok(())
    }

    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool> {
        let key = Self::cache_key(repo_id, branch);
        self.write_gen.fetch_add(1, Ordering::SeqCst);
        let updated = self
            .inner
            .update_build_status(repo_id, branch, expected_commit, status)
            .await?;
        let mut cache = self.cache.write().await;
        cache.remove(&key);
        Ok(updated)
    }

    async fn touch_last_accessed_at(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
    ) -> Result<bool> {
        let key = Self::cache_key(repo_id, branch);
        let touched = self
            .inner
            .touch_last_accessed_at(repo_id, branch, expected_commit)
            .await?;
        if touched {
            // Best-effort: keep the cached timestamp consistent with the durable
            // store without invalidating the hot entry.
            let mut cache = self.cache.write().await;
            if let Some((ts, info)) = cache.get_mut(&key) {
                if info.commit == expected_commit {
                    info.last_accessed_at = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .ok()
                        .map(|d| d.as_secs());
                    *ts = Instant::now();
                }
            }
        }
        Ok(touched)
    }

    async fn delete_branch(&self, repo_id: &RepoId, branch: &str) -> Result<()> {
        let key = Self::cache_key(repo_id, branch);
        // Record a tombstone before deleting so a build that fetched before this
        // point cannot re-create the branch by publishing after the delete. Use
        // the same clock (`synced_at` = fetch time, epoch secs) the publish path
        // stamps, so ordering is comparable.
        let deleted_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        {
            let mut tombstones = self.tombstones.write().await;
            tombstones.retain(|_, (_, seen)| seen.elapsed() < DELETE_TOMBSTONE_TTL);
            tombstones.insert(key.clone(), (deleted_at, Instant::now()));
        }
        // Do not hold the cache write lock across the inner store's I/O.
        self.write_gen.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_branch(repo_id, branch).await?;
        let mut cache = self.cache.write().await;
        cache.remove(&key);
        Ok(())
    }

    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        self.inner.list_branches(repo_id).await
    }

    async fn add_repo(&self, repo: &AddedRepo) -> Result<()> {
        self.inner.add_repo(repo).await
    }

    async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>> {
        self.inner.load_added_repo(repo_id).await
    }

    async fn remove_added_repo(&self, repo_id: &RepoId) -> Result<()> {
        self.inner.remove_added_repo(repo_id).await
    }

    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>> {
        self.inner.list_added_repos().await
    }

    async fn invalidate(&self, repo_id: &RepoId, branch: &str) {
        self.write_gen.fetch_add(1, Ordering::SeqCst);
        {
            let mut cache = self.cache.write().await;
            cache.remove(&Self::cache_key(repo_id, branch));
        }
        self.inner.invalidate(repo_id, branch).await;
    }

    async fn health(&self) -> Result<()> {
        self.inner.health().await
    }
}

/// Migrate the legacy single-file JSON refs store into a `RefStore`.
///
/// Entries are keyed by `owner/repo/branch`; we only keep the HEAD entry for
/// each repo and ignore branch-specific entries.
pub async fn migrate_legacy_refs(store: &dyn RefStore, legacy_path: &Path) -> Result<usize> {
    if !legacy_path.exists() {
        return Ok(0);
    }
    info!("migrating legacy refs from {}", legacy_path.display());
    let data = tokio::fs::read(legacy_path)
        .await
        .with_context(|| format!("read legacy refs {}", legacy_path.display()))?;
    let refs: HashMap<String, RefInfo> = serde_json::from_slice(&data)
        .with_context(|| format!("parse legacy refs {}", legacy_path.display()))?;

    let mut migrated = 0usize;
    for (key, info) in refs {
        // Legacy keys are owner/repo/HEAD or owner/repo/branch. Only migrate
        // the HEAD entry; branch-specific entries are ignored.
        if !key.ends_with("/HEAD") {
            continue;
        }
        if let Some((owner_repo, _branch)) = key.rsplit_once('/')
            && let Some((owner, repo)) = owner_repo.split_once('/')
        {
            let repo_id = RepoId::github(format!("{owner}/{repo}"));
            // Only save if the repo doesn't already have a stored ref.
            match store.load(&repo_id).await {
                Ok(None) => {
                    store.save(&repo_id, &info).await?;
                    migrated += 1;
                }
                Ok(Some(_)) => {
                    info!("ref store already has {owner}/{repo}; skipping legacy entry");
                }
                Err(e) => {
                    warn!("failed to check ref store for {owner}/{repo}: {e}");
                }
            }
        }
    }
    info!("migrated {migrated} repos from legacy refs store");
    Ok(migrated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn dummy_ref_info(commit: &str) -> RefInfo {
        RefInfo {
            commit: commit.to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_pack: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: Vec::new(),
            packs: Vec::new(),
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            full_pack: String::new(),
            clonepack_manifest: String::new(),
            metadata_chunk: String::new(),
            archive_chunks: Vec::new(),
            full_clonepack: crate::ClonepackArtifacts::default(),
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            build_ms: None,
            synced_at: None,
            generation: None,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn file_ref_store_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("ripclone/test");

        assert!(store.load(&repo_id).await.unwrap().is_none());

        let info = dummy_ref_info("abc123");
        store.save(&repo_id, &info).await.unwrap();

        let loaded = store.load(&repo_id).await.unwrap().unwrap();
        assert_eq!(loaded.commit, "abc123");

        let list = store.list().await.unwrap();
        assert_eq!(list, vec![RepoId::github("ripclone/test")]);
    }

    #[tokio::test]
    async fn file_ref_store_touch_last_accessed_at_bumps_timestamp_and_checks_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("o/r");

        let mut info = dummy_ref_info("abc123");
        info.last_accessed_at = Some(1);
        store.save_branch(&repo_id, "main", &info).await.unwrap();

        assert!(
            store
                .touch_last_accessed_at(&repo_id, "main", "abc123")
                .await
                .unwrap(),
            "touch must succeed when the commit matches"
        );
        let touched = store.load_branch(&repo_id, "main").await.unwrap().unwrap();
        assert!(touched.last_accessed_at.unwrap() > 1);

        assert!(
            !store
                .touch_last_accessed_at(&repo_id, "main", "mismatch")
                .await
                .unwrap(),
            "touch must refuse to update when the commit has moved"
        );
        let after_mismatch = store.load_branch(&repo_id, "main").await.unwrap().unwrap();
        assert_eq!(after_mismatch.commit, "abc123");
        assert_eq!(after_mismatch.last_accessed_at, touched.last_accessed_at);
    }

    #[tokio::test]
    async fn caching_ref_store_uses_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let file_store = FileRefStore::new(tmp.path());
        let store = CachingRefStore {
            inner: file_store,
            ttl: Duration::from_secs(60),
            cache: RwLock::new(HashMap::new()),
            write_gen: AtomicU64::new(0),
            tombstones: RwLock::new(HashMap::new()),
        };

        let repo_id = RepoId::github("o/r");
        let info = dummy_ref_info("cached");
        store.save(&repo_id, &info).await.unwrap();

        let loaded = store.load(&repo_id).await.unwrap().unwrap();
        assert_eq!(loaded.commit, "cached");
    }

    /// After an older write loses at the durable layer, the cache must not keep
    /// serving it — the next read must return the newer ref.
    #[tokio::test]
    async fn caching_ref_store_does_not_serve_stale_after_losing_write() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CachingRefStore {
            inner: FileRefStore::new(tmp.path()),
            ttl: Duration::from_secs(60),
            cache: RwLock::new(HashMap::new()),
            write_gen: AtomicU64::new(0),
            tombstones: RwLock::new(HashMap::new()),
        };
        let repo_id = RepoId::github("o/r");

        let mut newer = dummy_ref_info("c2");
        newer.synced_at = Some(2);
        let mut older = dummy_ref_info("c1");
        older.synced_at = Some(1);

        store.save_branch(&repo_id, "main", &newer).await.unwrap();
        // The durable store keeps the newer ref and skips this older one.
        store.save_branch(&repo_id, "main", &older).await.unwrap();

        let loaded = store.load_branch(&repo_id, "main").await.unwrap().unwrap();
        assert_eq!(
            loaded.commit, "c2",
            "cache must not serve the older ref that lost the durable write"
        );
    }

    /// Ordering follows the commit's history depth (generation), not the
    /// builder's clock: a build with a later wall-clock but shallower history
    /// must lose to a deeper one.
    #[tokio::test]
    async fn file_ref_store_orders_by_generation_not_clock() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo = RepoId::github("o/r");

        let mut deep = dummy_ref_info("c-deep");
        deep.generation = Some(10);
        deep.synced_at = Some(1); // old clock
        store.save_branch(&repo, "main", &deep).await.unwrap();

        // Later clock, shallower history → must lose (skew immunity).
        let mut recent_shallow = dummy_ref_info("c-shallow");
        recent_shallow.generation = Some(5);
        recent_shallow.synced_at = Some(1000);
        store
            .save_branch(&repo, "main", &recent_shallow)
            .await
            .unwrap();
        assert_eq!(
            store
                .load_branch(&repo, "main")
                .await
                .unwrap()
                .unwrap()
                .commit,
            "c-deep",
            "higher generation wins over a newer wall clock"
        );

        // A genuinely deeper build wins.
        let mut deeper = dummy_ref_info("c-deeper");
        deeper.generation = Some(11);
        deeper.synced_at = Some(2);
        store.save_branch(&repo, "main", &deeper).await.unwrap();
        assert_eq!(
            store
                .load_branch(&repo, "main")
                .await
                .unwrap()
                .unwrap()
                .commit,
            "c-deeper"
        );
    }

    #[test]
    fn should_replace_ref_rejects_empty_commit_candidate() {
        let empty = dummy_ref_info("");
        assert!(
            !should_replace_ref(None, &empty),
            "an empty commit must not create a stored ref"
        );

        let existing = dummy_ref_info("real");
        assert!(
            !should_replace_ref(Some(&existing), &empty),
            "an empty commit must not replace a real ref"
        );
    }

    #[tokio::test]
    async fn file_ref_store_status_update_is_commit_guarded_and_targeted() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo = RepoId::github("o/r");

        let mut info = dummy_ref_info("new");
        info.archive_chunks = vec!["archive-a".to_string(), "archive-b".to_string()];
        info.full_clonepack.manifest = "full-manifest".to_string();
        info.full_clonepack.commit = "new".to_string();
        info.build_status = Some("full history building".to_string());
        info.generation = Some(42);
        store.save_branch(&repo, "main", &info).await.unwrap();

        assert!(
            !store
                .update_build_status(&repo, "main", "old", "failed: stale")
                .await
                .unwrap(),
            "a stale status update must not touch the stored ref"
        );
        let loaded = store.load_branch(&repo, "main").await.unwrap().unwrap();
        assert_eq!(loaded.commit, "new");
        assert_eq!(
            loaded.build_status.as_deref(),
            Some("full history building")
        );
        assert_eq!(loaded.archive_chunks, vec!["archive-a", "archive-b"]);
        assert_eq!(loaded.full_clonepack.manifest, "full-manifest");
        assert_eq!(loaded.generation, Some(42));

        assert!(
            store
                .update_build_status(&repo, "main", "new", "failed: boom")
                .await
                .unwrap(),
            "the matching commit should accept the status update"
        );
        let loaded = store.load_branch(&repo, "main").await.unwrap().unwrap();
        assert_eq!(loaded.commit, "new");
        assert_eq!(loaded.build_status.as_deref(), Some("failed: boom"));
        assert_eq!(loaded.archive_chunks, vec!["archive-a", "archive-b"]);
        assert_eq!(loaded.full_clonepack.manifest, "full-manifest");
        assert_eq!(loaded.full_clonepack.commit, "new");
        assert_eq!(loaded.generation, Some(42));
    }

    #[tokio::test]
    async fn migrate_legacy_refs_skips_branch_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(tmp.path()));

        let mut legacy = HashMap::new();
        legacy.insert(
            "ripclone/test/HEAD".to_string(),
            dummy_ref_info("head-commit"),
        );
        legacy.insert(
            "ripclone/test/feature".to_string(),
            dummy_ref_info("feature-commit"),
        );

        let legacy_path = tmp.path().join(".ripclone-refs.json");
        tokio::fs::write(&legacy_path, serde_json::to_vec(&legacy).unwrap())
            .await
            .unwrap();

        let migrated = migrate_legacy_refs(store.as_ref(), &legacy_path)
            .await
            .unwrap();
        assert_eq!(migrated, 1);

        let loaded = store
            .load(&RepoId::github("ripclone/test"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.commit, "head-commit");
    }

    #[tokio::test]
    async fn file_ref_store_branch_roundtrip_and_list() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("o/r");

        let info_main = dummy_ref_info("main-commit");
        store
            .save_branch(&repo_id, "main", &info_main)
            .await
            .unwrap();

        let info_feature = dummy_ref_info("feature-commit");
        store
            .save_branch(&repo_id, "feature/foo-bar", &info_feature)
            .await
            .unwrap();

        let loaded_main = store.load_branch(&repo_id, "main").await.unwrap().unwrap();
        assert_eq!(loaded_main.commit, "main-commit");

        let loaded_feature = store
            .load_branch(&repo_id, "feature/foo-bar")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded_feature.commit, "feature-commit");

        let mut branches = store.list_branches(&repo_id).await.unwrap();
        branches.sort();
        assert_eq!(branches, vec!["feature/foo-bar", "main"]);

        let repos = store.list().await.unwrap();
        assert_eq!(repos, vec![RepoId::github("o/r")]);
    }

    #[tokio::test]
    async fn file_ref_store_head_and_branch_coexist() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("o/r");

        let head_info = dummy_ref_info("head-commit");
        store.save(&repo_id, &head_info).await.unwrap();

        let branch_info = dummy_ref_info("branch-commit");
        store
            .save_branch(&repo_id, "main", &branch_info)
            .await
            .unwrap();

        let branches = store.list_branches(&repo_id).await.unwrap();
        assert!(branches.contains(&"HEAD".to_string()));
        assert!(branches.contains(&"main".to_string()));
    }

    #[tokio::test]
    async fn file_ref_store_added_repos_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("o/r");
        let added = AddedRepo {
            repo_id: repo_id.clone(),
            added_at: 123,
            history_enabled: true,
            source: AddedRepoSource::Cli,
            repo_size_bytes: Some(42_000),
            state: RepoLifecycleState::Active,
            initialization_branch: None,
            initialization_target: None,
            activated_at: Some(123),
            failure: None,
        };

        store.add_repo(&added).await.unwrap();
        assert_eq!(
            store.load_added_repo(&repo_id).await.unwrap(),
            Some(added.clone())
        );
        assert_eq!(store.list_added_repos().await.unwrap(), vec![added]);

        store.remove_added_repo(&repo_id).await.unwrap();
        assert!(store.load_added_repo(&repo_id).await.unwrap().is_none());
        assert!(store.list_added_repos().await.unwrap().is_empty());
    }

    #[test]
    fn added_repo_legacy_json_requires_admission_reconciliation() {
        // Rows written before size classes and admission gating must still
        // parse, but may not silently bypass the usable-full-base invariant.
        let legacy = r#"{
            "repo_id": {"provider": "github", "path": "o/r"},
            "added_at": 1,
            "history_enabled": true,
            "source": "cli"
        }"#;
        let added: AddedRepo = serde_json::from_str(legacy).unwrap();
        assert_eq!(added.repo_size_bytes, None);
        assert_eq!(added.repo_id.path, "o/r");
        assert_eq!(added.state, RepoLifecycleState::Initializing);
    }

    fn initializing_repo(repo_id: RepoId) -> AddedRepo {
        AddedRepo {
            repo_id,
            added_at: 1,
            history_enabled: true,
            source: AddedRepoSource::Api,
            repo_size_bytes: None,
            state: RepoLifecycleState::Initializing,
            initialization_branch: Some("main".into()),
            initialization_target: None,
            activated_at: None,
            failure: None,
        }
    }

    #[tokio::test]
    async fn repo_admission_requires_pinned_matching_target_and_is_monotonic() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("o/admit");
        store
            .add_repo(&initializing_repo(repo_id.clone()))
            .await
            .unwrap();

        assert!(!store.activate_repo(&repo_id, "main", "c1").await.unwrap());
        assert!(
            !store
                .pin_repo_initialization(&repo_id, "other", "c1")
                .await
                .unwrap()
        );
        assert!(
            store
                .pin_repo_initialization(&repo_id, "main", "c1")
                .await
                .unwrap()
        );
        assert!(
            !store
                .pin_repo_initialization(&repo_id, "main", "c2")
                .await
                .unwrap()
        );
        assert!(!store.activate_repo(&repo_id, "main", "c2").await.unwrap());
        assert!(store.activate_repo(&repo_id, "main", "c1").await.unwrap());

        let active = store.load_added_repo(&repo_id).await.unwrap().unwrap();
        assert!(active.is_active());
        assert_eq!(active.initialization_target.as_deref(), Some("c1"));
        assert!(active.activated_at.is_some());
        assert!(
            !store
                .fail_repo_initialization(&repo_id, "main", Some("c1"), "late")
                .await
                .unwrap()
        );
        assert!(
            store
                .load_added_repo(&repo_id)
                .await
                .unwrap()
                .unwrap()
                .is_active()
        );
    }

    #[tokio::test]
    async fn head_initialization_canonicalizes_to_resolved_default_branch_once() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("o/head");
        let mut repo = initializing_repo(repo_id.clone());
        repo.initialization_branch = Some("HEAD".into());
        store.add_repo(&repo).await.unwrap();

        assert!(
            store
                .pin_repo_initialization(&repo_id, "main", "c1")
                .await
                .unwrap()
        );
        let pinned = store.load_added_repo(&repo_id).await.unwrap().unwrap();
        assert_eq!(pinned.initialization_branch.as_deref(), Some("main"));
        assert_eq!(pinned.initialization_target.as_deref(), Some("c1"));
        assert!(
            !store
                .pin_repo_initialization(&repo_id, "trunk", "c1")
                .await
                .unwrap()
        );
        assert!(store.activate_repo(&repo_id, "main", "c1").await.unwrap());
    }

    #[tokio::test]
    async fn stale_failure_cannot_poison_initialization_and_matching_failure_is_retryable() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("o/fail");
        store
            .add_repo(&initializing_repo(repo_id.clone()))
            .await
            .unwrap();
        store
            .pin_repo_initialization(&repo_id, "main", "new")
            .await
            .unwrap();

        assert!(
            !store
                .fail_repo_initialization(&repo_id, "main", Some("old"), "stale")
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .load_added_repo(&repo_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            RepoLifecycleState::Initializing
        );
        assert!(
            store
                .fail_repo_initialization(&repo_id, "main", Some("new"), "boom")
                .await
                .unwrap()
        );
        let failed = store.load_added_repo(&repo_id).await.unwrap().unwrap();
        assert_eq!(failed.state, RepoLifecycleState::Failed);
        assert_eq!(failed.failure.as_deref(), Some("boom"));

        store
            .add_repo(&initializing_repo(repo_id.clone()))
            .await
            .unwrap();
        assert!(
            store
                .pin_repo_initialization(&repo_id, "main", "retry")
                .await
                .unwrap()
        );
        assert!(
            store
                .activate_repo(&repo_id, "main", "retry")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn concurrent_activation_and_failure_has_no_active_to_failed_transition() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(FileRefStore::new(tmp.path()));
        let repo_id = RepoId::github("o/race");
        store
            .add_repo(&initializing_repo(repo_id.clone()))
            .await
            .unwrap();
        store
            .pin_repo_initialization(&repo_id, "main", "c1")
            .await
            .unwrap();

        let activate = {
            let store = store.clone();
            let repo_id = repo_id.clone();
            tokio::spawn(async move { store.activate_repo(&repo_id, "main", "c1").await.unwrap() })
        };
        let fail = {
            let store = store.clone();
            let repo_id = repo_id.clone();
            tokio::spawn(async move {
                store
                    .fail_repo_initialization(&repo_id, "main", Some("c1"), "race")
                    .await
                    .unwrap()
            })
        };
        let _ = tokio::join!(activate, fail);
        let state = store
            .load_added_repo(&repo_id)
            .await
            .unwrap()
            .unwrap()
            .state;
        assert!(matches!(
            state,
            RepoLifecycleState::Active | RepoLifecycleState::Failed
        ));
        if state == RepoLifecycleState::Active {
            assert!(
                !store
                    .fail_repo_initialization(&repo_id, "main", Some("c1"), "late")
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn file_ref_store_delete_branch_removes_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());
        let repo_id = RepoId::github("o/r");

        store
            .save_branch(&repo_id, "feature", &dummy_ref_info("c1"))
            .await
            .unwrap();
        assert!(
            store
                .load_branch(&repo_id, "feature")
                .await
                .unwrap()
                .is_some()
        );

        // First delete removes the stored ref.
        store.delete_branch(&repo_id, "feature").await.unwrap();
        assert!(
            store
                .load_branch(&repo_id, "feature")
                .await
                .unwrap()
                .is_none()
        );

        // Deleting again (or a never-stored branch) is a no-op, not an error.
        store.delete_branch(&repo_id, "feature").await.unwrap();
        store
            .delete_branch(&repo_id, "never-existed")
            .await
            .unwrap();

        // HEAD is never deletable: a branch-delete must not orphan the repo.
        store.save(&repo_id, &dummy_ref_info("head")).await.unwrap();
        store.delete_branch(&repo_id, "HEAD").await.unwrap();
        assert!(
            store.load(&repo_id).await.unwrap().is_some(),
            "HEAD ref must survive delete_branch(HEAD)"
        );
    }

    #[tokio::test]
    async fn caching_ref_store_delete_branch_evicts_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CachingRefStore::new(FileRefStore::new(tmp.path()));
        let repo_id = RepoId::github("o/r");

        store
            .save_branch(&repo_id, "feature", &dummy_ref_info("c1"))
            .await
            .unwrap();
        // Prime the in-memory cache so a stale entry could survive a naive delete.
        assert!(
            store
                .load_branch(&repo_id, "feature")
                .await
                .unwrap()
                .is_some()
        );

        store.delete_branch(&repo_id, "feature").await.unwrap();

        // The cached copy must be evicted, not just the backing file — otherwise
        // a deleted branch would keep being served until the TTL expires.
        assert!(
            store
                .load_branch(&repo_id, "feature")
                .await
                .unwrap()
                .is_none(),
            "delete must evict the cache, not serve a stale ref"
        );
    }

    /// A build that was already in flight when a branch was deleted must not
    /// resurrect it by publishing afterward, but a genuine re-create (a new push
    /// fetched after the delete) must still land.
    #[tokio::test]
    async fn caching_ref_store_delete_tombstone_blocks_stale_build_not_recreate() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CachingRefStore::new(FileRefStore::new(tmp.path()));
        let repo_id = RepoId::github("o/r");

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // The branch existed and was then deleted (webhook branch-delete).
        let mut original = dummy_ref_info("c1");
        original.synced_at = Some(now.saturating_sub(100));
        store
            .save_branch(&repo_id, "feature", &original)
            .await
            .unwrap();
        store.delete_branch(&repo_id, "feature").await.unwrap();

        // A build that fetched BEFORE the delete publishes afterward. Its
        // ordering timestamp predates the delete, so it must not re-create.
        let mut stale = dummy_ref_info("c1");
        stale.synced_at = Some(now.saturating_sub(50));
        store
            .save_branch(&repo_id, "feature", &stale)
            .await
            .unwrap();
        assert!(
            store
                .load_branch(&repo_id, "feature")
                .await
                .unwrap()
                .is_none(),
            "a stale in-flight build must not resurrect a deleted branch"
        );

        // A genuine re-create pushed AFTER the delete (newer ordering timestamp)
        // must land.
        let mut recreate = dummy_ref_info("c2");
        recreate.synced_at = Some(now + 100);
        store
            .save_branch(&repo_id, "feature", &recreate)
            .await
            .unwrap();
        let loaded = store.load_branch(&repo_id, "feature").await.unwrap();
        assert_eq!(
            loaded.map(|r| r.commit),
            Some("c2".to_string()),
            "a legitimate re-create after delete must land"
        );
    }

    /// Test double that lets the test pause `load_branch` mid-call so a concurrent
    /// write can advance the cache generation. Cloning is shallow (Arc) so the
    /// test can keep a handle to the channels while the cache owns one copy.
    #[derive(Clone)]
    struct PauseRefStore {
        inner: Arc<FileRefStore>,
        pause: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
        resume: Arc<tokio::sync::Notify>,
    }

    impl PauseRefStore {
        fn new(inner: Arc<FileRefStore>) -> (Self, tokio::sync::oneshot::Receiver<()>) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            (
                Self {
                    inner,
                    pause: Arc::new(tokio::sync::Mutex::new(Some(tx))),
                    resume: Arc::new(tokio::sync::Notify::new()),
                },
                rx,
            )
        }
    }

    #[async_trait]
    impl RefStore for PauseRefStore {
        async fn load(&self, repo_id: &RepoId) -> Result<Option<RefInfo>> {
            self.load_branch(repo_id, "HEAD").await
        }

        async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()> {
            self.inner.save(repo_id, info).await
        }

        async fn list(&self) -> Result<Vec<RepoId>> {
            self.inner.list().await
        }

        async fn load_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RefInfo>> {
            // Read from the durable store first, then pause so the test can inject
            // a concurrent write before the caching layer decides whether to cache.
            let info = self.inner.load_branch(repo_id, branch).await?;
            if let Some(tx) = self.pause.lock().await.take() {
                let _ = tx.send(());
                self.resume.notified().await;
            }
            Ok(info)
        }

        async fn save_branch(&self, repo_id: &RepoId, branch: &str, info: &RefInfo) -> Result<()> {
            self.inner.save_branch(repo_id, branch, info).await
        }

        async fn update_build_status(
            &self,
            repo_id: &RepoId,
            branch: &str,
            expected_commit: &str,
            status: &str,
        ) -> Result<bool> {
            self.inner
                .update_build_status(repo_id, branch, expected_commit, status)
                .await
        }

        async fn touch_last_accessed_at(
            &self,
            repo_id: &RepoId,
            branch: &str,
            expected_commit: &str,
        ) -> Result<bool> {
            self.inner
                .touch_last_accessed_at(repo_id, branch, expected_commit)
                .await
        }

        async fn delete_branch(&self, repo_id: &RepoId, branch: &str) -> Result<()> {
            self.inner.delete_branch(repo_id, branch).await
        }

        async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>> {
            self.inner.list_branches(repo_id).await
        }

        async fn add_repo(&self, repo: &AddedRepo) -> Result<()> {
            self.inner.add_repo(repo).await
        }

        async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>> {
            self.inner.load_added_repo(repo_id).await
        }

        async fn remove_added_repo(&self, repo_id: &RepoId) -> Result<()> {
            self.inner.remove_added_repo(repo_id).await
        }

        async fn list_added_repos(&self) -> Result<Vec<AddedRepo>> {
            self.inner.list_added_repos().await
        }
    }

    /// A read that overlaps with a write must not insert a stale value into the
    /// cache after the writer has committed a newer ref.
    #[tokio::test]
    async fn caching_ref_store_does_not_cache_stale_value_during_concurrent_write() {
        let tmp = tempfile::tempdir().unwrap();
        let inner = Arc::new(FileRefStore::new(tmp.path()));
        let (pause_store, paused) = PauseRefStore::new(inner);
        let store = Arc::new(CachingRefStore::new(pause_store.clone()));
        let repo_id = RepoId::github("o/r");

        let mut older = dummy_ref_info("c1");
        older.synced_at = Some(1);
        let mut newer = dummy_ref_info("c2");
        newer.synced_at = Some(2);

        // Seed the durable store with the older ref.
        store.save_branch(&repo_id, "main", &older).await.unwrap();

        // Start a load that will read the older value, then pause before caching.
        let store2 = store.clone();
        let repo_id2 = repo_id.clone();
        let load = tokio::spawn(async move { store2.load_branch(&repo_id2, "main").await });

        // Wait for the load to finish the durable read and reach the pause point.
        paused.await.unwrap();

        // While the load is paused before caching, commit a newer ref. This
        // advances the cache generation and removes any cache entry.
        store.save_branch(&repo_id, "main", &newer).await.unwrap();

        // Let the paused load finish. It loaded the old value, but because a
        // write happened during the read it must not cache that stale value.
        pause_store.resume.notify_one();
        let loaded = load.await.unwrap().unwrap().unwrap();
        assert_eq!(
            loaded.commit, "c1",
            "the in-flight read returns the value it loaded"
        );

        // The next read must see the newer ref, not a stale cached copy.
        let loaded = store.load_branch(&repo_id, "main").await.unwrap().unwrap();
        assert_eq!(
            loaded.commit, "c2",
            "concurrent write must prevent stale cache insert"
        );
    }
}
