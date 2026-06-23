use crate::RefInfo;
use crate::storage::S3Storage;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn};

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

/// Abstract store for repo → `RefInfo` mappings.
///
/// Implementations are expected to be shared across multiple ripclone backend
/// instances. Reads may be cached by the wrapping `CachingRefStore`; writes are
/// always durably persisted first.
#[async_trait]
pub trait RefStore: Send + Sync {
    /// Load the HEAD `RefInfo` for a repo, if one exists.
    async fn load(&self, owner: &str, repo: &str) -> Result<Option<RefInfo>>;

    /// Save the HEAD `RefInfo` for a repo.
    async fn save(&self, owner: &str, repo: &str, info: &RefInfo) -> Result<()>;

    /// List all repos that have a stored `RefInfo`.
    async fn list(&self) -> Result<Vec<(String, String)>>;

    /// Load the `RefInfo` for a specific branch.
    async fn load_branch(&self, owner: &str, repo: &str, branch: &str) -> Result<Option<RefInfo>>;

    /// Save the `RefInfo` for a specific branch.
    async fn save_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        info: &RefInfo,
    ) -> Result<()>;

    /// List all branches that have a stored `RefInfo` for this repo.
    async fn list_branches(&self, owner: &str, repo: &str) -> Result<Vec<String>>;
}

/// Local filesystem-backed ref store. One JSON file per repo.
pub struct FileRefStore {
    root: PathBuf,
}

impl FileRefStore {
    pub fn new(repo_root: &Path) -> Self {
        let root = repo_root.join(".ripclone-refs");
        // Best-effort creation of the root directory up front so `list()` works
        // on a fresh deployment.
        let _ = std::fs::create_dir_all(&root);
        Self { root }
    }

    /// Path to the legacy HEAD ref file. Kept for backward compatibility with
    /// refs created before branch-specific storage.
    fn path(&self, owner: &str, repo: &str) -> PathBuf {
        self.root.join(owner).join(format!("{}.json", repo))
    }

    fn branch_dir(&self, owner: &str, repo: &str) -> PathBuf {
        self.root.join(owner).join(repo)
    }

    fn branch_path(&self, owner: &str, repo: &str, branch: &str) -> PathBuf {
        self.branch_dir(owner, repo)
            .join(format!("{}.json", branch_slug(branch)))
    }
}

#[async_trait]
impl RefStore for FileRefStore {
    async fn load(&self, owner: &str, repo: &str) -> Result<Option<RefInfo>> {
        let path = self.path(owner, repo);
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

    async fn save(&self, owner: &str, repo: &str, info: &RefInfo) -> Result<()> {
        let path = self.path(owner, repo);
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
        tokio::fs::rename(&tmp_path, &path)
            .await
            .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<(String, String)>> {
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
                if ft.is_file() {
                    // Legacy HEAD ref: {owner}/{repo}.json
                    if let Some(repo) = name.strip_suffix(".json") {
                        out.push((owner.clone(), repo.to_string()));
                    }
                } else if ft.is_dir() {
                    // Branch-specific refs: {owner}/{repo}/{branch_slug}.json
                    out.push((owner.clone(), name));
                }
            }
        }
        Ok(out)
    }

    async fn load_branch(&self, owner: &str, repo: &str, branch: &str) -> Result<Option<RefInfo>> {
        if branch == "HEAD" {
            return self.load(owner, repo).await;
        }
        let path = self.branch_path(owner, repo, branch);
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

    async fn save_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        info: &RefInfo,
    ) -> Result<()> {
        if branch == "HEAD" {
            return self.save(owner, repo, info).await;
        }
        let path = self.branch_path(owner, repo, branch);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create ref store dir {}", parent.display()))?;
        }
        let data = serde_json::to_vec_pretty(info).context("serialize RefInfo")?;
        let tmp_path = path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, data)
            .await
            .with_context(|| format!("write temp ref store {}", tmp_path.display()))?;
        tokio::fs::rename(&tmp_path, &path)
            .await
            .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
        Ok(())
    }

    async fn list_branches(&self, owner: &str, repo: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        if self.path(owner, repo).exists() {
            out.push("HEAD".to_string());
        }
        let dir = self.branch_dir(owner, repo);
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
}

/// S3-compatible ref store. Stores one object per repo under
/// `{prefix}refs/{owner}/{repo}.json`.
pub struct S3RefStore {
    storage: Arc<S3Storage>,
}

impl S3RefStore {
    pub fn new(storage: Arc<S3Storage>) -> Self {
        Self { storage }
    }

    fn key(&self, owner: &str, repo: &str) -> String {
        format!("refs/{owner}/{repo}.json")
    }

    fn branch_key(&self, owner: &str, repo: &str, branch: &str) -> String {
        format!("refs/{owner}/{repo}/{}.json", branch_slug(branch))
    }

    fn branch_prefix(&self, owner: &str, repo: &str) -> String {
        format!("refs/{owner}/{repo}/")
    }
}

#[async_trait]
impl RefStore for S3RefStore {
    async fn load(&self, owner: &str, repo: &str) -> Result<Option<RefInfo>> {
        let key = self.key(owner, repo);
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

    async fn save(&self, owner: &str, repo: &str, info: &RefInfo) -> Result<()> {
        let key = self.key(owner, repo);
        let data = serde_json::to_vec_pretty(info).context("serialize RefInfo")?;

        // Avoid overwriting a newer sync with an older one. Read the current
        // object and only write back if the stored ref is older/missing.
        let if_match = match self.storage.get_object(&key).await {
            Ok(Some((etag, existing_bytes))) => {
                let existing: RefInfo = serde_json::from_slice(&existing_bytes)
                    .with_context(|| format!("parse existing S3 ref store {key}"))?;
                if existing.commit == info.commit {
                    // Same commit; still update so build_status/etc can change.
                    return self
                        .storage
                        .put_object(&key, &data, Some(&etag))
                        .await
                        .with_context(|| format!("write S3 ref store {key}"));
                }
                // Timestamps are the authoritative ordering signal. A missing
                // timestamp on either side is treated as "no information" and we
                // still write, relying on the conditional ETag for safety.
                match (existing.synced_at, info.synced_at) {
                    (Some(existing_ts), Some(new_ts)) if existing_ts > new_ts => {
                        warn!(
                            "ref store for {owner}/{repo} already has newer sync at {existing_ts}; skipping older {new_ts}"
                        );
                        return Ok(());
                    }
                    _ => Some(etag),
                }
            }
            Ok(None) => None,
            Err(e) => {
                warn!("failed to read existing S3 ref store {key}: {e}; writing unconditionally");
                None
            }
        };

        self.storage
            .put_object(&key, &data, if_match.as_deref())
            .await
            .with_context(|| format!("write S3 ref store {key}"))?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<(String, String)>> {
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
            let Some((owner, tail)) = rest.split_once('/') else {
                continue;
            };
            let repo = if let Some(repo_file) = tail.strip_suffix(".json") {
                // Legacy HEAD key: refs/{owner}/{repo}.json
                repo_file.to_string()
            } else if let Some((repo, _branch_file)) = tail.split_once('/') {
                // Branch key: refs/{owner}/{repo}/{branch_slug}.json
                repo.to_string()
            } else {
                continue;
            };
            if seen.insert((owner.to_string(), repo.clone())) {
                out.push((owner.to_string(), repo));
            }
        }
        Ok(out)
    }

    async fn load_branch(&self, owner: &str, repo: &str, branch: &str) -> Result<Option<RefInfo>> {
        if branch == "HEAD" {
            return self.load(owner, repo).await;
        }
        let key = self.branch_key(owner, repo, branch);
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

    async fn save_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        info: &RefInfo,
    ) -> Result<()> {
        if branch == "HEAD" {
            return self.save(owner, repo, info).await;
        }
        let key = self.branch_key(owner, repo, branch);
        let data = serde_json::to_vec_pretty(info).context("serialize RefInfo")?;
        self.storage
            .put_object(&key, &data, None)
            .await
            .with_context(|| format!("write S3 ref store {key}"))?;
        Ok(())
    }

    async fn list_branches(&self, owner: &str, repo: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        if self.load(owner, repo).await?.is_some() {
            out.push("HEAD".to_string());
        }
        let prefix = self.branch_prefix(owner, repo);
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
}

/// In-memory caching wrapper around a `RefStore`.
pub struct CachingRefStore<T: RefStore> {
    inner: T,
    ttl: Duration,
    cache: RwLock<HashMap<(String, String, String), (Instant, RefInfo)>>,
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
        }
    }

    fn cache_key(owner: &str, repo: &str, branch: &str) -> (String, String, String) {
        (owner.to_string(), repo.to_string(), branch.to_string())
    }
}

#[async_trait]
impl<T: RefStore> RefStore for CachingRefStore<T> {
    async fn load(&self, owner: &str, repo: &str) -> Result<Option<RefInfo>> {
        self.load_branch(owner, repo, "HEAD").await
    }

    async fn save(&self, owner: &str, repo: &str, info: &RefInfo) -> Result<()> {
        self.save_branch(owner, repo, "HEAD", info).await
    }

    async fn list(&self) -> Result<Vec<(String, String)>> {
        self.inner.list().await
    }

    async fn load_branch(&self, owner: &str, repo: &str, branch: &str) -> Result<Option<RefInfo>> {
        let key = Self::cache_key(owner, repo, branch);
        let mut cache = self.cache.write().await;
        if let Some((ts, info)) = cache.get(&key)
            && ts.elapsed() < self.ttl
        {
            return Ok(Some(info.clone()));
        }

        let info = self.inner.load_branch(owner, repo, branch).await?;
        if let Some(info) = &info {
            cache.insert(key, (Instant::now(), info.clone()));
        }
        Ok(info)
    }

    async fn save_branch(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        info: &RefInfo,
    ) -> Result<()> {
        let key = Self::cache_key(owner, repo, branch);
        let mut cache = self.cache.write().await;
        self.inner.save_branch(owner, repo, branch, info).await?;
        cache.insert(key, (Instant::now(), info.clone()));
        Ok(())
    }

    async fn list_branches(&self, owner: &str, repo: &str) -> Result<Vec<String>> {
        self.inner.list_branches(owner, repo).await
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
            // Only save if the repo doesn't already have a stored ref.
            match store.load(owner, repo).await {
                Ok(None) => {
                    store.save(owner, repo, &info).await?;
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
            head_buckets: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            synced_at: None,
        }
    }

    #[tokio::test]
    async fn file_ref_store_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());

        assert!(store.load("ripclone", "test").await.unwrap().is_none());

        let info = dummy_ref_info("abc123");
        store.save("ripclone", "test", &info).await.unwrap();

        let loaded = store.load("ripclone", "test").await.unwrap().unwrap();
        assert_eq!(loaded.commit, "abc123");

        let list = store.list().await.unwrap();
        assert_eq!(list, vec![("ripclone".to_string(), "test".to_string())]);
    }

    #[tokio::test]
    async fn caching_ref_store_uses_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let file_store = FileRefStore::new(tmp.path());
        let store = CachingRefStore {
            inner: file_store,
            ttl: Duration::from_secs(60),
            cache: RwLock::new(HashMap::new()),
        };

        let info = dummy_ref_info("cached");
        store.save("o", "r", &info).await.unwrap();

        let loaded = store.load("o", "r").await.unwrap().unwrap();
        assert_eq!(loaded.commit, "cached");
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

        let loaded = store.load("ripclone", "test").await.unwrap().unwrap();
        assert_eq!(loaded.commit, "head-commit");
    }

    #[tokio::test]
    async fn file_ref_store_branch_roundtrip_and_list() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());

        let info_main = dummy_ref_info("main-commit");
        store
            .save_branch("o", "r", "main", &info_main)
            .await
            .unwrap();

        let info_feature = dummy_ref_info("feature-commit");
        store
            .save_branch("o", "r", "feature/foo-bar", &info_feature)
            .await
            .unwrap();

        let loaded_main = store.load_branch("o", "r", "main").await.unwrap().unwrap();
        assert_eq!(loaded_main.commit, "main-commit");

        let loaded_feature = store
            .load_branch("o", "r", "feature/foo-bar")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded_feature.commit, "feature-commit");

        let mut branches = store.list_branches("o", "r").await.unwrap();
        branches.sort();
        assert_eq!(branches, vec!["feature/foo-bar", "main"]);

        let repos = store.list().await.unwrap();
        assert_eq!(repos, vec![("o".to_string(), "r".to_string())]);
    }

    #[tokio::test]
    async fn file_ref_store_head_and_branch_coexist() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileRefStore::new(tmp.path());

        let head_info = dummy_ref_info("head-commit");
        store.save("o", "r", &head_info).await.unwrap();

        let branch_info = dummy_ref_info("branch-commit");
        store
            .save_branch("o", "r", "main", &branch_info)
            .await
            .unwrap();

        let branches = store.list_branches("o", "r").await.unwrap();
        assert!(branches.contains(&"HEAD".to_string()));
        assert!(branches.contains(&"main".to_string()));
    }
}
