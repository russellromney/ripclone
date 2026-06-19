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

    fn path(&self, owner: &str, repo: &str) -> PathBuf {
        self.root.join(owner).join(format!("{}.json", repo))
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
                if !repo_entry.file_type().await?.is_file() {
                    continue;
                }
                let name = repo_entry.file_name().to_string_lossy().to_string();
                if let Some(repo) = name.strip_suffix(".json") {
                    out.push((owner.clone(), repo.to_string()));
                }
            }
        }
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
        let mut out = Vec::new();
        for key in keys {
            let Some(rest) = key.strip_prefix(prefix) else {
                continue;
            };
            let Some((owner, repo_file)) = rest.split_once('/') else {
                continue;
            };
            let Some(repo) = repo_file.strip_suffix(".json") else {
                continue;
            };
            out.push((owner.to_string(), repo.to_string()));
        }
        Ok(out)
    }
}

/// In-memory caching wrapper around a `RefStore`.
pub struct CachingRefStore<T: RefStore> {
    inner: T,
    ttl: Duration,
    cache: RwLock<HashMap<(String, String), (Instant, RefInfo)>>,
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
}

#[async_trait]
impl<T: RefStore> RefStore for CachingRefStore<T> {
    async fn load(&self, owner: &str, repo: &str) -> Result<Option<RefInfo>> {
        let key = (owner.to_string(), repo.to_string());
        let mut cache = self.cache.write().await;
        if let Some((ts, info)) = cache.get(&key) {
            if ts.elapsed() < self.ttl {
                return Ok(Some(info.clone()));
            }
        }

        let info = self.inner.load(owner, repo).await?;
        if let Some(info) = &info {
            cache.insert(key, (Instant::now(), info.clone()));
        }
        Ok(info)
    }

    async fn save(&self, owner: &str, repo: &str, info: &RefInfo) -> Result<()> {
        // Hold the cache lock across the persist so a concurrent load cannot
        // observe a stale value after the write completes.
        let mut cache = self.cache.write().await;
        self.inner.save(owner, repo, info).await?;
        let key = (owner.to_string(), repo.to_string());
        cache.insert(key, (Instant::now(), info.clone()));
        Ok(())
    }

    async fn list(&self) -> Result<Vec<(String, String)>> {
        self.inner.list().await
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
        if let Some((owner_repo, _branch)) = key.rsplit_once('/') {
            if let Some((owner, repo)) = owner_repo.split_once('/') {
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
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            full_pack: String::new(),
            clonepack_manifest: String::new(),
            metadata_chunk: String::new(),
            archive_chunks: Vec::new(),
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
}
