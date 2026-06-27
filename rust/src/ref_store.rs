use crate::RefInfo;
use crate::provider::RepoId;
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
/// - the new ref's `synced_at` is newer-than-or-equal to the stored one.
///
/// A missing `synced_at` on either side is treated as "no ordering
/// information" and the write is accepted; backends that have a stronger
/// atomic tie-break (the SQL conditional upsert, the S3 ETag CAS) rely on that
/// for the final ordering, while the file store serializes writes in-process.
pub(crate) fn should_replace_ref(existing: Option<&RefInfo>, new: &RefInfo) -> bool {
    let Some(existing) = existing else {
        return true;
    };
    if existing.commit == new.commit {
        return true;
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
    /// Phase 0: every returned repo is a GitHub `RepoId`. Later phases will need
    /// a provider registry to disambiguate `{provider}/{escaped_path}` keys.
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

    /// List all branches that have a stored `RefInfo` for this repo.
    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>>;

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
        // Best-effort creation of the root directory up front so `list()` works
        // on a fresh deployment.
        let _ = std::fs::create_dir_all(&root);
        Self {
            root,
            write_lock: tokio::sync::Mutex::new(()),
        }
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
                if ft.is_file() {
                    // Legacy HEAD ref: {owner}/{repo}.json
                    if let Some(repo) = name.strip_suffix(".json") {
                        out.push(RepoId::github(format!("{owner}/{repo}")));
                    }
                } else if ft.is_dir() {
                    // Branch-specific refs: {owner}/{repo}/{branch_slug}.json
                    out.push(RepoId::github(format!("{owner}/{name}")));
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
/// `{prefix}refs/{owner}/{repo}.json`.
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

    /// Read-compare-CAS write shared by HEAD (`save`) and branch
    /// (`save_branch`) saves. Applies [`should_replace_ref`] and uses the
    /// object's ETag as the atomic tie-break against a concurrent writer:
    /// if another sync lands between our read and write, the conditional PUT
    /// fails and we re-read and re-decide. This is what makes branch refs
    /// "newer never loses" instead of last-writer-wins.
    async fn save_keyed(&self, key: &str, info: &RefInfo) -> Result<()> {
        let data = serde_json::to_vec_pretty(info).context("serialize RefInfo")?;
        // Bounded so a pathological hot key can't spin forever; in practice one
        // or two reads settle it.
        for _ in 0..8 {
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
        }
        anyhow::bail!("S3 ref store {key}: gave up after repeated concurrent-write conflicts")
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
                out.push(RepoId::github(format!("{owner}/{repo}")));
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

    fn cache_key(repo_id: &RepoId, branch: &str) -> (String, String) {
        (repo_id.storage_key(), branch.to_string())
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
        let mut cache = self.cache.write().await;
        if let Some((ts, info)) = cache.get(&key)
            && ts.elapsed() < self.ttl
        {
            return Ok(Some(info.clone()));
        }

        let info = self.inner.load_branch(repo_id, branch).await?;
        if let Some(info) = &info {
            cache.insert(key, (Instant::now(), info.clone()));
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
        let mut cache = self.cache.write().await;
        self.inner.save_branch(repo_id, branch, info).await?;
        cache.insert(key, (Instant::now(), info.clone()));
        Ok(())
    }

    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        self.inner.list_branches(repo_id).await
    }

    async fn invalidate(&self, repo_id: &RepoId, branch: &str) {
        let mut cache = self.cache.write().await;
        cache.remove(&Self::cache_key(repo_id, branch));
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
            head_buckets: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            synced_at: None,
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
    async fn caching_ref_store_uses_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let file_store = FileRefStore::new(tmp.path());
        let store = CachingRefStore {
            inner: file_store,
            ttl: Duration::from_secs(60),
            cache: RwLock::new(HashMap::new()),
        };

        let repo_id = RepoId::github("o/r");
        let info = dummy_ref_info("cached");
        store.save(&repo_id, &info).await.unwrap();

        let loaded = store.load(&repo_id).await.unwrap().unwrap();
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
}
