use crate::cas::Cas;
use crate::metrics::Metrics;
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Local-disk retention manager for the content-addressed artifact store.
///
/// The manager keeps a set of "protected" hashes (artifacts referenced by the
/// current HEAD of each synced repo). On each run it scans the CAS directory
/// and deletes unprotected objects that are older than the configured max age,
/// then if the total footprint is still above the size cap it evicts the oldest
/// unprotected objects first.
#[derive(Clone)]
pub struct Retention {
    cas: Cas,
    protected: Arc<RwLock<HashSet<String>>>,
    protected_file: PathBuf,
    max_age: Option<Duration>,
    max_size_bytes: Option<u64>,
    metrics: Arc<Metrics>,
    /// Optional durable storage backend. When set, objects are only evicted
    /// from the local cache after confirming they exist in durable storage.
    durable_storage: Option<crate::storage::StorageRef>,
}

impl Retention {
    pub fn new(cas: Cas, metrics: Arc<Metrics>) -> Result<Self> {
        Self::with_config_and_storage(cas, metrics, Self::parse_age(), Self::parse_size(), None)
    }

    pub fn with_config(
        cas: Cas,
        metrics: Arc<Metrics>,
        max_age: Option<Duration>,
        max_size_bytes: Option<u64>,
    ) -> Result<Self> {
        Self::with_config_and_storage(cas, metrics, max_age, max_size_bytes, None)
    }

    pub fn with_config_and_storage(
        cas: Cas,
        metrics: Arc<Metrics>,
        max_age: Option<Duration>,
        max_size_bytes: Option<u64>,
        durable_storage: Option<crate::storage::StorageRef>,
    ) -> Result<Self> {
        let protected_file = cas.root().join(".ripclone-retention").join("protected.txt");
        if let Some(parent) = protected_file.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create retention dir {}", parent.display()))?;
        }
        let protected = Self::load_protected(&protected_file)?;
        Ok(Self {
            cas,
            protected: Arc::new(RwLock::new(protected)),
            protected_file,
            max_age,
            max_size_bytes,
            metrics,
            durable_storage,
        })
    }

    pub fn disabled(&self) -> bool {
        self.max_age.is_none() && self.max_size_bytes.is_none()
    }

    /// Mark a set of hashes as protected. Duplicates are ignored.
    pub async fn protect<I, S>(&self, hashes: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut set = self.protected.write().await;
        for h in hashes {
            set.insert(h.as_ref().to_string());
        }
        if let Err(e) = Self::save_protected(&self.protected_file, &set) {
            warn!("failed to persist protected hashes: {}", e);
        }
    }

    /// Spawn a background task that runs retention on the configured interval.
    pub fn spawn(self, interval: Duration) {
        if interval.is_zero() || self.disabled() {
            info!(
                "retention disabled (interval={:?}, age={:?}, max_size={:?})",
                interval, self.max_age, self.max_size_bytes
            );
            return;
        }
        info!(
            "retention task starting: interval={:?}, max_age={:?}, max_size={:?} bytes",
            interval, self.max_age, self.max_size_bytes
        );
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(e) = self.run_once().await {
                    warn!("retention run failed: {}", e);
                    self.metrics.record_retention_error();
                }
            }
        });
    }

    /// Run a single retention pass. Called by the background task and tests.
    pub async fn run_once(&self) -> Result<()> {
        let protected = self.protected.read().await.clone();
        let cas = self.cas.clone();
        let max_age = self.max_age;
        let max_size = self.max_size_bytes;
        let metrics = self.metrics.clone();
        let durable_storage = self.durable_storage.clone();

        let (deleted_age_bytes, deleted_size_bytes, deleted_count) =
            tokio::task::spawn_blocking(move || -> Result<(u64, u64, u64)> {
                let entries = list_cas_entries(cas.root())?;
                let is_durable = |hash: &str| -> bool {
                    match &durable_storage {
                        Some(storage) => storage.size(hash).is_ok(),
                        None => true,
                    }
                };

                // Phase 1: age-based eviction.
                let mut deleted_age_bytes: u64 = 0;
                let mut deleted_age_count: u64 = 0;
                let mut remaining: Vec<CasEntry> = Vec::new();
                for entry in entries {
                    if protected.contains(&entry.hash) {
                        remaining.push(entry);
                        continue;
                    }
                    if let Some(max_age) = max_age {
                        if entry.age >= max_age {
                            if !is_durable(&entry.hash) {
                                warn!(
                                    "skipping eviction of {}: not confirmed in durable storage",
                                    entry.hash
                                );
                                remaining.push(entry);
                                continue;
                            }
                            if let Err(e) = std::fs::remove_file(&entry.path) {
                                warn!("failed to remove old CAS object {}: {}", entry.hash, e);
                                remaining.push(entry);
                                continue;
                            }
                            deleted_age_bytes += entry.size;
                            deleted_age_count += 1;
                            continue;
                        }
                    }
                    remaining.push(entry);
                }

                // Phase 2: size-based eviction of oldest unprotected objects.
                let mut deleted_size_bytes: u64 = 0;
                let mut deleted_size_count: u64 = 0;
                if let Some(max_size) = max_size {
                    let total: u64 = remaining.iter().map(|e| e.size).sum();
                    if total > max_size {
                        let mut candidates: Vec<CasEntry> = remaining
                            .into_iter()
                            .filter(|e| !protected.contains(&e.hash))
                            .collect();
                        candidates.sort_by_key(|e| e.mtime);

                        let mut freed: u64 = 0;
                        let target = total - (max_size * 9 / 10); // free down to 90% of cap
                        for entry in candidates {
                            if freed >= target {
                                break;
                            }
                            if !is_durable(&entry.hash) {
                                warn!(
                                    "skipping eviction of {}: not confirmed in durable storage",
                                    entry.hash
                                );
                                continue;
                            }
                            if let Err(e) = std::fs::remove_file(&entry.path) {
                                warn!("failed to remove CAS object {}: {}", entry.hash, e);
                                continue;
                            }
                            freed += entry.size;
                            deleted_size_bytes += entry.size;
                            deleted_size_count += 1;
                        }
                    }
                }

                let total_deleted_count = deleted_age_count + deleted_size_count;
                Ok((deleted_age_bytes, deleted_size_bytes, total_deleted_count))
            })
            .await
            .context("retention task panicked")??;

        let total_deleted = deleted_age_bytes + deleted_size_bytes;
        if total_deleted > 0 || deleted_count > 0 {
            info!(
                "retention evicted {} objects ({} bytes; {} age, {} size)",
                deleted_count, total_deleted, deleted_age_bytes, deleted_size_bytes
            );
        }
        metrics.record_retention_run(total_deleted, deleted_count);
        Ok(())
    }

    fn load_protected(path: &Path) -> Result<HashSet<String>> {
        let mut set = HashSet::new();
        if !path.exists() {
            return Ok(set);
        }
        let data = std::fs::read_to_string(path)
            .with_context(|| format!("read protected hashes {}", path.display()))?;
        for line in data.lines() {
            let line = line.trim();
            if !line.is_empty() {
                set.insert(line.to_string());
            }
        }
        Ok(set)
    }

    fn save_protected(path: &Path, set: &HashSet<String>) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut data: Vec<&str> = set.iter().map(|s| s.as_str()).collect();
        data.sort_unstable();
        std::fs::write(path, data.join("\n"))
            .with_context(|| format!("write protected hashes {}", path.display()))?;
        Ok(())
    }

    pub fn parse_age() -> Option<Duration> {
        match std::env::var("RIPCLONE_RETENTION_MAX_AGE_DAYS") {
            Ok(s) if !s.is_empty() => s
                .parse::<u64>()
                .ok()
                .map(|d| Duration::from_secs(d * 24 * 60 * 60))
                .filter(|d| !d.is_zero()),
            _ => Some(Duration::from_secs(7 * 24 * 60 * 60)),
        }
    }

    pub fn parse_size() -> Option<u64> {
        match std::env::var("RIPCLONE_RETENTION_MAX_GB") {
            Ok(s) if !s.is_empty() => s
                .parse::<u64>()
                .ok()
                .map(|g| g * 1024 * 1024 * 1024)
                .filter(|b| *b > 0),
            _ => Some(100 * 1024 * 1024 * 1024), // 100 GiB default
        }
    }
}

struct CasEntry {
    hash: String,
    path: PathBuf,
    size: u64,
    mtime: SystemTime,
    age: Duration,
}

fn list_cas_entries(root: &Path) -> Result<Vec<CasEntry>> {
    let mut entries = Vec::new();
    let now = SystemTime::now();
    for prefix_dir in std::fs::read_dir(root)? {
        let prefix_dir = prefix_dir?;
        if !prefix_dir.file_type()?.is_dir() {
            continue;
        }
        let name = prefix_dir.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with('.') {
            continue;
        }
        for obj in std::fs::read_dir(prefix_dir.path())? {
            let obj = obj?;
            if !obj.file_type()?.is_file() {
                continue;
            }
            let hash = obj.file_name().to_string_lossy().to_string();
            if hash.len() != 64 && hash.len() != 40 {
                continue; // not a SHA-256 (64) or SHA-1 (40) object id
            }
            let meta = obj.metadata()?;
            let size = meta.len();
            let mtime = meta.modified()?;
            let age = now.duration_since(mtime).unwrap_or(Duration::ZERO);
            entries.push(CasEntry {
                hash,
                path: obj.path(),
                size,
                mtime,
                age,
            });
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use std::io::Write;

    fn make_cas(root: &Path) -> Cas {
        Cas::new(root).unwrap()
    }

    fn hash_of(data: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(data))
    }

    #[tokio::test]
    async fn retention_evicts_unprotected_old_objects() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = make_cas(tmp.path());

        let data1 = b"old-unprotected";
        let h1 = hash_of(data1);
        cas.put(data1.as_slice()).unwrap();
        let path1 = cas.path(&h1);
        let old = SystemTime::now() - Duration::from_secs(10 * 24 * 60 * 60);
        filetime::set_file_mtime(&path1, filetime::FileTime::from_system_time(old)).unwrap();

        let data2 = b"new-protected";
        let h2 = hash_of(data2);
        cas.put(data2.as_slice()).unwrap();

        let metrics = Metrics::new();
        let retention = Retention::with_config(
            cas.clone(),
            metrics.clone(),
            Some(Duration::from_secs(7 * 24 * 60 * 60)),
            None,
        )
        .unwrap();
        retention.protect(vec![h2.clone()]).await;

        retention.run_once().await.unwrap();

        assert!(!path1.exists(), "old unprotected object should be evicted");
        assert!(cas.path(&h2).exists(), "protected object should remain");
        assert_eq!(metrics.snapshot().retention_evicted_objects, 1);
        assert_eq!(
            metrics.snapshot().retention_evicted_bytes,
            data1.len() as u64
        );
    }

    #[tokio::test]
    async fn retention_evicts_oldest_when_over_size_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = make_cas(tmp.path());

        let data1 = b"aaaaaaaaaa";
        let data2 = b"bbbbbbbbbb";
        let h1 = hash_of(data1);
        let h2 = hash_of(data2);
        cas.put(data1.as_slice()).unwrap();
        cas.put(data2.as_slice()).unwrap();

        let path1 = cas.path(&h1);
        let older = SystemTime::now() - Duration::from_secs(3600);
        filetime::set_file_mtime(&path1, filetime::FileTime::from_system_time(older)).unwrap();

        let metrics = Metrics::new();
        let retention = Retention::with_config(
            cas.clone(),
            metrics.clone(),
            None,
            Some(15), // 15-byte cap; each object is 10 bytes
        )
        .unwrap();

        retention.run_once().await.unwrap();

        assert!(
            !path1.exists(),
            "oldest object should be evicted to meet cap"
        );
        assert!(cas.path(&h2).exists(), "newer object should remain");
    }

    #[tokio::test]
    async fn retention_counts_sha1_blobs_and_confirms_durability() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = make_cas(tmp.path());
        let durable_tmp = tempfile::tempdir().unwrap();
        let durable = crate::storage::local(durable_tmp.path()).unwrap();

        // Store a 40-character SHA-1 object (the format used for git blobs).
        let data = b"sha1-blob-object";
        use sha1::{Digest, Sha1};
        let sha1_hash = format!("{:x}", Sha1::digest(data));
        assert_eq!(sha1_hash.len(), 40);
        cas.put_with_hash(&sha1_hash, data).unwrap();
        let path = cas.path(&sha1_hash);

        let old = SystemTime::now() - Duration::from_secs(10 * 24 * 60 * 60);
        filetime::set_file_mtime(&path, filetime::FileTime::from_system_time(old)).unwrap();

        let metrics = Metrics::new();
        let retention = Retention::with_config_and_storage(
            cas.clone(),
            metrics.clone(),
            Some(Duration::from_secs(7 * 24 * 60 * 60)),
            None,
            Some(durable.clone()),
        )
        .unwrap();

        // Without the object in durable storage, retention must keep it.
        retention.run_once().await.unwrap();
        assert!(
            path.exists(),
            "object not in durable storage should not be evicted"
        );

        // After copying it to durable storage, retention may evict it.
        durable.put(&sha1_hash, data).unwrap();
        retention.run_once().await.unwrap();
        assert!(
            !path.exists(),
            "object confirmed durable should be evictable"
        );
    }
}
