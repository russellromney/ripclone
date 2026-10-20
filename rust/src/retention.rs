use crate::cas::Cas;
use crate::metrics::Metrics;
use crate::storage::StorageRef;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

/// Size-bounded cleanup for an S3-backed local build cache: past the bound,
/// the least recently written or served objects go first.
///
/// The cache is disposable only when a remote backend owns the durable copy.
/// This task never reads exact-result metadata and never deletes remote bytes.
#[derive(Clone)]
pub struct LocalCacheRetention {
    cas: Cas,
    max_size_bytes: Option<u64>,
    metrics: Arc<Metrics>,
    durable_storage: Option<StorageRef>,
}

/// Retention settings from the environment, read once at startup.
#[derive(Clone, Copy, Debug)]
pub struct RetentionSettings {
    /// Time between passes, 5 minutes. Tests shorten it with
    /// `RIPCLONE_RETENTION_INTERVAL_SECS`.
    pub interval: Duration,
    /// `RIPCLONE_RETENTION_MAX_BYTES`, the most bytes the cache keeps; `0`
    /// keeps any size. `None` (unset) is half the size of the disk that
    /// holds the cache.
    pub max_size_bytes: Option<u64>,
}

impl RetentionSettings {
    pub fn from_env() -> Result<Self> {
        const MAX: &str = "RIPCLONE_RETENTION_MAX_BYTES";
        let interval = crate::env::number("RIPCLONE_RETENTION_INTERVAL_SECS", 300)?;
        let max_size_bytes = (std::env::var_os(MAX).is_some())
            .then(|| crate::env::number(MAX, 0))
            .transpose()?;
        Ok(Self {
            interval: Duration::from_secs(interval),
            max_size_bytes,
        })
    }
}

impl LocalCacheRetention {
    pub fn new(
        cas: Cas,
        metrics: Arc<Metrics>,
        storage: StorageRef,
        settings: &RetentionSettings,
    ) -> Result<Self> {
        let durable_storage = storage.is_remote().then_some(storage);
        let max_size_bytes = match (&durable_storage, settings.max_size_bytes) {
            (None, _) | (_, Some(0)) => None,
            (Some(_), Some(bytes)) => Some(bytes),
            (Some(_), None) => Some(crate::capacity::disk_bytes(cas.root())? / 2),
        };
        Ok(Self {
            cas,
            max_size_bytes,
            metrics,
            durable_storage,
        })
    }

    #[cfg(test)]
    pub fn with_config(
        cas: Cas,
        metrics: Arc<Metrics>,
        max_size_bytes: Option<u64>,
        durable_storage: Option<StorageRef>,
    ) -> Self {
        Self {
            cas,
            max_size_bytes,
            metrics,
            durable_storage,
        }
    }

    pub fn disabled(&self) -> bool {
        self.durable_storage.is_none() || self.max_size_bytes.is_none()
    }

    pub fn spawn(self, interval: Duration) {
        if interval.is_zero() || self.disabled() {
            info!(
                "local cache retention disabled (interval={:?}, max_size={:?})",
                interval, self.max_size_bytes
            );
            return;
        }
        info!(
            "local cache retention starting: interval={:?}, max_size={:?} bytes",
            interval, self.max_size_bytes
        );
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(error) = self.run_once().await {
                    warn!("local cache retention failed: {error}");
                    self.metrics.record_local_cache_cleanup_error();
                }
            }
        });
    }

    pub async fn run_once(&self) -> Result<()> {
        let Some(durable_storage) = self.durable_storage.clone() else {
            return Ok(());
        };
        let Some(max_size) = self.max_size_bytes else {
            return Ok(());
        };
        let cas = self.cas.clone();
        let (deleted_bytes, deleted_count) =
            tokio::task::spawn_blocking(move || -> Result<(u64, u64)> {
                let mut entries = list_cas_entries(cas.root())?;
                let total: u64 = entries.iter().map(|entry| entry.size).sum();
                if total <= max_size {
                    return Ok((0, 0));
                }
                entries.sort_by_key(|entry| entry.mtime);
                let target = total - (max_size * 9 / 10);
                let (mut freed, mut count) = (0u64, 0u64);
                for entry in entries {
                    if freed >= target {
                        break;
                    }
                    if durable_storage.verify_durable_copy(&entry.hash).is_err() {
                        warn!(
                            "keeping local cache object {}: remote copy is not confirmed",
                            entry.hash
                        );
                        continue;
                    }
                    if let Err(error) = std::fs::remove_file(&entry.path) {
                        warn!(
                            "failed to remove local cache object {}: {error}",
                            entry.hash
                        );
                        continue;
                    }
                    freed += entry.size;
                    count += 1;
                }
                Ok((freed, count))
            })
            .await
            .context("local cache retention task panicked")??;

        if deleted_count > 0 {
            info!(
                "local cache retention removed {} objects ({} bytes)",
                deleted_count, deleted_bytes
            );
        }
        self.metrics
            .record_local_cache_cleanup(deleted_bytes, deleted_count);
        Ok(())
    }
}

struct CasEntry {
    hash: String,
    path: PathBuf,
    size: u64,
    mtime: SystemTime,
}

fn list_cas_entries(root: &Path) -> Result<Vec<CasEntry>> {
    let mut entries = Vec::new();
    for prefix_dir in std::fs::read_dir(root)? {
        let prefix_dir = prefix_dir?;
        if !prefix_dir.file_type()?.is_dir() {
            continue;
        }
        let name = prefix_dir.file_name();
        if name.to_string_lossy().starts_with('.') {
            continue;
        }
        for object in std::fs::read_dir(prefix_dir.path())? {
            let object = object?;
            if !object.file_type()?.is_file() {
                continue;
            }
            let hash = object.file_name().to_string_lossy().to_string();
            if hash.len() != 64 && hash.len() != 40 {
                continue;
            }
            let metadata = object.metadata()?;
            entries.push(CasEntry {
                hash,
                path: object.path(),
                size: metadata.len(),
                mtime: metadata.modified()?,
            });
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn local_storage_is_never_trimmed() {
        let directory = tempfile::tempdir().unwrap();
        let cas = Cas::new(directory.path()).unwrap();
        let hash = cas.put(b"durable local artifact").unwrap();
        let retention =
            LocalCacheRetention::with_config(cas.clone(), Metrics::new(), Some(1), None);

        retention.run_once().await.unwrap();

        assert!(cas.path(&hash).exists());
    }

    #[tokio::test]
    async fn trims_only_after_remote_copy_is_confirmed() {
        let local = tempfile::tempdir().unwrap();
        let durable = tempfile::tempdir().unwrap();
        let cas = Cas::new(local.path()).unwrap();
        let remote = crate::storage::local(durable.path()).unwrap();
        let bytes = b"remote-backed local cache";
        let hash = cas.put(bytes).unwrap();
        let retention = LocalCacheRetention::with_config(
            cas.clone(),
            Metrics::new(),
            Some(1),
            Some(remote.clone()),
        );

        retention.run_once().await.unwrap();
        assert!(cas.path(&hash).exists());

        remote.put(&hash, bytes).unwrap();
        retention.run_once().await.unwrap();
        assert!(!cas.path(&hash).exists());
        assert_eq!(remote.get(&hash).unwrap(), bytes);
    }

    #[tokio::test]
    async fn size_limit_removes_only_confirmed_remote_objects() {
        let local = tempfile::tempdir().unwrap();
        let durable = tempfile::tempdir().unwrap();
        let cas = Cas::new(local.path()).unwrap();
        let remote = crate::storage::local(durable.path()).unwrap();
        let local_only_hash = cas.put(b"local-only").unwrap();
        let remote_hash = cas.put(b"remote-copy").unwrap();
        remote.put(&remote_hash, b"remote-copy").unwrap();

        filetime::set_file_mtime(
            cas.path(&local_only_hash),
            filetime::FileTime::from_unix_time(1, 0),
        )
        .unwrap();
        filetime::set_file_mtime(
            cas.path(&remote_hash),
            filetime::FileTime::from_unix_time(2, 0),
        )
        .unwrap();

        let retention = LocalCacheRetention::with_config(
            cas.clone(),
            Metrics::new(),
            Some(1),
            Some(remote.clone()),
        );
        retention.run_once().await.unwrap();

        assert!(
            cas.path(&local_only_hash).exists(),
            "size cleanup must retain an object without a durable copy"
        );
        assert!(
            !cas.path(&remote_hash).exists(),
            "size cleanup may remove an object with a confirmed durable copy"
        );
        assert_eq!(remote.get(&remote_hash).unwrap(), b"remote-copy");
    }

    #[tokio::test]
    async fn size_limit_removes_the_oldest_first() {
        let local = tempfile::tempdir().unwrap();
        let durable = tempfile::tempdir().unwrap();
        let cas = Cas::new(local.path()).unwrap();
        let remote = crate::storage::local(durable.path()).unwrap();
        let hashes: Vec<String> = (0..3u8)
            .map(|i| {
                let bytes = [i; 100];
                let hash = cas.put(&bytes).unwrap();
                remote.put(&hash, &bytes).unwrap();
                let when = filetime::FileTime::from_unix_time(1 + i as i64, 0);
                filetime::set_file_mtime(cas.path(&hash), when).unwrap();
                hash
            })
            .collect();
        let total: u64 = (hashes.iter())
            .map(|hash| std::fs::metadata(cas.path(hash)).unwrap().len())
            .sum();

        // Over the limit by less than one object: trimming to 90% of it
        // removes only the oldest.
        let retention = LocalCacheRetention::with_config(
            cas.clone(),
            Metrics::new(),
            Some(total - 10),
            Some(remote),
        );
        retention.run_once().await.unwrap();

        let kept: Vec<bool> = (hashes.iter())
            .map(|hash| cas.path(hash).exists())
            .collect();
        assert_eq!(kept, [false, true, true]);
    }
}
