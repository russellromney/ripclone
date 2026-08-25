use crate::cas::Cas;
use crate::metrics::Metrics;
use crate::storage::StorageRef;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

/// Age- and size-bounded cleanup for an S3-backed local build cache.
///
/// The cache is disposable only when a remote backend owns the durable copy.
/// This task never reads exact-result metadata and never deletes remote bytes.
#[derive(Clone)]
pub struct LocalCacheRetention {
    cas: Cas,
    max_age: Option<Duration>,
    max_size_bytes: Option<u64>,
    metrics: Arc<Metrics>,
    durable_storage: Option<StorageRef>,
}

impl LocalCacheRetention {
    pub fn from_env(cas: Cas, metrics: Arc<Metrics>, storage: StorageRef) -> Self {
        let durable_storage = storage.is_remote().then_some(storage);
        Self {
            cas,
            max_age: durable_storage.as_ref().and_then(|_| Self::parse_age()),
            max_size_bytes: durable_storage.as_ref().and_then(|_| Self::parse_size()),
            metrics,
            durable_storage,
        }
    }

    #[cfg(test)]
    pub fn with_config(
        cas: Cas,
        metrics: Arc<Metrics>,
        max_age: Option<Duration>,
        max_size_bytes: Option<u64>,
        durable_storage: Option<StorageRef>,
    ) -> Self {
        Self {
            cas,
            max_age,
            max_size_bytes,
            metrics,
            durable_storage,
        }
    }

    pub fn disabled(&self) -> bool {
        self.durable_storage.is_none() || (self.max_age.is_none() && self.max_size_bytes.is_none())
    }

    pub fn spawn(self, interval: Duration) {
        if interval.is_zero() || self.disabled() {
            info!(
                "local cache retention disabled (interval={:?}, age={:?}, max_size={:?})",
                interval, self.max_age, self.max_size_bytes
            );
            return;
        }
        info!(
            "local cache retention starting: interval={:?}, max_age={:?}, max_size={:?} bytes",
            interval, self.max_age, self.max_size_bytes
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
        let cas = self.cas.clone();
        let max_age = self.max_age;
        let max_size = self.max_size_bytes;
        let (deleted_age_bytes, deleted_size_bytes, deleted_count) =
            tokio::task::spawn_blocking(move || -> Result<(u64, u64, u64)> {
                let entries = list_cas_entries(cas.root())?;
                let is_durable = |hash: &str| durable_storage.size(hash).is_ok();

                let mut deleted_age_bytes = 0u64;
                let mut deleted_age_count = 0u64;
                let mut remaining = Vec::new();
                for entry in entries {
                    if max_age.is_some_and(|age| entry.age >= age) {
                        if !is_durable(&entry.hash) {
                            warn!(
                                "keeping local cache object {}: remote copy is not confirmed",
                                entry.hash
                            );
                            remaining.push(entry);
                            continue;
                        }
                        if let Err(error) = std::fs::remove_file(&entry.path) {
                            warn!(
                                "failed to remove local cache object {}: {error}",
                                entry.hash
                            );
                            remaining.push(entry);
                            continue;
                        }
                        deleted_age_bytes += entry.size;
                        deleted_age_count += 1;
                    } else {
                        remaining.push(entry);
                    }
                }

                let mut deleted_size_bytes = 0u64;
                let mut deleted_size_count = 0u64;
                if let Some(max_size) = max_size {
                    let total: u64 = remaining.iter().map(|entry| entry.size).sum();
                    if total > max_size {
                        remaining.sort_by_key(|entry| entry.mtime);
                        let mut freed = 0u64;
                        let target = total - (max_size * 9 / 10);
                        for entry in remaining {
                            if freed >= target {
                                break;
                            }
                            if !is_durable(&entry.hash) {
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
                            deleted_size_bytes += entry.size;
                            deleted_size_count += 1;
                        }
                    }
                }

                Ok((
                    deleted_age_bytes,
                    deleted_size_bytes,
                    deleted_age_count + deleted_size_count,
                ))
            })
            .await
            .context("local cache retention task panicked")??;

        let total_deleted = deleted_age_bytes + deleted_size_bytes;
        if deleted_count > 0 {
            info!(
                "local cache retention removed {} objects ({} bytes; {} age, {} size)",
                deleted_count, total_deleted, deleted_age_bytes, deleted_size_bytes
            );
        }
        self.metrics
            .record_local_cache_cleanup(total_deleted, deleted_count);
        Ok(())
    }

    fn parse_age() -> Option<Duration> {
        match std::env::var("RIPCLONE_RETENTION_MAX_AGE_DAYS") {
            Ok(value) if !value.is_empty() => value
                .parse::<u64>()
                .ok()
                .map(|days| Duration::from_secs(days * 24 * 60 * 60))
                .filter(|duration| !duration.is_zero()),
            _ => Some(Duration::from_secs(7 * 24 * 60 * 60)),
        }
    }

    fn parse_size() -> Option<u64> {
        match std::env::var("RIPCLONE_RETENTION_MAX_GB") {
            Ok(value) if !value.is_empty() => value
                .parse::<u64>()
                .ok()
                .map(|gb| gb * 1024 * 1024 * 1024)
                .filter(|bytes| *bytes > 0),
            _ => Some(100 * 1024 * 1024 * 1024),
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
            let modified = metadata.modified()?;
            entries.push(CasEntry {
                hash,
                path: object.path(),
                size: metadata.len(),
                mtime: modified,
                age: now.duration_since(modified).unwrap_or(Duration::ZERO),
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
        let retention = LocalCacheRetention::with_config(
            cas.clone(),
            Metrics::new(),
            Some(Duration::ZERO),
            Some(1),
            None,
        );

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
            Some(Duration::ZERO),
            None,
            Some(remote.clone()),
        );

        retention.run_once().await.unwrap();
        assert!(cas.path(&hash).exists());

        remote.put(&hash, bytes).unwrap();
        retention.run_once().await.unwrap();
        assert!(!cas.path(&hash).exists());
        assert_eq!(remote.get(&hash).unwrap(), bytes);
    }
}
