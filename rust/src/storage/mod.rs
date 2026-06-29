use crate::cas::Cas;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub mod s3_storage;
pub use s3_storage::S3Storage;

/// Abstract storage backend for content-addressed artifacts.
///
/// The local filesystem-backed implementation (`LocalStorage`) is the default.
/// Object-storage backends (S3/R2/Tigris) can implement the same trait and
/// return signed URLs so clients read directly from the CDN.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Fetch the full object by hash.
    fn get(&self, hash: &str) -> Result<Vec<u8>>;

    /// Fetch a byte range from the object by hash.
    fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>>;

    /// Store the full object by hash.
    fn put(&self, hash: &str, data: &[u8]) -> Result<()>;

    /// Async store, used by the bulk upload path. Running the request on the
    /// caller's runtime (instead of the sync `put`, which hops to a separate
    /// runtime via `block_on`) keeps the client's HTTP connection pool warm, so
    /// concurrent uploads reuse connections instead of re-handshaking per chunk.
    /// Default falls back to the sync `put` (fine for the local backend).
    async fn put_async(&self, hash: &str, data: &[u8]) -> Result<()> {
        self.put(hash, data)
    }

    /// Read a named, non-content-addressed metadata blob — small durable
    /// bookkeeping such as the GC orphan ledger. Returns `None` when the key
    /// does not exist. Defaults to unsupported; durable backends override it.
    async fn get_meta(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        anyhow::bail!("named metadata objects are not supported by this backend")
    }

    /// Write a named metadata blob. See [`get_meta`](Self::get_meta).
    async fn put_meta(&self, _key: &str, _data: &[u8]) -> Result<()> {
        anyhow::bail!("named metadata objects are not supported by this backend")
    }

    /// Return the object size in bytes, if the backend can determine it
    /// without downloading the whole object.
    fn size(&self, hash: &str) -> Result<u64>;

    /// Return a signed URL valid for `expires_in`, if the backend supports
    /// direct client reads. `None` means the server must proxy bytes itself.
    fn signed_url(&self, _hash: &str, _expires_in: Duration) -> Option<String> {
        None
    }

    /// True when this backend is a durable remote object store (S3/R2/Tigris)
    /// that is the source of truth. When true, the local CAS is only a build
    /// cache and its copies can be dropped after upload. When false (local
    /// backend), the CAS *is* the source of truth and must be kept.
    fn is_remote(&self) -> bool {
        false
    }

    /// Regions where this backend stores durable bytes. Used for storage-status
    /// billing breakdown. Defaults to "local" for filesystem-backed storage.
    fn regions(&self) -> Vec<String> {
        vec!["local".to_string()]
    }

    /// Delete a single object by content hash.
    fn delete(&self, hash: &str) -> Result<()>;

    /// Delete many objects by content hash. The default implementation deletes
    /// one at a time; remote backends should override to use batch APIs.
    fn delete_batch(&self, hashes: &[String]) -> Result<u64> {
        let mut count = 0u64;
        for hash in hashes {
            self.delete(hash)?;
            count += 1;
        }
        Ok(count)
    }

    /// List every content-addressed object stored in this backend, with its
    /// last-modified time. Only objects whose keys are valid artifact IDs
    /// (64-character lowercase hex SHA-256) are returned.
    fn list_hashes(&self) -> Result<Vec<HashEntry>>;

    /// Cheap readiness probe used by `/readyz`. Should confirm the backend is
    /// reachable without doing real work. Default assumes healthy; the local
    /// backend does a write probe and the S3 backend does a bucket-reachability
    /// probe. Any new durable/remote backend should override this.
    fn health(&self) -> Result<()> {
        Ok(())
    }
}

/// One content-addressed object seen by the storage backend.
#[derive(Debug, Clone)]
pub struct HashEntry {
    pub hash: String,
    pub size: u64,
    pub modified: SystemTime,
}

/// Filesystem-backed storage using the existing CAS layout.
pub struct LocalStorage {
    cas: Cas,
}

impl LocalStorage {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        Ok(Self {
            cas: Cas::new(root)?,
        })
    }

    pub fn cas(&self) -> &Cas {
        &self.cas
    }

    fn validate_hash_name(name: &str) -> Result<String> {
        crate::cas::Cas::validate_artifact_id(name)
            .with_context(|| format!("invalid CAS object name: {}", name))?;
        Ok(name.to_string())
    }

    /// Resolve a named metadata key to a path under the CAS root, rejecting
    /// anything that could escape it. Keys are internal constants, so this is a
    /// guard, not a parser.
    fn meta_path(&self, key: &str) -> Result<PathBuf> {
        if key.is_empty()
            || key.starts_with('/')
            || key.split('/').any(|seg| seg.is_empty() || seg == "..")
        {
            anyhow::bail!("invalid metadata key: {key}");
        }
        Ok(self.cas.root().join(key))
    }
}

#[async_trait]
impl StorageBackend for LocalStorage {
    fn get(&self, hash: &str) -> Result<Vec<u8>> {
        self.cas.get(hash)
    }

    fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        self.cas.get_range(hash, start, len)
    }

    fn put(&self, hash: &str, data: &[u8]) -> Result<()> {
        self.cas.put_with_hash(hash, data)
    }

    async fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.meta_path(key)?;
        match std::fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read metadata {key}")),
        }
    }

    async fn put_meta(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.meta_path(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create metadata dir for {key}"))?;
        }
        // Unique per call (pid + a monotonic counter) so concurrent writers never
        // collide on the same temp path before the atomic rename.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = path.with_extension(format!("tmp.{}.{}", std::process::id(), n));
        std::fs::write(&tmp, data).with_context(|| format!("write metadata tmp {key}"))?;
        std::fs::rename(&tmp, &path).with_context(|| format!("rename metadata {key}"))?;
        Ok(())
    }

    fn size(&self, hash: &str) -> Result<u64> {
        let path = self.cas.path(hash);
        let meta = std::fs::metadata(&path).with_context(|| format!("stat CAS object {}", hash))?;
        Ok(meta.len())
    }

    fn delete(&self, hash: &str) -> Result<()> {
        self.cas.remove(hash)
    }

    fn delete_batch(&self, hashes: &[String]) -> Result<u64> {
        let mut count = 0u64;
        for hash in hashes {
            self.cas.remove(hash)?;
            count += 1;
        }
        Ok(count)
    }

    fn list_hashes(&self) -> Result<Vec<HashEntry>> {
        let root = self.cas.root();
        let mut out = Vec::new();
        let entries =
            std::fs::read_dir(root).with_context(|| format!("list CAS root {}", root.display()))?;
        for entry in entries {
            let entry = entry?;
            let ft = entry.file_type()?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !ft.is_dir() {
                // Root-level hash files are allowed too.
                if let Ok(hash) = Self::validate_hash_name(&name_str) {
                    let meta = entry.metadata()?;
                    out.push(HashEntry {
                        hash,
                        size: meta.len(),
                        modified: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    });
                }
                continue;
            }
            // Prefix directories are two-character hex.
            if name_str.len() != 2 || !name_str.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }
            for obj in std::fs::read_dir(entry.path())? {
                let obj = obj?;
                if !obj.file_type()?.is_file() {
                    continue;
                }
                let obj_name = obj.file_name().to_string_lossy().to_string();
                if let Ok(hash) = Self::validate_hash_name(&obj_name) {
                    let meta = obj.metadata()?;
                    out.push(HashEntry {
                        hash,
                        size: meta.len(),
                        modified: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    });
                }
            }
        }
        Ok(out)
    }

    fn health(&self) -> Result<()> {
        // Write+read+remove a tiny probe file under the CAS root. This catches
        // the realistic production failures a dir-stat misses: the data volume
        // unmounted/gone, remounted read-only, full (ENOSPC), or with lost
        // permissions. The temp file is removed on drop.
        probe_dir_writable(self.cas.root(), "CAS root")
    }
}

/// Create, write, and drop a tiny probe file under `dir` to verify it is a
/// writable directory. Used by readiness checks.
fn probe_dir_writable(dir: &Path, label: &str) -> Result<()> {
    use std::io::Write;
    let mut f = tempfile::Builder::new()
        .prefix(".readyz-probe-")
        .tempfile_in(dir)
        .with_context(|| format!("{label} not writable: {}", dir.display()))?;
    f.write_all(b"ok")
        .with_context(|| format!("{label} write failed: {}", dir.display()))?;
    f.flush()
        .with_context(|| format!("{label} flush failed: {}", dir.display()))?;
    Ok(())
}

pub type StorageRef = Arc<dyn StorageBackend>;

/// Convenience constructor for the default local backend.
pub fn local<P: AsRef<Path>>(root: P) -> Result<StorageRef> {
    Ok(Arc::new(LocalStorage::new(root)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_path_rejects_traversal_and_absolute() {
        let tmp = tempfile::tempdir().unwrap();
        let s = LocalStorage::new(tmp.path()).unwrap();
        // Valid internal keys resolve under the root.
        let ok = s.meta_path("gc/orphans.json").unwrap();
        assert!(ok.starts_with(tmp.path()));
        // Anything that could escape the root is rejected.
        for bad in [
            "",
            "/etc/passwd",
            "..",
            "gc/../../etc/passwd",
            "a//b",
            "gc/",
        ] {
            assert!(s.meta_path(bad).is_err(), "key {bad:?} must be rejected");
        }
    }

    #[tokio::test]
    async fn meta_round_trips_and_is_absent_until_written() {
        let tmp = tempfile::tempdir().unwrap();
        let s = LocalStorage::new(tmp.path()).unwrap();
        assert!(s.get_meta("gc/orphans.json").await.unwrap().is_none());
        s.put_meta("gc/orphans.json", b"{}").await.unwrap();
        assert_eq!(
            s.get_meta("gc/orphans.json").await.unwrap().as_deref(),
            Some(&b"{}"[..])
        );
        // Overwrite replaces the contents.
        s.put_meta("gc/orphans.json", b"[1]").await.unwrap();
        assert_eq!(
            s.get_meta("gc/orphans.json").await.unwrap().as_deref(),
            Some(&b"[1]"[..])
        );
        // The metadata object is not surfaced as a content-addressed hash.
        assert!(s.list_hashes().unwrap().is_empty());
    }
}
