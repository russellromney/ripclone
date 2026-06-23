use crate::cas::Cas;
use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub mod s3_storage;
pub use s3_storage::S3Storage;

/// Abstract storage backend for content-addressed artifacts.
///
/// The local filesystem-backed implementation (`LocalStorage`) is the default.
/// Object-storage backends (S3/R2/Tigris) can implement the same trait and
/// return signed URLs so clients read directly from the CDN.
pub trait StorageBackend: Send + Sync {
    /// Fetch the full object by hash.
    fn get(&self, hash: &str) -> Result<Vec<u8>>;

    /// Fetch a byte range from the object by hash.
    fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>>;

    /// Store the full object by hash.
    fn put(&self, hash: &str, data: &[u8]) -> Result<()>;

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
}

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
}

impl LocalStorage {
    fn validate_hash_name(name: &str) -> Result<String> {
        crate::cas::Cas::validate_artifact_id(name)
            .with_context(|| format!("invalid CAS object name: {}", name))?;
        Ok(name.to_string())
    }
}

pub type StorageRef = Arc<dyn StorageBackend>;

/// Convenience constructor for the default local backend.
pub fn local<P: AsRef<Path>>(root: P) -> Result<StorageRef> {
    Ok(Arc::new(LocalStorage::new(root)?))
}
