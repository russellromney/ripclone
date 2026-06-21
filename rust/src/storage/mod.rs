use crate::cas::Cas;
use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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
}

pub type StorageRef = Arc<dyn StorageBackend>;

/// Convenience constructor for the default local backend.
pub fn local<P: AsRef<Path>>(root: P) -> Result<StorageRef> {
    Ok(Arc::new(LocalStorage::new(root)?))
}
