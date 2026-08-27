use crate::cas::Cas;
use anyhow::{Context, Result};
use async_trait::async_trait;
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
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Fetch the full object by hash.
    fn get(&self, hash: &str) -> Result<Vec<u8>>;

    /// Fetch a byte range from the object by hash.
    fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>>;

    /// Open a local artifact for bounded server-side streaming. Remote stores
    /// normally return signed URLs and leave this as `None`.
    fn open_file(&self, _hash: &str) -> Result<Option<std::fs::File>> {
        Ok(None)
    }

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

    /// Store an existing file by hash. Backends that support file or stream
    /// upload should override this; the default keeps compatibility for small
    /// metadata-style callers and uncommon test backends.
    async fn put_file_async(&self, hash: &str, path: &Path) -> Result<()> {
        let expected_hash = hash.to_string();
        let verify_path = path.to_path_buf();
        let (actual_hash, _len) = tokio::task::spawn_blocking(move || {
            crate::cas::hash_file(&verify_path)
                .with_context(|| format!("hash {} before storage upload", verify_path.display()))
        })
        .await
        .context("storage put_file hash task")??;
        if actual_hash != expected_hash {
            anyhow::bail!(
                "storage upload source {} hash mismatch: expected {}, actual {}",
                path.display(),
                expected_hash,
                actual_hash
            );
        }
        let data = tokio::fs::read(path)
            .await
            .with_context(|| format!("read file {} for storage upload", path.display()))?;
        self.put_async(hash, &data).await
    }

    /// Return the object size in bytes, if the backend can determine it
    /// without downloading the whole object.
    fn size(&self, hash: &str) -> Result<u64>;

    /// Confirm that the durable backend contains this object without consulting
    /// a disposable local cache.
    fn verify_durable_copy(&self, hash: &str) -> Result<()>;

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

    /// Regions where this backend stores durable bytes. Used for the
    /// storage-status usage breakdown. Defaults to "local" for filesystem-backed
    /// storage.
    fn regions(&self) -> Vec<String> {
        vec!["local".to_string()]
    }

    /// Cheap readiness probe used by `/readyz`. Should confirm the backend is
    /// reachable without doing real work. Default assumes healthy; the local
    /// backend does a write probe and the S3 backend does a bucket-reachability
    /// probe. Any new durable/remote backend should override this.
    fn health(&self) -> Result<()> {
        Ok(())
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

#[async_trait]
impl StorageBackend for LocalStorage {
    fn get(&self, hash: &str) -> Result<Vec<u8>> {
        self.cas.get(hash)
    }

    fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        self.cas.get_range(hash, start, len)
    }

    fn open_file(&self, hash: &str) -> Result<Option<std::fs::File>> {
        let path = self.cas.path(hash);
        let file = std::fs::File::open(&path)
            .with_context(|| format!("open CAS object {hash} at {}", path.display()))?;
        Ok(Some(file))
    }

    fn put(&self, hash: &str, data: &[u8]) -> Result<()> {
        self.cas.put_with_hash(hash, data)
    }

    async fn put_file_async(&self, hash: &str, path: &Path) -> Result<()> {
        let cas = self.cas.clone();
        let hash = hash.to_string();
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || cas.put_file_with_hash(&hash, &path))
            .await
            .context("local storage put_file task")??;
        Ok(())
    }

    fn size(&self, hash: &str) -> Result<u64> {
        let path = self.cas.path(hash);
        let meta = std::fs::metadata(&path).with_context(|| format!("stat CAS object {}", hash))?;
        Ok(meta.len())
    }

    fn verify_durable_copy(&self, hash: &str) -> Result<()> {
        self.size(hash).map(|_| ())
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
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingStorage {
        puts: Mutex<Vec<(String, Vec<u8>)>>,
    }

    #[async_trait::async_trait]
    impl StorageBackend for RecordingStorage {
        fn get(&self, _hash: &str) -> Result<Vec<u8>> {
            anyhow::bail!("unsupported")
        }

        fn get_range(&self, _hash: &str, _start: u64, _len: u64) -> Result<Vec<u8>> {
            anyhow::bail!("unsupported")
        }

        fn put(&self, hash: &str, data: &[u8]) -> Result<()> {
            self.puts
                .lock()
                .unwrap()
                .push((hash.to_string(), data.to_vec()));
            Ok(())
        }

        fn size(&self, _hash: &str) -> Result<u64> {
            anyhow::bail!("unsupported")
        }

        fn verify_durable_copy(&self, _hash: &str) -> Result<()> {
            anyhow::bail!("unsupported")
        }
    }

    #[tokio::test]
    async fn put_file_async_rejects_source_hash_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let s = LocalStorage::new(tmp.path()).unwrap();
        let source = tmp.path().join("source.bin");
        std::fs::write(&source, b"wrong bytes").unwrap();
        let expected = crate::cas::hash(b"right bytes");

        let err = s.put_file_async(&expected, &source).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("hash mismatch"),
            "unexpected error: {err:#}"
        );
        assert!(
            !s.cas().path(&expected).exists(),
            "mismatched file upload must not publish a final object"
        );
    }

    #[tokio::test]
    async fn put_file_async_repairs_existing_corrupt_object() {
        let tmp = tempfile::tempdir().unwrap();
        let s = LocalStorage::new(tmp.path()).unwrap();
        let source = tmp.path().join("source.bin");
        std::fs::write(&source, b"correct bytes").unwrap();
        let expected = crate::cas::hash(b"correct bytes");
        let object_path = s.cas().path(&expected);
        std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        std::fs::write(&object_path, b"corrupt bytes").unwrap();

        s.put_file_async(&expected, &source).await.unwrap();
        assert_eq!(std::fs::read(&source).unwrap(), b"correct bytes");
        assert_eq!(std::fs::read(&object_path).unwrap(), b"correct bytes");
    }

    #[tokio::test]
    async fn default_put_file_async_rejects_source_hash_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source.bin");
        std::fs::write(&source, b"wrong bytes").unwrap();
        let s = RecordingStorage::default();
        let expected = crate::cas::hash(b"right bytes");

        let err = s.put_file_async(&expected, &source).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("hash mismatch"),
            "unexpected error: {err:#}"
        );
        assert!(s.puts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn default_put_file_async_uploads_matching_file() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source.bin");
        std::fs::write(&source, b"correct bytes").unwrap();
        let s = RecordingStorage::default();
        let expected = crate::cas::hash(b"correct bytes");

        s.put_file_async(&expected, &source).await.unwrap();
        assert_eq!(
            *s.puts.lock().unwrap(),
            vec![(expected, b"correct bytes".to_vec())]
        );
    }
}
