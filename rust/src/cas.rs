use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};

/// A minimal filesystem-backed content-addressed store.
#[derive(Clone)]
pub struct Cas {
    root: PathBuf,
}

impl Cas {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Validate that `hash` is a 64-character lowercase hex SHA-256 artifact id.
    pub fn validate_artifact_id(hash: &str) -> Result<()> {
        if hash.len() != 64 {
            anyhow::bail!("artifact id must be 64 hex characters");
        }
        Self::validate_lowercase_hex(hash).context("artifact id must be lowercase hex")
    }

    /// Validate a 40-character (SHA-1) or 64-character (SHA-256) object id.
    pub fn validate_object_id(hash: &str) -> Result<()> {
        if hash.len() != 40 && hash.len() != 64 {
            anyhow::bail!("object id must be 40 or 64 hex characters");
        }
        Self::validate_lowercase_hex(hash).context("object id must be lowercase hex")
    }

    fn validate_lowercase_hex(hash: &str) -> Result<()> {
        if !hash
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        {
            anyhow::bail!("string must be lowercase hex");
        }
        Ok(())
    }

    fn object_path(&self, hash: &str) -> Result<PathBuf> {
        Self::validate_object_id(hash).with_context(|| format!("invalid object id: {}", hash))?;
        Ok(self.root.join(&hash[..2]).join(hash))
    }

    pub fn put(&self, data: &[u8]) -> Result<String> {
        let hash = format!("{:x}", Sha256::digest(data));
        self.put_with_hash(&hash, data)?;
        Ok(hash)
    }

    pub fn put_with_hash(&self, hash: &str, data: &[u8]) -> Result<()> {
        let path = self.object_path(hash)?;
        // Content-addressed storage: if the object already exists, it is
        // guaranteed to be the same data. This also makes concurrent writers
        // idempotent.
        if path.exists() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Use a unique temp file per writer so concurrent puts of the same hash
        // do not collide on the same `.tmp` path.
        let tmp_path = path.with_extension(format!("tmp.{}", std::process::id()));
        let mut tmp_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .with_context(|| format!("create CAS object tmp {}", hash))?;
        tmp_file
            .write_all(data)
            .with_context(|| format!("write CAS object tmp {}", hash))?;
        tmp_file
            .sync_all()
            .with_context(|| format!("fsync CAS object tmp {}", hash))?;
        drop(tmp_file);
        std::fs::rename(&tmp_path, &path)
            .or_else(|e| {
                // Another concurrent writer may have won the race. Since the CAS
                // is content-addressed, the existing file is the correct data.
                if path.exists() {
                    let _ = std::fs::remove_file(&tmp_path);
                    Ok(())
                } else {
                    Err(e)
                }
            })
            .with_context(|| format!("rename CAS object {}", hash))?;
        Ok(())
    }

    pub fn put_file<P: AsRef<Path>>(&self, source: P) -> Result<(String, u64)> {
        let source = source.as_ref();
        let meta = std::fs::metadata(source)
            .with_context(|| format!("stat CAS source file {}", source.display()))?;
        let len = meta.len();
        std::fs::create_dir_all(&self.root)?;
        let mut input = std::fs::File::open(source)
            .with_context(|| format!("open CAS source file {}", source.display()))?;
        let mut tmp = tempfile::Builder::new()
            .prefix(".tmp.")
            .tempfile_in(&self.root)
            .with_context(|| format!("create CAS temp file in {}", self.root.display()))?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            let n = std::io::Read::read(&mut input, &mut buf)
                .with_context(|| format!("read CAS source file {}", source.display()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            tmp.as_file_mut()
                .write_all(&buf[..n])
                .context("write CAS temp file")?;
        }
        tmp.as_file_mut()
            .sync_all()
            .context("fsync CAS temp file")?;

        let hash = format!("{:x}", hasher.finalize());
        let path = self.object_path(&hash)?;
        let tmp_path = tmp.into_temp_path();
        if path.exists() {
            return Ok((hash, len));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(&tmp_path, &path)
            .or_else(|e| {
                if path.exists() {
                    let _ = std::fs::remove_file(&tmp_path);
                    Ok(())
                } else {
                    Err(e)
                }
            })
            .with_context(|| format!("rename CAS object {}", hash))?;
        Ok((hash, len))
    }

    pub fn get(&self, hash: &str) -> Result<Vec<u8>> {
        let path = self.object_path(hash)?;
        let data = std::fs::read(&path).with_context(|| format!("read CAS object {}", hash))?;
        let actual = format!("{:x}", Sha256::digest(&data));
        if actual != hash {
            anyhow::bail!(
                "CAS object {} is corrupt: hash mismatch (actual {})",
                hash,
                actual
            );
        }
        Ok(data)
    }

    /// Read a byte range from a CAS object without loading the whole file.
    pub fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let path = self.object_path(hash)?;
        let mut file = std::fs::File::open(&path)
            .with_context(|| format!("open CAS object {} for range read", hash))?;
        let meta = file.metadata().context("stat CAS object for range read")?;
        let file_len = meta.len();
        if start >= file_len || len > file_len - start {
            anyhow::bail!(
                "range {}+{} exceeds CAS object {} length {}",
                start,
                len,
                hash,
                file_len
            );
        }
        file.seek(SeekFrom::Start(start))
            .with_context(|| format!("seek CAS object {}", hash))?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf)
            .with_context(|| format!("read range from CAS object {}", hash))?;
        Ok(buf)
    }

    pub fn has(&self, hash: &str) -> bool {
        self.object_path(hash).map(|p| p.exists()).unwrap_or(false)
    }

    pub fn path(&self, hash: &str) -> PathBuf {
        // Fall back to a validated path so callers can still use it for local
        // existence checks; invalid hashes produce a path that cannot escape.
        self.object_path(hash)
            .unwrap_or_else(|_| self.root.join("__invalid__"))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Remove a local object. Best-effort: a missing object is not an error
    /// (used to evict build scratch once it is durable in remote storage).
    pub fn remove(&self, hash: &str) -> Result<()> {
        let path = self.object_path(hash)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("remove CAS object {}", hash)),
        }
    }
}

pub fn hash(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_artifact_id_accepts_sha256() {
        let h = "a".repeat(64);
        assert!(Cas::validate_artifact_id(&h).is_ok());
    }

    #[test]
    fn validate_artifact_id_rejects_traversal_and_non_hex() {
        assert!(Cas::validate_artifact_id("../etc/passwd").is_err());
        assert!(Cas::validate_artifact_id("..%2F..%2Fetc%2Fpasswd").is_err());
        assert!(Cas::validate_artifact_id(&"G".repeat(64)).is_err());
        assert!(Cas::validate_artifact_id("0123456789abcdef").is_err());
    }

    #[test]
    fn validate_object_id_accepts_sha1_and_sha256() {
        assert!(Cas::validate_object_id(&"a".repeat(40)).is_ok());
        assert!(Cas::validate_object_id(&"a".repeat(64)).is_ok());
        assert!(Cas::validate_object_id(&"a".repeat(50)).is_err());
    }

    #[test]
    fn object_path_cannot_escape_root() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        assert!(cas.object_path("../../etc/passwd").is_err());
        assert!(cas.object_path("abcdef").is_err());
        let valid = cas.object_path(&"a".repeat(64)).unwrap();
        assert!(valid.starts_with(tmp.path()));
        let sha1 = cas.object_path(&"a".repeat(40)).unwrap();
        assert!(sha1.starts_with(tmp.path()));
    }

    #[test]
    fn partial_write_is_not_served() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        let data = b"complete object";
        let hash = cas.put(data).unwrap();

        // Simulate a truncated object by writing garbage directly to the final path.
        let path = cas.path(&hash);
        std::fs::write(&path, b"trunc").unwrap();

        assert!(cas.get(&hash).is_err());
    }
}
