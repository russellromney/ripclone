use anyhow::{Context, Result};
use sha1::{Digest, Sha1};
use std::path::{Path, PathBuf};

/// A minimal filesystem-backed content-addressed store.
pub struct Cas {
    root: PathBuf,
}

impl Cas {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path(&self, hash: &str) -> PathBuf {
        // Use first two chars as subdirectory to avoid huge flat dirs.
        if hash.len() >= 2 {
            self.root.join(&hash[..2]).join(hash)
        } else {
            self.root.join(hash)
        }
    }

    pub fn put(&self, data: &[u8]) -> Result<String> {
        let hash = format!("{:x}", Sha1::digest(data));
        let path = self.path(&hash);
        if !path.exists() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, data)?;
        }
        Ok(hash)
    }

    pub fn put_with_hash(&self, hash: &str, data: &[u8]) -> Result<()> {
        let path = self.path(hash);
        if !path.exists() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, data)?;
        }
        Ok(())
    }

    pub fn get(&self, hash: &str) -> Result<Vec<u8>> {
        let path = self.path(hash);
        std::fs::read(&path).with_context(|| format!("CAS get failed for {}", hash))
    }

    pub fn has(&self, hash: &str) -> bool {
        self.path(hash).exists()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// Compute SHA-1 of bytes.
pub fn hash(data: &[u8]) -> String {
    format!("{:x}", Sha1::digest(data))
}
