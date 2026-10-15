use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

enum ObjectHasher {
    Sha1(sha1::Sha1),
    Sha256(Sha256),
}

impl ObjectHasher {
    fn for_object_id(hash: &str) -> Result<Self> {
        Cas::validate_object_id(hash)?;
        if hash.len() == 40 {
            Ok(Self::Sha1(sha1::Sha1::new()))
        } else {
            Ok(Self::Sha256(Sha256::new()))
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Sha1(hasher) => hasher.update(data),
            Self::Sha256(hasher) => hasher.update(data),
        }
    }

    fn finalize_hex(self) -> String {
        match self {
            Self::Sha1(hasher) => hex::encode(hasher.finalize()),
            Self::Sha256(hasher) => hex::encode(hasher.finalize()),
        }
    }
}

/// A minimal filesystem-backed content-addressed store.
#[derive(Clone)]
pub struct Cas {
    root: PathBuf,
    /// Sync each object's directory entry after the rename that installs it,
    /// so an object reported stored survives a crash. Off for a cache, whose
    /// lost entry is only a miss.
    durable: bool,
}

impl Cas {
    /// A store that is the source of truth: each stored object is durable.
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        Self::open(root, true)
    }

    /// A cache in front of a durable store (remote storage, or the client's
    /// download cache). A crash can lose its newest entries, which only costs
    /// a re-read, so it skips the directory syncs.
    pub fn cache<P: AsRef<Path>>(root: P) -> Result<Self> {
        Self::open(root, false)
    }

    fn open<P: AsRef<Path>>(root: P, durable: bool) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, durable })
    }

    /// Create the fan-out directory that holds `path`. A new directory's own
    /// entry in the root is synced before any object is renamed into it.
    fn create_object_dir(&self, path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("CAS object path has no parent: {}", path.display()))?;
        if parent.is_dir() {
            return Ok(());
        }
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create CAS directory {}", parent.display()))?;
        if self.durable {
            sync_dir(&self.root)?;
        }
        Ok(())
    }

    /// Make the rename that installed `path` durable.
    fn sync_installed(&self, path: &Path) -> Result<()> {
        match path.parent() {
            Some(parent) if self.durable => sync_dir(parent),
            _ => Ok(()),
        }
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
        let hash = hex::encode(Sha256::digest(data));
        self.put_with_hash(&hash, data)?;
        Ok(hash)
    }

    pub fn put_with_hash(&self, hash: &str, data: &[u8]) -> Result<()> {
        let actual = hash_bytes_for_object_id(hash, data)?;
        if actual != hash {
            anyhow::bail!("hash mismatch: expected {}, actual {}", hash, actual);
        }
        let path = self.object_path(hash)?;
        if path.exists() && self.verify_object(hash).is_ok() {
            return Ok(());
        }
        self.create_object_dir(&path)?;
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("CAS object path has no parent: {}", path.display()))?;
        let mut tmp = tempfile::Builder::new()
            .prefix(".tmp.")
            .tempfile_in(parent)
            .with_context(|| format!("create CAS object tmp {}", hash))?;
        let write_start = Instant::now();
        tmp.as_file_mut()
            .write_all(data)
            .with_context(|| format!("write CAS object tmp {}", hash))?;
        crate::perf::record_cas_write(write_start.elapsed(), data.len() as u64);
        let fsync_start = Instant::now();
        tmp.as_file_mut()
            .sync_all()
            .with_context(|| format!("fsync CAS object tmp {}", hash))?;
        crate::perf::record_cas_fsync(fsync_start.elapsed());
        self.install_verified_temp(hash, tmp.into_temp_path(), &path)?;
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
        let write_start = Instant::now();
        loop {
            let n = input
                .read(&mut buf)
                .with_context(|| format!("read CAS source file {}", source.display()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            tmp.as_file_mut()
                .write_all(&buf[..n])
                .context("write CAS temp file")?;
        }
        crate::perf::record_cas_write(write_start.elapsed(), len);
        let fsync_start = Instant::now();
        tmp.as_file_mut()
            .sync_all()
            .context("fsync CAS temp file")?;
        crate::perf::record_cas_fsync(fsync_start.elapsed());

        let hash = hex::encode(hasher.finalize());
        let path = self.object_path(&hash)?;
        let tmp_path = tmp.into_temp_path();
        if path.exists()
            && let Ok(existing_len) = self.verify_object(&hash)
        {
            return Ok((hash, existing_len));
        }
        self.create_object_dir(&path)?;
        self.install_verified_temp(&hash, tmp_path, &path)?;
        Ok((hash, len))
    }

    pub fn put_file_with_hash<P: AsRef<Path>>(&self, hash: &str, source: P) -> Result<u64> {
        let source = source.as_ref();
        let meta = std::fs::metadata(source)
            .with_context(|| format!("stat CAS source file {}", source.display()))?;
        let len = meta.len();
        let path = self.object_path(hash)?;
        if path.exists()
            && let Ok(existing_len) = self.verify_object(hash)
        {
            return Ok(existing_len);
        }
        std::fs::create_dir_all(&self.root)?;
        let mut input = std::fs::File::open(source)
            .with_context(|| format!("open CAS source file {}", source.display()))?;
        let mut tmp = tempfile::Builder::new()
            .prefix(".tmp.")
            .tempfile_in(&self.root)
            .with_context(|| format!("create CAS temp file in {}", self.root.display()))?;
        let mut hasher = ObjectHasher::for_object_id(hash)?;
        let mut buf = vec![0u8; 1024 * 1024];
        let write_start = Instant::now();
        loop {
            let n = input
                .read(&mut buf)
                .with_context(|| format!("read CAS source file {}", source.display()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            tmp.as_file_mut()
                .write_all(&buf[..n])
                .context("write CAS temp file")?;
        }
        crate::perf::record_cas_write(write_start.elapsed(), len);
        let actual = hasher.finalize_hex();
        if actual != hash {
            anyhow::bail!(
                "CAS source file {} hash mismatch: expected {}, actual {}",
                source.display(),
                hash,
                actual
            );
        }
        let fsync_start = Instant::now();
        tmp.as_file_mut()
            .sync_all()
            .context("fsync CAS temp file")?;
        crate::perf::record_cas_fsync(fsync_start.elapsed());

        let tmp_path = tmp.into_temp_path();
        self.create_object_dir(&path)?;
        self.install_verified_temp(hash, tmp_path, &path)?;
        Ok(len)
    }

    pub fn install_hashed_file<P: AsRef<Path>>(&self, hash: &str, source: P) -> Result<()> {
        let source = source.as_ref();
        let path = self.object_path(hash)?;
        if path.exists() && self.verify_object(hash).is_ok() {
            let _ = std::fs::remove_file(source);
            return Ok(());
        }
        let (actual, _len) = hash_file_for_object_id(hash, source)
            .with_context(|| format!("hash CAS source file {}", source.display()))?;
        if actual != hash {
            anyhow::bail!("hash mismatch: expected {}, actual {}", hash, actual);
        }
        self.create_object_dir(&path)?;
        match std::fs::rename(source, &path) {
            Ok(()) => {}
            Err(_first_err) if path.exists() => {
                if self.verify_object(hash).is_ok() {
                    let _ = std::fs::remove_file(source);
                    return Ok(());
                }
                std::fs::remove_file(&path)
                    .with_context(|| format!("remove corrupt CAS object {}", hash))?;
                std::fs::rename(source, &path)
                    .with_context(|| format!("replace corrupt CAS object {}", hash))?;
            }
            Err(e) => return Err(e).with_context(|| format!("rename CAS object {}", hash)),
        }
        self.sync_installed(&path)
    }

    fn install_verified_temp(
        &self,
        hash: &str,
        tmp_path: tempfile::TempPath,
        path: &Path,
    ) -> Result<()> {
        match std::fs::rename(&tmp_path, path) {
            Ok(()) => {}
            Err(_first_err) if path.exists() => {
                if self.verify_object(hash).is_ok() {
                    let _ = tmp_path.close();
                    return Ok(());
                }
                std::fs::remove_file(path)
                    .with_context(|| format!("remove corrupt CAS object {}", hash))?;
                std::fs::rename(&tmp_path, path)
                    .with_context(|| format!("replace corrupt CAS object {}", hash))?;
            }
            Err(e) => return Err(e).with_context(|| format!("rename CAS object {}", hash)),
        }
        self.sync_installed(path)
    }

    pub fn get(&self, hash: &str) -> Result<Vec<u8>> {
        let path = self.object_path(hash)?;
        let read_start = Instant::now();
        let data = std::fs::read(&path).with_context(|| format!("read CAS object {}", hash))?;
        crate::perf::record_cas_read(read_start.elapsed(), data.len() as u64);
        let actual = hash_bytes_for_object_id(hash, &data)?;
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
        use std::io::{Seek, SeekFrom};
        let path = self.object_path(hash)?;
        let mut file = std::fs::File::open(&path)
            .with_context(|| format!("open CAS object {} for range read", hash))?;
        let meta = file.metadata().context("stat CAS object for range read")?;
        let file_len = meta.len();
        let end = start.checked_add(len).context("CAS range end overflow")?;
        if start > file_len || end > file_len {
            anyhow::bail!(
                "range {}+{} exceeds CAS object {} length {}",
                start,
                len,
                hash,
                file_len
            );
        }
        let len = usize::try_from(len).context("CAS range length does not fit in usize")?;
        let len_u64 = u64::try_from(len).context("CAS range length overflows u64")?;
        file.seek(SeekFrom::Start(start))
            .with_context(|| format!("seek CAS object {}", hash))?;
        let mut buf = vec![0u8; len];
        let read_start = Instant::now();
        file.read_exact(&mut buf)
            .with_context(|| format!("read range from CAS object {}", hash))?;
        crate::perf::record_cas_read(read_start.elapsed(), len_u64);
        Ok(buf)
    }

    pub fn verify_object(&self, hash: &str) -> Result<u64> {
        let path = self.object_path(hash)?;
        let mut file =
            std::fs::File::open(&path).with_context(|| format!("open CAS object {}", hash))?;
        let mut hasher = ObjectHasher::for_object_id(hash)?;
        let mut buf = vec![0u8; 1024 * 1024];
        let mut len = 0u64;
        let read_start = Instant::now();
        loop {
            let n = file
                .read(&mut buf)
                .with_context(|| format!("read CAS object {}", hash))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            len += n as u64;
        }
        crate::perf::record_cas_read(read_start.elapsed(), len);
        let actual = hasher.finalize_hex();
        if actual != hash {
            anyhow::bail!(
                "CAS object {} is corrupt: hash mismatch (actual {})",
                hash,
                actual
            );
        }
        Ok(len)
    }

    pub fn copy_to_writer_verified_with<W, F>(
        &self,
        hash: &str,
        writer: &mut W,
        mut observe: F,
    ) -> Result<u64>
    where
        W: Write,
        F: FnMut(&[u8]),
    {
        let path = self.object_path(hash)?;
        let mut file =
            std::fs::File::open(&path).with_context(|| format!("open CAS object {}", hash))?;
        let mut hasher = ObjectHasher::for_object_id(hash)?;
        let mut buf = vec![0u8; 1024 * 1024];
        let mut len = 0u64;
        let read_start = Instant::now();
        loop {
            let n = file
                .read(&mut buf)
                .with_context(|| format!("read CAS object {}", hash))?;
            if n == 0 {
                break;
            }
            writer
                .write_all(&buf[..n])
                .with_context(|| format!("write streamed CAS object {}", hash))?;
            hasher.update(&buf[..n]);
            observe(&buf[..n]);
            len += n as u64;
        }
        crate::perf::record_cas_read(read_start.elapsed(), len);
        let actual = hasher.finalize_hex();
        if actual != hash {
            anyhow::bail!(
                "CAS object {} is corrupt: hash mismatch (actual {})",
                hash,
                actual
            );
        }
        Ok(len)
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
    /// (used to discard a cached build copy once remote storage is durable).
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
    hex::encode(Sha256::digest(data))
}

/// Flush a directory's entries (a rename or a new child) to disk.
fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("fsync CAS directory {}", dir.display()))
}

fn hash_bytes_for_object_id(hash: &str, data: &[u8]) -> Result<String> {
    let mut hasher = ObjectHasher::for_object_id(hash)?;
    hasher.update(data);
    Ok(hasher.finalize_hex())
}

pub fn hash_file<P: AsRef<Path>>(source: P) -> Result<(String, u64)> {
    hash_file_sha256(source)
}

fn hash_file_for_object_id<P: AsRef<Path>>(hash: &str, source: P) -> Result<(String, u64)> {
    hash_file_with_hasher(source, ObjectHasher::for_object_id(hash)?)
}

fn hash_file_sha256<P: AsRef<Path>>(source: P) -> Result<(String, u64)> {
    hash_file_with_hasher(source, ObjectHasher::Sha256(Sha256::new()))
}

fn hash_file_with_hasher<P: AsRef<Path>>(
    source: P,
    mut hasher: ObjectHasher,
) -> Result<(String, u64)> {
    let source = source.as_ref();
    let mut input = std::fs::File::open(source)
        .with_context(|| format!("open file {} for hashing", source.display()))?;
    let mut buf = vec![0u8; 1024 * 1024];
    let mut len = 0u64;
    loop {
        let n = input
            .read(&mut buf)
            .with_context(|| format!("read file {} for hashing", source.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        len += n as u64;
    }
    Ok((hasher.finalize_hex(), len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_and_object_id_validation() {
        // (id, valid artifact id (sha256), valid object id (sha1 or sha256))
        for (id, artifact, object) in [
            ("a".repeat(64), true, true),
            ("a".repeat(40), false, true),
            ("a".repeat(50), false, false),
            ("G".repeat(64), false, false),
            ("0123456789abcdef".to_string(), false, false),
            ("../etc/passwd".to_string(), false, false),
            ("..%2F..%2Fetc%2Fpasswd".to_string(), false, false),
        ] {
            assert_eq!(Cas::validate_artifact_id(&id).is_ok(), artifact, "{id}");
            assert_eq!(Cas::validate_object_id(&id).is_ok(), object, "{id}");
        }
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
    fn get_range_allows_empty_range_at_object_end() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        let data = b"range data";
        let hash = cas.put(data).unwrap();

        assert_eq!(
            cas.get_range(&hash, data.len() as u64, 0).unwrap(),
            Vec::<u8>::new()
        );
        assert!(cas.get_range(&hash, data.len() as u64 + 1, 0).is_err());
        assert!(cas.get_range(&hash, u64::MAX, 1).is_err());
    }

    /// Every write path replaces a corrupt object already at the hash's path.
    #[test]
    fn writes_repair_an_existing_corrupt_object() {
        type WriteFn = fn(&Cas, &str, &Path) -> Result<()>;
        let writes: [(&str, WriteFn, bool); 3] = [
            (
                "put_with_hash",
                |cas, hash, source| cas.put_with_hash(hash, &std::fs::read(source)?),
                true,
            ),
            (
                "put_file_with_hash",
                |cas, hash, source| cas.put_file_with_hash(hash, source).map(|_| ()),
                true,
            ),
            (
                "install_hashed_file",
                |cas, hash, source| cas.install_hashed_file(hash, source),
                false,
            ),
        ];
        for (case, write, keeps_source) in writes {
            let tmp = tempfile::tempdir().unwrap();
            let cas = Cas::new(tmp.path()).unwrap();
            let source = tmp.path().join("source");
            std::fs::write(&source, b"correct").unwrap();
            let hash = hash(b"correct");
            let object_path = cas.path(&hash);
            std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
            std::fs::write(&object_path, b"wrong").unwrap();

            write(&cas, &hash, &source).unwrap();
            assert_eq!(std::fs::read(&object_path).unwrap(), b"correct", "{case}");
            assert_eq!(source.exists(), keeps_source, "{case}");
        }
    }

    /// A source that does not match its hash is never installed, whether or
    /// not an object (here a corrupt one) already sits at the path; the source
    /// is kept.
    #[test]
    fn install_hashed_file_rejects_a_mismatched_source() {
        for existing in [None, Some(&b"corrupt"[..])] {
            let tmp = tempfile::tempdir().unwrap();
            let cas = Cas::new(tmp.path()).unwrap();
            let source = tmp.path().join("bundle.tmp");
            std::fs::write(&source, b"wrong").unwrap();
            let hash = hash(b"correct");
            let object_path = cas.path(&hash);
            if let Some(existing) = existing {
                std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
                std::fs::write(&object_path, existing).unwrap();
            }

            assert!(cas.install_hashed_file(&hash, &source).is_err());
            assert_eq!(std::fs::read(&source).unwrap(), b"wrong");
            assert_eq!(std::fs::read(&object_path).ok().as_deref(), existing);
        }
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
