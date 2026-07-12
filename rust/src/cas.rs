use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[cfg(test)]
thread_local! {
    static CANCEL_AWARE_HASH_BARRIER: std::cell::RefCell<Option<std::sync::Arc<std::sync::Barrier>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn install_cancel_aware_hash_barrier(barrier: std::sync::Arc<std::sync::Barrier>) {
    CANCEL_AWARE_HASH_BARRIER.with(|slot| *slot.borrow_mut() = Some(barrier));
}

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
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
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
        if path.exists() {
            if let Ok(existing_len) = self.verify_object(&hash) {
                return Ok((hash, existing_len));
            }
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        self.install_verified_temp(&hash, tmp_path, &path)?;
        Ok((hash, len))
    }

    pub fn put_file_with_hash<P: AsRef<Path>>(&self, hash: &str, source: P) -> Result<u64> {
        let source = source.as_ref();
        let meta = std::fs::metadata(source)
            .with_context(|| format!("stat CAS source file {}", source.display()))?;
        let len = meta.len();
        let path = self.object_path(hash)?;
        if path.exists() {
            if let Ok(existing_len) = self.verify_object(hash) {
                return Ok(existing_len);
            }
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
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
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
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::rename(source, &path) {
            Ok(()) => Ok(()),
            Err(_first_err) if path.exists() => {
                if self.verify_object(hash).is_ok() {
                    let _ = std::fs::remove_file(source);
                    return Ok(());
                }
                std::fs::remove_file(&path)
                    .with_context(|| format!("remove corrupt CAS object {}", hash))?;
                std::fs::rename(source, &path)
                    .with_context(|| format!("replace corrupt CAS object {}", hash))
            }
            Err(e) => Err(e).with_context(|| format!("rename CAS object {}", hash)),
        }
    }

    pub(crate) fn install_hashed_file_cancelled_bounded<P: AsRef<Path>>(
        &self,
        hash: &str,
        source: P,
        maximum: u64,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        let source = source.as_ref();
        if source.metadata()?.len() > maximum {
            anyhow::bail!("CAS install source exceeds bounded verification limit");
        }
        let path = self.object_path(hash)?;
        if path.exists() {
            match self.verify_object_cancelled_bounded(hash, maximum, cancelled) {
                Ok(_) => {
                    std::fs::remove_file(source)?;
                    return Ok(());
                }
                Err(error) if cancelled.is_cancelled() => return Err(error),
                Err(_) => {}
            }
        }
        let mut input = std::fs::File::open(source)
            .with_context(|| format!("open CAS install source {}", source.display()))?;
        let mut hasher = ObjectHasher::for_object_id(hash)?;
        let mut length = 0u64;
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            if cancelled.is_cancelled() {
                anyhow::bail!("CAS install verification cancelled");
            }
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            length = length
                .checked_add(read as u64)
                .context("CAS install source length overflow")?;
            if length > maximum {
                anyhow::bail!("CAS install source exceeds bounded verification limit");
            }
            hasher.update(&buffer[..read]);
        }
        if hasher.finalize_hex() != hash {
            anyhow::bail!("hash mismatch while installing CAS object {hash}");
        }
        if cancelled.is_cancelled() {
            anyhow::bail!("CAS install verification cancelled");
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::rename(source, &path) {
            Ok(()) => Ok(()),
            Err(_first_error) if path.exists() => {
                if self.verify_object_cancelled_bounded(hash, maximum, cancelled)? == length {
                    std::fs::remove_file(source)?;
                    Ok(())
                } else {
                    anyhow::bail!("racing CAS object has unexpected length")
                }
            }
            Err(error) => Err(error).with_context(|| format!("rename CAS object {hash}")),
        }
    }

    fn install_verified_temp(
        &self,
        hash: &str,
        tmp_path: tempfile::TempPath,
        path: &Path,
    ) -> Result<()> {
        match std::fs::rename(&tmp_path, path) {
            Ok(()) => Ok(()),
            Err(_first_err) if path.exists() => {
                if self.verify_object(hash).is_ok() {
                    let _ = tmp_path.close();
                    return Ok(());
                }
                std::fs::remove_file(path)
                    .with_context(|| format!("remove corrupt CAS object {}", hash))?;
                std::fs::rename(&tmp_path, path)
                    .with_context(|| format!("replace corrupt CAS object {}", hash))
            }
            Err(e) => Err(e).with_context(|| format!("rename CAS object {}", hash)),
        }
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

    /// Hash-verified streaming read with a hard allocation/I/O ceiling and
    /// cooperative cancellation. The metadata check is only an early reject:
    /// the loop enforces the ceiling again in case the file grows concurrently.
    pub(crate) fn get_cancelled_bounded(
        &self,
        hash: &str,
        maximum: u64,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<Vec<u8>> {
        let path = self.object_path(hash)?;
        let mut input =
            std::fs::File::open(&path).with_context(|| format!("open CAS object {}", hash))?;
        let expected = input.metadata()?.len();
        if expected > maximum {
            anyhow::bail!("CAS object exceeds bounded read limit");
        }
        let read_start = Instant::now();
        // This helper is reserved for small control objects. Do not turn an
        // authenticated-but-hostile descriptor into an eager allocation: grow
        // from one small read chunk while enforcing `maximum` on every read.
        let initial_capacity = usize::try_from(expected.min(64 * 1024))
            .context("CAS control object size does not fit this platform")?;
        let mut data = Vec::with_capacity(initial_capacity);
        let mut hasher = ObjectHasher::for_object_id(hash)?;
        let mut chunk = vec![0u8; 1024 * 1024];
        loop {
            if cancelled.is_cancelled() {
                anyhow::bail!("CAS object read cancelled");
            }
            let read = input
                .read(&mut chunk)
                .with_context(|| format!("read CAS object {}", hash))?;
            if read == 0 {
                break;
            }
            if (data.len() as u64)
                .checked_add(read as u64)
                .is_none_or(|len| len > maximum)
            {
                anyhow::bail!("CAS object exceeds bounded read limit");
            }
            #[cfg(test)]
            std::thread::sleep(std::time::Duration::from_millis(1));
            hasher.update(&chunk[..read]);
            data.extend_from_slice(&chunk[..read]);
        }
        crate::perf::record_cas_read(read_start.elapsed(), data.len() as u64);
        let actual = hasher.finalize_hex();
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
        let read_start = Instant::now();
        file.read_exact(&mut buf)
            .with_context(|| format!("read range from CAS object {}", hash))?;
        crate::perf::record_cas_read(read_start.elapsed(), len);
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

    /// Verify a cached object without an unbounded read and remain responsive
    /// while hashing multi-gigabyte pinned artifacts.
    pub(crate) fn verify_object_cancelled_bounded(
        &self,
        hash: &str,
        maximum: u64,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<u64> {
        let path = self.object_path(hash)?;
        let mut file =
            std::fs::File::open(&path).with_context(|| format!("open CAS object {hash}"))?;
        if file.metadata()?.len() > maximum {
            anyhow::bail!("CAS object exceeds bounded verification limit");
        }
        let mut hasher = ObjectHasher::for_object_id(hash)?;
        let mut buf = vec![0u8; 1024 * 1024];
        let mut len = 0u64;
        loop {
            if cancelled.is_cancelled() {
                anyhow::bail!("CAS object verification cancelled");
            }
            let n = file
                .read(&mut buf)
                .with_context(|| format!("read CAS object {hash}"))?;
            if n == 0 {
                break;
            }
            #[cfg(test)]
            std::thread::sleep(std::time::Duration::from_millis(1));
            len = len
                .checked_add(n as u64)
                .context("CAS object length overflow")?;
            if len > maximum {
                anyhow::bail!("CAS object exceeds bounded verification limit");
            }
            hasher.update(&buf[..n]);
            #[cfg(test)]
            if let Some(barrier) = CANCEL_AWARE_HASH_BARRIER.with(|slot| slot.borrow_mut().take()) {
                // A one-shot, thread-local hook makes cancellation tests wait
                // for this exact verifier to hash its first chunk. It cannot be
                // released by unrelated tests or block subsequent chunks.
                barrier.wait();
            }
        }
        if hasher.finalize_hex() != hash {
            anyhow::bail!("CAS object {hash} is corrupt");
        }
        Ok(len)
    }

    pub fn copy_to_writer_verified<W: Write>(&self, hash: &str, writer: &mut W) -> Result<u64> {
        self.copy_to_writer_verified_with(hash, writer, |_| {})
    }

    pub fn copy_to_writer_verified_cancelled<W: Write>(
        &self,
        hash: &str,
        writer: &mut W,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<u64> {
        self.copy_to_writer_verified_cancelled_bounded(hash, writer, u64::MAX, cancelled)
    }

    pub(crate) fn copy_to_writer_verified_cancelled_bounded<W: Write>(
        &self,
        hash: &str,
        writer: &mut W,
        maximum: u64,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<u64> {
        let path = self.object_path(hash)?;
        let mut file =
            std::fs::File::open(&path).with_context(|| format!("open CAS object {hash}"))?;
        if file.metadata()?.len() > maximum {
            anyhow::bail!("CAS object exceeds bounded streaming limit");
        }
        let mut hasher = ObjectHasher::for_object_id(hash)?;
        let mut buf = vec![0u8; 1024 * 1024];
        let mut len = 0u64;
        loop {
            if cancelled.is_cancelled() {
                anyhow::bail!("streaming CAS object cancelled");
            }
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            let next = len
                .checked_add(n as u64)
                .context("streaming CAS object length overflow")?;
            if next > maximum {
                anyhow::bail!("CAS object exceeds bounded streaming limit");
            }
            writer.write_all(&buf[..n])?;
            hasher.update(&buf[..n]);
            len = next;
        }
        if hasher.finalize_hex() != hash {
            anyhow::bail!("CAS object {hash} is corrupt");
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
    hex::encode(Sha256::digest(data))
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
    fn put_with_hash_repairs_existing_corrupt_object() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        let hash = hash(b"correct");
        let object_path = cas.path(&hash);
        std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        std::fs::write(&object_path, b"wrong").unwrap();

        cas.put_with_hash(&hash, b"correct").unwrap();
        assert_eq!(std::fs::read(&object_path).unwrap(), b"correct");
    }

    #[test]
    fn put_file_with_hash_repairs_existing_corrupt_object() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        let source = tmp.path().join("source");
        std::fs::write(&source, b"correct").unwrap();
        let hash = hash(b"correct");
        let object_path = cas.path(&hash);
        std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        std::fs::write(&object_path, b"wrong").unwrap();

        cas.put_file_with_hash(&hash, &source).unwrap();
        assert_eq!(std::fs::read(&object_path).unwrap(), b"correct");
        assert_eq!(std::fs::read(&source).unwrap(), b"correct");
    }

    #[test]
    fn install_hashed_file_repairs_existing_corrupt_object_and_removes_source() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        let source = tmp.path().join("bundle.tmp");
        std::fs::write(&source, b"correct bundle").unwrap();
        let hash = hash(b"correct bundle");
        let object_path = cas.path(&hash);
        std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        std::fs::write(&object_path, b"corrupt bundle").unwrap();

        cas.install_hashed_file(&hash, &source).unwrap();
        assert_eq!(std::fs::read(&object_path).unwrap(), b"correct bundle");
        assert!(!source.exists(), "source temp file should be installed");
    }

    #[test]
    fn install_hashed_file_rejects_mismatched_replacement_and_keeps_source() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        let source = tmp.path().join("bundle.tmp");
        std::fs::write(&source, b"wrong replacement").unwrap();
        let hash = hash(b"correct bundle");
        let object_path = cas.path(&hash);
        std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        std::fs::write(&object_path, b"corrupt bundle").unwrap();

        let err = cas.install_hashed_file(&hash, &source).unwrap_err();
        assert!(
            format!("{err:#}").contains("hash mismatch"),
            "unexpected error: {err:#}"
        );
        assert_eq!(std::fs::read(&source).unwrap(), b"wrong replacement");
        assert_eq!(std::fs::read(&object_path).unwrap(), b"corrupt bundle");
    }

    #[test]
    fn install_hashed_file_rejects_mismatched_new_object_and_keeps_source() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        let source = tmp.path().join("bundle.tmp");
        std::fs::write(&source, b"wrong bundle").unwrap();
        let hash = hash(b"correct bundle");
        let object_path = cas.path(&hash);

        let err = cas.install_hashed_file(&hash, &source).unwrap_err();
        assert!(
            format!("{err:#}").contains("hash mismatch"),
            "unexpected error: {err:#}"
        );
        assert_eq!(std::fs::read(&source).unwrap(), b"wrong bundle");
        assert!(!object_path.exists());
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

    #[test]
    fn bounded_control_read_rejects_sparse_hostile_object_before_read_or_cancel() {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Cas::new(tmp.path()).unwrap();
        let hash = "a".repeat(64);
        let path = cas.path(&hash);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(16 * 1024 * 1024 * 1024).unwrap();

        // A cancelled token deliberately distinguishes the metadata gate from
        // entering the read loop. The hostile sparse length must win without a
        // descriptor-sized allocation or any content I/O.
        let cancelled = tokio_util::sync::CancellationToken::new();
        cancelled.cancel();
        let error = cas
            .get_cancelled_bounded(&hash, 1024 * 1024, &cancelled)
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("bounded read limit"),
            "unexpected error: {error:#}"
        );
    }
}
