use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

/// Called with the hash of each object a build installs.
pub type OnStore = Arc<dyn Fn(&str) + Send + Sync>;

/// A minimal filesystem-backed content-addressed store.
///
/// Every object is written once to a temporary file, hashed as it is
/// written (or, for a file another program wrote, read once to hash it), and
/// renamed into place; it is copied only when the temporary file is on
/// another filesystem. An object path therefore always holds the complete
/// bytes of its hash; a crash leaves at most a temporary file behind.
#[derive(Clone)]
pub struct Cas {
    root: PathBuf,
    /// Sync each object's bytes before the rename that installs it, and the
    /// directory entry after it, so an object reported stored survives a
    /// crash. Off for a cache, whose lost entry is only a miss.
    durable: bool,
    on_store: Option<OnStore>,
}

impl Cas {
    /// A store that is the source of truth: each stored object is durable.
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        Self::open(root, true)
    }

    /// A cache in front of a durable store (remote storage, or the client's
    /// download cache). A crash can lose its newest entries, which only costs
    /// a re-read, so it skips the syncs.
    pub fn cache<P: AsRef<Path>>(root: P) -> Result<Self> {
        Self::open(root, false)
    }

    fn open<P: AsRef<Path>>(root: P, durable: bool) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            durable,
            on_store: None,
        })
    }

    /// The same store, calling `on_store` with each object it installs. A
    /// build uses it to start each artifact's upload as soon as it is stored.
    pub fn with_on_store(&self, on_store: OnStore) -> Self {
        Self {
            on_store: Some(on_store),
            ..self.clone()
        }
    }

    /// The same store without an `on_store` callback, for caching objects
    /// that are already in durable storage.
    pub fn detached(&self) -> Self {
        Self {
            on_store: None,
            ..self.clone()
        }
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

    /// Sync a finished object's bytes before it is installed, if durable.
    fn sync_file(&self, file: &std::fs::File) -> Result<()> {
        if self.durable {
            let start = Instant::now();
            file.sync_all().context("fsync CAS object")?;
            crate::perf::record_cas_fsync(start.elapsed());
        }
        Ok(())
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

    /// A new object, hashed as it is written. [`CasWriter::finish`] installs it.
    pub fn writer(&self) -> Result<CasWriter> {
        let tmp = scratch_file()?;
        Ok(CasWriter {
            cas: self.clone(),
            tmp,
            hasher: Sha256::new(),
            len: 0,
            write_start: Instant::now(),
        })
    }

    pub fn put(&self, data: &[u8]) -> Result<String> {
        let mut writer = self.writer()?;
        writer.write_all(data).context("write CAS object")?;
        Ok(writer.finish()?.0)
    }

    /// Store bytes that came from elsewhere under `hash`, after checking
    /// that they match it. An intact object already stored is kept.
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
        self.sync_file(tmp.as_file())?;
        self.install_temp(hash, tmp.into_temp_path())
    }

    /// Copy a file from elsewhere into the store under `hash`, checking it
    /// against `hash` as it is copied. Returns the object's length.
    pub fn put_file_with_hash<P: AsRef<Path>>(&self, hash: &str, source: P) -> Result<u64> {
        let source = source.as_ref();
        let path = self.object_path(hash)?;
        let mut input = std::fs::File::open(source)
            .with_context(|| format!("open CAS source file {}", source.display()))?;
        let mut tmp = scratch_file()?;
        let mut hasher = ObjectHasher::for_object_id(hash)?;
        let mut buf = vec![0u8; 1024 * 1024];
        let mut len = 0u64;
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
            len += n as u64;
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
        self.sync_file(tmp.as_file())?;
        self.create_object_dir(&path)?;
        self.install_temp(hash, tmp.into_temp_path())?;
        Ok(len)
    }

    /// Move a finished file into the store, reading it once to hash it. The
    /// file must not change afterwards; it is renamed into place, or copied
    /// when it is on another filesystem. Returns the hash and length.
    pub fn install_file<P: AsRef<Path>>(&self, source: P) -> Result<(String, u64)> {
        let source = source.as_ref();
        let file = std::fs::File::open(source)
            .with_context(|| format!("open CAS source file {}", source.display()))?;
        let (hash, len) = hash_reader(&file, Sha256::new())
            .with_context(|| format!("hash CAS source file {}", source.display()))?;
        self.sync_file(&file)?;
        drop(file);
        self.install(&hash, source)?;
        Ok((hash, len))
    }

    /// Rename `source`, whose bytes are `hash`, to the object's path. An
    /// object already there is replaced, so a damaged copy left by a crash of
    /// a cache that skips syncs is repaired by the next write.
    fn install(&self, hash: &str, source: &Path) -> Result<()> {
        let path = self.object_path(hash)?;
        self.create_object_dir(&path)?;
        match std::fs::rename(source, &path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
                let parent = path.parent().unwrap_or(&self.root);
                let tmp = tempfile::Builder::new()
                    .prefix(".tmp.")
                    .tempfile_in(parent)
                    .with_context(|| format!("create CAS object tmp {}", hash))?;
                let len = std::fs::copy(source, tmp.path())
                    .with_context(|| format!("copy {} into the CAS", source.display()))?;
                crate::perf::record_cas_write(std::time::Duration::ZERO, len);
                self.sync_file(tmp.as_file())?;
                std::fs::rename(tmp.path(), &path)
                    .with_context(|| format!("rename CAS object {}", hash))?;
                let _ = tmp.into_temp_path().keep();
                let _ = std::fs::remove_file(source);
            }
            // A platform that cannot replace an open file keeps an intact copy.
            Err(_) if self.verify_object(hash).is_ok() => {
                let _ = std::fs::remove_file(source);
            }
            Err(e) => return Err(e).with_context(|| format!("rename CAS object {}", hash)),
        }
        if self.durable
            && let Some(parent) = path.parent()
        {
            sync_dir(parent)?;
        }
        if let Some(on_store) = &self.on_store {
            on_store(hash);
        }
        Ok(())
    }

    fn install_temp(&self, hash: &str, tmp_path: tempfile::TempPath) -> Result<()> {
        self.install(hash, &tmp_path)?;
        // Renamed away: nothing for the guard to remove.
        let _ = tmp_path.keep();
        Ok(())
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

    /// Read an object this process installed during the current build,
    /// without hashing it again.
    pub fn read_fresh(&self, hash: &str) -> Result<Vec<u8>> {
        let path = self.object_path(hash)?;
        let read_start = Instant::now();
        let data = std::fs::read(&path).with_context(|| format!("read CAS object {}", hash))?;
        crate::perf::record_cas_read(read_start.elapsed(), data.len() as u64);
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
        let file =
            std::fs::File::open(&path).with_context(|| format!("open CAS object {}", hash))?;
        let read_start = Instant::now();
        let (actual, len) = hash_reader(&file, ObjectHasher::for_object_id(hash)?)
            .with_context(|| format!("read CAS object {}", hash))?;
        crate::perf::record_cas_read(read_start.elapsed(), len);
        if actual != hash {
            anyhow::bail!(
                "CAS object {} is corrupt: hash mismatch (actual {})",
                hash,
                actual
            );
        }
        Ok(len)
    }

    /// Copy an object into `writer`. With `verify`, its bytes are checked
    /// against `hash` as they are copied; an object this build installed
    /// can skip that.
    pub fn copy_to_writer<W: Write>(
        &self,
        hash: &str,
        writer: &mut W,
        verify: bool,
    ) -> Result<u64> {
        let path = self.object_path(hash)?;
        let mut file =
            std::fs::File::open(&path).with_context(|| format!("open CAS object {}", hash))?;
        let mut hasher = if verify {
            Some(ObjectHasher::for_object_id(hash)?)
        } else {
            None
        };
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
            if let Some(hasher) = &mut hasher {
                hasher.update(&buf[..n]);
            }
            len += n as u64;
        }
        crate::perf::record_cas_read(read_start.elapsed(), len);
        if let Some(hasher) = hasher {
            let actual = hasher.finalize_hex();
            if actual != hash {
                anyhow::bail!(
                    "CAS object {} is corrupt: hash mismatch (actual {})",
                    hash,
                    actual
                );
            }
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

/// A new CAS object being written: a temporary file in the store and the
/// SHA-256 of the bytes written so far.
pub struct CasWriter {
    cas: Cas,
    tmp: tempfile::NamedTempFile,
    hasher: Sha256,
    len: u64,
    write_start: Instant,
}

impl CasWriter {
    /// Bytes written so far.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Install the object. Returns its hash and length.
    pub fn finish(mut self) -> Result<(String, u64)> {
        self.tmp.flush().context("flush CAS temp file")?;
        crate::perf::record_cas_write(self.write_start.elapsed(), self.len);
        self.cas.sync_file(self.tmp.as_file())?;
        let hash = hex::encode(self.hasher.finalize());
        self.cas.install_temp(&hash, self.tmp.into_temp_path())?;
        Ok((hash, self.len))
    }
}

impl Write for CasWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.tmp.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.len += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.tmp.flush()
    }
}

/// A temporary file for a new object, in the process's temp directory. The
/// server points that at `<cache>/tmp`, on the store's filesystem, so the
/// object is installed by a rename, and startup clears what a crash left.
fn scratch_file() -> Result<tempfile::NamedTempFile> {
    tempfile::Builder::new()
        .prefix(".tmp.")
        .tempfile()
        .with_context(|| {
            format!(
                "create CAS temp file in {}",
                tempfile::env::temp_dir().display()
            )
        })
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

/// The SHA-256 and length of a file's bytes.
pub fn hash_file<P: AsRef<Path>>(source: P) -> Result<(String, u64)> {
    let source = source.as_ref();
    let file = std::fs::File::open(source)
        .with_context(|| format!("open file {} for hashing", source.display()))?;
    hash_reader(&file, Sha256::new())
        .with_context(|| format!("read file {} for hashing", source.display()))
}

fn hash_reader(mut input: impl Read, hasher: impl Into<ObjectHasher>) -> Result<(String, u64)> {
    let mut hasher = hasher.into();
    let mut buf = vec![0u8; 1024 * 1024];
    let mut len = 0u64;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        len += n as u64;
    }
    Ok((hasher.finalize_hex(), len))
}

impl From<Sha256> for ObjectHasher {
    fn from(hasher: Sha256) -> Self {
        Self::Sha256(hasher)
    }
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
                "install_file",
                |cas, _hash, source| cas.install_file(source).map(|_| ()),
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
