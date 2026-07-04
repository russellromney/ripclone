use anyhow::{Context, Result};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

pub(crate) fn with_file_lock<T>(path: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let _lock = FileLock::acquire(path)?;
    f()
}

pub(crate) fn write_0600_atomic(path: &Path, data: &[u8]) -> Result<()> {
    write_0600_atomic_inner(path, data, None)
}

fn write_0600_atomic_inner(
    path: &Path,
    data: &[u8],
    before_rename: Option<&dyn Fn(&Path) -> Result<()>>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create parent dir {}", parent.display()))?;
    }
    let (tmp, mut file) = create_tmp_0600(path)?;
    let write_result = (|| -> Result<()> {
        file.write_all(data)
            .with_context(|| format!("write temp file {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("fsync temp file {}", tmp.display()))?;
        drop(file);
        if let Some(hook) = before_rename {
            hook(&tmp)?;
        }
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename {} to {}", tmp.display(), path.display()))?;
        sync_parent(path)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    write_result
}

fn create_tmp_0600(path: &Path) -> Result<(PathBuf, std::fs::File)> {
    for _ in 0..16 {
        let tmp = tmp_path(path);
        match open_tmp_0600(&tmp) {
            Ok(file) => return Ok((tmp, file)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e).with_context(|| format!("create temp file {}", tmp.display())),
        }
    }
    anyhow::bail!(
        "could not create unique temp file next to {} after repeated attempts",
        path.display()
    )
}

fn tmp_path(path: &Path) -> PathBuf {
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("secure-file");
    path.with_file_name(format!(".{file_name}.tmp.{}.{}", std::process::id(), seq))
}

#[cfg(unix)]
fn open_tmp_0600(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_tmp_0600(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        let dir = std::fs::File::open(parent)
            .with_context(|| format!("open dir {}", parent.display()))?;
        dir.sync_all()
            .with_context(|| format!("fsync dir {}", parent.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
struct FileLock {
    _file: std::fs::File,
}

#[cfg(unix)]
impl FileLock {
    fn acquire(path: &Path) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::io::AsRawFd;
        let lock_path = lock_path(path);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create lock dir {}", parent.display()))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .with_context(|| format!("open lock file {}", lock_path.display()))?;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            return Err(anyhow::Error::new(std::io::Error::last_os_error()))
                .with_context(|| format!("flock lock file {}", lock_path.display()));
        }
        Ok(Self { _file: file })
    }
}

#[cfg(not(unix))]
struct FileLock;

#[cfg(not(unix))]
impl FileLock {
    fn acquire(_path: &Path) -> Result<Self> {
        Ok(Self)
    }
}

fn lock_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_os_string();
    p.push(".lock");
    PathBuf::from(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupted_write_leaves_old_file_intact() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write_0600_atomic(&path, b"old").unwrap();

        let err = write_0600_atomic_inner(
            &path,
            b"new",
            Some(&|_| anyhow::bail!("simulated interruption before rename")),
        )
        .unwrap_err();
        assert!(err.to_string().contains("simulated interruption"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "old");
    }

    #[test]
    fn stale_temp_name_is_retried() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        TMP_SEQ.store(1_000_000, Ordering::SeqCst);
        let stale = path.with_file_name(format!(
            ".tokens.json.tmp.{}.{}",
            std::process::id(),
            1_000_000
        ));
        std::fs::write(&stale, b"stale").unwrap();

        write_0600_atomic(&path, b"new").unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(std::fs::read_to_string(&stale).unwrap(), "stale");
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_creates_owner_only_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        write_0600_atomic(&path, b"server = \"https://example.com\"\n").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
