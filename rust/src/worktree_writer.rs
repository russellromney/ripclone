use crate::fsutil::{path_from_bytes, safe_create_dir_all, validate_relative_path};
use crate::manifest::FileEntry;
use anyhow::{Context, Result};
use filetime::{FileTime, set_file_mtime, set_symlink_file_times};
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

pub(crate) const INDEX_MTIME: FileTime = FileTime::from_unix_time(1, 0);

// Process-wide write-phase timing counters. These let the `--bench` report
// split the worktree-write cost into three buckets:
//
//   * prep  — path validation, parent-dir creation, symlink/exists probes, and
//             the per-file classification done before any bytes are written.
//   * io    — the open/write/close syscalls, and nothing else.
//   * mtime — the serial `utimensat` loop that stamps `INDEX_MTIME` on every
//             regular file *after* the batch has been written.
//
// All three are accumulated across every writer thread, so a caller reads them
// once after extraction with `take_write_timing`. The recording cost is a
// handful of atomic adds per batch (never per file), so it is always on.
static PREP_NS: AtomicU64 = AtomicU64::new(0);
static IO_NS: AtomicU64 = AtomicU64::new(0);
static MTIME_NS: AtomicU64 = AtomicU64::new(0);
static IO_FILES: AtomicU64 = AtomicU64::new(0);
static IO_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Default, Debug, Clone, Copy)]
pub struct WriteTiming {
    pub prep_ns: u64,
    pub io_ns: u64,
    pub mtime_ns: u64,
    pub files: u64,
    pub bytes: u64,
}

/// Read and reset the process-wide write-phase timing counters.
pub fn take_write_timing() -> WriteTiming {
    WriteTiming {
        prep_ns: PREP_NS.swap(0, Ordering::Relaxed),
        io_ns: IO_NS.swap(0, Ordering::Relaxed),
        mtime_ns: MTIME_NS.swap(0, Ordering::Relaxed),
        files: IO_FILES.swap(0, Ordering::Relaxed),
        bytes: IO_BYTES.swap(0, Ordering::Relaxed),
    }
}

fn record_prep(d: Duration) {
    PREP_NS.fetch_add(
        u64::try_from(d.as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

fn record_io(d: Duration, files: u64, bytes: u64) {
    IO_NS.fetch_add(
        u64::try_from(d.as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    IO_FILES.fetch_add(files, Ordering::Relaxed);
    IO_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

fn record_mtime(d: Duration) {
    MTIME_NS.fetch_add(
        u64::try_from(d.as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

#[derive(Clone, Copy, Debug)]
pub struct WriteOptions {
    /// Skip per-file parent directory creation/checks.
    ///
    /// Use this only when the caller has already validated every path and
    /// created all parent directories with `safe_create_dir_all` in the same
    /// extraction root.
    pub parents_prepared: bool,
    /// Stamp regular files with `INDEX_MTIME` after writing.
    ///
    /// Archive extraction can disable this when it refreshes the git index stat
    /// cache after materialization. Other callers keep the conservative default.
    pub stamp_mtime: bool,
    /// Skip final-component existence/symlink probes for a freshly-created
    /// install root whose parent directories were already validated.
    pub fresh_target: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            parents_prepared: false,
            stamp_mtime: true,
            fresh_target: false,
        }
    }
}

/// Options for a fresh indexed checkout: parents already prepared, a fresh
/// install root, and stat entries collected instead of stamping mtimes.
pub const FRESH_INDEXED_CHECKOUT: WriteOptions = WriteOptions {
    parents_prepared: true,
    stamp_mtime: false,
    fresh_target: true,
};

pub struct OwnedFileWrite {
    pub entry: FileEntry,
    pub content: FileWriteContent,
}

#[derive(Clone)]
pub struct FileSlice {
    pub data: Arc<Vec<u8>>,
    pub offset: usize,
    pub len: usize,
}

pub enum FileWriteContent {
    Owned(Vec<u8>),
    Shared {
        data: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    },
    Fragments(Vec<FileSlice>),
}

pub struct WriteOutcome {
    pub written: usize,
    pub stats: Vec<crate::git::MaterializedPathStat>,
}

impl FileWriteContent {
    pub fn shared(data: Arc<Vec<u8>>, offset: usize, len: usize) -> Self {
        Self::Shared { data, offset, len }
    }

    fn bytes_cow(&self) -> Cow<'_, [u8]> {
        match self {
            Self::Owned(content) => Cow::Borrowed(content.as_slice()),
            Self::Shared { data, offset, len } => Cow::Borrowed(&data[*offset..*offset + *len]),
            Self::Fragments(fragments) => {
                let mut bytes = Vec::with_capacity(fragments.iter().map(|f| f.len).sum());
                for fragment in fragments {
                    bytes.extend_from_slice(
                        &fragment.data[fragment.offset..fragment.offset + fragment.len],
                    );
                }
                Cow::Owned(bytes)
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Owned(content) => content.len(),
            Self::Shared { len, .. } => *len,
            Self::Fragments(fragments) => fragments.iter().map(|f| f.len).sum(),
        }
    }

    fn write_all_to(&self, file: &mut std::fs::File) -> Result<()> {
        use std::io::Write;
        match self {
            Self::Owned(content) => file.write_all(content).context("write file content"),
            Self::Shared { data, offset, len } => file
                .write_all(&data[*offset..*offset + *len])
                .context("write shared file content"),
            Self::Fragments(fragments) => {
                for fragment in fragments {
                    file.write_all(&fragment.data[fragment.offset..fragment.offset + fragment.len])
                        .context("write fragmented file content")?;
                }
                Ok(())
            }
        }
    }
}

impl From<Vec<u8>> for FileWriteContent {
    fn from(value: Vec<u8>) -> Self {
        Self::Owned(value)
    }
}

struct PreparedRegularWrite {
    target: PathBuf,
    index_path: Vec<u8>,
    mode: u32,
    content: FileWriteContent,
}

/// Write `writes` under `target_dir`. Symlinks are written during preparation;
/// regular files are written next, then stamped with `INDEX_MTIME` when
/// `options.stamp_mtime` is set. When it is not set, the outcome carries one
/// index stat entry per regular file instead.
pub fn write_owned_entries(
    target_dir: &Path,
    writes: Vec<OwnedFileWrite>,
    options: WriteOptions,
) -> Result<WriteOutcome> {
    if writes.is_empty() {
        return Ok(WriteOutcome {
            written: 0,
            stats: Vec::new(),
        });
    }

    let prep_start = Instant::now();
    let (written, regulars) = prepare_owned_entries(target_dir, writes, options)?;
    record_prep(prep_start.elapsed());
    let stats = write_regular_batch(&regulars, options)?;
    Ok(WriteOutcome { written, stats })
}

fn write_regular_batch(
    writes: &[PreparedRegularWrite],
    options: WriteOptions,
) -> Result<Vec<crate::git::MaterializedPathStat>> {
    if writes.is_empty() {
        return Ok(Vec::new());
    }
    let files = writes.len() as u64;
    let bytes: u64 = writes.iter().map(|w| w.content.len() as u64).sum();

    // Write every file first, then stamp mtimes, so the two phases are
    // measured separately.
    let io_start = Instant::now();
    let mut stats = Vec::new();
    for write in writes {
        let metadata = write_regular_content(&write.target, write.mode, &write.content)?;
        if !options.stamp_mtime {
            stats.push(crate::git::materialized_path_stat_from_metadata(
                write.index_path.clone(),
                &metadata,
            ));
        }
    }
    record_io(io_start.elapsed(), files, bytes);

    if options.stamp_mtime {
        let mtime_start = Instant::now();
        for write in writes {
            set_file_mtime(&write.target, INDEX_MTIME)
                .with_context(|| format!("set mtime {}", write.target.display()))?;
        }
        record_mtime(mtime_start.elapsed());
    }
    Ok(stats)
}

fn prepare_owned_entries(
    target_dir: &Path,
    writes: Vec<OwnedFileWrite>,
    options: WriteOptions,
) -> Result<(usize, Vec<PreparedRegularWrite>)> {
    let mut regulars = Vec::new();
    let mut written = 0usize;
    for write in writes {
        let path = path_from_bytes(&write.entry.path);
        validate_relative_path(path)
            .with_context(|| format!("refusing to extract unsafe path: {}", path.display()))?;

        if !options.parents_prepared
            && let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            safe_create_dir_all(target_dir, parent).with_context(|| {
                format!(
                    "create parent dir for {}",
                    String::from_utf8_lossy(&write.entry.path)
                )
            })?;
        }

        let target = target_dir.join(path);
        if !options.fresh_target && target.is_symlink() {
            std::fs::remove_file(&target)
                .with_context(|| format!("remove existing symlink {}", target.display()))?;
        }

        match write.entry.mode {
            0o120000 => {
                let content = write.content.bytes_cow();
                write_symlink_entry(&target, content.as_ref())?;
                written += 1;
            }
            0o100755 | 0o100644 => {
                if !options.fresh_target && target.exists() {
                    std::fs::remove_file(&target).ok();
                }
                let mode = if write.entry.mode == 0o100755 {
                    0o755
                } else {
                    0o644
                };
                regulars.push(PreparedRegularWrite {
                    target,
                    index_path: write.entry.path,
                    mode,
                    content: write.content,
                });
                written += 1;
            }
            _ => {
                anyhow::bail!(
                    "refusing to extract file {} with illegal mode 0o{:o}",
                    String::from_utf8_lossy(&write.entry.path),
                    write.entry.mode
                );
            }
        }
    }
    Ok((written, regulars))
}

fn write_regular_content(
    target: &Path,
    mode: u32,
    content: &FileWriteContent,
) -> Result<std::fs::Metadata> {
    if target.exists() {
        std::fs::remove_file(target).ok();
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut opts = std::fs::OpenOptions::new();
        opts.write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .custom_flags(libc::O_NOFOLLOW);
        let mut file = opts
            .open(target)
            .with_context(|| format!("open {}", target.display()))?;
        content.write_all_to(&mut file)?;
        file.metadata()
            .with_context(|| format!("stat {}", target.display()))
    }
    #[cfg(not(unix))]
    {
        let mut file =
            std::fs::File::create(target).with_context(|| format!("write {}", target.display()))?;
        content.write_all_to(&mut file)?;
        std::fs::metadata(target).with_context(|| format!("stat {}", target.display()))
    }
}

fn write_symlink_entry(target: &Path, content: &[u8]) -> Result<()> {
    // Always unlink first; `exists()` follows symlinks and would miss a broken
    // symlink left over from a previous extraction.
    if target.exists() || target.is_symlink() {
        std::fs::remove_file(target).ok();
    }
    #[cfg(unix)]
    {
        // A symlink target is an arbitrary byte string (git stores the raw blob);
        // it need not be valid UTF-8. Build the target straight from the bytes via
        // `OsStr` so a non-UTF-8 target clones byte-for-byte instead of aborting
        // the whole clone (F1).
        use std::os::unix::ffi::OsStrExt;
        let link_target = std::ffi::OsStr::from_bytes(content);
        std::os::unix::fs::symlink(link_target, target)
            .with_context(|| format!("symlink {}", target.display()))?;
        set_symlink_file_times(target, INDEX_MTIME, INDEX_MTIME)
            .with_context(|| format!("set symlink times {}", target.display()))?;
    }
    #[cfg(not(unix))]
    {
        // No raw-byte symlinks off unix; keep the best-effort lossy fallback.
        std::fs::write(target, String::from_utf8_lossy(content).as_bytes())
            .with_context(|| format!("write symlink fallback {}", target.display()))?;
        set_file_mtime(target, INDEX_MTIME)
            .with_context(|| format!("set mtime {}", target.display()))?;
    }
    Ok(())
}

/// Durability barrier for an opt-in (`RIPCLONE_FSYNC`) clone. fsyncs every file
/// in `files` and every directory in `dirs` so a crash right after the clone
/// cannot leave a torn tree that `git status` would call clean. The caller is
/// expected to pass the whole materialized set — every written file (including
/// the freshly written `.git/index` stat cache), every directory that holds one,
/// and, before the atomic publish, the staging root — with `dirs` already
/// deduplicated.
///
/// Each file and directory is opened read-only and flushed with `fsync`. Off
/// by default (see `RIPCLONE_FSYNC` in `docs/CONFIG.md`); enabling it trades
/// clone latency for durability.
pub fn fsync_paths_durable(files: &[PathBuf], dirs: &[PathBuf]) -> Result<()> {
    for path in files {
        let f = std::fs::File::open(path)
            .with_context(|| format!("open file for fsync {}", path.display()))?;
        f.sync_all()
            .with_context(|| format!("fsync file {}", path.display()))?;
    }
    for path in dirs {
        fsync_one_dir(path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn fsync_one_dir(path: &Path) -> Result<()> {
    let dir = std::fs::File::open(path)
        .with_context(|| format!("open dir for fsync {}", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("fsync dir {}", path.display()))
}

#[cfg(not(unix))]
fn fsync_one_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regular_write(path: &str, mode: u32, content: &[u8]) -> OwnedFileWrite {
        OwnedFileWrite {
            entry: FileEntry {
                path: path.as_bytes().to_vec(),
                mode,
                blob_sha1: Vec::new(),
                fragments: Vec::new(),
            },
            content: content.to_vec().into(),
        }
    }

    #[test]
    fn writer_writes_regular_file_modes_and_content() {
        let dir = tempfile::TempDir::new().unwrap();
        let files = [
            ("bin/tool", 0o100755, &b"#!/bin/sh\n"[..]),
            ("docs/readme.txt", 0o100644, b"hello\n"),
        ];
        let writes = files
            .iter()
            .map(|(path, mode, content)| regular_write(path, *mode, content))
            .collect();
        write_owned_entries(dir.path(), writes, WriteOptions::default()).unwrap();

        for (path, mode, content) in files {
            let path = dir.path().join(path);
            assert_eq!(std::fs::read(&path).unwrap(), content);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                    mode & 0o777,
                );
            }
        }
    }

    #[test]
    fn writer_rejects_parent_dir_paths() {
        let dir = tempfile::TempDir::new().unwrap();
        let write = regular_write("../escape", 0o100644, b"nope");
        assert!(write_owned_entries(dir.path(), vec![write], WriteOptions::default()).is_err());
    }
}
