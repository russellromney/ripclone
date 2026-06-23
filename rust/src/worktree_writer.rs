use crate::manifest::FileEntry;
use anyhow::{Context, Result};
use filetime::{FileTime, set_file_mtime, set_symlink_file_times};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

pub(crate) const INDEX_MTIME: FileTime = FileTime::from_unix_time(1, 0);
#[cfg(target_os = "linux")]
const IO_URING_MIN_BATCH_FILES: usize = 2;

// Process-wide write-phase timing counters. These let a caller (notably the
// `writer_bench` binary) split the worktree-write cost into three buckets that
// the per-clone benchmark conflates inside `write_ms`:
//
//   * prep  — path validation, parent-dir creation, symlink/exists probes, and
//             the per-file classification done before any bytes are written.
//   * io    — the actual open/write/close work (POSIX syscalls or the io_uring
//             submission), and nothing else.
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
    PREP_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
}

fn record_io(d: Duration, files: u64, bytes: u64) {
    IO_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
    IO_FILES.fetch_add(files, Ordering::Relaxed);
    IO_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

fn record_mtime(d: Duration) {
    MTIME_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
}

/// Convert a raw path byte slice to a `Path`. On Unix this preserves arbitrary
/// git path bytes; on other platforms we fall back to UTF-8.
pub fn path_from_bytes(bytes: &[u8]) -> &Path {
    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        Path::new(OsStr::from_bytes(bytes))
    }
    #[cfg(not(unix))]
    {
        let s = std::str::from_utf8(bytes).unwrap_or("<invalid utf8 path>");
        Path::new(s)
    }
}

/// Validate that `path` is a non-empty relative path with no `..` components
/// and no NUL bytes. This must be applied to every manifest path before any
/// filesystem operation.
pub fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        anyhow::bail!("path is empty");
    }
    if path.is_absolute() {
        anyhow::bail!("path is absolute: {}", path.display());
    }
    for comp in path.components() {
        match comp {
            std::path::Component::ParentDir => {
                anyhow::bail!("path contains parent-dir component: {}", path.display());
            }
            std::path::Component::Normal(_) => {}
            _ => {
                anyhow::bail!("path contains invalid component: {}", path.display());
            }
        }
    }
    if path.as_os_str().as_encoded_bytes().contains(&0) {
        anyhow::bail!("path contains NUL byte: {}", path.display());
    }
    Ok(())
}

/// Create a directory tree under `root` following only real directory
/// components. Any symlink encountered along the way is rejected.
pub fn safe_create_dir_all(root: &Path, rel: &Path) -> Result<()> {
    validate_relative_path(rel)?;
    let mut current = root.to_path_buf();
    for comp in rel.components() {
        if let std::path::Component::Normal(name) = comp {
            current.push(name);
            if current.is_symlink() {
                anyhow::bail!(
                    "refusing to follow symlinked directory: {}",
                    current.display()
                );
            }
            // Create unconditionally and tolerate a concurrent creator. Probing
            // with `exists()` first would race when several producer threads
            // create the same parent directory at once (one would win, the
            // others would hit EEXIST). `create_dir` + `AlreadyExists` is the
            // atomic form.
            match std::fs::create_dir(&current) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if current.is_symlink() {
                        anyhow::bail!(
                            "refusing to follow symlinked directory: {}",
                            current.display()
                        );
                    }
                    if !current.is_dir() {
                        anyhow::bail!("path is not a directory: {}", current.display());
                    }
                }
                Err(e) => {
                    return Err(e).with_context(|| format!("create dir {}", current.display()));
                }
            }
        }
    }
    Ok(())
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

#[derive(Clone)]
pub struct WorktreeWriter {
    backend: WriterBackend,
}

pub struct OwnedFileWrite {
    pub entry: FileEntry,
    pub content: FileWriteContent,
}

pub enum FileWriteContent {
    Owned(Vec<u8>),
    Shared {
        data: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    },
}

pub struct WriteOutcome {
    pub written: usize,
    pub stats: Vec<crate::git::MaterializedPathStat>,
}

impl FileWriteContent {
    pub fn shared(data: Arc<Vec<u8>>, offset: usize, len: usize) -> Self {
        Self::Shared { data, offset, len }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(content) => content.as_slice(),
            Self::Shared { data, offset, len } => &data[*offset..*offset + *len],
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Owned(content) => content.len(),
            Self::Shared { len, .. } => *len,
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<Vec<u8>> for FileWriteContent {
    fn from(value: Vec<u8>) -> Self {
        Self::Owned(value)
    }
}

struct PreparedRegularWrite {
    target: PathBuf,
    index_path: String,
    mode: u32,
    content: FileWriteContent,
}

#[derive(Clone)]
enum WriterBackend {
    Posix,
    #[cfg(target_os = "linux")]
    IoUring(linux_uring::UringWriter),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoUringMode {
    Auto,
    Force,
    Disabled,
}

impl IoUringMode {
    fn from_env() -> Self {
        match std::env::var("RIPCLONE_IO_URING") {
            Ok(v) => {
                let v = v.trim();
                if v.is_empty()
                    || v == "0"
                    || v.eq_ignore_ascii_case("false")
                    || v.eq_ignore_ascii_case("off")
                    || v.eq_ignore_ascii_case("no")
                {
                    IoUringMode::Disabled
                } else if v.eq_ignore_ascii_case("auto") {
                    IoUringMode::Auto
                } else if v == "1"
                    || v.eq_ignore_ascii_case("true")
                    || v.eq_ignore_ascii_case("on")
                    || v.eq_ignore_ascii_case("yes")
                {
                    IoUringMode::Force
                } else {
                    IoUringMode::Auto
                }
            }
            // Unset: default to trying io_uring on Linux (auto falls back to
            // POSIX if the kernel lacks support), POSIX on other platforms.
            Err(_) => {
                if cfg!(target_os = "linux") {
                    IoUringMode::Auto
                } else {
                    IoUringMode::Disabled
                }
            }
        }
    }
}

impl WorktreeWriter {
    pub fn new() -> Result<Self> {
        match IoUringMode::from_env() {
            IoUringMode::Disabled => Ok(Self::posix()),
            IoUringMode::Auto => Self::auto(),
            IoUringMode::Force => Self::io_uring(),
        }
    }

    pub fn posix() -> Self {
        Self {
            backend: WriterBackend::Posix,
        }
    }

    pub fn is_io_uring(&self) -> bool {
        match &self.backend {
            WriterBackend::Posix => false,
            #[cfg(target_os = "linux")]
            WriterBackend::IoUring(_) => true,
        }
    }

    fn auto() -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            match Self::io_uring() {
                Ok(writer) => Ok(writer),
                Err(e) => {
                    tracing::warn!(
                        "io_uring writer unavailable; falling back to POSIX writer: {e:#}"
                    );
                    Ok(Self::posix())
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(Self::posix())
        }
    }

    pub fn io_uring() -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let writer = linux_uring::UringWriter::new().context("start io_uring writer")?;
            writer.probe().context("probe io_uring writer")?;
            tracing::info!("using io_uring worktree writer");
            Ok(Self {
                backend: WriterBackend::IoUring(writer),
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            anyhow::bail!("RIPCLONE_IO_URING requested, but io_uring is Linux-only");
        }
    }

    pub fn write_entry(&self, target_dir: &Path, entry: &FileEntry, content: &[u8]) -> Result<()> {
        self.write_entry_inner(target_dir, entry, RegularContent::Borrowed(content))
    }

    pub fn write_owned_entry(
        &self,
        target_dir: &Path,
        entry: &FileEntry,
        content: Vec<u8>,
    ) -> Result<()> {
        self.write_entry_inner(target_dir, entry, RegularContent::Owned(content))
    }

    pub fn write_owned_entries(
        &self,
        target_dir: &Path,
        writes: Vec<OwnedFileWrite>,
    ) -> Result<usize> {
        self.write_owned_entries_with_options(target_dir, writes, WriteOptions::default())
            .map(|outcome| outcome.written)
    }

    pub fn write_owned_entries_in_prepared_dirs(
        &self,
        target_dir: &Path,
        writes: Vec<OwnedFileWrite>,
    ) -> Result<usize> {
        self.write_owned_entries_with_options(
            target_dir,
            writes,
            WriteOptions {
                parents_prepared: true,
                ..WriteOptions::default()
            },
        )
        .map(|outcome| outcome.written)
    }

    pub fn write_owned_entries_for_archive(
        &self,
        target_dir: &Path,
        writes: Vec<OwnedFileWrite>,
    ) -> Result<usize> {
        self.write_owned_entries_with_options(
            target_dir,
            writes,
            WriteOptions {
                parents_prepared: true,
                stamp_mtime: false,
                fresh_target: true,
            },
        )
        .map(|outcome| outcome.written)
    }

    pub fn write_owned_entries_for_fresh_checkout(
        &self,
        target_dir: &Path,
        writes: Vec<OwnedFileWrite>,
    ) -> Result<usize> {
        self.write_owned_entries_with_options(
            target_dir,
            writes,
            WriteOptions {
                parents_prepared: true,
                fresh_target: true,
                ..WriteOptions::default()
            },
        )
        .map(|outcome| outcome.written)
    }

    pub fn write_owned_entries_for_fresh_indexed_checkout(
        &self,
        target_dir: &Path,
        writes: Vec<OwnedFileWrite>,
    ) -> Result<WriteOutcome> {
        self.write_owned_entries_with_options(
            target_dir,
            writes,
            WriteOptions {
                parents_prepared: true,
                stamp_mtime: false,
                fresh_target: true,
            },
        )
    }

    pub fn write_owned_entries_for_fresh_indexed_checkout_deferred(
        &self,
        target_dir: &Path,
        writes: Vec<OwnedFileWrite>,
    ) -> Result<WriteOutcome> {
        let options = WriteOptions {
            parents_prepared: true,
            stamp_mtime: false,
            fresh_target: true,
        };
        if writes.is_empty() {
            return self.flush_deferred_writes();
        }

        let prep_start = Instant::now();
        let (written, regulars) = prepare_owned_entries(target_dir, writes, options)?;
        let immediate_written = written - regulars.len();
        record_prep(prep_start.elapsed());

        let mut outcome = self.write_regular_batch_deferred(regulars, options)?;
        outcome.written += immediate_written;
        Ok(outcome)
    }

    pub fn flush_deferred_writes(&self) -> Result<WriteOutcome> {
        match &self.backend {
            WriterBackend::Posix => Ok(WriteOutcome {
                written: 0,
                stats: Vec::new(),
            }),
            #[cfg(target_os = "linux")]
            WriterBackend::IoUring(writer) => writer.flush_deferred_writes(),
        }
    }

    pub fn write_owned_entries_with_options(
        &self,
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
        let stats = self.write_regular_batch(regulars, options)?;
        Ok(WriteOutcome { written, stats })
    }

    fn write_entry_inner(
        &self,
        target_dir: &Path,
        entry: &FileEntry,
        content: RegularContent<'_>,
    ) -> Result<()> {
        let path = path_from_bytes(&entry.path);
        validate_relative_path(path)
            .with_context(|| format!("refusing to extract unsafe path: {}", path.display()))?;

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            safe_create_dir_all(target_dir, parent).with_context(|| {
                format!(
                    "create parent dir for {}",
                    String::from_utf8_lossy(&entry.path)
                )
            })?;
        }

        let target = target_dir.join(path);

        // Refuse to operate through an existing symlink at the final component.
        // POSIX open would otherwise follow it and write outside the target dir.
        if target.is_symlink() {
            std::fs::remove_file(&target)
                .with_context(|| format!("remove existing symlink {}", target.display()))?;
        }

        match entry.mode {
            0o120000 => write_symlink_entry(&target, &entry.path, content.as_slice()),
            0o100755 | 0o100644 => {
                let mode = if entry.mode == 0o100755 { 0o755 } else { 0o644 };
                self.write_regular(&target, mode, content)?;
                set_file_mtime(&target, INDEX_MTIME)
                    .with_context(|| format!("set mtime {}", target.display()))?;
                Ok(())
            }
            _ => {
                anyhow::bail!(
                    "refusing to extract file {} with illegal mode 0o{:o}",
                    String::from_utf8_lossy(&entry.path),
                    entry.mode
                );
            }
        }
    }

    fn write_regular(&self, target: &Path, mode: u32, content: RegularContent<'_>) -> Result<()> {
        match &self.backend {
            WriterBackend::Posix => {
                write_regular_posix(target, mode, content.as_slice()).map(|_| ())
            }
            #[cfg(target_os = "linux")]
            WriterBackend::IoUring(writer) => match content {
                RegularContent::Borrowed(content) => writer.write_regular(target, mode, content),
                RegularContent::Owned(content) => writer.write_regular_owned(target, mode, content),
            },
        }
    }

    fn write_regular_batch(
        &self,
        writes: Vec<PreparedRegularWrite>,
        options: WriteOptions,
    ) -> Result<Vec<crate::git::MaterializedPathStat>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let files = writes.len() as u64;
        let bytes: u64 = writes.iter().map(|w| w.content.len() as u64).sum();
        let collect_stats = !options.stamp_mtime;
        match &self.backend {
            WriterBackend::Posix => {
                // Write every file first, then stamp mtimes, so the two phases
                // are measured separately and the shape matches the io_uring
                // backend (batched writes followed by a serial utimensat loop).
                let io_start = Instant::now();
                let stats = write_regular_batch_posix(&writes, collect_stats)?;
                record_io(io_start.elapsed(), files, bytes);

                if options.stamp_mtime {
                    let mtime_start = Instant::now();
                    for write in &writes {
                        set_file_mtime(&write.target, INDEX_MTIME)
                            .with_context(|| format!("set mtime {}", write.target.display()))?;
                    }
                    record_mtime(mtime_start.elapsed());
                }
                Ok(stats)
            }
            #[cfg(target_os = "linux")]
            WriterBackend::IoUring(writer) => {
                let targets: Vec<_> = writes.iter().map(|write| write.target.clone()).collect();
                let io_start = Instant::now();
                let stats = if should_use_posix_for_io_uring_batch(&writes) {
                    write_regular_batch_posix(&writes, collect_stats)?
                } else {
                    writer.write_regular_batch(writes, collect_stats)?
                };
                record_io(io_start.elapsed(), files, bytes);

                if options.stamp_mtime {
                    let mtime_start = Instant::now();
                    for target in targets {
                        set_file_mtime(&target, INDEX_MTIME)
                            .with_context(|| format!("set mtime {}", target.display()))?;
                    }
                    record_mtime(mtime_start.elapsed());
                }
                Ok(stats)
            }
        }
    }

    fn write_regular_batch_deferred(
        &self,
        writes: Vec<PreparedRegularWrite>,
        options: WriteOptions,
    ) -> Result<WriteOutcome> {
        if writes.is_empty() {
            return self.flush_deferred_writes();
        }
        let files = writes.len() as u64;
        let bytes: u64 = writes.iter().map(|w| w.content.len() as u64).sum();
        let collect_stats = !options.stamp_mtime;
        match &self.backend {
            WriterBackend::Posix => {
                let io_start = Instant::now();
                let stats = write_regular_batch_posix(&writes, collect_stats)?;
                record_io(io_start.elapsed(), files, bytes);
                Ok(WriteOutcome {
                    written: writes.len(),
                    stats,
                })
            }
            #[cfg(target_os = "linux")]
            WriterBackend::IoUring(writer) => {
                if should_use_posix_for_io_uring_batch(&writes) {
                    let mut completed = writer.flush_deferred_writes()?;
                    let io_start = Instant::now();
                    let stats = write_regular_batch_posix(&writes, collect_stats)?;
                    record_io(io_start.elapsed(), files, bytes);
                    completed.written += writes.len();
                    completed.stats.extend(stats);
                    Ok(completed)
                } else {
                    writer.write_regular_batch_deferred(writes, collect_stats)
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn should_use_posix_for_io_uring_batch(writes: &[PreparedRegularWrite]) -> bool {
    writes.len() < IO_URING_MIN_BATCH_FILES
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
                write_symlink_entry(&target, &write.entry.path, write.content.as_slice())?;
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
                    index_path: String::from_utf8_lossy(&write.entry.path).into_owned(),
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

enum RegularContent<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> RegularContent<'a> {
    fn as_slice(&self) -> &[u8] {
        match self {
            RegularContent::Borrowed(content) => content,
            RegularContent::Owned(content) => content.as_slice(),
        }
    }
}

fn write_regular_batch_posix(
    writes: &[PreparedRegularWrite],
    collect_stats: bool,
) -> Result<Vec<crate::git::MaterializedPathStat>> {
    let mut stats = Vec::new();
    for write in writes {
        let metadata = write_regular_posix(&write.target, write.mode, write.content.as_slice())?;
        if collect_stats {
            stats.push(crate::git::materialized_path_stat_from_metadata(
                write.index_path.clone(),
                &metadata,
            ));
        }
    }
    Ok(stats)
}

fn write_symlink_entry(target: &Path, path_bytes: &[u8], content: &[u8]) -> Result<()> {
    let link_target = std::str::from_utf8(content).with_context(|| {
        format!(
            "non-utf8 symlink target for {}",
            String::from_utf8_lossy(path_bytes)
        )
    })?;
    // Always unlink first; `exists()` follows symlinks and would miss a broken
    // symlink left over from a previous extraction.
    if target.exists() || target.is_symlink() {
        std::fs::remove_file(target).ok();
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(link_target, target)
            .with_context(|| format!("symlink {}", target.display()))?;
        set_symlink_file_times(target, INDEX_MTIME, INDEX_MTIME)
            .with_context(|| format!("set symlink times {}", target.display()))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(target, link_target.as_bytes())
            .with_context(|| format!("write symlink fallback {}", target.display()))?;
        set_file_mtime(target, INDEX_MTIME)
            .with_context(|| format!("set mtime {}", target.display()))?;
    }
    Ok(())
}

fn write_regular_posix(target: &Path, mode: u32, content: &[u8]) -> Result<std::fs::Metadata> {
    if target.exists() {
        std::fs::remove_file(target).ok();
    }

    #[cfg(unix)]
    {
        use std::io::Write;
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
        file.write_all(content)
            .with_context(|| format!("write {}", target.display()))?;
        file.metadata()
            .with_context(|| format!("stat {}", target.display()))
    }
    #[cfg(not(unix))]
    {
        std::fs::write(target, content).with_context(|| format!("write {}", target.display()))?;
        std::fs::metadata(target).with_context(|| format!("stat {}", target.display()))
    }
}

#[cfg(target_os = "linux")]
mod linux_uring {
    use super::*;
    use io_uring::{IoUring, opcode, squeue, types};
    use std::cell::RefCell;
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;
    use std::sync::Once;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const QUEUE_DEPTH: u32 = 4096;
    const MAX_BATCH_FILES: usize = 512;
    /// Largest per-ring overlap depth a submitter can request. Each in-flight
    /// window owns its own fixed-file slot range, so the registered slot
    /// table holds this many `MAX_BATCH_FILES`-sized ranges.
    const MAX_INFLIGHT_WINDOWS: usize = 4;
    const REGISTERED_FILE_SLOTS: usize = MAX_BATCH_FILES * MAX_INFLIGHT_WINDOWS;
    /// Completion-queue capacity. A window submits up to four ops per file
    /// (open/write/statx/close); with `MAX_INFLIGHT_WINDOWS` windows un-harvested
    /// the CQ must hold all of their completions without overflowing.
    const CQ_ENTRIES: u32 = (MAX_INFLIGHT_WINDOWS * MAX_BATCH_FILES * 4) as u32;
    static DIRECT_DESCRIPTOR_ENABLED_LOG: Once = Once::new();
    static DIRECT_DESCRIPTOR_FALLBACK_LOG: Once = Once::new();
    static SKIP_OPEN_CQE_ENABLED_LOG: Once = Once::new();
    static SKIP_WRITE_CQE_ENABLED_LOG: Once = Once::new();
    static OPTIMIZED_RING_ENABLED_LOG: Once = Once::new();
    static OPTIMIZED_RING_FALLBACK_LOG: Once = Once::new();
    static SQPOLL_RING_ENABLED_LOG: Once = Once::new();
    static SQPOLL_RING_FALLBACK_LOG: Once = Once::new();

    /// Set once a per-thread io_uring ring fails to initialize (e.g. ENOMEM or
    /// the locked-memory rlimit under heavy parallelism). Once set, every writer
    /// uses the POSIX path instead of hard-failing the clone. Process-wide: a
    /// memlock shortage affects every thread, and this also avoids a thundering
    /// herd of repeated ring-creation attempts that would each fail.
    static IO_URING_RUNTIME_DISABLED: AtomicBool = AtomicBool::new(false);
    static IO_URING_DISABLED_LOG: Once = Once::new();

    #[cfg(test)]
    pub(super) fn set_runtime_disabled_for_test(disabled: bool) {
        IO_URING_RUNTIME_DISABLED.store(disabled, Ordering::Relaxed);
    }

    #[derive(Clone, Copy)]
    pub(super) struct UringWriter;

    struct RawUringWriter {
        ring: IoUring,
        descriptor_mode: DescriptorMode,
        skip_open_success_cqe: bool,
        /// Windows submitted but not yet harvested, oldest at the front. Bounded
        /// by `max_inflight`; the front is harvested before exceeding it.
        pending_windows: std::collections::VecDeque<PendingDirectWindow>,
        /// Overlap depth: how many windows may be in flight before we block to
        /// harvest the oldest.
        max_inflight: usize,
        /// Next fixed-file slot range to use, in units of `MAX_BATCH_FILES`.
        slot_cursor: usize,
        next_window_id: u32,
        direct_window_verified: bool,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum DescriptorMode {
        NormalFd,
        DirectFd,
    }

    struct InFlightWrite {
        target: PathBuf,
        index_path: String,
        path: CString,
        flags: i32,
        mode: libc::mode_t,
        content: FileWriteContent,
        fd: Option<i32>,
        open_res: Option<i32>,
        write_res: Option<i32>,
        write_success_cqe_skipped: bool,
        statx_res: Option<i32>,
        close_res: Option<i32>,
    }

    struct PendingDirectWindow {
        window_id: u32,
        slot_base: usize,
        in_flight: Vec<InFlightWrite>,
        statx_buffers: Vec<libc::statx>,
        collect_stats: bool,
        expected_completions: usize,
        expected_closes: usize,
        completions_seen: usize,
        closes_seen: usize,
        submitted_ns: u64,
        record_io_on_harvest: bool,
        files: u64,
        bytes: u64,
    }

    impl PendingDirectWindow {
        fn is_complete(&self) -> bool {
            self.closes_seen >= self.expected_closes
                && self.completions_seen >= self.expected_completions
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FileOp {
        Open = 0,
        Write = 1,
        Close = 2,
        Statx = 3,
    }

    thread_local! {
        static THREAD_WRITER: RefCell<Option<RawUringWriter>> = const { RefCell::new(None) };
        // Desired per-ring overlap depth for the *next* ring created on this
        // thread. The scheduler sets this before its submitter writes anything;
        // the default (2) preserves the established double-buffered behavior for
        // every other caller.
        static DESIRED_INFLIGHT: std::cell::Cell<usize> = const { std::cell::Cell::new(2) };
    }

    /// Set the overlap depth for the io_uring ring on the current thread. Must
    /// be called before the thread's first write (the ring is created lazily).
    pub(super) fn set_thread_inflight(depth: usize) {
        DESIRED_INFLIGHT.with(|c| c.set(depth.clamp(1, MAX_INFLIGHT_WINDOWS)));
    }

    impl UringWriter {
        pub(super) fn new() -> Result<Self> {
            RawUringWriter::new().map(|_| Self)
        }

        /// Ensure this thread's io_uring ring exists. Returns `Err` if io_uring is
        /// unavailable — disabled at runtime, or the ring can't be created (e.g.
        /// ENOMEM). Callers below treat that as "use the POSIX writer". Checking
        /// this *before* consuming the write payload lets us fall back without
        /// having to clone it.
        fn ensure_ready(&self) -> Result<()> {
            with_thread_writer(|_| Ok(()))
        }

        pub(super) fn write_regular(&self, target: &Path, mode: u32, content: &[u8]) -> Result<()> {
            self.write_regular_owned(target, mode, content.to_vec())
        }

        pub(super) fn write_regular_owned(
            &self,
            target: &Path,
            mode: u32,
            content: Vec<u8>,
        ) -> Result<()> {
            if target.exists() {
                std::fs::remove_file(target).ok();
            }
            if self.ensure_ready().is_err() {
                return write_regular_posix(target, mode, &content)
                    .map(|_| ())
                    .with_context(|| format!("write {}", target.display()));
            }
            with_thread_writer(|writer| {
                writer
                    .write_regular_batch(
                        vec![PreparedRegularWrite {
                            target: target.to_path_buf(),
                            index_path: target.to_string_lossy().into_owned(),
                            mode,
                            content: content.into(),
                        }],
                        false,
                    )
                    .map(|_| ())
            })
            .with_context(|| format!("write {}", target.display()))
        }

        pub(super) fn write_regular_batch(
            &self,
            writes: Vec<PreparedRegularWrite>,
            collect_stats: bool,
        ) -> Result<Vec<crate::git::MaterializedPathStat>> {
            if writes.is_empty() {
                return Ok(Vec::new());
            }
            if self.ensure_ready().is_err() {
                return write_regular_batch_posix(&writes, collect_stats);
            }
            with_thread_writer(|writer| writer.write_regular_batch(writes, collect_stats))
        }

        pub(super) fn write_regular_batch_deferred(
            &self,
            writes: Vec<PreparedRegularWrite>,
            collect_stats: bool,
        ) -> Result<WriteOutcome> {
            if writes.is_empty() {
                return self.flush_deferred_writes();
            }
            if self.ensure_ready().is_err() {
                // io_uring unavailable: nothing was deferred on this thread, so a
                // straight POSIX write of this batch is the whole result.
                let written = writes.len();
                let stats = write_regular_batch_posix(&writes, collect_stats)?;
                return Ok(WriteOutcome { written, stats });
            }
            with_thread_writer(|writer| writer.write_regular_batch_deferred(writes, collect_stats))
        }

        pub(super) fn flush_deferred_writes(&self) -> Result<WriteOutcome> {
            // If this thread never created a ring (io_uring disabled, or it only
            // ever took the POSIX fallback), nothing was deferred here, so there
            // is nothing to flush — and creating a ring just to flush could
            // re-fail. Report an empty outcome.
            let has_ring = THREAD_WRITER.with(|cell| cell.borrow().is_some());
            if !has_ring {
                return Ok(WriteOutcome {
                    written: 0,
                    stats: Vec::new(),
                });
            }
            with_thread_writer(|writer| writer.flush_deferred_writes())
        }

        pub(super) fn probe(&self) -> Result<()> {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "ripclone-io-uring-probe-{}-{nanos}",
                std::process::id()
            ));
            let result = self.write_regular(&path, 0o600, b"ok").and_then(|_| {
                let content = std::fs::read(&path)
                    .with_context(|| format!("read probe {}", path.display()))?;
                if content != b"ok" {
                    anyhow::bail!("io_uring probe wrote unexpected content");
                }
                Ok(())
            });
            let _ = std::fs::remove_file(&path);
            result
        }
    }

    fn with_thread_writer<T>(f: impl FnOnce(&mut RawUringWriter) -> Result<T>) -> Result<T> {
        THREAD_WRITER.with(|cell| {
            let mut writer = cell.borrow_mut();
            if writer.is_none() {
                // Only *new* ring creation is gated by the runtime-disabled flag.
                // A thread that already has a working ring keeps using it, so any
                // writes it already deferred there are still flushed.
                if IO_URING_RUNTIME_DISABLED.load(Ordering::Relaxed) {
                    anyhow::bail!("io_uring disabled at runtime; using POSIX writer");
                }
                match RawUringWriter::new() {
                    Ok(w) => *writer = Some(w),
                    Err(e) => {
                        // A ring couldn't be created — typically ENOMEM / the
                        // locked-memory rlimit under heavy parallelism. Disable
                        // io_uring for the rest of the run so callers fall back to
                        // the POSIX writer instead of failing the clone.
                        IO_URING_RUNTIME_DISABLED.store(true, Ordering::Relaxed);
                        IO_URING_DISABLED_LOG.call_once(|| {
                            tracing::warn!(
                                "io_uring ring creation failed ({e:#}); disabling io_uring \
                                 and using the POSIX writer for the rest of this run"
                            )
                        });
                        return Err(e);
                    }
                }
            }
            f(writer
                .as_mut()
                .expect("thread-local io_uring writer initialized"))
        })
    }

    impl RawUringWriter {
        fn new() -> Result<Self> {
            let ring = Self::new_ring().context("initialize io_uring queue")?;
            let skip_open_success_cqe = ring.params().is_feature_skip_cqe_on_success();
            if skip_open_success_cqe {
                SKIP_OPEN_CQE_ENABLED_LOG.call_once(|| {
                    tracing::info!("io_uring successful direct-open CQEs will be skipped")
                });
            }
            let descriptor_mode = match ring
                .submitter()
                .register_files_sparse(REGISTERED_FILE_SLOTS as u32)
            {
                Ok(()) => {
                    DIRECT_DESCRIPTOR_ENABLED_LOG
                        .call_once(|| tracing::info!("io_uring direct descriptors enabled"));
                    DescriptorMode::DirectFd
                }
                Err(e) => {
                    tracing::debug!(
                        "io_uring direct descriptor registration unavailable; using normal fds: {e}"
                    );
                    DescriptorMode::NormalFd
                }
            };
            // Cap overlap depth so all in-flight windows' completions always fit
            // in the completion queue. A window posts up to 4 ops per file
            // (open/write/statx/close). If a fallback ring built with a smaller
            // CQ than requested, this keeps us from ever overflowing it.
            let cq_window_capacity =
                (ring.params().cq_entries() as usize / (MAX_BATCH_FILES * 4)).max(1);
            // The scheduler sets the depth per submitter via the thread-local;
            // for the plain per-thread path, `RIPCLONE_IO_URING_DEPTH` lets us
            // tune overlap without the scheduler at all.
            let desired = std::env::var("RIPCLONE_IO_URING_DEPTH")
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or_else(|| DESIRED_INFLIGHT.with(|c| c.get()));
            let max_inflight = desired
                .clamp(1, MAX_INFLIGHT_WINDOWS)
                .min(cq_window_capacity);
            Ok(Self {
                ring,
                descriptor_mode,
                skip_open_success_cqe,
                pending_windows: std::collections::VecDeque::new(),
                max_inflight,
                slot_cursor: 0,
                next_window_id: 1,
                direct_window_verified: false,
            })
        }

        fn new_ring() -> io::Result<IoUring> {
            let use_sqpoll = std::env::var("RIPCLONE_IO_URING_SQPOLL")
                .ok()
                .is_some_and(|value| {
                    matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES")
                });
            if use_sqpoll {
                let mut builder = IoUring::builder();
                builder.setup_sqpoll(1_000).setup_cqsize(CQ_ENTRIES);
                match builder.build(QUEUE_DEPTH) {
                    Ok(ring) => {
                        SQPOLL_RING_ENABLED_LOG
                            .call_once(|| tracing::info!("io_uring SQPOLL ring enabled"));
                        return Ok(ring);
                    }
                    Err(e) => {
                        SQPOLL_RING_FALLBACK_LOG.call_once(|| {
                            tracing::debug!(
                                "io_uring SQPOLL ring setup unavailable; using non-SQPOLL ring: {e}"
                            )
                        });
                    }
                }
            }

            let mut builder = IoUring::builder();
            builder
                .setup_single_issuer()
                .setup_defer_taskrun()
                .setup_coop_taskrun()
                .setup_cqsize(CQ_ENTRIES);
            match builder.build(QUEUE_DEPTH) {
                Ok(ring) => {
                    OPTIMIZED_RING_ENABLED_LOG.call_once(|| {
                        tracing::info!(
                            "io_uring optimized single-issuer/defer-taskrun ring enabled"
                        )
                    });
                    Ok(ring)
                }
                Err(e) => {
                    OPTIMIZED_RING_FALLBACK_LOG.call_once(|| {
                        tracing::debug!(
                            "io_uring optimized ring setup unavailable; using default setup: {e}"
                        )
                    });
                    // Still need a CQ large enough for the overlap depth.
                    IoUring::builder()
                        .setup_cqsize(CQ_ENTRIES)
                        .build(QUEUE_DEPTH)
                        .or_else(|_| IoUring::new(QUEUE_DEPTH))
                }
            }
        }

        fn write_regular_batch(
            &mut self,
            mut writes: Vec<PreparedRegularWrite>,
            collect_stats: bool,
        ) -> Result<Vec<crate::git::MaterializedPathStat>> {
            if !self.pending_windows.is_empty() {
                anyhow::bail!(
                    "cannot run synchronous io_uring writes while deferred writes are pending"
                );
            }
            let mut stats = Vec::new();
            while !writes.is_empty() {
                let n = writes.len().min(MAX_BATCH_FILES);
                let batch: Vec<_> = writes.drain(..n).collect();
                stats.extend(self.write_regular_window(batch, collect_stats)?);
            }
            Ok(stats)
        }

        fn write_regular_window(
            &mut self,
            writes: Vec<PreparedRegularWrite>,
            collect_stats: bool,
        ) -> Result<Vec<crate::git::MaterializedPathStat>> {
            if writes.is_empty() {
                return Ok(Vec::new());
            }

            let in_flight = prepare_in_flight(writes)?;

            match self.descriptor_mode {
                DescriptorMode::NormalFd => {
                    self.write_regular_window_normal(in_flight, collect_stats)
                }
                DescriptorMode::DirectFd => {
                    match self.write_regular_window_direct(in_flight, collect_stats) {
                        Ok(stats) => Ok(stats),
                        Err(DirectWriteError::Unsupported(in_flight)) => {
                            DIRECT_DESCRIPTOR_FALLBACK_LOG.call_once(|| {
                            tracing::info!(
                                "io_uring direct descriptors rejected by kernel; retrying with normal fds"
                            )
                        });
                            self.descriptor_mode = DescriptorMode::NormalFd;
                            self.write_regular_window_normal(in_flight, collect_stats)
                        }
                        Err(DirectWriteError::Other(e)) => Err(e),
                    }
                }
            }
        }

        fn write_regular_window_normal(
            &mut self,
            mut in_flight: Vec<InFlightWrite>,
            collect_stats: bool,
        ) -> Result<Vec<crate::git::MaterializedPathStat>> {
            let mut entries = Vec::with_capacity(in_flight.len());
            for (idx, write) in in_flight.iter().enumerate() {
                entries.push(
                    opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), write.path.as_ptr())
                        .flags(write.flags)
                        .mode(write.mode)
                        .build()
                        .user_data(user_data(0, idx, FileOp::Open)),
                );
            }
            self.submit_entries(&entries, entries.len(), "submit io_uring open batch")?;
            for (idx, op, res) in
                self.collect_completions(entries.len(), "wait for io_uring open batch completion")?
            {
                if op != FileOp::Open {
                    anyhow::bail!("unexpected io_uring completion in open phase");
                }
                let write = in_flight
                    .get_mut(idx)
                    .ok_or_else(|| anyhow::anyhow!("invalid io_uring completion index {idx}"))?;
                write.open_res = Some(res);
                if res >= 0 {
                    write.fd = Some(res);
                }
            }

            entries = Vec::with_capacity(in_flight.len() * 2);
            for (idx, write) in in_flight.iter_mut().enumerate() {
                if let Some(fd) = write.fd {
                    if write.content.is_empty() {
                        write.write_res = Some(0);
                    } else {
                        let content = write.content.as_slice();
                        entries.push(
                            opcode::Write::new(
                                types::Fd(fd),
                                content.as_ptr(),
                                content.len() as u32,
                            )
                            .offset(0)
                            .build()
                            .flags(squeue::Flags::IO_HARDLINK)
                            .user_data(user_data(
                                0,
                                idx,
                                FileOp::Write,
                            )),
                        );
                    }
                    entries.push(
                        opcode::Close::new(types::Fd(fd))
                            .build()
                            .user_data(user_data(0, idx, FileOp::Close)),
                    );
                }
            }
            if !entries.is_empty() {
                if let Err(e) = self.submit_entries(
                    &entries,
                    entries.len(),
                    "submit io_uring write/close batch",
                ) {
                    close_open_fds_sync(&mut in_flight);
                    return Err(e);
                }
                match self.collect_completions(
                    entries.len(),
                    "wait for io_uring write/close batch completion",
                ) {
                    Ok(completions) => {
                        for (idx, op, res) in completions {
                            match op {
                                FileOp::Write => {
                                    let write = in_flight.get_mut(idx).ok_or_else(|| {
                                        anyhow::anyhow!("invalid io_uring completion index {idx}")
                                    })?;
                                    write.write_res = Some(res);
                                }
                                FileOp::Close => {
                                    let write = in_flight.get_mut(idx).ok_or_else(|| {
                                        anyhow::anyhow!("invalid io_uring completion index {idx}")
                                    })?;
                                    write.close_res = Some(res);
                                    write.fd = None;
                                }
                                FileOp::Open | FileOp::Statx => {
                                    close_open_fds_sync(&mut in_flight);
                                    anyhow::bail!(
                                        "unexpected io_uring completion in write/close phase"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        close_open_fds_sync(&mut in_flight);
                        return Err(e);
                    }
                }
            }

            for write in &in_flight {
                check_open_result(write)?;
                check_write_result(write)?;
                check_close_result(write)?;
            }
            if collect_stats {
                in_flight
                    .iter()
                    .map(|write| {
                        std::fs::metadata(&write.target)
                            .with_context(|| format!("stat {}", write.target.display()))
                            .map(|metadata| {
                                crate::git::materialized_path_stat_from_metadata(
                                    write.index_path.clone(),
                                    &metadata,
                                )
                            })
                    })
                    .collect()
            } else {
                Ok(Vec::new())
            }
        }

        fn write_regular_window_direct(
            &mut self,
            in_flight: Vec<InFlightWrite>,
            collect_stats: bool,
        ) -> std::result::Result<Vec<crate::git::MaterializedPathStat>, DirectWriteError> {
            let pending = self.submit_direct_window(in_flight, collect_stats, false, 0)?;
            self.harvest_direct_window(pending)
        }

        fn write_regular_batch_deferred(
            &mut self,
            writes: Vec<PreparedRegularWrite>,
            collect_stats: bool,
        ) -> Result<WriteOutcome> {
            let files = writes.len();
            let bytes: u64 = writes.iter().map(|write| write.content.len() as u64).sum();
            let in_flight = prepare_in_flight(writes)?;

            match self.descriptor_mode {
                DescriptorMode::DirectFd if !self.direct_window_verified => {
                    let io_start = Instant::now();
                    match self.write_regular_window_direct(in_flight, collect_stats) {
                        Ok(stats) => {
                            record_io(io_start.elapsed(), files as u64, bytes);
                            Ok(WriteOutcome {
                                written: files,
                                stats,
                            })
                        }
                        Err(DirectWriteError::Unsupported(in_flight)) => {
                            DIRECT_DESCRIPTOR_FALLBACK_LOG.call_once(|| {
                                tracing::info!(
                                    "io_uring direct descriptors rejected by kernel; retrying with normal fds"
                                )
                            });
                            self.descriptor_mode = DescriptorMode::NormalFd;
                            let stats =
                                self.write_regular_window_normal(in_flight, collect_stats)?;
                            record_io(io_start.elapsed(), files as u64, bytes);
                            Ok(WriteOutcome {
                                written: files,
                                stats,
                            })
                        }
                        Err(DirectWriteError::Other(e)) => Err(e),
                    }
                }
                DescriptorMode::DirectFd => {
                    // If we are already at our overlap depth, harvest the oldest
                    // window first — that frees the fixed-file slot range the new
                    // window is about to reuse.
                    let (prev_written, prev_stats) =
                        if self.pending_windows.len() >= self.max_inflight {
                            let front = self
                                .pending_windows
                                .pop_front()
                                .expect("pending window present at depth");
                            let written = front.in_flight.len();
                            let stats = self
                                .harvest_direct_window(front)
                                .map_err(DirectWriteError::into_anyhow)?;
                            (written, stats)
                        } else {
                            (0, Vec::new())
                        };
                    let slot_base = self.slot_cursor * MAX_BATCH_FILES;
                    match self.submit_direct_window(in_flight, collect_stats, true, slot_base) {
                        Ok(current) => {
                            self.slot_cursor = (self.slot_cursor + 1) % self.max_inflight;
                            self.pending_windows.push_back(current);
                            Ok(WriteOutcome {
                                written: prev_written,
                                stats: prev_stats,
                            })
                        }
                        Err(DirectWriteError::Unsupported(in_flight)) => {
                            // Drain whatever is still in flight, fall back to
                            // normal fds, and write this window synchronously.
                            let mut completed = self.flush_deferred_writes()?;
                            completed.written += prev_written;
                            completed.stats.extend(prev_stats);
                            self.descriptor_mode = DescriptorMode::NormalFd;
                            let io_start = Instant::now();
                            let stats =
                                self.write_regular_window_normal(in_flight, collect_stats)?;
                            record_io(io_start.elapsed(), files as u64, bytes);
                            completed.written += files;
                            completed.stats.extend(stats);
                            Ok(completed)
                        }
                        Err(DirectWriteError::Other(e)) => Err(e),
                    }
                }
                DescriptorMode::NormalFd => {
                    let mut completed = self.flush_deferred_writes()?;
                    let io_start = Instant::now();
                    let stats = self.write_regular_window_normal(in_flight, collect_stats)?;
                    record_io(io_start.elapsed(), files as u64, bytes);
                    completed.written += files;
                    completed.stats.extend(stats);
                    Ok(completed)
                }
            }
        }

        fn flush_deferred_writes(&mut self) -> Result<WriteOutcome> {
            let mut written = 0usize;
            let mut stats = Vec::new();
            // Harvest oldest-first so each window's slot range is reclaimed in
            // submission order.
            while let Some(pending) = self.pending_windows.pop_front() {
                let window_written = pending.in_flight.len();
                let collect_stats = pending.collect_stats;
                match self.harvest_direct_window(pending) {
                    Ok(s) => {
                        written += window_written;
                        stats.extend(s);
                    }
                    Err(DirectWriteError::Unsupported(in_flight)) => {
                        DIRECT_DESCRIPTOR_FALLBACK_LOG.call_once(|| {
                            tracing::info!(
                                "io_uring direct descriptors rejected by kernel; retrying with normal fds"
                            )
                        });
                        self.descriptor_mode = DescriptorMode::NormalFd;
                        let s = self.write_regular_window_normal(in_flight, collect_stats)?;
                        written += window_written;
                        stats.extend(s);
                    }
                    Err(DirectWriteError::Other(e)) => return Err(e),
                }
            }
            // All windows drained: the next window can start from range 0 again.
            self.slot_cursor = 0;
            Ok(WriteOutcome { written, stats })
        }

        fn submit_direct_window(
            &mut self,
            mut in_flight: Vec<InFlightWrite>,
            collect_stats: bool,
            record_io_on_harvest: bool,
            slot_base: usize,
        ) -> std::result::Result<PendingDirectWindow, DirectWriteError> {
            let mut statx_buffers: Vec<libc::statx> = (0..in_flight.len())
                .map(|_| unsafe { std::mem::zeroed() })
                .collect();
            let mut entries = Vec::with_capacity(in_flight.len() * 4);
            let mut skipped_success_cqes = 0usize;
            let mut expected_closes = 0usize;
            let skip_write_success_cqe = self.skip_open_success_cqe && collect_stats;
            if skip_write_success_cqe {
                SKIP_WRITE_CQE_ENABLED_LOG.call_once(|| {
                    tracing::info!(
                        "io_uring successful direct-write CQEs will be skipped with statx size verification"
                    )
                });
            }
            let window_id = self.next_window_id();
            for idx in 0..in_flight.len() {
                let slot = (slot_base + idx) as u32;
                let path_ptr = in_flight[idx].path.as_ptr();
                let flags = in_flight[idx].flags;
                let mode = in_flight[idx].mode;
                let dest = types::DestinationSlot::try_from_slot_target(slot).map_err(|_| {
                    DirectWriteError::Other(anyhow::anyhow!("invalid fixed file slot {slot}"))
                })?;
                let mut open_flags = squeue::Flags::IO_LINK;
                if self.skip_open_success_cqe {
                    open_flags |= squeue::Flags::SKIP_SUCCESS;
                    skipped_success_cqes += 1;
                }
                entries.push(
                    opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_ptr)
                        .flags(flags & !libc::O_CLOEXEC)
                        .mode(mode)
                        .file_index(Some(dest))
                        .build()
                        .flags(open_flags)
                        .user_data(user_data(window_id, idx, FileOp::Open)),
                );
                if in_flight[idx].content.is_empty() {
                    in_flight[idx].write_res = Some(0);
                } else {
                    let (content_ptr, content_len) = {
                        let content = in_flight[idx].content.as_slice();
                        (content.as_ptr(), content.len() as u32)
                    };
                    let mut write_flags = squeue::Flags::IO_HARDLINK;
                    if skip_write_success_cqe {
                        write_flags |= squeue::Flags::SKIP_SUCCESS;
                        in_flight[idx].write_success_cqe_skipped = true;
                        skipped_success_cqes += 1;
                    }
                    entries.push(
                        opcode::Write::new(types::Fixed(slot), content_ptr, content_len)
                            .offset(0)
                            .build()
                            .flags(write_flags)
                            .user_data(user_data(window_id, idx, FileOp::Write)),
                    );
                }
                if collect_stats {
                    entries.push(
                        opcode::Statx::new(
                            types::Fd(libc::AT_FDCWD),
                            path_ptr,
                            &mut statx_buffers[idx] as *mut libc::statx as *mut types::statx,
                        )
                        .flags(libc::AT_SYMLINK_NOFOLLOW)
                        .mask(libc::STATX_BASIC_STATS)
                        .build()
                        .flags(squeue::Flags::IO_HARDLINK)
                        .user_data(user_data(
                            window_id,
                            idx,
                            FileOp::Statx,
                        )),
                    );
                }
                expected_closes += 1;
                entries.push(
                    opcode::Close::new(types::Fixed(slot))
                        .build()
                        .user_data(user_data(window_id, idx, FileOp::Close)),
                );
            }

            let expected_completions = entries.len() - skipped_success_cqes;
            let files = in_flight.len() as u64;
            let bytes = in_flight
                .iter()
                .map(|write| write.content.len() as u64)
                .sum();
            let submit_start = Instant::now();
            self.submit_entries(
                &entries,
                0,
                "submit io_uring direct open/write/statx/close batch",
            )
            .map_err(DirectWriteError::Other)?;
            Ok(PendingDirectWindow {
                window_id,
                slot_base,
                in_flight,
                statx_buffers,
                collect_stats,
                expected_completions,
                expected_closes,
                completions_seen: 0,
                closes_seen: 0,
                submitted_ns: submit_start.elapsed().as_nanos() as u64,
                record_io_on_harvest,
                files,
                bytes,
            })
        }

        fn harvest_direct_window(
            &mut self,
            mut pending: PendingDirectWindow,
        ) -> std::result::Result<Vec<crate::git::MaterializedPathStat>, DirectWriteError> {
            let wait_start = Instant::now();
            self.collect_direct_window_completions(
                &mut pending,
                "wait for io_uring direct open/write/statx/close batch completion",
            )
            .map_err(DirectWriteError::Other)?;
            if pending.record_io_on_harvest {
                let io_ns = pending.submitted_ns + wait_start.elapsed().as_nanos() as u64;
                record_io(Duration::from_nanos(io_ns), pending.files, pending.bytes);
            }
            if self.skip_open_success_cqe {
                for write in &mut pending.in_flight {
                    if write.open_res.is_none() {
                        write.open_res = Some(0);
                    }
                }
            }

            if direct_descriptors_unsupported(&pending.in_flight) {
                return Err(DirectWriteError::Unsupported(pending.in_flight));
            }

            for (idx, write) in pending.in_flight.iter().enumerate() {
                check_open_result(write).map_err(DirectWriteError::Other)?;
                check_write_result(write).map_err(DirectWriteError::Other)?;
                if pending.collect_stats {
                    let statx = &pending.statx_buffers[idx];
                    check_statx_result(write, statx).map_err(DirectWriteError::Other)?;
                }
                check_close_result(write).map_err(DirectWriteError::Other)?;
            }
            self.direct_window_verified = true;
            if pending.collect_stats {
                Ok(pending
                    .in_flight
                    .iter()
                    .zip(pending.statx_buffers.iter())
                    .map(|(write, statx)| {
                        crate::git::materialized_path_stat_from_statx(
                            write.index_path.clone(),
                            statx,
                        )
                    })
                    .collect())
            } else {
                Ok(Vec::new())
            }
        }

        fn next_window_id(&mut self) -> u32 {
            let id = self.next_window_id;
            self.next_window_id = self.next_window_id.wrapping_add(1);
            if self.next_window_id == 0 {
                self.next_window_id = 1;
            }
            id
        }

        fn submit_entries(
            &mut self,
            entries: &[squeue::Entry],
            wait_for: usize,
            context: &str,
        ) -> Result<()> {
            if entries.is_empty() {
                return Ok(());
            }
            {
                let mut sq = self.ring.submission();
                unsafe {
                    sq.push_multiple(entries)
                        .map_err(|_| anyhow::anyhow!("io_uring submission queue is full"))?;
                }
            }
            if wait_for == 0 {
                self.ring
                    .submitter()
                    .submit()
                    .with_context(|| context.to_string())?;
            } else {
                self.ring
                    .submitter()
                    .submit_and_wait(wait_for)
                    .with_context(|| context.to_string())?;
            }
            Ok(())
        }

        fn collect_completions(
            &mut self,
            expected: usize,
            context: &str,
        ) -> Result<Vec<(usize, FileOp, i32)>> {
            let mut completions = Vec::with_capacity(expected);
            while completions.len() < expected {
                {
                    let mut cq = self.ring.completion();
                    for cqe in &mut cq {
                        let (_window_id, idx, op) = parse_user_data(cqe.user_data())?;
                        completions.push((idx, op, cqe.result()));
                    }
                }
                if completions.len() < expected {
                    let remaining = expected - completions.len();
                    self.ring
                        .submitter()
                        .submit_and_wait(remaining)
                        .with_context(|| context.to_string())?;
                }
            }
            Ok(completions)
        }

        fn collect_direct_window_completions(
            &mut self,
            target: &mut PendingDirectWindow,
            context: &str,
        ) -> Result<()> {
            while !target.is_complete() {
                {
                    let mut cq = self.ring.completion();
                    for cqe in &mut cq {
                        let (window_id, idx, op) = parse_user_data(cqe.user_data())?;
                        if window_id == target.window_id {
                            apply_direct_completion(target, idx, op, cqe.result())?;
                        } else if let Some(pending) = self
                            .pending_windows
                            .iter_mut()
                            .find(|w| w.window_id == window_id)
                        {
                            // A completion for another still-in-flight window;
                            // record it so its later harvest sees a full set.
                            apply_direct_completion(pending, idx, op, cqe.result())?;
                        } else {
                            anyhow::bail!("unexpected io_uring completion for window {window_id}");
                        }
                    }
                }
                if !target.is_complete() {
                    self.ring
                        .submitter()
                        .submit_and_wait(1)
                        .with_context(|| context.to_string())?;
                }
            }
            Ok(())
        }
    }

    impl Drop for RawUringWriter {
        fn drop(&mut self) {
            let _ = self.flush_deferred_writes();
        }
    }

    fn prepare_in_flight(writes: Vec<PreparedRegularWrite>) -> Result<Vec<InFlightWrite>> {
        let mut in_flight = Vec::with_capacity(writes.len());
        for write in writes {
            if write.content.len() > u32::MAX as usize {
                anyhow::bail!(
                    "file too large for single io_uring write: {}",
                    write.target.display()
                );
            }
            let path = CString::new(write.target.as_os_str().as_bytes())
                .with_context(|| format!("path contains NUL byte: {}", write.target.display()))?;
            let flags =
                libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_CLOEXEC | libc::O_NOFOLLOW;
            in_flight.push(InFlightWrite {
                target: write.target,
                index_path: write.index_path,
                path,
                flags,
                mode: write.mode as libc::mode_t,
                content: write.content,
                fd: None,
                open_res: None,
                write_res: None,
                write_success_cqe_skipped: false,
                statx_res: None,
                close_res: None,
            });
        }
        Ok(in_flight)
    }

    fn apply_direct_completion(
        window: &mut PendingDirectWindow,
        idx: usize,
        op: FileOp,
        res: i32,
    ) -> Result<()> {
        let write = window
            .in_flight
            .get_mut(idx)
            .ok_or_else(|| anyhow::anyhow!("invalid io_uring completion index {idx}"))?;
        match op {
            FileOp::Open => {
                write.open_res = Some(res);
            }
            FileOp::Write => {
                write.write_res = Some(res);
            }
            FileOp::Statx => {
                write.statx_res = Some(res);
            }
            FileOp::Close => {
                write.close_res = Some(res);
                window.closes_seen += 1;
            }
        }
        window.completions_seen += 1;
        Ok(())
    }

    fn user_data(window_id: u32, idx: usize, op: FileOp) -> u64 {
        ((window_id as u64) << 32) | ((idx as u64) << 2) | op as u64
    }

    enum DirectWriteError {
        Unsupported(Vec<InFlightWrite>),
        Other(anyhow::Error),
    }

    impl DirectWriteError {
        fn into_anyhow(self) -> anyhow::Error {
            match self {
                DirectWriteError::Unsupported(_) => {
                    anyhow::anyhow!("io_uring direct descriptors became unsupported after probe")
                }
                DirectWriteError::Other(e) => e,
            }
        }
    }

    fn direct_descriptors_unsupported(writes: &[InFlightWrite]) -> bool {
        !writes.is_empty()
            && writes.iter().all(|write| {
                matches!(
                    write.open_res,
                    Some(res) if res == -libc::EINVAL || res == -libc::EOPNOTSUPP
                )
            })
    }

    fn parse_user_data(data: u64) -> Result<(u32, usize, FileOp)> {
        let window_id = (data >> 32) as u32;
        let idx = ((data & 0xffff_ffff) >> 2) as usize;
        let op = match data & 0b11 {
            0 => FileOp::Open,
            1 => FileOp::Write,
            2 => FileOp::Close,
            3 => FileOp::Statx,
            other => anyhow::bail!("invalid io_uring completion op {other}"),
        };
        Ok((window_id, idx, op))
    }

    fn check_open_result(write: &InFlightWrite) -> Result<()> {
        let res = write.open_res.ok_or_else(|| {
            anyhow::anyhow!("missing open completion for {}", write.target.display())
        })?;
        if res < 0 {
            Err(cqe_error(res)).with_context(|| format!("open {}", write.target.display()))?;
        }
        Ok(())
    }

    fn check_write_result(write: &InFlightWrite) -> Result<()> {
        if write.open_res.is_some_and(|res| res < 0) {
            return Ok(());
        }
        if write.write_success_cqe_skipped && write.write_res.is_none() {
            return Ok(());
        }
        let res = write.write_res.ok_or_else(|| {
            anyhow::anyhow!("missing write completion for {}", write.target.display())
        })?;
        if res < 0 {
            Err(cqe_error(res)).with_context(|| format!("write {}", write.target.display()))?;
        }
        let expected = write.content.len();
        if res as usize != expected {
            anyhow::bail!(
                "short io_uring write {}: wrote {} of {} bytes",
                write.target.display(),
                res,
                expected
            );
        }
        Ok(())
    }

    fn check_statx_result(write: &InFlightWrite, statx: &libc::statx) -> Result<()> {
        if write.open_res.is_some_and(|res| res < 0) {
            return Ok(());
        }
        if write.write_res.is_some_and(|res| res < 0) {
            return Ok(());
        }
        let res = write.statx_res.ok_or_else(|| {
            anyhow::anyhow!("missing statx completion for {}", write.target.display())
        })?;
        if res < 0 {
            Err(cqe_error(res)).with_context(|| format!("statx {}", write.target.display()))?;
        }
        let expected = write.content.len() as u64;
        if statx.stx_size != expected {
            anyhow::bail!(
                "short io_uring write {}: file size {} after writing {} bytes",
                write.target.display(),
                statx.stx_size,
                expected
            );
        }
        Ok(())
    }

    fn check_close_result(write: &InFlightWrite) -> Result<()> {
        if write.open_res.is_some_and(|res| res < 0) {
            return Ok(());
        }
        let res = write.close_res.ok_or_else(|| {
            anyhow::anyhow!("missing close completion for {}", write.target.display())
        })?;
        if res < 0 {
            Err(cqe_error(res)).with_context(|| format!("close {}", write.target.display()))?;
        }
        Ok(())
    }

    fn cqe_error(res: i32) -> io::Error {
        debug_assert!(res < 0);
        io::Error::from_raw_os_error(-res)
    }

    fn close_open_fds_sync(writes: &mut [InFlightWrite]) {
        for write in writes {
            if let Some(fd) = write.fd.take() {
                unsafe {
                    libc::close(fd);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Write scheduler  (DEPRECATED — slated for removal)
// ---------------------------------------------------------------------------
//
// Superseded by per-thread overlap depth (`RIPCLONE_IO_URING_DEPTH`), which is
// faster on throttled CPUs and free on dedicated ones. Kept opt-in for now; see
// docs/WRITER_SCHEDULER_EXPERIMENT.md for the A/B data and removal note.
//
// By default each extraction worker owns its own writer and io_uring ring, so a
// worker can only batch the files in its own chunk. When frames are small the
// windows stay small.
//
// `WorktreeWriteScheduler` (opt-in via `RIPCLONE_IO_URING_SCHEDULER`) splits the
// work: workers still do the CPU part (validate paths, make dirs, write
// symlinks), then hand the regular-file writes to a small pool of submitter
// threads. Each submitter owns one ring and groups writes from every worker
// routed to it, so windows fill up even when single frames are tiny. POSIX
// works too (submitters just write right away), so this is testable off Linux.
//
// Use it as: `submit(writes)` from each worker, then one `flush()` once all
// workers are done. `Drop` closes the channels and joins the threads.

/// Most files a submitter puts in one window. Two windows can be in flight at
/// once using separate slot ranges, so a window must fit `MAX_BATCH_FILES`.
const SCHEDULER_MAX_WINDOW_FILES: usize = 512;

/// Scheduler knobs. All can be set from the environment so the benchmark can
/// sweep them without recompiling.
///
/// DEPRECATED: the submitter-pool scheduler is superseded by per-thread overlap
/// (`RIPCLONE_IO_URING_DEPTH`) and slated for removal. See
/// `docs/WRITER_SCHEDULER_EXPERIMENT.md`.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    /// Submitter threads, each with its own ring.
    pub submitters: usize,
    /// Windows allowed in flight per submitter. `1` means no overlap.
    pub inflight: usize,
    /// Most files per window.
    pub batch_files: usize,
    /// Cut a window once it reaches this many bytes, even if it has fewer files.
    pub byte_cap: usize,
    /// How long a submitter waits for more work before writing a partial window.
    pub flush_timeout: Duration,
    /// Work-channel size per submitter. Blocks a producer when its submitter is
    /// behind.
    pub queue_depth: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            submitters: (cores / 2).clamp(1, 4),
            inflight: 2,
            batch_files: SCHEDULER_MAX_WINDOW_FILES,
            byte_cap: 16 * 1024 * 1024,
            flush_timeout: Duration::from_micros(200),
            queue_depth: 8,
        }
    }
}

impl SchedulerConfig {
    /// True when `RIPCLONE_IO_URING_SCHEDULER` is set to a truthy value.
    pub fn enabled() -> bool {
        std::env::var("RIPCLONE_IO_URING_SCHEDULER")
            .ok()
            .map(|v| {
                let v = v.trim();
                !(v.is_empty()
                    || v == "0"
                    || v.eq_ignore_ascii_case("false")
                    || v.eq_ignore_ascii_case("off")
                    || v.eq_ignore_ascii_case("no"))
            })
            .unwrap_or(false)
    }

    /// Build a config from defaults, overlaying any env knobs that are set.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(v) = env_usize("RIPCLONE_IO_URING_SUBMITTERS") {
            cfg.submitters = v.max(1);
        }
        if let Some(v) = env_usize("RIPCLONE_IO_URING_INFLIGHT") {
            cfg.inflight = v.max(1);
        }
        if let Some(v) = env_usize("RIPCLONE_IO_URING_BATCH_FILES") {
            cfg.batch_files = v.clamp(1, SCHEDULER_MAX_WINDOW_FILES);
        }
        if let Some(v) = env_usize("RIPCLONE_IO_URING_BYTE_CAP") {
            cfg.byte_cap = v.max(1);
        }
        if let Some(v) = env_usize("RIPCLONE_IO_URING_FLUSH_US") {
            cfg.flush_timeout = Duration::from_micros(v as u64);
        }
        if let Some(v) = env_usize("RIPCLONE_IO_URING_QUEUE_DEPTH") {
            cfg.queue_depth = v.max(1);
        }
        // A window must never exceed the slot-range cap regardless of overrides.
        cfg.batch_files = cfg.batch_files.clamp(1, SCHEDULER_MAX_WINDOW_FILES);
        cfg
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|s| s.trim().parse().ok())
}

/// Build the per-submitter backend writer. Enabling the scheduler implies
/// "try io_uring" by default; `RIPCLONE_IO_URING=0` still forces POSIX.
fn scheduler_backend_writer() -> Result<WorktreeWriter> {
    match IoUringMode::from_env() {
        // `from_env` returns `Disabled` for both "unset" and an explicit falsey
        // value. Distinguish them: unset means "scheduler default → auto".
        IoUringMode::Disabled if std::env::var_os("RIPCLONE_IO_URING").is_some() => {
            Ok(WorktreeWriter::posix())
        }
        IoUringMode::Force => WorktreeWriter::io_uring(),
        _ => WorktreeWriter::auto(),
    }
}

enum SubmitterMsg {
    Write(Vec<PreparedRegularWrite>),
    Flush(crossbeam_channel::Sender<Result<Vec<crate::git::MaterializedPathStat>>>),
}

struct SubmitterHandle {
    tx: crossbeam_channel::Sender<SubmitterMsg>,
    join: Option<std::thread::JoinHandle<()>>,
}

pub struct WorktreeWriteScheduler {
    target_dir: PathBuf,
    options: WriteOptions,
    submitters: Vec<SubmitterHandle>,
    next_submitter: AtomicUsize,
}

impl WorktreeWriteScheduler {
    /// Create a scheduler that writes into `target_dir`, reading knobs from the
    /// environment.
    pub fn new(target_dir: PathBuf, options: WriteOptions) -> Result<Self> {
        Self::with_config(target_dir, options, SchedulerConfig::from_env())
    }

    pub fn with_config(
        target_dir: PathBuf,
        options: WriteOptions,
        config: SchedulerConfig,
    ) -> Result<Self> {
        let n = config.submitters.max(1);
        let mut submitters = Vec::with_capacity(n);
        for i in 0..n {
            let (tx, rx) = crossbeam_channel::bounded::<SubmitterMsg>(config.queue_depth);
            let opts = options;
            let cfg = config;
            let join = std::thread::Builder::new()
                .name(format!("rc-uring-submit-{i}"))
                .spawn(move || run_submitter(rx, opts, cfg))
                .context("spawn io_uring submitter thread")?;
            submitters.push(SubmitterHandle {
                tx,
                join: Some(join),
            });
        }
        Ok(Self {
            target_dir,
            options,
            submitters,
            next_submitter: AtomicUsize::new(0),
        })
    }

    pub fn submitter_count(&self) -> usize {
        self.submitters.len()
    }

    /// Prepare `writes` on this thread, then send the regular-file writes to the
    /// submitter pool. Returns the file count (symlinks written here plus
    /// regular files queued).
    pub fn submit(&self, writes: Vec<OwnedFileWrite>) -> Result<usize> {
        if writes.is_empty() {
            return Ok(0);
        }
        let (written, regulars) = prepare_owned_entries(&self.target_dir, writes, self.options)?;
        if regulars.is_empty() {
            return Ok(written);
        }

        // Send the whole batch to one submitter, round-robin across calls. A
        // batch is ~one frame's files, so this keeps them together (one consumer
        // core, fuller windows) and avoids per-file hashing and bucket allocation.
        let n = self.submitters.len();
        let i = if n == 1 {
            0
        } else {
            self.next_submitter.fetch_add(1, Ordering::Relaxed) % n
        };
        self.send_to(i, regulars)?;
        Ok(written)
    }

    fn send_to(&self, i: usize, writes: Vec<PreparedRegularWrite>) -> Result<()> {
        self.submitters[i]
            .tx
            .send(SubmitterMsg::Write(writes))
            .map_err(|_| anyhow::anyhow!("io_uring submitter {i} stopped accepting writes"))
    }

    /// Wait for every submitter to finish and return the collected stats. Call
    /// once, after all `submit` calls have returned.
    pub fn flush(&self) -> Result<WriteOutcome> {
        let mut acks = Vec::with_capacity(self.submitters.len());
        for (i, handle) in self.submitters.iter().enumerate() {
            let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
            handle
                .tx
                .send(SubmitterMsg::Flush(ack_tx))
                .map_err(|_| anyhow::anyhow!("io_uring submitter {i} stopped before flush"))?;
            acks.push(ack_rx);
        }
        let mut stats = Vec::new();
        let mut first_err: Option<anyhow::Error> = None;
        for (i, ack_rx) in acks.into_iter().enumerate() {
            match ack_rx.recv() {
                Ok(Ok(s)) => stats.extend(s),
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some(anyhow::anyhow!("io_uring submitter {i} died before ack"));
                    }
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(WriteOutcome { written: 0, stats })
    }
}

impl Drop for WorktreeWriteScheduler {
    fn drop(&mut self) {
        // Drop the senders so each submitter sees a closed channel and exits,
        // then join.
        for handle in &mut self.submitters {
            let (dummy_tx, _dummy_rx) = crossbeam_channel::bounded::<SubmitterMsg>(0);
            let live = std::mem::replace(&mut handle.tx, dummy_tx);
            drop(live);
        }
        for handle in &mut self.submitters {
            if let Some(join) = handle.join.take() {
                let _ = join.join();
            }
        }
    }
}

fn run_submitter(
    rx: crossbeam_channel::Receiver<SubmitterMsg>,
    options: WriteOptions,
    cfg: SchedulerConfig,
) {
    let mut state = SubmitterState::new(options, cfg);
    loop {
        let msg = if state.buf.is_empty() {
            // Nothing pending: block until work or shutdown.
            match rx.recv() {
                Ok(msg) => msg,
                Err(_) => break,
            }
        } else {
            // Holding a partial window: flush it if no work arrives promptly.
            match rx.recv_timeout(cfg.flush_timeout) {
                Ok(msg) => msg,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    state.flush_partial();
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        };
        match msg {
            SubmitterMsg::Write(writes) => state.accept(writes),
            SubmitterMsg::Flush(ack) => {
                let result = state.drain_and_report();
                let _ = ack.send(result);
            }
        }
    }
    // Channel closed: the scheduler was dropped without `flush()`. Write out
    // anything left. No one can read a Result now, so a failure here would be
    // lost — log it so a partial write can't pass silently. Callers that call
    // `flush()` first leave nothing here, so this is usually a no-op.
    if let Err(e) = state.drain_and_report() {
        tracing::error!("io_uring submitter dropped with unflushed write failure: {e:#}");
    }
}

struct SubmitterState {
    writer: std::result::Result<WorktreeWriter, String>,
    options: WriteOptions,
    cfg: SchedulerConfig,
    buf: Vec<PreparedRegularWrite>,
    buf_bytes: usize,
    acc_stats: Vec<crate::git::MaterializedPathStat>,
    acc_err: Option<anyhow::Error>,
}

impl SubmitterState {
    fn new(options: WriteOptions, cfg: SchedulerConfig) -> Self {
        // Set the ring overlap depth before the writer makes this thread's ring
        // (it makes one during its probe).
        #[cfg(target_os = "linux")]
        linux_uring::set_thread_inflight(cfg.inflight);
        let writer = scheduler_backend_writer().map_err(|e| format!("{e:#}"));
        Self {
            writer,
            options,
            cfg,
            buf: Vec::new(),
            buf_bytes: 0,
            acc_stats: Vec::new(),
            acc_err: None,
        }
    }

    fn accept(&mut self, writes: Vec<PreparedRegularWrite>) {
        if self.acc_err.is_some() {
            return; // already failed; swallow until flush reports it
        }
        for w in &writes {
            self.buf_bytes += w.content.len();
        }
        self.buf.extend(writes);
        // Emit full windows while we have enough to fill one.
        while self.buf.len() >= self.cfg.batch_files || self.buf_bytes >= self.cfg.byte_cap {
            let take = self.next_window_len();
            self.emit(take);
            if self.acc_err.is_some() {
                break;
            }
        }
    }

    /// How many buffered files go in the next window: up to `batch_files`, or
    /// fewer if `byte_cap` is reached first. Always at least one.
    fn next_window_len(&self) -> usize {
        let mut bytes = 0usize;
        let mut count = 0usize;
        for w in &self.buf {
            bytes += w.content.len();
            count += 1;
            if count >= self.cfg.batch_files || bytes >= self.cfg.byte_cap {
                break;
            }
        }
        count.max(1).min(self.buf.len())
    }

    fn emit(&mut self, take: usize) {
        if take == 0 || self.acc_err.is_some() {
            return;
        }
        let window: Vec<PreparedRegularWrite> = self.buf.drain(..take).collect();
        let win_bytes: usize = window.iter().map(|w| w.content.len()).sum();
        self.buf_bytes = self.buf_bytes.saturating_sub(win_bytes);
        let writer = match &self.writer {
            Ok(w) => w,
            Err(e) => {
                self.acc_err = Some(anyhow::anyhow!("create worktree writer: {e}"));
                return;
            }
        };
        // The ring's own overlap depth (set from `cfg.inflight`) decides how
        // many windows stay in flight; a depth of 1 harvests each window before
        // the next is submitted, so no explicit flush is needed here.
        match writer.write_regular_batch_deferred(window, self.options) {
            Ok(outcome) => self.acc_stats.extend(outcome.stats),
            Err(e) => self.acc_err = Some(e),
        }
    }

    fn flush_partial(&mut self) {
        while !self.buf.is_empty() && self.acc_err.is_none() {
            let take = self.next_window_len();
            self.emit(take);
        }
    }

    /// Write out everything left (buffer plus any in-flight windows) and return
    /// the stats collected since the last drain, or the first error.
    fn drain_and_report(&mut self) -> Result<Vec<crate::git::MaterializedPathStat>> {
        self.flush_partial();
        if self.acc_err.is_none()
            && let Ok(w) = &self.writer
        {
            match w.flush_deferred_writes() {
                Ok(outcome) => self.acc_stats.extend(outcome.stats),
                Err(e) => self.acc_err = Some(e),
            }
        }
        self.buf.clear();
        self.buf_bytes = 0;
        let stats = std::mem::take(&mut self.acc_stats);
        match self.acc_err.take() {
            Some(e) => Err(e),
            None => Ok(stats),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posix_writer_writes_regular_file_modes_and_content() {
        let dir = tempfile::TempDir::new().unwrap();
        let writer = WorktreeWriter::posix();
        let entry = FileEntry {
            path: b"bin/tool".to_vec(),
            mode: 0o100755,
            blob_sha1: Vec::new(),
            fragments: Vec::new(),
        };

        writer
            .write_entry(dir.path(), &entry, b"#!/bin/sh\n")
            .unwrap();

        let path = dir.path().join("bin/tool");
        assert_eq!(std::fs::read(&path).unwrap(), b"#!/bin/sh\n");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o755
            );
        }
    }

    #[test]
    fn posix_writer_rejects_parent_dir_paths() {
        let dir = tempfile::TempDir::new().unwrap();
        let writer = WorktreeWriter::posix();
        let entry = FileEntry {
            path: b"../escape".to_vec(),
            mode: 0o100644,
            blob_sha1: Vec::new(),
            fragments: Vec::new(),
        };

        assert!(writer.write_entry(dir.path(), &entry, b"nope").is_err());
    }

    // A mid-run io_uring ring-allocation failure (e.g. ENOMEM / the locked-memory
    // rlimit under heavy parallelism) must degrade to the POSIX writer, never fail
    // the clone.
    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_falls_back_to_posix_when_runtime_disabled() {
        // Build a real io_uring writer; skip where io_uring is unavailable.
        let writer = match WorktreeWriter::io_uring() {
            Ok(w) => w,
            Err(_) => {
                eprintln!("skipping: io_uring unavailable on this host");
                return;
            }
        };
        // Simulate a per-thread ring allocation having failed and disabled
        // io_uring for the run.
        linux_uring::set_runtime_disabled_for_test(true);
        let dir = tempfile::TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();
        // More than IO_URING_MIN_BATCH_FILES so the io_uring batch path is taken
        // (and then falls back), not the small-batch POSIX shortcut.
        let writes: Vec<OwnedFileWrite> = (0..8)
            .map(|i| OwnedFileWrite {
                entry: FileEntry {
                    path: format!("dir/file-{i}.txt").into_bytes(),
                    mode: 0o100644,
                    blob_sha1: Vec::new(),
                    fragments: Vec::new(),
                },
                content: format!("content {i}\n").into_bytes().into(),
            })
            .collect();
        // Run on a fresh thread that has no ring yet, so ring creation is what's
        // attempted (and refused) — exercising the POSIX fallback rather than
        // reusing the ring this test thread created during the probe above.
        let result =
            std::thread::spawn(move || writer.write_owned_entries(&dir_path, writes)).join();
        // Restore before asserting so a failure can't leak the flag to other tests.
        linux_uring::set_runtime_disabled_for_test(false);
        let written = result
            .expect("writer thread panicked")
            .expect("disabled io_uring must fall back to POSIX, not fail");
        assert_eq!(written, 8);
        for i in 0..8 {
            assert_eq!(
                std::fs::read_to_string(dir.path().join(format!("dir/file-{i}.txt"))).unwrap(),
                format!("content {i}\n")
            );
        }
    }

    #[test]
    fn deferred_fresh_indexed_writer_flushes_all_batches() {
        let dir = tempfile::TempDir::new().unwrap();
        let writer = WorktreeWriter::posix();
        let mut total = 0usize;

        for batch in 0..3 {
            let writes = (0..4)
                .map(|i| {
                    let rel = format!("file-{batch}-{i}.txt");
                    let content = format!("batch={batch} file={i}\n").into_bytes();
                    OwnedFileWrite {
                        entry: FileEntry {
                            path: rel.as_bytes().to_vec(),
                            mode: 0o100644,
                            blob_sha1: Vec::new(),
                            fragments: Vec::new(),
                        },
                        content: content.into(),
                    }
                })
                .collect();
            let outcome = writer
                .write_owned_entries_for_fresh_indexed_checkout_deferred(dir.path(), writes)
                .unwrap();
            total += outcome.written;
        }
        total += writer.flush_deferred_writes().unwrap().written;

        assert_eq!(total, 12);
        for batch in 0..3 {
            for i in 0..4 {
                let rel = format!("file-{batch}-{i}.txt");
                let content = format!("batch={batch} file={i}\n");
                assert_eq!(
                    std::fs::read(dir.path().join(rel)).unwrap(),
                    content.as_bytes()
                );
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_writer_writes_regular_file_when_available() {
        let writer = match WorktreeWriter::io_uring() {
            Ok(writer) => writer,
            Err(e) => {
                eprintln!("skipping io_uring smoke test: {e:#}");
                return;
            }
        };
        let dir = tempfile::TempDir::new().unwrap();
        let entry = FileEntry {
            path: b"docs/readme.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: Vec::new(),
            fragments: Vec::new(),
        };

        writer
            .write_entry(dir.path(), &entry, b"hello from io_uring\n")
            .unwrap();

        let path = dir.path().join("docs/readme.txt");
        assert_eq!(std::fs::read(&path).unwrap(), b"hello from io_uring\n");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o644
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_writer_handles_parallel_writes_when_available() {
        let writer = match WorktreeWriter::io_uring() {
            Ok(writer) => std::sync::Arc::new(writer),
            Err(e) => {
                eprintln!("skipping io_uring parallel smoke test: {e:#}");
                return;
            }
        };
        let dir = tempfile::TempDir::new().unwrap();
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let writer = std::sync::Arc::clone(&writer);
                let root = dir.path().to_path_buf();
                scope.spawn(move || {
                    for i in 0..32 {
                        let rel = format!("shard-{worker}/file-{i}.txt");
                        let content = format!("worker={worker} file={i}\n");
                        let entry = FileEntry {
                            path: rel.as_bytes().to_vec(),
                            mode: 0o100644,
                            blob_sha1: Vec::new(),
                            fragments: Vec::new(),
                        };
                        writer
                            .write_entry(&root, &entry, content.as_bytes())
                            .unwrap();
                    }
                });
            }
        });

        for worker in 0..8 {
            for i in 0..32 {
                let rel = format!("shard-{worker}/file-{i}.txt");
                let content = format!("worker={worker} file={i}\n");
                assert_eq!(
                    std::fs::read(dir.path().join(rel)).unwrap(),
                    content.as_bytes()
                );
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_deferred_writer_flushes_all_batches_when_available() {
        let writer = match WorktreeWriter::io_uring() {
            Ok(writer) => writer,
            Err(e) => {
                eprintln!("skipping io_uring deferred smoke test: {e:#}");
                return;
            }
        };
        let dir = tempfile::TempDir::new().unwrap();
        let mut total = 0usize;

        for batch in 0..4 {
            let writes = (0..8)
                .map(|i| {
                    let rel = format!("deferred-{batch}-{i}.txt");
                    let content = format!("batch={batch} file={i}\n").into_bytes();
                    OwnedFileWrite {
                        entry: FileEntry {
                            path: rel.as_bytes().to_vec(),
                            mode: 0o100644,
                            blob_sha1: Vec::new(),
                            fragments: Vec::new(),
                        },
                        content: content.into(),
                    }
                })
                .collect();
            let outcome = writer
                .write_owned_entries_for_fresh_indexed_checkout_deferred(dir.path(), writes)
                .unwrap();
            total += outcome.written;
        }
        total += writer.flush_deferred_writes().unwrap().written;

        assert_eq!(total, 32);
        for batch in 0..4 {
            for i in 0..8 {
                let rel = format!("deferred-{batch}-{i}.txt");
                let content = format!("batch={batch} file={i}\n");
                assert_eq!(
                    std::fs::read(dir.path().join(rel)).unwrap(),
                    content.as_bytes()
                );
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn new_defaults_to_io_uring_on_linux_when_available() {
        // With the env unset, the default backend on Linux should be io_uring
        // (falling back to POSIX only if the kernel can't provide it).
        if std::env::var_os("RIPCLONE_IO_URING").is_some() {
            eprintln!("skipping: RIPCLONE_IO_URING is set in this environment");
            return;
        }
        // Only assert when io_uring is actually constructible on this kernel;
        // otherwise auto() legitimately falls back to POSIX.
        if WorktreeWriter::io_uring().is_ok() {
            assert!(
                WorktreeWriter::new().unwrap().is_io_uring(),
                "new() must default to io_uring on Linux when available"
            );
        }
    }

    // ----- scheduler tests (backend-agnostic: io_uring on Linux, POSIX else) --

    fn sched_config(
        submitters: usize,
        inflight: usize,
        batch_files: usize,
        byte_cap: usize,
    ) -> SchedulerConfig {
        SchedulerConfig {
            submitters,
            inflight,
            batch_files,
            byte_cap,
            flush_timeout: Duration::from_micros(200),
            queue_depth: 4,
        }
    }

    fn archive_opts() -> WriteOptions {
        // Matches the archive write path: dirs created on demand here (the unit
        // test does not pre-create them), no mtime stamp so stats are collected.
        WriteOptions {
            parents_prepared: false,
            stamp_mtime: false,
            fresh_target: false,
        }
    }

    fn regular(path: &str, content: &[u8]) -> OwnedFileWrite {
        OwnedFileWrite {
            entry: FileEntry {
                path: path.as_bytes().to_vec(),
                mode: 0o100644,
                blob_sha1: Vec::new(),
                fragments: Vec::new(),
            },
            content: content.to_vec().into(),
        }
    }

    fn run_scheduler_case(cfg: SchedulerConfig) {
        let dir = tempfile::TempDir::new().unwrap();
        let scheduler =
            WorktreeWriteScheduler::with_config(dir.path().to_path_buf(), archive_opts(), cfg)
                .unwrap();

        let mut total = 0usize;
        let batches = 6;
        let per = 17; // not a multiple of batch_files, to exercise partial windows
        for b in 0..batches {
            let writes: Vec<_> = (0..per)
                .map(|i| {
                    let rel = format!("dir{}/file-{b}-{i}.txt", i % 4);
                    let content = format!("batch={b} file={i} payload payload payload\n");
                    regular(&rel, content.as_bytes())
                })
                .collect();
            total += scheduler.submit(writes).unwrap();
        }
        let outcome = scheduler.flush().unwrap();

        assert_eq!(total, batches * per, "all files accounted for");
        // stamp_mtime=false → every regular file produces a stat.
        assert_eq!(
            outcome.stats.len(),
            batches * per,
            "one stat per regular file"
        );
        for b in 0..batches {
            for i in 0..per {
                let rel = format!("dir{}/file-{b}-{i}.txt", i % 4);
                let content = format!("batch={b} file={i} payload payload payload\n");
                assert_eq!(
                    std::fs::read(dir.path().join(&rel)).unwrap(),
                    content.as_bytes(),
                    "content for {rel}"
                );
            }
        }
    }

    #[test]
    fn scheduler_single_submitter_writes_all() {
        run_scheduler_case(sched_config(1, 2, 8, 1 << 20));
    }

    #[test]
    fn scheduler_multi_submitter_writes_all() {
        run_scheduler_case(sched_config(4, 2, 8, 1 << 20));
    }

    #[test]
    fn scheduler_inflight_one_writes_all() {
        run_scheduler_case(sched_config(2, 1, 8, 1 << 20));
    }

    #[test]
    fn scheduler_inflight_four_writes_all() {
        run_scheduler_case(sched_config(2, 4, 8, 1 << 20));
    }

    #[test]
    fn scheduler_deep_overlap_cycles_slot_ranges() {
        // One submitter, max overlap, small windows, and far more windows than
        // the overlap depth — forces the harvest-front / slot-range-reuse path
        // to cycle many times.
        let dir = tempfile::TempDir::new().unwrap();
        let scheduler = WorktreeWriteScheduler::with_config(
            dir.path().to_path_buf(),
            archive_opts(),
            sched_config(1, 4, 8, 1 << 20),
        )
        .unwrap();
        let n = 300;
        let writes: Vec<_> = (0..n)
            .map(|i| {
                regular(
                    &format!("deep/f{i:04}.txt"),
                    format!("payload-{i}").as_bytes(),
                )
            })
            .collect();
        let written = scheduler.submit(writes).unwrap();
        let outcome = scheduler.flush().unwrap();
        assert_eq!(written, n);
        assert_eq!(outcome.stats.len(), n);
        for i in 0..n {
            assert_eq!(
                std::fs::read(dir.path().join(format!("deep/f{i:04}.txt"))).unwrap(),
                format!("payload-{i}").as_bytes()
            );
        }
    }

    #[test]
    fn scheduler_tiny_byte_cap_forces_small_windows() {
        // byte_cap smaller than one file → every window is a single file, but
        // everything must still land.
        run_scheduler_case(sched_config(2, 2, 512, 1));
    }

    #[test]
    fn scheduler_large_batch_spanning_window_cap_writes_all() {
        // A single submit larger than the window cap must split into windows.
        let dir = tempfile::TempDir::new().unwrap();
        let scheduler = WorktreeWriteScheduler::with_config(
            dir.path().to_path_buf(),
            archive_opts(),
            sched_config(1, 2, 16, 1 << 20),
        )
        .unwrap();
        let writes: Vec<_> = (0..200)
            .map(|i| regular(&format!("flat/f{i}.txt"), format!("v{i}").as_bytes()))
            .collect();
        let written = scheduler.submit(writes).unwrap();
        let outcome = scheduler.flush().unwrap();
        assert_eq!(written, 200);
        assert_eq!(outcome.stats.len(), 200);
        for i in 0..200 {
            assert_eq!(
                std::fs::read(dir.path().join(format!("flat/f{i}.txt"))).unwrap(),
                format!("v{i}").as_bytes()
            );
        }
    }

    #[test]
    fn scheduler_materializes_symlinks_inline() {
        let dir = tempfile::TempDir::new().unwrap();
        let scheduler = WorktreeWriteScheduler::with_config(
            dir.path().to_path_buf(),
            archive_opts(),
            sched_config(2, 2, 8, 1 << 20),
        )
        .unwrap();
        let mut writes = vec![regular("real.txt", b"hello")];
        writes.push(OwnedFileWrite {
            entry: FileEntry {
                path: b"link.txt".to_vec(),
                mode: 0o120000,
                blob_sha1: Vec::new(),
                fragments: Vec::new(),
            },
            content: b"real.txt".to_vec().into(),
        });
        let written = scheduler.submit(writes).unwrap();
        scheduler.flush().unwrap();
        assert_eq!(written, 2, "symlink + regular both counted");
        assert_eq!(
            std::fs::read(dir.path().join("real.txt")).unwrap(),
            b"hello"
        );
        #[cfg(unix)]
        {
            let meta = std::fs::symlink_metadata(dir.path().join("link.txt")).unwrap();
            assert!(meta.file_type().is_symlink(), "link.txt is a symlink");
            assert_eq!(
                std::fs::read_link(dir.path().join("link.txt")).unwrap(),
                std::path::Path::new("real.txt")
            );
        }
    }

    #[test]
    fn scheduler_rejects_unsafe_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let scheduler = WorktreeWriteScheduler::with_config(
            dir.path().to_path_buf(),
            archive_opts(),
            sched_config(1, 2, 8, 1 << 20),
        )
        .unwrap();
        let err = scheduler
            .submit(vec![regular("../escape.txt", b"nope")])
            .unwrap_err();
        assert!(
            err.to_string().contains("unsafe path") || format!("{err:#}").contains("parent-dir"),
            "expected path-validation error, got: {err:#}"
        );
        // Nothing should have escaped the target dir.
        assert!(!dir.path().parent().unwrap().join("escape.txt").exists());
    }

    #[test]
    fn scheduler_empty_submit_and_flush_are_noops() {
        let dir = tempfile::TempDir::new().unwrap();
        let scheduler = WorktreeWriteScheduler::with_config(
            dir.path().to_path_buf(),
            archive_opts(),
            sched_config(2, 2, 8, 1 << 20),
        )
        .unwrap();
        assert_eq!(scheduler.submit(Vec::new()).unwrap(), 0);
        let outcome = scheduler.flush().unwrap();
        assert_eq!(outcome.written, 0);
        assert!(outcome.stats.is_empty());
    }

    #[test]
    fn scheduler_concurrent_producers_share_dir_without_eexist() {
        // Many producers create files under the SAME parent directory with
        // parents_prepared=false, so safe_create_dir_all races on the shared
        // dir. Must not fail with EEXIST.
        let dir = tempfile::TempDir::new().unwrap();
        let scheduler = std::sync::Arc::new(
            WorktreeWriteScheduler::with_config(
                dir.path().to_path_buf(),
                archive_opts(),
                sched_config(3, 2, 8, 1 << 20),
            )
            .unwrap(),
        );
        std::thread::scope(|scope| {
            for w in 0..8 {
                let scheduler = std::sync::Arc::clone(&scheduler);
                scope.spawn(move || {
                    let writes: Vec<_> = (0..25)
                        .map(|i| {
                            // All under one shared/nested directory.
                            regular(
                                &format!("shared/nested/w{w}_f{i}.txt"),
                                format!("w{w}f{i}").as_bytes(),
                            )
                        })
                        .collect();
                    scheduler.submit(writes).unwrap();
                });
            }
        });
        let outcome = scheduler.flush().unwrap();
        assert_eq!(outcome.stats.len(), 8 * 25);
        for w in 0..8 {
            for i in 0..25 {
                let rel = format!("shared/nested/w{w}_f{i}.txt");
                assert_eq!(
                    std::fs::read(dir.path().join(&rel)).unwrap(),
                    format!("w{w}f{i}").as_bytes()
                );
            }
        }
    }

    #[test]
    fn scheduler_concurrent_producers_write_all() {
        let dir = tempfile::TempDir::new().unwrap();
        let scheduler = std::sync::Arc::new(
            WorktreeWriteScheduler::with_config(
                dir.path().to_path_buf(),
                archive_opts(),
                sched_config(3, 2, 8, 1 << 20),
            )
            .unwrap(),
        );
        std::thread::scope(|scope| {
            for w in 0..6 {
                let scheduler = std::sync::Arc::clone(&scheduler);
                scope.spawn(move || {
                    for chunk in 0..4 {
                        let writes: Vec<_> = (0..10)
                            .map(|i| {
                                regular(
                                    &format!("w{w}/c{chunk}/f{i}.txt"),
                                    format!("w{w}c{chunk}f{i}").as_bytes(),
                                )
                            })
                            .collect();
                        scheduler.submit(writes).unwrap();
                    }
                });
            }
        });
        let outcome = scheduler.flush().unwrap();
        assert_eq!(outcome.stats.len(), 6 * 4 * 10);
        for w in 0..6 {
            for chunk in 0..4 {
                for i in 0..10 {
                    let rel = format!("w{w}/c{chunk}/f{i}.txt");
                    assert_eq!(
                        std::fs::read(dir.path().join(&rel)).unwrap(),
                        format!("w{w}c{chunk}f{i}").as_bytes(),
                        "content for {rel}"
                    );
                }
            }
        }
    }
}
