use crate::manifest::FileEntry;
use anyhow::{Context, Result};
use filetime::{FileTime, set_file_mtime, set_symlink_file_times};
use std::path::{Path, PathBuf};

pub(crate) const INDEX_MTIME: FileTime = FileTime::from_unix_time(1, 0);

/// Convert a raw path byte slice to a `Path`. On Unix this preserves arbitrary
/// git path bytes; on other platforms we fall back to UTF-8.
pub(crate) fn path_from_bytes(bytes: &[u8]) -> &Path {
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
            if !current.exists() {
                std::fs::create_dir(&current)
                    .with_context(|| format!("create dir {}", current.display()))?;
            } else if !current.is_dir() {
                anyhow::bail!("path is not a directory: {}", current.display());
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct WorktreeWriter {
    backend: WriterBackend,
}

pub(crate) struct OwnedFileWrite {
    pub(crate) entry: FileEntry,
    pub(crate) content: Vec<u8>,
}

struct PreparedRegularWrite {
    target: PathBuf,
    mode: u32,
    content: Vec<u8>,
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
            Err(_) => IoUringMode::Disabled,
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

    fn io_uring() -> Result<Self> {
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

    pub(crate) fn write_owned_entries(
        &self,
        target_dir: &Path,
        writes: Vec<OwnedFileWrite>,
    ) -> Result<usize> {
        if writes.is_empty() {
            return Ok(0);
        }

        let mut regulars = Vec::new();
        let mut written = 0usize;
        for write in writes {
            let path = path_from_bytes(&write.entry.path);
            validate_relative_path(path)
                .with_context(|| format!("refusing to extract unsafe path: {}", path.display()))?;

            if let Some(parent) = path.parent()
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
            if target.is_symlink() {
                std::fs::remove_file(&target)
                    .with_context(|| format!("remove existing symlink {}", target.display()))?;
            }

            match write.entry.mode {
                0o120000 => {
                    write_symlink_entry(&target, &write.entry.path, &write.content)?;
                    written += 1;
                }
                0o100755 | 0o100644 => {
                    if target.exists() {
                        std::fs::remove_file(&target).ok();
                    }
                    let mode = if write.entry.mode == 0o100755 {
                        0o755
                    } else {
                        0o644
                    };
                    regulars.push(PreparedRegularWrite {
                        target,
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

        self.write_regular_batch(regulars)?;
        Ok(written)
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
            WriterBackend::Posix => write_regular_posix(target, mode, content.as_slice()),
            #[cfg(target_os = "linux")]
            WriterBackend::IoUring(writer) => match content {
                RegularContent::Borrowed(content) => writer.write_regular(target, mode, content),
                RegularContent::Owned(content) => writer.write_regular_owned(target, mode, content),
            },
        }
    }

    fn write_regular_batch(&self, writes: Vec<PreparedRegularWrite>) -> Result<()> {
        match &self.backend {
            WriterBackend::Posix => {
                for write in writes {
                    write_regular_posix(&write.target, write.mode, &write.content)?;
                    set_file_mtime(&write.target, INDEX_MTIME)
                        .with_context(|| format!("set mtime {}", write.target.display()))?;
                }
                Ok(())
            }
            #[cfg(target_os = "linux")]
            WriterBackend::IoUring(writer) => {
                let targets: Vec<_> = writes.iter().map(|write| write.target.clone()).collect();
                writer.write_regular_batch(writes)?;
                for target in targets {
                    set_file_mtime(&target, INDEX_MTIME)
                        .with_context(|| format!("set mtime {}", target.display()))?;
                }
                Ok(())
            }
        }
    }
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

fn write_regular_posix(target: &Path, mode: u32, content: &[u8]) -> Result<()> {
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
    }
    #[cfg(not(unix))]
    {
        std::fs::write(target, content).with_context(|| format!("write {}", target.display()))?;
    }
    Ok(())
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
    use std::time::{SystemTime, UNIX_EPOCH};

    const QUEUE_DEPTH: u32 = 1024;
    const MAX_BATCH_FILES: usize = 256;
    static DIRECT_DESCRIPTOR_ENABLED_LOG: Once = Once::new();
    static DIRECT_DESCRIPTOR_FALLBACK_LOG: Once = Once::new();

    #[derive(Clone, Copy)]
    pub(super) struct UringWriter;

    struct RawUringWriter {
        ring: IoUring,
        descriptor_mode: DescriptorMode,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum DescriptorMode {
        NormalFd,
        DirectFd,
    }

    struct InFlightWrite {
        target: PathBuf,
        path: CString,
        flags: i32,
        mode: libc::mode_t,
        content: Vec<u8>,
        fd: Option<i32>,
        open_res: Option<i32>,
        write_res: Option<i32>,
        close_res: Option<i32>,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FileOp {
        Open = 0,
        Write = 1,
        Close = 2,
    }

    thread_local! {
        static THREAD_WRITER: RefCell<Option<RawUringWriter>> = const { RefCell::new(None) };
    }

    impl UringWriter {
        pub(super) fn new() -> Result<Self> {
            RawUringWriter::new().map(|_| Self)
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
            with_thread_writer(|writer| {
                writer.write_regular_batch(vec![PreparedRegularWrite {
                    target: target.to_path_buf(),
                    mode,
                    content,
                }])
            })
            .with_context(|| format!("write {}", target.display()))
        }

        pub(super) fn write_regular_batch(&self, writes: Vec<PreparedRegularWrite>) -> Result<()> {
            if writes.is_empty() {
                return Ok(());
            }
            with_thread_writer(|writer| writer.write_regular_batch(writes))
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
                *writer = Some(RawUringWriter::new()?);
            }
            f(writer
                .as_mut()
                .expect("thread-local io_uring writer initialized"))
        })
    }

    impl RawUringWriter {
        fn new() -> Result<Self> {
            let ring = IoUring::new(QUEUE_DEPTH).context("initialize io_uring queue")?;
            let descriptor_mode = match ring
                .submitter()
                .register_files_sparse(MAX_BATCH_FILES as u32)
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
            Ok(Self {
                ring,
                descriptor_mode,
            })
        }

        fn write_regular_batch(&mut self, mut writes: Vec<PreparedRegularWrite>) -> Result<()> {
            while !writes.is_empty() {
                let n = writes.len().min(MAX_BATCH_FILES);
                let batch: Vec<_> = writes.drain(..n).collect();
                self.write_regular_window(batch)?;
            }
            Ok(())
        }

        fn write_regular_window(&mut self, writes: Vec<PreparedRegularWrite>) -> Result<()> {
            if writes.is_empty() {
                return Ok(());
            }

            let mut in_flight = Vec::with_capacity(writes.len());
            for write in writes {
                if write.content.len() > u32::MAX as usize {
                    anyhow::bail!(
                        "file too large for single io_uring write: {}",
                        write.target.display()
                    );
                }
                let path =
                    CString::new(write.target.as_os_str().as_bytes()).with_context(|| {
                        format!("path contains NUL byte: {}", write.target.display())
                    })?;
                let flags = libc::O_WRONLY
                    | libc::O_CREAT
                    | libc::O_TRUNC
                    | libc::O_CLOEXEC
                    | libc::O_NOFOLLOW;
                in_flight.push(InFlightWrite {
                    target: write.target,
                    path,
                    flags,
                    mode: write.mode as libc::mode_t,
                    content: write.content,
                    fd: None,
                    open_res: None,
                    write_res: None,
                    close_res: None,
                });
            }

            match self.descriptor_mode {
                DescriptorMode::NormalFd => self.write_regular_window_normal(in_flight),
                DescriptorMode::DirectFd => match self.write_regular_window_direct(in_flight) {
                    Ok(()) => Ok(()),
                    Err(DirectWriteError::Unsupported(in_flight)) => {
                        DIRECT_DESCRIPTOR_FALLBACK_LOG.call_once(|| {
                            tracing::info!(
                                "io_uring direct descriptors rejected by kernel; retrying with normal fds"
                            )
                        });
                        self.descriptor_mode = DescriptorMode::NormalFd;
                        self.write_regular_window_normal(in_flight)
                    }
                    Err(DirectWriteError::Other(e)) => Err(e),
                },
            }
        }

        fn write_regular_window_normal(&mut self, mut in_flight: Vec<InFlightWrite>) -> Result<()> {
            let mut entries = Vec::with_capacity(in_flight.len());
            for (idx, write) in in_flight.iter().enumerate() {
                entries.push(
                    opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), write.path.as_ptr())
                        .flags(write.flags)
                        .mode(write.mode)
                        .build()
                        .user_data(user_data(idx, FileOp::Open)),
                );
            }
            self.submit_entries(&entries, "submit io_uring open batch")?;
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
                        entries.push(
                            opcode::Write::new(
                                types::Fd(fd),
                                write.content.as_ptr(),
                                write.content.len() as u32,
                            )
                            .offset(0)
                            .build()
                            .flags(squeue::Flags::IO_HARDLINK)
                            .user_data(user_data(idx, FileOp::Write)),
                        );
                    }
                    entries.push(
                        opcode::Close::new(types::Fd(fd))
                            .build()
                            .user_data(user_data(idx, FileOp::Close)),
                    );
                }
            }
            if !entries.is_empty() {
                if let Err(e) = self.submit_entries(&entries, "submit io_uring write/close batch") {
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
                                FileOp::Open => {
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
            Ok(())
        }

        fn write_regular_window_direct(
            &mut self,
            mut in_flight: Vec<InFlightWrite>,
        ) -> std::result::Result<(), DirectWriteError> {
            let mut entries = Vec::with_capacity(in_flight.len() * 3);
            for idx in 0..in_flight.len() {
                let write = &in_flight[idx];
                let slot = idx as u32;
                let dest = types::DestinationSlot::try_from_slot_target(slot).map_err(|_| {
                    DirectWriteError::Other(anyhow::anyhow!("invalid fixed file slot {slot}"))
                })?;
                entries.push(
                    opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), write.path.as_ptr())
                        .flags(write.flags & !libc::O_CLOEXEC)
                        .mode(write.mode)
                        .file_index(Some(dest))
                        .build()
                        .flags(squeue::Flags::IO_LINK)
                        .user_data(user_data(idx, FileOp::Open)),
                );
                if write.content.is_empty() {
                    in_flight[idx].write_res = Some(0);
                } else {
                    entries.push(
                        opcode::Write::new(
                            types::Fixed(slot),
                            write.content.as_ptr(),
                            write.content.len() as u32,
                        )
                        .offset(0)
                        .build()
                        .flags(squeue::Flags::IO_HARDLINK)
                        .user_data(user_data(idx, FileOp::Write)),
                    );
                }
                entries.push(
                    opcode::Close::new(types::Fixed(slot))
                        .build()
                        .user_data(user_data(idx, FileOp::Close)),
                );
            }

            self.submit_entries(&entries, "submit io_uring direct open/write/close batch")
                .map_err(DirectWriteError::Other)?;
            let completions = self
                .collect_completions(
                    entries.len(),
                    "wait for io_uring direct open/write/close batch completion",
                )
                .map_err(DirectWriteError::Other)?;
            for (idx, op, res) in completions {
                let write = in_flight
                    .get_mut(idx)
                    .ok_or_else(|| anyhow::anyhow!("invalid io_uring completion index {idx}"))
                    .map_err(DirectWriteError::Other)?;
                match op {
                    FileOp::Open => {
                        write.open_res = Some(res);
                    }
                    FileOp::Write => {
                        write.write_res = Some(res);
                    }
                    FileOp::Close => {
                        write.close_res = Some(res);
                    }
                }
            }

            if direct_descriptors_unsupported(&in_flight) {
                return Err(DirectWriteError::Unsupported(in_flight));
            }

            for write in &in_flight {
                check_open_result(write).map_err(DirectWriteError::Other)?;
                check_write_result(write).map_err(DirectWriteError::Other)?;
                check_close_result(write).map_err(DirectWriteError::Other)?;
            }
            Ok(())
        }

        fn submit_entries(&mut self, entries: &[squeue::Entry], context: &str) -> Result<()> {
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
            self.ring
                .submitter()
                .submit_and_wait(1)
                .with_context(|| context.to_string())?;
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
                        let (idx, op) = parse_user_data(cqe.user_data())?;
                        completions.push((idx, op, cqe.result()));
                    }
                }
                if completions.len() < expected {
                    self.ring
                        .submitter()
                        .submit_and_wait(1)
                        .with_context(|| context.to_string())?;
                }
            }
            Ok(completions)
        }
    }

    fn user_data(idx: usize, op: FileOp) -> u64 {
        ((idx as u64) << 2) | op as u64
    }

    enum DirectWriteError {
        Unsupported(Vec<InFlightWrite>),
        Other(anyhow::Error),
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

    fn parse_user_data(data: u64) -> Result<(usize, FileOp)> {
        let idx = (data >> 2) as usize;
        let op = match data & 0b11 {
            0 => FileOp::Open,
            1 => FileOp::Write,
            2 => FileOp::Close,
            other => anyhow::bail!("invalid io_uring completion op {other}"),
        };
        Ok((idx, op))
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
}
