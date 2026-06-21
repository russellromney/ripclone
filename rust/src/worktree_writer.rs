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
                        "io_uring writer unavailable; falling back to POSIX writer: {e}"
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
    use crossbeam_channel::bounded;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;

    enum UringRequest {
        Write(WriteRequest),
        WriteBatch(WriteBatchRequest),
        Shutdown(crossbeam_channel::Sender<()>),
    }

    struct WriteRequest {
        target: PathBuf,
        mode: u32,
        content: Vec<u8>,
        reply: crossbeam_channel::Sender<Result<()>>,
    }

    struct WriteBatchRequest {
        writes: Vec<PreparedRegularWrite>,
        reply: crossbeam_channel::Sender<Result<()>>,
    }

    #[derive(Clone)]
    pub(super) struct UringWriter {
        inner: Arc<UringInner>,
    }

    struct UringInner {
        tx: mpsc::UnboundedSender<UringRequest>,
        handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    }

    impl UringWriter {
        pub(super) fn new() -> Result<Self> {
            let (tx, mut rx) = mpsc::unbounded_channel::<UringRequest>();
            let (ready_tx, ready_rx) = bounded::<Result<()>>(1);

            let handle = std::thread::Builder::new()
                .name("ripclone-io-uring-writer".to_string())
                .spawn(move || {
                    let ready_for_runtime = ready_tx.clone();
                    let result = catch_unwind(AssertUnwindSafe(move || {
                        tokio_uring::builder().entries(256).start(async move {
                            let _ = ready_for_runtime.send(Ok(()));
                            while let Some(req) = rx.recv().await {
                                match req {
                                    UringRequest::Write(req) => {
                                        tokio_uring::spawn(async move {
                                            let result = write_regular_uring(
                                                req.target,
                                                req.mode,
                                                req.content,
                                            )
                                            .await;
                                            let _ = req.reply.send(result);
                                        });
                                    }
                                    UringRequest::WriteBatch(req) => {
                                        tokio_uring::spawn(async move {
                                            let result =
                                                write_regular_batch_uring(req.writes).await;
                                            let _ = req.reply.send(result);
                                        });
                                    }
                                    UringRequest::Shutdown(reply) => {
                                        let _ = reply.send(());
                                        break;
                                    }
                                }
                            }
                        });
                    }));
                    if result.is_err() {
                        let _ = ready_tx.send(Err(anyhow::anyhow!(
                            "io_uring runtime initialization panicked"
                        )));
                    }
                })
                .context("spawn io_uring writer thread")?;

            match ready_rx.recv_timeout(Duration::from_secs(2)) {
                Ok(Ok(())) => Ok(Self {
                    inner: Arc::new(UringInner {
                        tx,
                        handle: Mutex::new(Some(handle)),
                    }),
                }),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(anyhow::anyhow!("io_uring writer did not start: {e}")),
            }
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

            let (reply_tx, reply_rx) = bounded(1);
            self.inner
                .tx
                .send(UringRequest::Write(WriteRequest {
                    target: target.to_path_buf(),
                    mode,
                    content,
                    reply: reply_tx,
                }))
                .map_err(|_| anyhow::anyhow!("io_uring writer thread stopped"))?;
            reply_rx
                .recv()
                .context("receive io_uring write result")?
                .with_context(|| format!("write {}", target.display()))
        }

        pub(super) fn write_regular_batch(&self, writes: Vec<PreparedRegularWrite>) -> Result<()> {
            if writes.is_empty() {
                return Ok(());
            }

            let (reply_tx, reply_rx) = bounded(1);
            self.inner
                .tx
                .send(UringRequest::WriteBatch(WriteBatchRequest {
                    writes,
                    reply: reply_tx,
                }))
                .map_err(|_| anyhow::anyhow!("io_uring writer thread stopped"))?;
            reply_rx
                .recv()
                .context("receive io_uring batch write result")?
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

    impl Drop for UringInner {
        fn drop(&mut self) {
            let (reply_tx, reply_rx) = bounded(1);
            let _ = self.tx.send(UringRequest::Shutdown(reply_tx));
            let _ = reply_rx.recv_timeout(Duration::from_secs(2));
            if let Some(handle) = self.handle.lock().ok().and_then(|mut h| h.take()) {
                let _ = handle.join();
            }
        }
    }

    async fn write_regular_batch_uring(writes: Vec<PreparedRegularWrite>) -> Result<()> {
        let mut tasks = Vec::with_capacity(writes.len());
        for write in writes {
            tasks.push(tokio_uring::spawn(write_regular_uring(
                write.target,
                write.mode,
                write.content,
            )));
        }
        for task in tasks {
            task.await??;
        }
        Ok(())
    }

    async fn write_regular_uring(target: PathBuf, mode: u32, content: Vec<u8>) -> Result<()> {
        use std::os::unix::fs::OpenOptionsExt;

        let mut opts = tokio_uring::fs::OpenOptions::new();
        opts.write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .custom_flags(libc::O_NOFOLLOW);
        let file = opts
            .open(&target)
            .await
            .with_context(|| format!("open {}", target.display()))?;
        let (res, _content) = file.write_all_at(content, 0).await;
        res.with_context(|| format!("write {}", target.display()))?;
        file.close()
            .await
            .with_context(|| format!("close {}", target.display()))?;
        Ok(())
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
