use crate::git;
use crate::manifest::{FileEntry, FrameInfo, MetadataChunk as Manifest};
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use filetime::{FileTime, set_file_mtime, set_symlink_file_times};
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha256Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::info;

const INDEX_MTIME: FileTime = FileTime::from_unix_time(1, 0);

/// Check whether per-blob SHA-1 verification should be skipped.
///
/// **Unsafe:** archive chunk hashes are still verified, and `git index-pack`
/// recomputes object hashes when the local pack is built, but skipping the
/// per-blob check means a malicious or buggy server could serve wrong content
/// that still passes chunk-level checks. Only use this when you fully trust the
/// server and need the extra CPU savings.
fn skip_sha1_verify() -> bool {
    std::env::var_os("RIPCLONE_UNSAFE_SKIP_SHA1_VERIFY").is_some()
}

/// Convert a manifest blob_sha1 slice to a fixed 20-byte array.
fn blob_sha1_to_array(sha1: &[u8]) -> Result<[u8; 20]> {
    sha1.try_into()
        .map_err(|_| anyhow::anyhow!("manifest blob_sha1 must be 20 bytes, got {}", sha1.len()))
}

/// Convert a raw path byte slice to a `Path`. On Unix this preserves arbitrary
/// git path bytes; on other platforms we fall back to UTF-8.
fn path_from_bytes(bytes: &[u8]) -> &std::path::Path {
    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        std::path::Path::new(OsStr::from_bytes(bytes))
    }
    #[cfg(not(unix))]
    {
        let s = std::str::from_utf8(bytes).unwrap_or("<invalid utf8 path>");
        std::path::Path::new(s)
    }
}

struct PendingFile {
    fragments: Vec<Option<Vec<u8>>>,
    remaining: usize,
}

pub struct ExtractStats {
    pub files: usize,
    pub raw_bytes: u64,
}

/// A consecutive group of archive frames fetched in a single HTTP range request.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Index of the archive chunk this range lives in.
    pub chunk_index: usize,
    /// Inclusive start frame index.
    pub start_frame: usize,
    /// Exclusive end frame index.
    pub end_frame: usize,
    /// Byte offset of the first frame within the archive chunk.
    pub byte_start: u64,
    /// Byte offset one past the last frame within the archive chunk.
    pub byte_end: u64,
}

impl Chunk {
    pub fn compressed_len(&self) -> u64 {
        self.byte_end - self.byte_start
    }
}

/// Group consecutive frames into chunks whose total compressed size is at most
/// `chunk_size`. A frame larger than `chunk_size` gets its own chunk.
fn compute_chunks(frames: &[FrameInfo], chunk_size: u64) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut byte_start = 0u64;
    let mut current_len = 0u64;
    let mut current_chunk_index = frames.first().map(|f| f.chunk_index as usize).unwrap_or(0);

    for (i, frame) in frames.iter().enumerate() {
        let frame_len = frame.compressed_len as u64;
        let frame_chunk = frame.chunk_index as usize;
        // Start a new chunk when crossing into a different archive chunk or when
        // the current frame would not fit.
        if frame_chunk != current_chunk_index
            || (current_len > 0 && current_len + frame_len > chunk_size)
        {
            let end = i;
            chunks.push(Chunk {
                chunk_index: current_chunk_index,
                start_frame: start,
                end_frame: end,
                byte_start,
                byte_end: byte_start + current_len,
            });
            start = i;
            byte_start = frame.chunk_offset;
            current_len = 0;
            current_chunk_index = frame_chunk;
        }
        current_len += frame_len;
    }

    // Ensure every frame is covered, including zero-length frames produced by
    // empty files. A chunk with zero compressed bytes still carries the empty
    // frames to the writer so the files are created.
    if !frames.is_empty() {
        chunks.push(Chunk {
            chunk_index: current_chunk_index,
            start_frame: start,
            end_frame: frames.len(),
            byte_start,
            byte_end: byte_start + current_len,
        });
    }

    chunks
}

/// Extract a working-tree archive into `target_dir` using the supplied manifest
/// and a local archive file.
///
/// `target_dir` must already contain a skeleton `.git` with skip-worktree set
/// on all tracked paths. After extraction those paths are cleared.
///
/// If `git_dir` is `Some`, every verified blob is also written into
/// `.git/objects/pack` as a locally-built packfile.
pub fn extract_archive(
    archive_path: &Path,
    manifest_path: &Path,
    target_dir: &Path,
    git_dir: Option<&Path>,
    dictionary: Option<&[u8]>,
) -> Result<ExtractStats> {
    let mut archive_file = File::open(archive_path)
        .with_context(|| format!("open archive {}", archive_path.display()))?;
    let mut archive_data = Vec::new();
    archive_file
        .read_to_end(&mut archive_data)
        .context("read archive")?;
    let archive = Arc::new(archive_data);

    // For a local archive the whole object is already in memory, so slice it
    // into several chunks and let the fetcher/writer pools parallelize.
    extract_archive_with_chunk_fetcher(
        manifest_path,
        target_dir,
        git_dir,
        dictionary,
        DEFAULT_LOCAL_CHUNK_SIZE,
        move |chunk: &Chunk| {
            let start = chunk.byte_start as usize;
            let end = chunk.byte_end as usize;
            if end > archive.len() {
                anyhow::bail!("chunk {:?} extends past archive end", chunk);
            }
            Ok(archive[start..end].to_vec())
        },
    )
}

/// Extract a working-tree archive using a caller-provided chunk fetcher.
///
/// This uses a small pipeline:
///   - a fetch pool with roughly one thread per CPU core minus one, pulling
///     chunk jobs from a bounded queue and issuing HTTP range requests
///   - a write/decompress pool with the same size, consuming compressed frames,
///     decompressing them, and writing the files for each frame
///
/// Frames are grouped into chunks of at most `chunk_size` compressed bytes so
/// that a single range request can satisfy several frames. On high-latency
/// links this dramatically reduces the number of round-trips.
///
/// If `git_dir` is `Some`, every verified blob is also collected and written
/// into `.git/objects/pack` as a locally-built packfile. This lets a single
/// archive download satisfy both the working tree and the git object store.
pub fn extract_archive_with_chunk_fetcher<F>(
    manifest_path: &Path,
    target_dir: &Path,
    git_dir: Option<&Path>,
    dictionary: Option<&[u8]>,
    chunk_size: u64,
    fetch_chunk: F,
) -> Result<ExtractStats>
where
    F: Fn(&Chunk) -> Result<Vec<u8>> + Send + Sync + 'static,
{
    let fetch_start = Instant::now();
    let mut manifest_file = File::open(manifest_path)
        .with_context(|| format!("open manifest {}", manifest_path.display()))?;
    let mut manifest_bytes = Vec::new();
    manifest_file
        .read_to_end(&mut manifest_bytes)
        .context("read manifest")?;
    let manifest = Manifest::read(&mut manifest_bytes.as_slice())?;

    // Validate every path before creating any directories, then create parents
    // safely (refusing symlinks and parent-dir escapes).
    for entry in manifest.files.iter() {
        validate_relative_path(path_from_bytes(&entry.path)).with_context(|| {
            format!(
                "invalid manifest path: {}",
                String::from_utf8_lossy(&entry.path)
            )
        })?;
    }
    let dirs: HashSet<PathBuf> = manifest
        .files
        .iter()
        .filter_map(|e| {
            let p = path_from_bytes(&e.path);
            let parent = p.parent()?;
            if parent.as_os_str().is_empty() {
                return None;
            }
            Some(parent.to_path_buf())
        })
        .collect();
    let mut dirs: Vec<_> = dirs.into_iter().collect();
    dirs.sort();
    for dir in dirs {
        safe_create_dir_all(target_dir, &dir)
            .with_context(|| format!("create dir {}", dir.display()))?;
    }

    let target_dir = target_dir.to_path_buf();
    let manifest = Arc::new(manifest);

    // If the caller wants a local blob pack, spawn a background builder thread.
    // It receives (sha1, content) pairs over a bounded channel and writes them
    // to a temp pack file, so peak memory stays bounded.
    let expected_blob_count = git_dir.map(|_| {
        let mut unique: HashSet<[u8; 20]> = HashSet::new();
        for entry in manifest.files.iter() {
            let sha1: [u8; 20] = entry
                .blob_sha1
                .as_slice()
                .try_into()
                .expect("manifest blob_sha1 must be 20 bytes");
            unique.insert(sha1);
        }
        unique.len()
    });
    let (blob_pack_tx, blob_pack_handle): (
        Option<crossbeam_channel::Sender<crate::blob_pack::BlobPackInput>>,
        Option<std::thread::JoinHandle<Result<PathBuf>>>,
    ) = if let Some(git_dir) = git_dir {
        let expected = expected_blob_count.expect("expected count present with git_dir");
        let (tx, handle) = crate::blob_pack::spawn_blob_pack_builder(git_dir, expected)
            .context("spawn blob pack builder")?;
        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    // Group fragments by frame so each writer thread can write every file
    // slice that belongs to a given decompressed frame.
    let fragments_by_frame = Arc::new(manifest.fragments_by_frame());

    // Files split across multiple frames need all fragments assembled before
    // writing. Most files are single-fragment, so this map is small.
    let mut pending_files: HashMap<usize, PendingFile> = HashMap::new();
    for (file_idx, entry) in manifest.files.iter().enumerate() {
        if entry.fragments.len() > 1 {
            pending_files.insert(
                file_idx,
                PendingFile {
                    fragments: vec![None; entry.fragments.len()],
                    remaining: entry.fragments.len(),
                },
            );
        }
    }
    let pending_files = Arc::new(Mutex::new(pending_files));

    let chunks = compute_chunks(&manifest.frames, chunk_size);

    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    // Launch roughly one fetcher per core minus one so high-latency links can
    // keep a large number of HTTP range requests in flight. Writers decompress
    // and write files; give them the same count so fast networks don't stall
    // behind a single writer while fetchers are idle.
    let fetch_threads = std::env::var("RIPCLONE_FETCH_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| (num_cpus - 1).max(1));
    let write_threads = std::env::var("RIPCLONE_WRITE_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| (num_cpus - 1).max(1));
    // Use a small bounded queue so we don't buffer the whole archive in memory.
    let queue_depth = (fetch_threads * 2).max(write_threads * 2);
    info!(
        "extracting {} files across {} frames in {} chunks (fetch_threads={}, write_threads={}, queue_depth={})",
        manifest.files.len(),
        manifest.frames.len(),
        chunks.len(),
        fetch_threads,
        write_threads,
        queue_depth
    );

    let (job_tx, job_rx): (Sender<Chunk>, Receiver<Chunk>) = bounded(queue_depth);
    let (compressed_tx, compressed_rx): (
        Sender<(usize, Result<Vec<u8>>)>,
        Receiver<(usize, Result<Vec<u8>>)>,
    ) = bounded(queue_depth);
    let (done_tx, done_rx): (Sender<Result<usize>>, Receiver<Result<usize>>) =
        bounded(manifest.frames.len());

    let fetcher = Arc::new(fetch_chunk);
    let dictionary = dictionary.map(|d| Arc::new(d.to_vec()));

    // Spawn fetcher threads: they pull chunk jobs, fetch the byte range, slice
    // it into per-frame compressed buffers, and push those to the writer pool.
    for _ in 0..fetch_threads {
        let fetcher = fetcher.clone();
        let job_rx: Receiver<Chunk> = job_rx.clone();
        let compressed_tx: Sender<(usize, Result<Vec<u8>>)> = compressed_tx.clone();
        let manifest2 = manifest.clone();
        std::thread::spawn(move || {
            while let Ok(chunk) = job_rx.recv() {
                let chunk_bytes: Result<Vec<u8>> = fetcher(&chunk).with_context(|| {
                    format!("fetch chunk bytes={}-{}", chunk.byte_start, chunk.byte_end)
                });
                match chunk_bytes {
                    Ok(bytes) => {
                        for idx in chunk.start_frame..chunk.end_frame {
                            let frame = &manifest2.frames[idx];
                            let off = (frame.chunk_offset - chunk.byte_start) as usize;
                            let len = frame.compressed_len as usize;
                            let res = if off + len > bytes.len() {
                                Err(anyhow::anyhow!(
                                    "frame {} (off={} len={}) out of chunk bounds (len={})",
                                    idx,
                                    off,
                                    len,
                                    bytes.len()
                                ))
                            } else {
                                Ok(bytes[off..off + len].to_vec())
                            };
                            if compressed_tx.send((idx, res)).is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        for idx in chunk.start_frame..chunk.end_frame {
                            if compressed_tx
                                .send((idx, Err(anyhow::anyhow!("chunk fetch failed: {}", e))))
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
    drop(job_rx);
    drop(compressed_tx);

    // Spawn writer threads: they decompress frames and write files.
    for _ in 0..write_threads {
        let compressed_rx: Receiver<(usize, Result<Vec<u8>>)> = compressed_rx.clone();
        let done_tx: Sender<Result<usize>> = done_tx.clone();
        let manifest2 = manifest.clone();
        let fragments_by_frame2 = fragments_by_frame.clone();
        let pending_files2 = pending_files.clone();
        let target_dir2 = target_dir.clone();
        let dictionary2 = dictionary.clone();
        let blob_pack_tx2 = blob_pack_tx.clone();
        std::thread::spawn(move || {
            while let Ok((idx, res)) = compressed_rx.recv() {
                let result: Result<usize> = (|| {
                    let compressed = res?;
                    let frame = &manifest2.frames[idx];
                    // Empty frames (produced by empty files) have no compressed
                    // bytes and decompress to an empty buffer.
                    let raw: Arc<Vec<u8>> =
                        Arc::new(if frame.compressed_len == 0 && frame.raw_len == 0 {
                            Vec::new()
                        } else {
                            match dictionary2.as_ref() {
                                Some(dict) => {
                                    let mut decompressor =
                                        zstd::bulk::Decompressor::with_dictionary(dict.as_slice())
                                            .context("create zstd decompressor with dictionary")?;
                                    decompressor
                                        .decompress(&compressed, frame.raw_len as usize)
                                        .with_context(|| {
                                            format!("decompress frame {} with dictionary", idx)
                                        })?
                                }
                                None => zstd::decode_all(compressed.as_slice())
                                    .with_context(|| format!("decompress frame {}", idx))?,
                            }
                        });
                    if raw.len() != frame.raw_len as usize {
                        anyhow::bail!(
                            "frame {} raw length mismatch: {} vs {}",
                            idx,
                            raw.len(),
                            frame.raw_len
                        );
                    }

                    let pairs = fragments_by_frame2
                        .get(&(idx as u32))
                        .cloned()
                        .unwrap_or_default();
                    let mut written = 0usize;
                    for (file_idx, frag_idx) in &pairs {
                        let entry = &manifest2.files[*file_idx];
                        let fragment = &entry.fragments[*frag_idx];
                        let off = fragment.frame_offset as usize;
                        let len = fragment.raw_len as usize;
                        if off + len > raw.len() {
                            anyhow::bail!(
                                "fragment for {} extends past frame {}",
                                String::from_utf8_lossy(&entry.path),
                                idx
                            );
                        }
                        let content = &raw[off..off + len];

                        if entry.fragments.len() == 1 {
                            if !skip_sha1_verify() {
                                let hash = <Sha1 as Sha1Digest>::digest(content);
                                if hash.as_slice() != entry.blob_sha1 {
                                    anyhow::bail!(
                                        "sha1 mismatch for {}",
                                        String::from_utf8_lossy(&entry.path)
                                    );
                                }
                            }
                            if let Some(ref tx) = blob_pack_tx2 {
                                let sha1 = blob_sha1_to_array(&entry.blob_sha1)?;
                                tx.send(crate::blob_pack::BlobPackInput::FrameSlice {
                                    sha1,
                                    frame: Arc::clone(&raw),
                                    offset: off,
                                    len,
                                })
                                .context("blob pack builder closed")?;
                            }
                            write_entry(&target_dir2, entry, content)?;
                            written += 1;
                        } else {
                            let mut guard = pending_files2.lock().unwrap();
                            let pending = guard.get_mut(file_idx).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "missing pending state for {}",
                                    String::from_utf8_lossy(&entry.path)
                                )
                            })?;
                            pending.fragments[*frag_idx] = Some(content.to_vec());
                            pending.remaining -= 1;
                            if pending.remaining == 0 {
                                let pending = guard.remove(file_idx).expect("pending file missing");
                                drop(guard);
                                let mut full = Vec::with_capacity(entry.total_len() as usize);
                                for frag in pending.fragments {
                                    full.extend_from_slice(&frag.expect("fragment missing"));
                                }
                                if !skip_sha1_verify() {
                                    let hash = <Sha1 as Sha1Digest>::digest(&full);
                                    if hash.as_slice() != entry.blob_sha1 {
                                        anyhow::bail!(
                                            "sha1 mismatch for {}",
                                            String::from_utf8_lossy(&entry.path)
                                        );
                                    }
                                }
                                write_entry(&target_dir2, entry, &full)?;
                                if let Some(ref tx) = blob_pack_tx2 {
                                    let sha1 = blob_sha1_to_array(&entry.blob_sha1)?;
                                    tx.send(crate::blob_pack::BlobPackInput::Owned {
                                        sha1,
                                        content: full,
                                    })
                                    .context("blob pack builder closed")?;
                                }
                                written += 1;
                            }
                        }
                    }
                    Ok(written)
                })();
                if done_tx.send(result).is_err() {
                    break;
                }
            }
        });
    }
    drop(compressed_rx);
    drop(done_tx);

    // Enqueue all chunk jobs.
    for chunk in &chunks {
        job_tx.send(chunk.clone()).context("send chunk fetch job")?;
    }
    drop(job_tx);

    // Collect results from all writers.
    let mut files_written = 0usize;
    let mut error: Option<anyhow::Error> = None;
    for _ in 0..manifest.frames.len() {
        match done_rx.recv() {
            Ok(Ok(n)) => files_written += n,
            Ok(Err(e)) => error = Some(e),
            Err(_) => {
                error = Some(anyhow::anyhow!("writer thread disappeared"));
                break;
            }
        }
    }
    if files_written != manifest.files.len() && error.is_none() {
        error = Some(anyhow::anyhow!(
            "extractor wrote {} files but manifest contains {}; frames={}",
            files_written,
            manifest.files.len(),
            manifest.frames.len()
        ));
    }

    if error.is_none() {
        info!(
            "fetched/decompressed/wrote {} frames ({} chunks) and {} files in {:?} ({} fetchers, {} writers, chunk_size={})",
            manifest.frames.len(),
            chunks.len(),
            files_written,
            fetch_start.elapsed(),
            fetch_threads,
            write_threads,
            chunk_size,
        );
    }

    let raw_total: u64 = manifest.files.iter().map(|e| e.total_len()).sum();

    // Clear skip-worktree for every materialized path, but only if extraction
    // succeeded. If it failed we still need to shut down the pack builder.
    if error.is_none() {
        let clear_start = Instant::now();
        let paths: Vec<String> = manifest
            .files
            .iter()
            .map(|e| String::from_utf8_lossy(&e.path).into_owned())
            .collect();
        if let Err(e) = git::clear_skip_worktree_index(&target_dir, &paths) {
            error = Some(e);
        } else {
            info!(
                "cleared skip-worktree for {} paths in {:?}",
                paths.len(),
                clear_start.elapsed()
            );
        }
    }

    // Always shut down the pack builder so the thread does not leak, even
    // when extraction failed.
    drop(blob_pack_tx);
    let pack_result: Option<Result<PathBuf>> = blob_pack_handle.map(|handle| {
        let pack_start = Instant::now();
        match handle.join() {
            Ok(Ok(path)) => {
                info!(
                    "built and installed local blob pack at {} in {:?}",
                    path.display(),
                    pack_start.elapsed()
                );
                Ok(path)
            }
            Ok(Err(e)) => Err(e).context("build and install local blob pack"),
            Err(_) => Err(anyhow::anyhow!("blob pack builder thread panicked")),
        }
    });

    if let Some(e) = error {
        return Err(e);
    }
    if let Some(Err(e)) = pack_result {
        return Err(e);
    }

    Ok(ExtractStats {
        files: files_written,
        raw_bytes: raw_total,
    })
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
fn safe_create_dir_all(root: &Path, rel: &Path) -> Result<()> {
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

fn write_entry(target_dir: &Path, entry: &FileEntry, content: &[u8]) -> Result<()> {
    let path = path_from_bytes(&entry.path);
    validate_relative_path(path)
        .with_context(|| format!("refusing to extract unsafe path: {}", path.display()))?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            safe_create_dir_all(target_dir, parent).with_context(|| {
                format!(
                    "create parent dir for {}",
                    String::from_utf8_lossy(&entry.path)
                )
            })?;
        }
    }

    let target = target_dir.join(path);

    // Refuse to operate through an existing symlink at the final component.
    // `OpenOptions::create` would follow it and write outside the target dir.
    if target.is_symlink() {
        std::fs::remove_file(&target)
            .with_context(|| format!("remove existing symlink {}", target.display()))?;
    }

    match entry.mode {
        0o120000 => {
            // Symlink: content is the target path.
            let link_target = std::str::from_utf8(content).with_context(|| {
                format!(
                    "non-utf8 symlink target for {}",
                    String::from_utf8_lossy(&entry.path)
                )
            })?;
            // Always unlink first; `exists()` follows symlinks and would miss a
            // broken symlink left over from a previous extraction.
            if target.exists() || target.is_symlink() {
                std::fs::remove_file(&target).ok();
            }
            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(link_target, &target)
                    .with_context(|| format!("symlink {}", target.display()))?;
                set_symlink_file_times(&target, INDEX_MTIME, INDEX_MTIME)
                    .with_context(|| format!("set symlink times {}", target.display()))?;
            }
            #[cfg(not(unix))]
            {
                std::fs::write(&target, link_target.as_bytes())
                    .with_context(|| format!("write symlink fallback {}", target.display()))?;
                set_file_mtime(&target, INDEX_MTIME)
                    .with_context(|| format!("set mtime {}", target.display()))?;
            }
        }
        0o100755 | 0o100644 => {
            // Only unlink if something is already there; on a fresh clone most paths
            // are missing, so this saves a syscall per file.
            if target.exists() {
                std::fs::remove_file(&target).ok();
            }

            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                let mode = if entry.mode == 0o100755 { 0o755 } else { 0o644 };
                let mut opts = OpenOptions::new();
                opts.write(true).create(true).truncate(true).mode(mode);
                let mut file = opts
                    .open(&target)
                    .with_context(|| format!("open {}", target.display()))?;
                file.write_all(content)
                    .with_context(|| format!("write {}", target.display()))?;
            }
            #[cfg(not(unix))]
            {
                std::fs::write(&target, content)
                    .with_context(|| format!("write {}", target.display()))?;
            }
            set_file_mtime(&target, INDEX_MTIME)
                .with_context(|| format!("set mtime {}", target.display()))?;
        }
        _ => {
            anyhow::bail!(
                "refusing to extract file {} with illegal mode 0o{:o}",
                String::from_utf8_lossy(&entry.path),
                entry.mode
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Fragment, FrameInfo, MetadataChunk};
    use sha1::{Digest, Sha1};
    use tempfile::TempDir;

    fn sha1_bytes(data: &[u8]) -> [u8; 20] {
        Sha1::digest(data).into()
    }

    fn git_blob_hash(content: &[u8]) -> [u8; 20] {
        let mut data = Vec::new();
        data.extend_from_slice(b"blob ");
        data.extend_from_slice(content.len().to_string().as_bytes());
        data.push(0);
        data.extend_from_slice(content);
        sha1_bytes(&data)
    }

    fn empty_manifest() -> MetadataChunk {
        MetadataChunk::new()
    }

    fn init_git_dir(target: &Path) {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(target)
            .args(["init", "-q"])
            .status()
            .expect("git init failed");
        assert!(status.success());
    }

    fn extract_manifest(
        manifest: &MetadataChunk,
        target: &Path,
        archive_chunks: Vec<Vec<u8>>,
    ) -> Result<ExtractStats> {
        init_git_dir(target);
        let manifest_path = target.join("manifest.pb");
        {
            let mut f = File::create(&manifest_path)?;
            manifest.write(&mut f)?;
        }
        extract_archive_with_chunk_fetcher(
            &manifest_path,
            target,
            None,
            None,
            u64::MAX,
            move |chunk| {
                archive_chunks
                    .get(chunk.chunk_index)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("missing chunk {}", chunk.chunk_index))
            },
        )
    }

    #[test]
    fn all_empty_files_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("out");
        std::fs::create_dir(&target).unwrap();

        let mut manifest = empty_manifest();
        manifest.files.push(FileEntry {
            path: b"a.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(b"").to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 0,
            }],
        });
        manifest.files.push(FileEntry {
            path: b"b.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(b"").to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 0,
            }],
        });
        // One empty frame covering both files.
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 0,
            raw_len: 0,
        });

        let stats = extract_manifest(&manifest, &target, vec![vec![]]).unwrap();
        assert_eq!(stats.files, 2);
        assert!(target.join("a.txt").exists());
        assert!(target.join("b.txt").exists());
        assert_eq!(std::fs::read(target.join("a.txt")).unwrap(), b"");
    }

    #[test]
    fn empty_file_after_large_file_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("out");
        std::fs::create_dir(&target).unwrap();

        let large = vec![b'x'; 100];
        let large_compressed = zstd::encode_all(large.as_slice(), 1).unwrap();
        let mut manifest = empty_manifest();
        manifest.files.push(FileEntry {
            path: b"big.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(&large).to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: large.len() as u32,
            }],
        });
        manifest.files.push(FileEntry {
            path: b"empty.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(b"").to_vec(),
            fragments: vec![Fragment {
                frame_index: 1,
                frame_offset: 0,
                raw_len: 0,
            }],
        });
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: large_compressed.len() as u32,
            raw_len: large.len() as u32,
        });
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: large_compressed.len() as u64,
            compressed_len: 0,
            raw_len: 0,
        });

        let stats = extract_manifest(&manifest, &target, vec![large_compressed]).unwrap();
        assert_eq!(stats.files, 2);
        assert_eq!(std::fs::read(target.join("big.txt")).unwrap(), large);
        assert!(target.join("empty.txt").exists());
    }

    #[test]
    fn rejects_path_traversal() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("out");
        std::fs::create_dir(&target).unwrap();

        let mut manifest = empty_manifest();
        manifest.files.push(FileEntry {
            path: b"../../evil.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(b"").to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 0,
            }],
        });
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 0,
            raw_len: 0,
        });

        assert!(extract_manifest(&manifest, &target, vec![vec![]]).is_err());
        assert!(!tmp.path().join("evil.txt").exists());
    }

    #[test]
    fn rejects_symlinked_parent() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("out");
        std::fs::create_dir(&target).unwrap();

        let outside = tmp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        let trap = target.join("trap");
        std::os::unix::fs::symlink(&outside, &trap).unwrap();

        let mut manifest = empty_manifest();
        manifest.files.push(FileEntry {
            path: b"trap/escaped.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(b"").to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 0,
            }],
        });
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 0,
            raw_len: 0,
        });

        assert!(extract_manifest(&manifest, &target, vec![vec![]]).is_err());
        assert!(!outside.join("escaped.txt").exists());
    }

    #[test]
    fn rejects_setuid_mode() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("out");
        std::fs::create_dir(&target).unwrap();

        let mut manifest = empty_manifest();
        manifest.files.push(FileEntry {
            path: b"setuid.txt".to_vec(),
            mode: 0o104755,
            blob_sha1: sha1_bytes(b"").to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 0,
            }],
        });
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 0,
            raw_len: 0,
        });

        assert!(extract_manifest(&manifest, &target, vec![vec![]]).is_err());
        assert!(!target.join("setuid.txt").exists());
    }

    #[test]
    fn archive_extract_builds_blob_pack() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("out");
        std::fs::create_dir(&target).unwrap();
        init_git_dir(&target);

        let data_a = b"hello blob pack";
        let data_b = b"second file";
        let mut manifest = empty_manifest();
        manifest.files.push(FileEntry {
            path: b"a.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(data_a).to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: data_a.len() as u32,
            }],
        });
        manifest.files.push(FileEntry {
            path: b"b.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(data_b).to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: data_a.len() as u32,
                raw_len: data_b.len() as u32,
            }],
        });

        let mut raw_frame = Vec::new();
        raw_frame.extend_from_slice(data_a);
        raw_frame.extend_from_slice(data_b);
        let compressed = zstd::encode_all(raw_frame.as_slice(), 1).unwrap();
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: compressed.len() as u32,
            raw_len: raw_frame.len() as u32,
        });

        let manifest_path = target.join("manifest.pb");
        {
            let mut f = File::create(&manifest_path).unwrap();
            manifest.write(&mut f).unwrap();
        }

        extract_archive_with_chunk_fetcher(
            &manifest_path,
            &target,
            Some(&target.join(".git")),
            None,
            u64::MAX,
            move |_chunk| Ok(compressed.clone()),
        )
        .unwrap();

        let pack_dir = target.join(".git").join("objects").join("pack");
        let packs: Vec<_> = std::fs::read_dir(&pack_dir)
            .unwrap()
            .filter_map(|e| {
                let p = e.ok()?.path();
                if p.extension()? == "pack" {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(packs.len(), 1, "expected one blob pack");

        // Verify git can read both blobs.
        for data in [data_a.as_slice(), data_b.as_slice()] {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(&target)
                .args(["cat-file", "blob", &hex::encode(git_blob_hash(data))])
                .output()
                .unwrap();
            assert!(output.status.success(), "git cat-file failed: {:?}", output);
            assert_eq!(output.stdout, data);
        }
    }
}

/// Default compressed chunk size for streaming extractions. A chunk contains
/// one or more consecutive frames fetched with a single HTTP range request.
/// 6 MiB is a middle ground: big enough to amortize per-request overhead on
/// fast links, small enough to avoid a long latency tail on CPU/bandwidth
/// constrained agents.
const DEFAULT_STREAMING_CHUNK_SIZE: u64 = 6 * 1024 * 1024;

/// Default chunk size when extracting from a local archive file. Smaller than
/// the streaming default because the local path is CPU-bound and benefits from
/// more parallel slicing/decompression.
const DEFAULT_LOCAL_CHUNK_SIZE: u64 = 2 * 1024 * 1024;

/// Extract a working-tree archive from a channel of pre-fetched archive chunks.
///
/// This is the unified pipeline path: async download tasks fetch archive chunks
/// concurrently and push them into `chunk_rx`. The extractor decompresses frames
/// and writes files while later chunks are still downloading.
///
/// `manifest_path` must point to a protobuf `MetadataChunk`. The chunk index in
/// each received message must match `FrameInfo.chunk_index` for the frames in
/// that chunk.
///
/// If `git_dir` is `Some`, every verified blob is also collected and written
/// into `.git/objects/pack` as a locally-built packfile.
pub fn extract_archive_from_chunk_receiver(
    manifest_path: &Path,
    target_dir: &Path,
    git_dir: Option<&Path>,
    dictionary: Option<&[u8]>,
    chunk_rx: Receiver<(usize, Result<Vec<u8>>)>,
) -> Result<ExtractStats> {
    let fetch_start = Instant::now();

    let mut manifest_file = File::open(manifest_path)
        .with_context(|| format!("open manifest {}", manifest_path.display()))?;
    let mut manifest_bytes = Vec::new();
    manifest_file
        .read_to_end(&mut manifest_bytes)
        .context("read manifest")?;
    let manifest = Manifest::read(&mut manifest_bytes.as_slice())?;

    // Validate every path before creating any directories, then create parents
    // safely (refusing symlinks and parent-dir escapes).
    for entry in manifest.files.iter() {
        validate_relative_path(path_from_bytes(&entry.path)).with_context(|| {
            format!(
                "invalid manifest path: {}",
                String::from_utf8_lossy(&entry.path)
            )
        })?;
    }
    let dirs: HashSet<PathBuf> = manifest
        .files
        .iter()
        .filter_map(|e| {
            let p = path_from_bytes(&e.path);
            let parent = p.parent()?;
            if parent.as_os_str().is_empty() {
                return None;
            }
            Some(parent.to_path_buf())
        })
        .collect();
    let mut dirs: Vec<_> = dirs.into_iter().collect();
    dirs.sort();
    for dir in dirs {
        safe_create_dir_all(target_dir, &dir)
            .with_context(|| format!("create dir {}", dir.display()))?;
    }

    let target_dir = target_dir.to_path_buf();
    let manifest = Arc::new(manifest);

    let expected_blob_count = git_dir.map(|_| {
        let mut unique: HashSet<[u8; 20]> = HashSet::new();
        for entry in manifest.files.iter() {
            let sha1: [u8; 20] = entry
                .blob_sha1
                .as_slice()
                .try_into()
                .expect("manifest blob_sha1 must be 20 bytes");
            unique.insert(sha1);
        }
        unique.len()
    });
    let (blob_pack_tx, blob_pack_handle): (
        Option<crossbeam_channel::Sender<crate::blob_pack::BlobPackInput>>,
        Option<std::thread::JoinHandle<Result<PathBuf>>>,
    ) = if let Some(git_dir) = git_dir {
        let expected = expected_blob_count.expect("expected count present with git_dir");
        let (tx, handle) = crate::blob_pack::spawn_blob_pack_builder(git_dir, expected)
            .context("spawn blob pack builder")?;
        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    let fragments_by_frame = Arc::new(manifest.fragments_by_frame());

    let mut pending_files: HashMap<usize, PendingFile> = HashMap::new();
    for (file_idx, entry) in manifest.files.iter().enumerate() {
        if entry.fragments.len() > 1 {
            pending_files.insert(
                file_idx,
                PendingFile {
                    fragments: vec![None; entry.fragments.len()],
                    remaining: entry.fragments.len(),
                },
            );
        }
    }
    let pending_files = Arc::new(Mutex::new(pending_files));

    let chunks = compute_chunks(&manifest.frames, u64::MAX);

    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let fetch_threads = std::env::var("RIPCLONE_FETCH_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| (num_cpus - 1).max(1));
    let write_threads = std::env::var("RIPCLONE_WRITE_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| (num_cpus - 1).max(1));
    let queue_depth = (fetch_threads * 2).max(write_threads * 2);

    info!(
        "extracting {} files across {} frames in {} archive chunks (fetch_threads={}, write_threads={}, queue_depth={})",
        manifest.files.len(),
        manifest.frames.len(),
        chunks.len(),
        fetch_threads,
        write_threads,
        queue_depth
    );

    let chunks_by_index: HashMap<usize, Chunk> =
        chunks.into_iter().map(|c| (c.chunk_index, c)).collect();

    let (compressed_tx, compressed_rx): (
        Sender<(usize, Result<Vec<u8>>)>,
        Receiver<(usize, Result<Vec<u8>>)>,
    ) = bounded(queue_depth);
    let (done_tx, done_rx): (Sender<Result<usize>>, Receiver<Result<usize>>) =
        bounded(manifest.frames.len());

    let dictionary = dictionary.map(|d| Arc::new(d.to_vec()));

    // Fetcher threads read whole archive chunks from the channel, slice them into
    // per-frame compressed buffers, and push those to the writer pool.
    for _ in 0..fetch_threads {
        let chunk_rx: Receiver<(usize, Result<Vec<u8>>)> = chunk_rx.clone();
        let compressed_tx: Sender<(usize, Result<Vec<u8>>)> = compressed_tx.clone();
        let chunks_by_index = chunks_by_index.clone();
        let manifest2 = manifest.clone();
        std::thread::spawn(move || {
            while let Ok((idx, res)) = chunk_rx.recv() {
                let chunk = match chunks_by_index.get(&idx) {
                    Some(c) => c.clone(),
                    None => {
                        let _ = compressed_tx.send((
                            idx,
                            Err(anyhow::anyhow!("unknown archive chunk index {}", idx)),
                        ));
                        continue;
                    }
                };
                match res {
                    Ok(bytes) => {
                        for frame_idx in chunk.start_frame..chunk.end_frame {
                            let frame = &manifest2.frames[frame_idx];
                            let off = (frame.chunk_offset - chunk.byte_start) as usize;
                            let len = frame.compressed_len as usize;
                            let out = if off + len > bytes.len() {
                                Err(anyhow::anyhow!(
                                    "frame {} (off={} len={}) out of chunk {} bounds (len={})",
                                    frame_idx,
                                    off,
                                    len,
                                    idx,
                                    bytes.len()
                                ))
                            } else {
                                Ok(bytes[off..off + len].to_vec())
                            };
                            if compressed_tx.send((frame_idx, out)).is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        for frame_idx in chunk.start_frame..chunk.end_frame {
                            if compressed_tx
                                .send((
                                    frame_idx,
                                    Err(anyhow::anyhow!("chunk {} failed: {}", idx, e)),
                                ))
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
    drop(chunk_rx);
    drop(compressed_tx);

    // Spawn writer threads: they decompress frames and write files.
    for _ in 0..write_threads {
        let compressed_rx: Receiver<(usize, Result<Vec<u8>>)> = compressed_rx.clone();
        let done_tx: Sender<Result<usize>> = done_tx.clone();
        let manifest2 = manifest.clone();
        let fragments_by_frame2 = fragments_by_frame.clone();
        let pending_files2 = pending_files.clone();
        let target_dir2 = target_dir.clone();
        let dictionary2 = dictionary.clone();
        let blob_pack_tx2 = blob_pack_tx.clone();
        std::thread::spawn(move || {
            while let Ok((idx, res)) = compressed_rx.recv() {
                let result: Result<usize> = (|| {
                    let compressed = res?;
                    let frame = &manifest2.frames[idx];
                    let raw: Arc<Vec<u8>> =
                        Arc::new(if frame.compressed_len == 0 && frame.raw_len == 0 {
                            Vec::new()
                        } else {
                            match dictionary2.as_ref() {
                                Some(dict) => {
                                    let mut decompressor =
                                        zstd::bulk::Decompressor::with_dictionary(dict.as_slice())
                                            .context("create zstd decompressor with dictionary")?;
                                    decompressor
                                        .decompress(&compressed, frame.raw_len as usize)
                                        .with_context(|| {
                                            format!("decompress frame {} with dictionary", idx)
                                        })?
                                }
                                None => zstd::decode_all(compressed.as_slice())
                                    .with_context(|| format!("decompress frame {}", idx))?,
                            }
                        });
                    if raw.len() != frame.raw_len as usize {
                        anyhow::bail!(
                            "frame {} raw length mismatch: {} vs {}",
                            idx,
                            raw.len(),
                            frame.raw_len
                        );
                    }

                    let pairs = fragments_by_frame2
                        .get(&(idx as u32))
                        .cloned()
                        .unwrap_or_default();
                    let mut written = 0usize;
                    for (file_idx, frag_idx) in &pairs {
                        let entry = &manifest2.files[*file_idx];
                        let fragment = &entry.fragments[*frag_idx];
                        let off = fragment.frame_offset as usize;
                        let len = fragment.raw_len as usize;
                        if off + len > raw.len() {
                            anyhow::bail!(
                                "fragment for {} extends past frame {}",
                                String::from_utf8_lossy(&entry.path),
                                idx
                            );
                        }
                        let content = &raw[off..off + len];

                        if entry.fragments.len() == 1 {
                            if !skip_sha1_verify() {
                                let hash = <Sha1 as Sha1Digest>::digest(content);
                                if hash.as_slice() != entry.blob_sha1 {
                                    anyhow::bail!(
                                        "sha1 mismatch for {}",
                                        String::from_utf8_lossy(&entry.path)
                                    );
                                }
                            }
                            if let Some(ref tx) = blob_pack_tx2 {
                                let sha1 = blob_sha1_to_array(&entry.blob_sha1)?;
                                tx.send(crate::blob_pack::BlobPackInput::FrameSlice {
                                    sha1,
                                    frame: Arc::clone(&raw),
                                    offset: off,
                                    len,
                                })
                                .context("blob pack builder closed")?;
                            }
                            write_entry(&target_dir2, entry, content)?;
                            written += 1;
                        } else {
                            let mut guard = pending_files2.lock().unwrap();
                            let pending = guard.get_mut(file_idx).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "missing pending state for {}",
                                    String::from_utf8_lossy(&entry.path)
                                )
                            })?;
                            pending.fragments[*frag_idx] = Some(content.to_vec());
                            pending.remaining -= 1;
                            if pending.remaining == 0 {
                                let pending = guard.remove(file_idx).expect("pending file missing");
                                drop(guard);
                                let mut full = Vec::with_capacity(entry.total_len() as usize);
                                for frag in pending.fragments {
                                    full.extend_from_slice(&frag.expect("fragment missing"));
                                }
                                if !skip_sha1_verify() {
                                    let hash = <Sha1 as Sha1Digest>::digest(&full);
                                    if hash.as_slice() != entry.blob_sha1 {
                                        anyhow::bail!(
                                            "sha1 mismatch for {}",
                                            String::from_utf8_lossy(&entry.path)
                                        );
                                    }
                                }
                                write_entry(&target_dir2, entry, &full)?;
                                if let Some(ref tx) = blob_pack_tx2 {
                                    let sha1 = blob_sha1_to_array(&entry.blob_sha1)?;
                                    tx.send(crate::blob_pack::BlobPackInput::Owned {
                                        sha1,
                                        content: full,
                                    })
                                    .context("blob pack builder closed")?;
                                }
                                written += 1;
                            }
                        }
                    }
                    Ok(written)
                })();
                if done_tx.send(result).is_err() {
                    break;
                }
            }
        });
    }
    drop(compressed_rx);
    drop(done_tx);

    // Collect results from all writers.
    let mut files_written = 0usize;
    let mut error: Option<anyhow::Error> = None;
    for _ in 0..manifest.frames.len() {
        match done_rx.recv() {
            Ok(Ok(n)) => files_written += n,
            Ok(Err(e)) => error = Some(e),
            Err(_) => {
                error = Some(anyhow::anyhow!("writer thread disappeared"));
                break;
            }
        }
    }
    if files_written != manifest.files.len() && error.is_none() {
        error = Some(anyhow::anyhow!(
            "extractor wrote {} files but manifest contains {}; frames={}",
            files_written,
            manifest.files.len(),
            manifest.frames.len()
        ));
    }

    if error.is_none() {
        info!(
            "fetched/decompressed/wrote {} frames and {} files in {:?} ({} fetchers, {} writers)",
            manifest.frames.len(),
            files_written,
            fetch_start.elapsed(),
            fetch_threads,
            write_threads,
        );
    }

    let raw_total: u64 = manifest.files.iter().map(|e| e.total_len()).sum();

    if error.is_none() {
        let clear_start = Instant::now();
        let paths: Vec<String> = manifest
            .files
            .iter()
            .map(|e| String::from_utf8_lossy(&e.path).into_owned())
            .collect();
        if let Err(e) = git::clear_skip_worktree_index(&target_dir, &paths) {
            error = Some(e);
        } else {
            info!(
                "cleared skip-worktree for {} paths in {:?}",
                paths.len(),
                clear_start.elapsed()
            );
        }
    }

    drop(blob_pack_tx);
    let pack_result: Option<Result<PathBuf>> = blob_pack_handle.map(|handle| {
        let pack_start = Instant::now();
        match handle.join() {
            Ok(Ok(path)) => {
                info!(
                    "built and installed local blob pack at {} in {:?}",
                    path.display(),
                    pack_start.elapsed()
                );
                Ok(path)
            }
            Ok(Err(e)) => Err(e).context("build and install local blob pack"),
            Err(_) => Err(anyhow::anyhow!("blob pack builder thread panicked")),
        }
    });

    if let Some(e) = error {
        return Err(e);
    }
    if let Some(Err(e)) = pack_result {
        return Err(e);
    }

    Ok(ExtractStats {
        files: files_written,
        raw_bytes: raw_total,
    })
}

/// Fetch and extract a working-tree archive using parallel HTTP range requests.
/// This is the streaming/parallel client path: the archive is never loaded into
/// memory as a single object. Consecutive frames are coalesced into chunks to
/// reduce the number of round-trips.
///
/// If `git_dir` is `Some`, every verified blob is also written into
/// `.git/objects/pack` as a locally-built packfile.
pub fn extract_archive_streaming(
    manifest_path: &Path,
    target_dir: &Path,
    git_dir: Option<&Path>,
    dictionary: Option<&[u8]>,
    archive_hash: &str,
    server: &str,
    token: Option<&str>,
) -> Result<ExtractStats> {
    use anyhow::Context;
    use reqwest::header::{AUTHORIZATION, RANGE};

    let chunk_size = std::env::var("RIPCLONE_CHUNK_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_STREAMING_CHUNK_SIZE);

    let client = reqwest::blocking::Client::new();
    let server = server.to_string();
    let archive_hash = archive_hash.to_string();
    let auth_header = token.map(|t| format!("Ripclone {}", t));

    extract_archive_with_chunk_fetcher(
        manifest_path,
        target_dir,
        git_dir,
        dictionary,
        chunk_size,
        move |chunk: &Chunk| {
            let start = chunk.byte_start;
            let end = chunk.byte_end.saturating_sub(1);
            let expected_len = chunk.compressed_len();
            let url = format!("{}/v1/artifacts/{}", server, archive_hash);
            let mut req = client
                .get(&url)
                .header(RANGE, format!("bytes={}-{}", start, end));
            if let Some(auth) = &auth_header {
                req = req.header(AUTHORIZATION, auth);
            }
            let resp = req
                .send()
                .with_context(|| format!("range request bytes={}-{} from {}", start, end, url))?;
            if resp.status() == reqwest::StatusCode::PARTIAL_CONTENT {
                let data = resp
                    .bytes()
                    .with_context(|| format!("read bytes={}-{} response", start, end))?;
                if data.len() as u64 != expected_len {
                    anyhow::bail!(
                        "range request bytes={}-{} length mismatch: expected {}, got {}",
                        start,
                        end,
                        expected_len,
                        data.len()
                    );
                }
                Ok(data.to_vec())
            } else if resp.status().is_success() {
                anyhow::bail!(
                    "range request bytes={}-{} returned 200 instead of 206",
                    start,
                    end
                );
            } else {
                anyhow::bail!(
                    "range request bytes={}-{} failed: {}",
                    start,
                    end,
                    resp.status()
                );
            }
        },
    )
}

/// Extract a working tree by fetching each archive chunk whole.
///
/// `manifest_path` must point to a protobuf `MetadataChunk` whose frame table
/// references the archive chunks in `archive_chunk_hashes` by index. Each chunk
/// is fetched with a single GET, decompressed frame-by-frame, and written to
/// `target_dir`.
/// `signed_chunk_urls` may be omitted or may contain one entry per archive
/// chunk hash. A `Some(url)` entry is fetched directly; `None` falls back to
/// the gateway's `/v1/artifacts/{hash}` endpoint.
///
/// If `git_dir` is `Some`, every verified blob is also written into
/// `.git/objects/pack` as a locally-built packfile.
pub fn extract_clonepack_streaming(
    manifest_path: &Path,
    archive_chunk_hashes: &[String],
    signed_chunk_urls: Option<Vec<Option<String>>>,
    target_dir: &Path,
    git_dir: Option<&Path>,
    dictionary: Option<&[u8]>,
    server: &str,
    token: Option<&str>,
) -> Result<ExtractStats> {
    use anyhow::Context;
    use reqwest::header::AUTHORIZATION;

    let client = reqwest::blocking::Client::new();
    let server = server.to_string();
    let hashes: Vec<String> = archive_chunk_hashes.to_vec();
    let signed: Vec<Option<String>> = signed_chunk_urls.unwrap_or_default();
    let auth_header = token.map(|t| format!("Ripclone {}", t));

    extract_archive_with_chunk_fetcher(
        manifest_path,
        target_dir,
        git_dir,
        dictionary,
        u64::MAX,
        move |chunk: &Chunk| {
            let hash = hashes
                .get(chunk.chunk_index)
                .with_context(|| format!("missing hash for archive chunk {}", chunk.chunk_index))?;
            let signed_url = signed.get(chunk.chunk_index).and_then(|o| o.as_deref());
            let url = signed_url
                .map(|u| u.to_string())
                .unwrap_or_else(|| format!("{}/v1/artifacts/{}", server, hash));
            let fetch_start = Instant::now();
            let mut req = client.get(&url);
            if signed_url.is_none() {
                // Signed URLs are self-authenticating; only send the ripclone
                // token when falling back to the gateway.
                if let Some(auth) = &auth_header {
                    req = req.header(AUTHORIZATION, auth);
                }
            }
            let resp = req.send().with_context(|| {
                format!("fetch archive chunk {} from {}", chunk.chunk_index, url)
            })?;
            if !resp.status().is_success() {
                anyhow::bail!(
                    "archive chunk {} fetch failed: {}",
                    chunk.chunk_index,
                    resp.status()
                );
            }
            let bytes = resp
                .bytes()
                .with_context(|| format!("read archive chunk {} response", chunk.chunk_index))?;
            let actual_hash = hex::encode(<Sha256 as Sha256Digest>::digest(&bytes));
            if actual_hash != *hash {
                anyhow::bail!(
                    "archive chunk {} hash mismatch: expected {}, got {}",
                    chunk.chunk_index,
                    hash,
                    actual_hash
                );
            }
            info!(
                "fetched archive chunk {} ({} bytes) in {:?}",
                chunk.chunk_index,
                bytes.len(),
                fetch_start.elapsed()
            );
            Ok(bytes.to_vec())
        },
    )
}
