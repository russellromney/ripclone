use crate::fsutil::{path_from_bytes, safe_create_dir_all, validate_relative_path};
use crate::manifest::{FileEntry, FrameInfo, MetadataChunk as Manifest};
use crate::worktree_writer::{
    FileSlice, FileWriteContent, OwnedFileWrite, WorktreeWriter, WriteOptions,
};
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use flate2::read::ZlibDecoder;
use sha1::{Digest as Sha1Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::info;

const PACK_WRITE_BATCH_FILES: usize = 512;

/// Convert a manifest blob_sha1 slice to a fixed 20-byte array.
fn blob_sha1_to_array(sha1: &[u8]) -> Result<[u8; 20]> {
    sha1.try_into()
        .map_err(|_| anyhow::anyhow!("manifest blob_sha1 must be 20 bytes, got {}", sha1.len()))
}

struct PendingFile {
    fragments: Vec<Option<FileSlice>>,
    remaining: usize,
}

#[derive(Debug)]
pub struct ExtractStats {
    pub files: usize,
    pub raw_bytes: u64,
    pub stats: Vec<crate::git::MaterializedPathStat>,
}

pub struct PackExtractResult {
    pub files: usize,
    pub stats: Vec<crate::git::MaterializedPathStat>,
}

struct ArchiveFrameWriteResult {
    written: usize,
    stats: Vec<crate::git::MaterializedPathStat>,
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
fn compute_chunks(frames: &[FrameInfo], chunk_size: u64) -> Result<Vec<Chunk>> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut byte_start = 0u64;
    let mut current_len = 0u64;
    let mut current_chunk_index = frames
        .first()
        .map(|f| usize::try_from(f.chunk_index))
        .transpose()
        .context("archive chunk index does not fit in usize")?
        .unwrap_or(0);

    for (i, frame) in frames.iter().enumerate() {
        let frame_len = u64::from(frame.compressed_len);
        let frame_chunk = usize::try_from(frame.chunk_index)
            .context("archive chunk index does not fit in usize")?;
        // Start a new chunk when crossing into a different archive chunk or when
        // the current frame would not fit.
        if frame_chunk != current_chunk_index
            || (current_len > 0
                && current_len
                    .checked_add(frame_len)
                    .ok_or_else(|| anyhow::anyhow!("archive chunk length overflow"))?
                    > chunk_size)
        {
            let end = i;
            let byte_end = byte_start
                .checked_add(current_len)
                .ok_or_else(|| anyhow::anyhow!("archive chunk offset overflow"))?;
            chunks.push(Chunk {
                chunk_index: current_chunk_index,
                start_frame: start,
                end_frame: end,
                byte_start,
                byte_end,
            });
            start = i;
            byte_start = frame.chunk_offset;
            current_len = 0;
            current_chunk_index = frame_chunk;
        }
        current_len = current_len
            .checked_add(frame_len)
            .ok_or_else(|| anyhow::anyhow!("archive chunk length overflow"))?;
    }

    // Ensure every frame is covered, including zero-length frames produced by
    // empty files. A chunk with zero compressed bytes still carries the empty
    // frames to the writer so the files are created.
    if !frames.is_empty() {
        let byte_end = byte_start
            .checked_add(current_len)
            .ok_or_else(|| anyhow::anyhow!("archive chunk offset overflow"))?;
        chunks.push(Chunk {
            chunk_index: current_chunk_index,
            start_frame: start,
            end_frame: frames.len(),
            byte_start,
            byte_end,
        });
    }

    Ok(chunks)
}

fn slice_archive_frame(
    bytes: &bytes::Bytes,
    chunk: &Chunk,
    frame: &FrameInfo,
    frame_idx: usize,
    chunk_idx: usize,
) -> Result<bytes::Bytes> {
    let relative_offset = frame
        .chunk_offset
        .checked_sub(chunk.byte_start)
        .with_context(|| {
            format!(
                "frame {} starts before archive chunk {} (frame offset={} chunk start={})",
                frame_idx, chunk_idx, frame.chunk_offset, chunk.byte_start
            )
        })?;
    let offset =
        usize::try_from(relative_offset).context("archive frame offset does not fit in usize")?;
    let len = usize::try_from(frame.compressed_len)
        .context("archive frame length does not fit in usize")?;
    let end = offset
        .checked_add(len)
        .context("archive frame slice end overflow")?;
    if bytes.get(offset..end).is_none() {
        anyhow::bail!(
            "frame {} (offset={} len={}) out of chunk {} bounds (start={} len={})",
            frame_idx,
            frame.chunk_offset,
            frame.compressed_len,
            chunk_idx,
            chunk.byte_start,
            bytes.len()
        );
    }
    Ok(bytes.slice(offset..end))
}

fn read_archive_manifest(manifest_path: &Path) -> Result<Manifest> {
    let mut manifest_file = File::open(manifest_path)
        .with_context(|| format!("open manifest {}", manifest_path.display()))?;
    let mut manifest_bytes = Vec::new();
    manifest_file
        .read_to_end(&mut manifest_bytes)
        .context("read manifest")?;
    Manifest::read(&mut manifest_bytes.as_slice())
}

/// Extract a working-tree archive into `target_dir` using the supplied manifest
/// and a local archive file.
pub fn extract_archive(
    archive_path: &Path,
    manifest_path: &Path,
    target_dir: &Path,
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
        Some(target_dir),
        dictionary,
        DEFAULT_LOCAL_CHUNK_SIZE,
        move |chunk: &Chunk| {
            let start = usize::try_from(chunk.byte_start)
                .context("archive chunk start does not fit in usize")?;
            let end = usize::try_from(chunk.byte_end)
                .context("archive chunk end does not fit in usize")?;
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
pub fn extract_archive_with_chunk_fetcher<F>(
    manifest_path: &Path,
    target_dir: Option<&Path>,
    dictionary: Option<&[u8]>,
    chunk_size: u64,
    fetch_chunk: F,
) -> Result<ExtractStats>
where
    F: Fn(&Chunk) -> Result<Vec<u8>> + Send + Sync + 'static,
{
    let manifest = read_archive_manifest(manifest_path)?;
    let chunks = compute_chunks(&manifest.frames, chunk_size)?;
    let chunk_map: HashMap<usize, Chunk> = chunks.iter().cloned().enumerate().collect();
    let (fetch_threads, write_threads) = archive_thread_counts();
    let queue_depth = (fetch_threads * 2).max(write_threads * 2);
    let (job_tx, job_rx): (Sender<(usize, Chunk)>, Receiver<(usize, Chunk)>) = bounded(queue_depth);
    let (chunk_tx, chunk_rx) = bounded(queue_depth);
    let fetcher = Arc::new(fetch_chunk);

    for _ in 0..fetch_threads {
        let fetcher = fetcher.clone();
        let job_rx = job_rx.clone();
        let chunk_tx = chunk_tx.clone();
        std::thread::spawn(move || {
            while let Ok((idx, chunk)) = job_rx.recv() {
                let res = fetcher(&chunk).map(bytes::Bytes::from).with_context(|| {
                    format!("fetch chunk bytes={}-{}", chunk.byte_start, chunk.byte_end)
                });
                if chunk_tx.send((idx, res)).is_err() {
                    break;
                }
            }
        });
    }
    drop(job_rx);
    drop(chunk_tx);

    std::thread::spawn(move || {
        for (idx, chunk) in chunks.into_iter().enumerate() {
            if job_tx.send((idx, chunk)).is_err() {
                break;
            }
        }
    });

    extract_archive_from_chunk_receiver_with_chunks(
        manifest_path,
        target_dir,
        dictionary,
        chunk_rx,
        chunk_map,
    )
}

/// `WriteOptions` shared by every archive regular-file write.
const ARCHIVE_WRITE_OPTIONS: WriteOptions = WriteOptions {
    parents_prepared: true,
    stamp_mtime: false,
    fresh_target: false,
};

fn flush_archive_writes(
    writer: &WorktreeWriter,
    target_dir: &Path,
    pending_writes: &mut Vec<OwnedFileWrite>,
) -> Result<crate::worktree_writer::WriteOutcome> {
    if pending_writes.is_empty() {
        return Ok(crate::worktree_writer::WriteOutcome {
            written: 0,
            stats: Vec::new(),
        });
    }
    writer.write_owned_entries_with_options(
        target_dir,
        std::mem::take(pending_writes),
        ARCHIVE_WRITE_OPTIONS,
    )
}

/// Default chunk size when extracting from a local archive file. Smaller than
/// the streaming default because the local path is CPU-bound and benefits from
/// more parallel slicing/decompression.
const DEFAULT_LOCAL_CHUNK_SIZE: u64 = 2 * 1024 * 1024;

fn archive_thread_counts() -> (usize, usize) {
    let fetch_threads =
        crate::gix_util::worker_threads("archive-fetch", crate::gix_util::default_worker_threads());
    let write_threads =
        crate::gix_util::worker_threads("archive-write", crate::gix_util::default_worker_threads());
    (fetch_threads, write_threads)
}

fn decompress_frame_recorded(
    compressed: &[u8],
    frame: &FrameInfo,
    dictionary: Option<&[u8]>,
    idx: usize,
) -> Result<Vec<u8>> {
    let start = Instant::now();
    let raw_len =
        usize::try_from(frame.raw_len).context("archive frame raw length does not fit in usize")?;
    let raw = if frame.compressed_len == 0 && frame.raw_len == 0 {
        Vec::new()
    } else {
        match dictionary {
            Some(dict) => {
                let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict)
                    .context("create zstd decompressor with dictionary")?;
                decompressor
                    .decompress(compressed, raw_len)
                    .with_context(|| format!("decompress frame {} with dictionary", idx))?
            }
            None => zstd::bulk::Decompressor::new()
                .context("create zstd decompressor")?
                .decompress(compressed, raw_len)
                .with_context(|| format!("decompress frame {}", idx))?,
        }
    };
    crate::perf::record_zstd_inflate(start.elapsed(), compressed.len(), raw.len());
    Ok(raw)
}

fn sha1_digest_recorded(content: &[u8]) -> sha1::digest::Output<Sha1> {
    let start = Instant::now();
    let hash = <Sha1 as Sha1Digest>::digest(content);
    crate::perf::record_sha1(start.elapsed(), content.len());
    hash
}

fn sha1_digest_fragments_recorded(fragments: &[FileSlice]) -> sha1::digest::Output<Sha1> {
    let start = Instant::now();
    let mut hasher = Sha1::new();
    let mut bytes = 0usize;
    for fragment in fragments {
        let data = &fragment.data[fragment.offset..fragment.offset + fragment.len];
        bytes += data.len();
        hasher.update(data);
    }
    let hash = hasher.finalize();
    crate::perf::record_sha1(start.elapsed(), bytes);
    hash
}

/// Extract a working-tree archive from a channel of pre-fetched archive chunks.
///
/// This is the unified pipeline path: async download tasks fetch archive chunks
/// concurrently and push them into `chunk_rx`. The extractor decompresses frames
/// and writes files while later chunks are still downloading.
///
/// `manifest_path` must point to a protobuf `MetadataChunk`. The chunk index in
/// each received message must match `FrameInfo.chunk_index` for the frames in
/// that chunk.
pub fn extract_archive_from_chunk_receiver(
    manifest_path: &Path,
    target_dir: Option<&Path>,
    dictionary: Option<&[u8]>,
    chunk_rx: Receiver<(usize, Result<bytes::Bytes>)>,
) -> Result<ExtractStats> {
    let manifest = read_archive_manifest(manifest_path)?;
    let chunks_by_index: HashMap<usize, Chunk> = compute_chunks(&manifest.frames, u64::MAX)?
        .into_iter()
        .map(|chunk| (chunk.chunk_index, chunk))
        .collect();
    extract_archive_from_chunk_receiver_with_chunks(
        manifest_path,
        target_dir,
        dictionary,
        chunk_rx,
        chunks_by_index,
    )
}

fn extract_archive_from_chunk_receiver_with_chunks(
    manifest_path: &Path,
    target_dir: Option<&Path>,
    dictionary: Option<&[u8]>,
    chunk_rx: Receiver<(usize, Result<bytes::Bytes>)>,
    chunks_by_index: HashMap<usize, Chunk>,
) -> Result<ExtractStats> {
    let fetch_start = Instant::now();

    let mut manifest_file = File::open(manifest_path)
        .with_context(|| format!("open manifest {}", manifest_path.display()))?;
    let mut manifest_bytes = Vec::new();
    manifest_file
        .read_to_end(&mut manifest_bytes)
        .context("read manifest")?;
    let manifest = Manifest::read(&mut manifest_bytes.as_slice())?;

    // Validate every blob_sha1 length up front so downstream code can rely on
    // a fixed 20-byte representation.
    for entry in manifest.files.iter() {
        blob_sha1_to_array(&entry.blob_sha1).with_context(|| {
            format!(
                "invalid blob_sha1 for {}",
                String::from_utf8_lossy(&entry.path)
            )
        })?;
    }

    // Validate every path and create parent directories only when we are
    // materializing the working tree. Lazy callers pass no target dir to build
    // only a local blob pack.
    if let Some(target_dir) = target_dir {
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
    }

    let target_dir = target_dir.map(Path::to_path_buf);
    let manifest = Arc::new(manifest);

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

    let (fetch_threads, write_threads) = archive_thread_counts();
    let queue_depth = (fetch_threads * 2).max(write_threads * 2);
    let chunk_count = chunks_by_index.len();

    info!(
        "extracting {} files across {} frames in {} archive chunks (fetch_threads={}, write_threads={}, queue_depth={})",
        manifest.files.len(),
        manifest.frames.len(),
        chunk_count,
        fetch_threads,
        write_threads,
        queue_depth
    );

    let (compressed_tx, compressed_rx): (
        Sender<(usize, Result<bytes::Bytes>)>,
        Receiver<(usize, Result<bytes::Bytes>)>,
    ) = bounded(queue_depth);
    let (done_tx, done_rx): (
        Sender<Result<ArchiveFrameWriteResult>>,
        Receiver<Result<ArchiveFrameWriteResult>>,
    ) = bounded(manifest.frames.len());

    let dictionary = dictionary.map(|d| Arc::new(d.to_vec()));

    // Fetcher threads read whole archive chunks from the channel, slice them into
    // per-frame compressed buffers, and push those to the writer pool.
    for _ in 0..fetch_threads {
        let chunk_rx: Receiver<(usize, Result<bytes::Bytes>)> = chunk_rx.clone();
        let compressed_tx: Sender<(usize, Result<bytes::Bytes>)> = compressed_tx.clone();
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
                            // A corrupted manifest can put chunk_offset below the
                            // chunk's start or past its end; validate the complete
                            // usize range before constructing a zero-copy slice.
                            let out = slice_archive_frame(&bytes, &chunk, frame, frame_idx, idx);
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
        let compressed_rx: Receiver<(usize, Result<bytes::Bytes>)> = compressed_rx.clone();
        let done_tx: Sender<Result<ArchiveFrameWriteResult>> = done_tx.clone();
        let manifest2 = manifest.clone();
        let fragments_by_frame2 = fragments_by_frame.clone();
        let pending_files2 = pending_files.clone();
        let target_dir2 = target_dir.clone();
        let dictionary2 = dictionary.clone();
        std::thread::spawn(move || {
            let writer = target_dir2.as_ref().map(|_| WorktreeWriter::new());
            while let Ok((idx, res)) = compressed_rx.recv() {
                let result: Result<ArchiveFrameWriteResult> = (|| {
                    let writer = match writer.as_ref() {
                        Some(Ok(writer)) => Some(writer),
                        Some(Err(e)) => anyhow::bail!("create worktree writer: {e:#}"),
                        None => None,
                    };
                    let compressed = res?;
                    let frame = &manifest2.frames[idx];
                    let raw = Arc::new(decompress_frame_recorded(
                        compressed.as_ref(),
                        frame,
                        dictionary2.as_deref().map(|d| d.as_slice()),
                        idx,
                    )?);
                    let raw_len = usize::try_from(frame.raw_len)
                        .context("archive frame raw length does not fit in usize")?;
                    if raw.len() != raw_len {
                        anyhow::bail!(
                            "frame {} raw length mismatch: {} vs {}",
                            idx,
                            raw.len(),
                            frame.raw_len
                        );
                    }

                    // R2: borrow the frame's fragment-pair list from the shared
                    // map instead of cloning it per frame (the loop only reads it).
                    let frame_key = u32::try_from(idx)
                        .context("archive frame index does not fit in manifest key")?;
                    let pairs = fragments_by_frame2
                        .get(&frame_key)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]);
                    let mut written = 0usize;
                    let mut stats = Vec::new();
                    let mut pending_writes: Vec<OwnedFileWrite> =
                        Vec::with_capacity(PACK_WRITE_BATCH_FILES);
                    for (file_idx, frag_idx) in pairs {
                        let entry = &manifest2.files[*file_idx];
                        let fragment = &entry.fragments[*frag_idx];
                        let off = usize::try_from(fragment.frame_offset)
                            .context("fragment frame offset does not fit in usize")?;
                        let len = usize::try_from(fragment.raw_len)
                            .context("fragment raw length does not fit in usize")?;
                        let end = off
                            .checked_add(len)
                            .context("fragment frame slice end overflow")?;
                        if raw.get(off..end).is_none() {
                            anyhow::bail!(
                                "fragment for {} extends past frame {}",
                                String::from_utf8_lossy(&entry.path),
                                idx
                            );
                        }
                        let content = raw
                            .get(off..end)
                            .context("validated fragment range disappeared")?;

                        if entry.fragments.len() == 1 {
                            let hash = sha1_digest_recorded(content);
                            if hash.as_slice() != entry.blob_sha1 {
                                anyhow::bail!(
                                    "sha1 mismatch for {}",
                                    String::from_utf8_lossy(&entry.path)
                                );
                            }
                            if let Some((target_dir, writer)) = target_dir2.as_ref().zip(writer) {
                                pending_writes.push(OwnedFileWrite {
                                    entry: entry.clone(),
                                    content: FileWriteContent::shared(Arc::clone(&raw), off, len),
                                });
                                if pending_writes.len() >= PACK_WRITE_BATCH_FILES {
                                    let outcome = flush_archive_writes(
                                        writer,
                                        target_dir,
                                        &mut pending_writes,
                                    )?;
                                    written += outcome.written;
                                    stats.extend(outcome.stats);
                                }
                            }
                        } else {
                            let completed = {
                                let mut guard = pending_files2.lock().map_err(|_| {
                                    anyhow::anyhow!("pending file state lock poisoned")
                                })?;
                                let pending = guard.get_mut(file_idx).ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "missing pending state for {}",
                                        String::from_utf8_lossy(&entry.path)
                                    )
                                })?;
                                let slot =
                                    pending.fragments.get_mut(*frag_idx).ok_or_else(|| {
                                        anyhow::anyhow!(
                                            "fragment index {} out of range for {}",
                                            frag_idx,
                                            String::from_utf8_lossy(&entry.path)
                                        )
                                    })?;
                                if slot.is_some() {
                                    anyhow::bail!(
                                        "duplicate fragment {} for {}",
                                        frag_idx,
                                        String::from_utf8_lossy(&entry.path)
                                    );
                                }
                                *slot = Some(FileSlice {
                                    data: Arc::clone(&raw),
                                    offset: off,
                                    len,
                                });
                                pending.remaining = pending
                                    .remaining
                                    .checked_sub(1)
                                    .context("pending fragment count underflow")?;
                                if pending.remaining == 0 {
                                    Some(guard.remove(file_idx).ok_or_else(|| {
                                        anyhow::anyhow!("pending file missing after completion")
                                    })?)
                                } else {
                                    None
                                }
                            };
                            if let Some(pending) = completed {
                                let fragments: Vec<FileSlice> = pending
                                    .fragments
                                    .into_iter()
                                    .enumerate()
                                    .map(|(index, fragment)| {
                                        fragment.ok_or_else(|| {
                                            anyhow::anyhow!(
                                                "fragment {} missing at completion for {}",
                                                index,
                                                String::from_utf8_lossy(&entry.path)
                                            )
                                        })
                                    })
                                    .collect::<Result<_>>()?;
                                let hash = sha1_digest_fragments_recorded(&fragments);
                                if hash.as_slice() != entry.blob_sha1 {
                                    anyhow::bail!(
                                        "sha1 mismatch for {}",
                                        String::from_utf8_lossy(&entry.path)
                                    );
                                }
                                if let Some((target_dir, writer)) = target_dir2.as_ref().zip(writer)
                                {
                                    pending_writes.push(OwnedFileWrite {
                                        entry: entry.clone(),
                                        content: FileWriteContent::Fragments(fragments),
                                    });
                                    if pending_writes.len() >= PACK_WRITE_BATCH_FILES {
                                        let outcome = flush_archive_writes(
                                            writer,
                                            target_dir,
                                            &mut pending_writes,
                                        )?;
                                        written += outcome.written;
                                        stats.extend(outcome.stats);
                                    }
                                }
                            }
                        }
                    }
                    if let Some((target_dir, writer)) = target_dir2.as_ref().zip(writer) {
                        let outcome =
                            flush_archive_writes(writer, target_dir, &mut pending_writes)?;
                        written += outcome.written;
                        stats.extend(outcome.stats);
                    }
                    Ok(ArchiveFrameWriteResult { written, stats })
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
    let mut stat_cache = Vec::new();
    let mut error: Option<anyhow::Error> = None;
    for _ in 0..manifest.frames.len() {
        match done_rx.recv() {
            Ok(Ok(result)) => {
                files_written += result.written;
                stat_cache.extend(result.stats);
            }
            Ok(Err(e)) => error = Some(e),
            Err(_) => {
                error = Some(anyhow::anyhow!("writer thread disappeared"));
                break;
            }
        }
    }
    let expected_files = if target_dir.is_some() {
        manifest.files.len()
    } else {
        0
    };
    if files_written != expected_files && error.is_none() {
        error = Some(anyhow::anyhow!(
            "extractor wrote {} files but expected {}; frames={}",
            files_written,
            expected_files,
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

    let raw_total = manifest.files.iter().try_fold(0u64, |total, entry| {
        total
            .checked_add(entry.checked_total_len()?)
            .context("archive raw byte total overflow")
    })?;

    if let Some(e) = error {
        return Err(e);
    }

    Ok(ExtractStats {
        files: files_written,
        raw_bytes: raw_total,
        stats: stat_cache,
    })
}

/// Map from a 20-byte git blob sha1 to the working-tree paths (and modes) that
/// contain that blob. Built from the manifest file table.
pub type BlobPathMap = HashMap<[u8; 20], Vec<(Vec<u8>, u32)>>;

/// Build a [`BlobPathMap`] from the manifest file entries.
pub fn build_blob_path_map(files: &[FileEntry]) -> BlobPathMap {
    let mut map: BlobPathMap = HashMap::new();
    for f in files {
        if f.blob_sha1.len() == 20 {
            let mut key = [0u8; 20];
            key.copy_from_slice(&f.blob_sha1);
            map.entry(key).or_default().push((f.path.clone(), f.mode));
        }
    }
    map
}

/// Validate every path and pre-create all parent directories single-threaded, so
/// the parallel per-pack writers never race on `mkdir`.
pub fn prepare_worktree_dirs(target_dir: &Path, files: &[FileEntry]) -> Result<()> {
    for entry in files {
        validate_relative_path(path_from_bytes(&entry.path)).with_context(|| {
            format!(
                "invalid manifest path: {}",
                String::from_utf8_lossy(&entry.path)
            )
        })?;
    }
    let dirs: HashSet<PathBuf> = files
        .iter()
        .filter_map(|e| {
            let parent = path_from_bytes(&e.path).parent()?;
            if parent.as_os_str().is_empty() {
                return None;
            }
            Some(parent.to_path_buf())
        })
        .collect();
    let mut dirs: Vec<_> = dirs.into_iter().collect();
    dirs.sort();
    for dir in &dirs {
        safe_create_dir_all(target_dir, dir)
            .with_context(|| format!("create dir {}", dir.display()))?;
    }
    Ok(())
}

/// Parse an in-memory, undeltified git pack and write every blob whose sha1 is
/// in `blob_paths` to its working-tree path(s) under `target_dir`. Returns the
/// number of files written.
///
/// Only OBJ_COMMIT/TREE/BLOB/TAG are handled; delta objects are rejected (the
/// server builds these packs with `--window=0`). Parent directories must already
/// exist (see [`prepare_worktree_dirs`]).
pub fn extract_blobs_from_pack_bytes(
    pack: &[u8],
    blob_paths: &BlobPathMap,
    target_dir: &Path,
    writer: &WorktreeWriter,
) -> Result<PackExtractResult> {
    if pack.len() < 12 || &pack[0..4] != b"PACK" {
        anyhow::bail!("invalid pack header");
    }
    let count = usize::try_from(u32::from_be_bytes([pack[8], pack[9], pack[10], pack[11]]))
        .context("pack object count does not fit in usize")?;
    let mut off = 12usize;
    let mut written = 0usize;
    let mut stats = Vec::new();
    let mut pending_writes: Vec<OwnedFileWrite> = Vec::with_capacity(PACK_WRITE_BATCH_FILES);

    for i in 0..count {
        if off >= pack.len() {
            anyhow::bail!("pack object {} starts past end of pack", i);
        }
        let (obj_type, size, hdr_len) = parse_pack_obj_header(&pack[off..])
            .with_context(|| format!("parse object {} header", i))?;
        let data_start = off
            .checked_add(hdr_len)
            .context("pack object data offset overflow")?;
        if obj_type == 6 || obj_type == 7 {
            anyhow::bail!(
                "unexpected delta object (type {}) in undeltified pack",
                obj_type
            );
        }
        if data_start > pack.len() {
            anyhow::bail!("pack object {} data starts past end of pack", i);
        }
        let inflate_start = Instant::now();
        let mut dec = ZlibDecoder::new(&pack[data_start..]);
        // Preserve the single-allocation fast path without trusting a hostile
        // object's advertised size beyond the containing pack bytes.
        let mut content = Vec::with_capacity(pack_object_initial_capacity(size, pack.len()));
        dec.read_to_end(&mut content)
            .with_context(|| format!("inflate pack object {}", i))?;
        let content_len = u64::try_from(content.len()).context("pack object length overflow")?;
        if content_len != size {
            anyhow::bail!(
                "pack object {} size mismatch: header={} actual={}",
                i,
                size,
                content.len()
            );
        }
        let compressed_len = usize::try_from(dec.total_in())
            .context("compressed pack object length does not fit in usize")?;
        crate::perf::record_zlib_inflate(inflate_start.elapsed(), compressed_len, content.len());
        off = data_start
            .checked_add(compressed_len)
            .context("pack object end offset overflow")?;

        // type 3 == OBJ_BLOB. The manifest keys blobs by the plain sha1 of the
        // raw content (no git "blob <len>\0" header), so match that.
        if obj_type == 3 {
            let sha: [u8; 20] = sha1_digest_recorded(&content).into();
            if let Some(paths) = blob_paths.get(&sha) {
                let path_count = paths.len();
                for (idx, (path, mode)) in paths.iter().enumerate() {
                    let entry = FileEntry {
                        path: path.clone(),
                        mode: *mode,
                        blob_sha1: Vec::new(),
                        fragments: Vec::new(),
                    };
                    let content = if idx + 1 == path_count {
                        std::mem::take(&mut content)
                    } else {
                        content.clone()
                    };
                    pending_writes.push(OwnedFileWrite {
                        entry,
                        content: content.into(),
                    });
                    if pending_writes.len() >= PACK_WRITE_BATCH_FILES {
                        let outcome = writer
                            .write_owned_entries_for_fresh_indexed_checkout_deferred(
                                target_dir,
                                std::mem::take(&mut pending_writes),
                            )?;
                        written += outcome.written;
                        stats.extend(outcome.stats);
                    }
                }
            }
        }
    }
    let outcome = writer
        .write_owned_entries_for_fresh_indexed_checkout_deferred(target_dir, pending_writes)?;
    written += outcome.written;
    stats.extend(outcome.stats);
    let outcome = writer.flush_deferred_writes()?;
    written += outcome.written;
    stats.extend(outcome.stats);
    Ok(PackExtractResult {
        files: written,
        stats,
    })
}

fn pack_object_initial_capacity(advertised_size: u64, pack_len: usize) -> usize {
    usize::try_from(advertised_size)
        .unwrap_or(usize::MAX)
        .min(pack_len)
}

/// Parse a git pack object header: 3-bit type + little-endian base-128 size.
/// Returns `(type, uncompressed_size, header_byte_len)`.
fn parse_pack_obj_header(buf: &[u8]) -> Result<(u8, u64, usize)> {
    if buf.is_empty() {
        anyhow::bail!("truncated pack object header");
    }
    let mut i = 0usize;
    let b = buf[i];
    i += 1;
    let obj_type = (b >> 4) & 0x07;
    let mut size = (b & 0x0f) as u64;
    let mut shift = 4u32;
    let mut cont = b & 0x80 != 0;
    while cont {
        if shift >= 64 {
            anyhow::bail!("pack object size varint overflows u64");
        }
        if i >= buf.len() {
            anyhow::bail!("truncated pack object size varint");
        }
        let b = buf[i];
        i += 1;
        let part = (b & 0x7f) as u64;
        let remaining = 64 - shift;
        if remaining < 7 && part >= (1u64 << remaining) {
            anyhow::bail!("pack object size varint overflows u64");
        }
        size |= part << shift;
        shift += 7;
        cont = b & 0x80 != 0;
    }
    Ok((obj_type, size, i))
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

    fn empty_manifest() -> MetadataChunk {
        MetadataChunk::new()
    }

    #[test]
    fn rejects_pack_object_size_varint_with_excess_continuations() {
        let header = vec![0x80; 10];
        let err = parse_pack_obj_header(&header).unwrap_err();
        assert!(
            err.to_string().contains("varint")
                && (err.to_string().contains("overflow") || err.to_string().contains("truncated")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn hostile_pack_object_size_cannot_reserve_beyond_pack_bytes() {
        assert_eq!(pack_object_initial_capacity(u64::MAX, 4096), 4096);
        assert_eq!(pack_object_initial_capacity(128, 4096), 128);
    }

    fn extract_manifest(
        manifest: &MetadataChunk,
        target: &Path,
        archive_chunks: Vec<Vec<u8>>,
    ) -> Result<ExtractStats> {
        let manifest_path = target.join("manifest.pb");
        {
            let mut f = File::create(&manifest_path)?;
            manifest.write(&mut f)?;
        }
        extract_archive_with_chunk_fetcher(
            &manifest_path,
            Some(target),
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
    fn rejects_malformed_blob_sha1() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("out");
        std::fs::create_dir(&target).unwrap();

        let mut bad_sha1 = sha1_bytes(b"").to_vec();
        bad_sha1.truncate(19);

        let mut manifest = empty_manifest();
        manifest.files.push(FileEntry {
            path: b"bad.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: bad_sha1,
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

        let err = extract_manifest(&manifest, &target, vec![vec![]])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("invalid blob_sha1"),
            "expected malformed blob_sha1 error, got: {}",
            err
        );
    }

    /// Negative: content that does not match the recorded blob_sha1 must fail.
    #[test]
    fn rejects_blob_sha1_mismatch() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("out");
        std::fs::create_dir(&target).unwrap();

        let mut manifest = empty_manifest();
        manifest.files.push(FileEntry {
            path: b"x.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(b"not-the-frame-content").to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 3,
            }],
        });
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 3,
            raw_len: 3,
        });

        assert!(extract_manifest(&manifest, &target, vec![vec![b'y'; 3]]).is_err());
    }

    #[test]
    fn rejects_pack_object_size_varint_overflow() {
        let mut header = vec![0xb0]; // blob object, continuation bit set
        header.extend(std::iter::repeat_n(0x80, 8));
        header.push(0x10); // only four bits remain at this position

        let err = parse_pack_obj_header(&header).unwrap_err();
        assert!(
            err.to_string().contains("overflows u64"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn hostile_pack_object_size_cannot_reserve_beyond_pack_bytes() {
        assert_eq!(pack_object_initial_capacity(u64::MAX, 4096), 4096);
        assert_eq!(pack_object_initial_capacity(128, 4096), 128);
    }
}
