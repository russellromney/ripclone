use crate::cas::Cas;
use crate::clonepack::{FileEntry, Fragment, FrameInfo};
use crate::manifest::MetadataChunk;
use anyhow::{Context, Result};
use fastcdc::v2020::StreamCDC;
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// Default compressed size cap for a single archive chunk.
pub const DEFAULT_ARCHIVE_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

/// Content-defined chunking bounds for the worktree stream (fastcdc v2020).
/// Boundaries depend on content, not cumulative position, so a localized edit
/// only re-chunks nearby frames — every other frame stays byte-identical and can
/// be reused without recompression. Frames stay in path order (the stream is the
/// path-ordered concatenation of file contents), so there is no compression
/// penalty vs fixed framing.
const CDC_MIN: usize = 1024 * 1024;
const CDC_AVG: usize = 4 * 1024 * 1024;
const CDC_MAX: usize = 16 * 1024 * 1024;

/// How many raw frame bytes to buffer before compressing a batch in parallel.
/// The worktree is streamed through the chunker one frame at a time, so peak
/// memory is bounded to roughly this batch plus the largest single blob —
/// instead of the whole worktree at once — while still handing rayon enough
/// frames per batch to keep every core busy.
const STREAM_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Compress one CDC frame with zstd (optionally with a trained dictionary).
/// Empty frames stay empty. Frames are independent, so this is the unit of both
/// parallel compression and content-addressed reuse.
fn compress_frame(raw: &[u8], level: i32, dictionary: Option<&[u8]>) -> Result<Vec<u8>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    match dictionary {
        Some(dict) => zstd::bulk::Compressor::with_dictionary(level, dict)
            .context("create zstd compressor with dictionary")?
            .compress(raw)
            .context("zstd compress frame with dictionary"),
        None => zstd::encode_all(raw, level).context("zstd compress frame"),
    }
}

/// Cut `stream` into content-defined frames; returns each frame's `(start, end)`
/// byte range. An empty stream yields a single empty frame so empty-file
/// fragments always have a frame to point at. Production code streams the tree
/// through [`StreamCDC`] instead (see [`ArchiveBuilder::stream_cdc`]); this
/// slice-based form is kept as the reference the tests check streaming against.
#[cfg(test)]
fn cdc_frame_bounds(stream: &[u8]) -> Vec<(usize, usize)> {
    if stream.is_empty() {
        return vec![(0, 0)];
    }
    fastcdc::v2020::FastCDC::new(stream, CDC_MIN, CDC_AVG, CDC_MAX)
        .map(|c| (c.offset, c.offset + c.length))
        .collect()
}

/// Map a file's byte range `[start, start+len)` in the stream to fragments over
/// the CDC frames. `frame_hint` is the first frame index that might overlap
/// (monotonically advanced by the caller, since files are processed in stream
/// order). Returns the fragments and the updated hint.
fn fragments_for(
    bounds: &[(usize, usize)],
    start: usize,
    len: usize,
    mut hint: usize,
) -> (Vec<Fragment>, usize) {
    // Advance past frames that end at or before this file's start.
    while hint < bounds.len() && bounds[hint].1 <= start && len > 0 {
        hint += 1;
    }
    // For an empty file, point at the frame that contains `start` (or the last).
    if len == 0 {
        let mut idx = hint.min(bounds.len().saturating_sub(1));
        while idx + 1 < bounds.len() && bounds[idx].1 <= start {
            idx += 1;
        }
        let (frs, _) = bounds[idx];
        return (
            vec![Fragment {
                frame_index: idx as u32,
                frame_offset: start.saturating_sub(frs) as u32,
                raw_len: 0,
            }],
            hint,
        );
    }
    let end = start + len;
    let mut frags = Vec::new();
    let mut k = hint;
    while k < bounds.len() && bounds[k].0 < end {
        let (frs, fre) = bounds[k];
        let os = start.max(frs);
        let oe = end.min(fre);
        if oe > os {
            frags.push(Fragment {
                frame_index: k as u32,
                frame_offset: (os - frs) as u32,
                raw_len: (oe - os) as u32,
            });
        }
        k += 1;
    }
    (frags, hint)
}

pub struct ArchiveStats {
    pub files: usize,
    pub frames: usize,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
}

pub struct ArchiveBuilder {
    mirror: PathBuf,
}

impl ArchiveBuilder {
    pub fn new<P: AsRef<Path>>(mirror: P) -> Self {
        Self {
            mirror: mirror.as_ref().to_path_buf(),
        }
    }

    /// Build a working-tree archive and its manifest for `commit`.
    ///
    /// Walks the git tree directly so the archive contains every tracked file,
    /// including files that `git archive` would omit because of `export-ignore`
    /// attributes. Modes, symlinks, and path encoding are preserved exactly.
    ///
    /// This produces a single on-disk archive file; use `build_chunks` if you
    /// want content-addressed archive chunks instead.
    pub fn build(
        &self,
        commit: &str,
        archive_path: &Path,
        manifest_path: &Path,
        level: i32,
        dictionary: Option<&[u8]>,
    ) -> Result<ArchiveStats> {
        let (manifest, chunks, stats) = self.build_chunks(commit, level, dictionary, u64::MAX)?;
        if chunks.len() != 1 {
            anyhow::bail!(
                "expected single archive chunk for file output, got {}",
                chunks.len()
            );
        }
        std::fs::write(archive_path, &chunks[0])
            .with_context(|| format!("write archive {}", archive_path.display()))?;
        let mut manifest_file = File::create(manifest_path)
            .with_context(|| format!("create manifest {}", manifest_path.display()))?;
        manifest.write(&mut manifest_file)?;
        manifest_file.flush().context("flush manifest")?;
        Ok(stats)
    }

    /// Build the working tree into content-addressed archive chunks.
    ///
    /// Returns the metadata chunk (containing the frame and file tables) and a
    /// vector of raw archive chunk byte vectors. `target_chunk_size` caps the
    /// compressed size of each chunk; a frame larger than the target gets its
    /// own chunk.
    pub fn build_chunks(
        &self,
        commit: &str,
        level: i32,
        dictionary: Option<&[u8]>,
        target_chunk_size: u64,
    ) -> Result<(MetadataChunk, Vec<Vec<u8>>, ArchiveStats)> {
        if !self.mirror.exists() {
            anyhow::bail!("mirror not found: {}", self.mirror.display());
        }

        let repo = git2::Repository::open_bare(&self.mirror)
            .with_context(|| format!("open bare repo {}", self.mirror.display()))?;
        let oid = repo
            .revparse_single(commit)
            .with_context(|| format!("resolve commit {}", commit))?
            .id();
        let commit_obj = repo
            .find_commit(oid)
            .with_context(|| format!("find commit {}", oid))?;
        let tree = commit_obj
            .tree()
            .with_context(|| format!("read commit tree for {}", oid))?;

        // Stream the path-ordered worktree through the chunker, compressing each
        // batch of frames in parallel. The whole worktree is never held in
        // memory — peak stays ~one batch + the largest blob. Frames are
        // independent, so compression parallelizes cleanly within a batch.
        let (mut manifest, bounds, raw_total, compressed_frames) =
            self.stream_cdc(&repo, &tree, |batch| {
                use rayon::prelude::*;
                batch
                    .par_iter()
                    .map(|&(_, raw)| compress_frame(raw, level, dictionary))
                    .collect::<Result<Vec<_>>>()
            })?;

        // Assemble compressed frames into chunks of ~target_chunk_size, recording
        // each frame's placement.
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut current_chunk: Vec<u8> = Vec::new();
        for (i, compressed) in compressed_frames.iter().enumerate() {
            if !current_chunk.is_empty()
                && current_chunk.len() as u64 + compressed.len() as u64 > target_chunk_size
            {
                chunks.push(std::mem::take(&mut current_chunk));
            }
            manifest.frames.push(FrameInfo {
                chunk_index: chunks.len() as u32,
                chunk_offset: current_chunk.len() as u64,
                compressed_len: compressed.len() as u32,
                raw_len: (bounds[i].1 - bounds[i].0) as u32,
            });
            current_chunk.extend_from_slice(compressed);
            if current_chunk.len() as u64 >= target_chunk_size {
                chunks.push(std::mem::take(&mut current_chunk));
            }
        }
        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        let compressed_total: u64 = chunks.iter().map(|c| c.len() as u64).sum();
        let files = manifest.files.len();
        let frames = manifest.frames.len();

        Ok((
            manifest,
            chunks,
            ArchiveStats {
                files,
                frames,
                raw_bytes: raw_total,
                compressed_bytes: compressed_total,
            },
        ))
    }

    /// Stream the path-ordered worktree through content-defined chunking without
    /// ever materializing the whole worktree in memory.
    ///
    /// Blobs are served one at a time (peak memory is the largest single blob,
    /// not the sum of all files) and cut into frames by [`StreamCDC`], which
    /// produces byte-identical boundaries to the slice-based chunker for the same
    /// content. Frames are handed to `process_batch` in batches bounded by
    /// [`STREAM_BATCH_BYTES`], so the per-frame work (compression, hashing) can
    /// run in parallel while peak memory stays ~one batch + one blob.
    ///
    /// Returns `(manifest_with_files, bounds, raw_total, outputs)` where
    /// `bounds[i] = (start, end)` is frame i's byte range, the manifest has its
    /// `files` (with fragments) populated but `frames` empty, and `outputs[i]` is
    /// `process_batch`'s result for frame i (one per frame, in order).
    #[allow(clippy::type_complexity)]
    fn stream_cdc<T, P>(
        &self,
        repo: &git2::Repository,
        tree: &git2::Tree,
        mut process_batch: P,
    ) -> Result<(MetadataChunk, Vec<(usize, usize)>, u64, Vec<T>)>
    where
        P: FnMut(&[(usize, &[u8])]) -> Result<Vec<T>>,
    {
        // Enumerate blobs (raw paths, oids, modes) up front — cheap metadata, no
        // content. Raw `name_bytes()` paths keep the file table byte-exact for
        // non-UTF8 names (matching the worktree the client extracts).
        let mut blobs: Vec<(Vec<u8>, git2::Oid, u32)> = Vec::new();
        collect_blobs_raw(repo, tree, &[], &mut blobs).context("walk git tree for archive")?;

        // The reader records each file's (path, mode, sha1) + byte range as it
        // serves blob bytes; we recover that table after the stream is drained.
        let rec: FileRec = Rc::new(RefCell::new(FileTable::default()));
        let reader = TreeBlobReader::new(repo, blobs, rec.clone());

        let mut bounds: Vec<(usize, usize)> = Vec::new();
        let mut outputs: Vec<T> = Vec::new();
        let mut batch: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut batch_bytes = 0usize;

        for chunk in StreamCDC::new(reader, CDC_MIN, CDC_AVG, CDC_MAX) {
            let chunk = chunk.map_err(|e| anyhow::anyhow!("content-defined chunking: {e}"))?;
            let start = chunk.offset as usize;
            bounds.push((start, start + chunk.length));
            batch_bytes += chunk.data.len();
            batch.push((bounds.len() - 1, chunk.data));
            if batch_bytes >= STREAM_BATCH_BYTES {
                flush_batch(&mut batch, &mut outputs, &mut process_batch)?;
                batch_bytes = 0;
            }
        }
        flush_batch(&mut batch, &mut outputs, &mut process_batch)?;

        // Propagate any blob-read error the reader hit (surfaced as io::Other).
        let table = Rc::try_unwrap(rec)
            .map_err(|_| anyhow::anyhow!("file table still referenced after stream"))?
            .into_inner();
        if let Some(err) = table.err {
            return Err(err).context("build archive from tree");
        }
        let (mut files, ranges) = (table.files, table.ranges);
        let raw_total = ranges.last().map(|&(s, l)| (s + l) as u64).unwrap_or(0);

        // Empty worktree: synthesize one empty frame so empty-file fragments
        // always have a frame to point at (matches the slice-based chunker).
        if bounds.is_empty() {
            bounds.push((0, 0));
            let res = process_batch(&[(0, &[][..])])?;
            anyhow::ensure!(
                res.len() == 1,
                "process_batch returned {} outputs for the empty frame",
                res.len()
            );
            outputs.extend(res);
        }

        // Map each file's byte range to fragments (two-pointer; files and frames
        // are both in increasing stream order).
        let mut hint = 0usize;
        for (i, &(start, len)) in ranges.iter().enumerate() {
            let (frags, new_hint) = fragments_for(&bounds, start, len, hint);
            hint = new_hint;
            files[i].fragments = frags;
        }

        let mut manifest = MetadataChunk::new();
        manifest.files = files;
        Ok((manifest, bounds, raw_total, outputs))
    }

    /// Convenience: build from a repo string "owner/repo" by locating the bare
    /// mirror under `repo_root`.
    pub fn build_repo(
        repo_root: &Path,
        owner: &str,
        repo_name: &str,
        commit: &str,
        archive_path: &Path,
        manifest_path: &Path,
        level: i32,
        dictionary: Option<&[u8]>,
    ) -> Result<ArchiveStats> {
        let mirror = repo_root.join(format!("{}_{}.git", owner, repo_name));
        let builder = ArchiveBuilder::new(&mirror);
        builder.build(commit, archive_path, manifest_path, level, dictionary)
    }

    /// Build only the metadata *files table* (path, mode, blob sha1) — no zstd
    /// frames. Editable clones materialize the worktree from the HEAD-closure
    /// packs, not the archive, so they only need this table; the expensive frame
    /// compression ([`build_chunks`]) is only needed for files mode. This still
    /// reads each blob (to hash it) but skips compression, so it is much cheaper
    /// — letting two-phase publish depth=1 without waiting on the archive.
    /// `frames` is left empty.
    pub fn build_files_table(&self, commit: &str) -> Result<MetadataChunk> {
        if !self.mirror.exists() {
            anyhow::bail!("mirror not found: {}", self.mirror.display());
        }
        let repo = git2::Repository::open_bare(&self.mirror)
            .with_context(|| format!("open bare repo {}", self.mirror.display()))?;
        let oid = repo
            .revparse_single(commit)
            .with_context(|| format!("resolve commit {}", commit))?
            .id();
        let tree = repo
            .find_commit(oid)
            .with_context(|| format!("find commit {}", oid))?
            .tree()
            .with_context(|| format!("read commit tree for {}", oid))?;

        let mut manifest = MetadataChunk::new();
        let mut walk_err: Option<anyhow::Error> = None;
        tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
            if walk_err.is_some() {
                return git2::TreeWalkResult::Skip;
            }
            if entry.kind() != Some(git2::ObjectType::Blob) {
                return git2::TreeWalkResult::Ok;
            }
            let name = entry.name().unwrap_or("");
            let path = if root.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", root.trim_end_matches('/'), name)
            };
            let mode = entry.filemode_raw() as u32;
            let obj = match entry.to_object(&repo) {
                Ok(o) => o,
                Err(e) => {
                    walk_err =
                        Some(anyhow::Error::from(e).context(format!("read object for {}", path)));
                    return git2::TreeWalkResult::Skip;
                }
            };
            match obj.as_blob() {
                Some(blob) => manifest.files.push(FileEntry {
                    path: path.into_bytes(),
                    mode,
                    blob_sha1: sha1_bytes(blob.content()).to_vec(),
                    fragments: Vec::new(),
                }),
                None => {
                    walk_err = Some(anyhow::anyhow!(
                        "expected blob for {} but got {:?}",
                        path,
                        obj.kind()
                    ));
                    return git2::TreeWalkResult::Skip;
                }
            }
            git2::TreeWalkResult::Ok
        })
        .context("walk git tree")?;
        if let Some(err) = walk_err {
            return Err(err).context("build files table from tree");
        }
        Ok(manifest)
    }

    /// Incremental files table: like [`build_files_table`] but reuses the prior
    /// table's content hash for every path NOT in `changed`, so only changed or
    /// added blobs are read + hashed. `changed` is the set of paths that differ
    /// from the previously synced commit (see [`crate::git::diff_name_set`]);
    /// `prev_files` is the prior sync's files table. Paths are encoded exactly as
    /// in [`build_files_table`] so prior entries match by path. Falls back to a
    /// full hash for any path not found in the prior table.
    ///
    /// This is O(changed) blob reads instead of O(worktree), which is the bulk of
    /// the depth=1 files-table cost on a re-sync.
    pub fn build_files_table_incremental(
        &self,
        commit: &str,
        prev_files: &[FileEntry],
        changed: &std::collections::HashSet<Vec<u8>>,
    ) -> Result<MetadataChunk> {
        if !self.mirror.exists() {
            anyhow::bail!("mirror not found: {}", self.mirror.display());
        }
        // Prior path -> content hash. Mode is always taken from the current tree
        // walk (so a mode-only change is picked up even if we reuse the hash).
        let prev: std::collections::HashMap<&[u8], &[u8]> = prev_files
            .iter()
            .map(|f| (f.path.as_slice(), f.blob_sha1.as_slice()))
            .collect();

        let repo = git2::Repository::open_bare(&self.mirror)
            .with_context(|| format!("open bare repo {}", self.mirror.display()))?;
        let oid = repo
            .revparse_single(commit)
            .with_context(|| format!("resolve commit {}", commit))?
            .id();
        let tree = repo
            .find_commit(oid)
            .with_context(|| format!("find commit {}", oid))?
            .tree()
            .with_context(|| format!("read commit tree for {}", oid))?;

        // Manual recursion so paths are built from RAW bytes (`name_bytes()`) at
        // every level. This is critical for correctness: `changed` comes from
        // `git diff -z` (raw, unquoted bytes), so a raw walk path matches it
        // exactly. (git2's `tree.walk` hands back the directory prefix as a lossy
        // `&str`, which could fail to match a non-UTF8 changed path and wrongly
        // reuse a stale content hash.) For a path not present in `changed`, we
        // reuse the prior hash when the path is found in the prior table, and
        // otherwise read+hash the blob — so a non-UTF8 path simply re-hashes
        // (safe), never reuses a stale hash.
        let mut blobs: Vec<(Vec<u8>, git2::Oid, u32)> = Vec::new();
        collect_blobs_raw(&repo, &tree, &[], &mut blobs)
            .context("walk git tree for incremental files table")?;

        let mut manifest = MetadataChunk::new();
        for (path, blob_oid, mode) in blobs {
            let reused = if changed.contains(&path) {
                None
            } else {
                prev.get(path.as_slice()).map(|h| h.to_vec())
            };
            let blob_sha1 = match reused {
                Some(h) => h,
                None => {
                    let blob = repo
                        .find_blob(blob_oid)
                        .with_context(|| format!("read blob {} for {:?}", blob_oid, path))?;
                    sha1_bytes(blob.content()).to_vec()
                }
            };
            manifest.files.push(FileEntry {
                path,
                mode,
                blob_sha1,
                fragments: Vec::new(),
            });
        }
        Ok(manifest)
    }

    /// Build the archive chunks and store them in `cas`.
    ///
    /// The returned metadata chunk contains only the frame/file tables; the
    /// caller is expected to add the .git artifacts and store the final metadata
    /// chunk itself.
    ///
    /// Returns `(archive_chunk_hashes, metadata_chunk)`.
    pub fn build_into_cas(
        &self,
        commit: &str,
        cas: &Cas,
        level: i32,
        dictionary: Option<&[u8]>,
    ) -> Result<(Vec<String>, MetadataChunk)> {
        let (metadata, chunks, _stats) =
            self.build_chunks(commit, level, dictionary, DEFAULT_ARCHIVE_CHUNK_SIZE)?;
        let mut archive_chunk_hashes = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            archive_chunk_hashes.push(cas.put(&chunk)?);
        }
        Ok((archive_chunk_hashes, metadata))
    }

    /// Build the archive with per-frame incremental reuse: one content-addressed
    /// compressed chunk per CDC frame. A frame whose raw bytes are unchanged
    /// (found in `prev` by raw-bytes hash) reuses the prior compressed chunk —
    /// no recompression, no re-upload. Returns `(all_chunk_hashes, metadata,
    /// new_chunk_hashes, frames)`: every frame's chunk (manifest order), the
    /// metadata (frames + files), only the freshly built chunks (to upload), and
    /// the frame index to persist for the next sync.
    ///
    /// `prev` maps a frame's `raw_hash` to its prior `(chunk_hash,
    /// compressed_len)`. Reused chunks are already durable in storage.
    pub fn build_into_cas_incremental(
        &self,
        commit: &str,
        cas: &Cas,
        level: i32,
        dictionary: Option<&[u8]>,
        prev: &std::collections::HashMap<String, (String, u64)>,
    ) -> Result<(
        Vec<String>,
        MetadataChunk,
        Vec<String>,
        Vec<crate::ArchiveFrame>,
    )> {
        if !self.mirror.exists() {
            anyhow::bail!("mirror not found: {}", self.mirror.display());
        }
        let repo = git2::Repository::open_bare(&self.mirror)
            .with_context(|| format!("open bare repo {}", self.mirror.display()))?;
        let oid = repo
            .revparse_single(commit)
            .with_context(|| format!("resolve commit {}", commit))?
            .id();
        let tree = repo
            .find_commit(oid)
            .with_context(|| format!("find commit {}", oid))?
            .tree()
            .with_context(|| format!("read commit tree for {}", oid))?;

        // Stream the worktree through the chunker. For each frame, hash the raw
        // bytes (the reuse key) and compress only frames not already in `prev` —
        // in parallel per batch, never holding the whole worktree in memory.
        let (mut manifest, bounds, _raw_total, processed) =
            self.stream_cdc(&repo, &tree, |batch| {
                use rayon::prelude::*;
                batch
                    .par_iter()
                    .map(|&(_, raw)| -> Result<(String, Option<Vec<u8>>)> {
                        let h = crate::cas::hash(raw);
                        if prev.contains_key(&h) {
                            Ok((h, None)) // reuse — skip compression
                        } else {
                            Ok((h, Some(compress_frame(raw, level, dictionary)?)))
                        }
                    })
                    .collect::<Result<Vec<_>>>()
            })?;

        let mut all_chunks = Vec::with_capacity(bounds.len());
        let mut new_chunks = Vec::new();
        let mut frames = Vec::with_capacity(bounds.len());
        for i in 0..bounds.len() {
            let raw_len = (bounds[i].1 - bounds[i].0) as u64;
            let (raw_hash, comp_opt) = &processed[i];
            let (chunk_hash, compressed_len) = match prev.get(raw_hash) {
                Some((ch, cl)) => (ch.clone(), *cl),
                None => {
                    let comp = comp_opt.as_ref().expect("non-reused frame compressed");
                    let ch = cas.put(comp)?;
                    new_chunks.push(ch.clone());
                    (ch, comp.len() as u64)
                }
            };
            // One chunk per frame: chunk_index == frame index, offset 0.
            manifest.frames.push(FrameInfo {
                chunk_index: i as u32,
                chunk_offset: 0,
                compressed_len: compressed_len as u32,
                raw_len: raw_len as u32,
            });
            all_chunks.push(chunk_hash.clone());
            frames.push(crate::ArchiveFrame {
                raw_hash: raw_hash.clone(),
                chunk_hash,
                compressed_len,
                raw_len,
            });
        }
        Ok((all_chunks, manifest, new_chunks, frames))
    }
}

pub fn sha1_bytes(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}

/// Run `process_batch` over one batch of frames and append the results to
/// `outputs` in frame order, then clear `batch`. Used by [`ArchiveBuilder::stream_cdc`]
/// to flush a bounded window of frames (mid-stream and at the end).
fn flush_batch<T, P>(
    batch: &mut Vec<(usize, Vec<u8>)>,
    outputs: &mut Vec<T>,
    process_batch: &mut P,
) -> Result<()>
where
    P: FnMut(&[(usize, &[u8])]) -> Result<Vec<T>>,
{
    if batch.is_empty() {
        return Ok(());
    }
    let view: Vec<(usize, &[u8])> = batch.iter().map(|(i, d)| (*i, d.as_slice())).collect();
    let res = process_batch(&view)?;
    anyhow::ensure!(
        res.len() == view.len(),
        "process_batch returned {} outputs for {} frames",
        res.len(),
        view.len()
    );
    outputs.extend(res);
    batch.clear();
    Ok(())
}

/// File table accumulated by [`TreeBlobReader`] while it streams blob bytes: one
/// [`FileEntry`] per blob (fragments filled in later) and the parallel
/// `(start, len)` byte range of each file in the stream. `err` holds the first
/// blob-read failure, since the `Read` trait can only surface an `io::Error`.
#[derive(Default)]
struct FileTable {
    files: Vec<FileEntry>,
    ranges: Vec<(usize, usize)>,
    err: Option<anyhow::Error>,
}

type FileRec = Rc<RefCell<FileTable>>;

/// A [`Read`] over the path-ordered concatenation of a tree's blob contents.
/// Loads one blob at a time (peak memory is the largest blob, never the whole
/// worktree) and records each file's entry + byte range into the shared
/// [`FileTable`] as it serves bytes. A blob-read error is stored in the table
/// and surfaced as an `io::Error` so the streaming chunker stops.
struct TreeBlobReader<'r> {
    repo: &'r git2::Repository,
    blobs: std::vec::IntoIter<(Vec<u8>, git2::Oid, u32)>,
    cur: Vec<u8>,
    pos: usize,
    next_start: usize,
    rec: FileRec,
}

impl<'r> TreeBlobReader<'r> {
    fn new(
        repo: &'r git2::Repository,
        blobs: Vec<(Vec<u8>, git2::Oid, u32)>,
        rec: FileRec,
    ) -> Self {
        Self {
            repo,
            blobs: blobs.into_iter(),
            cur: Vec::new(),
            pos: 0,
            next_start: 0,
            rec,
        }
    }

    /// Load the next blob into `cur`, recording its file entry + byte range.
    /// Returns Ok(true) if a blob was loaded (possibly empty), Ok(false) at EOF.
    fn load_next(&mut self) -> Result<bool> {
        let (path, oid, mode) = match self.blobs.next() {
            Some(b) => b,
            None => return Ok(false),
        };
        let content = self
            .repo
            .find_blob(oid)
            .with_context(|| format!("read blob {} for {:?}", oid, String::from_utf8_lossy(&path)))?
            .content()
            .to_vec();
        let start = self.next_start;
        let len = content.len();
        {
            let mut t = self.rec.borrow_mut();
            t.files.push(FileEntry {
                path,
                mode,
                blob_sha1: sha1_bytes(&content).to_vec(),
                fragments: Vec::new(),
            });
            t.ranges.push((start, len));
        }
        self.next_start = start + len;
        self.cur = content;
        self.pos = 0;
        Ok(true)
    }
}

impl Read for TreeBlobReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.pos < self.cur.len() {
                let n = (self.cur.len() - self.pos).min(buf.len());
                buf[..n].copy_from_slice(&self.cur[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            match self.load_next() {
                Ok(true) => continue,
                Ok(false) => return Ok(0),
                Err(e) => {
                    self.rec.borrow_mut().err = Some(e);
                    return Err(std::io::Error::other("blob read failed"));
                }
            }
        }
    }
}

/// Recursively collect every blob in `tree` as `(raw_path_bytes, blob_oid,
/// mode)` in pre-order. Paths are built from `name_bytes()` at every level so
/// they are byte-exact (no lossy UTF-8), matching `git diff -z` output.
/// Directories recurse; submodules (commit entries) are skipped; symlinks are
/// blobs and are included.
fn collect_blobs_raw(
    repo: &git2::Repository,
    tree: &git2::Tree,
    prefix: &[u8],
    out: &mut Vec<(Vec<u8>, git2::Oid, u32)>,
) -> Result<()> {
    for entry in tree.iter() {
        let mut path = prefix.to_vec();
        if !path.is_empty() {
            path.push(b'/');
        }
        path.extend_from_slice(entry.name_bytes());
        match entry.kind() {
            Some(git2::ObjectType::Tree) => {
                let sub = entry
                    .to_object(repo)
                    .with_context(|| format!("read subtree {:?}", path))?
                    .peel_to_tree()
                    .with_context(|| format!("peel subtree {:?}", path))?;
                collect_blobs_raw(repo, &sub, &path, out)?;
            }
            Some(git2::ObjectType::Blob) => {
                out.push((path, entry.id(), entry.filemode_raw() as u32));
            }
            _ => {} // commit (submodule) or other: not a worktree file
        }
    }
    Ok(())
}

/// Maximum size of a single file sample used for dictionary training.
/// Large binary blobs don't help the dictionary much and slow training down.
const MAX_SAMPLE_FILE_BYTES: usize = 200 * 1024;

/// Train a zstd dictionary from the working-tree files at `commit` in `mirror`.
///
/// `max_size` is the maximum size of the generated dictionary. `sample_bytes`
/// is an approximate cap on how much file data to feed into the trainer.
/// Training is expensive, so this is intended to run once per repo per day, not
/// per clone.
pub fn train_dictionary(
    mirror: &std::path::Path,
    commit: &str,
    max_size: usize,
    sample_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
    if !mirror.exists() {
        anyhow::bail!("mirror not found: {}", mirror.display());
    }

    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    let mut child = std::process::Command::new("git")
        .arg("-C")
        .arg(mirror.as_os_str())
        .args(["archive", "--format=tar", "--end-of-options", commit])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawn git archive for dictionary training")?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing archive stdout"))?;
    let mut tar = tar::Archive::new(stdout);

    let mut samples: Vec<Vec<u8>> = Vec::new();
    let mut total = 0usize;
    let mut finished_early = false;

    for entry in tar.entries().context("read archive entries")? {
        let mut entry = entry.context("tar entry")?;
        let entry_type = entry.header().entry_type();
        if !matches!(
            entry_type,
            tar::EntryType::Regular | tar::EntryType::Symlink
        ) {
            continue;
        }

        let content = if entry_type == tar::EntryType::Symlink {
            let target = entry
                .link_name()
                .with_context(|| "read symlink target")?
                .ok_or_else(|| anyhow::anyhow!("missing symlink target"))?;
            let target = target.to_str().with_context(|| "non-utf8 symlink target")?;
            target.as_bytes().to_vec()
        } else {
            let mut content = Vec::new();
            entry.read_to_end(&mut content).context("read tar entry")?;
            content
        };

        // Skip large individual files; the dictionary is most useful for the
        // long tail of small source/text files.
        if content.len() > MAX_SAMPLE_FILE_BYTES {
            continue;
        }

        if total + content.len() > sample_bytes && !samples.is_empty() {
            finished_early = true;
            break;
        }
        total += content.len();
        samples.push(content);
    }

    // If we stopped reading before git archive finished, kill it so we don't
    // hang waiting for its stdout pipe to drain.
    if finished_early {
        let _ = child.kill();
    }
    let status = child.wait().context("git archive wait")?;
    if !finished_early && !status.success() {
        anyhow::bail!("git archive failed during dictionary training");
    }

    if samples.is_empty() {
        anyhow::bail!("no samples found for dictionary training");
    }

    let dict = zstd::dict::from_samples(&samples, max_size).context("train zstd dictionary")?;
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Incremental archive: identical commit reuses every frame; a small edit at
    /// the end of the stream reuses all earlier (unchanged) frames and rebuilds
    /// only the affected one. Verifies the CDC reuse mechanic.
    #[test]
    fn incremental_archive_reuses_unchanged_frames() {
        use std::collections::HashMap;
        let pr = |len: usize, seed: u8| {
            (0..len)
                .map(|i| (i.wrapping_mul(2654435761) as u8) ^ seed)
                .collect::<Vec<u8>>()
        };
        let tmp = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init_bare(tmp.path()).unwrap();
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let builder = ArchiveBuilder::new(tmp.path());

        // ~20 MiB of incompressible data → several CDC frames; small file last.
        let big = pr(20 * 1024 * 1024, 1);
        let c1 = commit_onto_bytes(&repo, &[(b"a.bin", &big), (b"z.txt", b"hello")]);
        let empty: HashMap<String, (String, u64)> = HashMap::new();
        let (all1, _m1, new1, frames1) = builder
            .build_into_cas_incremental(&c1, &cas, 6, None, &empty)
            .unwrap();
        assert_eq!(new1.len(), all1.len(), "first build: every frame built");
        assert!(frames1.len() >= 2, "20 MiB should span multiple CDC frames");

        let prev: HashMap<String, (String, u64)> = frames1
            .iter()
            .map(|f| (f.raw_hash.clone(), (f.chunk_hash.clone(), f.compressed_len)))
            .collect();

        // Same commit → full reuse.
        let (_a, _m, new1b, _f) = builder
            .build_into_cas_incremental(&c1, &cas, 6, None, &prev)
            .unwrap();
        assert_eq!(new1b.len(), 0, "identical commit reuses all frames");

        // Change only the trailing small file → only the last frame changes.
        let c2 = commit_onto_bytes(&repo, &[(b"a.bin", &big), (b"z.txt", b"changed!")]);
        let (all2, _m2, new2, _f2) = builder
            .build_into_cas_incremental(&c2, &cas, 6, None, &prev)
            .unwrap();
        assert!(
            new2.len() < all2.len(),
            "re-sync reuses unchanged frames (built {} of {})",
            new2.len(),
            all2.len()
        );
        assert!(!new2.is_empty(), "the changed frame is rebuilt");
    }

    fn commit_files(files: &[(&str, &[u8])]) -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init_bare(tmp.path()).unwrap();
        let sig = git2::Signature::now("test", "test@example.com").unwrap();
        let mut idx = repo.index().unwrap();
        let zero_time = git2::IndexTime::new(0, 0);
        for (path, bytes) in files {
            let blob_oid = repo.blob(bytes).unwrap();
            let entry = git2::IndexEntry {
                ctime: zero_time,
                mtime: zero_time,
                dev: 0,
                ino: 0,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                file_size: bytes.len() as u32,
                id: blob_oid,
                flags: 0,
                flags_extended: 0,
                path: path.as_bytes().to_vec(),
            };
            idx.add(&entry).unwrap();
        }
        idx.write().unwrap();
        let tree_id = idx.write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let commit_oid = repo
            .commit(Some("HEAD"), &sig, &sig, "test", &tree, &[])
            .unwrap();
        (tmp, commit_oid.to_string())
    }

    /// Commit `files` onto `repo`'s HEAD (with the current HEAD as parent if any)
    /// and return the new commit oid. Lets a test build a history in one repo.
    fn commit_onto(repo: &git2::Repository, files: &[(&str, &[u8])]) -> String {
        let sig = git2::Signature::now("test", "test@example.com").unwrap();
        let mut idx = repo.index().unwrap();
        let zero = git2::IndexTime::new(0, 0);
        for (path, bytes) in files {
            let blob_oid = repo.blob(bytes).unwrap();
            idx.add(&git2::IndexEntry {
                ctime: zero,
                mtime: zero,
                dev: 0,
                ino: 0,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                file_size: bytes.len() as u32,
                id: blob_oid,
                flags: 0,
                flags_extended: 0,
                path: path.as_bytes().to_vec(),
            })
            .unwrap();
        }
        idx.write().unwrap();
        let tree = repo.find_tree(idx.write_tree().unwrap()).unwrap();
        let parents: Vec<git2::Commit> = match repo.head().ok().and_then(|h| h.target()) {
            Some(t) => vec![repo.find_commit(t).unwrap()],
            None => vec![],
        };
        let parent_refs: Vec<&git2::Commit> = parents.iter().collect();
        repo.commit(Some("HEAD"), &sig, &sig, "c", &tree, &parent_refs)
            .unwrap()
            .to_string()
    }

    /// The incremental files table (prior table + diff) must be byte-identical to
    /// a full rebuild at the new commit. A false "unchanged" would reuse a stale
    /// content hash — silently wrong — so this equivalence is the safety net.
    #[test]
    fn incremental_files_table_matches_full() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init_bare(tmp.path()).unwrap();
        // c1: a, b, dir/c. c2: modify b, add dir/e, remove dir/c, keep a.
        let c1 = commit_onto(
            &repo,
            &[("a.txt", b"aaa"), ("b.txt", b"bbb"), ("dir/c.txt", b"ccc")],
        );
        let c2 = commit_onto(
            &repo,
            &[
                ("a.txt", b"aaa"),
                ("b.txt", b"BBB-changed"),
                ("dir/e.txt", b"eee"),
            ],
        );

        let builder = ArchiveBuilder::new(tmp.path());
        let prev = builder.build_files_table(&c1).unwrap();
        let full = builder.build_files_table(&c2).unwrap();
        let changed = crate::git::diff_name_set(tmp.path(), &c1, &c2).unwrap();
        let inc = builder
            .build_files_table_incremental(&c2, &prev.files, &changed)
            .unwrap();

        let sort = |m: &MetadataChunk| {
            let mut v: Vec<(Vec<u8>, u32, Vec<u8>)> = m
                .files
                .iter()
                .map(|f| (f.path.clone(), f.mode, f.blob_sha1.clone()))
                .collect();
            v.sort();
            v
        };
        assert_eq!(sort(&inc), sort(&full), "incremental table != full table");
        // Sanity: the changed set drove a real diff (b modified, e added, c removed).
        assert!(changed.contains(b"b.txt".as_slice()));
    }

    /// Commit raw-byte paths (so we can use a non-UTF8 filename).
    fn commit_onto_bytes(repo: &git2::Repository, files: &[(&[u8], &[u8])]) -> String {
        let sig = git2::Signature::now("test", "test@example.com").unwrap();
        let mut idx = repo.index().unwrap();
        let zero = git2::IndexTime::new(0, 0);
        for (path, bytes) in files {
            let blob_oid = repo.blob(bytes).unwrap();
            idx.add(&git2::IndexEntry {
                ctime: zero,
                mtime: zero,
                dev: 0,
                ino: 0,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                file_size: bytes.len() as u32,
                id: blob_oid,
                flags: 0,
                flags_extended: 0,
                path: path.to_vec(),
            })
            .unwrap();
        }
        idx.write().unwrap();
        let tree = repo.find_tree(idx.write_tree().unwrap()).unwrap();
        let parents: Vec<git2::Commit> = match repo.head().ok().and_then(|h| h.target()) {
            Some(t) => vec![repo.find_commit(t).unwrap()],
            None => vec![],
        };
        let parent_refs: Vec<&git2::Commit> = parents.iter().collect();
        repo.commit(Some("HEAD"), &sig, &sig, "c", &tree, &parent_refs)
            .unwrap()
            .to_string()
    }

    /// Regression: a non-UTF8 filename whose CONTENT changes must get a fresh
    /// content hash in the incremental table, never the stale prior hash. The
    /// raw-byte walk keeps the walk path byte-equal to `git diff -z` output so the
    /// change is detected. (With a lossy UTF-8 walk key, the changed path would
    /// not match and the old hash would be silently reused.)
    #[test]
    fn incremental_files_table_non_utf8_change_not_stale() {
        let weird: &[u8] = b"caf\xe9.txt"; // invalid UTF-8 (Latin-1 é)
        let tmp = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init_bare(tmp.path()).unwrap();
        let c1 = commit_onto_bytes(&repo, &[(b"a.txt", b"a"), (weird, b"v1")]);
        let c2 = commit_onto_bytes(&repo, &[(b"a.txt", b"a"), (weird, b"v2-changed")]);

        let builder = ArchiveBuilder::new(tmp.path());
        let prev = builder.build_files_table(&c1).unwrap();
        let changed = crate::git::diff_name_set(tmp.path(), &c1, &c2).unwrap();
        let inc = builder
            .build_files_table_incremental(&c2, &prev.files, &changed)
            .unwrap();

        // The diff must report the raw non-UTF8 path.
        assert!(
            changed.contains(weird),
            "diff must report the non-UTF8 changed path"
        );
        // The incremental entry for the raw path must carry the NEW content hash.
        let entry = inc
            .files
            .iter()
            .find(|f| f.path == weird)
            .expect("non-UTF8 entry present with raw path");
        assert_eq!(
            entry.blob_sha1,
            sha1_bytes(b"v2-changed").to_vec(),
            "must be the fresh hash, not the stale v1 hash"
        );
        assert_ne!(
            entry.blob_sha1,
            sha1_bytes(b"v1").to_vec(),
            "must not reuse the stale prior hash"
        );
    }

    #[test]
    fn archive_chunks_respect_target_size() {
        let pseudo_random = |len: usize| {
            (0..len)
                .map(|i| i.wrapping_mul(0x9E3779B9) as u8)
                .collect::<Vec<u8>>()
        };
        let files: Vec<(&str, Vec<u8>)> = vec![
            ("zero.bin", vec![0u8; 3 * 1024 * 1024]),
            ("one.bin", vec![1u8; 5 * 1024 * 1024]),
            ("random_4m.bin", pseudo_random(4 * 1024 * 1024)),
            ("random_8m.bin", pseudo_random(8 * 1024 * 1024)),
            ("big_random.bin", pseudo_random(12 * 1024 * 1024)),
        ];
        let files_ref: Vec<(&str, &[u8])> = files.iter().map(|(p, b)| (*p, b.as_slice())).collect();
        let (tmp, commit) = commit_files(&files_ref);
        let builder = ArchiveBuilder::new(tmp.path());
        let (_metadata, chunks, _stats) = builder
            .build_chunks(&commit, 6, None, 8 * 1024 * 1024)
            .unwrap();
        let target = 8 * 1024 * 1024;
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(
                chunk.len() as u64 <= target,
                "chunk {} exceeds {} byte target: {}",
                i,
                target,
                chunk.len()
            );
        }
    }

    /// Round-trip across multiple frames: content larger than FRAME_MAX is split
    /// into several frames, compressed in parallel, then must extract byte-exact.
    /// Guards the parallel-compression + chunk-assembly refactor.
    #[test]
    fn archive_roundtrip_multiframe() {
        let pr = |len: usize, seed: u8| {
            (0..len)
                .map(|i| (i.wrapping_mul(2654435761) as u8) ^ seed)
                .collect::<Vec<u8>>()
        };
        let files: Vec<(&str, Vec<u8>)> = vec![
            ("a.txt", b"hello world\n".to_vec()),
            ("empty.txt", Vec::new()),
            ("dir/small.bin", pr(4096, 9)),
            ("dir/medium.bin", pr(7 * 1024 * 1024, 3)), // spans 2 frames
            ("big.bin", pr(14 * 1024 * 1024, 7)),       // spans 3 frames
            ("tail.txt", b"after the big blob\n".to_vec()),
        ];
        let files_ref: Vec<(&str, &[u8])> = files.iter().map(|(p, b)| (*p, b.as_slice())).collect();
        let (tmp, commit) = commit_files(&files_ref);

        let out = tempfile::tempdir().unwrap();
        let arch = out.path().join("a.zst");
        let man = out.path().join("a.manifest");
        ArchiveBuilder::new(tmp.path())
            .build(&commit, &arch, &man, 6, None)
            .unwrap();

        let dest = out.path().join("extracted");
        std::fs::create_dir_all(&dest).unwrap();
        crate::extract::extract_archive(&arch, &man, &dest, None, None).unwrap();

        for (name, content) in &files {
            let got = std::fs::read(dest.join(name)).unwrap_or_else(|e| panic!("read {name}: {e}"));
            assert_eq!(&got, content, "roundtrip mismatch for {name}");
        }
    }

    /// The streaming chunker must produce byte-identical frame boundaries to the
    /// slice-based chunker over the same path-ordered worktree. This is what lets
    /// us drop the whole-worktree buffer without invalidating already-stored
    /// archives or per-frame reuse keys.
    #[test]
    fn stream_cdc_bounds_match_slice_cdc() {
        // High-entropy xorshift bytes so CDC finds real content-defined cut
        // points (not just max-size cuts) — that's what makes the streaming vs.
        // slice boundary equivalence a meaningful check.
        let pr = |len: usize, seed: u64| {
            let mut s = seed ^ 0x9E3779B97F4A7C15;
            (0..len)
                .map(|_| {
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    (s >> 33) as u8
                })
                .collect::<Vec<u8>>()
        };
        let files: Vec<(&str, Vec<u8>)> = vec![
            ("a.txt", b"hello\n".to_vec()),
            ("empty.txt", Vec::new()),
            ("dir/m.bin", pr(7 * 1024 * 1024, 3)),
            ("big.bin", pr(20 * 1024 * 1024, 7)),
            ("z.txt", b"tail\n".to_vec()),
        ];
        let files_ref: Vec<(&str, &[u8])> = files.iter().map(|(p, b)| (*p, b.as_slice())).collect();
        let (tmp, commit) = commit_files(&files_ref);

        let builder = ArchiveBuilder::new(tmp.path());
        let repo = git2::Repository::open_bare(tmp.path()).unwrap();
        let tree = repo
            .find_commit(repo.revparse_single(&commit).unwrap().id())
            .unwrap()
            .tree()
            .unwrap();

        // Streaming bounds (process closure is a no-op: one unit per frame).
        let (_m, stream_bounds, raw_total, outputs) = builder
            .stream_cdc(&repo, &tree, |batch| Ok(vec![(); batch.len()]))
            .unwrap();
        assert_eq!(outputs.len(), stream_bounds.len(), "one output per frame");

        // Reference: concatenate the same blobs in walk order and slice-CDC them.
        let mut blobs = Vec::new();
        collect_blobs_raw(&repo, &tree, &[], &mut blobs).unwrap();
        let mut full = Vec::new();
        for (_p, oid, _mode) in &blobs {
            full.extend_from_slice(repo.find_blob(*oid).unwrap().content());
        }
        let ref_bounds = cdc_frame_bounds(&full);

        assert_eq!(
            stream_bounds, ref_bounds,
            "streaming CDC boundaries must match slice CDC"
        );
        assert_eq!(
            raw_total,
            full.len() as u64,
            "raw_total must match stream len"
        );
        assert!(
            stream_bounds.len() >= 3,
            "test data should span many frames"
        );
    }

    /// A non-UTF8 filename must survive into the archive manifest byte-exact (the
    /// streaming walk uses raw `name_bytes()` paths, not lossy UTF-8), so the
    /// client materializes the right path.
    #[test]
    fn archive_manifest_preserves_non_utf8_path() {
        let weird: &[u8] = b"caf\xe9.txt"; // invalid UTF-8 (Latin-1 é)
        let tmp = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init_bare(tmp.path()).unwrap();
        let commit = commit_onto_bytes(&repo, &[(b"a.txt", b"hi"), (weird, b"payload")]);

        let (manifest, _chunks, _stats) = ArchiveBuilder::new(tmp.path())
            .build_chunks(&commit, 6, None, u64::MAX)
            .unwrap();

        assert!(
            manifest.files.iter().any(|f| f.path == weird),
            "archive manifest must keep the raw non-UTF8 path"
        );
    }
}
