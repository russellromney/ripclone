use crate::cas::Cas;
use crate::clonepack::{FileEntry, Fragment, FrameInfo};
use crate::manifest::MetadataChunk;
use anyhow::{Context, Result};
use fastcdc::v2020::StreamCDC;
use rayon::prelude::*;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

/// Default compressed size of an archive download bundle.
pub const DEFAULT_ARCHIVE_CHUNK_SIZE: u64 = 1024 * 1024;

/// Content-defined chunking bounds for the worktree stream (fastcdc v2020).
/// Boundaries depend on content, not cumulative position, so a localized edit
/// only re-chunks nearby frames — every other frame stays byte-identical and can
/// be reused without recompression. Frames stay in path order (the stream is the
/// path-ordered concatenation of file contents), so there is no compression
/// penalty vs fixed framing.
#[derive(Clone, Copy, Debug)]
struct CdcBounds {
    min: usize,
    avg: usize,
    max: usize,
}

/// The production bounds. Only tests build with others.
const CDC: CdcBounds = CdcBounds {
    min: 256 * 1024,
    avg: 1024 * 1024,
    max: 4 * 1024 * 1024,
};

/// Raw frame bytes the chunker hands the compressor at once. One batch is
/// compressed while the next is cut and the reader loads ahead, so peak
/// memory is a few batches and windows, never the whole worktree.
const STREAM_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// Raw blob bytes the reader loads per window, several blobs at a time.
const READ_WINDOW_BYTES: usize = 16 * 1024 * 1024;

/// Changed bytes an incremental build reads into memory at once. Gaps are
/// read, cut and compressed in batches of about this size; a single larger
/// gap makes the build stream the whole worktree instead.
const MAX_GAP_BYTES: usize = 128 * 1024 * 1024;

/// A blob in tree order: raw path, object id, git mode.
type Blob = (Vec<u8>, gix::hash::ObjectId, u32);

/// Compress one CDC frame with zstd. Empty frames stay empty. Frames are
/// independent, so this is the unit of both parallel compression and
/// content-addressed reuse.
fn compress_frame(raw: &[u8], level: i32) -> Result<Vec<u8>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    zstd::encode_all(raw, level).context("zstd compress frame")
}

/// Cut `stream` into content-defined frames; returns each frame's `(start, end)`
/// byte range. An empty stream yields a single empty frame so empty-file
/// fragments always have a frame to point at. Production code streams the tree
/// through [`StreamCDC`] instead; this slice-based form is the reference the
/// tests check streaming against.
#[cfg(test)]
fn cdc_frame_bounds(stream: &[u8]) -> Vec<(usize, usize)> {
    if stream.is_empty() {
        return vec![(0, 0)];
    }
    fastcdc::v2020::FastCDC::new(stream, CDC.min, CDC.avg, CDC.max)
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
) -> Result<(Vec<Fragment>, usize)> {
    if bounds.is_empty() {
        anyhow::bail!("cannot map file range onto an empty frame table");
    }
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
        return Ok((
            vec![Fragment {
                frame_index: u32::try_from(idx).context("archive has too many frames")?,
                frame_offset: u32::try_from(start.saturating_sub(frs))
                    .context("empty file frame offset is too large")?,
                raw_len: 0,
            }],
            hint,
        ));
    }
    let end = start.checked_add(len).context("file range end overflow")?;
    let mut frags = Vec::new();
    let mut k = hint;
    while k < bounds.len() && bounds[k].0 < end {
        let (frs, fre) = bounds[k];
        let os = start.max(frs);
        let oe = end.min(fre);
        if oe > os {
            frags.push(Fragment {
                frame_index: u32::try_from(k).context("archive has too many frames")?,
                frame_offset: u32::try_from(os - frs)
                    .context("file fragment frame offset is too large")?,
                raw_len: u32::try_from(oe - os).context("file fragment length is too large")?,
            });
        }
        k += 1;
    }
    Ok((frags, hint))
}

pub struct ArchiveStats {
    pub files: usize,
    pub frames: usize,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
}

/// A ready Files build of an ancestor, which a new build takes unchanged
/// frames and download bundles from.
pub struct ArchiveBase<'a> {
    /// The base's frames, in stream order.
    pub frames: &'a [crate::ArchiveFrame],
    /// The base's metadata: its files with fragments, and its frames placed
    /// in download bundles.
    pub metadata: &'a MetadataChunk,
    /// The base's download bundles. They are durable in storage.
    pub bundles: &'a [String],
}

/// What a build reused and what it made, for the build log.
#[derive(Debug, Default, Clone, Copy)]
pub struct ArchiveReuse {
    /// Whether the build read only the changed bytes.
    pub incremental: bool,
    /// Blob bytes read from the mirror.
    pub bytes_read: u64,
    pub frames_built: usize,
    pub frames_reused: usize,
    pub bundles_built: usize,
    pub bundles_reused: usize,
}

pub struct ArchiveBuildOutput {
    /// Every download bundle, in frame order.
    pub download_bundle_hashes: Vec<String>,
    /// Bundles this build wrote. The others are the base's, already durable.
    pub new_bundle_hashes: Vec<String>,
    pub metadata: MetadataChunk,
    pub archive_frames: Vec<crate::ArchiveFrame>,
    pub reuse: ArchiveReuse,
}

/// One frame ready for a bundle: its compressed bytes when this build made
/// them, or the base frame that holds the same bytes.
struct BuiltFrame {
    raw_hash: String,
    raw_len: u64,
    chunk_hash: String,
    compressed_len: u64,
    bytes: Option<Vec<u8>>,
    base_frame: Option<usize>,
}

/// Hash a raw frame and take the base's compressed frame when one holds the
/// same bytes; otherwise compress it.
fn build_frame(raw: &[u8], level: i32, base: Option<&BaseIndex>) -> Result<BuiltFrame> {
    let raw_hash = crate::cas::hash(raw);
    if let Some(base) = base
        && let Some(&k) = base.by_raw.get(raw_hash.as_str())
    {
        let frame = &base.frames[k];
        return Ok(BuiltFrame {
            raw_hash,
            raw_len: raw.len() as u64,
            chunk_hash: frame.chunk_hash.clone(),
            compressed_len: frame.compressed_len,
            bytes: None,
            base_frame: Some(k),
        });
    }
    let bytes = compress_frame(raw, level)?;
    Ok(BuiltFrame {
        raw_hash,
        raw_len: raw.len() as u64,
        chunk_hash: crate::cas::hash(&bytes),
        compressed_len: bytes.len() as u64,
        bytes: Some(bytes),
        base_frame: None,
    })
}

/// A base build, indexed for reuse.
struct BaseIndex<'a> {
    frames: &'a [crate::ArchiveFrame],
    /// Each base frame's start in the base stream; one extra entry holds the
    /// stream's length.
    starts: Vec<usize>,
    /// Each base frame's bundle and offset in it.
    places: Vec<(&'a str, u64)>,
    /// Base frame by the hash of its raw bytes.
    by_raw: HashMap<&'a str, usize>,
    /// Base bundle by the chunk hashes of its frames, in order.
    bundles: HashMap<String, &'a str>,
    /// Base file by path: content SHA-1, start in the base stream, length.
    files: HashMap<&'a [u8], (&'a [u8], usize, usize)>,
}

impl<'a> BaseIndex<'a> {
    fn new(base: &ArchiveBase<'a>) -> Result<Self> {
        let infos = &base.metadata.frames;
        anyhow::ensure!(
            infos.len() == base.frames.len(),
            "base has {} frames but its metadata places {}",
            base.frames.len(),
            infos.len()
        );
        let mut starts = Vec::with_capacity(infos.len() + 1);
        let mut places = Vec::with_capacity(infos.len());
        let mut by_raw = HashMap::with_capacity(infos.len());
        let mut bundles: HashMap<String, &'a str> = HashMap::new();
        let mut key = String::new();
        let mut pos = 0usize;
        for (k, (frame, info)) in base.frames.iter().zip(infos).enumerate() {
            anyhow::ensure!(
                frame.compressed_len == u64::from(info.compressed_len)
                    && frame.raw_len == u64::from(info.raw_len),
                "base frame {k} does not match its metadata"
            );
            let bundle = base
                .bundles
                .get(info.chunk_index as usize)
                .with_context(|| format!("base frame {k} names a missing bundle"))?;
            starts.push(pos);
            pos = pos
                .checked_add(usize::try_from(frame.raw_len)?)
                .context("base stream length overflow")?;
            places.push((bundle.as_str(), info.chunk_offset));
            by_raw.insert(frame.raw_hash.as_str(), k);
            key.push_str(&frame.chunk_hash);
            let last_in_bundle = infos
                .get(k + 1)
                .is_none_or(|next| next.chunk_index != info.chunk_index);
            if last_in_bundle {
                bundles.insert(std::mem::take(&mut key), bundle.as_str());
            }
        }
        starts.push(pos);
        let mut files = HashMap::with_capacity(base.metadata.files.len());
        let mut file_pos = 0usize;
        for file in &base.metadata.files {
            let len = file.fragments.iter().try_fold(0usize, |total, fragment| {
                total.checked_add(fragment.raw_len as usize)
            });
            let len = len.context("base file length overflow")?;
            files.insert(
                file.path.as_slice(),
                (file.blob_sha1.as_slice(), file_pos, len),
            );
            file_pos = file_pos
                .checked_add(len)
                .context("base file offset overflow")?;
        }
        anyhow::ensure!(
            file_pos == pos,
            "base files cover {file_pos} bytes but its frames cover {pos}"
        );
        Ok(Self {
            frames: base.frames,
            starts,
            places,
            by_raw,
            bundles,
            files,
        })
    }

    /// The compressed bytes of base frame `k`, from the local copy of its
    /// bundle or from storage, checked against the frame's hash.
    fn read_frame(
        &self,
        cas: &Cas,
        storage: Option<&crate::storage::StorageRef>,
        k: usize,
    ) -> Result<Vec<u8>> {
        let frame = &self.frames[k];
        let (bundle, offset) = self.places[k];
        let matches = |data: &[u8]| {
            data.len() as u64 == frame.compressed_len && crate::cas::hash(data) == frame.chunk_hash
        };
        if let Ok(data) = cas.get_range(bundle, offset, frame.compressed_len)
            && matches(&data)
        {
            return Ok(data);
        }
        let Some(storage) = storage else {
            anyhow::bail!(
                "archive frame {} is not in the local copy of bundle {bundle}",
                frame.chunk_hash
            );
        };
        let data = storage
            .get_range(bundle, offset, frame.compressed_len)
            .with_context(|| {
                format!(
                    "read archive frame {} from bundle {bundle}",
                    frame.chunk_hash
                )
            })?;
        anyhow::ensure!(
            matches(&data),
            "archive frame {} from bundle {bundle}: hash mismatch",
            frame.chunk_hash
        );
        Ok(data)
    }
}

/// Groups frames into download bundles, in order. A bundle ends after a
/// frame whose hash passes a fixed test once the bundle holds half the
/// target, or at twice the target. The cuts depend on the frames, not on
/// where the stream starts, so after an edit the bundles away from it come
/// out the same as the base's and are reused whole: no bytes written, none
/// uploaded.
struct Bundler<'a> {
    cas: &'a Cas,
    storage: Option<&'a crate::storage::StorageRef>,
    base: Option<&'a BaseIndex<'a>>,
    min: u64,
    max: u64,
    group: Vec<BuiltFrame>,
    group_len: u64,
    bundles: Vec<String>,
    new_bundles: Vec<String>,
    infos: Vec<FrameInfo>,
    frames: Vec<crate::ArchiveFrame>,
    reuse: ArchiveReuse,
}

impl<'a> Bundler<'a> {
    fn new(
        cas: &'a Cas,
        storage: Option<&'a crate::storage::StorageRef>,
        base: Option<&'a BaseIndex<'a>>,
        target: u64,
    ) -> Self {
        let target = target.max(2);
        Self {
            cas,
            storage,
            base,
            min: target / 2,
            max: target.saturating_mul(2),
            group: Vec::new(),
            group_len: 0,
            bundles: Vec::new(),
            new_bundles: Vec::new(),
            infos: Vec::new(),
            frames: Vec::new(),
            reuse: ArchiveReuse::default(),
        }
    }

    fn push(&mut self, frame: BuiltFrame) -> Result<()> {
        if !self.group.is_empty() && self.group_len + frame.compressed_len > self.max {
            self.close()?;
        }
        self.group_len += frame.compressed_len;
        let cut = self.group_len >= self.max
            || (self.group_len >= self.min && cuts_after(&frame.chunk_hash));
        self.group.push(frame);
        if cut {
            self.close()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if self.group.is_empty() {
            return Ok(());
        }
        let index = u32::try_from(self.bundles.len()).context("too many archive bundles")?;
        let key: String = self.group.iter().map(|f| f.chunk_hash.as_str()).collect();
        let hash = match self.base.and_then(|base| base.bundles.get(&key)) {
            Some(hash) => {
                self.reuse.bundles_reused += 1;
                hash.to_string()
            }
            None => {
                let hash = self.write_group()?;
                self.reuse.bundles_built += 1;
                self.new_bundles.push(hash.clone());
                hash
            }
        };
        let mut offset = 0u64;
        for frame in self.group.drain(..) {
            if frame.bytes.is_some() {
                self.reuse.frames_built += 1;
            } else {
                self.reuse.frames_reused += 1;
            }
            self.infos.push(FrameInfo {
                chunk_index: index,
                chunk_offset: offset,
                compressed_len: u32::try_from(frame.compressed_len)
                    .context("compressed archive frame exceeds manifest limit")?,
                raw_len: u32::try_from(frame.raw_len)
                    .context("raw archive frame exceeds manifest limit")?,
            });
            offset += frame.compressed_len;
            self.frames.push(crate::ArchiveFrame {
                raw_hash: frame.raw_hash,
                chunk_hash: frame.chunk_hash,
                compressed_len: frame.compressed_len,
                raw_len: frame.raw_len,
            });
        }
        self.bundles.push(hash);
        self.group_len = 0;
        Ok(())
    }

    /// Write the open group as a new bundle. Frames this build did not make
    /// are read from the base's bundles, several at once.
    fn write_group(&self) -> Result<String> {
        let (cas, storage, base) = (self.cas, self.storage, self.base);
        let fetched = self
            .group
            .par_iter()
            .map(|frame| match (&frame.bytes, frame.base_frame, base) {
                (Some(_), _, _) => Ok(None),
                (None, Some(k), Some(base)) => base.read_frame(cas, storage, k).map(Some),
                _ => anyhow::bail!("archive frame {} has no bytes", frame.chunk_hash),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut writer = self.cas.writer().context("create archive bundle")?;
        for (frame, fetched) in self.group.iter().zip(&fetched) {
            let bytes = frame
                .bytes
                .as_deref()
                .or(fetched.as_deref())
                .unwrap_or_default();
            writer.write_all(bytes).context("write archive bundle")?;
        }
        let (hash, len) = writer.finish().context("install archive bundle")?;
        anyhow::ensure!(
            len == self.group_len,
            "archive bundle length {len} != frames {}",
            self.group_len
        );
        Ok(hash)
    }

    fn finish(
        mut self,
    ) -> Result<(
        Vec<String>,
        Vec<String>,
        Vec<FrameInfo>,
        Vec<crate::ArchiveFrame>,
        ArchiveReuse,
    )> {
        self.close()?;
        Ok((
            self.bundles,
            self.new_bundles,
            self.infos,
            self.frames,
            self.reuse,
        ))
    }
}

/// Whether a bundle may end after the frame with this hash: about half do.
fn cuts_after(chunk_hash: &str) -> bool {
    chunk_hash.as_bytes().last().is_some_and(|b| b % 2 == 0)
}

/// The bytes of one blob to read: the whole blob, or a range of it.
struct Piece {
    oid: gix::hash::ObjectId,
    size: usize,
    lo: usize,
    hi: usize,
    sha1: bool,
}

/// Read pieces of blobs on several threads. Each blob must have the size the
/// caller planned with. With `sha1`, a piece also returns its whole blob's
/// SHA-1. Results keep the input order.
#[allow(clippy::type_complexity)]
fn read_pieces(mirror: &Path, pieces: Vec<Piece>) -> Result<Vec<(Vec<u8>, Option<[u8; 20]>)>> {
    let workers = crate::gix_util::default_worker_threads();
    crate::gix_util::parallel_map_repo(mirror, pieces, workers, |repo, piece| {
        let mut blob = repo
            .find_blob(piece.oid)
            .with_context(|| format!("read blob {}", piece.oid))?;
        anyhow::ensure!(
            blob.data.len() == piece.size,
            "blob {} has {} bytes, expected {}",
            piece.oid,
            blob.data.len(),
            piece.size
        );
        let sha1 = piece.sha1.then(|| sha1_bytes(&blob.data));
        let data = if piece.lo == 0 && piece.hi == blob.data.len() {
            std::mem::take(&mut blob.data)
        } else {
            blob.data[piece.lo..piece.hi].to_vec()
        };
        Ok((data, sha1))
    })
}

/// Sizes of blobs from their object headers, on several threads.
fn blob_sizes(mirror: &Path, oids: Vec<gix::hash::ObjectId>) -> Result<Vec<usize>> {
    let workers = crate::gix_util::default_worker_threads();
    crate::gix_util::parallel_map_repo(mirror, oids, workers, |repo, oid| {
        let size = repo
            .find_header(*oid)
            .with_context(|| format!("find header {oid}"))?
            .size();
        usize::try_from(size).context("blob size exceeds usize")
    })
}

/// A [`Read`] over windows of blob bytes that a reader thread sends in order.
struct WindowReader {
    rx: mpsc::Receiver<Vec<Vec<u8>>>,
    parts: VecDeque<Vec<u8>>,
    pos: usize,
}

impl Read for WindowReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if let Some(part) = self.parts.front() {
                if self.pos < part.len() {
                    let n = (part.len() - self.pos).min(buf.len());
                    buf[..n].copy_from_slice(&part[self.pos..self.pos + n]);
                    self.pos += n;
                    return Ok(n);
                }
                self.parts.pop_front();
                self.pos = 0;
                continue;
            }
            match self.rx.recv() {
                Ok(window) => self.parts = window.into(),
                Err(_) => return Ok(0),
            }
        }
    }
}

/// The worktree's files in tree order, with their sizes and stream offsets.
struct Layout {
    blobs: Vec<Blob>,
    gitlinks: Vec<Vec<u8>>,
    sizes: Vec<usize>,
    starts: Vec<usize>,
    total: usize,
}

impl Layout {
    fn new(blobs: Vec<Blob>, gitlinks: Vec<Vec<u8>>, sizes: Vec<usize>) -> Result<Self> {
        let mut starts = Vec::with_capacity(sizes.len());
        let mut total = 0usize;
        for size in &sizes {
            starts.push(total);
            total = total
                .checked_add(*size)
                .context("archive stream length overflow")?;
        }
        Ok(Self {
            blobs,
            gitlinks,
            sizes,
            starts,
            total,
        })
    }

    /// The metadata's files table: each file's fragments over `bounds`.
    fn files_table(&self, sha1s: &[&[u8]], bounds: &[(usize, usize)]) -> Result<MetadataChunk> {
        let mut manifest = MetadataChunk::new();
        manifest.gitlinks = self.gitlinks.clone();
        let mut hint = 0usize;
        for (idx, (path, _, mode)) in self.blobs.iter().enumerate() {
            let (fragments, next) = fragments_for(bounds, self.starts[idx], self.sizes[idx], hint)?;
            hint = next;
            manifest.files.push(FileEntry {
                path: path.clone(),
                mode: *mode,
                blob_sha1: sha1s[idx].to_vec(),
                fragments,
            });
        }
        Ok(manifest)
    }
}

/// Head's files table, when it lists exactly this tree's files in order.
fn head_hashes<'h>(head: &'h MetadataChunk, blobs: &[Blob]) -> Option<Vec<&'h [u8]>> {
    let aligned = head.files.len() == blobs.len()
        && head.files.iter().zip(blobs).all(|(f, (path, _, mode))| {
            f.path == *path && f.mode == *mode && f.blob_sha1.len() == 20
        });
    aligned.then(|| head.files.iter().map(|f| f.blob_sha1.as_slice()).collect())
}

/// An incremental build's plan: base frames to keep, as `(new_start,
/// new_end, base frame)`, and the byte ranges between them to read again.
struct Plan {
    reused: Vec<(usize, usize, usize)>,
    gaps: Vec<(usize, usize)>,
}

pub struct ArchiveBuilder {
    mirror: PathBuf,
    cdc: CdcBounds,
}

impl ArchiveBuilder {
    pub fn new<P: AsRef<Path>>(mirror: P) -> Self {
        Self {
            mirror: mirror.as_ref().to_path_buf(),
            cdc: CDC,
        }
    }

    /// The same builder with smaller frames, so a test can span many frames
    /// with little data.
    #[cfg(test)]
    fn with_cdc(self, cdc: CdcBounds) -> Self {
        Self { cdc, ..self }
    }

    fn open_tree(&self, commit: &str) -> Result<(gix::Repository, gix::hash::ObjectId)> {
        if !self.mirror.exists() {
            anyhow::bail!("mirror not found: {}", self.mirror.display());
        }
        let repo = crate::gix_util::open_repo(&self.mirror)
            .with_context(|| format!("open repo {}", self.mirror.display()))?;
        let id = repo
            .rev_parse_single(commit)
            .with_context(|| format!("resolve commit {}", commit))?;
        let tree_id = repo
            .find_commit(id)
            .with_context(|| format!("find commit {}", id))?
            .tree_id()
            .with_context(|| format!("read tree for commit {}", id))?
            .detach();
        Ok((repo, tree_id))
    }

    /// Walk the tree and size every blob from its header.
    fn layout(&self, commit: &str) -> Result<Layout> {
        let (repo, tree_id) = self.open_tree(commit)?;
        let mut blobs = Vec::new();
        let gitlinks = collect_blobs_raw_gix(&repo, tree_id, &mut blobs)
            .context("walk git tree for archive")?;
        let sizes = blob_sizes(&self.mirror, blobs.iter().map(|b| b.1).collect())?;
        Layout::new(blobs, gitlinks, sizes)
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
    ) -> Result<ArchiveStats> {
        let (manifest, chunks, stats) = self.build_chunks(commit, level, u64::MAX)?;
        if chunks.len() > 1 {
            anyhow::bail!(
                "expected at most one archive chunk for file output, got {}",
                chunks.len()
            );
        }
        let archive = chunks.first().map(Vec::as_slice).unwrap_or_default();
        std::fs::write(archive_path, archive)
            .with_context(|| format!("write archive {}", archive_path.display()))?;
        let mut manifest_file = File::create(manifest_path)
            .with_context(|| format!("create manifest {}", manifest_path.display()))?;
        manifest.write(&mut manifest_file)?;
        manifest_file.flush().context("flush manifest")?;
        Ok(stats)
    }

    /// Build the working tree into archive chunks held in memory.
    ///
    /// Returns the metadata chunk (containing the frame and file tables) and a
    /// vector of raw archive chunk byte vectors. `target_chunk_size` caps the
    /// compressed size of each chunk; a frame larger than the target gets its
    /// own chunk.
    pub fn build_chunks(
        &self,
        commit: &str,
        level: i32,
        target_chunk_size: u64,
    ) -> Result<(MetadataChunk, Vec<Vec<u8>>, ArchiveStats)> {
        let target_chunk_size = target_chunk_size.max(1);
        let layout = self.layout(commit)?;
        let mut compressed_frames: Vec<Vec<u8>> = Vec::new();
        let (bounds, sha1s) = self.stream_frames(&layout, true, level, None, |frame| {
            compressed_frames.push(frame.bytes.unwrap_or_default());
            Ok(())
        })?;
        let sha1s: Vec<&[u8]> = sha1s.iter().map(|h| h.as_slice()).collect();
        let mut manifest = if layout.blobs.is_empty() {
            MetadataChunk::new()
        } else {
            layout.files_table(&sha1s, &bounds)?
        };
        manifest.gitlinks = layout.gitlinks.clone();

        // Assemble compressed frames into chunks of ~target_chunk_size, recording
        // each frame's placement.
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut current_chunk: Vec<u8> = Vec::new();
        let mut current_frame_count = 0usize;
        for (i, compressed) in compressed_frames.iter().enumerate() {
            let would_exceed = !current_chunk.is_empty()
                && (current_chunk.len() as u64).saturating_add(compressed.len() as u64)
                    > target_chunk_size;
            if would_exceed {
                chunks.push(std::mem::take(&mut current_chunk));
                current_frame_count = 0;
            }
            manifest.frames.push(FrameInfo {
                chunk_index: u32::try_from(chunks.len()).context("archive has too many chunks")?,
                chunk_offset: current_chunk.len() as u64,
                compressed_len: u32::try_from(compressed.len())
                    .context("compressed archive frame is too large")?,
                raw_len: u32::try_from(bounds[i].1 - bounds[i].0)
                    .context("raw archive frame is too large")?,
            });
            current_chunk.extend_from_slice(compressed);
            current_frame_count += 1;
            if current_chunk.len() as u64 >= target_chunk_size {
                chunks.push(std::mem::take(&mut current_chunk));
                current_frame_count = 0;
            }
        }
        if current_frame_count > 0 {
            chunks.push(current_chunk);
        }

        let compressed_total = chunks.iter().map(|chunk| chunk.len() as u64).sum();
        let files = manifest.files.len();
        let frames = manifest.frames.len();
        Ok((
            manifest,
            chunks,
            ArchiveStats {
                files,
                frames,
                raw_bytes: layout.total as u64,
                compressed_bytes: compressed_total,
            },
        ))
    }

    /// Stream the whole worktree through content-defined chunking and hand
    /// each frame, in order, to `consume`.
    ///
    /// Three stages overlap: a reader thread loads windows of blobs, several
    /// blobs at once; this thread cuts frames; a compressor thread compresses
    /// each batch of frames on all cores and passes them to `consume`. Memory
    /// stays a few windows and batches, never the whole worktree. With
    /// `sha1`, the reader also hashes each blob for the files table.
    ///
    /// Returns each frame's `(start, end)` and, with `sha1`, each blob's
    /// SHA-1. A worktree of only empty files gets one empty frame.
    #[allow(clippy::type_complexity)]
    fn stream_frames<C>(
        &self,
        layout: &Layout,
        sha1: bool,
        level: i32,
        base: Option<&BaseIndex>,
        mut consume: C,
    ) -> Result<(Vec<(usize, usize)>, Vec<[u8; 20]>)>
    where
        C: FnMut(BuiltFrame) -> Result<()> + Send,
    {
        let mirror = self.mirror.as_path();
        let (bounds, sha1s) = std::thread::scope(|scope| -> Result<_> {
            let (window_tx, window_rx) = mpsc::sync_channel::<Vec<Vec<u8>>>(2);
            let reader = scope.spawn(move || -> Result<Vec<[u8; 20]>> {
                let mut sha1s = Vec::new();
                let mut i = 0usize;
                while i < layout.blobs.len() {
                    let mut j = i;
                    let mut bytes = 0usize;
                    while j < layout.blobs.len()
                        && (j == i || bytes + layout.sizes[j] <= READ_WINDOW_BYTES)
                    {
                        bytes += layout.sizes[j];
                        j += 1;
                    }
                    let pieces = (i..j)
                        .map(|k| Piece {
                            oid: layout.blobs[k].1,
                            size: layout.sizes[k],
                            lo: 0,
                            hi: layout.sizes[k],
                            sha1,
                        })
                        .collect();
                    let mut window = Vec::with_capacity(j - i);
                    for (data, hash) in read_pieces(mirror, pieces)? {
                        window.push(data);
                        sha1s.extend(hash);
                    }
                    if window_tx.send(window).is_err() {
                        break;
                    }
                    i = j;
                }
                Ok(sha1s)
            });

            let (batch_tx, batch_rx) = mpsc::sync_channel::<Vec<Vec<u8>>>(1);
            let consume = &mut consume;
            let compressor = scope.spawn(move || -> Result<()> {
                for batch in batch_rx {
                    let built = batch
                        .par_iter()
                        .map(|raw| build_frame(raw, level, base))
                        .collect::<Result<Vec<_>>>()?;
                    for frame in built {
                        consume(frame)?;
                    }
                }
                Ok(())
            });

            let input = WindowReader {
                rx: window_rx,
                parts: VecDeque::new(),
                pos: 0,
            };
            let mut bounds = Vec::new();
            let mut batch = Vec::new();
            let mut batch_bytes = 0usize;
            let mut cut_error = None;
            for chunk in StreamCDC::new(input, self.cdc.min, self.cdc.avg, self.cdc.max) {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(e) => {
                        cut_error = Some(anyhow::anyhow!("content-defined chunking: {e}"));
                        break;
                    }
                };
                let start = usize::try_from(chunk.offset)?;
                bounds.push((start, start + chunk.length));
                batch_bytes += chunk.length;
                batch.push(chunk.data);
                if batch_bytes >= STREAM_BATCH_BYTES {
                    if batch_tx.send(std::mem::take(&mut batch)).is_err() {
                        break;
                    }
                    batch_bytes = 0;
                }
            }
            if cut_error.is_none() && !batch.is_empty() {
                let _ = batch_tx.send(batch);
            }
            drop(batch_tx);
            let compressed = compressor
                .join()
                .map_err(|_| anyhow::anyhow!("archive compressor panicked"))?;
            let read = reader
                .join()
                .map_err(|_| anyhow::anyhow!("archive reader panicked"))?;
            compressed.context("compress archive frames")?;
            let sha1s = read.context("read worktree blobs")?;
            if let Some(e) = cut_error {
                return Err(e);
            }
            Ok((bounds, sha1s))
        })?;

        let streamed = bounds.last().map_or(0, |b| b.1);
        anyhow::ensure!(
            streamed == layout.total,
            "archive stream has {streamed} bytes, expected {}",
            layout.total
        );
        let mut bounds = bounds;
        if bounds.is_empty() && !layout.blobs.is_empty() {
            bounds.push((0, 0));
            consume(build_frame(&[], level, base)?)?;
        }
        Ok((bounds, sha1s))
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
    ) -> Result<ArchiveStats> {
        let mirror = repo_root.join(format!("{}_{}.git", owner, repo_name));
        let builder = ArchiveBuilder::new(&mirror);
        builder.build(commit, archive_path, manifest_path, level)
    }

    /// Build only the metadata *files table* (path, mode, blob sha1) — no zstd
    /// frames. Editable clones materialize the worktree from the HEAD-closure
    /// packs, not the archive, so they only need this table; the expensive frame
    /// compression is only needed for files mode. This still reads each blob (to
    /// hash it) but skips compression, so it is much cheaper — letting Head
    /// publish without waiting on Files. `frames` is left empty.
    pub fn build_files_table(&self, commit: &str) -> Result<MetadataChunk> {
        let (repo, tree_id) = self.open_tree(commit)?;
        let mut blobs = Vec::new();
        let gitlinks = collect_blobs_raw_gix(&repo, tree_id, &mut blobs)
            .context("collect blobs for files table")?;

        // Hash blobs in parallel. Many-file repos (e.g. node_modules) spend most
        // of the files-table time reading/decompressing small blobs; per-thread
        // gix handles share the mmap'd ODB and keep order deterministically.
        let hashes =
            hash_blobs_parallel(&self.mirror, blobs).context("hash blobs for files table")?;

        let mut manifest = MetadataChunk::new();
        manifest.gitlinks = gitlinks;
        for (path, mode, blob_sha1) in hashes {
            manifest.files.push(FileEntry {
                path,
                mode,
                blob_sha1,
                fragments: Vec::new(),
            });
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
        // Prior path -> content hash. Mode is always taken from the current tree
        // walk (so a mode-only change is picked up even if we reuse the hash).
        let prev: HashMap<&[u8], &[u8]> = prev_files
            .iter()
            .map(|f| (f.path.as_slice(), f.blob_sha1.as_slice()))
            .collect();
        let (repo, tree_id) = self.open_tree(commit)?;

        // Paths are built from raw bytes at every level so they match
        // `git diff -z` and the `changed` set exactly.
        let mut blobs: Vec<Blob> = Vec::new();
        let gitlinks = collect_blobs_raw_gix(&repo, tree_id, &mut blobs)
            .context("walk git tree for incremental files table")?;

        let mut manifest = MetadataChunk::new();
        manifest.gitlinks = gitlinks;
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
                    sha1_bytes(&blob.data).to_vec()
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

    /// Build the archive for `commit` into download bundles in `cas`.
    ///
    /// `head` is Head's metadata for the same commit; its files table gives
    /// every file's content SHA-1, so no blob is hashed here. `base` is the
    /// ready Files build of an ancestor. With both, the build reads only the
    /// changed bytes: a base frame that lies wholly inside a run of files
    /// unchanged since the base (same path, same SHA-1) is kept, and only the
    /// ranges between kept frames are read, cut and compressed. Without them,
    /// or when more than half the bytes changed, or one changed range exceeds
    /// [`MAX_GAP_BYTES`], the whole worktree is streamed.
    ///
    /// Frames go into bundles that are cut by frame content, so bundles away
    /// from an edit match the base's and are reused whole. Frames that must
    /// move to a new bundle are read from the local copy of the base bundle,
    /// or from storage.
    pub fn build_into_cas(
        &self,
        commit: &str,
        cas: &Cas,
        storage: Option<&crate::storage::StorageRef>,
        level: i32,
        head: Option<&MetadataChunk>,
        base: Option<&ArchiveBase>,
        bundle_size: u64,
    ) -> Result<ArchiveBuildOutput> {
        let (repo, tree_id) = self.open_tree(commit)?;
        let mut blobs = Vec::new();
        let gitlinks = collect_blobs_raw_gix(&repo, tree_id, &mut blobs)
            .context("walk git tree for archive")?;
        drop(repo);
        let head_sha1s = head.and_then(|head| head_hashes(head, &blobs));
        let base_index = match base.map(BaseIndex::new).transpose() {
            Ok(index) => index,
            Err(e) => {
                tracing::warn!("Files base is unusable ({e:#}); building without it");
                None
            }
        };
        let plan = match (&head_sha1s, &base_index) {
            (Some(sha1s), Some(index)) => self.plan_incremental(&blobs, sha1s, index)?,
            _ => None,
        };

        let incremental = plan.is_some();
        let mut bundler = Bundler::new(cas, storage, base_index.as_ref(), bundle_size);
        let (layout, bounds, owned_sha1s, bytes_read) = match plan {
            Some((plan, sizes)) => {
                let layout = Layout::new(blobs, gitlinks, sizes)?;
                let base = base_index
                    .as_ref()
                    .context("incremental plan without a base")?;
                let (bounds, read) = self.build_gaps(&layout, &plan, level, base, &mut bundler)?;
                (layout, bounds, Vec::new(), read)
            }
            None => {
                let sizes = blob_sizes(&self.mirror, blobs.iter().map(|b| b.1).collect())?;
                let layout = Layout::new(blobs, gitlinks, sizes)?;
                let (bounds, sha1s) = self.stream_frames(
                    &layout,
                    head_sha1s.is_none(),
                    level,
                    base_index.as_ref(),
                    |frame| bundler.push(frame),
                )?;
                let total = layout.total;
                (layout, bounds, sha1s, total)
            }
        };
        let (bundles, new_bundles, infos, frames, mut reuse) = bundler.finish()?;
        reuse.incremental = incremental;
        reuse.bytes_read = bytes_read as u64;

        let owned: Vec<&[u8]> = owned_sha1s.iter().map(|h| h.as_slice()).collect();
        let sha1s: &[&[u8]] = match &head_sha1s {
            Some(sha1s) => sha1s,
            None => &owned,
        };
        let mut metadata = if layout.blobs.is_empty() {
            let mut metadata = MetadataChunk::new();
            metadata.gitlinks = layout.gitlinks.clone();
            metadata
        } else {
            layout.files_table(sha1s, &bounds)?
        };
        metadata.frames = infos;
        Ok(ArchiveBuildOutput {
            download_bundle_hashes: bundles,
            new_bundle_hashes: new_bundles,
            metadata,
            archive_frames: frames,
            reuse,
        })
    }

    /// Plan an incremental build: which base frames to keep and which byte
    /// ranges to read again. Returns the plan and every blob's size, or `None`
    /// when the whole worktree should be streamed instead.
    fn plan_incremental(
        &self,
        blobs: &[Blob],
        sha1s: &[&[u8]],
        base: &BaseIndex,
    ) -> Result<Option<(Plan, Vec<usize>)>> {
        // A file is unchanged when the base has the same path with the same
        // content; it keeps the base's size and start. Other files are sized
        // from their headers.
        let mut sizes = vec![0usize; blobs.len()];
        let mut old_starts: Vec<Option<usize>> = vec![None; blobs.len()];
        let mut unknown = Vec::new();
        for (idx, (path, _, _)) in blobs.iter().enumerate() {
            match base.files.get(path.as_slice()) {
                Some(&(sha1, start, len)) if sha1 == sha1s[idx] => {
                    sizes[idx] = len;
                    old_starts[idx] = Some(start);
                }
                _ => unknown.push(idx),
            }
        }
        let headers = blob_sizes(&self.mirror, unknown.iter().map(|&i| blobs[i].1).collect())?;
        for (&idx, size) in unknown.iter().zip(headers) {
            sizes[idx] = size;
        }

        // Runs of unchanged files that are also adjacent in the base stream:
        // `(old_start, old_end, new_start)`.
        let mut segments: Vec<(usize, usize, usize)> = Vec::new();
        let mut new_pos = 0usize;
        for idx in 0..blobs.len() {
            if let Some(old) = old_starts[idx] {
                let extend = segments
                    .last()
                    .is_some_and(|&(o0, o1, n0)| o1 == old && n0 + (o1 - o0) == new_pos);
                if extend {
                    segments.last_mut().expect("segment").1 += sizes[idx];
                } else {
                    segments.push((old, old + sizes[idx], new_pos));
                }
            }
            new_pos = new_pos
                .checked_add(sizes[idx])
                .context("archive stream length overflow")?;
        }
        let total = new_pos;
        if total == 0 {
            return Ok(None);
        }

        // Keep each base frame that lies wholly inside one segment, at the
        // segment's new position.
        let mut reused = Vec::new();
        let mut seg = 0usize;
        let mut by_old: Vec<&(usize, usize, usize)> = segments.iter().collect();
        by_old.sort_by_key(|s| s.0);
        for k in 0..base.frames.len() {
            let (fs, fe) = (base.starts[k], base.starts[k + 1]);
            if fe == fs {
                continue;
            }
            while seg < by_old.len() && by_old[seg].1 <= fs {
                seg += 1;
            }
            if let Some(&&(o0, o1, n0)) = by_old.get(seg)
                && o0 <= fs
                && fe <= o1
            {
                reused.push((n0 + (fs - o0), n0 + (fe - o0), k));
            }
        }
        reused.sort_by_key(|r| r.0);

        // Drop kept frames that would leave a gap shorter than a minimum
        // frame, so new frames do not shrink build after build.
        let mut kept: Vec<(usize, usize, usize)> = Vec::with_capacity(reused.len());
        let mut pos = 0usize;
        for (ns, ne, k) in reused {
            if ns < pos {
                return Ok(None);
            }
            let gap = ns - pos;
            if gap > 0 && gap < self.cdc.min {
                continue;
            }
            kept.push((ns, ne, k));
            pos = ne;
        }
        while let Some(&(_, ne, _)) = kept.last() {
            let tail = total - ne;
            if tail == 0 || tail >= self.cdc.min {
                break;
            }
            kept.pop();
        }
        if kept.is_empty() {
            return Ok(None);
        }
        let mut gaps = Vec::new();
        let mut pos = 0usize;
        for &(ns, ne, _) in &kept {
            if ns > pos {
                gaps.push((pos, ns));
            }
            pos = ne;
        }
        if total > pos {
            gaps.push((pos, total));
        }
        let gap_bytes: usize = gaps.iter().map(|(a, b)| b - a).sum();
        if gap_bytes > total / 2 || gaps.iter().any(|(a, b)| b - a > MAX_GAP_BYTES) {
            return Ok(None);
        }
        Ok(Some((Plan { reused: kept, gaps }, sizes)))
    }

    /// Read and cut the plan's gaps, then hand every frame, kept and new, to
    /// `bundler` in stream order. Gaps go in batches of up to
    /// [`MAX_GAP_BYTES`]. Returns the frame bounds and the bytes read.
    fn build_gaps(
        &self,
        layout: &Layout,
        plan: &Plan,
        level: i32,
        base: &BaseIndex,
        bundler: &mut Bundler,
    ) -> Result<(Vec<(usize, usize)>, usize)> {
        let mut bounds = Vec::new();
        let mut bytes_read = 0usize;
        let mut kept = plan.reused.iter().peekable();
        let mut emit_kept =
            |end: usize, bounds: &mut Vec<(usize, usize)>, bundler: &mut Bundler| {
                while let Some(&&(start, stop, k)) = kept.peek().filter(|r| r.0 < end) {
                    kept.next();
                    let frame = &base.frames[k];
                    bounds.push((start, stop));
                    bundler.push(BuiltFrame {
                        raw_hash: frame.raw_hash.clone(),
                        raw_len: frame.raw_len,
                        chunk_hash: frame.chunk_hash.clone(),
                        compressed_len: frame.compressed_len,
                        bytes: None,
                        base_frame: Some(k),
                    })?;
                }
                anyhow::Ok(())
            };
        let mut rest = plan.gaps.as_slice();
        while !rest.is_empty() {
            let mut n = 0usize;
            let mut batch_bytes = 0usize;
            while n < rest.len()
                && (n == 0 || batch_bytes + (rest[n].1 - rest[n].0) <= MAX_GAP_BYTES)
            {
                batch_bytes += rest[n].1 - rest[n].0;
                n += 1;
            }
            let (batch, tail) = rest.split_at(n);
            rest = tail;
            for (start, end, frame) in self.cut_gaps(layout, batch, level, base)? {
                emit_kept(start, &mut bounds, bundler)?;
                bounds.push((start, end));
                bundler.push(frame)?;
            }
            bytes_read += batch_bytes;
        }
        emit_kept(usize::MAX, &mut bounds, bundler)?;

        // The frames must tile the new stream exactly.
        let tiled = bounds.first().map(|b| b.0) == Some(0)
            && bounds.last().map(|b| b.1) == Some(layout.total)
            && bounds.windows(2).all(|w| w[0].1 == w[1].0);
        anyhow::ensure!(tiled, "incremental archive frames do not tile the worktree");
        Ok((bounds, bytes_read))
    }

    /// Read `gaps` of the new stream on several threads, cut each into
    /// frames, and build the frames on all cores. Returns each frame with its
    /// `(start, end)` in the stream, in order.
    fn cut_gaps(
        &self,
        layout: &Layout,
        gaps: &[(usize, usize)],
        level: i32,
        base: &BaseIndex,
    ) -> Result<Vec<(usize, usize, BuiltFrame)>> {
        let mut pieces = Vec::new();
        let mut owners = Vec::new();
        for (g, &(g0, g1)) in gaps.iter().enumerate() {
            let mut idx = layout
                .starts
                .partition_point(|&start| start <= g0)
                .saturating_sub(1);
            while idx < layout.blobs.len() && layout.starts[idx] < g1 {
                let (start, size) = (layout.starts[idx], layout.sizes[idx]);
                if size > 0 && start + size > g0 {
                    pieces.push(Piece {
                        oid: layout.blobs[idx].1,
                        size,
                        lo: g0.max(start) - start,
                        hi: g1.min(start + size) - start,
                        sha1: false,
                    });
                    owners.push(g);
                }
                idx += 1;
            }
        }
        let mut buffers: Vec<Vec<u8>> = gaps
            .iter()
            .map(|&(g0, g1)| Vec::with_capacity(g1 - g0))
            .collect();
        for ((data, _), g) in read_pieces(&self.mirror, pieces)?.into_iter().zip(owners) {
            buffers[g].extend_from_slice(&data);
        }
        let mut cuts = Vec::new();
        for (g, (buffer, &(g0, g1))) in buffers.iter().zip(gaps).enumerate() {
            anyhow::ensure!(
                buffer.len() == g1 - g0,
                "archive gap {g0}..{g1} read {} bytes",
                buffer.len()
            );
            for c in fastcdc::v2020::FastCDC::new(buffer, self.cdc.min, self.cdc.avg, self.cdc.max)
            {
                cuts.push((g, c.offset, c.length));
            }
        }
        cuts.par_iter()
            .map(|&(g, offset, len)| {
                let frame = build_frame(&buffers[g][offset..offset + len], level, Some(base))?;
                let start = gaps[g].0 + offset;
                Ok((start, start + len, frame))
            })
            .collect()
    }
}

pub fn sha1_bytes(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}

/// Collect every blob in `tree_id` as `(raw_path_bytes, blob_oid, mode)` in
/// git pre-order. Paths are built from raw entry names at every level so they
/// are byte-exact (no lossy UTF-8), matching `git diff -z` output. Returns the
/// paths of submodule gitlinks, which are not files.
fn collect_blobs_raw_gix(
    repo: &gix::Repository,
    tree_id: gix::hash::ObjectId,
    out: &mut Vec<Blob>,
) -> Result<Vec<Vec<u8>>> {
    let mut recorder = gix::traverse::tree::Recorder::default();
    gix::traverse::tree::depthfirst(
        tree_id,
        gix::traverse::tree::depthfirst::State::default(),
        &repo.objects,
        &mut recorder,
    )
    .context("walk git tree")?;
    let mut gitlinks = Vec::new();
    for entry in recorder.records {
        // Trees are recursed into; a submodule gitlink is a directory, not a file.
        if entry.mode.is_commit() {
            gitlinks.push(entry.filepath.to_vec());
            continue;
        }
        if entry.mode.is_tree() {
            continue;
        }
        out.push((
            entry.filepath.to_vec(),
            entry.oid,
            entry.mode.value() as u32,
        ));
    }
    Ok(gitlinks)
}

/// Hash a list of `(path, blob_oid, mode)` tuples in parallel using scoped OS
/// threads and per-chunk gix handles. Results keep the input order so the
/// manifest order stays deterministic.
fn hash_blobs_parallel(
    mirror: &std::path::Path,
    blobs: Vec<Blob>,
) -> anyhow::Result<Vec<(Vec<u8>, u32, Vec<u8>)>> {
    const PARALLEL_HASH_THRESHOLD: usize = 64;

    if blobs.len() < PARALLEL_HASH_THRESHOLD {
        let repo = crate::gix_util::open_repo(mirror)?;
        return blobs
            .into_iter()
            .map(|(path, oid, mode)| {
                let blob = repo.find_blob(oid).with_context(|| {
                    format!("read blob {} for {}", oid, String::from_utf8_lossy(&path))
                })?;
                Ok((path, mode, sha1_bytes(&blob.data).to_vec()))
            })
            .collect::<anyhow::Result<Vec<_>>>();
    }

    let num_workers =
        crate::gix_util::worker_threads("hash", crate::gix_util::default_worker_threads());
    crate::gix_util::parallel_map_repo(mirror, blobs, num_workers, |repo, (path, oid, mode)| {
        let blob = repo
            .find_blob(*oid)
            .with_context(|| format!("read blob {} for {}", oid, String::from_utf8_lossy(path)))?;
        Ok((path.clone(), *mode, sha1_bytes(&blob.data).to_vec()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worktree stream for `commit`: every blob in tree order.
    fn tree_stream(mirror: &Path, commit: &str) -> Vec<u8> {
        let repo = crate::gix_util::open_repo(mirror).unwrap();
        let id = repo.rev_parse_single(commit).unwrap();
        let tree_id = repo.find_commit(id).unwrap().tree_id().unwrap().detach();
        let mut blobs = Vec::new();
        collect_blobs_raw_gix(&repo, tree_id, &mut blobs).unwrap();
        let mut stream = Vec::new();
        for (_p, oid, _mode) in &blobs {
            stream.extend_from_slice(&repo.find_blob(*oid).unwrap().data);
        }
        stream
    }

    /// Decompress a build's frames from its bundles, in order, after checking
    /// the metadata's geometry. `get` reads a bundle.
    fn archive_stream(out: &ArchiveBuildOutput, get: impl Fn(&str) -> Vec<u8>) -> Vec<u8> {
        out.metadata.validate_geometry().unwrap();
        let lengths = crate::clonepack::archive_chunk_lengths(&out.metadata).unwrap();
        assert_eq!(lengths.len(), out.download_bundle_hashes.len());
        let bundles: Vec<Vec<u8>> = out.download_bundle_hashes.iter().map(|h| get(h)).collect();
        for (bundle, len) in bundles.iter().zip(&lengths) {
            assert_eq!(bundle.len() as u64, *len, "bundle length");
        }
        let mut stream = Vec::new();
        for info in &out.metadata.frames {
            let bundle = &bundles[info.chunk_index as usize];
            let start = info.chunk_offset as usize;
            let raw = &bundle[start..start + info.compressed_len as usize];
            if info.raw_len > 0 {
                stream.extend_from_slice(&zstd::decode_all(raw).unwrap());
            }
        }
        stream
    }

    /// A build with no Head table and no base streams the whole worktree:
    /// its bundles are in CAS and decompress, in order, to the worktree.
    #[test]
    fn archive_bundles_reproduce_the_worktree_stream() {
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
            ("a.txt", b"hello world\n".to_vec()),
            ("dir/medium.bin", pr(7 * 1024 * 1024, 3)),
            ("big.bin", pr(20 * 1024 * 1024, 7)),
            ("z.txt", b"tail\n".to_vec()),
        ];
        let files_ref: Vec<(&str, &[u8])> = files.iter().map(|(p, b)| (*p, b.as_slice())).collect();
        let (tmp, commit) = commit_files(&files_ref);

        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let out = ArchiveBuilder::new(tmp.path())
            .build_into_cas(
                &commit,
                &cas,
                None,
                6,
                None,
                None,
                DEFAULT_ARCHIVE_CHUNK_SIZE,
            )
            .unwrap();
        assert!(
            out.archive_frames.len() >= 3,
            "test data should span multiple CDC frames, got {}",
            out.archive_frames.len()
        );
        assert!(!out.reuse.incremental);
        assert_eq!(out.new_bundle_hashes, out.download_bundle_hashes);
        let stream = archive_stream(&out, |h| cas.get(h).unwrap());
        assert_eq!(stream, tree_stream(tmp.path(), &commit));
    }

    /// Small frames and bundles, so a little data spans many of each.
    const SMALL_CDC: CdcBounds = CdcBounds {
        min: CDC.min / 64,
        avg: CDC.avg / 64,
        max: CDC.max / 64,
    };
    const SMALL_BUNDLE: u64 = 64 * 1024;

    /// The base build of an incremental test, kept with what it was built from.
    struct Prior<'a> {
        builder: &'a ArchiveBuilder,
        cas: &'a Cas,
        out: &'a ArchiveBuildOutput,
        bundle_size: u64,
    }

    /// Build `commit` against `prior` with Head's files table, as the server
    /// does, and check the result: it decompresses to the worktree, its files
    /// table matches a full one, and it read and wrote less than everything.
    fn build_on(
        prior: &Prior,
        storage: Option<&crate::storage::StorageRef>,
        commit: &str,
    ) -> Result<ArchiveBuildOutput> {
        let head = prior.builder.build_files_table(commit).unwrap();
        let base = ArchiveBase {
            frames: &prior.out.archive_frames,
            metadata: &prior.out.metadata,
            bundles: &prior.out.download_bundle_hashes,
        };
        let out = prior.builder.build_into_cas(
            commit,
            prior.cas,
            storage,
            3,
            Some(&head),
            Some(&base),
            prior.bundle_size,
        )?;
        let fl = |m: &MetadataChunk| {
            m.files
                .iter()
                .map(|f| (f.path.clone(), f.mode, f.blob_sha1.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(fl(&out.metadata), fl(&head), "files table");
        Ok(out)
    }

    /// An incremental build reads only the changed bytes, keeps the base's
    /// other frames and bundles, and is exact, across edit positions and
    /// shapes: a wrong offset or a stale frame would change the stream. An
    /// identical commit reuses everything. The scenarios run in parallel.
    #[test]
    fn incremental_archive_is_exact_and_reuses_the_base() {
        let mix_blob = |seed| mix_blob(seed, 200_000);
        let base: Vec<(String, Vec<u8>)> = (0..10)
            .map(|s| (format!("f{s:02}.bin"), mix_blob(s)))
            .collect();

        let scenarios: Vec<
            Box<dyn Fn(&[(String, Vec<u8>)]) -> Vec<(String, Vec<u8>)> + Send + Sync>,
        > = vec![
            // Nothing changes.
            Box::new(|base| base.to_vec()),
            Box::new(|base| {
                // Same-size edit in the middle.
                let mut middle = base.to_vec();
                middle[5].1 = mix_blob(900);
                middle
            }),
            Box::new(|base| {
                // Edit at the head.
                let mut head = base.to_vec();
                head[0].1 = mix_blob(901);
                head
            }),
            Box::new(|base| {
                // Edit at the tail.
                let mut tail = base.to_vec();
                tail[9].1 = mix_blob(902);
                tail
            }),
            Box::new(|base| {
                // A growing edit shifts what follows forward.
                let mut grow = base.to_vec();
                grow[5].1 = {
                    let mut v = mix_blob(903);
                    v.extend(mix_blob(904));
                    v
                };
                grow
            }),
            Box::new(|base| {
                // A deletion shifts what follows back.
                let mut del = base.to_vec();
                del.remove(5);
                del
            }),
            Box::new(|base| {
                // A new file between unchanged neighbors.
                let mut add = base.to_vec();
                add.insert(5, ("f05b.bin".to_string(), mix_blob(905)));
                add
            }),
            Box::new(|base| {
                // Several edits, with unchanged frames between them.
                let mut spread = base.to_vec();
                spread[1].1 = mix_blob(906);
                spread[5].1 = mix_blob(907);
                spread[8].1 = mix_blob(908);
                spread
            }),
            Box::new(|base| {
                // A modify, a delete and an insert at once.
                let mut mixed = base.to_vec();
                mixed[2].1 = mix_blob(909);
                mixed.remove(6);
                mixed.insert(4, ("f03b.bin".to_string(), mix_blob(910)));
                mixed
            }),
        ];

        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let builder = ArchiveBuilder::new(tmp.path()).with_cdc(SMALL_CDC);
        let c1 = commit_onto(&repo, &as_refs(&base));
        let out = builder
            .build_into_cas(&c1, &cas, None, 3, None, None, SMALL_BUNDLE)
            .unwrap();
        assert!(
            out.archive_frames.len() > 15 && out.download_bundle_hashes.len() > 3,
            "need many frames and bundles, got {} and {}",
            out.archive_frames.len(),
            out.download_bundle_hashes.len()
        );
        let prior = Prior {
            builder: &builder,
            cas: &cas,
            out: &out,
            bundle_size: SMALL_BUNDLE,
        };
        // Commits update one ref, so they are made in turn.
        let commits: Vec<String> = scenarios
            .iter()
            .map(|scenario| commit_onto(&repo, &as_refs(&scenario(&base))))
            .collect();
        std::thread::scope(|scope| {
            let (prior, mirror) = (&prior, tmp.path());
            let handles: Vec<_> = commits
                .iter()
                .enumerate()
                .map(|(i, c2)| {
                    scope.spawn(move || {
                        let out = build_on(prior, None, c2).unwrap();
                        let stream = archive_stream(&out, |h| prior.cas.get(h).unwrap());
                        assert!(
                            stream == tree_stream(mirror, c2),
                            "scenario {i}: stream differs"
                        );
                        let reuse = out.reuse;
                        assert!(reuse.incremental, "scenario {i}: {reuse:?}");
                        assert!(reuse.frames_reused > 0, "scenario {i}: {reuse:?}");
                        assert!(reuse.bundles_reused > 0, "scenario {i}: {reuse:?}");
                        if i == 0 {
                            assert_eq!(reuse.bytes_read, 0, "identical commit: {reuse:?}");
                            assert_eq!(reuse.frames_built + reuse.bundles_built, 0, "{reuse:?}");
                        } else {
                            assert!(reuse.bytes_read > 0, "scenario {i}: {reuse:?}");
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("incremental scenario panicked");
            }
        });
    }

    /// The base's bundles may be gone from the local cache (evicted after
    /// upload, or a fresh disk). Frames that move to a new bundle are then
    /// read from storage and checked: a correct copy builds an exact
    /// archive; a corrupt copy of the right length fails the build.
    #[test]
    fn incremental_archive_reads_base_frames_from_storage() {
        let base: Vec<(String, Vec<u8>)> = (0..10)
            .map(|s| (format!("f{s:02}.bin"), mix_blob(s, 200_000)))
            .collect();
        let mut edited = base.clone();
        edited[5].1 = mix_blob(900, 200_000);

        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::local(storage_dir.path()).unwrap();
        let builder = ArchiveBuilder::new(tmp.path()).with_cdc(SMALL_CDC);
        let c1 = commit_onto(&repo, &as_refs(&base));
        let c2 = commit_onto(&repo, &as_refs(&edited));
        // Bundles of several frames, so frames next to the edit move.
        let bundle_size = 16 * SMALL_BUNDLE;
        let out = builder
            .build_into_cas(&c1, &cas, None, 3, None, None, bundle_size)
            .unwrap();
        for hash in &out.download_bundle_hashes {
            storage.put(hash, &cas.get(hash).unwrap()).unwrap();
            cas.remove(hash).unwrap();
        }
        let prior = Prior {
            builder: &builder,
            cas: &cas,
            out: &out,
            bundle_size,
        };

        let corrupt: crate::storage::StorageRef = std::sync::Arc::new(CorruptStorage);
        let Err(err) = build_on(&prior, Some(&corrupt), &c2) else {
            panic!("a corrupt base frame must fail the build");
        };
        assert!(
            format!("{err:#}").contains("hash mismatch"),
            "unexpected error: {err:#}"
        );

        let rebuilt = build_on(&prior, Some(&storage), &c2).unwrap();
        assert!(rebuilt.reuse.frames_reused > 0, "{:?}", rebuilt.reuse);
        let stream = archive_stream(&rebuilt, |h| {
            cas.get(h).or_else(|_| storage.get(h)).unwrap()
        });
        assert!(stream == tree_stream(tmp.path(), &c2), "stream differs");
    }

    /// Storage that returns wrong bytes of the right length.
    struct CorruptStorage;

    #[async_trait::async_trait]
    impl crate::storage::StorageBackend for CorruptStorage {
        fn get(&self, _hash: &str) -> Result<Vec<u8>> {
            anyhow::bail!("not found")
        }

        fn get_range(&self, _hash: &str, _start: u64, len: u64) -> Result<Vec<u8>> {
            Ok(vec![0x5a; len as usize])
        }

        fn put(&self, _hash: &str, _data: &[u8]) -> Result<()> {
            anyhow::bail!("unsupported")
        }

        fn size(&self, _hash: &str) -> Result<u64> {
            anyhow::bail!("unsupported")
        }

        fn verify_durable_copy(&self, _hash: &str) -> Result<()> {
            anyhow::bail!("unsupported")
        }
    }

    fn commit_files(files: &[(&str, &[u8])]) -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let commit = crate::test_fixture::commit(&repo, files);
        (tmp, commit)
    }

    /// Commit `files` onto `repo`'s HEAD (with the current HEAD as parent if any)
    /// and return the new commit oid. Lets a test build a history in one repo.
    fn commit_onto(repo: &gix::Repository, files: &[(&str, &[u8])]) -> String {
        crate::test_fixture::commit(repo, files)
    }

    /// The incremental files table (prior table + diff) must be byte-identical to
    /// a full rebuild at the new commit. A false "unchanged" would reuse a stale
    /// content hash — silently wrong — so this equivalence is the safety net.
    #[test]
    fn incremental_files_table_matches_full() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
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

    /// `len` well-mixed (CDC-cuttable) bytes: splitmix64, eight bytes per step.
    fn mix_blob(seed: u64, len: usize) -> Vec<u8> {
        let mut blob = Vec::with_capacity(len + 8);
        let mut i = 0u64;
        while blob.len() < len {
            let mut z = (seed << 40)
                .wrapping_add(i)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            blob.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
            i += 1;
        }
        blob.truncate(len);
        blob
    }

    fn as_refs(fs: &[(String, Vec<u8>)]) -> Vec<(&str, &[u8])> {
        fs.iter().map(|(p, b)| (p.as_str(), b.as_slice())).collect()
    }

    /// Commit raw-byte paths (so we can use a non-UTF8 filename).
    fn commit_onto_bytes(repo: &gix::Repository, files: &[(&[u8], &[u8])]) -> String {
        crate::test_fixture::commit_bytes(repo, files)
    }

    /// Helper to commit files with explicit git modes (e.g. symlinks, executables).
    fn commit_with_modes(repo: &gix::Repository, files: &[(&str, u32, &[u8])]) -> String {
        crate::test_fixture::commit_with_modes(repo, files)
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
        let repo = crate::test_fixture::init_bare(tmp.path());
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

    /// Modes other than 100644 (regular files) must survive the gix tree walk:
    /// symlinks (120000) and executable blobs (100755). This is a regression guard
    /// against interpreting `entry.mode.value()` as only a regular blob mode.
    #[test]
    fn archive_preserves_symlink_and_executable_modes() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let commit = commit_with_modes(
            &repo,
            &[
                ("plain.txt", 0o100644, b"plain".as_slice()),
                ("run.sh", 0o100755, b"#!/bin/sh\necho hi".as_slice()),
                ("link", 0o120000, b"plain.txt".as_slice()),
            ],
        );

        let builder = ArchiveBuilder::new(tmp.path());
        let manifest = builder.build_files_table(&commit).unwrap();
        let by_path: std::collections::HashMap<&[u8], u32> = manifest
            .files
            .iter()
            .map(|f| (f.path.as_slice(), f.mode))
            .collect();

        assert_eq!(
            by_path.get(b"plain.txt".as_slice()).copied(),
            Some(0o100644)
        );
        assert_eq!(
            by_path.get(b"run.sh".as_slice()).copied(),
            Some(0o100755),
            "executable mode must be preserved"
        );
        assert_eq!(
            by_path.get(b"link".as_slice()).copied(),
            Some(0o120000),
            "symlink mode must be preserved"
        );

        // The full chunk builder must also stream the symlink target bytes in order.
        let (_metadata, _chunks, _stats) = builder
            .build_chunks(&commit, 6, DEFAULT_ARCHIVE_CHUNK_SIZE)
            .unwrap();
    }

    /// Submodule entries (mode 160000) are not worktree files and must not be
    /// treated as blobs. This guards against a gix walk that only skipped trees.
    #[test]
    fn collect_blobs_raw_gix_skips_submodules() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());

        // An orphan commit that will stand in for a submodule object.
        let sub_tmp = tempfile::tempdir().unwrap();
        let sub_repo = crate::test_fixture::init_bare(sub_tmp.path());
        let sub_commit =
            crate::test_fixture::commit(&sub_repo, &[("README.md", b"submodule-readme")]);

        // Main tree contains a regular file and a submodule entry.
        let commit = crate::test_fixture::commit_with_modes(
            &repo,
            &[
                ("file.txt", 0o100644, b"file"),
                ("vendor/sub", 0o160000, sub_commit.as_bytes()),
            ],
        );
        let commit_id = commit.parse::<gix::hash::ObjectId>().unwrap();
        let tree_id = repo
            .find_commit(commit_id)
            .unwrap()
            .tree_id()
            .unwrap()
            .detach();

        let gix_repo = crate::gix_util::open_repo(tmp.path()).unwrap();
        let mut blobs = Vec::new();
        collect_blobs_raw_gix(&gix_repo, tree_id, &mut blobs).unwrap();

        assert_eq!(blobs.len(), 1, "submodule entry must be skipped");
        assert_eq!(blobs[0].0, b"file.txt");
        assert_eq!(blobs[0].2, 0o100644);
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
        let target = 8 * 1024 * 1024;
        let (_metadata, chunks, _stats) = builder.build_chunks(&commit, 6, target).unwrap();
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

    /// An empty tree must produce an empty manifest and zero chunks.
    #[test]
    fn archive_empty_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let commit = crate::test_fixture::commit(&repo, &[]);

        let builder = ArchiveBuilder::new(tmp.path());
        let manifest = builder.build_files_table(&commit).unwrap();
        assert!(manifest.files.is_empty());
        let (metadata, chunks, stats) = builder
            .build_chunks(&commit, 1, DEFAULT_ARCHIVE_CHUNK_SIZE)
            .unwrap();
        assert!(chunks.is_empty());
        assert_eq!(stats.raw_bytes, 0);
        assert!(metadata.files.is_empty());
    }

    #[test]
    fn archive_only_empty_files_keeps_an_empty_archive_chunk() {
        let (tmp, commit) = commit_files(&[("empty.txt", b"")]);
        let builder = ArchiveBuilder::new(tmp.path());

        let (metadata, chunks, stats) = builder
            .build_chunks(&commit, 1, DEFAULT_ARCHIVE_CHUNK_SIZE)
            .unwrap();
        assert_eq!(stats.files, 1);
        assert_eq!(stats.frames, 1);
        assert_eq!(metadata.frames.len(), 1);
        assert_eq!(chunks, vec![Vec::<u8>::new()]);

        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let output = builder
            .build_into_cas(
                &commit,
                &cas,
                None,
                1,
                None,
                None,
                DEFAULT_ARCHIVE_CHUNK_SIZE,
            )
            .unwrap();
        assert_eq!(output.download_bundle_hashes.len(), 1);
        assert_eq!(
            cas.get(&output.download_bundle_hashes[0]).unwrap(),
            Vec::<u8>::new()
        );
    }

    /// Negative: building from a missing mirror must error cleanly.
    #[test]
    fn archive_missing_mirror_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let builder = ArchiveBuilder::new(tmp.path().join("does-not-exist"));
        assert!(builder.build_files_table("HEAD").is_err());
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
            .build(&commit, &arch, &man, 6)
            .unwrap();

        let dest = out.path().join("extracted");
        std::fs::create_dir_all(&dest).unwrap();
        crate::extract::extract_archive(&arch, &man, &dest).unwrap();

        for (name, content) in &files {
            let got = std::fs::read(dest.join(name)).unwrap_or_else(|e| panic!("read {name}: {e}"));
            assert_eq!(&got, content, "roundtrip mismatch for {name}");
        }
    }

    /// The pipelined stream (parallel reads, cutting, compressing) must cut
    /// the same frames as the slice-based chunker over the same worktree, and
    /// hash each blob as the files table does.
    #[test]
    fn stream_frames_match_slice_cdc() {
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
        let layout = builder.layout(&commit).unwrap();
        let mut frames = 0usize;
        let (stream_bounds, sha1s) = builder
            .stream_frames(&layout, true, 1, None, |_| {
                frames += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(frames, stream_bounds.len(), "one frame per bound");

        let full = tree_stream(tmp.path(), &commit);
        assert_eq!(
            stream_bounds,
            cdc_frame_bounds(&full),
            "streaming CDC boundaries must match slice CDC"
        );
        assert!(
            stream_bounds.len() >= 3,
            "test data should span many frames"
        );
        let table = builder.build_files_table(&commit).unwrap();
        let expected: Vec<Vec<u8>> = table.files.iter().map(|f| f.blob_sha1.clone()).collect();
        let got: Vec<Vec<u8>> = sha1s.iter().map(|h| h.to_vec()).collect();
        assert_eq!(got, expected, "blob SHA-1s");
    }

    /// A non-UTF8 filename must survive into the archive manifest byte-exact (the
    /// streaming walk uses raw `name_bytes()` paths, not lossy UTF-8), so the
    /// client materializes the right path.
    #[test]
    fn archive_manifest_preserves_non_utf8_path() {
        let weird: &[u8] = b"caf\xe9.txt"; // invalid UTF-8 (Latin-1 é)
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let commit = commit_onto_bytes(&repo, &[(b"a.txt", b"hi"), (weird, b"payload")]);

        let (manifest, _chunks, _stats) = ArchiveBuilder::new(tmp.path())
            .build_chunks(&commit, 6, u64::MAX)
            .unwrap();

        assert!(
            manifest.files.iter().any(|f| f.path == weird),
            "archive manifest must keep the raw non-UTF8 path"
        );
    }
}
