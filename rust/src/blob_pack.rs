use crate::git;
use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Build a git packfile containing undeltified blob objects from raw content.
///
/// `blobs` is a list of `(sha1_hex, content)` pairs. The SHA-1 is not trusted;
/// it is used only to sort objects for deterministic output. The actual object
/// data is re-hashed by `git index-pack` when the index is built.
///
/// Returns the SHA-1 hash (hex) of the packfile content.
pub fn build_blob_pack<P: AsRef<Path>>(
    pack_path: P,
    blobs: &[(String, Vec<u8>)],
) -> Result<String> {
    if blobs.is_empty() {
        anyhow::bail!("no blobs to pack");
    }

    let mut file = File::create(&pack_path)
        .with_context(|| format!("create blob pack {}", pack_path.as_ref().display()))?;
    let mut hasher = Sha1::new();

    // Deduplicate by sha1 hex. Multiple files may share the same blob; git
    // packfiles cannot contain duplicate objects.
    let mut unique: HashMap<&str, &[u8]> = HashMap::new();
    for (sha1_hex, content) in blobs {
        unique
            .entry(sha1_hex.as_str())
            .or_insert(content.as_slice());
    }

    // Header: "PACK" + version 2 + object count.
    let header = {
        let count = unique.len() as u32;
        let mut h = Vec::with_capacity(12);
        h.extend_from_slice(b"PACK");
        h.extend_from_slice(&2u32.to_be_bytes());
        h.extend_from_slice(&count.to_be_bytes());
        h
    };
    file.write_all(&header).context("write blob pack header")?;
    hasher.update(&header);

    // Sort by sha1 hex for deterministic pack output.
    let mut sorted: Vec<_> = unique.into_iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    for (sha1_hex, content) in sorted {
        // The pack entry stores type=blob, size=content length, and the
        // zlib-compressed content bytes. git index-pack reconstructs the
        // serialized object "blob <size>\0<content>" to compute the hash.
        let encoded = encode_object_entry(3, content.len(), content)?;
        file.write_all(&encoded)
            .with_context(|| format!("write blob object {}", sha1_hex))?;
        hasher.update(&encoded);
    }

    // Trailer: SHA-1 of everything before it.
    let trailer = hasher.finalize();
    file.write_all(&trailer)
        .context("write blob pack trailer")?;
    file.flush().context("flush blob pack")?;

    Ok(hex::encode(trailer))
}

/// Build a packfile + index for the given blobs and install them into
/// `.git/objects/pack/`.
///
/// `git_dir` should be the `.git` directory of the target repo.
pub fn build_and_install_blob_pack<P: AsRef<Path>>(
    git_dir: P,
    blobs: &[(String, Vec<u8>)],
) -> Result<PathBuf> {
    let git_dir = git_dir.as_ref();
    let pack_dir = git_dir.join("objects").join("pack");
    std::fs::create_dir_all(&pack_dir)?;

    // Build to a temporary path first; once we know the hash we rename it.
    let tmp_pack = pack_dir.join("blob-pack-tmp.pack");
    let pack_hash = build_blob_pack(&tmp_pack, blobs)?;

    // Name the pack with its hash so git index-pack writes the idx next to it.
    let final_path = pack_dir.join(format!("pack-{}.pack", pack_hash));
    std::fs::rename(&tmp_pack, &final_path)
        .with_context(|| format!("rename blob pack to {}", final_path.display()))?;

    // Generate the index. git index-pack will create pack-<hash>.idx.
    git::index_pack(git_dir, &final_path)?;

    Ok(final_path)
}

/// Streaming builder for a local blob pack.
///
/// Objects are written one at a time to a temporary file, so peak memory is
/// bounded by the largest single blob plus the internal zlib buffer. The
/// final pack is renamed to `pack-<hash>.pack` and indexed once the caller
/// calls `finalize`.
pub struct StreamingBlobPackBuilder {
    git_dir: PathBuf,
    tmp_path: PathBuf,
    writer: BufWriter<File>,
    hasher: Sha1,
    seen: HashSet<[u8; 20]>,
    expected_count: u32,
    written_count: u32,
}

impl StreamingBlobPackBuilder {
    /// Create a new builder. `expected_objects` is the number of unique blobs
    /// that will be added; it is written into the pack header.
    pub fn new<P: AsRef<Path>>(git_dir: P, expected_objects: usize) -> Result<Self> {
        let git_dir = git_dir.as_ref().to_path_buf();
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;
        let tmp_path = pack_dir.join("blob-pack-tmp.pack");
        let file = File::create(&tmp_path)
            .with_context(|| format!("create temp blob pack {}", tmp_path.display()))?;
        // A large buffer amortizes syscalls when writing many small objects.
        let mut writer = BufWriter::with_capacity(256 * 1024, file);

        let expected_count = expected_objects as u32;
        let header = {
            let mut h = Vec::with_capacity(12);
            h.extend_from_slice(b"PACK");
            h.extend_from_slice(&2u32.to_be_bytes());
            h.extend_from_slice(&expected_count.to_be_bytes());
            h
        };
        writer
            .write_all(&header)
            .context("write blob pack header")?;

        let mut hasher = Sha1::new();
        hasher.update(&header);

        Ok(Self {
            git_dir,
            tmp_path,
            writer,
            hasher,
            seen: HashSet::with_capacity(expected_objects),
            expected_count,
            written_count: 0,
        })
    }

    /// Add a blob to the pack. Duplicate SHA-1s are ignored. `sha1` is used
    /// only for deduplication; the actual object hash is recomputed by git
    /// from the content.
    pub fn add_blob(&mut self, sha1: &[u8; 20], content: &[u8]) -> Result<()> {
        if !self.seen.insert(*sha1) {
            return Ok(());
        }
        let header = encode_type_and_size(3, content.len());
        let compressed = compress_blob(content)?;
        {
            let mut hw = HashingWriter {
                inner: &mut self.writer,
                hasher: &mut self.hasher,
            };
            hw.write_all(&header).context("write object header")?;
            hw.write_all(&compressed).context("write compressed blob")?;
        }
        self.written_count += 1;
        Ok(())
    }

    /// Write a pre-compressed blob entry to the pack. The caller is responsible
    /// for deduplication. Used by the parallel builder.
    pub fn write_compressed_entry(&mut self, content_len: usize, compressed: &[u8]) -> Result<()> {
        let header = encode_type_and_size(3, content_len);
        {
            let mut hw = HashingWriter {
                inner: &mut self.writer,
                hasher: &mut self.hasher,
            };
            hw.write_all(&header).context("write object header")?;
            hw.write_all(compressed).context("write compressed blob")?;
        }
        self.written_count += 1;
        Ok(())
    }

    /// Finish the pack, rename it to its content-hash name, run
    /// `git index-pack`, and return the final `.pack` path.
    pub fn finalize(mut self) -> Result<PathBuf> {
        if self.written_count != self.expected_count {
            anyhow::bail!(
                "blob pack object count mismatch: expected {}, wrote {}",
                self.expected_count,
                self.written_count
            );
        }
        let trailer = self.hasher.finalize();
        self.writer
            .write_all(&trailer)
            .context("write blob pack trailer")?;
        self.writer.flush().context("flush blob pack")?;
        let mut file = self
            .writer
            .into_inner()
            .context("unwrap blob pack writer")?;
        drop(file);

        let pack_hash = hex::encode(trailer);
        let final_path = self
            .tmp_path
            .parent()
            .expect("pack dir exists")
            .join(format!("pack-{}.pack", pack_hash));
        std::fs::rename(&self.tmp_path, &final_path)
            .with_context(|| format!("rename blob pack to {}", final_path.display()))?;

        git::index_pack(&self.git_dir, &final_path)?;
        Ok(final_path)
    }
}

/// Input to the blob-pack builder. Most blobs can be sent as a slice into a
/// shared decompressed frame, avoiding a per-blob copy. Multi-fragment files
/// that have already been assembled into an owned `Vec` can send it directly.
#[derive(Clone)]
pub enum BlobPackInput {
    /// A view into a decompressed frame; the frame is kept alive via `Arc`.
    FrameSlice {
        sha1: [u8; 20],
        frame: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    },
    /// An already-assembled blob owned by the caller.
    Owned { sha1: [u8; 20], content: Vec<u8> },
}

impl BlobPackInput {
    pub fn sha1(&self) -> &[u8; 20] {
        match self {
            BlobPackInput::FrameSlice { sha1, .. } => sha1,
            BlobPackInput::Owned { sha1, .. } => sha1,
        }
    }

    fn content(&self) -> &[u8] {
        match self {
            BlobPackInput::FrameSlice {
                frame, offset, len, ..
            } => &frame[*offset..*offset + *len],
            BlobPackInput::Owned { content, .. } => content,
        }
    }

    fn content_len(&self) -> usize {
        self.content().len()
    }
}

/// Spawn a background thread pool that builds a blob pack from a channel.
///
/// Compression is parallelized across `RIPCLONE_BLOB_PACK_THREADS` worker
/// threads; a single writer thread appends the compressed entries to the pack
/// file so the final pack remains valid and sequential. This keeps the
/// per-blob memory bounded while saturating CPU for zlib compression.
///
/// Returns a sender for blob entries and a join handle that yields the final
/// pack path. Drop the sender when all blobs have been submitted, then join
/// the handle to finalize.
pub fn spawn_blob_pack_builder<P: AsRef<Path>>(
    git_dir: P,
    expected_objects: usize,
) -> Result<(
    crossbeam_channel::Sender<BlobPackInput>,
    std::thread::JoinHandle<Result<PathBuf>>,
)> {
    let git_dir = git_dir.as_ref().to_path_buf();
    let channel_depth = std::env::var("RIPCLONE_BLOB_PACK_CHANNEL_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2048);
    let threads = std::env::var("RIPCLONE_BLOB_PACK_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
        .max(1);

    // Channel from extractor threads -> compressor pool.
    let (raw_tx, raw_rx): (Sender<BlobPackInput>, Receiver<BlobPackInput>) = bounded(channel_depth);

    // Channel from compressor pool -> pack writer thread.
    let (compressed_tx, compressed_rx): (
        Sender<Result<(usize, Vec<u8>)>>,
        Receiver<Result<(usize, Vec<u8>)>>,
    ) = bounded(channel_depth);

    // Shared deduplication set. Compressors claim a SHA-1 before compressing so
    // duplicate blobs are only written once.
    let seen: Arc<Mutex<HashSet<[u8; 20]>>> =
        Arc::new(Mutex::new(HashSet::with_capacity(expected_objects)));

    // Spawn compressor threads.
    let mut compressor_handles: Vec<JoinHandle<()>> = Vec::with_capacity(threads);
    for _ in 0..threads {
        let raw_rx: Receiver<BlobPackInput> = raw_rx.clone();
        let compressed_tx: Sender<Result<(usize, Vec<u8>)>> = compressed_tx.clone();
        let seen = Arc::clone(&seen);
        compressor_handles.push(std::thread::spawn(move || {
            while let Ok(input) = raw_rx.recv() {
                let sha1 = *input.sha1();
                // Claim this SHA-1; if another thread already has it, skip.
                let is_new = {
                    let mut set = seen.lock().unwrap_or_else(|e| e.into_inner());
                    set.insert(sha1)
                };
                if !is_new {
                    continue;
                }
                let content = input.content();
                let len = content.len();
                let res = compress_blob(content)
                    .with_context(|| format!("compress blob of {} bytes", len))
                    .map(|compressed| (len, compressed));
                if compressed_tx.send(res).is_err() {
                    break;
                }
            }
        }));
    }
    drop(raw_rx);
    drop(compressed_tx);

    // Spawn the pack writer thread.
    let writer_handle = std::thread::spawn(move || {
        let mut builder = StreamingBlobPackBuilder::new(&git_dir, expected_objects)
            .context("create streaming blob pack builder")?;
        while let Ok(result) = compressed_rx.recv() {
            let (content_len, compressed) = result.context("compressor thread failed")?;
            builder
                .write_compressed_entry(content_len, &compressed)
                .context("write compressed blob entry")?;
        }
        builder.finalize().context("finalize blob pack")
    });

    // Outer join handle waits for both compressors and the writer, propagating
    // any error from the writer (compressor panics are already unwrapped).
    let handle = std::thread::spawn(move || {
        for h in compressor_handles {
            h.join()
                .map_err(|e| anyhow::anyhow!("blob pack compressor thread panicked: {:?}", e))?;
        }
        writer_handle
            .join()
            .map_err(|e| anyhow::anyhow!("blob pack writer thread panicked: {:?}", e))?
    });

    Ok((raw_tx, handle))
}

/// A Write adapter that hashes every byte before forwarding it.
struct HashingWriter<'a, W> {
    inner: W,
    hasher: &'a mut Sha1,
}

impl<'a, W: Write> Write for HashingWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Return the raw git object data for a blob: `blob <size>\0<content>`.
fn git_blob_object_data(content: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(5 + content.len().to_string().len() + 1 + content.len());
    data.extend_from_slice(b"blob ");
    data.extend_from_slice(content.len().to_string().as_bytes());
    data.push(0);
    data.extend_from_slice(content);
    data
}

/// Encode a pack object entry: variable-length type+size header followed by
/// zlib-compressed object content. `size` is the uncompressed content length;
/// git reconstructs the serialized object header when indexing.
fn encode_object_entry(obj_type: u8, size: usize, content: &[u8]) -> Result<Vec<u8>> {
    let mut header = encode_type_and_size(obj_type, size);
    let compressed = compress_blob(content)?;
    header.extend_from_slice(&compressed);
    Ok(header)
}

/// Compress raw blob content with the configured pack compression level.
fn compress_blob(content: &[u8]) -> Result<Vec<u8>> {
    let level = std::env::var("RIPCLONE_BLOB_PACK_COMPRESSION_LEVEL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(level.min(9).max(0)));
    encoder
        .write_all(content)
        .context("compress blob content")?;
    encoder.finish().context("finish blob compression")
}

/// Encode the variable-length integer used in git packfiles.
/// First byte: 1 continuation bit + 3 type bits + 4 size bits.
/// Subsequent bytes: 1 continuation bit + 7 size bits.
fn encode_type_and_size(obj_type: u8, mut size: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let first = ((obj_type & 0x7) << 4) | (size as u8 & 0xf);
    size >>= 4;
    if size > 0 {
        out.push(first | 0x80);
    } else {
        out.push(first);
        return out;
    }

    while size > 0 {
        let byte = (size as u8) & 0x7f;
        size >>= 7;
        if size > 0 {
            out.push(byte | 0x80);
        } else {
            out.push(byte);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_type_and_size() {
        // Simple cases.
        assert_eq!(encode_type_and_size(3, 0), vec![0b011_0000]);
        assert_eq!(encode_type_and_size(3, 15), vec![0b011_1111]);
        // Needs continuation: size 16 = 0b1_0000, type 3 -> 0b011_0000 | 0x80.
        assert_eq!(encode_type_and_size(3, 16), vec![0b1011_0000, 0b0000_0001]);
    }

    #[test]
    fn test_git_blob_object_data() {
        let data = git_blob_object_data(b"hello");
        assert_eq!(&data, b"blob 5\0hello");
    }

    #[test]
    fn test_build_blob_pack_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let git_dir = tmp.path().join(".git");
        std::fs::create_dir_all(&git_dir).unwrap();
        let status = std::process::Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(tmp.path())
            .status()
            .unwrap();
        assert!(status.success());

        let blobs = vec![
            (hex::encode(sha1_bytes(b"hello")), b"hello".to_vec()),
            (hex::encode(sha1_bytes(b"world")), b"world".to_vec()),
        ];
        let pack_path = build_and_install_blob_pack(&git_dir, &blobs).unwrap();
        assert!(pack_path.exists());
        let idx_path = pack_path.with_extension("idx");
        assert!(idx_path.exists());

        // Verify the pack is valid.
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(tmp.path())
            .args(["verify-pack", "-v", pack_path.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(output.status.success(), "verify-pack failed: {:?}", output);

        // Verify git can read a blob.
        let mut obj_data = Vec::new();
        obj_data.extend_from_slice(b"blob 5\0hello");
        let hash = hex::encode(sha1::Sha1::digest(&obj_data));
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(tmp.path())
            .args(["cat-file", "blob", &hash])
            .output()
            .unwrap();
        assert!(output.status.success(), "cat-file failed: {:?}", output);
        assert_eq!(output.stdout, b"hello");
    }

    fn sha1_bytes(data: &[u8]) -> [u8; 20] {
        sha1::Sha1::digest(data).into()
    }

    #[test]
    fn test_streaming_blob_pack_builder_deduplicates() {
        let tmp = tempfile::TempDir::new().unwrap();
        let git_dir = tmp.path().join(".git");
        std::fs::create_dir_all(&git_dir).unwrap();
        let status = std::process::Command::new("git")
            .arg("init")
            .arg("-q")
            .arg(tmp.path())
            .status()
            .unwrap();
        assert!(status.success());

        // Three files, two unique blobs.
        let mut builder = StreamingBlobPackBuilder::new(&git_dir, 2).unwrap();
        builder.add_blob(&sha1_bytes(b"a"), b"a").unwrap();
        builder.add_blob(&sha1_bytes(b"b"), b"b").unwrap();
        builder.add_blob(&sha1_bytes(b"a"), b"a").unwrap();
        let pack_path = builder.finalize().unwrap();
        assert!(pack_path.exists());

        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(tmp.path())
            .args(["verify-pack", "-v", pack_path.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(output.status.success(), "verify-pack failed: {:?}", output);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let lines: Vec<&str> = stdout.lines().filter(|l| l.contains("blob")).collect();
        assert_eq!(lines.len(), 2, "expected 2 unique blobs, got: {:?}", lines);
    }
}
