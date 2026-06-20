use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Streaming writer for the head-blobs pack file.
///
/// Chunks are fetched concurrently and may complete out of order. This writer
/// writes each chunk to the correct file offset and updates the running SHA-256
/// hash incrementally as chunks become available in order. Out-of-order chunks
/// are buffered in memory until their predecessors arrive.
pub struct HeadBlobsWriter {
    temp_path: PathBuf,
    file: File,
    chunk_offsets: Vec<u64>,
    chunk_lens: Vec<u64>,
    next_index: usize,
    buffer: BTreeMap<usize, Vec<u8>>,
    hasher: Sha256,
    total_len: u64,
    written: u64,
}

impl HeadBlobsWriter {
    pub fn new(pack_dir: &Path, chunk_refs: &[crate::clonepack::ChunkRef]) -> Result<Self> {
        std::fs::create_dir_all(pack_dir)?;

        let mut chunk_offsets = Vec::with_capacity(chunk_refs.len());
        let mut chunk_lens = Vec::with_capacity(chunk_refs.len());
        let mut off = 0u64;
        for r in chunk_refs {
            chunk_offsets.push(off);
            chunk_lens.push(r.len);
            off += r.len;
        }

        let temp_path = pack_dir.join("pack-incomplete.pack");
        let file = File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temp_path)
            .with_context(|| format!("open head-blobs pack {}", temp_path.display()))?;

        Ok(Self {
            temp_path,
            file,
            chunk_offsets,
            chunk_lens,
            next_index: 0,
            buffer: BTreeMap::new(),
            hasher: Sha256::new(),
            total_len: off,
            written: 0,
        })
    }

    /// Write a chunk at its index. Returns `true` when every chunk has been
    /// written and hashed.
    pub fn write_chunk(&mut self, index: usize, bytes: &[u8]) -> Result<bool> {
        if index >= self.chunk_offsets.len() {
            anyhow::bail!("head-blobs chunk index {} out of range", index);
        }
        let expected = self.chunk_lens[index] as usize;
        if bytes.len() != expected {
            anyhow::bail!(
                "head-blobs chunk {} size mismatch: expected {}, got {}",
                index,
                expected,
                bytes.len()
            );
        }

        if index == self.next_index {
            self.write_at_offset(self.chunk_offsets[index], bytes)?;
            self.hasher.update(bytes);
            self.written += bytes.len() as u64;
            self.next_index += 1;
            // Flush any buffered sequential chunks.
            while let Some(bytes) = self.buffer.remove(&self.next_index) {
                self.write_at_offset(self.chunk_offsets[self.next_index], &bytes)?;
                self.hasher.update(&bytes);
                self.written += bytes.len() as u64;
                self.next_index += 1;
            }
        } else {
            self.buffer.insert(index, bytes.to_vec());
        }

        Ok(self.next_index >= self.chunk_offsets.len())
    }

    fn write_at_offset(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        self.file
            .seek(SeekFrom::Start(offset))
            .context("seek head-blobs pack")?;
        self.file
            .write_all(bytes)
            .context("write head-blobs pack chunk")?;
        Ok(())
    }

    /// Rename the completed pack to its content hash and write the companion idx.
    /// Returns the pack hash and the final pack path.
    pub fn finalize(self, idx_data: &[u8]) -> Result<(String, PathBuf)> {
        if self.next_index < self.chunk_offsets.len() {
            anyhow::bail!(
                "head-blobs pack incomplete: {} of {} chunks written",
                self.next_index,
                self.chunk_offsets.len()
            );
        }
        // Make sure the file is the right length (holes on some filesystems can
        // leave the file short if the last chunk did not extend to the end).
        self.file
            .set_len(self.total_len)
            .context("truncate head-blobs pack")?;
        drop(self.file);

        let hash = format!("{:x}", self.hasher.finalize());
        let final_path = self
            .temp_path
            .parent()
            .unwrap_or(Path::new(""))
            .join(format!("pack-{}.pack", hash));
        std::fs::rename(&self.temp_path, &final_path)
            .with_context(|| format!("rename head-blobs pack to {}", final_path.display()))?;

        let idx_path = final_path.with_extension("idx");
        std::fs::write(&idx_path, idx_data)
            .with_context(|| format!("write head-blobs idx {}", idx_path.display()))?;

        Ok((hash, final_path))
    }
}
