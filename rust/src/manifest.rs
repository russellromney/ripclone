pub use crate::clonepack::{
    ChunkRef, ClonepackManifest, FileEntry, Fragment, FrameInfo, MetadataChunk,
};

use anyhow::{Context, Result};
use prost::Message;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::io::{Read, Write};

pub type Manifest = MetadataChunk;

impl MetadataChunk {
    /// Create an empty metadata chunk.
    pub fn new() -> Self {
        Self {
            skeleton_pack: Vec::new(),
            skeleton_idx: Vec::new(),
            prebuilt_index: Vec::new(),
            frames: Vec::new(),
            files: Vec::new(),
        }
    }

    /// Serialize the metadata chunk as protobuf.
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        let bytes = self.encode_to_vec();
        writer.write_all(&bytes).context("write metadata chunk")?;
        Ok(())
    }

    /// Deserialize a metadata chunk from protobuf and validate its geometry.
    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .context("read metadata chunk")?;
        let manifest = Self::decode(bytes.as_slice()).context("decode metadata chunk")?;
        manifest.validate_geometry()?;
        Ok(manifest)
    }

    /// Validate frame/file/fragment geometry and reject illegal modes.
    pub fn validate_geometry(&self) -> Result<()> {
        const ALLOWED_MODES: [u32; 3] = [0o100644, 0o100755, 0o120000];

        for (file_idx, entry) in self.files.iter().enumerate() {
            if !ALLOWED_MODES.contains(&entry.mode) {
                anyhow::bail!(
                    "file {} has illegal mode 0o{:o}",
                    String::from_utf8_lossy(&entry.path),
                    entry.mode
                );
            }
            if entry.path.is_empty() {
                anyhow::bail!("file {} has empty path", file_idx);
            }
            if entry.fragments.is_empty() {
                anyhow::bail!(
                    "file {} has no fragments",
                    String::from_utf8_lossy(&entry.path)
                );
            }
            for (frag_idx, fragment) in entry.fragments.iter().enumerate() {
                let frame_idx = fragment.frame_index as usize;
                if frame_idx >= self.frames.len() {
                    anyhow::bail!(
                        "file {} fragment {} references missing frame {}",
                        String::from_utf8_lossy(&entry.path),
                        frag_idx,
                        fragment.frame_index
                    );
                }
                let frame = &self.frames[frame_idx];
                let end = fragment
                    .frame_offset
                    .checked_add(fragment.raw_len)
                    .ok_or_else(|| anyhow::anyhow!("fragment bounds overflow"))?;
                if end > frame.raw_len {
                    anyhow::bail!(
                        "file {} fragment {} extends past frame {}: {}+{} > {}",
                        String::from_utf8_lossy(&entry.path),
                        frag_idx,
                        fragment.frame_index,
                        fragment.frame_offset,
                        fragment.raw_len,
                        frame.raw_len
                    );
                }
            }
        }

        for (frame_idx, frame) in self.frames.iter().enumerate() {
            let end = frame
                .chunk_offset
                .checked_add(frame.compressed_len as u64)
                .ok_or_else(|| anyhow::anyhow!("frame {} compressed bounds overflow", frame_idx))?;
            // We cannot check against the real chunk length here because the
            // manifest does not carry archive chunk bytes, but we can at least
            // ensure the arithmetic did not wrap.
            let _ = end;
        }

        Ok(())
    }

    /// Group file fragments by frame index for extraction.
    /// Returns a map from frame index to a list of `(file_index, fragment_index)`
    /// pairs so the consumer can locate the owning `FileEntry` and `Fragment`.
    pub fn fragments_by_frame(&self) -> HashMap<u32, Vec<(usize, usize)>> {
        let mut map: HashMap<u32, Vec<(usize, usize)>> = HashMap::new();
        for (file_idx, entry) in self.files.iter().enumerate() {
            for (frag_idx, fragment) in entry.fragments.iter().enumerate() {
                map.entry(fragment.frame_index)
                    .or_default()
                    .push((file_idx, frag_idx));
            }
        }
        map
    }
}

/// Verify every frame and each single-fragment file's SHA-1 against the supplied
/// archive chunks. `fetch_chunk` returns the uncompressed bytes for a given
/// chunk index.
pub fn verify_archive<F>(metadata: &MetadataChunk, mut fetch_chunk: F) -> Result<()>
where
    F: FnMut(u32) -> Result<Vec<u8>>,
{
    let by_frame = metadata.fragments_by_frame();

    for (frame_index, frame) in metadata.frames.iter().enumerate() {
        let chunk = fetch_chunk(frame.chunk_index)
            .with_context(|| format!("fetch chunk {}", frame.chunk_index))?;
        let start = frame.chunk_offset as usize;
        let end = start + frame.compressed_len as usize;
        if end > chunk.len() {
            anyhow::bail!("frame {} extends past chunk end", frame_index);
        }
        let raw = zstd::decode_all(&chunk[start..end])
            .with_context(|| format!("decompress frame {}", frame_index))?;
        if raw.len() != frame.raw_len as usize {
            anyhow::bail!(
                "frame {} raw length mismatch: {} vs {}",
                frame_index,
                raw.len(),
                frame.raw_len
            );
        }

        if let Some(pairs) = by_frame.get(&(frame_index as u32)) {
            for (file_idx, frag_idx) in pairs {
                let entry = &metadata.files[*file_idx];
                let fragment = &entry.fragments[*frag_idx];
                let off = fragment.frame_offset as usize;
                let len = fragment.raw_len as usize;
                if off + len > raw.len() {
                    anyhow::bail!(
                        "fragment for {} extends past frame {}",
                        String::from_utf8_lossy(&entry.path),
                        frame_index
                    );
                }
                let content = &raw[off..off + len];
                let hash = Sha1::digest(content);
                if hash.as_slice() != entry.blob_sha1 {
                    // We can only verify the full file SHA-1 when the file
                    // is contained in a single fragment. Multi-fragment
                    // files are verified during extraction by concatenating
                    // fragments in order.
                    if entry.fragments.len() == 1 {
                        anyhow::bail!("sha1 mismatch for {}", String::from_utf8_lossy(&entry.path));
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clonepack::Fragment;

    #[test]
    fn manifest_roundtrip() {
        let mut manifest = MetadataChunk::new();
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 13,
            raw_len: 5,
        });
        manifest.files.push(FileEntry {
            path: b"hello.txt".to_vec(),
            mode: 0o100644,
            blob_sha1: vec![1u8; 20],
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 5,
            }],
        });

        let mut buf = Vec::new();
        manifest.write(&mut buf).unwrap();

        let mut reader = buf.as_slice();
        let parsed = MetadataChunk::read(&mut reader).unwrap();
        assert_eq!(parsed.frames.len(), 1);
        assert_eq!(parsed.files.len(), 1);
        assert_eq!(parsed.files[0].path, b"hello.txt");
        assert_eq!(parsed.files[0].mode, 0o100644);
        assert_eq!(parsed.files[0].blob_sha1, vec![1u8; 20]);
    }

    #[test]
    fn verify_archive_catches_sha1_mismatch() {
        let raw = b"hello";
        let compressed = zstd::encode_all(raw.as_slice(), 1).unwrap();

        let mut manifest = MetadataChunk::new();
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: compressed.len() as u32,
            raw_len: raw.len() as u32,
        });
        manifest.files.push(FileEntry {
            path: b"x".to_vec(),
            mode: 0o100644,
            blob_sha1: vec![0u8; 20], // wrong hash
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: raw.len() as u32,
            }],
        });

        let archive = compressed.clone();
        let err = verify_archive(&manifest, |_| Ok(archive.clone())).unwrap_err();
        assert!(
            err.to_string().contains("sha1 mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn verify_archive_happy_path() {
        let raw = b"hello world";
        let compressed = zstd::encode_all(raw.as_slice(), 1).unwrap();

        let mut manifest = MetadataChunk::new();
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: compressed.len() as u32,
            raw_len: raw.len() as u32,
        });
        manifest.files.push(FileEntry {
            path: b"x".to_vec(),
            mode: 0o100644,
            blob_sha1: sha1_bytes(raw.as_slice()).to_vec(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: raw.len() as u32,
            }],
        });

        verify_archive(&manifest, |_| Ok(compressed.clone())).unwrap();
    }

    fn sha1_bytes(data: &[u8]) -> [u8; 20] {
        Sha1::digest(data).into()
    }
}
