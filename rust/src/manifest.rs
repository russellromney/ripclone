pub use crate::clonepack::{
    ChunkRef, ClonepackManifest, FileEntry, Fragment, FrameInfo, MetadataChunk,
};

use anyhow::{Context, Result};
use prost::Message;
use std::collections::{HashMap, HashSet};
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
        Self::decode_and_validate(&bytes)
    }

    /// Decode a metadata chunk and validate all geometry before any caller uses
    /// its offsets or file paths.
    pub fn decode_and_validate(bytes: &[u8]) -> Result<Self> {
        let manifest = Self::decode(bytes).context("decode metadata chunk")?;
        manifest.validate_geometry()?;
        Ok(manifest)
    }

    /// Validate frame/file/fragment geometry and reject illegal modes.
    pub fn validate_geometry(&self) -> Result<()> {
        const ALLOWED_MODES: [u32; 3] = [0o100644, 0o100755, 0o120000];
        let mut paths = HashSet::with_capacity(self.files.len());

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
            crate::fsutil::validate_relative_path(crate::fsutil::path_from_bytes(&entry.path))
                .with_context(|| {
                    format!(
                        "file {} has unsafe path: {}",
                        file_idx,
                        String::from_utf8_lossy(&entry.path)
                    )
                })?;
            if !paths.insert(entry.path.as_slice()) {
                anyhow::bail!(
                    "duplicate file path: {}",
                    String::from_utf8_lossy(&entry.path)
                );
            }
            if entry.blob_sha1.len() != 20 {
                anyhow::bail!(
                    "file {} has invalid blob_sha1 length: expected 20, got {}",
                    String::from_utf8_lossy(&entry.path),
                    entry.blob_sha1.len()
                );
            }
            // Files-table metadata intentionally omits archive fragments while
            // the archive is built asynchronously. Once frames exist, every
            // file must be represented so extraction cannot silently drop it.
            if entry.fragments.is_empty() && !self.frames.is_empty() {
                anyhow::bail!(
                    "file {} has no fragments",
                    String::from_utf8_lossy(&entry.path)
                );
            }
            for (frag_idx, fragment) in entry.fragments.iter().enumerate() {
                let frame_idx = usize::try_from(fragment.frame_index)
                    .context("manifest frame index does not fit in usize")?;
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

        let mut previous_chunk = None;
        let mut previous_end = 0u64;
        for (frame_idx, frame) in self.frames.iter().enumerate() {
            let end = frame
                .chunk_offset
                .checked_add(frame.compressed_len as u64)
                .ok_or_else(|| anyhow::anyhow!("frame {} compressed bounds overflow", frame_idx))?;

            match previous_chunk {
                None => {
                    if frame.chunk_index != 0 || frame.chunk_offset != 0 {
                        anyhow::bail!(
                            "first frame must start at archive chunk 0 offset 0, got chunk {} offset {}",
                            frame.chunk_index,
                            frame.chunk_offset
                        );
                    }
                }
                Some(previous) if frame.chunk_index < previous => {
                    anyhow::bail!(
                        "frame {} moves archive chunk index backwards from {} to {}",
                        frame_idx,
                        previous,
                        frame.chunk_index
                    );
                }
                Some(previous) if frame.chunk_index == previous => {
                    if frame.chunk_offset != previous_end {
                        anyhow::bail!(
                            "frame {} is not contiguous with archive chunk {}: offset {} != {}",
                            frame_idx,
                            frame.chunk_index,
                            frame.chunk_offset,
                            previous_end
                        );
                    }
                }
                Some(previous) => {
                    if frame.chunk_index != previous.saturating_add(1) || frame.chunk_offset != 0 {
                        anyhow::bail!(
                            "frame {} starts a non-contiguous archive chunk: previous {}, current {} offset {}",
                            frame_idx,
                            previous,
                            frame.chunk_index,
                            frame.chunk_offset
                        );
                    }
                }
            }
            previous_chunk = Some(frame.chunk_index);
            previous_end = end;
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
    fn validate_geometry_rejects_non_contiguous_archive_frames() {
        let mut manifest = MetadataChunk::new();
        manifest.frames = vec![
            FrameInfo {
                chunk_index: 0,
                chunk_offset: 0,
                compressed_len: 1,
                raw_len: 0,
            },
            FrameInfo {
                chunk_index: 1,
                chunk_offset: 0,
                compressed_len: 1,
                raw_len: 0,
            },
            FrameInfo {
                chunk_index: 0,
                chunk_offset: 1,
                compressed_len: 1,
                raw_len: 0,
            },
        ];

        let err = manifest.validate_geometry().unwrap_err();
        assert!(
            err.to_string()
                .contains("moves archive chunk index backwards")
        );
    }

    #[test]
    fn validate_geometry_rejects_duplicate_paths() {
        let mut manifest = MetadataChunk::new();
        manifest.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 0,
            raw_len: 0,
        });
        let file = |path| FileEntry {
            path,
            mode: 0o100644,
            blob_sha1: vec![0; 20],
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 0,
            }],
        };
        manifest.files = vec![file(b"same".to_vec()), file(b"same".to_vec())];

        let err = manifest.validate_geometry().unwrap_err();
        assert!(err.to_string().contains("duplicate file path"));
    }
}
