pub use crate::clonepack::{FileEntry, FrameInfo, MetadataChunk, MetadataDelta};

use anyhow::{Context, Result};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};

/// A zstd frame's first bytes. A protobuf metadata chunk cannot start with
/// them: 0x28 would be field 5 as a varint, and field 5 is a message.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
const METADATA_ZSTD_LEVEL: i32 = 3;
/// Largest decompressed metadata chunk a reader accepts.
const MAX_METADATA_BYTES: u64 = 1 << 30;

impl MetadataChunk {
    /// Create an empty metadata chunk.
    pub fn new() -> Self {
        Self {
            frames: Vec::new(),
            files: Vec::new(),
            gitlinks: Vec::new(),
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

    /// The stored form: the protobuf, zstd-compressed. Paths and hashes
    /// compress about 3x, and every clone downloads this before its files.
    pub fn encode_stored(&self) -> Result<Vec<u8>> {
        zstd::encode_all(self.encode_to_vec().as_slice(), METADATA_ZSTD_LEVEL)
            .context("compress metadata chunk")
    }

    /// Decode a metadata chunk, stored compressed or as plain protobuf, and
    /// validate all geometry before any caller uses its offsets or file paths.
    /// Plain protobuf never starts with the zstd magic: every field here is
    /// length-delimited, so its first byte is never 0x28.
    pub fn decode_and_validate(bytes: &[u8]) -> Result<Self> {
        let plain;
        let bytes = if bytes.starts_with(&ZSTD_MAGIC) {
            let mut out = Vec::new();
            zstd::stream::Decoder::new(bytes)
                .context("open metadata chunk")?
                .take(MAX_METADATA_BYTES + 1)
                .read_to_end(&mut out)
                .context("decompress metadata chunk")?;
            anyhow::ensure!(
                out.len() as u64 <= MAX_METADATA_BYTES,
                "metadata chunk is larger than {MAX_METADATA_BYTES} bytes"
            );
            plain = out;
            plain.as_slice()
        } else {
            bytes
        };
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

        for gitlink in &self.gitlinks {
            if gitlink.is_empty() {
                anyhow::bail!("gitlink has empty path");
            }
            crate::fsutil::validate_relative_path(crate::fsutil::path_from_bytes(gitlink))
                .with_context(|| {
                    format!("unsafe gitlink path: {}", String::from_utf8_lossy(gitlink))
                })?;
            if !paths.insert(gitlink.as_slice()) {
                anyhow::bail!("duplicate path: {}", String::from_utf8_lossy(gitlink));
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

/// Where each frame of one table is in another: for each frame of `from`,
/// its index in `to` plus one, or 0 when `to` lacks it. Frames are the same
/// frame at the same offset of the same bundle; `from_bundles` and
/// `to_bundles` are the bundle hashes the tables' chunk indexes name.
pub fn frame_map(
    from: &[FrameInfo],
    from_bundles: &[String],
    to: &[FrameInfo],
    to_bundles: &[String],
) -> Result<Vec<u32>> {
    let bundle = |bundles: &[String], frame: &FrameInfo| -> Result<String> {
        bundles
            .get(frame.chunk_index as usize)
            .cloned()
            .with_context(|| format!("frame names missing bundle {}", frame.chunk_index))
    };
    let mut at = HashMap::with_capacity(to.len());
    for (index, frame) in to.iter().enumerate() {
        let slot = u32::try_from(index + 1).context("too many frames")?;
        at.insert((bundle(to_bundles, frame)?, frame.chunk_offset), slot);
    }
    from.iter()
        .map(|frame| {
            Ok(at
                .get(&(bundle(from_bundles, frame)?, frame.chunk_offset))
                .copied()
                .unwrap_or(0))
        })
        .collect()
}

/// The frame map of `first` then `second` (see [`frame_map`]).
pub fn compose_frame_maps(first: &[u32], second: &[u32]) -> Result<Vec<u32>> {
    first
        .iter()
        .map(|&slot| match slot {
            0 => Ok(0),
            slot => second
                .get(slot as usize - 1)
                .copied()
                .context("frame map names a missing frame"),
        })
        .collect()
}

impl MetadataDelta {
    /// The changes from `base` to `table`. `base_frames` is where each base
    /// frame is in `table` (see [`frame_map`]).
    pub fn between(
        base: &MetadataChunk,
        base_frames: Vec<u32>,
        table: &MetadataChunk,
    ) -> Result<Self> {
        anyhow::ensure!(
            base_frames.len() == base.frames.len(),
            "frame map has {} frames, the base {}",
            base_frames.len(),
            base.frames.len()
        );
        let in_table: HashSet<&[u8]> = table.files.iter().map(|f| f.path.as_slice()).collect();
        let base_files: HashMap<&[u8], &FileEntry> =
            base.files.iter().map(|f| (f.path.as_slice(), f)).collect();
        let kept = |entry: &FileEntry| {
            base_files.get(entry.path.as_slice()).is_some_and(|old| {
                old.mode == entry.mode
                    && old.blob_sha1 == entry.blob_sha1
                    && old.fragments.len() == entry.fragments.len()
                    && old
                        .fragments
                        .iter()
                        .zip(&entry.fragments)
                        .all(|(old, new)| {
                            base_frames.get(old.frame_index as usize)
                                == Some(&(new.frame_index + 1))
                                && old.frame_offset == new.frame_offset
                                && old.raw_len == new.raw_len
                        })
            })
        };
        Ok(Self {
            frames: table.frames.clone(),
            files: table.files.iter().filter(|f| !kept(f)).cloned().collect(),
            removed: (base.files.iter())
                .filter(|f| !in_table.contains(f.path.as_slice()))
                .map(|f| f.path.clone())
                .collect(),
            gitlinks: table.gitlinks.clone(),
            base_frames,
        })
    }

    /// The table this delta makes of `base`, validated.
    pub fn apply(&self, base: &MetadataChunk) -> Result<MetadataChunk> {
        anyhow::ensure!(
            self.base_frames.len() == base.frames.len(),
            "delta maps {} base frames, the base has {}",
            self.base_frames.len(),
            base.frames.len()
        );
        let changed: HashMap<&[u8], &FileEntry> =
            self.files.iter().map(|f| (f.path.as_slice(), f)).collect();
        let removed: HashSet<&[u8]> = self.removed.iter().map(Vec::as_slice).collect();
        let (mut files, mut placed, mut removed_seen) =
            (Vec::with_capacity(base.files.len()), HashSet::new(), 0);
        for entry in &base.files {
            let path = entry.path.as_slice();
            if removed.contains(path) {
                removed_seen += 1;
            } else if let Some(&new) = changed.get(path) {
                placed.insert(path);
                files.push(new.clone());
            } else {
                let mut entry = entry.clone();
                for fragment in &mut entry.fragments {
                    let slot = (self.base_frames.get(fragment.frame_index as usize).copied())
                        .context("base fragment names a missing frame")?;
                    anyhow::ensure!(
                        slot != 0,
                        "{} keeps a frame the delta drops",
                        String::from_utf8_lossy(path)
                    );
                    fragment.frame_index = slot - 1;
                }
                files.push(entry);
            }
        }
        anyhow::ensure!(
            removed_seen == removed.len(),
            "delta removes a path its base lacks"
        );
        files.extend(
            (self.files.iter())
                .filter(|f| !placed.contains(f.path.as_slice()))
                .cloned(),
        );
        let table = MetadataChunk {
            frames: self.frames.clone(),
            files,
            gitlinks: self.gitlinks.clone(),
        };
        table.validate_geometry()?;
        Ok(table)
    }

    /// The stored form: the protobuf, zstd-compressed.
    pub fn encode_stored(&self) -> Result<Vec<u8>> {
        zstd::encode_all(self.encode_to_vec().as_slice(), METADATA_ZSTD_LEVEL)
            .context("compress metadata delta")
    }

    /// Decode the stored form.
    pub fn decode_stored(bytes: &[u8]) -> Result<Self> {
        let mut out = Vec::new();
        zstd::stream::Decoder::new(bytes)
            .context("open metadata delta")?
            .take(MAX_METADATA_BYTES + 1)
            .read_to_end(&mut out)
            .context("decompress metadata delta")?;
        anyhow::ensure!(
            out.len() as u64 <= MAX_METADATA_BYTES,
            "metadata delta is larger than {MAX_METADATA_BYTES} bytes"
        );
        Self::decode(out.as_slice()).context("decode metadata delta")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clonepack::Fragment;

    fn frame(chunk_index: u32, chunk_offset: u64, compressed_len: u32) -> FrameInfo {
        FrameInfo {
            chunk_index,
            chunk_offset,
            compressed_len,
            raw_len: 100,
        }
    }

    fn file(path: &str, blob: u8, fragments: &[(u32, u32)]) -> FileEntry {
        FileEntry {
            path: path.as_bytes().to_vec(),
            mode: 0o100644,
            blob_sha1: vec![blob; 20],
            fragments: (fragments.iter())
                .map(|&(frame_index, frame_offset)| Fragment {
                    frame_index,
                    frame_offset,
                    raw_len: 10,
                })
                .collect(),
        }
    }

    /// A delta keeps the base entries whose frames the table keeps, renumbers
    /// their frames, and carries every other change: an entry changed in
    /// place, one moved off a dropped frame, one removed, one added.
    #[test]
    fn delta_rebuilds_the_table_from_its_base() {
        let bundles = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let base = MetadataChunk {
            frames: vec![frame(0, 0, 10), frame(0, 10, 10), frame(1, 0, 10)],
            files: vec![
                file("a", 1, &[(0, 0)]),
                file("b", 2, &[(1, 0)]),
                file("c", 3, &[(2, 0)]),
                file("d", 4, &[(0, 10)]),
            ],
            gitlinks: vec![b"sub".to_vec()],
        };
        let table = MetadataChunk {
            frames: vec![frame(0, 0, 10), frame(1, 0, 10)],
            files: vec![
                file("a", 5, &[(1, 0)]),
                file("c", 3, &[(0, 0)]),
                file("d", 4, &[(1, 10)]),
                file("e", 6, &[(1, 20)]),
            ],
            gitlinks: vec![],
        };
        let map = frame_map(
            &base.frames,
            &bundles(&["A", "B"]),
            &table.frames,
            &bundles(&["B", "C"]),
        )
        .unwrap();
        assert_eq!(map, [0, 0, 1]);
        let delta = MetadataDelta::between(&base, map, &table).unwrap();
        let paths = |files: &[FileEntry]| files.iter().map(|f| f.path.clone()).collect::<Vec<_>>();
        assert_eq!(
            paths(&delta.files),
            [b"a".to_vec(), b"d".to_vec(), b"e".to_vec()]
        );
        assert_eq!(delta.removed, [b"b".to_vec()]);
        let stored = delta.encode_stored().unwrap();
        let delta = MetadataDelta::decode_stored(&stored).unwrap();
        assert_eq!(delta.apply(&base).unwrap(), table);

        // A kept entry on a frame the delta drops is an error, not a file
        // read from the wrong bytes.
        let mut wrong = delta.clone();
        wrong.base_frames[2] = 0;
        assert!(wrong.apply(&base).is_err());
        let mut wrong = delta.clone();
        wrong.removed.push(b"zz".to_vec());
        assert!(wrong.apply(&base).is_err());
        assert!(delta.apply(&table).is_err(), "applied to another base");
    }

    /// A Head table has no frames: the delta is its changed entries.
    #[test]
    fn delta_of_a_table_without_frames() {
        let base = MetadataChunk {
            frames: vec![],
            files: vec![file("x", 1, &[]), file("y", 2, &[])],
            gitlinks: vec![],
        };
        let table = MetadataChunk {
            frames: vec![],
            files: vec![file("x", 1, &[]), file("y", 3, &[]), file("z", 4, &[])],
            gitlinks: vec![],
        };
        let delta = MetadataDelta::between(&base, vec![], &table).unwrap();
        assert_eq!(delta.files.len(), 2);
        assert_eq!(delta.apply(&base).unwrap(), table);
    }

    #[test]
    fn frame_maps_compose() {
        assert_eq!(compose_frame_maps(&[2, 0, 1], &[0, 3]).unwrap(), [3, 0, 0]);
        assert!(compose_frame_maps(&[3], &[1]).is_err());
    }

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

        let stored = manifest.encode_stored().unwrap();
        assert!(stored.starts_with(&ZSTD_MAGIC));
        assert_eq!(
            MetadataChunk::decode_and_validate(&stored).unwrap(),
            manifest
        );
        let mut corrupt = stored.clone();
        corrupt.truncate(ZSTD_MAGIC.len() + 2);
        assert!(MetadataChunk::decode_and_validate(&corrupt).is_err());

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
