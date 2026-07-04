//! Property and fuzz tests for the manifest → working-tree extraction path.
//!
//! The core invariant: given any `MetadataChunk` manifest and archive bytes, the
//! extractor must either return `Err` or produce a working tree whose file set
//! exactly matches the manifest's declared entries. It must never silently write
//! a short or partial tree, and it must never panic on malformed input.

use std::collections::BTreeSet;
use std::fs::File;
use std::path::{Path, PathBuf};

use proptest::prelude::*;
use prost::Message;
use ripclone::clonepack::ClonepackManifest;
use ripclone::extract::{ExtractStats, extract_archive_with_chunk_fetcher};
use ripclone::manifest::{FileEntry, Fragment, FrameInfo, MetadataChunk};
use sha1::{Digest, Sha1};

const MODE_REGULAR: u32 = 0o100644;
const MODE_EXEC: u32 = 0o100755;
const MODE_SYMLINK: u32 = 0o120000;

/// A planned working-tree file: where it lands and what it should contain.
#[derive(Debug, Clone)]
struct PlannedFile {
    path: Vec<u8>,
    mode: u32,
    content: Vec<u8>,
}

/// A fully coherent manifest plus the archive chunk bytes it decodes against.
#[derive(Debug, Clone)]
struct Plan {
    manifest: MetadataChunk,
    archive_chunks: Vec<Vec<u8>>,
    files: Vec<PlannedFile>,
}

fn sha1_bytes(data: &[u8]) -> Vec<u8> {
    Sha1::digest(data).to_vec()
}

/// Candidate paths that never collide on a case-insensitive filesystem and never
/// nest a file inside another file's path (no dir/file conflicts).
fn candidate_paths() -> Vec<&'static [u8]> {
    vec![
        b"f0.txt".as_slice(),
        b"f1.txt".as_slice(),
        b"f2.bin".as_slice(),
        b"f3".as_slice(),
        b"sub/g0.txt".as_slice(),
        b"sub/g1.dat".as_slice(),
        b"deep/a/h0.txt".as_slice(),
        b"deep/a/h1.txt".as_slice(),
        b"another/i0".as_slice(),
    ]
}

/// Strategy for a single planned file: a path index, a mode, and content.
fn planned_file_strategy() -> impl Strategy<Value = (usize, u32, Vec<u8>)> {
    let mode = prop_oneof![
        6 => Just(MODE_REGULAR),
        2 => Just(MODE_EXEC),
        2 => Just(MODE_SYMLINK),
    ];
    // Content: anything from empty to a few hundred bytes of arbitrary data.
    let content = prop::collection::vec(any::<u8>(), 0..300);
    (0..candidate_paths().len(), mode, content)
}

prop_compose! {
    /// Build a coherent plan: a set of unique-path files distributed across
    /// frames (some files split into multiple fragments) and archive chunks.
    fn plan_strategy()(
        raw_files in prop::collection::vec(planned_file_strategy(), 0..7),
        frame_count in 1usize..4,
        split_seeds in prop::collection::vec(0usize..4, 0..7),
        chunk_count in 1usize..3,
    ) -> Plan {
        build_plan(raw_files, frame_count, split_seeds, chunk_count)
    }
}

/// Assemble a manifest + archive from raw file specs. Each file's content is
/// partitioned into one or more fragments, each appended to a chosen frame's raw
/// buffer; frames are then compressed and laid out into archive chunks.
fn build_plan(
    raw_files: Vec<(usize, u32, Vec<u8>)>,
    frame_count: usize,
    split_seeds: Vec<usize>,
    chunk_count: usize,
) -> Plan {
    let paths = candidate_paths();
    let frame_count = frame_count.max(1);
    let chunk_count = chunk_count.max(1);

    // Deduplicate by path (case-insensitively, since the test may run on a
    // case-insensitive filesystem). Symlinks need non-empty content, so force a
    // byte in for empty symlink targets.
    let mut used_paths: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut files: Vec<PlannedFile> = Vec::new();
    for (path_idx, mode, mut content) in raw_files.into_iter() {
        let path = paths[path_idx % paths.len()].to_vec();
        let lower = path.to_ascii_lowercase();
        if !used_paths.insert(lower) {
            continue;
        }
        // Symlink targets are stored as raw bytes; they must be non-empty and
        // contain no NUL.
        if mode == MODE_SYMLINK {
            content.retain(|&b| b != 0);
            if content.is_empty() {
                content = vec![b'x'];
            }
        }
        files.push(PlannedFile {
            path,
            mode,
            content,
        });
    }

    // Build per-frame raw buffers and per-file fragment lists.
    let mut frame_raw: Vec<Vec<u8>> = vec![Vec::new(); frame_count];
    let mut entries: Vec<FileEntry> = Vec::new();
    for (fi, file) in files.iter().enumerate() {
        let split_into = if file.content.len() >= 2 {
            (split_seeds.get(fi).copied().unwrap_or(0) % 3) + 1
        } else {
            1
        };
        let parts = partition(&file.content, split_into);
        let mut fragments: Vec<Fragment> = Vec::new();
        for (pi, part) in parts.iter().enumerate() {
            // Spread fragments across frames deterministically.
            let frame_index = (fi + pi) % frame_count;
            let buf = &mut frame_raw[frame_index];
            let frame_offset = buf.len() as u32;
            buf.extend_from_slice(part);
            fragments.push(Fragment {
                frame_index: frame_index as u32,
                frame_offset,
                raw_len: part.len() as u32,
            });
        }
        entries.push(FileEntry {
            path: file.path.clone(),
            mode: file.mode,
            blob_sha1: sha1_bytes(&file.content),
            fragments,
        });
    }

    // Compress frames and lay them out into archive chunks. Frames are assigned
    // to chunks in contiguous blocks so the frame table stays chunk-ordered.
    let mut frames: Vec<FrameInfo> = Vec::with_capacity(frame_count);
    let mut archive_chunks: Vec<Vec<u8>> = vec![Vec::new(); chunk_count];
    for (frame_index, raw) in frame_raw.iter().enumerate() {
        let chunk_index = (frame_index * chunk_count) / frame_count;
        let chunk_index = chunk_index.min(chunk_count - 1);
        let compressed: Vec<u8> = if raw.is_empty() {
            Vec::new()
        } else {
            zstd::encode_all(raw.as_slice(), 1).expect("zstd encode")
        };
        let chunk = &mut archive_chunks[chunk_index];
        let chunk_offset = chunk.len() as u64;
        chunk.extend_from_slice(&compressed);
        frames.push(FrameInfo {
            chunk_index: chunk_index as u32,
            chunk_offset,
            compressed_len: compressed.len() as u32,
            raw_len: raw.len() as u32,
        });
    }

    let mut manifest = MetadataChunk::new();
    manifest.frames = frames;
    manifest.files = entries;

    Plan {
        manifest,
        archive_chunks,
        files,
    }
}

/// Split `data` into `parts` contiguous slices (the last absorbs the remainder).
fn partition(data: &[u8], parts: usize) -> Vec<Vec<u8>> {
    if parts <= 1 || data.len() < 2 {
        return vec![data.to_vec()];
    }
    let chunk = data.len() / parts;
    let mut out = Vec::with_capacity(parts);
    let mut start = 0;
    for p in 0..parts {
        let end = if p + 1 == parts {
            data.len()
        } else {
            start + chunk
        };
        out.push(data[start..end].to_vec());
        start = end;
    }
    out
}

/// Extract a manifest + archive into a fresh temp dir. Returns the result along
/// with the `TempDir` guard (kept alive by the caller so the tree can be
/// inspected) and the target directory inside it.
fn extract_plan(
    manifest: &MetadataChunk,
    archive_chunks: &[Vec<u8>],
) -> (anyhow::Result<ExtractStats>, tempfile::TempDir, PathBuf) {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let target = tmp.path().join("out");
    std::fs::create_dir(&target).expect("create target");
    let manifest_path = target.join("manifest.pb");
    {
        let mut f = File::create(&manifest_path).expect("create manifest");
        manifest.write(&mut f).expect("write manifest");
    }
    let chunks = archive_chunks.to_vec();
    let result = extract_archive_with_chunk_fetcher(
        &manifest_path,
        Some(&target),
        None,
        u64::MAX,
        move |chunk| {
            chunks
                .get(chunk.chunk_index)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing chunk {}", chunk.chunk_index))
        },
    );
    (result, tmp, target)
}

/// Recursively collect every file/symlink path under `root`, relative to it,
/// excluding the manifest file we wrote in alongside the extracted tree.
fn collect_tree(root: &Path) -> BTreeSet<Vec<u8>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<Vec<u8>>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let meta = match std::fs::symlink_metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path.strip_prefix(root).unwrap();
                out.insert(path_to_bytes(rel));
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(root, root, &mut out);
    out.remove(b"manifest.pb".as_slice());
    out
}

#[cfg(unix)]
fn path_to_bytes(p: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    p.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_to_bytes(p: &Path) -> Vec<u8> {
    p.to_string_lossy().into_owned().into_bytes()
}

/// Assert the on-disk tree matches the declared files exactly, in set and
/// content.
fn assert_tree_matches(dir: &Path, files: &[PlannedFile]) {
    let on_disk = collect_tree(dir);
    let declared: BTreeSet<Vec<u8>> = files.iter().map(|f| f.path.clone()).collect();
    assert_eq!(
        on_disk, declared,
        "on-disk file set must exactly match declared manifest entries"
    );
    for file in files {
        let full = dir.join(bytes_to_path(&file.path));
        if file.mode == MODE_SYMLINK {
            let meta = std::fs::symlink_metadata(&full).expect("symlink stat");
            assert!(
                meta.file_type().is_symlink(),
                "{:?} must be a symlink",
                full
            );
            let target = std::fs::read_link(&full).expect("read_link");
            assert_eq!(path_to_bytes(&target), file.content, "symlink target bytes");
        } else {
            let got = std::fs::read(&full).expect("read file");
            assert_eq!(got, file.content, "content for {:?}", full);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perm = std::fs::metadata(&full).unwrap().permissions().mode();
                if file.mode == MODE_EXEC {
                    assert_eq!(perm & 0o111, 0o111, "exec bit for {:?}", full);
                } else {
                    assert_eq!(perm & 0o111, 0, "no exec bit for {:?}", full);
                }
            }
        }
    }
}

#[cfg(unix)]
fn bytes_to_path(b: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStrExt;
    PathBuf::from(std::ffi::OsStr::from_bytes(b))
}

#[cfg(not(unix))]
fn bytes_to_path(b: &[u8]) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(b).into_owned())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// A coherent manifest must extract to a tree that exactly matches its
    /// declared entries — every file present, correct content, nothing extra.
    #[test]
    fn coherent_manifest_yields_complete_tree(plan in plan_strategy()) {
        let (result, _tmp, dir) = extract_plan(&plan.manifest, &plan.archive_chunks);
        let stats = result.expect("coherent manifest must extract cleanly");
        prop_assert_eq!(stats.files, plan.files.len());
        assert_tree_matches(&dir, &plan.files);
    }

    /// Corrupting the archive bytes must never yield a silently partial tree:
    /// extraction either errors, or produces exactly the declared tree with
    /// correct content (a corruption that happens to be benign).
    #[test]
    fn corrupted_archive_errs_or_matches(
        plan in plan_strategy(),
        flip_chunk in any::<usize>(),
        flip_pos in any::<usize>(),
        flip_xor in 1u8..=255,
        truncate in any::<bool>(),
    ) {
        prop_assume!(!plan.archive_chunks.is_empty());
        let mut chunks = plan.archive_chunks.clone();
        let ci = flip_chunk % chunks.len();
        if !chunks[ci].is_empty() {
            if truncate {
                let new_len = flip_pos % chunks[ci].len();
                chunks[ci].truncate(new_len);
            } else {
                let pos = flip_pos % chunks[ci].len();
                chunks[ci][pos] ^= flip_xor;
            }
        } else {
            // Nothing to corrupt; treat as a no-op pass.
            chunks[ci].push(flip_xor);
        }
        let (result, _tmp, dir) = extract_plan(&plan.manifest, &chunks);
        match result {
            Err(_) => {}
            Ok(stats) => {
                // If it claimed success, the tree must still be complete and
                // correct.
                prop_assert_eq!(stats.files, plan.files.len());
                assert_tree_matches(&dir, &plan.files);
            }
        }
    }

    /// Corrupting manifest geometry (offsets, lengths, sha1s) must never panic
    /// and never yield a silently partial tree.
    #[test]
    fn corrupted_manifest_errs_or_matches(
        plan in plan_strategy(),
        which in any::<usize>(),
        delta in any::<u32>(),
        // Flip a sha1 byte only sometimes, so geometry-only mutations also reach
        // the benign/Ok path instead of always tripping a sha1 mismatch.
        flip_sha1 in any::<bool>(),
    ) {
        let mut manifest = plan.manifest.clone();
        let mut mutated = false;
        if !manifest.frames.is_empty() {
            let f = which % manifest.frames.len();
            match which % 4 {
                0 => manifest.frames[f].raw_len = manifest.frames[f].raw_len.wrapping_add(delta),
                1 => manifest.frames[f].compressed_len =
                    manifest.frames[f].compressed_len.wrapping_add(delta),
                2 => manifest.frames[f].chunk_offset =
                    manifest.frames[f].chunk_offset.wrapping_add(delta as u64),
                _ => manifest.frames[f].chunk_index =
                    manifest.frames[f].chunk_index.wrapping_add(delta),
            }
            mutated = true;
        }
        if !manifest.files.is_empty() {
            let fi = (which / 4) % manifest.files.len();
            if !manifest.files[fi].fragments.is_empty() {
                let gi = which % manifest.files[fi].fragments.len();
                manifest.files[fi].fragments[gi].raw_len =
                    manifest.files[fi].fragments[gi].raw_len.wrapping_add(delta);
                mutated = true;
            }
            if flip_sha1 && !manifest.files[fi].blob_sha1.is_empty() {
                let bi = (delta as usize) % manifest.files[fi].blob_sha1.len();
                manifest.files[fi].blob_sha1[bi] ^= 0xff;
                mutated = true;
            }
        }
        prop_assume!(mutated);

        // Must not panic. Either an error, or — if the mutation was benign — a
        // tree that still matches exactly.
        let (result, _tmp, dir) = extract_plan(&manifest, &plan.archive_chunks);
        match result {
            Err(_) => {}
            Ok(stats) => {
                prop_assert_eq!(stats.files, plan.files.len());
                assert_tree_matches(&dir, &plan.files);
            }
        }
    }

    /// Raw arbitrary bytes fed to the protobuf decoders must never panic; they
    /// must cleanly return Ok or Err.
    #[test]
    fn raw_bytes_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        // MetadataChunk::read decodes then validates geometry.
        let _ = MetadataChunk::read(&mut bytes.as_slice());
        // Top-level manifest decode.
        if let Ok(m) = ClonepackManifest::decode(bytes.as_slice()) {
            // Re-encoding a decoded message must also not panic.
            let _ = m.encode_to_vec();
        }
        // Direct MetadataChunk decode without validation, then validate.
        if let Ok(m) = MetadataChunk::decode(bytes.as_slice()) {
            let _ = m.validate_geometry();
        }
    }
}

/// Hand-written regression: a multi-frame, multi-fragment, mixed-mode tree must
/// round-trip completely.
#[test]
fn explicit_mixed_tree_roundtrips() {
    let files = vec![
        (0usize, MODE_REGULAR, b"hello world".to_vec()),
        (1, MODE_EXEC, b"#!/bin/sh\necho hi\n".to_vec()),
        (4, MODE_SYMLINK, b"f0.txt".to_vec()),
        (5, MODE_REGULAR, Vec::new()), // empty file
        (6, MODE_REGULAR, vec![b'z'; 250]),
    ];
    let plan = build_plan(files, 3, vec![1, 2, 1, 1, 3], 2);
    let (result, _tmp, dir) = extract_plan(&plan.manifest, &plan.archive_chunks);
    let stats = result.unwrap();
    assert_eq!(stats.files, plan.files.len());
    assert_tree_matches(&dir, &plan.files);
}

/// Regression: a frame whose `chunk_offset` sits below its chunk group's start
/// makes the slice offset underflow. The extractor must return an error rather
/// than panicking (or hanging) on the underflow.
#[test]
fn frame_offset_below_chunk_start_errs() {
    let comp_a = zstd::encode_all(b"aaa".as_slice(), 1).unwrap();
    let comp_b = zstd::encode_all(b"bbb".as_slice(), 1).unwrap();
    let comp_c = zstd::encode_all(b"ccc".as_slice(), 1).unwrap();

    // Chunk 0 holds frame 0. Chunk 1 holds frames 1 and 2, but frame 2's offset
    // (0) is below frame 1's offset (50), which becomes the group's start.
    let mut chunk1 = vec![0u8; 50];
    chunk1.extend_from_slice(&comp_b);
    let c_start = chunk1.len();
    chunk1.extend_from_slice(&comp_c);

    let mut manifest = MetadataChunk::new();
    manifest.frames = vec![
        FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: comp_a.len() as u32,
            raw_len: 3,
        },
        FrameInfo {
            chunk_index: 1,
            chunk_offset: 50,
            compressed_len: comp_b.len() as u32,
            raw_len: 3,
        },
        FrameInfo {
            chunk_index: 1,
            chunk_offset: c_start as u64,
            compressed_len: comp_c.len() as u32,
            raw_len: 3,
        },
    ];
    manifest.files = vec![
        FileEntry {
            path: b"a.txt".to_vec(),
            mode: MODE_REGULAR,
            blob_sha1: sha1_bytes(b"aaa"),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 3,
            }],
        },
        FileEntry {
            path: b"c.txt".to_vec(),
            mode: MODE_REGULAR,
            blob_sha1: sha1_bytes(b"ccc"),
            fragments: vec![Fragment {
                frame_index: 2,
                frame_offset: 0,
                raw_len: 3,
            }],
        },
    ];

    // Corrupt frame 2's offset to sit below the chunk-1 group start (frame 1's
    // offset of 50), forcing the slice offset to underflow.
    manifest.frames[2].chunk_offset = 0;

    let (result, _tmp, _dir) = extract_plan(&manifest, &[comp_a, chunk1]);
    assert!(
        result.is_err(),
        "underflowing frame offset must error, not panic"
    );
}
