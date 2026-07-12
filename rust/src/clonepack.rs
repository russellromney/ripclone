pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/ripclone.rs"));
}

pub use pb::{
    ChunkRef, ClonepackManifest, FileEntry, Fragment, FrameInfo, MetadataChunk, PackEntry,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use std::path::Path;

impl FileEntry {
    /// Total uncompressed size of the file across all fragments.
    pub fn total_len(&self) -> u64 {
        self.fragments.iter().map(|f| f.raw_len as u64).sum()
    }
}

/// Convert a hex hash string to raw bytes for protobuf `bytes` fields.
pub fn hash_from_hex(hex: &str) -> anyhow::Result<Vec<u8>> {
    hex::decode(hex).map_err(|e| anyhow::anyhow!("invalid hex hash: {}", e))
}

/// Convert raw hash bytes from protobuf back to a hex string.
pub fn hash_to_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Return every chunk reference contained in a clonepack manifest. This is the
/// single shared definition used by status reporting, remote GC, and local
/// retention so a new manifest field can never be added in one place and
/// forgotten in another.
pub fn manifest_chunk_refs(manifest: &ClonepackManifest) -> Vec<&ChunkRef> {
    let mut refs = Vec::new();
    if let Some(ref meta) = manifest.metadata_chunk {
        refs.push(meta);
    }
    refs.extend(&manifest.archive_chunks);
    refs.extend(&manifest.head_blobs_chunks);
    if let Some(ref idx) = manifest.head_blobs_idx {
        refs.push(idx);
    }
    for pack in &manifest.packs {
        if let Some(ref pack_chunk) = pack.pack {
            refs.push(pack_chunk);
        }
        if let Some(ref idx_chunk) = pack.idx {
            refs.push(idx_chunk);
        }
    }
    if let Some(ref midx) = manifest.midx {
        refs.push(midx);
    }
    if let Some(ref idx_bundle) = manifest.idx_bundle {
        refs.push(idx_bundle);
    }
    refs
}

/// Return the idx bytes for one manifest pack, either from the shared idx bundle
/// or from the caller's separately fetched idx object. This is shared by the
/// client git-dir reconstruction and server-side mirror seeding so both validate
/// bundle slices identically.
pub fn manifest_pack_idx_bytes(
    entry: &PackEntry,
    index: usize,
    idx_bundle: Option<&Bytes>,
    fetched_idx: Option<Bytes>,
) -> Result<Bytes> {
    if let Some(bundle) = idx_bundle {
        let idx_ref = entry
            .idx
            .as_ref()
            .with_context(|| format!("pack {index} missing idx ref"))?;
        let off = entry.idx_bundle_offset as usize;
        let end = off
            .checked_add(idx_ref.len as usize)
            .context("idx bundle offset overflow")?;
        if bundle.get(off..end).is_none() {
            anyhow::bail!("idx {index} slice out of bundle range");
        }
        let slice = bundle.slice(off..end);
        let want = hash_to_hex(&idx_ref.hash);
        let got = crate::cas::hash(&slice);
        if got != want {
            anyhow::bail!("idx {index} bundle slice hash mismatch: expected {want}, got {got}");
        }
        Ok(slice)
    } else {
        let idx_ref = entry
            .idx
            .as_ref()
            .with_context(|| format!("pack {index} missing idx ref"))?;
        let idx_bytes =
            fetched_idx.with_context(|| format!("pack {index} missing fetched idx bytes"))?;
        let want = hash_to_hex(&idx_ref.hash);
        if idx_bytes.len() as u64 != idx_ref.len {
            anyhow::bail!(
                "idx {index} size mismatch: expected {}, got {}",
                idx_ref.len,
                idx_bytes.len()
            );
        }
        let got = crate::cas::hash(&idx_bytes);
        if got != want {
            anyhow::bail!("idx {index} hash mismatch: expected {want}, got {got}");
        }
        Ok(idx_bytes)
    }
}

/// Install manifest pack/idx bytes into a git objects/pack directory.
///
/// Git pack file names are derived from the pack trailer hash, matching the
/// existing client reconstruction path and letting a later `git fetch` negotiate
/// against the seeded object database as an ordinary local mirror.
pub fn install_manifest_pack_bytes<I>(pack_dir: &Path, packs: I) -> Result<u64>
where
    I: IntoIterator<Item = (Bytes, Bytes)>,
{
    std::fs::create_dir_all(pack_dir)
        .with_context(|| format!("create pack dir {}", pack_dir.display()))?;

    let mut total = 0u64;
    for (pack_bytes, idx_bytes) in packs {
        if pack_bytes.len() < 20 {
            anyhow::bail!("pack too short ({} bytes)", pack_bytes.len());
        }
        let name = hex::encode(&pack_bytes[pack_bytes.len() - 20..]);
        std::fs::write(pack_dir.join(format!("pack-{}.pack", name)), &pack_bytes)
            .with_context(|| format!("write pack {}", name))?;
        std::fs::write(pack_dir.join(format!("pack-{}.idx", name)), &idx_bytes)
            .with_context(|| format!("write idx {}", name))?;
        total += (pack_bytes.len() + idx_bytes.len()) as u64;
    }
    Ok(total)
}

pub(crate) fn install_manifest_pack_bytes_cancelled<I>(
    pack_dir: &Path,
    packs: I,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<u64>
where
    I: IntoIterator<Item = (Bytes, Bytes)>,
{
    std::fs::create_dir_all(pack_dir)
        .with_context(|| format!("create pack dir {}", pack_dir.display()))?;
    let mut total = 0u64;
    for (pack_bytes, idx_bytes) in packs {
        if pack_bytes.len() < 20 {
            anyhow::bail!("pack too short ({} bytes)", pack_bytes.len());
        }
        let name = hex::encode(&pack_bytes[pack_bytes.len() - 20..]);
        for (suffix, bytes) in [("pack", &pack_bytes), ("idx", &idx_bytes)] {
            let mut output = std::fs::File::create(pack_dir.join(format!("pack-{name}.{suffix}")))?;
            for chunk in bytes.chunks(1024 * 1024) {
                if cancelled.is_cancelled() {
                    anyhow::bail!("pack installation cancelled");
                }
                std::io::Write::write_all(&mut output, chunk)?;
            }
        }
        total = total
            .checked_add((pack_bytes.len() + idx_bytes.len()) as u64)
            .context("pack installation byte count overflow")?;
    }
    Ok(total)
}

/// Collect the distinct clonepack manifest hashes referenced by a `RefInfo`.
/// Shared by `/status` and GC reachability so both agree on what is reachable.
pub fn collect_manifest_hashes(info: &crate::RefInfo) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for hash in [
        &info.full_clonepack.manifest,
        &info.shallow_clonepack.manifest,
        &info.clonepack_manifest,
    ] {
        if !hash.is_empty() && seen.insert(hash.to_string()) {
            out.push(hash.to_string());
        }
    }
    out
}

/// Compute the compressed byte length of each archive chunk from the frame table.
pub fn archive_chunk_lengths(metadata: &MetadataChunk) -> Vec<u64> {
    let mut lengths = Vec::new();
    for frame in &metadata.frames {
        let idx = frame.chunk_index as usize;
        let end = frame.chunk_offset + frame.compressed_len as u64;
        if idx >= lengths.len() {
            lengths.resize(idx + 1, 0);
        }
        if end > lengths[idx] {
            lengths[idx] = end;
        }
    }
    lengths
}
