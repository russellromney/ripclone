pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/ripclone.rs"));
}

pub use pb::{
    ChunkRef, ClonepackManifest, FileEntry, Fragment, FrameInfo, MetadataChunk, PackEntry,
};

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
