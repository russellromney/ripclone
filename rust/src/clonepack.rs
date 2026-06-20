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
