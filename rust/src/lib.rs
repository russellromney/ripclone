#![allow(
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::manual_checked_ops,
    clippy::suspicious_open_options,
    dead_code,
    deprecated
)]

pub mod archive;
pub mod bench;
pub mod blob_pack;
pub mod cas;
pub mod client;
pub mod clonepack;
pub mod extract;
pub mod fusefs;
pub mod git;
pub mod manifest;
pub mod metrics;
pub mod mode;
pub mod oidc;
pub mod overlay;
pub mod pack;
pub mod rcgit;
pub mod ref_store;
pub mod retention;
pub mod server;
pub mod sidecar;
pub mod snapshot;
pub mod storage;
pub mod validation;
pub mod worktree_writer;

use anyhow::Result;

/// Split a repo string "owner/name" into its parts.
pub fn parse_repo(repo: &str) -> Result<(&str, &str)> {
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        anyhow::bail!("repo must be owner/name, got: {}", repo);
    }
    Ok((parts[0], parts[1]))
}

/// One editable-clone pack and its idx, by content hash. Ordered to match the
/// `packs` list in the clonepack manifest so the ref endpoint can sign each
/// without re-decoding the manifest.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PackArtifact {
    pub pack: String,
    pub idx: String,
}

/// A pack + idx with their byte lengths. Used for LSM sealed levels, where the
/// lengths must be remembered (the bytes have been evicted from local CAS) so a
/// later sync can reference them in the manifest without re-reading them.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SizedPack {
    pub pack: String,
    pub pack_len: u64,
    pub idx: String,
    pub idx_len: u64,
}

/// One immutable, content-addressed history level in the LSM build: the deltified
/// packs for the commit range `(<previous level tip>, tip_commit]`. Sealed once
/// and thereafter referenced by hash; never rebuilt.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct HistoryLevel {
    pub tip_commit: String,
    pub packs: Vec<SizedPack>,
}

/// One content-defined archive frame from the last build, for incremental reuse:
/// `raw_hash` is the hash of the frame's raw (uncompressed) bytes — the reuse key
/// — and `chunk_hash` is the content-addressed compressed chunk. On a re-sync, a
/// frame whose raw bytes are unchanged reuses the prior compressed chunk: no
/// recompression, no re-upload.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ArchiveFrame {
    pub raw_hash: String,
    pub chunk_hash: String,
    pub compressed_len: u64,
    pub raw_len: u64,
}

/// One bucket of the HEAD-closure (depth=1) packs. Objects are partitioned into
/// fixed buckets by oid prefix, so a re-sync only rebuilds the buckets whose
/// object set changed and reuses the rest by hash (`git pack-objects` is
/// deterministic for a fixed oid list, so an unchanged bucket reproduces the
/// exact same pack). `oidset_hash` is the hash of the bucket's sorted oid list —
/// the reuse key. Undeltified packs compress each object independently, so this
/// bucketing has no compression cost.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct HeadBucket {
    pub oidset_hash: String,
    pub pack: SizedPack,
}

/// Artifact hashes for one clonepack variant (e.g. shallow depth=1 or full).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ClonepackArtifacts {
    pub manifest: String,
    pub metadata_chunk: String,
    pub skeleton_pack: String,
    pub skeleton_idx: String,
    pub prebuilt_index: String,
    /// CAS hash of the pre-built multi-pack-index over this variant's packs.
    /// Empty for older refs (client falls back to building the MIDX itself).
    #[serde(default)]
    pub midx: String,
    /// CAS hash of the concatenated idx bundle for this variant's packs. Empty
    /// for older refs (client falls back to fetching each idx individually).
    #[serde(default)]
    pub idx_bundle: String,
    /// The commit this variant's clonepack is built for. May differ from
    /// `RefInfo.commit` during two-phase publish (depth=0 briefly serves the
    /// previous commit while the new full history builds). Empty = same as
    /// `RefInfo.commit`.
    #[serde(default)]
    pub commit: String,
}

/// Artifact hashes returned by the server for a single ref.
///
/// Every artifact is stored in the CAS and can be fetched by its hash from
/// `/v1/artifacts/{hash}` (or the `/v1/packs/{hash}` legacy endpoint).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RefInfo {
    pub commit: String,
    pub parent_commit: Option<String>,
    pub default_branch: String,
    pub skeleton_pack: String,
    pub skeleton_idx: String,
    pub head_blobs_pack: String,
    pub head_blobs_idx: String,
    /// Content-addressed chunks of the head-blobs pack. The full pack is the
    /// concatenation of these chunks in order. New builds split the pack so the
    /// client can fetch it in parallel.
    #[serde(default)]
    pub head_blobs_chunks: Vec<String>,
    /// Editable-clone packs (pack + idx hashes), ordered to match the manifest's
    /// `packs` list. Kept here so the ref endpoint can sign each pack/idx URL
    /// without decoding the manifest, and for retention protection.
    #[serde(default)]
    pub packs: Vec<PackArtifact>,
    pub prebuilt_index: String,
    pub archive: String,
    pub manifest: String,
    /// Optional full-history pack (empty when not built).
    pub full_pack: String,
    /// Clonepack manifest hash (protobuf). Archive chunks are referenced inside it.
    /// Kept for backward compatibility; use `full_clonepack.manifest`.
    #[serde(default)]
    pub clonepack_manifest: String,
    /// Metadata chunk hash (protobuf). Kept at the top level so the ref endpoint
    /// can hand out a signed URL for it without re-decoding the manifest.
    /// Kept for backward compatibility; use `full_clonepack.metadata_chunk`.
    #[serde(default)]
    pub metadata_chunk: String,
    /// Archive chunk hashes referenced by the clonepack manifest. Kept for
    /// retention protection and debugging.
    #[serde(default)]
    pub archive_chunks: Vec<String>,
    /// Full-history clonepack (all reachable commits/trees).
    #[serde(default)]
    pub full_clonepack: ClonepackArtifacts,
    /// Shallow clonepack (single commit + HEAD trees). Matches `git clone --depth=1`.
    #[serde(default)]
    pub shallow_clonepack: ClonepackArtifacts,
    /// LSM sealed history levels (oldest first). Empty unless the LSM build is
    /// enabled. Each level is immutable and content-addressed; a sync only builds
    /// the tail past the last level's tip. See ROADMAP "LSM incremental history".
    #[serde(default)]
    pub history_levels: Vec<HistoryLevel>,
    /// HEAD-closure pack buckets from the last build, for incremental reuse: a
    /// re-sync rebuilds only the buckets whose object set changed. Populated by
    /// the two-phase build; empty otherwise. See [`HeadBucket`].
    #[serde(default)]
    pub head_buckets: Vec<HeadBucket>,
    /// Content-defined archive frames from the last build, for incremental reuse:
    /// a re-sync recompresses + re-uploads only the frames whose raw bytes
    /// changed. See [`ArchiveFrame`].
    #[serde(default)]
    pub archive_frames: Vec<ArchiveFrame>,
    /// Optional build status used by the async /v1/build worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_status: Option<String>,
    /// Unix timestamp (seconds) when this ref was last synced. Used by shared
    /// ref stores to avoid overwriting newer commits with older ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<u64>,
}
