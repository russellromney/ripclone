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

use anyhow::Result;

/// Split a repo string "owner/name" into its parts.
pub fn parse_repo(repo: &str) -> Result<(&str, &str)> {
    let parts: Vec<&str> = repo.splitn(2, '/').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        anyhow::bail!("repo must be owner/name, got: {}", repo);
    }
    Ok((parts[0], parts[1]))
}

/// Artifact hashes for one clonepack variant (e.g. shallow depth=1 or full).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ClonepackArtifacts {
    pub manifest: String,
    pub metadata_chunk: String,
    pub skeleton_pack: String,
    pub skeleton_idx: String,
    pub prebuilt_index: String,
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
    /// Optional build status used by the async /v1/build worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_status: Option<String>,
    /// Unix timestamp (seconds) when this ref was last synced. Used by shared
    /// ref stores to avoid overwriting newer commits with older ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<u64>,
}
