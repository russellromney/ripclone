#![allow(
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::manual_checked_ops,
    clippy::suspicious_open_options
)]

//! Library support for the `ripclone` binaries.
//!
//! The stable surface is intentionally small while the crate is pre-1.0:
//! client configuration, manifest types, storage backends, and server entry
//! points used by the bundled binaries. Modules marked `doc(hidden)` are public
//! for in-repo binaries and integration tests, not a stability promise.

#[cfg(test)]
pub(crate) static ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

// mimalloc as the global allocator on musl targets. The Linux release binaries
// are statically linked against musl (so one binary runs on any Linux, Alpine
// included), but musl's default allocator is markedly slower than glibc's under
// the concurrent, allocation-heavy pack build / archive extract paths — and
// performance is the product. mimalloc closes that gap so a static musl binary
// keeps glibc-class throughput. Scoped to musl only: glibc and macOS already
// ship capable allocators, so those builds are left untouched. Defined here in
// the library crate so every binary that links it (ripclone, ripclone-server,
// ripclone-worker) picks it up. NOTE: this cfg path only
// compiles on a musl build — the host `cargo check` never sees it. It is built
// and run by the `musl` CI job (scripts/musl-smoke.sh).
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// `#[global_allocator]` compiles fine even if mimalloc never actually serves an
// allocation (a stale attribute, a second allocator winning, a link-order
// surprise). Ask mimalloc itself whether the pointer Rust just handed us came
// out of its heap. Runs under the `musl` CI job.
#[cfg(all(test, target_env = "musl"))]
mod musl_global_allocator {
    unsafe extern "C" {
        /// mimalloc's own predicate: true iff `p` lies in a region mimalloc owns.
        fn mi_is_in_heap_region(p: *const core::ffi::c_void) -> bool;
    }

    #[test]
    fn rust_allocations_are_served_by_mimalloc() {
        let boxed = Box::new(42_u64);
        assert!(
            unsafe { mi_is_in_heap_region(std::ptr::from_ref(&*boxed).cast()) },
            "small Box allocation did not come from mimalloc's heap"
        );

        // Large allocation: a different mimalloc path (and the one the pack /
        // archive hot loops hit), still mimalloc-owned.
        let big = vec![0_u8; 8 << 20];
        assert!(
            unsafe { mi_is_in_heap_region(big.as_ptr().cast()) },
            "8 MiB Vec allocation did not come from mimalloc's heap"
        );
    }
}

/// Wire-protocol version negotiated between the CLI and the server. Bump this
/// only on a breaking change to the client/server protocol — independent of the
/// crate version, so the two binaries can be released on their own cadence as
/// long as their protocol versions match. Surfaced at `/v1/version` and by
/// `ripclone version`.
pub const PROTOCOL_VERSION: u32 = 2;

#[doc(hidden)]
pub mod api_job_queue;
pub mod api_ref_store;
pub mod archive;
#[doc(hidden)]
pub mod auth;
pub mod backends;
#[doc(hidden)]
pub mod bench;
pub mod cas;
pub mod client;
#[doc(hidden)]
pub mod clone_metrics;
pub mod clonepack;
pub mod config;
pub mod control;
#[doc(hidden)]
pub mod extract;
#[doc(hidden)]
pub mod fsutil;
#[doc(hidden)]
pub mod git;
#[doc(hidden)]
pub mod gix_util;
#[doc(hidden)]
pub mod job_token;
pub mod manifest;
#[doc(hidden)]
pub mod meta;
#[doc(hidden)]
pub mod metrics;
pub mod mode;
#[doc(hidden)]
pub mod oidc;
#[doc(hidden)]
pub mod overlay;
pub mod pack;
pub mod perf;
pub mod provider;
pub mod provider_config;
#[doc(hidden)]
pub mod queue;
pub mod ref_store;
pub mod repo_config;
#[doc(hidden)]
pub mod retention;
#[doc(hidden)]
pub mod secure_file;
pub mod server;
#[cfg(target_os = "linux")]
#[doc(hidden)]
pub mod statx_compat;
pub mod storage;

#[cfg(test)]
pub mod test_fixture;
#[doc(hidden)]
pub mod validation;
#[doc(hidden)]
pub mod webhook;
#[doc(hidden)]
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
/// lengths must be remembered (the bytes may be absent from local CAS) so a
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

/// Artifact hashes for one clonepack variant (e.g. shallow depth=1 or full).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClonepackArtifacts {
    pub manifest: String,
    pub metadata_chunk: String,
    pub skeleton_pack: String,
    pub skeleton_idx: String,
    pub prebuilt_index: String,
    /// CAS hash of the pre-built multi-pack-index over this variant's packs.
    pub midx: String,
    /// CAS hash of the required concatenated idx bundle for this variant's packs.
    pub idx_bundle: String,
    /// The commit this variant's clonepack is built for. Complete artifacts
    /// always carry this identity explicitly.
    pub commit: String,
}

/// Published Head artifacts for one exact commit.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct HeadResult {
    pub clonepack: ClonepackArtifacts,
    pub parent_commit: Option<String>,
    #[serde(default)]
    pub packs: Vec<PackArtifact>,
    #[serde(default)]
    pub base_commit: String,
    #[serde(default)]
    pub base_packs: Vec<SizedPack>,
}

/// Published Full artifacts for one exact commit.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FullResult {
    pub clonepack: ClonepackArtifacts,
    #[serde(default)]
    pub packs: Vec<PackArtifact>,
    #[serde(default)]
    pub history_levels: Vec<HistoryLevel>,
}

/// Published Files artifacts for one exact commit.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FilesResult {
    pub clonepack: ClonepackArtifacts,
    #[serde(default)]
    pub archive_chunks: Vec<String>,
    #[serde(default)]
    pub archive_frames: Vec<ArchiveFrame>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExactResultKind {
    Head,
    Full,
    Files,
}

/// Return whether one stored output is structurally usable for `commit`.
///
/// Files may validly contain zero archive chunks (for an empty committed tree),
/// and MIDX is optional for every result. The remaining hashes are required by
/// the corresponding clone/install or follow-on build path.
pub(crate) fn exact_output_artifacts_ready(
    commit: &str,
    result: ExactResultKind,
    artifacts: &ClonepackArtifacts,
) -> bool {
    let valid_hash = |hash: &str| crate::cas::Cas::validate_artifact_id(hash).is_ok();
    if artifacts.commit != commit
        || !valid_hash(&artifacts.manifest)
        || !valid_hash(&artifacts.metadata_chunk)
    {
        return false;
    }

    match result {
        ExactResultKind::Head | ExactResultKind::Full => {
            valid_hash(&artifacts.skeleton_pack)
                && valid_hash(&artifacts.skeleton_idx)
                && valid_hash(&artifacts.prebuilt_index)
                && valid_hash(&artifacts.idx_bundle)
        }
        ExactResultKind::Files => true,
    }
}

pub(crate) fn exact_output_ready(info: &RefInfo, result: ExactResultKind, commit: &str) -> bool {
    if info.commit != commit {
        return false;
    }
    let artifacts = match result {
        ExactResultKind::Head => info.head.as_ref().map(|output| &output.clonepack),
        ExactResultKind::Full => info.full.as_ref().map(|output| &output.clonepack),
        ExactResultKind::Files => info.files.as_ref().map(|output| &output.clonepack),
    };
    artifacts.is_some_and(|artifacts| exact_output_artifacts_ready(commit, result, artifacts))
}

pub(crate) fn exact_result_complete(info: &RefInfo, commit: &str) -> bool {
    exact_output_ready(info, ExactResultKind::Head, commit)
        && exact_output_ready(info, ExactResultKind::Full, commit)
        && exact_output_ready(info, ExactResultKind::Files, commit)
}

impl std::fmt::Display for ExactResultKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Head => "head",
            Self::Full => "full",
            Self::Files => "files",
        })
    }
}

/// Artifact hashes returned by the server for a single ref.
///
/// Every artifact is stored in the CAS and can be fetched by its hash from
/// `/v1/artifacts/{hash}`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RefInfo {
    pub commit: String,
    pub head: Option<HeadResult>,
    pub full: Option<FullResult>,
    pub files: Option<FilesResult>,
}
