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

// mimalloc as the global allocator on musl targets. The Linux release binaries
// are statically linked against musl (so one binary runs on any Linux, Alpine
// included), but musl's default allocator is markedly slower than glibc's under
// the concurrent, allocation-heavy pack build / archive extract paths — and
// performance is the product. mimalloc closes that gap so a static musl binary
// keeps glibc-class throughput. Scoped to musl only: glibc and macOS already
// ship capable allocators, so those builds are left untouched. Defined here in
// the library crate so every binary that links it (ripclone, ripclone-server,
// ripclone-worker, git-remote-ripclone) picks it up. NOTE: this cfg path only
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
pub const PROTOCOL_VERSION: u32 = 1;

#[doc(hidden)]
pub mod api_job_queue;
pub mod api_ref_store;
pub mod archive;
#[doc(hidden)]
pub mod artifact_admission;
#[doc(hidden)]
pub mod artifact_builder;
#[doc(hidden)]
pub mod artifact_manifest;
#[doc(hidden)]
pub mod artifact_scheduler;
#[doc(hidden)]
pub mod artifact_scheduler_backend;
#[doc(hidden)]
pub mod artifact_scheduler_libsql;
pub mod artifact_scheduler_mysql;
#[doc(hidden)]
pub mod artifact_scheduler_postgres;
#[doc(hidden)]
pub mod auth;
pub mod backends;
#[doc(hidden)]
pub mod bench;
pub mod cas;
pub mod client;
#[doc(hidden)]
pub mod clone_metrics;
pub mod clone_plan;
pub mod clone_transport;
pub mod clonepack;
pub mod config;
/// Provider-agnostic compute dispatch (`RIPCLONE_DISPATCH=fly|exec|http|mock`).
///
/// The cloud webhook/cron and self-host escape hatches wake workers through
/// [`dispatch::ComputeProvider`]; nothing outside the module knows the platform.
#[doc(hidden)]
pub mod dispatch;
#[doc(hidden)]
pub mod extract;
#[doc(hidden)]
pub mod fsutil;
#[doc(hidden)]
pub mod git;
#[doc(hidden)]
pub mod git_source;
#[doc(hidden)]
pub mod git_source_registry;
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
pub mod pinned_bundle;
pub mod provider;
pub mod provider_config;
#[doc(hidden)]
pub mod queue;
pub mod ref_store;
pub mod remote_gc;
pub mod repo_config;
#[doc(hidden)]
pub mod retention;
#[doc(hidden)]
pub mod runtime_adapters;
#[doc(hidden)]
pub mod secure_file;
pub mod server;
#[doc(hidden)]
pub mod sidecar;
#[doc(hidden)]
pub mod snapshot;
#[doc(hidden)]
pub mod source_snapshot;
#[cfg(target_os = "linux")]
#[doc(hidden)]
pub mod statx_compat;
pub mod storage;
#[doc(hidden)]
pub mod sync_coordinator;
pub mod topup;

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
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
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
    /// The commit whose depth-1 closure the HEAD *base* packs cover. A re-sync
    /// packs only the objects new since this commit (`closure(HEAD) −
    /// closure(head_base_commit)`) into a fresh delta pack, so the base and the
    /// delta are disjoint by construction — no object is ever in two HEAD packs
    /// (which would double-materialize a worktree file). Empty before the first
    /// two-phase build. The background phase rebases (rebuilds the base at the
    /// current commit) once the cumulative delta grows past
    /// `RIPCLONE_HEAD_REBASE_BYTES`. See [`crate::pack::PackBuilder`].
    #[serde(default)]
    pub head_base_commit: String,
    /// The HEAD base packs (closure of `head_base_commit`), with lengths so a
    /// re-sync can reference these now-evicted packs in the clonepack manifest
    /// without re-reading their bytes. Carried unchanged across delta syncs;
    /// replaced wholesale on a rebase.
    #[serde(default)]
    pub head_base_packs: Vec<SizedPack>,
    /// Content-defined archive frames from the last build, for incremental reuse:
    /// a re-sync recompresses + re-uploads only the frames whose raw bytes
    /// changed. See [`ArchiveFrame`].
    #[serde(default)]
    pub archive_frames: Vec<ArchiveFrame>,
    /// Optional build status used by the async /v1/build worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_status: Option<String>,
    /// Wall-clock milliseconds for the most recent full build, populated once
    /// full-history/files artifacts finish and surfaced by `/status`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_ms: Option<u64>,
    /// Unix timestamp (seconds) when this ref was last synced. Legacy ordering
    /// signal, kept as a fallback for refs (or repos) without a `generation`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<u64>,
    /// Unix timestamp (seconds) when this ref was last considered "warm".
    /// The periodic warm-TTL sweep uses this (falling back to `synced_at`) to
    /// decide when a ref's clonepack artifacts have gone idle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_accessed_at: Option<u64>,
    /// When true, the warm-TTL sweep never evicts this ref's artifacts. An
    /// operator or external control plane may set this flag for repos that should
    /// stay warm; the server simply honors it.
    #[serde(default)]
    pub warm_pinned: bool,
    /// The commit's depth in git history (`git rev-list --count`). This is the
    /// primary ordering signal for "a newer sync never loses": recency follows
    /// the commit's place in history, not the builder's clock, so two builders
    /// with skewed clocks still order correctly. `None` on refs written before
    /// this field existed, where callers fall back to `synced_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
}
