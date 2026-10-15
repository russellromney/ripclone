//! Build the Head, Full and Files results of a commit from the bare mirror
//! and upload their artifacts.
//!
//! Every disk, CAS and storage step runs on the blocking pool through
//! [`cancel::spawn_blocking`], so a build never stalls the API's async
//! workers, and the build's cancellation token follows it there. A build
//! checks the token between steps and before each fallback, so a build whose
//! lease was lost stops instead of starting more work.

use super::duration_ms;
use super::uploads::Uploads;
use crate::archive::ArchiveBuilder;
use crate::cancel::{self, Cancel};
use crate::cas::Cas;
use crate::clonepack::{ChunkRef, ClonepackManifest, hash_from_hex, hash_to_hex};
use crate::metrics::SyncPhaseMetrics;
use crate::pack::PackBuilder;
use crate::provider::RepoId;
use anyhow::{Context, Result};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::{info, warn};

/// Step timings of one Files or Full build, logged when it ends as one line:
/// `build-phases kind=<files|full> repo=<key> commit=<short> outcome=<ready|failed>`
/// followed by a `<step>_ms=<n>` pair for each step that ran, in order, and
/// `total_ms=<n>`.
#[derive(Debug, Default)]
pub(super) struct BuildPhases {
    steps: Vec<(&'static str, u64)>,
}

impl BuildPhases {
    /// Record `step` as taking the time since `start`. A step recorded twice
    /// adds up.
    pub(super) fn record(&mut self, step: &'static str, start: Instant) {
        self.add(step, duration_ms(start.elapsed()));
    }

    pub(super) fn add(&mut self, step: &'static str, ms: u64) {
        match self.steps.iter_mut().find(|(name, _)| *name == step) {
            Some((_, total)) => *total = total.saturating_add(ms),
            None => self.steps.push((step, ms)),
        }
    }

    pub(super) fn report(
        &self,
        kind: &str,
        repo_id: &RepoId,
        commit: &str,
        ready: bool,
        started: Instant,
    ) {
        use std::fmt::Write;
        let mut line = format!(
            "build-phases kind={} repo={} commit={} outcome={}",
            kind,
            repo_id.storage_key(),
            &commit[..7.min(commit.len())],
            if ready { "ready" } else { "failed" }
        );
        for (step, ms) in &self.steps {
            let _ = write!(line, " {step}_ms={ms}");
        }
        let _ = write!(line, " total_ms={}", duration_ms(started.elapsed()));
        info!("{line}");
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncPhases {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mirror_fetch_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_packs_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skeleton_build_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_table_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prebuilt_index_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_head_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_publish_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_head_ms: Option<u64>,
}

impl From<&SyncPhases> for SyncPhaseMetrics {
    fn from(phases: &SyncPhases) -> Self {
        Self {
            mirror_fetch_ms: phases.mirror_fetch_ms,
            head_packs_ms: phases.head_packs_ms,
            skeleton_build_ms: phases.skeleton_build_ms,
            files_table_ms: phases.files_table_ms,
            prebuilt_index_ms: phases.prebuilt_index_ms,
            upload_head_ms: phases.upload_head_ms,
            ref_publish_ms: phases.ref_publish_ms,
            publish_head_ms: phases.publish_head_ms,
        }
    }
}

fn tuple_to_sized(p: &(String, u64, String, u64)) -> crate::SizedPack {
    crate::SizedPack {
        pack: p.0.clone(),
        pack_len: p.1,
        idx: p.2.clone(),
        idx_len: p.3,
    }
}

fn sized_to_tuple(p: &crate::SizedPack) -> (String, u64, String, u64) {
    (p.pack.clone(), p.pack_len, p.idx.clone(), p.idx_len)
}

/// Compact history down to at most this many levels (merging the smallest
/// adjacent pair) so the level count stays bounded under seal-every-sync.
fn lsm_max_levels() -> Result<usize> {
    crate::env::number("RIPCLONE_LSM_MAX_LEVELS", 16)
}

/// Head packs past the base that trigger a rebase onto a new base.
fn head_rebase_bytes() -> Result<u64> {
    crate::env::number("RIPCLONE_HEAD_REBASE_BYTES", 128 * 1024 * 1024)
}

/// Fail on a bad value of a setting the builder reads at build time.
pub(super) fn check_environment() -> Result<()> {
    lsm_max_levels()?;
    head_rebase_bytes()?;
    Ok(())
}

/// Given the prior sealed levels and a freshly built tail, decide whether to
/// seal the tail into a new level, then compact the level set back under
/// `max_levels`. Returns `(history_packs, new_pack_tuples, new_levels)` where
/// `history_packs` is every history pack this manifest references (all levels
/// flattened — prior levels reused by hash), `new_pack_tuples` is the packs
/// freshly built this sync (the tail plus any compaction output to upload), and
/// `new_levels` is the levels to persist for the next sync.
/// `head_packs` is not included here (the caller handles the HEAD closure).
#[allow(clippy::too_many_arguments)]
async fn seal_and_compact(
    mirror_dir: &std::path::Path,
    cas: &Cas,
    commit: &str,
    prev_levels: Vec<crate::HistoryLevel>,
    sealed_tip: Option<String>,
    tail_packs: Vec<(String, u64, String, u64)>,
    max_levels: usize,
    phases: &mut BuildPhases,
) -> Result<(
    Vec<(String, u64, String, u64)>,
    Vec<(String, u64, String, u64)>,
    Vec<crate::HistoryLevel>,
)> {
    // Seal the tail into a new immutable level whenever HEAD advanced past the
    // last sealed tip and the tail is non-empty. The cold base (sealed_tip None)
    // always advances, so it always seals and becomes level 0. Compaction keeps
    // the level count bounded.
    let advances = sealed_tip.as_deref() != Some(commit);
    let seal = advances && !tail_packs.is_empty();
    let mut levels = prev_levels;
    let mut new_tuples = tail_packs.clone();
    if seal {
        levels.push(crate::HistoryLevel {
            tip_commit: commit.to_string(),
            packs: tail_packs.iter().map(tuple_to_sized).collect(),
        });
        let packed_mib: u64 =
            tail_packs.iter().map(|(_, plen, _, _)| plen).sum::<u64>() / (1024 * 1024);
        info!(
            "LSM: sealed level {} at {} ({} packs, {} MiB packed)",
            levels.len() - 1,
            &commit[..7.min(commit.len())],
            tail_packs.len(),
            packed_mib
        );
    }

    // Compact (off-thread; it re-packs ranges) so the level count stays bounded.
    if levels.len() > max_levels {
        let start = Instant::now();
        let before = levels.len();
        let (md, c, levels_in, max) = (
            mirror_dir.to_path_buf(),
            cas.clone(),
            levels.clone(),
            max_levels,
        );
        let res = cancel::spawn_blocking(move || {
            PackBuilder::new(&md, &c).compact_levels(levels_in, max)
        })
        .await
        .context("compaction task")??;
        new_tuples.extend(res.new_packs.iter().cloned());
        levels = res.levels;
        info!("LSM: compacted {} levels -> {}", before, levels.len());
        phases.record("level_compact", start);
    }

    // Manifest history = every sealed level's packs (prior reused by hash + any
    // compaction output), flattened. We always seal an advancing non-empty tail,
    // so there is never an unsealed `(sealed_tip, HEAD]` remainder to append: the
    // only time `seal` is false is when the tail is empty (HEAD didn't advance).
    let history_packs: Vec<(String, u64, String, u64)> = levels
        .iter()
        .flat_map(|l| l.packs.iter().map(sized_to_tuple))
        .collect();
    Ok((history_packs, new_tuples, levels))
}

/// Load and decode a prior sync's metadata chunk and return its files table.
/// Bytes come from local CAS or object storage (the disposable local cache may
/// have been cleared after a prior upload). Returns `None` on any failure — the
/// caller then falls back to a full (non-incremental) build, so this is purely
/// best-effort optimization, never a correctness dependency.
fn load_metadata_files(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    metadata_hash: &str,
) -> Option<Vec<crate::clonepack::FileEntry>> {
    let bytes = cas
        .get(metadata_hash)
        .or_else(|_| storage.get(metadata_hash))
        .ok()?;
    let md = crate::clonepack::MetadataChunk::decode_and_validate(bytes.as_slice()).ok()?;
    Some(md.files)
}

/// [`load_metadata_files`] on the blocking pool.
async fn load_metadata_files_async(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    metadata_hash: &str,
) -> Option<Vec<crate::clonepack::FileEntry>> {
    let (cas, storage, hash) = (cas.clone(), storage.clone(), metadata_hash.to_string());
    cancel::spawn_blocking(move || load_metadata_files(&cas, &storage, &hash))
        .await
        .ok()
        .flatten()
}

/// Build one result's PackEntry list and concatenated idx bundle.
fn assemble_variant(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    tagged: &[(&(String, u64, String, u64), bool)],
) -> Result<(Vec<crate::clonepack::PackEntry>, Option<ChunkRef>, String)> {
    if tagged.is_empty() {
        return Ok((Vec::new(), None, String::new()));
    }
    use std::io::Write;

    let mut bundle = cas.writer().context("create idx bundle")?;
    let mut entries = Vec::with_capacity(tagged.len());
    let mut len = 0u64;
    for &(pack, history_only) in tagged {
        let idx_bytes = cas.get(&pack.2).or_else(|_| storage.get(&pack.2))?;
        let offset = len;
        bundle
            .write_all(&idx_bytes)
            .context("write idx bytes to bundle")?;
        len += idx_bytes.len() as u64;
        entries.push(crate::clonepack::PackEntry {
            pack: Some(ChunkRef {
                hash: hash_from_hex(&pack.0)?,
                len: pack.1,
            }),
            idx: Some(ChunkRef {
                hash: hash_from_hex(&pack.2)?,
                len: pack.3,
            }),
            history_only,
            idx_bundle_offset: offset,
        });
    }
    let (hash, stored_len) = bundle.finish()?;
    anyhow::ensure!(
        stored_len == len,
        "idx bundle length changed while storing: expected {len}, got {stored_len}"
    );
    Ok((
        entries,
        Some(ChunkRef {
            hash: hash_from_hex(&hash)?,
            len,
        }),
        hash,
    ))
}

/// Build a multi-pack-index over `packs` from local CAS.
///
/// Git reads the packs where they lie in the *local* CAS (linked into a
/// scratch repo; no object-storage fallback), so only call this when every
/// pack was built this sync and is still local — e.g. the head MIDX is
/// shipped only when all head buckets were freshly built. For a set with
/// reused packs absent from the local cache, omit the MIDX and let the
/// client build its own.
fn assemble_midx(
    cas: &Cas,
    packs: &[(String, u64, String, u64)],
) -> Result<(Option<ChunkRef>, String)> {
    if packs.is_empty() {
        return Ok((None, String::new()));
    }
    let files: Vec<_> = packs
        .iter()
        .map(|(ph, _, ih, _)| (cas.path(ph), cas.path(ih)))
        .collect();
    let scratch = tempfile::TempDir::new().context("create midx scratch dir")?;
    let midx = crate::git::write_multi_pack_index_over(&files, scratch.path())?;
    let (hash, len) = cas.install_file(&midx)?;
    Ok((
        Some(ChunkRef {
            hash: hash_from_hex(&hash)?,
            len,
        }),
        hash,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn make_manifest(
    commit: &str,
    parent: &Option<String>,
    archive_chunks: &[ChunkRef],
    metadata_hash: &str,
    metadata_len: u64,
    packs: Vec<crate::clonepack::PackEntry>,
    midx: Option<ChunkRef>,
    idx_bundle: Option<ChunkRef>,
) -> Result<ClonepackManifest> {
    Ok(ClonepackManifest {
        commit: commit.to_string(),
        parent_commit: parent.clone(),
        metadata_chunk: Some(ChunkRef {
            hash: hash_from_hex(metadata_hash)?,
            len: metadata_len,
        }),
        archive_chunks: archive_chunks.to_vec(),
        packs,
        midx,
        idx_bundle,
        ..Default::default()
    })
}

/// Build the `[ChunkRef]` list for the archive chunks of a metadata chunk.
fn archive_chunk_refs(
    archive_chunk_hashes: &[String],
    metadata_chunk: &crate::clonepack::MetadataChunk,
) -> Result<Vec<ChunkRef>> {
    let lengths = crate::clonepack::archive_chunk_lengths(metadata_chunk)?;
    if lengths.len() != archive_chunk_hashes.len() {
        anyhow::bail!(
            "archive chunk hash/length mismatch: hashes={} lengths={}",
            archive_chunk_hashes.len(),
            lengths.len()
        );
    }
    archive_chunk_hashes
        .iter()
        .zip(lengths.iter())
        .map(|(hash, len)| {
            Ok(ChunkRef {
                hash: hash_from_hex(hash)?,
                len: *len,
            })
        })
        .collect()
}

fn archive_publish_upload_hashes(
    metadata_hash: &str,
    clonepack_hash: &str,
    download_bundle_hashes: &[String],
    new_reuse_frame_hashes: &[String],
) -> Vec<String> {
    let mut uploads: Vec<String> = vec![metadata_hash.to_string(), clonepack_hash.to_string()];
    uploads.extend(download_bundle_hashes.iter().cloned());
    uploads.extend(new_reuse_frame_hashes.iter().cloned());
    uploads.retain(|h| !h.is_empty());
    uploads.sort();
    uploads.dedup();
    uploads
}

/// After upload, a remote backend may drop local pack copies while keeping the
/// tiny idx files used by later bundle and MIDX builds. Local storage owns the
/// durable copy and keeps every uploaded artifact.
async fn settle_storage(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    uploaded: Vec<String>,
    keep_idx: std::collections::HashSet<String>,
) {
    if storage.is_remote() {
        let cas = cas.clone();
        let _ = tokio::task::spawn_blocking(move || {
            for h in uploaded.iter().filter(|h| !keep_idx.contains(*h)) {
                let _ = cas.remove(h);
            }
        })
        .await;
    }
}

fn pack_artifacts_of(packs: &[(String, u64, String, u64)]) -> Vec<crate::PackArtifact> {
    packs
        .iter()
        .map(|(p, _, i, _)| crate::PackArtifact {
            pack: p.clone(),
            idx: i.clone(),
        })
        .collect()
}

/// Result of the Head-closure build: a small delta pack against the
/// immutable base, or a fresh full base on a cold sync / rebase. See
/// `build_head_delta_pack` / `build_head_packs`.
struct HeadBuild {
    /// Every current HEAD pack (base + delta), manifest order — for the clonepack.
    all_packs: Vec<(String, u64, String, u64)>,
    /// Only the packs built this sync (to upload). Reused base packs are durable.
    new_built: Vec<(String, u64, String, u64)>,
    /// The commit whose closure `base_packs` covers (carried, or = commit on cold).
    base_commit: String,
    /// The base packs (closure of `base_commit`), carried unchanged across deltas.
    base_packs: Vec<crate::SizedPack>,
    /// True when every pack was built this sync (cold/rebase) → head MIDX buildable.
    all_local: bool,
    elapsed_ms: u64,
}

/// Build and upload the Head result for `commit`. Head packs are a delta
/// against `base`, an immutable Head pack set of an ancestor; without a base,
/// or when the base commit cannot be read, the full closure is packed and
/// becomes the base. `previous_head`, the ready Head of an ancestor, lets the
/// files table hash only the paths changed since it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_head_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    commit: &str,
    parent: Option<String>,
    previous_head: Option<&crate::HeadResult>,
    base: Option<crate::HeadBase>,
    storage: &crate::storage::StorageRef,
    phases: &mut SyncPhases,
) -> Result<crate::HeadResult> {
    let uploads = Uploads::new(cas, storage);
    let cas = &uploads.cas();

    // Build the Head closure and shallow skeleton.
    let t = Instant::now();
    let sk_start = Instant::now();
    let (md1, c1, cm1) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let shallow_skeleton_handle = cancel::spawn_blocking(move || {
        let s = Instant::now();
        let r = PackBuilder::new(&md1, &c1).build_shallow_skeleton_pack(&cm1);
        info!("Head skeleton {:?}", s.elapsed());
        r
    });
    // The prebuilt index reads the commit's tree from the mirror, so it builds
    // alongside the skeleton rather than after it.
    let (md4, c4, cm4) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let prebuilt_index_handle = cancel::spawn_blocking(move || {
        let s = Instant::now();
        let index = PackBuilder::new(&md4, &c4).build_prebuilt_index(&cm4)?;
        anyhow::Ok((index, duration_ms(s.elapsed())))
    });
    // Head-closure packs, incremental by delta against an immutable base: keep the
    // base packs (closure of `head_base_commit`) and pack only the depth-1 objects
    // new since that base (`closure(HEAD) − closure(base)`) into a delta pack. The
    // base and delta are disjoint by construction, so no object is ever in two HEAD
    // packs (which would double-materialize a worktree file). A cold sync (no base)
    // packs the full closure as the base. The cumulative delta grows as HEAD moves
    // from the base; once it reaches RIPCLONE_HEAD_REBASE_BYTES, the Full build
    // compacts a new base for later Heads, off the depth=1 critical path.
    let head_target = 4 * 1024 * 1024;
    let base = base.filter(|base| !base.commit.is_empty() && !base.packs.is_empty());
    let (md2, c2, cm2) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let head_handle = cancel::spawn_blocking(move || -> Result<HeadBuild> {
        let s = Instant::now();
        let b = PackBuilder::new(&md2, &c2);
        // Delta path: a base exists; pack only what is new since the base. A
        // base the mirror cannot read falls back to the cold path.
        let base =
            base.and_then(
                |base| match b.build_head_delta_pack(&cm2, &base.commit, head_target) {
                    Ok(delta) => Some((base, delta)),
                    Err(e) => {
                        warn!(
                            "Head delta vs base {} failed ({e:#}); packing a full base",
                            &base.commit[..7.min(base.commit.len())]
                        );
                        None
                    }
                },
            );
        match base {
            Some((base, delta)) => {
                let mut all_packs: Vec<(String, u64, String, u64)> =
                    base.packs.iter().map(sized_to_tuple).collect();
                all_packs.extend(delta.iter().cloned());
                let elapsed = s.elapsed();
                info!(
                    "Head packs (delta vs base {}: {} new pack(s), {} total) {:?}",
                    &base.commit[..7.min(base.commit.len())],
                    delta.len(),
                    all_packs.len(),
                    elapsed
                );
                Ok(HeadBuild {
                    all_packs,
                    new_built: delta,
                    base_commit: base.commit,
                    base_packs: base.packs,
                    all_local: false,
                    elapsed_ms: duration_ms(elapsed),
                })
            }
            // Cold path: no base yet → pack the full closure as the base.
            None => {
                Cancel::check_current()?;
                let base = b.build_head_packs(&cm2, head_target)?;
                let base_packs = base.iter().map(tuple_to_sized).collect();
                let elapsed = s.elapsed();
                info!("Head packs (full base, {} packs) {:?}", base.len(), elapsed);
                Ok(HeadBuild {
                    all_packs: base.clone(),
                    new_built: base,
                    base_commit: cm2,
                    base_packs,
                    all_local: true,
                    elapsed_ms: duration_ms(elapsed),
                })
            }
        }
    });
    // Head builds only the cheap files table (no zstd frames): editable
    // depth=1 materializes the worktree from the HEAD-closure packs, so it does
    // not need the archive. The full zstd archive (for files mode) is built in
    // concurrently afterward, off the time-to-Head critical path.
    //
    // Files-table by-diff: with an ancestor's ready Head, reuse its content
    // hashes for unchanged paths and read+hash only the blobs that changed since
    // that commit (O(changed) instead of O(worktree)). Falls back to a full
    // table when there is no previous table.
    let prev_files: Option<Vec<crate::clonepack::FileEntry>> = match previous_head {
        Some(head) => load_metadata_files_async(cas, storage, &head.clonepack.metadata_chunk).await,
        None => None,
    };
    let prev_commit_for_diff = previous_head.map(|head| head.clonepack.commit.clone());
    // Carry the prior files table + commit so the bounded archive can hash only
    // changed files and reuse frames for the unchanged prefix/suffix.
    let (md3, cm3) = (mirror_dir.to_path_buf(), commit.to_string());
    let ft_start = Instant::now();
    let files_table_handle = match (prev_files, prev_commit_for_diff) {
        (Some(pf), Some(from)) if !from.is_empty() => {
            let (md, cm, frm) = (md3.clone(), cm3.clone(), from);
            cancel::spawn_blocking(move || {
                let s = Instant::now();
                // If the diff fails (e.g. prev.commit was pruned after a
                // force-push), fall back to a full rebuild rather than failing
                // the sync — reuse is purely an optimization.
                match crate::git::diff_name_set(&md, &frm, &cm) {
                    Ok(changed) => {
                        let r = ArchiveBuilder::new(&md)
                            .build_files_table_incremental(&cm, &pf, &changed);
                        info!(
                            "Head files table (incremental, {} changed) {:?}",
                            changed.len(),
                            s.elapsed()
                        );
                        r
                    }
                    Err(e) => {
                        Cancel::check_current()?;
                        warn!("files-table diff failed ({e:#}); full rebuild");
                        let r = ArchiveBuilder::new(&md).build_files_table(&cm);
                        info!("Head files table (full, diff fallback) {:?}", s.elapsed());
                        r
                    }
                }
            })
        }
        _ => cancel::spawn_blocking(move || {
            let s = Instant::now();
            let r = ArchiveBuilder::new(&md3).build_files_table(&cm3);
            info!("Head files table (full) {:?}", s.elapsed());
            r
        }),
    };
    let (shallow_skeleton_pack, shallow_skeleton_idx) = shallow_skeleton_handle
        .await
        .context("shallow skeleton")??;
    phases.skeleton_build_ms = Some(duration_ms(sk_start.elapsed()));
    let head_built = head_handle.await.context("head packs")??;
    phases.head_packs_ms = Some(head_built.elapsed_ms);
    let head_packs = head_built.all_packs.clone();
    let metadata_base = files_table_handle.await.context("files table")??;
    phases.files_table_ms = Some(duration_ms(ft_start.elapsed()));
    info!("Head result: packs+skeleton+files-table {:?}", t.elapsed());

    let (shallow_prebuilt_index, prebuilt_index_ms) = prebuilt_index_handle
        .await
        .context("shallow prebuilt index")??;
    phases.prebuilt_index_ms = Some(prebuilt_index_ms);

    Cancel::check_current()?;
    // Assemble the metadata chunk, idx bundle, MIDX and manifest from CAS.
    let (fc, fs, fcommit, fparent) = (
        cas.clone(),
        storage.clone(),
        commit.to_string(),
        parent.clone(),
    );
    let (fsk_pack, fsk_idx, fprebuilt, fpacks, all_built) = (
        shallow_skeleton_pack.clone(),
        shallow_skeleton_idx.clone(),
        shallow_prebuilt_index.clone(),
        head_packs.clone(),
        head_built.all_local,
    );
    let (shallow_metadata_hash, shallow_clonepack_hash, head_idx_bundle_hash, head_midx_hash) =
        cancel::spawn_blocking(move || -> Result<(String, String, String, String)> {
            let (cas, storage) = (&fc, &fs);
            let mut shallow_meta = metadata_base;
            // Built moments ago by this build.
            shallow_meta.skeleton_pack = cas.read_fresh(&fsk_pack)?;
            shallow_meta.skeleton_idx = cas.read_fresh(&fsk_idx)?;
            shallow_meta.prebuilt_index = cas.read_fresh(&fprebuilt)?;
            let shallow_meta_data = shallow_meta.encode_to_vec();
            let shallow_metadata_hash = cas.put(&shallow_meta_data)?;

            // Head has no archive frames. Files publishes its own archive result.
            let archive_chunks = archive_chunk_refs(&[], &shallow_meta)?;
            let head_tagged: Vec<(&(String, u64, String, u64), bool)> =
                fpacks.iter().map(|p| (p, false)).collect();
            let (head_entries, head_idx_bundle_ref, head_idx_bundle_hash) =
                assemble_variant(cas, storage, &head_tagged)?;
            Cancel::check_current()?;
            // Ship the head MIDX only on a cold full base (all pack bytes still
            // local). On a delta re-sync the base packs may be absent from local
            // cache, so omit it — the client builds its own MIDX from the
            // per-pack idxs.
            let (head_midx_ref, head_midx_hash) = if all_built {
                assemble_midx(cas, &fpacks)?
            } else {
                (None, String::new())
            };

            let shallow_manifest = make_manifest(
                &fcommit,
                &fparent,
                &archive_chunks,
                &shallow_metadata_hash,
                shallow_meta_data.len() as u64,
                head_entries,
                head_midx_ref,
                head_idx_bundle_ref,
            )?;
            let shallow_clonepack_hash = cas.put(&shallow_manifest.encode_to_vec())?;
            Ok((
                shallow_metadata_hash,
                shallow_clonepack_hash,
                head_idx_bundle_hash,
                head_midx_hash,
            ))
        })
        .await
        .context("Head manifest task")??;

    let head = crate::HeadResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: shallow_clonepack_hash.clone(),
            metadata_chunk: shallow_metadata_hash.clone(),
            skeleton_pack: shallow_skeleton_pack.clone(),
            skeleton_idx: shallow_skeleton_idx.clone(),
            prebuilt_index: shallow_prebuilt_index.clone(),
            midx: head_midx_hash.clone(),
            idx_bundle: head_idx_bundle_hash.clone(),
            commit: commit.to_string(),
        },
        parent_commit: parent.clone(),
        packs: pack_artifacts_of(&head_packs),
        base_commit: head_built.base_commit.clone(),
        base_packs: head_built.base_packs.clone(),
    };

    // Upload Head artifacts (shallow skeleton/index/metadata, head idx-bundle
    // + midx, shallow manifest, and only the FRESHLY BUILT head packs+idx).
    // Reused bucket packs are already durable in storage from a prior sync.
    let mut head_uploads: Vec<String> = vec![
        shallow_skeleton_pack.clone(),
        shallow_skeleton_idx.clone(),
        shallow_prebuilt_index.clone(),
        shallow_metadata_hash.clone(),
        shallow_clonepack_hash.clone(),
        head_idx_bundle_hash.clone(),
        head_midx_hash.clone(),
    ];
    for (p, _, i, _) in &head_built.new_built {
        head_uploads.push(p.clone());
        head_uploads.push(i.clone());
    }
    let head_idx_keep: std::collections::HashSet<String> =
        head_packs.iter().map(|(_, _, ih, _)| ih.clone()).collect();
    Cancel::check_current()?;
    // Each upload started when its artifact was stored; wait for the rest.
    let upload_start = Instant::now();
    uploads.start_all(&head_uploads);
    let uploaded = uploads.finish().await?;
    settle_storage(cas, storage, uploaded, head_idx_keep).await;
    phases.upload_head_ms = Some(duration_ms(upload_start.elapsed()));

    Ok(head)
}

/// Storage amplification for one ref: durable bytes in object storage divided by
/// the upstream bare-mirror size, split by artifact class. Content-addressed
/// storage may be shared across refs, so this attributes every hash reachable
/// from the given ref to that ref.
#[derive(Debug, Clone, serde::Serialize)]
struct StorageAmplification {
    repo_size_bytes: u64,
    head_pack_bytes: u64,
    history_pack_bytes: u64,
    archive_chunk_bytes: u64,
    metadata_bytes: u64,
    total_storage_bytes: u64,
    amplification: f64,
}

/// Recursively sum file sizes under `dir`.
fn dir_size(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    total += dir_size(&path);
                }
            }
        }
    }
    total
}

/// Classify every hash reachable from `info` by artifact class and sum its
/// bytes in `storage`.
fn measure_storage_amplification(
    storage: &crate::storage::StorageRef,
    info: &crate::RefInfo,
    mirror_dir: &std::path::Path,
) -> Option<StorageAmplification> {
    let repo_size = dir_size(mirror_dir);
    let mut head_pack_bytes = 0u64;
    let mut history_pack_bytes = 0u64;
    let mut archive_chunk_bytes = 0u64;
    let mut metadata_bytes = 0u64;

    let add = |hash: &str, bucket: &mut u64| {
        if hash.is_empty() {
            return;
        }
        if let Ok(size) = storage.size(hash) {
            *bucket += size;
        }
    };

    // Head closure packs (base + delta) and their idx files.
    if let Some(head) = &info.head {
        for pack in &head.packs {
            add(&pack.pack, &mut head_pack_bytes);
            add(&pack.idx, &mut head_pack_bytes);
        }
    }

    // Full-history / LSM sealed levels.
    if let Some(full) = &info.full {
        for level in &full.history_levels {
            for pack in &level.packs {
                add(&pack.pack, &mut history_pack_bytes);
                add(&pack.idx, &mut history_pack_bytes);
            }
        }
    }

    // Archive chunks referenced directly from the ref.
    if let Some(files) = &info.files {
        for hash in &files.archive_chunks {
            add(hash, &mut archive_chunk_bytes);
        }
    }

    // Metadata: manifests, metadata chunks, skeleton/index, prebuilt index,
    // idx bundle, and MIDX.
    for artifacts in [
        info.head.as_ref().map(|result| &result.clonepack),
        info.full.as_ref().map(|result| &result.clonepack),
        info.files.as_ref().map(|result| &result.clonepack),
    ]
    .into_iter()
    .flatten()
    {
        for hash in [
            &artifacts.manifest,
            &artifacts.metadata_chunk,
            &artifacts.skeleton_pack,
            &artifacts.skeleton_idx,
            &artifacts.prebuilt_index,
            &artifacts.idx_bundle,
            &artifacts.midx,
        ] {
            add(hash, &mut metadata_bytes);
        }
    }

    let total_storage_bytes = head_pack_bytes
        .saturating_add(history_pack_bytes)
        .saturating_add(archive_chunk_bytes)
        .saturating_add(metadata_bytes);
    let amplification = if repo_size == 0 {
        0.0
    } else {
        total_storage_bytes as f64 / repo_size as f64
    };

    Some(StorageAmplification {
        repo_size_bytes: repo_size,
        head_pack_bytes,
        history_pack_bytes,
        archive_chunk_bytes,
        metadata_bytes,
        total_storage_bytes,
        amplification,
    })
}

pub(super) fn report_sync_phases(repo_id: &RepoId, commit: &str, phases: &SyncPhases) {
    let report = serde_json::json!({
        "kind": "sync-phases",
        "repo": repo_id.storage_key(),
        "commit": &commit[..7.min(commit.len())],
        "phases": phases,
    });
    info!("{}", report.to_string());
}

/// Print a JSON benchmark report when `RIPCLONE_BENCH` is set. Mirrors the
/// client-side `--bench` report style: one structured object per completed
/// build, emitted at INFO so it can be scraped from logs. Phase timings are in
/// the `sync-phases` report written when Head publishes.
pub(super) fn report_sync_bench(
    repo_id: &RepoId,
    commit: &str,
    storage: &crate::storage::StorageRef,
    info: &crate::RefInfo,
    mirror_dir: &std::path::Path,
) {
    let amplification = measure_storage_amplification(storage, info, mirror_dir);
    let report = serde_json::json!({
        "kind": "sync-bench",
        "repo": repo_id.storage_key(),
        "commit": &commit[..7.min(commit.len())],
        "storage_amplification": amplification,
    });
    info!("{}", report.to_string());
}

fn manifest_pack_tuples(manifest: &ClonepackManifest) -> Result<Vec<(String, u64, String, u64)>> {
    manifest
        .packs
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let pack = entry
                .pack
                .as_ref()
                .with_context(|| format!("Head manifest pack {index} missing pack"))?;
            let idx = entry
                .idx
                .as_ref()
                .with_context(|| format!("Head manifest pack {index} missing idx"))?;
            Ok((
                hash_to_hex(&pack.hash),
                pack.len,
                hash_to_hex(&idx.hash),
                idx.len,
            ))
        })
        .collect()
}

/// Build and upload the Full result: the Head packs plus history levels.
/// When the Head delta has reached `RIPCLONE_HEAD_REBASE_BYTES`, the Head
/// packs are first compacted into a fresh base for `commit`. Full uses the
/// compact packs and records them as `head_base`, so later Heads delta against
/// it. The published Head result never changes. `before_cold` runs right
/// before a cold history build, the whole history packed as level 0: on the
/// first Full, and when the tail from the sealed tip cannot be listed.
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_full_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    commit: &str,
    head: &crate::HeadResult,
    mut previous_levels: Vec<crate::HistoryLevel>,
    storage: &crate::storage::StorageRef,
    before_cold: impl std::future::Future<Output = ()>,
    phases: &mut BuildPhases,
) -> Result<crate::FullResult> {
    let uploads = Uploads::new(cas, storage);
    let cas = &uploads.cas();
    // Head was built on this server moments ago, so local CAS usually still
    // has its manifest.
    let (mc, ms, mhash) = (
        cas.clone(),
        storage.clone(),
        head.clonepack.manifest.clone(),
    );
    let head_manifest_bytes =
        cancel::spawn_blocking(move || mc.get(&mhash).or_else(|_| ms.get(&mhash)))
            .await
            .context("Head manifest read task")?
            .context("read Head manifest for Full")?;
    let head_manifest = ClonepackManifest::decode(head_manifest_bytes.as_slice())
        .context("decode Head manifest for Full")?;
    anyhow::ensure!(
        head_manifest.commit == commit && head.clonepack.commit == commit,
        "Head result identity mismatch for {}@{commit}",
        repo_id.storage_key()
    );
    let mut head_packs = manifest_pack_tuples(&head_manifest)?;
    let parent = head.parent_commit.clone();

    let mut sealed_tip = previous_levels.last().map(|level| level.tip_commit.clone());
    let build_tail = |since: Option<String>| {
        let (mirror, cas, commit) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
        cancel::spawn_blocking(move || {
            PackBuilder::new(&mirror, &cas).build_history_tail(&commit, since.as_deref())
        })
    };
    // A tail the mirror cannot list from the sealed tip builds cold instead:
    // a new level 0 with the whole history.
    let tail = match sealed_tip.clone() {
        None => None,
        Some(tip) => match build_tail(Some(tip.clone()))
            .await
            .context("Full history task")?
        {
            Ok(built) => Some(built),
            Err(error) => {
                Cancel::check_current()?;
                warn!(
                    "history tail from {} failed ({error:#}); building history cold",
                    &tip[..7.min(tip.len())]
                );
                previous_levels.clear();
                sealed_tip = None;
                None
            }
        },
    };
    let built_history = match tail {
        Some(built) => built,
        None => {
            let start = Instant::now();
            before_cold.await;
            phases.record("bitmap", start);
            Cancel::check_current()?;
            build_tail(None).await.context("Full history task")??
        }
    };
    phases.add("history", built_history.pack_ms);
    phases.add("history_store", built_history.store_ms);

    let (history_packs, new_history, levels) = seal_and_compact(
        mirror_dir,
        cas,
        commit,
        previous_levels,
        sealed_tip,
        built_history.packs,
        lsm_max_levels()?,
        phases,
    )
    .await?;

    let rebase_bytes = head_rebase_bytes()?;
    let delta_bytes: u64 = head_packs
        .iter()
        .skip(head.base_packs.len())
        .map(|(_, pack_len, _, _)| *pack_len)
        .sum();
    let mut head_base = None;
    Cancel::check_current()?;
    if delta_bytes > 0 && delta_bytes >= rebase_bytes {
        let start = Instant::now();
        let compact_mirror = mirror_dir.to_path_buf();
        let compact_cas = cas.clone();
        let compact_commit = commit.to_string();
        let compact_packs = cancel::spawn_blocking(move || {
            PackBuilder::new(&compact_mirror, &compact_cas)
                .build_head_packs(&compact_commit, 4 * 1024 * 1024)
        })
        .await
        .context("compact Head packs")??;
        for (pack, _, idx, _) in &compact_packs {
            uploads.start_all([pack, idx]);
        }
        info!(
            "compacted Head packs for {}@{} ({} MiB delta -> {} packs)",
            repo_id.storage_key(),
            &commit[..7.min(commit.len())],
            delta_bytes / (1024 * 1024),
            compact_packs.len()
        );
        head_base = Some(crate::HeadBase {
            commit: commit.to_string(),
            packs: compact_packs.iter().map(tuple_to_sized).collect(),
        });
        head_packs = compact_packs;
        phases.record("head_rebase", start);
    }

    Cancel::check_current()?;
    let (vc, vs, vcommit, vparent, vmetadata) = (
        cas.clone(),
        storage.clone(),
        commit.to_string(),
        parent.clone(),
        head.clonepack.metadata_chunk.clone(),
    );
    let (vhead, vhistory) = (head_packs.clone(), history_packs.clone());
    let metadata_len = head_manifest.metadata_chunk.as_ref().map(|chunk| chunk.len);
    let start = Instant::now();
    let (manifest_hash, idx_bundle_hash) = cancel::spawn_blocking(move || -> Result<_> {
        let tagged: Vec<(&(String, u64, String, u64), bool)> = vhead
            .iter()
            .map(|pack| (pack, false))
            .chain(vhistory.iter().map(|pack| (pack, true)))
            .collect();
        let (entries, idx_bundle_ref, idx_bundle_hash) = assemble_variant(&vc, &vs, &tagged)?;
        let metadata_len = match metadata_len {
            Some(len) => len,
            None => vs.size(&vmetadata).context("size Head metadata for Full")?,
        };
        let manifest = make_manifest(
            &vcommit,
            &vparent,
            &[],
            &vmetadata,
            metadata_len,
            entries,
            None,
            idx_bundle_ref,
        )?;
        Ok((vc.put(&manifest.encode_to_vec())?, idx_bundle_hash))
    })
    .await
    .context("Full manifest task")??;
    phases.record("idx_bundle", start);
    uploads.start_all([&manifest_hash, &idx_bundle_hash]);
    for (pack, _, idx, _) in &new_history {
        uploads.start_all([pack, idx]);
    }
    // Later builds read the idx files of the packs this Full references.
    let keep_idx = head_packs
        .iter()
        .chain(&new_history)
        .map(|(_, _, idx, _)| idx.clone())
        .collect();
    let start = Instant::now();
    let uploaded = uploads.finish().await?;
    settle_storage(cas, storage, uploaded, keep_idx).await;
    phases.record("upload", start);

    let mut all_packs = head_packs;
    all_packs.extend(history_packs);
    Ok(crate::FullResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: manifest_hash,
            metadata_chunk: head.clonepack.metadata_chunk.clone(),
            skeleton_pack: head.clonepack.skeleton_pack.clone(),
            skeleton_idx: head.clonepack.skeleton_idx.clone(),
            prebuilt_index: head.clonepack.prebuilt_index.clone(),
            midx: String::new(),
            idx_bundle: idx_bundle_hash,
            commit: commit.to_string(),
        },
        packs: pack_artifacts_of(&all_packs),
        history_levels: levels,
        head_base,
    })
}

/// Build and upload the Files result. `parent_files`, the parent's ready
/// Files result and commit, lets the archive reuse unchanged frames.
pub(super) async fn build_files_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    commit: &str,
    head: &crate::HeadResult,
    parent_files: Option<(&str, &crate::FilesResult)>,
    storage: &crate::storage::StorageRef,
    compression_level: i32,
    phases: &mut BuildPhases,
) -> Result<crate::FilesResult> {
    let uploads = Uploads::new(cas, storage);
    let cas = &uploads.cas();
    let start = Instant::now();
    let previous_frames = parent_files
        .map(|(_, files)| files.archive_frames.clone())
        .unwrap_or_default();
    let previous_files = match parent_files {
        Some((_, files)) => {
            load_metadata_files_async(cas, storage, &files.clonepack.metadata_chunk)
                .await
                .unwrap_or_default()
        }
        None => Vec::new(),
    };
    let previous_commit = parent_files
        .map(|(commit, _)| commit.to_string())
        .unwrap_or_default();
    phases.record("base_files", start);
    let start = Instant::now();
    let archive_mirror = mirror_dir.to_path_buf();
    let archive_cas = cas.clone();
    let archive_storage = storage.clone();
    let archive_commit = commit.to_string();
    let archive_output = cancel::spawn_blocking(move || {
        ArchiveBuilder::new(&archive_mirror).build_into_cas_bounded(
            &archive_commit,
            &archive_cas,
            Some(&archive_storage),
            compression_level,
            &previous_frames,
            &previous_files,
            &previous_commit,
            crate::archive::DEFAULT_ARCHIVE_CHUNK_SIZE,
        )
    })
    .await
    .context("Files archive task")??;
    phases.record("archive", start);

    Cancel::check_current()?;
    let start = Instant::now();
    let archive_chunk_hashes = archive_output.download_bundle_hashes;
    let mut metadata = archive_output.metadata;
    let (fc, fs, fcommit, fhead) = (
        cas.clone(),
        storage.clone(),
        commit.to_string(),
        head.clone(),
    );
    let fchunks = archive_chunk_hashes.clone();
    let (metadata_hash, manifest_hash) = cancel::spawn_blocking(move || -> Result<_> {
        let fetch = |hash: &str| fc.get(hash).or_else(|_| fs.get(hash));
        metadata.skeleton_pack = fetch(&fhead.clonepack.skeleton_pack)?;
        metadata.skeleton_idx = fetch(&fhead.clonepack.skeleton_idx)?;
        metadata.prebuilt_index = fetch(&fhead.clonepack.prebuilt_index)?;
        let metadata_bytes = metadata.encode_to_vec();
        let metadata_hash = fc.put(&metadata_bytes)?;
        let archive_chunks = archive_chunk_refs(&fchunks, &metadata)?;
        let manifest = make_manifest(
            &fcommit,
            &fhead.parent_commit,
            &archive_chunks,
            &metadata_hash,
            metadata_bytes.len() as u64,
            Vec::new(),
            None,
            None,
        )?;
        let manifest_hash = fc.put(&manifest.encode_to_vec())?;
        Ok((metadata_hash, manifest_hash))
    })
    .await
    .context("Files manifest task")??;
    phases.record("manifest", start);
    let publish_uploads = archive_publish_upload_hashes(
        &metadata_hash,
        &manifest_hash,
        &archive_chunk_hashes,
        &archive_output.new_reuse_frame_hashes,
    );
    let start = Instant::now();
    uploads.start_all(&publish_uploads);
    let uploaded = uploads.finish().await?;
    settle_storage(cas, storage, uploaded, std::collections::HashSet::new()).await;
    phases.record("upload", start);

    Ok(crate::FilesResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: manifest_hash,
            metadata_chunk: metadata_hash,
            skeleton_pack: head.clonepack.skeleton_pack.clone(),
            skeleton_idx: head.clonepack.skeleton_idx.clone(),
            prebuilt_index: head.clonepack.prebuilt_index.clone(),
            midx: String::new(),
            idx_bundle: String::new(),
            commit: commit.to_string(),
        },
        archive_chunks: archive_chunk_hashes,
        archive_frames: archive_output.archive_frames,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_publish_uploads_download_bundles_and_reuse_frames() {
        let uploads = archive_publish_upload_hashes(
            "metadata",
            "clonepack",
            &["bundle-b".to_string(), "bundle-a".to_string()],
            &["frame-a".to_string(), "bundle-a".to_string(), String::new()],
        );
        assert_eq!(
            uploads,
            vec![
                "bundle-a".to_string(),
                "bundle-b".to_string(),
                "clonepack".to_string(),
                "frame-a".to_string(),
                "metadata".to_string(),
            ]
        );
    }

    #[test]
    fn archive_chunk_refs_rejects_hash_length_mismatch() {
        let mut metadata = crate::clonepack::MetadataChunk::new();
        metadata.frames.push(crate::clonepack::FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 4,
            raw_len: 10,
        });
        metadata.frames.push(crate::clonepack::FrameInfo {
            chunk_index: 1,
            chunk_offset: 0,
            compressed_len: 5,
            raw_len: 11,
        });

        assert!(archive_chunk_refs(&["a".repeat(64)], &metadata).is_err());
    }
}
