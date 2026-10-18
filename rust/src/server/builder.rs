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
/// followed by a `<step>_ms=<n>` pair for each step that ran, in order,
/// `total_ms=<n>`, and a `<name>_bytes=<n>` pair for each size recorded.
#[derive(Debug, Default)]
pub(super) struct BuildPhases {
    steps: Vec<(&'static str, u64)>,
    sizes: Vec<(&'static str, u64)>,
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

    /// Record `name` as `bytes` long, such as the bytes a step wrote.
    pub(super) fn size(&mut self, name: &'static str, bytes: u64) {
        self.sizes.push((name, bytes));
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
        for (name, bytes) in &self.sizes {
            let _ = write!(line, " {name}_bytes={bytes}");
        }
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
    pub files_table_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prebuilt_index_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_head_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_publish_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_head_ms: Option<u64>,
    /// Bytes of the Head packs this sync built: the delta since the base,
    /// or the whole base on a cold sync.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_packs_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_metadata_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_idx_bundle_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_head_bytes: Option<u64>,
}

impl From<&SyncPhases> for SyncPhaseMetrics {
    fn from(phases: &SyncPhases) -> Self {
        Self {
            mirror_fetch_ms: phases.mirror_fetch_ms,
            head_packs_ms: phases.head_packs_ms,
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

/// Head level bytes past the base that trigger a rebase onto a new base.
/// Levels past a quarter of the base, or [`MAX_HEAD_LEVEL_PACKS`] packs,
/// also do.
fn head_rebase_bytes() -> Result<u64> {
    crate::env::number("RIPCLONE_HEAD_REBASE_BYTES", 128 * 1024 * 1024)
}

/// How much larger than its compact archive, in percent, a Files archive
/// with tails may grow before the next build compacts it.
const FILES_TAIL_PERCENT: u64 = 25;
/// The zstd level of the Files archive.
const FILES_COMPRESSION_LEVEL: i32 = 6;

/// Head level packs past which Full packs a new base.
const MAX_HEAD_LEVEL_PACKS: usize = 64;

/// Tail bundles past which the next Files build compacts.
const MAX_FILES_TAILS: usize = 32;

/// Fail on a bad value of a setting the builder reads at build time.
pub(super) fn check_environment() -> Result<()> {
    lsm_max_levels()?;
    head_rebase_bytes()?;
    Ok(())
}

/// Whether a Files build may add a tail to `parent`, whose compact archive
/// is `compact`: while `parent` downloads less than `percent` more bytes
/// than `compact` and has fewer than [`MAX_FILES_TAILS`] tail bundles.
fn files_tail_fits(
    parent: &crate::FilesResult,
    compact: &crate::FilesResult,
    percent: u64,
) -> bool {
    let bytes = |files: &crate::FilesResult| -> u64 {
        files.archive_frames.iter().map(|f| f.compressed_len).sum()
    };
    let packed: std::collections::HashSet<&String> = compact.archive_chunks.iter().collect();
    let tails = parent
        .archive_chunks
        .iter()
        .filter(|hash| !packed.contains(hash))
        .count();
    u128::from(bytes(parent)) * 100 < u128::from(bytes(compact)) * u128::from(100 + percent)
        && tails < MAX_FILES_TAILS
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

/// Load and decode a prior build's metadata chunk. Bytes come from local CAS
/// or object storage (the disposable local cache may have been cleared after
/// a prior upload). Returns `None` on any failure; callers that only use it
/// to reuse work then build without it.
fn load_metadata(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    metadata_hash: &str,
) -> Option<crate::clonepack::MetadataChunk> {
    let bytes = cas
        .get(metadata_hash)
        .or_else(|_| storage.get(metadata_hash))
        .ok()?;
    crate::clonepack::MetadataChunk::decode_and_validate(bytes.as_slice()).ok()
}

/// [`load_metadata`] on the blocking pool.
async fn load_metadata_async(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    metadata_hash: &str,
) -> Option<crate::clonepack::MetadataChunk> {
    let (cas, storage, hash) = (cas.clone(), storage.clone(), metadata_hash.to_string());
    cancel::spawn_blocking(move || load_metadata(&cas, &storage, &hash))
        .await
        .ok()
        .flatten()
}

/// One manifest entry for `pack`, whose idx is at `idx_bundle_offset` in the
/// idx bundle when the manifest has one.
fn pack_entry(
    pack: &(String, u64, String, u64),
    history_only: bool,
    idx_bundle_offset: u64,
) -> Result<crate::clonepack::PackEntry> {
    Ok(crate::clonepack::PackEntry {
        pack: Some(ChunkRef {
            hash: hash_from_hex(&pack.0)?,
            len: pack.1,
        }),
        idx: Some(ChunkRef {
            hash: hash_from_hex(&pack.2)?,
            len: pack.3,
        }),
        history_only,
        idx_bundle_offset,
    })
}

/// Objects fetched from object storage at once by [`get_all_cached`].
const FETCH_THREADS: usize = 16;

/// The bytes of each of `hashes`, from the local CAS or else from object
/// storage, several at once. What storage returns is kept in the local CAS,
/// so a builder that did not make these objects fetches each one once.
fn get_all_cached(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    hashes: &[&str],
) -> Result<Vec<Vec<u8>>> {
    // Caching is not storing: no upload hook.
    let cache = cas.detached();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let threads = FETCH_THREADS.min(hashes.len());
    let fetched: Vec<Vec<(usize, Result<Vec<u8>>)>> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut out = Vec::new();
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some(hash) = hashes.get(i) else { break };
                        let bytes = cache.get(hash).or_else(|_| {
                            let bytes = storage.get(hash)?;
                            cache.put_with_hash(hash, &bytes)?;
                            anyhow::Ok(bytes)
                        });
                        out.push((i, bytes));
                    }
                    out
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("fetch worker panicked"))
            .collect()
    });
    let mut all: Vec<Option<Vec<u8>>> = vec![None; hashes.len()];
    for (i, bytes) in fetched.into_iter().flatten() {
        all[i] = Some(bytes.with_context(|| format!("read {}", hashes[i]))?);
    }
    all.into_iter()
        .zip(hashes)
        .map(|(bytes, hash)| bytes.with_context(|| format!("{hash} was not fetched")))
        .collect()
}

/// Build a Head's PackEntry list and the bundle of its packs' idx files.
fn assemble_head_packs(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    packs: &[(String, u64, String, u64)],
) -> Result<(Vec<crate::clonepack::PackEntry>, Option<ChunkRef>, String)> {
    if packs.is_empty() {
        return Ok((Vec::new(), None, String::new()));
    }
    use std::io::Write;

    let idx_hashes: Vec<&str> = packs.iter().map(|pack| pack.2.as_str()).collect();
    let idxs = get_all_cached(cas, storage, &idx_hashes)?;
    let mut bundle = cas.writer().context("create idx bundle")?;
    let mut entries = Vec::with_capacity(packs.len());
    let mut len = 0u64;
    for (pack, idx_bytes) in packs.iter().zip(idxs) {
        bundle
            .write_all(&idx_bytes)
            .context("write idx bytes to bundle")?;
        entries.push(pack_entry(pack, false, len)?);
        len += idx_bytes.len() as u64;
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

/// After upload, a remote backend may drop local copies, keeping `keep`:
/// the idx files used by later bundle and MIDX builds, the manifests and
/// metadata the next builds read, and the archive bundles the next Files
/// build reads unchanged frames from. Local storage
/// owns the durable copy and keeps every uploaded artifact. The local cache's
/// retention bounds what is kept.
async fn settle_storage(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    uploaded: Vec<String>,
    keep: std::collections::HashSet<String>,
) {
    if storage.is_remote() {
        let cas = cas.clone();
        let _ = tokio::task::spawn_blocking(move || {
            for h in uploaded.iter().filter(|h| !keep.contains(*h)) {
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

/// Result of the Head-closure build: a new level on the prior Head packs, or
/// a fresh full base on a cold sync. See `build_head_level` /
/// `build_head_packs`.
struct HeadBuild {
    /// Every current HEAD pack (base + levels), manifest order — for the clonepack.
    all_packs: Vec<(String, u64, String, u64)>,
    /// Only the packs built this sync (to upload). Reused base packs are durable.
    new_built: Vec<(String, u64, String, u64)>,
    /// The commit whose closure `base_packs` covers (carried, or = commit on cold).
    base_commit: String,
    /// The base packs (closure of `base_commit`), carried unchanged across levels.
    base_packs: Vec<crate::SizedPack>,
    /// The levels after the base, this sync's last.
    levels: Vec<crate::SizedPack>,
    /// True when every pack was built this sync (cold/rebase) → head MIDX buildable.
    all_local: bool,
    elapsed_ms: u64,
}

/// Build and upload the Head result for `commit`. Head packs are `base`, the
/// Head packs of an ancestor, plus one new level; without a base, or when
/// its packs cannot be read, the full closure is packed and becomes the
/// base.
pub(super) async fn build_head_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    commit: &str,
    parent: Option<String>,
    base: Option<crate::HeadBase>,
    storage: &crate::storage::StorageRef,
    phases: &mut SyncPhases,
) -> Result<crate::HeadResult> {
    let uploads = Uploads::new(cas, storage);
    let cas = &uploads.cas();

    // The prebuilt index, the Head packs and the files table each read the
    // commit's tree from the mirror, so they build at once.
    let t = Instant::now();
    let (md4, c4, cm4) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let prebuilt_index_handle = cancel::spawn_blocking(move || {
        let s = Instant::now();
        let index = PackBuilder::new(&md4, &c4).build_prebuilt_index(&cm4)?;
        anyhow::Ok((index, duration_ms(s.elapsed())))
    });
    // Head packs are a base plus levels. A Head on an earlier Head keeps
    // all its packs and adds one level: the depth-1 objects no earlier pack
    // has, read from their idx files. Packs stay disjoint by construction, so
    // no object is in two Head packs (which would double-materialize a
    // worktree file). A cold sync (no base) packs the full closure as the
    // base. Once the levels reach RIPCLONE_HEAD_REBASE_BYTES or a quarter of
    // the base, the Full build packs a new base for later Heads, off the
    // depth=1 critical path.
    let head_target = 4 * 1024 * 1024;
    let base = base.filter(|base| !base.commit.is_empty() && !base.packs.is_empty());
    let (md2, c2, s2, cm2) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        storage.clone(),
        commit.to_string(),
    );
    let head_handle = cancel::spawn_blocking(move || -> Result<HeadBuild> {
        let s = Instant::now();
        let b = PackBuilder::new(&md2, &c2);
        // Level path: a base exists. Prior packs whose idx files cannot be
        // read fall back to the cold path.
        let leveled = base.and_then(|base| {
            let level = (|| -> Result<_> {
                let idx: Vec<&str> = (base.packs.iter().chain(&base.levels))
                    .map(|pack| pack.idx.as_str())
                    .collect();
                let mut have = std::collections::HashSet::new();
                for bytes in get_all_cached(&c2, &s2, &idx)? {
                    have.extend(crate::pack::idx_object_ids(&bytes)?);
                }
                b.build_head_level(&cm2, &have, head_target)
            })();
            match level {
                Ok(level) => Some((base, level)),
                Err(e) => {
                    warn!(
                        "Head level on base {} failed ({e:#}); packing a full base",
                        &base.commit[..7.min(base.commit.len())]
                    );
                    None
                }
            }
        });
        match leveled {
            Some((base, level)) => {
                let mut levels = base.levels;
                levels.extend(level.iter().map(tuple_to_sized));
                let all_packs: Vec<_> = (base.packs.iter().chain(&levels))
                    .map(sized_to_tuple)
                    .collect();
                let elapsed = s.elapsed();
                info!(
                    "Head packs (level on base {}: {} new pack(s), {} total) {:?}",
                    &base.commit[..7.min(base.commit.len())],
                    level.len(),
                    all_packs.len(),
                    elapsed
                );
                Ok(HeadBuild {
                    all_packs,
                    new_built: level,
                    base_commit: base.commit,
                    base_packs: base.packs,
                    levels,
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
                    levels: Vec::new(),
                    all_local: true,
                    elapsed_ms: duration_ms(elapsed),
                })
            }
        }
    });
    // Head builds only the files table, from the tree alone: editable
    // depth=1 materializes the worktree from the Head packs, so it does not
    // need the archive.
    let (md3, cm3) = (mirror_dir.to_path_buf(), commit.to_string());
    let ft_start = Instant::now();
    let files_table_handle =
        cancel::spawn_blocking(move || ArchiveBuilder::new(&md3).build_files_table(&cm3));
    let head_built = head_handle.await.context("head packs")??;
    phases.head_packs_ms = Some(head_built.elapsed_ms);
    let head_packs = head_built.all_packs.clone();
    let metadata_base = files_table_handle.await.context("files table")??;
    phases.files_table_ms = Some(duration_ms(ft_start.elapsed()));
    info!("Head result: packs+files-table {:?}", t.elapsed());

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
    let (fpacks, all_built) = (head_packs.clone(), head_built.all_local);
    let (
        shallow_metadata_hash,
        shallow_clonepack_hash,
        head_idx_bundle_hash,
        head_midx_hash,
        head_metadata_len,
        head_idx_bundle_len,
    ) = cancel::spawn_blocking(
        move || -> Result<(String, String, String, String, u64, u64)> {
            let (cas, storage) = (&fc, &fs);
            let mut shallow_meta = metadata_base;
            shallow_meta.prebuilt_index = shallow_prebuilt_index;
            let shallow_meta_data = shallow_meta.encode_stored()?;
            let shallow_metadata_hash = cas.put(&shallow_meta_data)?;

            // Head has no archive frames. Files publishes its own archive result.
            let archive_chunks = archive_chunk_refs(&[], &shallow_meta)?;
            let (head_entries, head_idx_bundle_ref, head_idx_bundle_hash) =
                assemble_head_packs(cas, storage, &fpacks)?;
            let idx_bundle_len = head_idx_bundle_ref.as_ref().map_or(0, |r| r.len);
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
                shallow_meta_data.len() as u64,
                idx_bundle_len,
            ))
        },
    )
    .await
    .context("Head manifest task")??;
    phases.head_metadata_bytes = Some(head_metadata_len);
    phases.head_idx_bundle_bytes = Some(head_idx_bundle_len);
    phases.head_packs_bytes = Some(
        head_built
            .new_built
            .iter()
            .map(|(_, pack_len, _, idx_len)| pack_len + idx_len)
            .sum(),
    );

    let head = crate::HeadResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: shallow_clonepack_hash.clone(),
            metadata_chunk: shallow_metadata_hash.clone(),
            midx: head_midx_hash.clone(),
            idx_bundle: head_idx_bundle_hash.clone(),
            commit: commit.to_string(),
        },
        parent_commit: parent.clone(),
        packs: pack_artifacts_of(&head_packs),
        base_commit: head_built.base_commit.clone(),
        base_packs: head_built.base_packs.clone(),
        levels: head_built.levels.clone(),
    };

    // Upload Head artifacts (metadata, idx bundle, MIDX, manifest, and only
    // the freshly built head packs and idx).
    // Reused bucket packs are already durable in storage from a prior sync.
    let mut head_uploads: Vec<String> = vec![
        shallow_metadata_hash.clone(),
        shallow_clonepack_hash.clone(),
        head_idx_bundle_hash.clone(),
        head_midx_hash.clone(),
    ];
    for (p, _, i, _) in &head_built.new_built {
        head_uploads.push(p.clone());
        head_uploads.push(i.clone());
    }
    // Files and Full of this commit, and the next Head, read the manifest and
    // metadata; keeping them saves a download of what was just uploaded.
    let head_idx_keep: std::collections::HashSet<String> = head_packs
        .iter()
        .map(|(_, _, ih, _)| ih.clone())
        .chain([
            shallow_metadata_hash.clone(),
            shallow_clonepack_hash.clone(),
        ])
        .collect();
    Cancel::check_current()?;
    // Each upload started when its artifact was stored; wait for the rest.
    let upload_start = Instant::now();
    uploads.start_all(&head_uploads);
    let uploaded = uploads.finish().await?;
    phases.upload_head_bytes = Some(uploads.bytes());
    settle_storage(cas, storage, uploaded, head_idx_keep).await;
    phases.upload_head_ms = Some(duration_ms(upload_start.elapsed()));

    Ok(head)
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
/// When the Head levels have reached `RIPCLONE_HEAD_REBASE_BYTES`, the Head
/// packs are first rebased into a fresh base for `commit` (see
/// [`PackBuilder::rebase_head_packs`]). Full uses the rebased packs and
/// records them as `head_base`, so later Heads add levels on it. The published Head result never changes. A cold history build packs
/// the whole history as level 0: on the first Full, and when the tail from
/// the sealed tip cannot be listed.
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_full_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    commit: &str,
    head: &crate::HeadResult,
    mut previous_levels: Vec<crate::HistoryLevel>,
    storage: &crate::storage::StorageRef,
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

    let base_bytes: u64 = head.base_packs.iter().map(|pack| pack.pack_len).sum();
    let head_levels = &head_packs[head.base_packs.len().min(head_packs.len())..];
    let level_packs = head_levels.len();
    let delta_bytes: u64 = head_levels.iter().map(|(_, len, _, _)| *len).sum();
    let rebase_bytes = head_rebase_bytes()?.min(base_bytes / 4);
    let mut head_base = None;
    Cancel::check_current()?;
    if level_packs > 0 && (delta_bytes >= rebase_bytes || level_packs > MAX_HEAD_LEVEL_PACKS) {
        let start = Instant::now();
        let (rm, rc, rs, rcommit) = (
            mirror_dir.to_path_buf(),
            cas.clone(),
            storage.clone(),
            commit.to_string(),
        );
        let base_count = head.base_packs.len().min(head_packs.len());
        let current = head_packs.clone();
        let (compact_packs, kept) = cancel::spawn_blocking(move || {
            let builder = PackBuilder::new(&rm, &rc);
            // Packs whose idx files cannot be read are packed again whole.
            let idx: Vec<&str> = current[..base_count]
                .iter()
                .map(|(_, _, idx, _)| idx.as_str())
                .collect();
            match get_all_cached(&rc, &rs, &idx) {
                Ok(idx) => {
                    let base: Vec<_> = current[..base_count].iter().cloned().zip(idx).collect();
                    builder.rebase_head_packs(&rcommit, &base, 4 * 1024 * 1024)
                }
                Err(error) => {
                    warn!("Head base idx files unreadable ({error:#}); packing a full base");
                    Ok((builder.build_head_packs(&rcommit, 4 * 1024 * 1024)?, 0))
                }
            }
        })
        .await
        .context("compact Head packs")??;
        for (pack, _, idx, _) in &compact_packs[kept..] {
            uploads.start_all([pack, idx]);
        }
        let built_bytes: u64 = compact_packs[kept..]
            .iter()
            .map(|(_, len, _, _)| *len)
            .sum();
        info!(
            "compacted Head packs for {}@{} ({} MiB in levels; kept {} of {} base packs, \
             packed {} MiB in {} new)",
            repo_id.storage_key(),
            &commit[..7.min(commit.len())],
            delta_bytes / (1024 * 1024),
            kept,
            base_count,
            built_bytes / (1024 * 1024),
            compact_packs.len() - kept
        );
        head_base = Some(crate::HeadBase {
            commit: commit.to_string(),
            packs: compact_packs.iter().map(tuple_to_sized).collect(),
            levels: Vec::new(),
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
    let manifest_hash = cancel::spawn_blocking(move || -> Result<_> {
        // The history levels hold every object, so a full clone installs only
        // them, each with its own idx, and reads the HEAD packs for the
        // working tree alone: no idx bundle.
        let entries = vhead
            .iter()
            .map(|pack| pack_entry(pack, false, 0))
            .chain(vhistory.iter().map(|pack| pack_entry(pack, true, 0)))
            .collect::<Result<Vec<_>>>()?;
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
            None,
        )?;
        vc.put(&manifest.encode_to_vec())
    })
    .await
    .context("Full manifest task")??;
    phases.record("manifest", start);
    uploads.start_all([&manifest_hash]);
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
    phases.size(
        "history",
        new_history.iter().map(|(_, p, _, i)| p + i).sum(),
    );
    phases.size("upload", uploads.bytes());
    settle_storage(cas, storage, uploaded, keep_idx).await;
    phases.record("upload", start);

    let mut all_packs = head_packs;
    all_packs.extend(history_packs);
    Ok(crate::FullResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: manifest_hash,
            metadata_chunk: head.clonepack.metadata_chunk.clone(),
            midx: String::new(),
            idx_bundle: String::new(),
            commit: commit.to_string(),
        },
        packs: pack_artifacts_of(&all_packs),
        history_levels: levels,
        head_base,
    })
}

/// Build and upload the Files result. `parent_files`, a ready Files result of
/// an ancestor and its commit, lets the archive read only changed bytes: a
/// tail on it holds only the blobs it lacks, and a compaction (once the
/// tails grow past [`FILES_TAIL_PERCENT`]) reuses the unchanged frames
/// and bundles of its compact archive.
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_files_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    commit: &str,
    parent: Option<String>,
    parent_files: Option<(&str, &crate::FilesResult)>,
    storage: &crate::storage::StorageRef,
    phases: &mut BuildPhases,
) -> Result<crate::FilesResult> {
    let uploads = Uploads::new(cas, storage);
    let cas = &uploads.cas();
    let start = Instant::now();
    let previous = parent_files.map(|(_, files)| files);
    let compact = previous.map(|files| files.compact.as_deref().unwrap_or(files).clone());
    let tail = match (previous, &compact) {
        (Some(previous), Some(compact)) => files_tail_fits(previous, compact, FILES_TAIL_PERCENT),
        _ => false,
    };
    let base_files = if tail {
        previous.cloned()
    } else {
        compact.clone()
    };
    let base_metadata = match &base_files {
        Some(files) => load_metadata_async(cas, storage, &files.clonepack.metadata_chunk).await,
        None => None,
    };
    let base = base_metadata.zip(base_files);
    phases.record("base_files", start);

    let start = Instant::now();
    let (archive_mirror, archive_cas, archive_storage, archive_commit) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        storage.clone(),
        commit.to_string(),
    );
    let archive_output = cancel::spawn_blocking(move || {
        let base = base
            .as_ref()
            .map(|(metadata, files)| crate::archive::ArchiveBase {
                frames: &files.archive_frames,
                metadata,
                bundles: &files.archive_chunks,
            });
        let builder = ArchiveBuilder::new(&archive_mirror);
        if tail && let Some(base) = &base {
            match builder.build_tail_into_cas(
                &archive_commit,
                &archive_cas,
                FILES_COMPRESSION_LEVEL,
                base,
                crate::archive::DEFAULT_ARCHIVE_CHUNK_SIZE,
            ) {
                Ok(output) => return Ok(output),
                Err(e) => warn!("Files tail is unusable ({e:#}); building without it"),
            }
            return builder.build_into_cas(
                &archive_commit,
                &archive_cas,
                Some(&archive_storage),
                FILES_COMPRESSION_LEVEL,
                None,
                crate::archive::DEFAULT_ARCHIVE_CHUNK_SIZE,
            );
        }
        builder.build_into_cas(
            &archive_commit,
            &archive_cas,
            Some(&archive_storage),
            FILES_COMPRESSION_LEVEL,
            base.as_ref(),
            crate::archive::DEFAULT_ARCHIVE_CHUNK_SIZE,
        )
    })
    .await
    .context("Files archive task")??;
    phases.record("archive", start);
    info!("Files archive {:?}", archive_output.reuse);
    phases.size("read", archive_output.reuse.bytes_read);
    phases.size(
        "download",
        archive_output
            .archive_frames
            .iter()
            .map(|f| f.compressed_len)
            .sum(),
    );
    let compact = compact.filter(|_| archive_output.reuse.tail).map(Box::new);

    Cancel::check_current()?;
    let start = Instant::now();
    let archive_chunk_hashes = archive_output.download_bundle_hashes;
    let metadata = archive_output.metadata;
    let (fc, fcommit, fparent) = (cas.clone(), commit.to_string(), parent);
    let fchunks = archive_chunk_hashes.clone();
    let (metadata_hash, manifest_hash, metadata_len) =
        cancel::spawn_blocking(move || -> Result<_> {
            let metadata_bytes = metadata.encode_stored()?;
            let metadata_hash = fc.put(&metadata_bytes)?;
            let archive_chunks = archive_chunk_refs(&fchunks, &metadata)?;
            let manifest = make_manifest(
                &fcommit,
                &fparent,
                &archive_chunks,
                &metadata_hash,
                metadata_bytes.len() as u64,
                Vec::new(),
                None,
                None,
            )?;
            let manifest_hash = fc.put(&manifest.encode_to_vec())?;
            Ok((metadata_hash, manifest_hash, metadata_bytes.len() as u64))
        })
        .await
        .context("Files manifest task")??;
    phases.record("manifest", start);
    phases.size("metadata", metadata_len);

    // Reused bundles are the base's, already durable.
    let mut publish_uploads = vec![metadata_hash.clone(), manifest_hash.clone()];
    publish_uploads.extend(archive_output.new_bundle_hashes.iter().cloned());
    let start = Instant::now();
    uploads.start_all(&publish_uploads);
    let uploaded = uploads.finish().await?;
    phases.size("upload", uploads.bytes());
    let keep = archive_chunk_hashes
        .iter()
        .cloned()
        .chain([metadata_hash.clone()])
        .collect();
    settle_storage(cas, storage, uploaded, keep).await;
    phases.record("upload", start);

    Ok(crate::FilesResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: manifest_hash,
            metadata_chunk: metadata_hash,
            midx: String::new(),
            idx_bundle: String::new(),
            commit: commit.to_string(),
        },
        archive_chunks: archive_chunk_hashes,
        archive_frames: archive_output.archive_frames,
        compact,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn get_all_cached_fetches_in_order_and_keeps_what_storage_had() {
        let tmp = tempfile::tempdir().unwrap();
        let remote = Cas::new(tmp.path().join("remote")).unwrap();
        let storage: crate::storage::StorageRef =
            Arc::new(crate::storage::LocalStorage::new(tmp.path().join("remote")).unwrap());
        let stored = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen = stored.clone();
        let cas = Cas::new(tmp.path().join("local"))
            .unwrap()
            .with_on_store(Arc::new(move |hash: &str| {
                seen.lock().unwrap().push(hash.to_string())
            }));
        let local_only = cas.detached().put(b"local").unwrap();
        let objects: Vec<Vec<u8>> = (0..40).map(|i| format!("idx {i}").into_bytes()).collect();
        let mut hashes: Vec<String> = objects.iter().map(|o| remote.put(o).unwrap()).collect();
        hashes.insert(7, local_only.clone());

        let refs: Vec<&str> = hashes.iter().map(String::as_str).collect();
        let got = get_all_cached(&cas, &storage, &refs).unwrap();
        let mut want = objects.clone();
        want.insert(7, b"local".to_vec());
        assert_eq!(got, want);
        assert!(
            hashes.iter().all(|h| cas.has(h)),
            "fetched objects are cached"
        );
        assert!(stored.lock().unwrap().is_empty(), "caching uploads nothing");

        let missing = crate::cas::hash(b"nowhere");
        let err = get_all_cached(&cas, &storage, &[&hashes[0], &missing]).unwrap_err();
        assert!(format!("{err:#}").contains(&missing), "{err:#}");
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
