use crate::cas::Cas;
use crate::git;
use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub struct PackBuilder<'a> {
    repo: PathBuf,
    cas: &'a Cas,
}

/// Result of an LSM incremental build. Each pack is `(pack_hash, pack_len,
/// idx_hash, idx_len)`.
pub struct IncrementalPacks {
    /// Undeltified HEAD-closure packs (the worktree source).
    pub head_packs: Vec<(String, u64, String, u64)>,
    /// Deltified packs for the new commit range since the last sealed level.
    pub tail_packs: Vec<(String, u64, String, u64)>,
    /// Total raw (uncompressed) size of the tail objects, for sealing decisions.
    pub tail_raw_bytes: u64,
}

/// Result of compacting LSM levels: the new (bounded) level set, plus the pack
/// tuples that were freshly built by the merges (to upload). Packs that were
/// merged away are now unreferenced and left for GC.
pub struct CompactResult {
    pub levels: Vec<crate::HistoryLevel>,
    pub new_packs: Vec<(String, u64, String, u64)>,
}

impl<'a> PackBuilder<'a> {
    pub fn new<P: AsRef<Path>>(repo: P, cas: &'a Cas) -> Self {
        Self {
            repo: repo.as_ref().to_path_buf(),
            cas,
        }
    }

    /// Build a packfile + idx containing commit + all reachable trees + symlink blobs.
    /// Symlink blobs are small and let `git status` verify symlinks without
    /// fetching them lazily.
    pub fn build_skeleton_pack(&self, commit: &str) -> Result<(String, String)> {
        self.build_skeleton_pack_with_depth(commit, None)
    }

    /// Build a skeleton pack limited to `max_depth` commits. `max_depth = None`
    /// is the full history; `Some(1)` produces a depth=1 skeleton.
    pub fn build_skeleton_pack_with_depth(
        &self,
        commit: &str,
        max_depth: Option<usize>,
    ) -> Result<(String, String)> {
        let shas = git::list_object_shas_with_depth(&self.repo, commit, max_depth)?;
        let types = git::classify_objects(&self.repo, &shas.iter().cloned().collect())?;
        let mut skeleton_shas: Vec<String> = shas
            .into_iter()
            .filter(|sha| {
                matches!(
                    types.get(sha).map(|s| s.as_str()),
                    Some("commit") | Some("tree")
                )
            })
            .collect();

        let symlink_shas = git::symlink_blob_shas(&self.repo, commit)?;
        skeleton_shas.extend(symlink_shas);

        self.pack_and_index(&skeleton_shas)
    }

    /// Build a depth=1 skeleton pack (single commit + HEAD trees + symlinks).
    pub fn build_shallow_skeleton_pack(&self, commit: &str) -> Result<(String, String)> {
        self.build_skeleton_pack_with_depth(commit, Some(1))
    }

    /// Build a packfile + idx containing all objects reachable from a commit.
    pub fn build_full_pack(&self, commit: &str) -> Result<(String, String)> {
        let shas = git::list_object_shas(&self.repo, commit)?;
        self.pack_and_index(&shas)
    }

    /// Build a delta skeleton pack: objects not present in parent commit's skeleton.
    pub fn build_delta_skeleton_pack(
        &self,
        commit: &str,
        parent_commit: &str,
    ) -> Result<(String, String)> {
        let commit_shas = git::list_object_shas(&self.repo, commit)?;
        let parent_shas: HashSet<String> = git::list_object_shas(&self.repo, parent_commit)?
            .into_iter()
            .collect();

        let types = git::classify_objects(&self.repo, &commit_shas.iter().cloned().collect())?;

        let delta_shas: Vec<String> = commit_shas
            .into_iter()
            .filter(|sha| {
                !parent_shas.contains(sha)
                    && matches!(
                        types.get(sha).map(|s| s.as_str()),
                        Some("commit") | Some("tree")
                    )
            })
            .collect();

        self.pack_and_index(&delta_shas)
    }

    /// Build a packfile + idx containing every blob reachable from `commit`.
    /// This is the "head-blobs" pack: it gives the client all HEAD content so
    /// `git diff`, `git show`, and edits work immediately without further
    /// object downloads.
    pub fn build_head_blobs_pack(&self, commit: &str) -> Result<(String, String)> {
        let entries = git::list_tree_entries(&self.repo, commit)?;
        let blob_shas: Vec<String> = entries
            .into_iter()
            .filter(|(_, _, _, obj_type)| obj_type == "blob")
            .map(|(_, _, sha, _)| sha)
            .collect();
        self.pack_and_index(&blob_shas)
    }

    /// Build a ready-to-use `.git/index` from the skeleton pack.
    ///
    /// The index contains every tracked path with `skip-worktree` set and
    /// accurate cached blob sizes. After the client materializes files it must
    /// clear skip-worktree for those paths.
    pub fn build_prebuilt_index(&self, commit: &str, skeleton_pack_hash: &str) -> Result<String> {
        let tmp = tempfile::TempDir::new()?;
        let work = tmp.path();
        let git_dir = work.join(".git");

        git::init(work)?;
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;

        let pack_data = self
            .cas
            .get(skeleton_pack_hash)
            .context("fetch skeleton pack for index build")?;
        let pack_path = pack_dir.join("skeleton.pack");
        std::fs::write(&pack_path, &pack_data)?;
        git::index_pack(&git_dir, &pack_path)?;

        git::set_head(&git_dir, commit)?;
        git::read_tree(&git_dir, commit)?;

        let sizes = git::ls_tree_sizes(&self.repo, commit)?;
        git::update_index_sizes(&git_dir, &sizes)?;

        // Mark every tracked path as skip-worktree. The client clears this bit
        // for files it actually materializes from the archive.
        git::set_skip_worktree_all(work)?;

        let index_bytes = std::fs::read(git_dir.join("index")).context("read prebuilt index")?;
        self.cas.put(&index_bytes)
    }

    /// Ensure all blobs reachable from a commit are stored in the CAS.
    pub fn store_blobs(&self, commit: &str) -> Result<usize> {
        let shas = git::list_object_shas(&self.repo, commit)?;
        let types = git::classify_objects(&self.repo, &shas.iter().cloned().collect())?;
        let mut count = 0;
        for sha in shas {
            if matches!(types.get(&sha).map(|s| s.as_str()), Some("blob")) {
                let content = git::cat_file(&self.repo, &sha)?;
                self.cas.put_with_hash(&sha, &content)?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Build a complete object pack for `commit` limited to the last `depth`
    /// commits (`None` = full history). The pack contains commits, trees, and
    /// every blob reachable from those commits — i.e. everything needed to
    /// materialize the working tree at HEAD and inspect the included history.
    ///
    /// Objects are stored undeltified (`git pack-objects --window=0`) so the
    /// client can extract files without resolving deltas.
    pub fn build_depth_pack(&self, commit: &str, depth: Option<usize>) -> Result<(String, String)> {
        let object_shas = git::list_object_shas_with_depth(&self.repo, commit, depth)?;
        let (ph, _, ih, _) = self.pack_and_index_inner(&object_shas, true)?;
        Ok((ph, ih))
    }

    /// Build the depth content as many small, self-contained git packs, each
    /// targeting roughly `target_raw_bytes` of uncompressed object content.
    /// Objects are partitioned greedily by raw size so packs are evenly sized
    /// (~1-2 MB compressed for a few-MB raw target); each pack is independently
    /// valid (non-thin), so the client can install and extract them in parallel
    /// as they download.
    ///
    /// Returns `(pack_hash, pack_len, idx_hash, idx_len)` for each pack.
    pub fn build_depth_packs(
        &self,
        commit: &str,
        depth: Option<usize>,
        target_raw_bytes: u64,
    ) -> Result<Vec<(String, u64, String, u64)>> {
        let oids = git::list_object_shas_with_depth(&self.repo, commit, depth)?;
        if oids.is_empty() {
            bail!("no objects to pack for {}", commit);
        }
        Ok(self.build_packs_from_oids(&oids, target_raw_bytes, true)?.0)
    }

    /// Build the depth content as two layers of mini-packs:
    ///   - `head`: the HEAD-tree closure (commit + trees + every *current* blob).
    ///     Needed by every clone depth; this is the working tree.
    ///   - `history`: every other reachable object (old blob versions, ancestor
    ///     commits/trees) — i.e. full history minus the HEAD closure. Needed only
    ///     for deeper clones.
    ///
    /// A depth=1 clonepack lists only `head`; a full (depth=0) clonepack lists
    /// `head` + `history`. The packs are content-addressed, so the depth=1 set is
    /// literally a subset of the full set — no separate HEAD pack is built.
    ///
    /// `history` is empty for a single-commit repo. Note: `history` is only as
    /// complete as the mirror's available history (deepen/unshallow the mirror
    /// for a true full clone).
    /// Build the two pack buckets for `commit`:
    /// - HEAD closure: undeltified, partitioned at `head_target_raw_bytes` into
    ///   many small packs the client downloads in parallel and hand-parses.
    /// - History (everything else): deltified, partitioned at the much larger
    ///   `history_target_raw_bytes` into a *handful* of packs. The client only
    ///   installs these (git reads them), so they must be few and large — using
    ///   the small HEAD target here would explode a big repo into ~1k packs and
    ///   ~1k `git pack-objects` spawns (observed: bun = 6.2 GiB raw → 1058 packs,
    ///   26-minute build).
    pub fn build_layered_packs(
        &self,
        commit: &str,
        head_target_raw_bytes: u64,
        history_target_raw_bytes: u64,
    ) -> Result<(
        Vec<(String, u64, String, u64)>,
        Vec<(String, u64, String, u64)>,
    )> {
        use std::collections::HashSet;
        let head_oids = git::list_object_shas_with_depth(&self.repo, commit, Some(1))?;
        if head_oids.is_empty() {
            bail!("no objects to pack for {}", commit);
        }
        let head_set: HashSet<&str> = head_oids.iter().map(String::as_str).collect();
        let all_oids = git::list_object_shas_with_depth(&self.repo, commit, None)?;
        let history_oids: Vec<String> = all_oids
            .into_iter()
            .filter(|o| !head_set.contains(o.as_str()))
            .collect();

        // HEAD closure: undeltified so the client can hand-parse blobs straight
        // from the downloaded bytes for the working tree.
        let (head_packs, _) =
            self.build_packs_from_oids(&head_oids, head_target_raw_bytes, true)?;
        // History: deltified (undeltified history is multi-GB). The client never
        // hand-parses these — they're only installed for the object DB and git
        // reads them, resolving deltas itself. Few large packs, not many small
        // ones.
        let history_packs = if history_oids.is_empty() {
            Vec::new()
        } else {
            self.build_packs_from_oids(&history_oids, history_target_raw_bytes, false)?
                .0
        };
        Ok((head_packs, history_packs))
    }

    /// Build just the HEAD closure (undeltified small packs) — the depth=1
    /// payload. Used by two-phase publish to get a clonable depth=1 fast, before
    /// the (slow) history packs are built.
    pub fn build_head_packs(
        &self,
        commit: &str,
        head_target_raw_bytes: u64,
    ) -> Result<Vec<(String, u64, String, u64)>> {
        let head_oids = git::list_object_shas_with_depth(&self.repo, commit, Some(1))?;
        if head_oids.is_empty() {
            bail!("no objects to pack for {}", commit);
        }
        Ok(self
            .build_packs_from_oids(&head_oids, head_target_raw_bytes, true)?
            .0)
    }

    /// Build just the history packs (deltified, everything reachable from
    /// `commit` minus the HEAD closure). Used by two-phase publish in the
    /// background after the depth=1 clonepack is already published.
    pub fn build_history_packs(
        &self,
        commit: &str,
        history_target_raw_bytes: u64,
    ) -> Result<Vec<(String, u64, String, u64)>> {
        use std::collections::HashSet;
        let head_oids = git::list_object_shas_with_depth(&self.repo, commit, Some(1))?;
        let head_set: HashSet<&str> = head_oids.iter().map(String::as_str).collect();
        let all_oids = git::list_object_shas_with_depth(&self.repo, commit, None)?;
        let history_oids: Vec<String> = all_oids
            .into_iter()
            .filter(|o| !head_set.contains(o.as_str()))
            .collect();
        if history_oids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .build_packs_from_oids(&history_oids, history_target_raw_bytes, false)?
            .0)
    }

    /// LSM incremental build. Packs the HEAD closure (undeltified, for the
    /// worktree) and the *tail* — the objects introduced in `(sealed_tip, commit]`
    /// (deltified, for the object DB). When `sealed_tip` is `None` the tail is the
    /// whole history.
    ///
    /// Unlike [`build_layered_packs`], the tail is NOT reduced by the HEAD closure:
    /// a sealed level must hold the full range so it stays correct as an immutable
    /// artifact even after the HEAD closure changes in later syncs. Current blobs
    /// therefore appear in both the (undeltified) HEAD packs and the (deltified)
    /// history packs; git dedups by OID on read. Returns the tail's total raw size
    /// so the caller can decide whether to seal it into a new level.
    pub fn build_incremental_packs(
        &self,
        commit: &str,
        sealed_tip: Option<&str>,
        head_target_raw_bytes: u64,
        history_target_raw_bytes: u64,
    ) -> Result<IncrementalPacks> {
        let head_oids = git::list_object_shas_with_depth(&self.repo, commit, Some(1))?;
        if head_oids.is_empty() {
            bail!("no objects to pack for {}", commit);
        }
        let tail_oids = git::list_object_shas_in_range(&self.repo, sealed_tip, commit)?;

        let (head_packs, _) =
            self.build_packs_from_oids(&head_oids, head_target_raw_bytes, true)?;
        let (tail_packs, tail_raw_bytes) = if tail_oids.is_empty() {
            (Vec::new(), 0)
        } else {
            self.build_packs_from_oids(&tail_oids, history_target_raw_bytes, false)?
        };
        Ok(IncrementalPacks {
            head_packs,
            tail_packs,
            tail_raw_bytes,
        })
    }

    /// Build only the deltified *tail* — objects introduced in `(sealed_tip,
    /// commit]` (the whole history when `sealed_tip` is `None`). This is the
    /// history half of [`build_incremental_packs`], for callers (two-phase
    /// phase 2) that already built the HEAD closure earlier. Returns the tail
    /// packs and their total raw size (for sealing decisions).
    pub fn build_history_tail(
        &self,
        commit: &str,
        sealed_tip: Option<&str>,
        history_target_raw_bytes: u64,
    ) -> Result<(Vec<(String, u64, String, u64)>, u64)> {
        let tail_oids = git::list_object_shas_in_range(&self.repo, sealed_tip, commit)?;
        if tail_oids.is_empty() {
            return Ok((Vec::new(), 0));
        }
        self.build_packs_from_oids(&tail_oids, history_target_raw_bytes, false)
    }

    /// Size-tiered compaction of LSM history levels. Levels are ordered oldest →
    /// newest, each covering the commit range `(prev_level.tip, this.tip]`. While
    /// there are more than `max_levels`, the adjacent pair with the smallest
    /// combined byte size is merged: their union range is re-packed into one new
    /// level. Choosing the smallest pair keeps the large base level untouched —
    /// only small recent tails are merged — so each compaction re-deltifies a
    /// small range, not the whole history.
    pub fn compact_levels(
        &self,
        mut levels: Vec<crate::HistoryLevel>,
        max_levels: usize,
        history_target_raw_bytes: u64,
    ) -> Result<CompactResult> {
        let mut new_packs = Vec::new();
        let level_bytes =
            |l: &crate::HistoryLevel| -> u64 { l.packs.iter().map(|p| p.pack_len).sum() };
        while levels.len() > max_levels.max(1) && levels.len() >= 2 {
            // Adjacent pair (best, best+1) with the smallest combined size.
            let mut best = 0usize;
            let mut best_bytes = u64::MAX;
            for i in 0..levels.len() - 1 {
                let b = level_bytes(&levels[i]) + level_bytes(&levels[i + 1]);
                if b < best_bytes {
                    best_bytes = b;
                    best = i;
                }
            }
            // Merged range: (start, tip] where start is the tip of the level
            // before `best` (None when best is the base), tip is level[best+1].
            let start = best.checked_sub(1).map(|i| levels[i].tip_commit.clone());
            let tip = levels[best + 1].tip_commit.clone();
            let oids = git::list_object_shas_in_range(&self.repo, start.as_deref(), &tip)?;
            let packs = if oids.is_empty() {
                Vec::new()
            } else {
                self.build_packs_from_oids(&oids, history_target_raw_bytes, false)?
                    .0
            };
            new_packs.extend(packs.iter().cloned());
            let merged = crate::HistoryLevel {
                tip_commit: tip,
                packs: packs
                    .iter()
                    .map(|p| crate::SizedPack {
                        pack: p.0.clone(),
                        pack_len: p.1,
                        idx: p.2.clone(),
                        idx_len: p.3,
                    })
                    .collect(),
            };
            levels.splice(best..best + 2, [merged]);
        }
        Ok(CompactResult { levels, new_packs })
    }

    /// Partition `oids` greedily by raw size into `~target_raw_bytes` batches and
    /// pack each as a self-contained mini-pack. Returns the per-pack
    /// `(pack_hash, pack_len, idx_hash, idx_len)` tuples plus the total raw
    /// (uncompressed) size of all `oids` (so callers can make sealing decisions
    /// without a second `object_sizes` pass).
    fn build_packs_from_oids(
        &self,
        oids: &[String],
        target_raw_bytes: u64,
        undeltified: bool,
    ) -> Result<(Vec<(String, u64, String, u64)>, u64)> {
        if oids.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let sizes = git::object_sizes(&self.repo, oids)?;
        let total_raw: u64 = oids
            .iter()
            .map(|o| sizes.get(o).copied().unwrap_or(0))
            .sum();

        // Greedy size-based partitioning. A single object larger than the target
        // gets its own pack (we can't split one object across packs).
        let target = target_raw_bytes.max(1);
        let mut batches: Vec<Vec<String>> = Vec::new();
        let mut cur: Vec<String> = Vec::new();
        let mut cur_bytes = 0u64;
        for oid in oids {
            let sz = sizes.get(oid).copied().unwrap_or(0);
            if !cur.is_empty() && cur_bytes + sz > target {
                batches.push(std::mem::take(&mut cur));
                cur_bytes = 0;
            }
            cur.push(oid.clone());
            cur_bytes += sz;
        }
        if !cur.is_empty() {
            batches.push(cur);
        }

        // `undeltified` (`--window=0`): each object stored whole so the client
        // can hand-parse blobs from the bytes (HEAD closure). Deltified packs are
        // compact and only installed for the object DB (history); git resolves
        // their deltas. Each pack is self-contained (non-thin).
        //
        // Undeltified packs are single-threaded + CPU-light, so pack the batches
        // across cores. Deltified `pack-objects` is already git-internally
        // threaded, so we keep those serial to avoid CPU oversubscription.
        let packs: Vec<(String, u64, String, u64)> = if undeltified && batches.len() > 1 {
            use rayon::prelude::*;
            batches
                .par_iter()
                .map(|batch| self.pack_and_index_inner(batch, undeltified))
                .collect::<Result<Vec<_>>>()?
        } else {
            batches
                .iter()
                .map(|batch| self.pack_and_index_inner(batch, undeltified))
                .collect::<Result<Vec<_>>>()?
        };
        Ok((packs, total_raw))
    }

    fn pack_and_index(&self, object_shas: &[String]) -> Result<(String, String)> {
        let (ph, _, ih, _) = self.pack_and_index_inner(object_shas, false)?;
        Ok((ph, ih))
    }

    /// Pack `object_shas` and store the pack + idx in the CAS. Returns
    /// `(pack_hash, pack_len, idx_hash, idx_len)` — the lengths come straight
    /// from the bytes we already read, so callers never re-`get` from the CAS.
    fn pack_and_index_inner(
        &self,
        object_shas: &[String],
        undeltified: bool,
    ) -> Result<(String, u64, String, u64)> {
        if object_shas.is_empty() {
            bail!("no objects to pack");
        }

        let tmp = tempfile::TempDir::new()?;
        let prefix = tmp.path().join("pack");
        if undeltified {
            git::pack_objects_undeltified_to_prefix(&self.repo, object_shas, &prefix)?;
        } else {
            git::pack_objects_to_prefix(&self.repo, object_shas, &prefix)?;
        }

        let mut pack_path: Option<PathBuf> = None;
        let mut idx_path: Option<PathBuf> = None;
        for entry in std::fs::read_dir(tmp.path())? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "pack" {
                    pack_path = Some(path);
                } else if ext == "idx" {
                    idx_path = Some(path);
                }
            }
        }

        let pack_path = pack_path.context("missing generated pack file")?;
        let idx_path = idx_path.context("missing generated idx file")?;

        let pack_data = std::fs::read(&pack_path)?;
        let idx_data = std::fs::read(&idx_path)?;
        let pack_len = pack_data.len() as u64;
        let idx_len = idx_data.len() as u64;
        let pack_hash = self.cas.put(&pack_data)?;
        let idx_hash = self.cas.put(&idx_data)?;

        Ok((pack_hash, pack_len, idx_hash, idx_len))
    }
}
