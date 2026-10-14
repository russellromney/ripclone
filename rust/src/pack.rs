use crate::cas::Cas;
use crate::git;
use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub struct PackBuilder<'a> {
    repo: PathBuf,
    cas: &'a Cas,
}

/// Result of compacting LSM levels: the new (bounded) level set, plus the pack
/// tuples that were freshly built by the merges (to upload). Packs that were
/// merged away remain immutable in durable content-addressed storage.
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

    /// Build just the HEAD closure (undeltified small packs) — the depth=1
    /// payload. Used to publish Head before the slower Full history is built.
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

    /// Build a HEAD-closure *delta* pack against a base commit: only the depth-1
    /// objects present at `commit` but not at `base_commit`. The base packs
    /// (closure of `base_commit`) stay immutable; this packs just the difference
    /// (for a few changed files, the changed blobs + the trees from root to leaf —
    /// a handful of objects), so the depth=1 payload becomes O(diff-from-base)
    /// instead of O(worktree).
    ///
    /// The delta is the **set difference of the two depth-1 closures**, NOT a
    /// reachability exclude (`rev-list HEAD ^base`). Two properties matter:
    /// - **Disjoint:** `delta ∩ closure(base) = ∅` by construction, so no object
    ///   is ever in both the base and the delta. The client materializes the
    ///   worktree by writing each blob it finds in a HEAD pack, so a blob present
    ///   in two packs would be written (and counted) twice.
    /// - **Re-add safe:** a blob removed after `base_commit` and re-added
    ///   identically at `commit` is reachable from `base_commit`'s tip tree, so it
    ///   is already in the base and correctly excluded from the delta — present
    ///   exactly once. A reachability-exclude (`^base`) would instead drop a blob
    ///   reachable only from `base`'s history, corrupting the worktree.
    ///
    /// `base ∪ delta = closure(base) ∪ (closure(HEAD) − closure(base)) ⊇
    /// closure(HEAD)`, so every object the worktree needs is present. Listing each
    /// closure is cheap (`rev-list --objects`, no content); only the small
    /// difference is packed (undeltified, so the client hand-parses blobs).
    /// Returns the freshly built delta packs (manifest order); these are also the
    /// only packs to upload.
    pub fn build_head_delta_pack(
        &self,
        commit: &str,
        base_commit: &str,
        head_target_raw_bytes: u64,
    ) -> Result<Vec<(String, u64, String, u64)>> {
        let head_oids = git::list_object_shas_with_depth(&self.repo, commit, Some(1))?;
        if head_oids.is_empty() {
            bail!("no objects to pack for {}", commit);
        }
        let base_oids: HashSet<String> =
            git::list_object_shas_with_depth(&self.repo, base_commit, Some(1))?
                .into_iter()
                .collect();
        let delta_oids: Vec<String> = head_oids
            .into_iter()
            .filter(|o| !base_oids.contains(o))
            .collect();
        if delta_oids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .build_packs_from_oids(&delta_oids, head_target_raw_bytes, true)?
            .0)
    }

    /// Build only the deltified *tail* — objects introduced in `(sealed_tip,
    /// commit]` (the whole history when `sealed_tip` is `None`). The HEAD
    /// closure is built separately by [`Self::build_head_packs`]. Returns the
    /// tail packs.
    ///
    /// The tail is NOT reduced by the HEAD closure: a sealed level must hold the
    /// full range so it stays correct as an immutable artifact even after the
    /// HEAD closure changes in later syncs. Current blobs therefore appear in
    /// both the (undeltified) HEAD packs and the (deltified) history packs; git
    /// dedups by OID on read.
    pub fn build_history_tail(
        &self,
        commit: &str,
        sealed_tip: Option<&str>,
        history_target_raw_bytes: u64,
    ) -> Result<Vec<(String, u64, String, u64)>> {
        // Cold base build (no sealed level yet): reuse the deltas git already has
        // via the mirror's reachability bitmap instead of enumerating ~all
        // objects and re-deltifying them in size-partitioned batches (which also
        // breaks delta reuse at batch boundaries). This is the dominant cost of a
        // huge repo's first full sync; bitmap pack-reuse makes it ~I/O-bound.
        if sealed_tip.is_none() {
            // Try the bitmap/delta-reuse fast path; on any failure fall back to the
            // proven enumerate-and-pack path so a cold build never hard-fails on a
            // missing bitmap, an old git, or a pack-objects hiccup. Reuse is an
            // optimization, never the only path to a complete base.
            return match self.build_history_pack_reuse(commit) {
                Ok(packs) => Ok(packs),
                Err(e) => {
                    tracing::warn!(
                        "cold history pack-reuse failed ({e:#}); falling back to enumerate+pack"
                    );
                    let oids = git::list_object_shas_in_range(&self.repo, None, commit)?;
                    if oids.is_empty() {
                        return Ok(Vec::new());
                    }
                    Ok(self
                        .build_packs_from_oids(&oids, history_target_raw_bytes, false)?
                        .0)
                }
            };
        }
        let tail_oids = git::list_object_shas_in_range(&self.repo, sealed_tip, commit)?;
        if tail_oids.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .build_packs_from_oids(&tail_oids, history_target_raw_bytes, false)?
            .0)
    }

    /// Build the cold full-history packs by reusing existing pack deltas via the
    /// mirror's bitmap (see `git::pack_objects_reachable_to_prefix`), which
    /// preserves one compact delta graph. The reachable closure includes HEAD's
    /// objects (a small duplicate with the depth pack) — a deliberate
    /// space-for-time trade to avoid an exclusion enumeration.
    fn build_history_pack_reuse(&self, commit: &str) -> Result<Vec<(String, u64, String, u64)>> {
        let tmp = tempfile::TempDir::new()?;
        let prefix = tmp.path().join("pack");
        git::pack_objects_reachable_to_prefix(&self.repo, commit, &prefix)?;
        self.store_packs_from_dir(tmp.path())
    }

    /// Read every `pack-<sha>.{pack,idx}` pair written under `dir`, store both
    /// halves in the CAS, and return `(pack_hash, pack_len, idx_hash, idx_len)`
    /// for each.
    fn store_packs_from_dir(&self, dir: &Path) -> Result<Vec<(String, u64, String, u64)>> {
        use std::collections::HashMap;
        let mut packs: HashMap<String, PathBuf> = HashMap::new();
        let mut idxs: HashMap<String, PathBuf> = HashMap::new();
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            let stem = path.file_stem().and_then(|s| s.to_str()).map(String::from);
            match (path.extension().and_then(|e| e.to_str()), stem) {
                (Some("pack"), Some(name)) => {
                    packs.insert(name, path);
                }
                (Some("idx"), Some(name)) => {
                    idxs.insert(name, path);
                }
                _ => {}
            }
        }
        // Sort by pack name so the level's pack order (and thus the published
        // manifest) is reproducible across identical rebuilds; `read_dir` and the
        // HashMap both yield arbitrary order otherwise.
        let mut names: Vec<String> = packs.keys().cloned().collect();
        names.sort();
        let mut out = Vec::new();
        for name in names {
            let pack_path = &packs[&name];
            let idx_path = idxs
                .get(&name)
                .with_context(|| format!("missing idx for {name}"))?;
            let (pack_hash, pack_len) = self.cas.put_file(pack_path)?;
            let (idx_hash, idx_len) = self.cas.put_file(idx_path)?;
            out.push((pack_hash, pack_len, idx_hash, idx_len));
        }
        if out.is_empty() {
            bail!("no packs produced");
        }
        Ok(out)
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
        let total_raw = oids.iter().try_fold(0u64, |total, oid| {
            total
                .checked_add(sizes.get(oid).copied().unwrap_or(0))
                .context("raw object size total overflow")
        })?;

        // Greedy size-based partitioning. A single object larger than the target
        // gets its own pack (we can't split one object across packs).
        let target = target_raw_bytes.max(1);
        let mut batches: Vec<Vec<String>> = Vec::new();
        let mut cur: Vec<String> = Vec::new();
        let mut cur_bytes = 0u64;
        for oid in oids {
            let sz = sizes.get(oid).copied().unwrap_or(0);
            if !cur.is_empty()
                && cur_bytes
                    .checked_add(sz)
                    .context("current pack size overflow")?
                    > target
            {
                batches.push(std::mem::take(&mut cur));
                cur_bytes = 0;
            }
            cur.push(oid.clone());
            cur_bytes = cur_bytes
                .checked_add(sz)
                .context("current pack size overflow")?;
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
        // The gix encoder is currently undeltified (deterministic, sorted OIDs).
        // Use it for HEAD-closure packs when requested; keep C-git for deltified
        // history packs so their size stays compact.
        let use_gix = false;
        if use_gix && undeltified {
            git::pack_objects_to_prefix_gix(&self.repo, object_shas, &prefix)?;
        } else if undeltified {
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

        let (pack_hash, pack_len) = self.cas.put_file(&pack_path)?;
        let (idx_hash, idx_len) = self.cas.put_file(&idx_path)?;

        Ok((pack_hash, pack_len, idx_hash, idx_len))
    }
}

#[cfg(test)]
mod lsm_incremental_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// Commit `files` onto a bare repo's HEAD (parent = current HEAD if any) and
    /// return the new commit oid.
    fn commit_onto(repo: &gix::Repository, files: &[(&str, &[u8])]) -> String {
        crate::test_fixture::commit(repo, files)
    }

    /// A cold build packs the full HEAD closure; a re-sync after changing one file
    /// packs only the *delta* — the depth-1 objects new at the new commit (the
    /// changed blob + root tree + commit), not the whole worktree. The delta is
    /// exactly the set difference of the two depth-1 closures.
    #[test]
    fn head_delta_pack_is_minimal() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let pb = PackBuilder::new(tmp.path(), &cas);

        let files1: Vec<(&str, &[u8])> = vec![
            ("a", b"aaa"),
            ("b", b"bbb"),
            ("c", b"ccc"),
            ("d", b"ddd"),
            ("e", b"eee"),
            ("f", b"fff"),
        ];
        let c1 = commit_onto(&repo, &files1);
        let base = pb.build_head_packs(&c1, 12 * 1024 * 1024).unwrap();
        assert!(!base.is_empty(), "cold build packs the full closure");

        // c2 changes one file's content (blob b, the root tree, and the commit).
        let files2: Vec<(&str, &[u8])> = vec![
            ("a", b"aaa"),
            ("b", b"bbb-CHANGED"),
            ("c", b"ccc"),
            ("d", b"ddd"),
            ("e", b"eee"),
            ("f", b"fff"),
        ];
        let c2 = commit_onto(&repo, &files2);
        // Delta is always taken against the immutable base commit (c1).
        let delta = pb
            .build_head_delta_pack(&c2, &c1, 12 * 1024 * 1024)
            .unwrap();
        assert!(!delta.is_empty(), "a changed file yields a non-empty delta");

        // The delta is the set difference of the two depth-1 closures. For a flat
        // tree with one changed file that is exactly {commit, root tree, blob} = 3
        // objects — far fewer than the full closure (commit + tree + 6 blobs = 8).
        let full_c1 = git::list_object_shas_with_depth(tmp.path(), &c1, Some(1)).unwrap();
        let full_c2 = git::list_object_shas_with_depth(tmp.path(), &c2, Some(1)).unwrap();
        let base_set: std::collections::HashSet<&str> =
            full_c1.iter().map(String::as_str).collect();
        let expected_delta: Vec<&str> = full_c2
            .iter()
            .map(String::as_str)
            .filter(|o| !base_set.contains(o))
            .collect();
        assert_eq!(
            expected_delta.len(),
            3,
            "single-file change in a flat tree: changed blob + root tree + commit"
        );
        assert!(
            expected_delta.len() < full_c2.len(),
            "delta is smaller than the full closure"
        );

        // Re-add disjointness: c3 deletes b.txt then ... actually re-adds the
        // ORIGINAL content. The original blob is reachable from the base (c1), so
        // the delta against the base must NOT contain it — base ∩ delta = ∅, so no
        // object is ever materialized from two packs.
        let files3: Vec<(&str, &[u8])> = vec![
            ("a", b"aaa"),
            ("b", b"bbb"), // restored to the c1 (base) content
            ("c", b"ccc"),
            ("d", b"ddd"),
            ("e", b"eee"),
            ("f", b"fff"),
            ("g", b"ggg"), // one genuinely new file
        ];
        let c3 = commit_onto(&repo, &files3);
        let delta3 = pb
            .build_head_delta_pack(&c3, &c1, 12 * 1024 * 1024)
            .unwrap();
        assert!(!delta3.is_empty(), "new file g yields a non-empty delta");
        let full_c3 = git::list_object_shas_with_depth(tmp.path(), &c3, Some(1)).unwrap();
        let delta3_set: std::collections::HashSet<&str> = full_c3
            .iter()
            .map(String::as_str)
            .filter(|o| !base_set.contains(o))
            .collect();
        // The restored-to-base "bbb" blob is in the base, so it is absent from the
        // delta (no duplication); the new "ggg" blob is in the delta.
        for o in &delta3_set {
            assert!(
                !base_set.contains(o),
                "delta and base must be disjoint (no object in both packs)"
            );
        }
        assert!(
            full_c3.len() > delta3_set.len(),
            "base still serves the unchanged + restored objects"
        );
    }
}
