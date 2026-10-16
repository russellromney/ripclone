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

/// History packs built for one range, with the time spent in `pack-objects`
/// and in storing the packs in the CAS.
#[derive(Default)]
pub struct HistoryBuild {
    pub packs: Vec<(String, u64, String, u64)>,
    pub pack_ms: u64,
    pub store_ms: u64,
}

fn elapsed_ms(start: std::time::Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl<'a> PackBuilder<'a> {
    pub fn new<P: AsRef<Path>>(repo: P, cas: &'a Cas) -> Self {
        Self {
            repo: repo.as_ref().to_path_buf(),
            cas,
        }
    }

    /// Build a ready-to-use `.git/index` for `commit`.
    ///
    /// The index contains every tracked path with `skip-worktree` set and
    /// accurate cached blob sizes. After the client materializes files it must
    /// clear skip-worktree for those paths.
    pub fn build_prebuilt_index(&self, commit: &str) -> Result<Vec<u8>> {
        let tmp = tempfile::TempDir::new()?;
        let index_path = tmp.path().join("index");
        git::write_prebuilt_index(&self.repo, commit, &index_path)?;
        Ok(std::fs::read(&index_path)?)
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
        self.build_packs_from_oids(&head_oids, head_target_raw_bytes)
    }

    /// Build a Head *level*: the depth-1 objects of `commit` that are not in
    /// `have`, the objects of the Head packs it builds on (from their idx
    /// files, see [`idx_object_ids`]). Each push adds one small level, its
    /// changed blobs and the trees from root to leaf, instead of packing again
    /// everything new since the base.
    ///
    /// - **Disjoint:** no object of the level is in `have`, so no object is
    ///   in two Head packs. The client materializes the worktree by writing
    ///   each blob it finds in a Head pack, so a blob in two packs would be
    ///   written twice.
    /// - **Complete:** `have ∪ level ⊇ closure(commit)`, whatever `have` holds:
    ///   a blob deleted and re-added identically is still in an earlier pack.
    ///
    /// Objects in `have` that `commit` no longer uses stay in their packs
    /// until a rebase packs a new base. Listing the closure is cheap
    /// (`rev-list --objects`, no content); only the level is packed,
    /// undeltified, so the client hand-parses blobs. Returns the new packs in
    /// manifest order; these are the only packs to upload.
    pub fn build_head_level(
        &self,
        commit: &str,
        have: &HashSet<String>,
        head_target_raw_bytes: u64,
    ) -> Result<Vec<(String, u64, String, u64)>> {
        let head_oids = git::list_object_shas_with_depth(&self.repo, commit, Some(1))?;
        if head_oids.is_empty() {
            bail!("no objects to pack for {}", commit);
        }
        let level: Vec<String> = head_oids
            .into_iter()
            .filter(|o| !have.contains(o))
            .collect();
        self.build_packs_from_oids(&level, head_target_raw_bytes)
    }

    /// Pack a new Head base for `commit` from the current base and levels,
    /// each given with its idx bytes. A base pack in which objects `commit`
    /// no longer uses take at most a tenth of the bytes is kept as it is.
    /// The rest of the closure (the live objects of the other base packs
    /// and of every level) is packed again, so a rebase uploads the packs
    /// that changed, not the whole base. Levels are never kept: they are
    /// small and folding them keeps the pack count bounded.
    ///
    /// Returns the new base's packs in manifest order (kept packs first)
    /// and the number of kept packs; the packs after them are new.
    pub fn rebase_head_packs(
        &self,
        commit: &str,
        base: &[((String, u64, String, u64), Vec<u8>)],
        head_target_raw_bytes: u64,
    ) -> Result<(Vec<(String, u64, String, u64)>, usize)> {
        let closure = git::list_object_shas_with_depth(&self.repo, commit, Some(1))?;
        if closure.is_empty() {
            bail!("no objects to pack for {}", commit);
        }
        let live: HashSet<&str> = closure.iter().map(String::as_str).collect();
        let mut kept = Vec::new();
        let mut kept_ids = HashSet::new();
        for (pack, idx) in base {
            let objects = idx_object_sizes(idx, pack.1)?;
            let dead: u64 = objects
                .iter()
                .filter(|(id, _)| !live.contains(id.as_str()))
                .map(|(_, len)| len)
                .sum();
            if dead * 10 <= pack.1 {
                kept.push(pack.clone());
                kept_ids.extend(objects.into_iter().map(|(id, _)| id));
            }
        }
        let rest: Vec<String> = closure
            .into_iter()
            .filter(|id| !kept_ids.contains(id))
            .collect();
        let count = kept.len();
        kept.extend(self.build_packs_from_oids(&rest, head_target_raw_bytes)?);
        Ok((kept, count))
    }

    /// Build only the deltified *tail*: objects introduced in `(sealed_tip,
    /// commit]` (the whole history when `sealed_tip` is `None`). The HEAD
    /// closure is built separately by [`Self::build_head_packs`]. Returns the
    /// tail packs, none when the range has no objects.
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
    ) -> Result<HistoryBuild> {
        self.build_history_range(commit, sealed_tip)
    }

    /// Pack the objects of `(exclude, tip]` with git's own walk and delta
    /// reuse (see `git::pack_objects_range_to_prefix`) and store the pack.
    /// `tip`'s whole history is the pack the mirror's first fetch wrote, when
    /// that fetch was of `tip`: that pack is stored as it is.
    fn build_history_range(&self, tip: &str, exclude: Option<&str>) -> Result<HistoryBuild> {
        crate::cancel::Cancel::check_current()?;
        let tmp = tempfile::TempDir::new()?;
        let prefix = tmp.path().join("pack");
        let start = std::time::Instant::now();
        let fetched = exclude
            .is_none()
            .then(|| git::fetched_history_pack(&self.repo, tip))
            .flatten();
        if let Some((pack, idx)) = fetched {
            for (from, ext) in [(&pack, "pack"), (&idx, "idx")] {
                let to = tmp.path().join(format!("fetched.{ext}"));
                if std::fs::hard_link(from, &to).is_err() {
                    std::fs::copy(from, &to).with_context(|| format!("copy {}", from.display()))?;
                }
            }
            tracing::info!(
                "history of {}: stored the mirror's fetched pack",
                &tip[..7.min(tip.len())]
            );
        } else {
            git::pack_objects_range_to_prefix(&self.repo, tip, exclude, &prefix)?;
        }
        let pack_ms = elapsed_ms(start);
        let start = std::time::Instant::now();
        let packs = self.store_packs_from_dir(tmp.path())?;
        Ok(HistoryBuild {
            packs,
            pack_ms,
            store_ms: elapsed_ms(start),
        })
    }

    /// Move every `pack-<sha>.{pack,idx}` pair written under `dir` into the
    /// CAS, and return `(pack_hash, pack_len, idx_hash, idx_len)`
    /// for each. A pack with no objects is dropped.
    fn store_packs_from_dir(&self, dir: &Path) -> Result<Vec<(String, u64, String, u64)>> {
        use std::collections::HashMap;
        // A pack is a 12-byte header, its objects, and a 20-byte checksum.
        const EMPTY_PACK_LEN: u64 = 32;
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
            if std::fs::metadata(pack_path)?.len() <= EMPTY_PACK_LEN {
                continue;
            }
            let (pack_hash, pack_len) = self.cas.install_file(pack_path)?;
            let (idx_hash, idx_len) = self.cas.install_file(idx_path)?;
            out.push((pack_hash, pack_len, idx_hash, idx_len));
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
            let packs = self.build_history_range(&tip, start.as_deref())?.packs;
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
    /// pack each as a self-contained, undeltified mini-pack: each object is
    /// stored whole so the client can hand-parse blobs from the bytes (HEAD
    /// closure). Returns the per-pack `(pack_hash, pack_len, idx_hash, idx_len)`
    /// tuples.
    fn build_packs_from_oids(
        &self,
        oids: &[String],
        target_raw_bytes: u64,
    ) -> Result<Vec<(String, u64, String, u64)>> {
        if oids.is_empty() {
            return Ok(Vec::new());
        }
        let sizes = git::object_sizes(&self.repo, oids)?;

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

        // Undeltified packs are single-threaded + CPU-light, so pack the
        // batches across cores. Rayon threads do not inherit the build's
        // cancel token.
        use rayon::prelude::*;
        let cancel = crate::cancel::Cancel::current().unwrap_or_default();
        batches
            .par_iter()
            .map(|batch| cancel.enter(|| self.pack_undeltified(batch)))
            .collect()
    }

    /// Pack `object_shas` and store the pack + idx in the CAS. Returns
    /// `(pack_hash, pack_len, idx_hash, idx_len)` — the lengths come straight
    /// from the bytes we already read, so callers never re-`get` from the CAS.
    fn pack_undeltified(&self, object_shas: &[String]) -> Result<(String, u64, String, u64)> {
        if object_shas.is_empty() {
            bail!("no objects to pack");
        }
        crate::cancel::Cancel::check_current()?;

        let tmp = tempfile::TempDir::new()?;
        let prefix = tmp.path().join("pack");
        git::pack_objects_undeltified_to_prefix(&self.repo, object_shas, &prefix)?;
        let mut packs = self.store_packs_from_dir(tmp.path())?;
        anyhow::ensure!(packs.len() == 1, "pack-objects wrote {} packs", packs.len());
        Ok(packs.remove(0))
    }
}

/// The object ids of a version 2 pack idx with the bytes each object takes
/// in its pack, `pack_len` long: the distance to the next object's offset,
/// or to the pack's trailing checksum.
pub fn idx_object_sizes(idx: &[u8], pack_len: u64) -> Result<Vec<(String, u64)>> {
    let ids = idx_object_ids(idx)?;
    let count = ids.len();
    let offsets_at = 8 + 256 * 4 + count * (20 + 4);
    let large_at = offsets_at + count * 4;
    let offsets = (0..count)
        .map(|i| {
            let at = offsets_at + i * 4;
            let word = idx
                .get(at..at + 4)
                .context("pack idx is shorter than its offsets")?;
            let word = u32::from_be_bytes(word.try_into()?);
            if word & 0x8000_0000 == 0 {
                return Ok(u64::from(word));
            }
            let at = large_at + (word & 0x7fff_ffff) as usize * 8;
            let large = idx
                .get(at..at + 8)
                .context("pack idx is shorter than its large offsets")?;
            Ok(u64::from_be_bytes(large.try_into()?))
        })
        .collect::<Result<Vec<u64>>>()?;
    let end = pack_len
        .checked_sub(20)
        .context("pack is shorter than its checksum")?;
    let mut order: Vec<usize> = (0..count).collect();
    order.sort_by_key(|&i| offsets[i]);
    let mut sizes = vec![0; count];
    for (k, &i) in order.iter().enumerate() {
        let next = order.get(k + 1).map_or(end, |&j| offsets[j]);
        sizes[i] = next
            .checked_sub(offsets[i])
            .context("pack idx offset is past the end of its pack")?;
    }
    Ok(ids.into_iter().zip(sizes).collect())
}

/// The object ids, in hex, of a version 2 pack idx.
pub fn idx_object_ids(idx: &[u8]) -> Result<Vec<String>> {
    const HEADER: usize = 8;
    const FANOUT: usize = 256 * 4;
    anyhow::ensure!(
        idx.len() >= HEADER + FANOUT && idx[..HEADER] == [0xff, b't', b'O', b'c', 0, 0, 0, 2],
        "not a version 2 pack idx"
    );
    let count = u32::from_be_bytes(idx[HEADER + FANOUT - 4..HEADER + FANOUT].try_into()?) as usize;
    let names = idx
        .get(HEADER + FANOUT..HEADER + FANOUT + count * 20)
        .context("pack idx is shorter than its object count")?;
    Ok(names.chunks_exact(20).map(hex::encode).collect())
}

#[cfg(test)]
mod lsm_incremental_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Commit `files` onto a bare repo's HEAD (parent = current HEAD if any) and
    /// return the new commit oid.
    fn commit_onto(repo: &gix::Repository, files: &[(&str, &[u8])]) -> String {
        crate::test_fixture::commit(repo, files)
    }

    /// Object ids in the packs `build_head_*` returned, read from their idx
    /// files with `git show-index`.
    fn packed_objects(cas: &Cas, packs: &[(String, u64, String, u64)]) -> BTreeSet<String> {
        use std::io::Write;
        let mut out = BTreeSet::new();
        for (_, _, idx_hash, _) in packs {
            let idx = cas.get(idx_hash).unwrap();
            let mut child = std::process::Command::new("git")
                .arg("show-index")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(&idx).unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success());
            // Each line: `<offset> <object id> (<crc>)`.
            for line in String::from_utf8(output.stdout).unwrap().lines() {
                out.insert(line.split_whitespace().nth(1).unwrap().to_string());
            }
        }
        out
    }

    fn rev_parse(repo: &Path, revs: &[String]) -> BTreeSet<String> {
        revs.iter()
            .map(|rev| {
                let output = std::process::Command::new("git")
                    .arg("-C")
                    .arg(repo)
                    .args(["rev-parse", "--verify", rev])
                    .output()
                    .unwrap();
                assert!(output.status.success(), "rev-parse {rev}");
                String::from_utf8(output.stdout).unwrap().trim().to_string()
            })
            .collect()
    }

    /// A cold build packs the full HEAD closure; a re-sync packs only the
    /// depth-1 objects that are new since the base commit. An object the base
    /// already has (here a blob restored to its base content) is not packed
    /// again.
    #[test]
    fn head_levels_hold_only_new_objects() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let pb = PackBuilder::new(tmp.path(), &cas);
        let target = 12 * 1024 * 1024;
        let base_files: Vec<(&str, &[u8])> = vec![
            ("a", b"aaa"),
            ("b", b"bbb"),
            ("c", b"ccc"),
            ("d", b"ddd"),
            ("e", b"eee"),
            ("f", b"fff"),
        ];
        let c1 = commit_onto(&repo, &base_files);
        let base = packed_objects(&cas, &pb.build_head_packs(&c1, target).unwrap());
        let mut c1_objects = vec![c1.clone(), format!("{c1}^{{tree}}")];
        c1_objects.extend(base_files.iter().map(|(name, _)| format!("{c1}:{name}")));
        assert_eq!(base, rev_parse(tmp.path(), &c1_objects));

        // Each level is the objects of its commit's closure that no earlier
        // pack has, and the idx parser lists exactly a pack's objects.
        let mut packs = pb.build_head_packs(&c1, target).unwrap();
        let mut level = |files: &[(&str, &[u8])]| {
            let commit = commit_onto(&repo, files);
            let mut have = HashSet::new();
            for (_, _, idx, _) in &packs {
                let ids = idx_object_ids(&cas.get(idx).unwrap()).unwrap();
                assert_eq!(
                    ids.iter().cloned().collect::<BTreeSet<_>>(),
                    packed_objects(&cas, &[("".into(), 0, idx.clone(), 0)])
                );
                have.extend(ids);
            }
            let built = pb.build_head_level(&commit, &have, target).unwrap();
            packs.extend(built.iter().cloned());
            (commit, packed_objects(&cas, &built))
        };

        // c2 changes b: the level is c2, its root tree, and the new blob.
        let mut c2_files = base_files.clone();
        c2_files[1] = ("b", b"bbb-CHANGED");
        let (c2, got) = level(&c2_files);
        let want = [c2.clone(), format!("{c2}^{{tree}}"), format!("{c2}:b")];
        assert_eq!(got, rev_parse(tmp.path(), &want));

        // c3 restores b to its base content and adds g: the restored blob is
        // in the base packs only.
        let mut c3_files = base_files.clone();
        c3_files.push(("g", b"ggg"));
        let (c3, got) = level(&c3_files);
        let want = [c3.clone(), format!("{c3}^{{tree}}"), format!("{c3}:g")];
        assert_eq!(got, rev_parse(tmp.path(), &want));
        assert!(got.is_disjoint(&base));

        // c4 brings back c2's b: that blob is in c2's level, not packed again.
        let mut c4_files = c3_files.clone();
        c4_files[1] = ("b", b"bbb-CHANGED");
        let (c4, got) = level(&c4_files);
        assert_eq!(
            got,
            rev_parse(tmp.path(), &[c4.clone(), format!("{c4}^{{tree}}")])
        );

        // A corrupt idx is refused.
        assert!(idx_object_ids(b"not an idx").is_err());
    }

    #[test]
    fn rebase_keeps_base_packs_that_stay_live() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let pb = PackBuilder::new(tmp.path(), &cas);
        // Incompressible 8 KiB blobs, two per 16 KiB pack (and the commit
        // and root tree with one of them): five packs.
        let noise = |seed: u8| -> Vec<u8> {
            let mut x = u32::from(seed).wrapping_mul(2_654_435_761) | 1;
            (0..8192)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 17;
                    x ^= x << 5;
                    x as u8
                })
                .collect()
        };
        let blobs: Vec<Vec<u8>> = (0..9).map(noise).collect();
        let names = ["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"];
        let files: Vec<(&str, &[u8])> = names
            .iter()
            .zip(&blobs)
            .map(|(n, b)| (*n, &b[..]))
            .collect();
        let c1 = commit_onto(&repo, &files);
        let target = 16 * 1024;
        let base = pb.build_head_packs(&c1, target).unwrap();
        assert_eq!(base.len(), 5);

        // The idx sizes are what git reports for each object in its pack.
        let dir = tempfile::tempdir().unwrap();
        for (pack, pack_len, idx, _) in &base {
            let idx_bytes = cas.get(idx).unwrap();
            std::fs::write(dir.path().join("p.pack"), cas.get(pack).unwrap()).unwrap();
            std::fs::write(dir.path().join("p.idx"), &idx_bytes).unwrap();
            let out = std::process::Command::new("git")
                .args(["verify-pack", "-v"])
                .arg(dir.path().join("p.idx"))
                .output()
                .unwrap();
            assert!(out.status.success());
            let want: BTreeSet<(String, u64)> = String::from_utf8(out.stdout)
                .unwrap()
                .lines()
                .filter_map(|line| {
                    let f: Vec<&str> = line.split_whitespace().collect();
                    (f.len() >= 5 && f[0].len() == 40)
                        .then(|| (f[0].to_string(), f[3].parse().unwrap()))
                })
                .collect();
            let got = idx_object_sizes(&idx_bytes, *pack_len).unwrap();
            assert_eq!(got.iter().cloned().collect::<BTreeSet<_>>(), want);
            assert_eq!(
                got.iter().map(|(_, len)| len).sum::<u64>(),
                pack_len - 12 - 20
            );
        }
        assert!(idx_object_sizes(&cas.get(&base[0].2).unwrap(), 10).is_err());

        // c2 changes f7. The pack with the commit and root tree loses only
        // a small part of its bytes, so it is kept with the packs f7 is not
        // in. The pack that held f7 is half dead, so it is dropped: the new
        // commit, tree and f7 are packed with that pack's live blob.
        let mut c2_files = files.clone();
        c2_files[7] = ("f7", &blobs[8]);
        let c2 = commit_onto(&repo, &c2_files);
        let with_idx: Vec<_> = base
            .iter()
            .map(|pack| (pack.clone(), cas.get(&pack.2).unwrap()))
            .collect();
        let (rebased, kept) = pb.rebase_head_packs(&c2, &with_idx, target).unwrap();
        let old_f7 = rev_parse(tmp.path(), &[format!("{c1}:f7")]);
        let stays: Vec<_> = base
            .iter()
            .filter(|pack| packed_objects(&cas, &[(*pack).clone()]).is_disjoint(&old_f7))
            .cloned()
            .collect();
        assert_eq!(kept, 4);
        assert_eq!(rebased[..kept], stays[..]);
        let built = packed_objects(&cas, &rebased[kept..]);
        let mut want = rev_parse(
            tmp.path(),
            &[c2.clone(), format!("{c2}^{{tree}}"), format!("{c2}:f7")],
        );
        let dropped: Vec<_> = base
            .iter()
            .filter(|pack| !stays.contains(pack))
            .cloned()
            .collect();
        want.extend(packed_objects(&cas, &dropped).difference(&old_f7).cloned());
        assert_eq!(built, want);

        // Disjoint and complete: every pack is read for the worktree.
        let kept_objects = packed_objects(&cas, &rebased[..kept]);
        assert!(built.is_disjoint(&kept_objects));
        let mut closure: Vec<String> = vec![c2.clone(), format!("{c2}^{{tree}}")];
        closure.extend(names.iter().map(|name| format!("{c2}:{name}")));
        let all: BTreeSet<String> = built.union(&kept_objects).cloned().collect();
        assert!(rev_parse(tmp.path(), &closure).is_subset(&all));
    }
}
