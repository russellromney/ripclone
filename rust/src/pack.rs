use crate::cas::Cas;
use crate::git;
use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub struct PackBuilder<'a> {
    repo: PathBuf,
    cas: &'a Cas,
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
    pub fn build_depth_pack(
        &self,
        commit: &str,
        depth: Option<usize>,
    ) -> Result<(String, String)> {
        let object_shas = git::list_object_shas_with_depth(&self.repo, commit, depth)?;
        self.pack_and_index_inner(&object_shas, true)
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
        self.build_packs_from_oids(&oids, target_raw_bytes)
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
    pub fn build_layered_packs(
        &self,
        commit: &str,
        target_raw_bytes: u64,
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

        let head_packs = self.build_packs_from_oids(&head_oids, target_raw_bytes)?;
        let history_packs = if history_oids.is_empty() {
            Vec::new()
        } else {
            self.build_packs_from_oids(&history_oids, target_raw_bytes)?
        };
        Ok((head_packs, history_packs))
    }

    /// Partition `oids` greedily by raw size into `~target_raw_bytes` batches and
    /// pack each as a self-contained, undeltified (`--window=0`) mini-pack.
    /// Returns `(pack_hash, pack_len, idx_hash, idx_len)` per pack.
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

        let mut packs = Vec::with_capacity(batches.len());
        for batch in batches {
            // Undeltified (`--window=0`): each object is stored whole, so the
            // client can read blobs straight from the downloaded pack bytes
            // (plain zlib, no delta resolution, no shared-repo access). Each pack
            // is also self-contained (non-thin).
            let (pack_hash, idx_hash) = self.pack_and_index_inner(&batch, true)?;
            let pack_len = self.cas.get(&pack_hash)?.len() as u64;
            let idx_len = self.cas.get(&idx_hash)?.len() as u64;
            packs.push((pack_hash, pack_len, idx_hash, idx_len));
        }
        Ok(packs)
    }

    fn pack_and_index(&self, object_shas: &[String]) -> Result<(String, String)> {
        self.pack_and_index_inner(object_shas, false)
    }

    fn pack_and_index_inner(
        &self,
        object_shas: &[String],
        undeltified: bool,
    ) -> Result<(String, String)> {
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
        let pack_hash = self.cas.put(&pack_data)?;
        let idx_hash = self.cas.put(&idx_data)?;

        Ok((pack_hash, idx_hash))
    }
}
