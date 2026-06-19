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
        let shas = git::list_object_shas(&self.repo, commit)?;
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

    fn pack_and_index(&self, object_shas: &[String]) -> Result<(String, String)> {
        if object_shas.is_empty() {
            bail!("no objects to pack");
        }

        let tmp = tempfile::TempDir::new()?;
        let prefix = tmp.path().join("pack");
        git::pack_objects_to_prefix(&self.repo, object_shas, &prefix)?;

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
