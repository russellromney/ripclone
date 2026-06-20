use crate::cas::Cas;
use crate::git;
use anyhow::{Context, Result};
use filetime::{FileTime, set_file_mtime, set_symlink_file_times};
use flate2::Compression;
use flate2::write::GzEncoder;
use std::path::{Path, PathBuf};

const INDEX_MTIME: FileTime = FileTime::from_unix_time(1, 0);

pub struct SnapshotInfo {
    pub hash: String,
    pub size: u64,
    pub commit: String,
    pub hot_files: usize,
}

pub struct SnapshotBuilder<'a> {
    mirror: PathBuf,
    cas: &'a Cas,
}

impl<'a> SnapshotBuilder<'a> {
    pub fn new<P: AsRef<Path>>(mirror: P, cas: &'a Cas) -> Self {
        Self {
            mirror: mirror.as_ref().to_path_buf(),
            cas,
        }
    }

    /// Build a skeleton snapshot tarball for `commit`.
    ///
    /// The snapshot contains a skeleton `.git/` directory (commit + tree objects
    /// in a pack, index, HEAD) plus up to `hot_file_count` pre-materialized files
    /// that an agent is likely to read first.
    pub fn build(
        &self,
        commit: &str,
        skeleton_pack_hash: &str,
        hot_file_count: usize,
    ) -> Result<SnapshotInfo> {
        let tmp = tempfile::TempDir::new()?;
        let work = tmp.path();
        let git_dir = work.join(".git");

        // Initialize a fresh git dir and populate it with the skeleton pack.
        git::init(work)?;
        let pack_dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&pack_dir)?;

        // Hide the ripclone metadata directory from git status.
        let info_dir = git_dir.join("info");
        std::fs::create_dir_all(&info_dir)?;
        std::fs::write(info_dir.join("exclude"), b".ripclone/\n")?;
        let pack_data = self
            .cas
            .get(skeleton_pack_hash)
            .context("fetch skeleton pack from CAS")?;
        let pack_path = pack_dir.join("skeleton.pack");
        std::fs::write(&pack_path, &pack_data)?;
        git::index_pack(&git_dir, &pack_path)?;
        git::set_head(&git_dir, commit)?;
        git::read_tree(&git_dir, commit)?;

        // Update index so git status can trust stat(2) without reading blobs.
        let sizes = git::ls_tree_sizes(&self.mirror, commit)?;
        git::update_index_sizes(&git_dir, &sizes)?;

        // Configure git for a lazy-checkout environment. We keep symlinks as
        // symlinks and use only size/mtime for stat checks. Modes are preserved
        // by the archive/sidecar materialization, so we leave core.fileMode at
        // its default (true on Unix).
        git::run_git(work, &["config", "core.symlinks", "true"])?;
        git::run_git(work, &["config", "core.checkStat", "minimal"])?;

        // Materialize hot files into the working tree.
        let hot_files = git::hot_files(&self.mirror, commit, hot_file_count, 5)?;
        for path in &hot_files {
            if let Err(e) = self.materialize_blob(commit, path, work, &git_dir) {
                tracing::warn!("failed to materialize hot file {}: {}", path, e);
            }
        }

        // Mark all remaining (non-materialized) entries as skip-worktree so git
        // status stays clean without needing the actual files on disk.
        // These commands run in the working tree, not the .git dir.
        git::set_skip_worktree_all(work)?;
        git::clear_skip_worktree_index(work, &hot_files)?;

        // Build a gzipped tarball of the skeleton working tree in memory.
        let mut tar_bytes = Vec::new();
        {
            let enc = GzEncoder::new(&mut tar_bytes, Compression::default());
            let mut builder = tar::Builder::new(enc);
            builder.follow_symlinks(false);
            builder.append_dir_all(".", work)?;
            let enc = builder.into_inner()?;
            enc.finish()?;
        }

        let hash = self.cas.put(&tar_bytes)?;
        Ok(SnapshotInfo {
            hash,
            size: tar_bytes.len() as u64,
            commit: commit.to_string(),
            hot_files: hot_files.len(),
        })
    }

    fn materialize_blob(
        &self,
        commit: &str,
        path: &str,
        work: &Path,
        git_dir: &Path,
    ) -> Result<()> {
        let entry = git::ls_tree_entry(&self.mirror, commit, path)?;
        let (mode, sha) = match entry {
            Some(e) => e,
            None => anyhow::bail!("path not found in tree: {}", path),
        };

        // Fetch the blob content directly from the bare mirror. The server has
        // the full mirror; individual blobs may not be in the CAS yet.
        let content = git::cat_file(&self.mirror, &sha)?;
        let target = work.join(path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if mode.starts_with("120") {
            #[cfg(unix)]
            {
                let content_str = String::from_utf8_lossy(&content);
                std::os::unix::fs::symlink(content_str.as_ref(), &target)?;
                set_symlink_file_times(&target, INDEX_MTIME, INDEX_MTIME)?;
            }
            #[cfg(not(unix))]
            {
                std::fs::write(&target, &content)?;
                set_file_mtime(&target, INDEX_MTIME)?;
            }
        } else {
            std::fs::write(&target, &content)?;
            #[cfg(unix)]
            if mode == "100755" {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(&target)?.permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&target, perms)?;
            }
            set_file_mtime(&target, INDEX_MTIME)?;
        }

        let _ = git_dir;
        Ok(())
    }
}

/// Extract a gzipped tarball into `target`.
pub fn extract_snapshot<Q: AsRef<Path>>(snapshot_data: &[u8], target: Q) -> Result<()> {
    let target = target.as_ref();
    std::fs::create_dir_all(target)?;
    let decoder = flate2::read::GzDecoder::new(snapshot_data);
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(target)?;
    Ok(())
}
