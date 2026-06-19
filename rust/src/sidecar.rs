use crate::client::Client;
use crate::git;
use anyhow::{Context, Result};
use filetime::{FileTime, set_file_mtime, set_symlink_file_times};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

const INDEX_MTIME: FileTime = FileTime::from_unix_time(1, 0);
const BATCH_SIZE: usize = 200;

/// Configuration written by `ripclone clone` so the sidecar knows which repo and
/// server to finish materializing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoConfig {
    pub server: String,
    /// Full owner/name form, e.g. "oven-sh/bun".
    pub repo: String,
    pub owner: String,
    pub repo_name: String,
    pub commit: String,
    pub branch: String,
}

impl RepoConfig {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join(".ripclone").join("config.json")
    }
}

/// Run the sidecar in `dir` until every tracked file is materialized.
pub async fn run(dir: &Path) -> Result<()> {
    let config_bytes = tokio::fs::read_to_string(RepoConfig::path(dir))
        .await
        .context("read .ripclone/config.json")?;
    let config: RepoConfig =
        serde_json::from_str(&config_bytes).context("parse .ripclone/config.json")?;

    let client = Client::new(config.server.clone());

    // Submodules cannot be materialized as blobs; clear them once so the loop
    // does not hang on paths the server cannot serve.
    let dir_buf = dir.to_path_buf();
    let submodules = tokio::task::spawn_blocking(move || list_submodule_paths(&dir_buf))
        .await
        .context("list submodule paths task")??;
    if !submodules.is_empty() {
        let dir_buf = dir.to_path_buf();
        let to_clear: Vec<String> = submodules.iter().cloned().collect();
        tokio::task::spawn_blocking(move || git::clear_skip_worktree_index(&dir_buf, &to_clear))
            .await
            .context("clear submodule skip-worktree")??;
    }

    loop {
        let dir_buf = dir.to_path_buf();
        let submodule_set = submodules.clone();
        let skipped = tokio::task::spawn_blocking(move || list_skip_worktree(&dir_buf))
            .await
            .context("list skip-worktree task")??;
        let skipped: Vec<String> = skipped
            .into_iter()
            .filter(|p| !submodule_set.contains(p))
            .collect();

        if skipped.is_empty() {
            break;
        }

        let batch: Vec<String> = skipped.into_iter().take(BATCH_SIZE).collect();
        let tar = client
            .fetch_batch(
                &config.owner,
                &config.repo_name,
                &config.branch,
                &config.commit,
                &batch,
            )
            .await
            .context("fetch batch")?;

        let expected: HashSet<String> = batch.iter().cloned().collect();
        let dir_buf = dir.to_path_buf();
        let batch_for_extract = batch.clone();
        let extracted = tokio::task::spawn_blocking(move || {
            extract_batch(&dir_buf, &tar, &expected).map(|paths| (paths, batch_for_extract))
        })
        .await
        .context("extract batch task")??;

        let dir_buf = dir.to_path_buf();
        tokio::task::spawn_blocking(move || git::clear_skip_worktree_index(&dir_buf, &extracted.0))
            .await
            .context("clear skip-worktree task")??;
    }

    Ok(())
}

/// Return tracked paths that are git submodules (mode 160000).
fn list_submodule_paths(dir: &Path) -> Result<HashSet<String>> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir.as_os_str())
        .args(["ls-tree", "-r", "-z", "HEAD"])
        .output()
        .context("git ls-tree -r -z HEAD")?;

    if !output.status.success() {
        anyhow::bail!(
            "git ls-tree failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut paths = HashSet::new();
    for record in String::from_utf8_lossy(&output.stdout).split('\0') {
        if record.is_empty() {
            continue;
        }
        // Format: <mode> SP <type> SP <sha> TAB <path>
        let tab_pos = record.rfind('\t').unwrap_or(0);
        let path = record[tab_pos + 1..].to_string();
        let meta = &record[..tab_pos];
        if meta.starts_with("160000") {
            paths.insert(path);
        }
    }
    Ok(paths)
}

/// Return every tracked path that still has the skip-worktree bit set.
///
/// Uses `-z` so non-ASCII paths are returned verbatim instead of C-quoted.
fn list_skip_worktree(dir: &Path) -> Result<Vec<String>> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir.as_os_str())
        .args(["ls-files", "-v", "-z"])
        .output()
        .context("git ls-files -v -z")?;

    if !output.status.success() {
        anyhow::bail!(
            "git ls-files -v -z failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut paths = Vec::new();
    for record in output.stdout.split(|&b| b == 0) {
        if record.is_empty() {
            continue;
        }
        // Format: <status> SP <path>
        if record.len() < 2 || record[0] != b'S' || record[1] != b' ' {
            continue;
        }
        let path = std::str::from_utf8(&record[2..])
            .with_context(|| format!("non-utf8 path in git ls-files: {:?}", &record[2..]))?;
        if !path.is_empty() {
            paths.push(path.to_string());
        }
    }
    Ok(paths)
}

/// Extract a tar archive of working-tree files into `dir`, returning the paths
/// that were actually written. Only paths in `expected` are processed as a
/// safety guard.
fn extract_batch(dir: &Path, tar_bytes: &[u8], expected: &HashSet<String>) -> Result<Vec<String>> {
    let mut archive = tar::Archive::new(tar_bytes);
    let mut written = Vec::new();

    for entry in archive.entries().context("read tar entries")? {
        let mut entry = entry.context("tar entry")?;
        let path_bytes = entry.path_bytes().into_owned();
        let path_str = String::from_utf8_lossy(&path_bytes);
        let path = Path::new(path_str.as_ref());

        // Reject absolute paths or paths that try to escape the repo.
        if path.is_absolute()
            || path
                .components()
                .any(|c| c == std::path::Component::ParentDir)
        {
            anyhow::bail!("refusing to extract unsafe tar path: {}", path_str);
        }

        if !expected.contains(path_str.as_ref()) {
            continue;
        }

        let target = dir.join(path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }

        let entry_type = entry.header().entry_type();
        if entry_type == tar::EntryType::Symlink {
            let link_target = entry
                .link_name()
                .with_context(|| format!("read link target for {}", path_str))?
                .ok_or_else(|| anyhow::anyhow!("missing link target for {}", path_str))?;
            let link_target = link_target
                .to_str()
                .with_context(|| format!("non-utf8 link target for {}", path_str))?;

            if target.exists() {
                std::fs::remove_file(&target).ok();
            }

            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(link_target, &target)
                    .with_context(|| format!("symlink {}", target.display()))?;
                set_symlink_file_times(&target, INDEX_MTIME, INDEX_MTIME)
                    .with_context(|| format!("set symlink times {}", target.display()))?;
            }
            #[cfg(not(unix))]
            {
                std::fs::write(&target, link_target.as_bytes())
                    .with_context(|| format!("write symlink fallback {}", target.display()))?;
                set_file_mtime(&target, INDEX_MTIME)
                    .with_context(|| format!("set mtime {}", target.display()))?;
            }
        } else {
            if target.exists() {
                std::fs::remove_file(&target).ok();
            }
            entry
                .unpack(&target)
                .with_context(|| format!("unpack {}", target.display()))?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = entry.header().mode().unwrap_or(0o644);
                let mut perms = std::fs::metadata(&target)?.permissions();
                perms.set_mode(mode);
                std::fs::set_permissions(&target, perms)
                    .with_context(|| format!("set permissions {}", target.display()))?;
            }

            set_file_mtime(&target, INDEX_MTIME)
                .with_context(|| format!("set mtime {}", target.display()))?;
        }

        written.push(path_str.to_string());
    }

    Ok(written)
}
