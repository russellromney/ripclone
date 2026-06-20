use anyhow::{Context, Result, bail};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

/// Update the index entries' cached file sizes (and zero out stat timestamps)
/// so that `git status` can trust stat(2) without re-reading every blob.
pub fn update_index_sizes<P: AsRef<Path>>(git_dir: P, sizes: &HashMap<String, u64>) -> Result<()> {
    let index_path = git_dir.as_ref().join("index");
    let mut index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;

    let updates: Vec<(String, u32, u16, u16, git2::Oid, u32)> = index
        .iter()
        .filter_map(|entry| {
            let path = String::from_utf8_lossy(&entry.path).to_string();
            sizes.get(&path).map(|&size| {
                (
                    path,
                    entry.mode,
                    entry.flags,
                    entry.flags_extended,
                    entry.id,
                    size as u32,
                )
            })
        })
        .collect();

    for (path, mode, flags, flags_extended, id, file_size) in updates {
        let e = git2::IndexEntry {
            ctime: git2::IndexTime::new(1, 0),
            mtime: git2::IndexTime::new(1, 0),
            dev: 0,
            ino: 0,
            mode,
            uid: 0,
            gid: 0,
            file_size,
            id,
            flags,
            flags_extended,
            path: path.into_bytes(),
        };
        index.add(&e).with_context(|| {
            format!(
                "updating index entry for {}",
                String::from_utf8_lossy(&e.path)
            )
        })?;
    }

    // Rebuild the cache-tree extension so git can quickly determine whether
    // directories contain tracked files (needed for untracked-file detection).
    let _ = index.write_tree();
    index
        .write()
        .with_context(|| format!("writing index at {}", index_path.display()))?;
    Ok(())
}

/// Mark every tracked path in the index as skip-worktree directly via `git2`.
/// `repo_dir` is the working tree (containing `.git`).
/// This lets git treat the working tree as clean even when files are not
/// materialized yet, which is essential for skeleton/lazy-checkout snapshots.
/// Clear the skip-worktree bit for every entry in the index.
/// Returns the number of entries that were cleared.
pub fn clear_skip_worktree_all<P: AsRef<Path>>(repo_dir: P) -> Result<usize> {
    let index_path = repo_dir.as_ref().join(".git").join("index");
    let mut index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;

    const SKIP_WORKTREE_BIT: u16 = 1 << 4;
    let entries: Vec<_> = index.iter().collect();
    let mut cleared = 0;
    for entry in entries {
        if entry.flags_extended & SKIP_WORKTREE_BIT != 0 {
            let updated = git2::IndexEntry {
                flags_extended: entry.flags_extended & !SKIP_WORKTREE_BIT,
                ..entry
            };
            index.add(&updated).with_context(|| {
                format!(
                    "clear skip-worktree for {}",
                    String::from_utf8_lossy(&updated.path)
                )
            })?;
            cleared += 1;
        }
    }
    index
        .write()
        .with_context(|| format!("writing index at {}", index_path.display()))?;
    Ok(cleared)
}

pub fn set_skip_worktree_all<P: AsRef<Path>>(repo_dir: P) -> Result<()> {
    let index_path = repo_dir.as_ref().join(".git").join("index");
    let mut index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;

    const SKIP_WORKTREE_BIT: u16 = 1 << 4;
    let entries: Vec<_> = index.iter().collect();
    for entry in entries {
        let updated = git2::IndexEntry {
            ctime: entry.ctime,
            mtime: entry.mtime,
            dev: entry.dev,
            ino: entry.ino,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            file_size: entry.file_size,
            id: entry.id,
            flags: entry.flags,
            flags_extended: entry.flags_extended | SKIP_WORKTREE_BIT,
            path: entry.path.to_vec(),
        };
        index.add(&updated).with_context(|| {
            format!(
                "set skip-worktree for {}",
                String::from_utf8_lossy(&updated.path)
            )
        })?;
    }
    index
        .write()
        .with_context(|| format!("writing index at {}", index_path.display()))?;
    Ok(())
}

/// Clear the skip-worktree bit for a set of paths directly via `git2`.
/// `repo_dir` is the working tree (containing `.git`).
/// This avoids spawning a `git update-index` subprocess for every extraction.
pub fn clear_skip_worktree_index<P: AsRef<Path>>(repo_dir: P, paths: &[String]) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let index_path = repo_dir.as_ref().join(".git").join("index");
    let mut index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;

    let to_clear: std::collections::HashSet<&str> = paths.iter().map(|s| s.as_str()).collect();
    let mut updates = Vec::new();
    for entry in index.iter() {
        let path = String::from_utf8_lossy(&entry.path).to_string();
        if to_clear.contains(path.as_str()) {
            let mut flags_extended = entry.flags_extended;
            flags_extended &= !(1 << 4); // clear SKIP_WORKTREE bit (bit 4)
            updates.push((path, flags_extended, entry));
        }
    }
    for (path, flags_extended, entry) in updates {
        let updated = git2::IndexEntry {
            flags_extended,
            ..entry
        };
        index
            .add(&updated)
            .with_context(|| format!("clear skip-worktree for {}", path))?;
    }
    index
        .write()
        .with_context(|| format!("writing index at {}", index_path.display()))?;
    Ok(())
}

/// Materialize the entire index into the working tree using `git checkout-index`.
/// This is typically much faster than writing files one-by-one from an archive
/// because git batches reads through the pack index and uses fewer syscalls.
pub fn checkout_index<P: AsRef<Path>>(repo: P) -> Result<()> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(["checkout-index", "-a", "-f"])
        .status()
        .with_context(|| {
            format!(
                "failed to run git checkout-index in {}",
                repo.as_ref().display()
            )
        })?;
    if !status.success() {
        bail!("git checkout-index failed");
    }
    Ok(())
}

/// Variant of `checkout_index` that lets the git directory and working tree live
/// in different places. Used for worktrees where `GIT_DIR` is the worktree's
/// metadata dir and `GIT_WORK_TREE` is the overlay lower directory.
pub fn checkout_index_with_git_dir(git_dir: &Path, work_tree: &Path) -> Result<()> {
    let status = Command::new("git")
        .env("GIT_DIR", git_dir)
        .env("GIT_WORK_TREE", work_tree)
        .args(["checkout-index", "-a", "-f"])
        .status()
        .with_context(|| {
            format!(
                "failed to run git checkout-index GIT_DIR={} GIT_WORK_TREE={}",
                git_dir.display(),
                work_tree.display()
            )
        })?;
    if !status.success() {
        bail!("git checkout-index failed");
    }
    Ok(())
}

/// Clear the skip-worktree bit for every entry in the index at `git_dir/index`.
/// Returns the number of entries that were cleared.
pub fn clear_skip_worktree_all_git_dir<P: AsRef<Path>>(git_dir: P) -> Result<usize> {
    let index_path = git_dir.as_ref().join("index");
    let mut index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;

    const SKIP_WORKTREE_BIT: u16 = 1 << 4;
    let entries: Vec<_> = index.iter().collect();
    let mut cleared = 0;
    for entry in entries {
        if entry.flags_extended & SKIP_WORKTREE_BIT != 0 {
            let updated = git2::IndexEntry {
                flags_extended: entry.flags_extended & !SKIP_WORKTREE_BIT,
                ..entry
            };
            index.add(&updated).with_context(|| {
                format!(
                    "clear skip-worktree for {}",
                    String::from_utf8_lossy(&updated.path)
                )
            })?;
            cleared += 1;
        }
    }
    index
        .write()
        .with_context(|| format!("writing index at {}", index_path.display()))?;
    Ok(cleared)
}

/// Run a git command in a repo and return stdout as String.
pub fn run_git<P: AsRef<Path>>(repo: P, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(args)
        .output()
        .with_context(|| format!("failed to run git {:?} in {:?}", args, repo.as_ref()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {:?} failed: {}", args, stderr);
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn resolve_commit<P: AsRef<Path>>(repo: P, rev: &str) -> Result<String> {
    crate::validation::validate_git_rev(rev).with_context(|| format!("invalid rev: {}", rev))?;
    // git rev-parse does not support --end-of-options; the rev is already
    // validated, so pass it directly.
    run_git(repo, &["rev-parse", rev])
}

pub fn default_branch<P: AsRef<Path>>(repo: P) -> Result<String> {
    run_git(repo, &["rev-parse", "--abbrev-ref", "HEAD"])
}

pub fn last_commits<P: AsRef<Path>>(repo: P, branch: &str, count: usize) -> Result<Vec<String>> {
    crate::validation::validate_git_rev(branch)
        .with_context(|| format!("invalid branch: {}", branch))?;
    let out = run_git(
        repo,
        &[
            "log",
            "--format=%H",
            "--first-parent",
            "-n",
            &count.to_string(),
            "--end-of-options",
            branch,
        ],
    )?;
    Ok(out.lines().map(|s| s.to_string()).collect())
}

pub fn list_object_shas<P: AsRef<Path>>(repo: P, commit: &str) -> Result<Vec<String>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    let out = run_git(
        repo,
        &[
            "rev-list",
            "--objects",
            "--no-object-names",
            "--end-of-options",
            commit,
        ],
    )?;
    Ok(out.lines().map(|s| s.to_string()).collect())
}

/// Return the blob SHAs of all symlinks reachable from `commit`.
pub fn symlink_blob_shas<P: AsRef<Path>>(repo: P, commit: &str) -> Result<Vec<String>> {
    let entries = list_tree_entries(repo, commit)?;
    Ok(entries
        .into_iter()
        .filter(|(_, raw_mode, _, obj_type)| obj_type == "blob" && raw_mode.starts_with("120"))
        .map(|(_, _, sha, _)| sha)
        .collect())
}

/// List every tree entry reachable from `commit`.
/// Returns tuples of `(path, raw_mode, sha, object_type)`.
pub fn list_tree_entries<P: AsRef<Path>>(
    repo: P,
    commit: &str,
) -> Result<Vec<(String, String, String, String)>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    let out = run_git(repo, &["ls-tree", "-r", "-z", "--end-of-options", commit])?;
    let mut entries = Vec::new();
    for record in out.split('\0') {
        if record.is_empty() {
            continue;
        }
        let parts: Vec<&str> = record.splitn(4, '\t').collect();
        if parts.len() != 2 {
            continue;
        }
        let meta: Vec<&str> = parts[0].split_whitespace().collect();
        if meta.len() != 3 {
            continue;
        }
        let path = parts[1].to_string();
        let raw_mode = meta[0].to_string();
        let obj_type = meta[1].to_string();
        let sha = meta[2].to_string();
        entries.push((path, raw_mode, sha, obj_type));
    }
    Ok(entries)
}

/// Classify many objects by type in one batch using temp files.
pub fn classify_objects<P: AsRef<Path>>(
    repo: P,
    shas: &HashSet<String>,
) -> Result<HashMap<String, String>> {
    if shas.is_empty() {
        return Ok(HashMap::new());
    }

    let mut input = String::new();
    for sha in shas {
        input.push_str(sha);
        input.push('\n');
    }

    let input_file = tempfile::NamedTempFile::new()?;
    let output_file = tempfile::NamedTempFile::new()?;
    std::fs::write(input_file.path(), input.as_bytes())?;

    let repo_str = repo.as_ref().to_str().context("repo path not UTF-8")?;
    let cmd = format!(
        "git -C '{}' cat-file --batch-check='%(objectname) %(objecttype)' < '{}' > '{}'",
        shell_escape(repo_str),
        shell_escape(input_file.path().to_str().unwrap()),
        shell_escape(output_file.path().to_str().unwrap())
    );

    let status = Command::new("sh")
        .args(["-c", &cmd])
        .status()
        .context("git cat-file shell")?;
    if !status.success() {
        bail!("cat-file failed");
    }

    let output = std::fs::read_to_string(output_file.path())?;
    let mut map = HashMap::new();
    for line in output.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            map.insert(parts[0].to_string(), parts[1].to_string());
        }
    }
    Ok(map)
}

/// Build a packfile containing the given object SHAs.
/// Uses a shell subprocess to avoid Rust pipe-buffer deadlocks.
pub fn pack_objects<P: AsRef<Path>, Q: AsRef<Path>>(
    repo: P,
    object_shas: &[String],
    output_path: Q,
) -> Result<()> {
    if object_shas.is_empty() {
        bail!("no objects to pack");
    }

    let mut input = String::new();
    for sha in object_shas {
        input.push_str(sha);
        input.push('\n');
    }

    let input_file = tempfile::NamedTempFile::new()?;
    std::fs::write(input_file.path(), input.as_bytes())?;

    let repo_str = repo.as_ref().to_str().context("repo path not UTF-8")?;
    let output_str = output_path
        .as_ref()
        .to_str()
        .context("output path not UTF-8")?;
    let cmd = format!(
        "git -C '{}' pack-objects --stdout < '{}' > '{}'",
        shell_escape(repo_str),
        shell_escape(input_file.path().to_str().unwrap()),
        shell_escape(output_str)
    );

    let status = Command::new("sh")
        .args(["-c", &cmd])
        .status()
        .context("git pack-objects shell")?;

    if !status.success() {
        bail!("pack-objects failed");
    }

    Ok(())
}

/// Build a packfile + index containing the given object SHAs, writing them to
/// files starting with `prefix`. Git emits `<prefix>-<packhash>.pack` and
/// `<prefix>-<packhash>.idx`.
pub fn pack_objects_to_prefix<P: AsRef<Path>, Q: AsRef<Path>>(
    repo: P,
    object_shas: &[String],
    prefix: Q,
) -> Result<()> {
    if object_shas.is_empty() {
        bail!("no objects to pack");
    }

    let mut input = String::new();
    for sha in object_shas {
        input.push_str(sha);
        input.push('\n');
    }

    let input_file = tempfile::NamedTempFile::new()?;
    std::fs::write(input_file.path(), input.as_bytes())?;

    if let Some(parent) = prefix.as_ref().parent() {
        std::fs::create_dir_all(parent).context("create pack prefix directory")?;
    }

    let repo_str = repo.as_ref().to_str().context("repo path not UTF-8")?;
    let prefix_str = prefix.as_ref().to_str().context("prefix path not UTF-8")?;
    let cmd = format!(
        "git -C '{}' pack-objects '{}' < '{}'",
        shell_escape(repo_str),
        shell_escape(prefix_str),
        shell_escape(input_file.path().to_str().unwrap())
    );

    let status = Command::new("sh")
        .args(["-c", &cmd])
        .status()
        .context("git pack-objects shell")?;

    if !status.success() {
        bail!("pack-objects failed");
    }

    Ok(())
}

fn shell_escape(s: &str) -> String {
    // Minimal escaping for paths without quotes.
    s.replace('\\', "\\\\").replace('\'', "'\\''")
}

pub fn index_pack<P: AsRef<Path>, Q: AsRef<Path>>(git_dir: P, pack_path: Q) -> Result<()> {
    let path_str = pack_path.as_ref().to_str().context("pack path not UTF-8")?;
    let status = Command::new("git")
        .env("GIT_DIR", git_dir.as_ref().as_os_str())
        .args(["index-pack", path_str])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .context("git index-pack")?;
    if !status.success() {
        bail!("index-pack failed");
    }
    Ok(())
}

pub fn init<P: AsRef<Path>>(git_dir: P) -> Result<()> {
    let status = Command::new("git")
        .args(["init", "-q", &git_dir.as_ref().to_string_lossy()])
        .status()
        .context("git init")?;
    if !status.success() {
        bail!("git init failed");
    }
    Ok(())
}

pub fn set_head<P: AsRef<Path>>(git_dir: P, commit: &str) -> Result<()> {
    std::fs::write(git_dir.as_ref().join("HEAD"), format!("{}\n", commit))?;
    Ok(())
}

/// Populate the index from the tree at `commit` without materializing blobs.
pub fn read_tree<P: AsRef<Path>>(git_dir: P, commit: &str) -> Result<()> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    run_git(git_dir, &["read-tree", "--end-of-options", commit])?;
    Ok(())
}

/// Return a map from path to blob size for every blob in the commit tree.
/// Uses `git ls-tree -r -l` so it requires the blob objects to be present
/// (e.g. in a bare mirror or full clone).
pub fn ls_tree_sizes<P: AsRef<Path>>(repo: P, commit: &str) -> Result<HashMap<String, u64>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    let out = run_git(
        repo,
        &["ls-tree", "-r", "-l", "-z", "--end-of-options", commit],
    )?;
    let mut map = HashMap::new();
    for record in out.split('\0') {
        if record.is_empty() {
            continue;
        }
        // Format: <mode> SP <type> SP <sha> SP <size> TAB <path>
        // Size is "-" for submodules and may be "BAD" if objects are missing.
        let tab_pos = record.rfind('\t').context("no tab in ls-tree record")?;
        let path = record[tab_pos + 1..].to_string();
        let meta = &record[..tab_pos];
        let parts: Vec<&str> = meta.split_whitespace().collect();
        if parts.len() < 4 || parts[1] != "blob" {
            continue;
        }
        if let Ok(size) = parts[3].parse::<u64>() {
            map.insert(path, size);
        }
    }
    Ok(map)
}

pub fn ls_tree_entry<P: AsRef<Path>>(
    repo: P,
    commit: &str,
    path: &str,
) -> Result<Option<(String, String)>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    if path.contains('\0') {
        anyhow::bail!("path contains NUL byte");
    }
    let out = run_git(repo, &["ls-tree", "--end-of-options", commit, "--", path])?;
    if out.is_empty() {
        return Ok(None);
    }
    // format: <mode> SP <type> SP <sha> TAB <path>
    let parts: Vec<&str> = out.split_whitespace().collect();
    if parts.len() < 4 {
        bail!("unexpected ls-tree output: {}", out);
    }
    Ok(Some((parts[0].to_string(), parts[2].to_string())))
}

pub fn cat_file<P: AsRef<Path>>(repo: P, sha: &str) -> Result<Vec<u8>> {
    crate::validation::validate_object_id(sha)
        .with_context(|| format!("invalid object id: {}", sha))?;
    let output = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(["cat-file", "-p", "--end-of-options", sha])
        .output()
        .context("cat-file -p")?;
    if !output.status.success() {
        bail!("cat-file -p {} failed", sha);
    }
    Ok(output.stdout)
}

/// Fetch the contents of many blob SHAs in a single `git cat-file --batch` call.
///
/// Streams one SHA at a time so the pipe never backs up, which avoids the
/// deadlock risk of buffering the entire output in memory before reading.
pub fn cat_file_batch<P: AsRef<Path>>(
    repo: P,
    shas: &[String],
) -> Result<std::collections::HashMap<String, Vec<u8>>> {
    if shas.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let mut child = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn git cat-file --batch")?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing stdout"))?;

    let mut writer = std::io::BufWriter::new(stdin);
    let mut reader = std::io::BufReader::new(stdout);
    let mut map = std::collections::HashMap::with_capacity(shas.len());

    for sha in shas {
        writer.write_all(sha.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        let mut header = String::new();
        reader.read_line(&mut header)?;
        let header = header.trim_end();
        if header.starts_with("missing ") || header.is_empty() {
            bail!(
                "cat-file --batch missing object for {} (header: {:?})",
                sha,
                header
            );
        }
        let parts: Vec<&str> = header.split_whitespace().collect();
        if parts.len() < 3 {
            bail!("unexpected cat-file header: {}", header);
        }
        let size: usize = parts[2]
            .parse()
            .with_context(|| format!("invalid size in header: {}", header))?;

        let mut content = vec![0u8; size];
        reader.read_exact(&mut content)?;
        map.insert(sha.clone(), content);
    }

    drop(writer);
    let status = child.wait().context("git cat-file --batch wait")?;
    if !status.success() {
        bail!("git cat-file --batch failed");
    }

    Ok(map)
}

pub fn object_type<P: AsRef<Path>>(repo: P, sha: &str) -> Result<String> {
    crate::validation::validate_object_id(sha)
        .with_context(|| format!("invalid object id: {}", sha))?;
    run_git(repo, &["cat-file", "-t", "--end-of-options", sha])
}

/// Sync a bare mirror of a GitHub repo. Creates if missing, fetches if exists.
/// If `github_token` is provided, it is embedded in the HTTPS URL as
/// `https://x-access-token:<token>@github.com/...` so private repos can be
/// mirrored. This form works for both PATs and GitHub App installation tokens.
pub fn sync_bare_mirror<P: AsRef<Path>>(
    mirror_dir: P,
    owner: &str,
    repo: &str,
    branch: &str,
    depth: usize,
    github_token: Option<&str>,
) -> Result<()> {
    crate::validation::validate_repo_id(owner)
        .with_context(|| format!("invalid owner: {}", owner))?;
    crate::validation::validate_repo_id(repo).with_context(|| format!("invalid repo: {}", repo))?;
    let fetch_ref = if branch == "HEAD" {
        "HEAD".to_string()
    } else {
        crate::validation::validate_git_rev(branch)
            .with_context(|| format!("invalid branch: {}", branch))?;
        format!("refs/heads/{}", branch)
    };
    let url = match github_token {
        Some(token) => format!(
            "https://x-access-token:{}@github.com/{}/{}.git",
            token, owner, repo
        ),
        None => format!("https://github.com/{}/{}.git", owner, repo),
    };
    if mirror_dir.as_ref().exists() {
        let status = Command::new("git")
            .arg("-C")
            .arg(mirror_dir.as_ref().as_os_str())
            .args(["fetch", "--depth", &depth.to_string(), "origin", &fetch_ref])
            .status()
            .context("git fetch")?;
        if !status.success() {
            bail!("fetch failed");
        }
    } else {
        std::fs::create_dir_all(mirror_dir.as_ref().parent().unwrap_or(Path::new("")))?;
        let status = Command::new("git")
            .args([
                "clone",
                "--mirror",
                "--depth",
                &depth.to_string(),
                &url,
                &mirror_dir.as_ref().to_string_lossy(),
            ])
            .status()
            .context("git clone mirror")?;
        if !status.success() {
            bail!("clone mirror failed");
        }
        // If a specific branch was requested, fetch it into the new mirror.
        if branch != "HEAD" {
            let status = Command::new("git")
                .arg("-C")
                .arg(mirror_dir.as_ref().as_os_str())
                .args(["fetch", "--depth", &depth.to_string(), "origin", &fetch_ref])
                .status()
                .context("git fetch branch")?;
            if !status.success() {
                bail!("fetch branch failed");
            }
        }
    }
    Ok(())
}

pub fn parent_commit<P: AsRef<Path>>(repo: P, commit: &str) -> Result<Option<String>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    // git rev-parse does not support --end-of-options; the commit is already
    // validated, so pass it directly.
    let out = run_git(repo, &["rev-parse", &format!("{}^", commit)])?;
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(out))
    }
}

/// Find a commit object in a git dir's object store.
pub fn find_commit_in_git_dir<P: AsRef<Path>>(git_dir: P) -> Result<String> {
    let objects_dir = git_dir.as_ref().join("objects");
    for entry in walkdir::WalkDir::new(&objects_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.is_file() {
            let rel = path.strip_prefix(&objects_dir)?;
            let sha = rel
                .to_string_lossy()
                .replace(&std::path::MAIN_SEPARATOR.to_string(), "");
            if sha.len() == 40 {
                if let Ok(t) = object_type(&git_dir, &sha) {
                    if t == "commit" {
                        return Ok(sha);
                    }
                }
            }
        }
    }
    bail!("no commit object found")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn debug_list_objects() {
        let repo = Path::new("/tmp/ripclone-repos/oven-sh_bun.git");
        let start = std::time::Instant::now();
        let shas = list_object_shas(repo, "HEAD").unwrap();
        println!("list_object_shas: {} in {:?}", shas.len(), start.elapsed());
    }

    #[test]
    #[ignore]
    fn debug_classify() {
        let repo = Path::new("/tmp/ripclone-repos/oven-sh_bun.git");
        let shas = list_object_shas(repo, "HEAD").unwrap();
        let set: HashSet<String> = shas.into_iter().collect();
        let start = std::time::Instant::now();
        let types = classify_objects(repo, &set).unwrap();
        println!("classify_objects: {} in {:?}", types.len(), start.elapsed());
    }

    #[test]
    #[ignore]
    fn debug_pack() {
        let repo = Path::new("/tmp/ripclone-repos/oven-sh_bun.git");
        let shas = list_object_shas(repo, "HEAD").unwrap();
        let set: HashSet<String> = shas.into_iter().collect();
        let types = classify_objects(repo, &set).unwrap();
        let skel: Vec<String> = set
            .into_iter()
            .filter(|sha| {
                matches!(
                    types.get(sha).map(|s| s.as_str()),
                    Some("commit") | Some("tree")
                )
            })
            .collect();
        let out = std::path::Path::new("/tmp/test-skel.pack");
        let start = std::time::Instant::now();
        pack_objects(repo, &skel, out).unwrap();
        println!(
            "pack_objects: {} bytes in {:?}",
            std::fs::metadata(out).unwrap().len(),
            start.elapsed()
        );
    }

    #[test]
    fn test_cat_file_batch() {
        let repo = Path::new("/tmp/bun-inspect.git");
        if !repo.exists() {
            return;
        }
        let shas = vec!["5ff4e6424be88db31ae443f926f289aa726a88b1".to_string()];
        let map = cat_file_batch(repo, &shas).unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&shas[0]));
    }
}

/// Return a list of paths likely to be needed by an agent, ordered by priority.
/// Currently includes top-level tracked files followed by files changed in the
/// last `commit_count` commits, de-duplicated and capped at `max_count`.
pub fn hot_files<P: AsRef<Path>>(
    repo: P,
    commit: &str,
    max_count: usize,
    commit_count: usize,
) -> Result<Vec<String>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    let mut paths: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    // 1. Top-level tracked files (skip directories; agents read files).
    let top = run_git(
        &repo,
        &[
            "ls-tree",
            "-z",
            "--format=%(objecttype) %(path)",
            "--end-of-options",
            commit,
        ],
    )?;
    for record in top.split('\0').filter(|s| !s.is_empty()) {
        let mut parts = record.splitn(2, ' ');
        let obj_type = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("").to_string();
        if obj_type == "blob" && seen.insert(path.clone()) {
            paths.push(path);
        }
    }

    // 2. Files changed in recent commits.
    let commits = last_commits(&repo, commit, commit_count)?;
    for c in commits {
        crate::validation::validate_object_id(&c)
            .with_context(|| format!("invalid commit sha: {}", c))?;
        let out = run_git(
            &repo,
            &[
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-r",
                "-z",
                "--end-of-options",
                &c,
            ],
        )?;
        for name in out.split('\0').filter(|s| !s.is_empty()) {
            let path = name.to_string();
            if seen.insert(path.clone()) {
                paths.push(path);
            }
        }
    }

    paths.truncate(max_count);
    Ok(paths)
}

/// Build a tar archive containing the requested paths from a commit.
///
/// Uses `git archive` so modes and symlinks are preserved exactly. The
/// returned bytes are a standard `.tar` archive (not compressed) so the client
/// can stream-extract it directly into the working tree.
pub fn build_path_tar<P: AsRef<Path>>(repo: P, commit: &str, paths: &[String]) -> Result<Vec<u8>> {
    if paths.is_empty() {
        anyhow::bail!("no paths for batch tar");
    }
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    for path in paths {
        if path.contains('\0') {
            anyhow::bail!("path contains NUL byte: {}", path);
        }
    }

    let mut cmd = std::process::Command::new("git");
    cmd.arg("-C")
        .arg(repo.as_ref().as_os_str())
        .arg("archive")
        .arg("--format=tar")
        .arg("--end-of-options")
        .arg(commit)
        .arg("--");
    for path in paths {
        cmd.arg(path);
    }

    let output = cmd.output().context("git archive")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git archive failed: {}", stderr);
    }
    Ok(output.stdout)
}

/// Materialize a file in a git dir by fetching its blob from a source repo.
pub fn materialize_file<P: AsRef<Path>, Q: AsRef<Path>>(
    source_repo: P,
    git_dir: Q,
    commit: &str,
    path: &str,
    target_root: Option<P>,
) -> Result<usize> {
    let entry = ls_tree_entry(&source_repo, commit, path)?;
    let (mode, sha) = match entry {
        Some(e) => e,
        None => bail!("path not found in tree: {}", path),
    };

    let content = cat_file(&source_repo, &sha)?;
    let target = match target_root {
        Some(root) => root.as_ref().join(path),
        None => git_dir
            .as_ref()
            .parent()
            .unwrap_or(Path::new(""))
            .join(path),
    };

    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&target, &content)?;

    #[cfg(unix)]
    if mode == "100755" {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&target)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&target, perms)?;
    }

    Ok(content.len())
}
