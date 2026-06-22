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

const SKIP_WORKTREE_BIT: u16 = 1 << 4;

/// Update the skip-worktree bit for a set of paths directly via `git2`.
/// `repo_dir` is the working tree (containing `.git`).
/// This avoids spawning a `git update-index` subprocess.
pub fn update_index_skip_worktree<P: AsRef<Path>>(
    repo_dir: P,
    paths: &[String],
    set: bool,
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let index_path = repo_dir.as_ref().join(".git").join("index");
    update_index_skip_worktree_at(&index_path, paths, set)
}

fn update_index_skip_worktree_at(
    index_path: &std::path::Path,
    paths: &[String],
    set: bool,
) -> Result<()> {
    let mut index = git2::Index::open(index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;

    let target: HashSet<&str> = paths.iter().map(|s| s.as_str()).collect();
    let entries: Vec<_> = index.iter().collect();
    let mut changed = false;
    for entry in entries {
        let path = String::from_utf8_lossy(&entry.path).to_string();
        if target.contains(path.as_str()) {
            let current = entry.flags_extended & SKIP_WORKTREE_BIT != 0;
            if current == set {
                continue;
            }
            let flags_extended = if set {
                entry.flags_extended | SKIP_WORKTREE_BIT
            } else {
                entry.flags_extended & !SKIP_WORKTREE_BIT
            };
            let updated = git2::IndexEntry {
                flags_extended,
                ..entry
            };
            index
                .add(&updated)
                .with_context(|| format!("update skip-worktree for {}", path))?;
            changed = true;
        }
    }
    if changed {
        index
            .write()
            .with_context(|| format!("writing index at {}", index_path.display()))?;
    }
    Ok(())
}

/// Mark every tracked path in the index as skip-worktree.
/// `repo_dir` is the working tree (containing `.git`).
/// This lets git treat the working tree as clean even when files are not
/// materialized yet, which is essential for skeleton/lazy-checkout snapshots.
pub fn set_skip_worktree_all<P: AsRef<Path>>(repo_dir: P) -> Result<()> {
    let repo_dir = repo_dir.as_ref();
    let index_path = repo_dir.join(".git").join("index");
    let index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;
    let paths: Vec<String> = index
        .iter()
        .map(|entry| String::from_utf8_lossy(&entry.path).to_string())
        .collect();
    update_index_skip_worktree(repo_dir, &paths, true)
}

/// Set skip-worktree on every tracked path only if at least one entry is
/// missing the bit. Use this on a client that receives a prebuilt index from
/// the server: it is a fast no-op when the server already set the bit.
pub fn ensure_skip_worktree_all<P: AsRef<Path>>(repo_dir: P) -> Result<()> {
    let repo_dir = repo_dir.as_ref();
    let index_path = repo_dir.join(".git").join("index");
    let index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;
    let paths: Vec<String> = index
        .iter()
        .filter(|entry| entry.flags_extended & SKIP_WORKTREE_BIT == 0)
        .map(|entry| String::from_utf8_lossy(&entry.path).to_string())
        .collect();
    update_index_skip_worktree(repo_dir, &paths, true)
}

/// Clear the skip-worktree bit for every entry in the index.
/// Returns the number of entries that were cleared.
pub fn clear_skip_worktree_all<P: AsRef<Path>>(repo_dir: P) -> Result<usize> {
    let repo_dir = repo_dir.as_ref();
    let index_path = repo_dir.join(".git").join("index");
    let index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;
    let paths: Vec<String> = index
        .iter()
        .filter(|entry| entry.flags_extended & SKIP_WORKTREE_BIT != 0)
        .map(|entry| String::from_utf8_lossy(&entry.path).to_string())
        .collect();
    let cleared = paths.len();
    update_index_skip_worktree(repo_dir, &paths, false)?;
    Ok(cleared)
}

/// Clear the skip-worktree bit for a set of paths directly via `git2`.
/// `repo_dir` is the working tree (containing `.git`).
/// This avoids spawning a `git update-index` subprocess for every extraction.
pub fn clear_skip_worktree_index<P: AsRef<Path>>(repo_dir: P, paths: &[String]) -> Result<()> {
    update_index_skip_worktree(repo_dir, paths, false)
}

/// Clear skip-worktree and refresh cached stat data for materialized paths.
///
/// This lets fresh checkout/materialization paths skip the per-file mtime stamp:
/// the index is updated to match the actual files on disk instead.
pub fn clear_skip_worktree_index_and_refresh_stats<P: AsRef<Path>>(
    repo_dir: P,
    paths: &[String],
) -> Result<()> {
    clear_skip_worktree_index_with_stats(repo_dir, paths, &[])
}

pub fn clear_skip_worktree_index_with_stats<P: AsRef<Path>>(
    repo_dir: P,
    paths: &[String],
    stats: &[MaterializedPathStat],
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let repo_dir = repo_dir.as_ref();
    let index_path = repo_dir.join(".git").join("index");
    let mut index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;

    let target: HashSet<&str> = paths.iter().map(|s| s.as_str()).collect();
    let stats_by_path: HashMap<&str, &IndexStat> =
        stats.iter().map(|s| (s.path.as_str(), &s.stat)).collect();
    let entries: Vec<_> = index.iter().collect();
    let mut changed = false;
    for entry in entries {
        let path = String::from_utf8_lossy(&entry.path).to_string();
        if !target.contains(path.as_str()) {
            continue;
        }
        let fallback_stat;
        let stat = if let Some(stat) = stats_by_path.get(path.as_str()) {
            *stat
        } else {
            let full_path = repo_dir.join(index_path_from_bytes(&entry.path));
            let metadata = std::fs::symlink_metadata(&full_path)
                .with_context(|| format!("stat materialized file {}", full_path.display()))?;
            fallback_stat = index_stat_from_metadata(&metadata);
            &fallback_stat
        };
        let updated = git2::IndexEntry {
            ctime: stat.ctime,
            mtime: stat.mtime,
            dev: stat.dev,
            ino: stat.ino,
            mode: entry.mode,
            uid: stat.uid,
            gid: stat.gid,
            file_size: stat.file_size,
            id: entry.id,
            flags: entry.flags,
            flags_extended: entry.flags_extended & !SKIP_WORKTREE_BIT,
            path: entry.path,
        };
        index
            .add(&updated)
            .with_context(|| format!("refresh index stats for {}", path))?;
        changed = true;
    }
    if changed {
        index
            .write()
            .with_context(|| format!("writing index at {}", index_path.display()))?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct MaterializedPathStat {
    pub path: String,
    stat: IndexStat,
}

#[derive(Debug)]
struct IndexStat {
    ctime: git2::IndexTime,
    mtime: git2::IndexTime,
    dev: u32,
    ino: u32,
    uid: u32,
    gid: u32,
    file_size: u32,
}

pub fn materialized_path_stat_from_metadata(
    path: String,
    metadata: &std::fs::Metadata,
) -> MaterializedPathStat {
    MaterializedPathStat {
        path,
        stat: index_stat_from_metadata(metadata),
    }
}

#[cfg(target_os = "linux")]
pub fn materialized_path_stat_from_statx(
    path: String,
    statx: &libc::statx,
) -> MaterializedPathStat {
    MaterializedPathStat {
        path,
        stat: index_stat_from_statx(statx),
    }
}

#[cfg(unix)]
fn index_path_from_bytes(path: &[u8]) -> std::path::PathBuf {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    std::path::PathBuf::from(OsStr::from_bytes(path))
}

#[cfg(not(unix))]
fn index_path_from_bytes(path: &[u8]) -> std::path::PathBuf {
    std::path::PathBuf::from(String::from_utf8_lossy(path).into_owned())
}

#[cfg(unix)]
fn index_stat_from_metadata(metadata: &std::fs::Metadata) -> IndexStat {
    use std::os::unix::fs::MetadataExt;
    IndexStat {
        ctime: git2::IndexTime::new(
            clamp_i64_to_i32(metadata.ctime()),
            metadata.ctime_nsec() as u32,
        ),
        mtime: git2::IndexTime::new(
            clamp_i64_to_i32(metadata.mtime()),
            metadata.mtime_nsec() as u32,
        ),
        dev: truncate_u64_to_u32(metadata.dev()),
        ino: truncate_u64_to_u32(metadata.ino()),
        uid: metadata.uid(),
        gid: metadata.gid(),
        file_size: truncate_u64_to_u32(metadata.len()),
    }
}

#[cfg(not(unix))]
fn index_stat_from_metadata(metadata: &std::fs::Metadata) -> IndexStat {
    IndexStat {
        ctime: git2::IndexTime::new(0, 0),
        mtime: git2::IndexTime::new(0, 0),
        dev: 0,
        ino: 0,
        uid: 0,
        gid: 0,
        file_size: truncate_u64_to_u32(metadata.len()),
    }
}

#[cfg(target_os = "linux")]
fn index_stat_from_statx(statx: &libc::statx) -> IndexStat {
    IndexStat {
        ctime: git2::IndexTime::new(
            clamp_i64_to_i32(statx.stx_ctime.tv_sec),
            statx.stx_ctime.tv_nsec,
        ),
        mtime: git2::IndexTime::new(
            clamp_i64_to_i32(statx.stx_mtime.tv_sec),
            statx.stx_mtime.tv_nsec,
        ),
        dev: truncate_u64_to_u32(make_dev(statx.stx_dev_major, statx.stx_dev_minor)),
        ino: truncate_u64_to_u32(statx.stx_ino),
        uid: statx.stx_uid,
        gid: statx.stx_gid,
        file_size: truncate_u64_to_u32(statx.stx_size),
    }
}

#[cfg(target_os = "linux")]
fn make_dev(major: u32, minor: u32) -> u64 {
    let major = major as u64;
    let minor = minor as u64;
    ((major & 0x00000fff) << 8)
        | (minor & 0x000000ff)
        | ((minor & 0xffffff00) << 12)
        | ((major & 0xfffff000) << 32)
}

fn clamp_i64_to_i32(value: i64) -> i32 {
    value.clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

fn truncate_u64_to_u32(value: u64) -> u32 {
    value.min(u32::MAX as u64) as u32
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
    let index = git2::Index::open(&index_path)
        .with_context(|| format!("opening index at {}", index_path.display()))?;
    let paths: Vec<String> = index
        .iter()
        .filter(|entry| entry.flags_extended & SKIP_WORKTREE_BIT != 0)
        .map(|entry| String::from_utf8_lossy(&entry.path).to_string())
        .collect();
    let cleared = paths.len();
    update_index_skip_worktree_at(&index_path, &paths, false)?;
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
    list_object_shas_with_depth(repo, commit, None)
}

/// List objects reachable from `to` but not from `from` — i.e. the objects
/// introduced in the commit range `(from, to]`. `from = None` means "everything
/// reachable from `to`". Used by the LSM build to pack a single history range.
pub fn list_object_shas_in_range<P: AsRef<Path>>(
    repo: P,
    from: Option<&str>,
    to: &str,
) -> Result<Vec<String>> {
    crate::validation::validate_git_rev(to).with_context(|| format!("invalid commit: {}", to))?;
    if let Some(f) = from {
        crate::validation::validate_git_rev(f).with_context(|| format!("invalid commit: {}", f))?;
    }
    // `rev-list --objects <to> ^<from>`: objects reachable from `to` but not from
    // `from`. The `^<from>` exclude form (rather than `--not`) composes with
    // `--end-of-options`, so every flag stays before the revs.
    let exclude = from.map(|f| format!("^{}", f));
    // `--use-bitmap-index` lets git answer this from the mirror's reachability
    // bitmap when one exists (see `write_bitmap`); it falls back to a normal
    // walk otherwise, so it is always safe to pass.
    let mut args: Vec<&str> = vec![
        "rev-list",
        "--objects",
        "--no-object-names",
        "--use-bitmap-index",
        "--end-of-options",
        to,
    ];
    if let Some(e) = exclude.as_deref() {
        args.push(e);
    }
    let out = run_git(repo, &args)?;
    Ok(out.lines().map(|s| s.to_string()).collect())
}

/// Set of worktree paths (raw bytes) that differ between commits `from` and
/// `to` — added, modified, deleted, or mode-changed. Used to rebuild only the
/// changed entries on a re-sync (files-table by-diff, etc.).
///
/// Uses `-z` (NUL-separated, never quoted) so the returned bytes match the tree
/// walk's raw path bytes exactly — a quoted path could otherwise fail to match
/// and be wrongly treated as unchanged (a correctness hazard). `--no-renames`
/// makes a rename a delete+add so the new path is reported (and rebuilt).
pub fn diff_name_set<P: AsRef<Path>>(
    repo: P,
    from: &str,
    to: &str,
) -> Result<std::collections::HashSet<Vec<u8>>> {
    crate::validation::validate_git_rev(from)
        .with_context(|| format!("invalid commit: {}", from))?;
    crate::validation::validate_git_rev(to).with_context(|| format!("invalid commit: {}", to))?;
    let output = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args([
            "diff",
            "--name-only",
            "-z",
            "--no-renames",
            "--end-of-options",
            from,
            to,
        ])
        .output()
        .with_context(|| format!("git diff {}..{}", from, to))?;
    if !output.status.success() {
        bail!(
            "git diff {}..{} failed: {}",
            from,
            to,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output
        .stdout
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_vec())
        .collect())
}

/// List objects reachable from `commit`, optionally limiting the commit history
/// depth. `max_depth = None` returns the full history. With a depth of `1`, only
/// the HEAD commit and the trees/blobs reachable from it are returned, which is
/// exactly what `git clone --depth=1` needs.
pub fn list_object_shas_with_depth<P: AsRef<Path>>(
    repo: P,
    commit: &str,
    max_depth: Option<usize>,
) -> Result<Vec<String>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    let depth_str = max_depth.map(|d| d.to_string());
    let mut args: Vec<&str> = vec![
        "rev-list",
        "--objects",
        "--no-object-names",
        "--end-of-options",
        commit,
    ];
    if let Some(d) = depth_str.as_deref() {
        args.insert(1, "-n");
        args.insert(2, d);
    } else {
        // Full reachability: use the mirror's bitmap when present (no-op
        // otherwise). Skipped for depth-limited walks, where it doesn't apply.
        args.insert(1, "--use-bitmap-index");
    }
    let out = run_git(repo, &args)?;
    Ok(out.lines().map(|s| s.to_string()).collect())
}

/// Write a multi-pack-index over all packs in `repo_dir`'s object store so git
/// object lookups stay O(log n) regardless of how many packs are installed.
/// Cheap: indexes the existing `.idx` files; no pack data is rewritten. Best
/// effort — a failure only loses the lookup speedup, not correctness.
pub fn write_multi_pack_index<P: AsRef<Path>>(repo_dir: P) -> Result<()> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_dir.as_ref().as_os_str())
        .args(["multi-pack-index", "write"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("spawn git multi-pack-index write")?;
    if !status.success() {
        anyhow::bail!("git multi-pack-index write failed");
    }
    Ok(())
}

/// Write a commit-graph over all reachable commits so the many `rev-list` /
/// graph walks during a build (skeleton + layered packs) don't re-parse commit
/// objects from the packfile each time. A fresh `--mirror` clone has none. Cheap
/// to build and best-effort — a failure only loses the speedup.
///
/// Uses `--split`: on a re-sync only the new commits are written into a new
/// graph layer instead of rewriting the whole graph, so this stays O(new
/// commits) rather than O(all commits) every sync.
pub fn write_commit_graph<P: AsRef<Path>>(repo_dir: P) -> Result<()> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_dir.as_ref().as_os_str())
        .args([
            "commit-graph",
            "write",
            "--reachable",
            "--split",
            "--no-progress",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("spawn git commit-graph write")?;
    if !status.success() {
        anyhow::bail!("git commit-graph write failed");
    }
    Ok(())
}

/// Write a multi-pack-index *with a reachability bitmap* over the mirror's packs
/// so `rev-list`/`pack-objects` can answer reachability by OR-ing precomputed
/// bitmaps instead of walking the commit+tree graph. This is what makes the
/// full skeleton/history enumerations fast on a fresh `--mirror` clone (GitHub
/// ships bitmaps, but our `git fetch` of all refs can leave them stale/absent).
/// Best-effort — a failure only loses the speedup. Building the bitmap itself
/// costs one reachability walk, so call it once after fetch, before the heavy
/// builds, not on the depth=1 fast path.
pub fn write_bitmap<P: AsRef<Path>>(repo_dir: P) -> Result<()> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_dir.as_ref().as_os_str())
        .args(["multi-pack-index", "write", "--bitmap", "--no-progress"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("spawn git multi-pack-index write --bitmap")?;
    if !status.success() {
        anyhow::bail!("git multi-pack-index write --bitmap failed");
    }
    Ok(())
}

/// Build a multi-pack-index over the given packs (each a `(pack_bytes,
/// idx_bytes)` pair) and return the raw `multi-pack-index` file bytes. The packs
/// are laid out with the same `pack-<trailer>.{pack,idx}` filenames the client
/// uses, so the MIDX references them by those names and is byte-for-byte usable
/// on the client once it installs the same packs — no client-side
/// `git multi-pack-index write` needed.
pub fn build_multi_pack_index_bytes(packs: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<u8>> {
    if packs.is_empty() {
        anyhow::bail!("no packs to index");
    }
    let tmp = tempfile::TempDir::new()?;
    let dir = tmp.path();
    // Minimal bare object store so `git multi-pack-index write` has a repo and
    // an `objects/pack` directory laid out exactly like the client's.
    let status = Command::new("git")
        .args(["init", "--bare", "-q"])
        .arg(dir.as_os_str())
        .status()
        .context("git init --bare for midx")?;
    if !status.success() {
        anyhow::bail!("git init --bare for midx failed");
    }
    let pack_dir = dir.join("objects").join("pack");
    std::fs::create_dir_all(&pack_dir)?;
    for (pack_bytes, idx_bytes) in packs {
        if pack_bytes.len() < 20 {
            anyhow::bail!("pack too short to name by trailer");
        }
        // Git names packs by the 20-byte trailer sha; match the client exactly.
        let name = hex::encode(&pack_bytes[pack_bytes.len() - 20..]);
        std::fs::write(pack_dir.join(format!("pack-{}.pack", name)), pack_bytes)?;
        std::fs::write(pack_dir.join(format!("pack-{}.idx", name)), idx_bytes)?;
    }
    let status = Command::new("git")
        .arg("-C")
        .arg(dir.as_os_str())
        .args(["multi-pack-index", "write"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("spawn git multi-pack-index write")?;
    if !status.success() {
        anyhow::bail!("git multi-pack-index write (server pregen) failed");
    }
    std::fs::read(pack_dir.join("multi-pack-index")).context("read generated multi-pack-index")
}

/// Return the raw (uncompressed) size of each object via
/// `git cat-file --batch-check`. Used to partition objects into evenly-sized
/// pack batches.
pub fn object_sizes<P: AsRef<Path>>(repo: P, oids: &[String]) -> Result<HashMap<String, u64>> {
    if oids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut input = String::with_capacity(oids.len() * 41);
    for oid in oids {
        input.push_str(oid);
        input.push('\n');
    }
    let input_file = tempfile::NamedTempFile::new()?;
    std::fs::write(input_file.path(), input.as_bytes())?;
    let stdin = std::fs::File::open(input_file.path())?;
    let out = Command::new("git")
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args([
            "cat-file",
            "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        ])
        .stdin(stdin)
        .stderr(Stdio::inherit())
        .output()
        .context("git cat-file --batch-check")?;
    if !out.status.success() {
        bail!("git cat-file --batch-check failed");
    }
    let text = String::from_utf8(out.stdout).context("batch-check output not UTF-8")?;
    let mut map = HashMap::with_capacity(oids.len());
    for line in text.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }
        if let Ok(size) = parts[2].parse::<u64>() {
            map.insert(parts[0].to_string(), size);
        }
    }
    Ok(map)
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
    pack_objects_to_prefix_inner(repo, object_shas, prefix, &[])
}

/// Like `pack_objects_to_prefix` but stores every object whole — no new delta
/// search (`--window=0`) and no reuse of deltas already present in the source
/// repo (`--no-reuse-delta`). Required so the client can read objects with plain
/// zlib and no delta resolution, at the cost of a larger pack.
pub fn pack_objects_undeltified_to_prefix<P: AsRef<Path>, Q: AsRef<Path>>(
    repo: P,
    object_shas: &[String],
    prefix: Q,
) -> Result<()> {
    pack_objects_to_prefix_inner(
        repo,
        object_shas,
        prefix,
        &["--window=0", "--no-reuse-delta"],
    )
}

fn pack_objects_to_prefix_inner<P: AsRef<Path>, Q: AsRef<Path>>(
    repo: P,
    object_shas: &[String],
    prefix: Q,
    extra_args: &[&str],
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
    let extra = if extra_args.is_empty() {
        String::new()
    } else {
        format!(" {}", extra_args.join(" "))
    };
    let cmd = format!(
        "git -C '{}' pack-objects{} '{}' < '{}'",
        shell_escape(repo_str),
        extra,
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
    github_token: Option<&str>,
) -> Result<()> {
    crate::validation::validate_repo_id(owner)
        .with_context(|| format!("invalid owner: {}", owner))?;
    crate::validation::validate_repo_id(repo).with_context(|| format!("invalid repo: {}", repo))?;
    // Validate the branch name (used later for commit resolution). The mirror
    // itself always fetches *all* refs, so the branch is not part of the fetch.
    if branch != "HEAD" {
        crate::validation::validate_git_rev(branch)
            .with_context(|| format!("invalid branch: {}", branch))?;
    }
    // The origin base is normally GitHub, but is overridable via
    // RIPCLONE_ORIGIN_BASE so tests (and self-hosted setups) can mirror from a
    // local `file://` origin without any network. Tokens are only injected for
    // the real GitHub host.
    let base = std::env::var("RIPCLONE_ORIGIN_BASE")
        .ok()
        .filter(|b| !b.is_empty())
        .unwrap_or_else(|| "https://github.com".to_string());
    let url = match github_token {
        Some(token) if base == "https://github.com" => format!(
            "https://x-access-token:{}@github.com/{}/{}.git",
            token, owner, repo
        ),
        _ => format!("{}/{}/{}.git", base.trim_end_matches('/'), owner, repo),
    };
    // The mirror is always a *complete* clone. The "full" (depth=0) clonepack is
    // built from `rev-list HEAD` over this mirror, so any shallow boundary would
    // silently truncate it and break `git rev-list`/`fsck` on the client. We
    // therefore never pass `--depth`: a depth-limited fetch would re-shallow an
    // already-complete mirror. depth=1 ("head") clones are still cheap — they're
    // a content-addressed subset built at pack time, not a shallower mirror.
    if mirror_dir.as_ref().exists() {
        // A `--mirror` clone is configured with `+refs/*:refs/*` (and prunes), so
        // a plain `git fetch origin` advances every branch + HEAD to the latest.
        // Do NOT fetch an explicit ref like `HEAD` here: that only updates
        // FETCH_HEAD and leaves the mirror's branch refs stale, so a re-sync after
        // a new push would silently keep serving the old commit.
        //
        // A leftover shallow mirror (from the old `--depth 50` default) carries a
        // `shallow` marker file; `--unshallow` completes it (and still fetches all
        // refs) once, after which plain fetches keep it complete.
        let is_shallow = mirror_dir.as_ref().join("shallow").exists();
        let mut args: Vec<&str> = vec!["fetch"];
        if is_shallow {
            args.push("--unshallow");
        }
        args.push("origin");
        let status = Command::new("git")
            .arg("-C")
            .arg(mirror_dir.as_ref().as_os_str())
            .args(&args)
            .status()
            .context("git fetch")?;
        if !status.success() {
            bail!("fetch failed");
        }
    } else {
        // A fresh `--mirror` clone copies every ref (branches, tags, HEAD), so no
        // follow-up branch fetch is needed.
        std::fs::create_dir_all(mirror_dir.as_ref().parent().unwrap_or(Path::new("")))?;
        let status = Command::new("git")
            .args([
                "clone",
                "--mirror",
                &url,
                &mirror_dir.as_ref().to_string_lossy(),
            ])
            .status()
            .context("git clone mirror")?;
        if !status.success() {
            bail!("clone mirror failed");
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
            if sha.len() == 40
                && let Ok(t) = object_type(&git_dir, &sha)
                && t == "commit"
            {
                return Ok(sha);
            }
        }
    }
    bail!("no commit object found")
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
