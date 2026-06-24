use anyhow::{Context, Result, bail};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::AtomicBool;

const SKIP_WORKTREE_FLAG: gix::index::entry::Flags = gix::index::entry::Flags::SKIP_WORKTREE;

fn open_index_file(path: &Path) -> Result<gix::index::File> {
    gix::index::File::at(
        path,
        gix::hash::Kind::Sha1,
        false,
        gix::index::decode::Options::default(),
    )
    .with_context(|| format!("opening index at {}", path.display()))
}

fn write_index_file(index: &mut gix::index::File) -> Result<()> {
    index
        .write(gix::index::write::Options::default())
        .with_context(|| format!("writing index at {}", index.path().display()))
}

/// Update the index entries' cached file sizes (and zero out stat timestamps)
/// so that `git status` can trust stat(2) without re-reading every blob.
pub fn update_index_sizes<P: AsRef<Path>>(git_dir: P, sizes: &HashMap<String, u64>) -> Result<()> {
    let index_path = git_dir.as_ref().join("index");
    let mut index = open_index_file(&index_path)?;

    for (path, &size) in sizes {
        let entry = index
            .entry_mut_by_path_and_stage(
                gix::bstr::BStr::new(path.as_bytes()),
                gix::index::entry::Stage::Unconflicted,
            )
            .with_context(|| format!("path {path} not in index"))?;
        entry.stat.size = size as u32;
        entry.stat.ctime = gix::index::entry::stat::Time { secs: 1, nsecs: 0 };
        entry.stat.mtime = gix::index::entry::stat::Time { secs: 1, nsecs: 0 };
        entry.stat.dev = 0;
        entry.stat.ino = 0;
        entry.stat.uid = 0;
        entry.stat.gid = 0;
    }

    write_index_file(&mut index)
}

/// Update the skip-worktree bit for a set of paths directly via gix.
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
    let mut index = open_index_file(index_path)?;

    let mut changed = false;
    for path in paths {
        let Some(entry) = index.entry_mut_by_path_and_stage(
            gix::bstr::BStr::new(path.as_bytes()),
            gix::index::entry::Stage::Unconflicted,
        ) else {
            continue;
        };
        let current = entry.flags.contains(SKIP_WORKTREE_FLAG);
        if current == set {
            continue;
        }
        entry.flags.set(SKIP_WORKTREE_FLAG, set);
        if set {
            entry.flags.insert(gix::index::entry::Flags::EXTENDED);
        } else if (entry.flags & (gix::index::entry::Flags::INTENT_TO_ADD | SKIP_WORKTREE_FLAG))
            .is_empty()
        {
            entry.flags.remove(gix::index::entry::Flags::EXTENDED);
        }
        changed = true;
    }
    if changed {
        write_index_file(&mut index)?;
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
    let index = open_index_file(&index_path)?;
    let paths: Vec<String> = index
        .entries()
        .iter()
        .map(|entry| String::from_utf8_lossy(entry.path_in(index.path_backing())).to_string())
        .collect();
    update_index_skip_worktree(repo_dir, &paths, true)
}

/// Set skip-worktree on every tracked path only if at least one entry is
/// missing the bit. Use this on a client that receives a prebuilt index from
/// the server: it is a fast no-op when the server already set the bit.
pub fn ensure_skip_worktree_all<P: AsRef<Path>>(repo_dir: P) -> Result<()> {
    let repo_dir = repo_dir.as_ref();
    let index_path = repo_dir.join(".git").join("index");
    let index = open_index_file(&index_path)?;
    let paths: Vec<String> = index
        .entries()
        .iter()
        .filter(|entry| !entry.flags.contains(SKIP_WORKTREE_FLAG))
        .map(|entry| String::from_utf8_lossy(entry.path_in(index.path_backing())).to_string())
        .collect();
    update_index_skip_worktree(repo_dir, &paths, true)
}

/// Clear the skip-worktree bit for every entry in the index.
/// Returns the number of entries that were cleared.
pub fn clear_skip_worktree_all<P: AsRef<Path>>(repo_dir: P) -> Result<usize> {
    let repo_dir = repo_dir.as_ref();
    let index_path = repo_dir.join(".git").join("index");
    let index = open_index_file(&index_path)?;
    let paths: Vec<String> = index
        .entries()
        .iter()
        .filter(|entry| entry.flags.contains(SKIP_WORKTREE_FLAG))
        .map(|entry| String::from_utf8_lossy(entry.path_in(index.path_backing())).to_string())
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
    if !index_path.exists() {
        // No index means no skip-worktree state to clear (e.g. extracting into a
        // plain directory rather than a git worktree).
        return Ok(());
    }
    let mut index = open_index_file(&index_path)?;

    let stats_by_path: HashMap<&str, &IndexStat> =
        stats.iter().map(|s| (s.path.as_str(), &s.stat)).collect();
    let mut changed = false;
    for path in paths {
        let Some(entry) = index.entry_mut_by_path_and_stage(
            gix::bstr::BStr::new(path.as_bytes()),
            gix::index::entry::Stage::Unconflicted,
        ) else {
            continue;
        };
        let stat = if let Some(stat) = stats_by_path.get(path.as_str()) {
            **stat
        } else {
            let full_path = repo_dir.join(index_path_from_bytes(path.as_bytes()));
            let metadata = std::fs::symlink_metadata(&full_path)
                .with_context(|| format!("stat materialized file {}", full_path.display()))?;
            index_stat_from_metadata(&metadata)
        };
        entry.stat = stat;
        entry.flags.remove(SKIP_WORKTREE_FLAG);
        if (entry.flags & (gix::index::entry::Flags::INTENT_TO_ADD | SKIP_WORKTREE_FLAG)).is_empty()
        {
            entry.flags.remove(gix::index::entry::Flags::EXTENDED);
        }
        changed = true;
    }
    if changed {
        write_index_file(&mut index)?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct MaterializedPathStat {
    pub path: String,
    stat: IndexStat,
}

type IndexStat = gix::index::entry::Stat;

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
        ctime: gix::index::entry::stat::Time {
            secs: clamp_i64_to_u32(metadata.ctime()),
            nsecs: metadata.ctime_nsec() as u32,
        },
        mtime: gix::index::entry::stat::Time {
            secs: clamp_i64_to_u32(metadata.mtime()),
            nsecs: metadata.mtime_nsec() as u32,
        },
        dev: truncate_u64_to_u32(metadata.dev()),
        ino: truncate_u64_to_u32(metadata.ino()),
        uid: metadata.uid(),
        gid: metadata.gid(),
        size: truncate_u64_to_u32(metadata.len()),
    }
}

#[cfg(not(unix))]
fn index_stat_from_metadata(metadata: &std::fs::Metadata) -> IndexStat {
    IndexStat {
        ctime: gix::index::entry::stat::Time { secs: 0, nsecs: 0 },
        mtime: gix::index::entry::stat::Time { secs: 0, nsecs: 0 },
        dev: 0,
        ino: 0,
        uid: 0,
        gid: 0,
        size: truncate_u64_to_u32(metadata.len()),
    }
}

#[cfg(target_os = "linux")]
fn index_stat_from_statx(statx: &libc::statx) -> IndexStat {
    IndexStat {
        ctime: gix::index::entry::stat::Time {
            secs: clamp_i64_to_u32(statx.stx_ctime.tv_sec),
            nsecs: statx.stx_ctime.tv_nsec,
        },
        mtime: gix::index::entry::stat::Time {
            secs: clamp_i64_to_u32(statx.stx_mtime.tv_sec),
            nsecs: statx.stx_mtime.tv_nsec,
        },
        dev: truncate_u64_to_u32(make_dev(statx.stx_dev_major, statx.stx_dev_minor)),
        ino: truncate_u64_to_u32(statx.stx_ino),
        uid: statx.stx_uid,
        gid: statx.stx_gid,
        size: truncate_u64_to_u32(statx.stx_size),
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

fn clamp_i64_to_u32(value: i64) -> u32 {
    value.clamp(0, u32::MAX as i64) as u32
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
    let index = open_index_file(&index_path)?;
    let paths: Vec<String> = index
        .entries()
        .iter()
        .filter(|entry| entry.flags.contains(SKIP_WORKTREE_FLAG))
        .map(|entry| String::from_utf8_lossy(entry.path_in(index.path_backing())).to_string())
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
    crate::gix_util::resolve_commit(repo, rev)
}

pub fn default_branch<P: AsRef<Path>>(repo: P) -> Result<String> {
    crate::gix_util::default_branch(repo)
}

pub fn last_commits<P: AsRef<Path>>(repo: P, branch: &str, count: usize) -> Result<Vec<String>> {
    crate::validation::validate_git_rev(branch)
        .with_context(|| format!("invalid branch: {}", branch))?;
    crate::gix_util::last_commits(repo, branch, count)
}

pub fn list_object_shas<P: AsRef<Path>>(repo: P, commit: &str) -> Result<Vec<String>> {
    crate::gix_util::list_object_shas_with_depth(repo, commit, None)
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
    crate::gix_util::list_object_shas_in_range(repo, from, to)
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
    crate::gix_util::list_object_shas_with_depth(repo, commit, max_depth)
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
    crate::gix_util::object_sizes(repo, oids)
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
    crate::gix_util::list_tree_entries(repo, commit)
}

/// Classify many objects by type in one batch using temp files.
pub fn classify_objects<P: AsRef<Path>>(
    repo: P,
    shas: &HashSet<String>,
) -> Result<HashMap<String, String>> {
    crate::gix_util::classify_objects(repo, shas)
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

/// Read and encode objects as pack base entries using the persistent rayon pool
/// and per-chunk gix handles. The returned vector is sorted by object id so the
/// final pack is deterministic.
fn encode_objects_parallel(
    repo: &gix::Repository,
    ids: Vec<gix::hash::ObjectId>,
) -> Result<Vec<gix_pack::data::output::Entry>> {
    const PARALLEL_ENCODE_THRESHOLD: usize = 128;

    if ids.len() < PARALLEL_ENCODE_THRESHOLD {
        return ids
            .into_iter()
            .map(|id| {
                let obj = repo
                    .find_object(id)
                    .with_context(|| format!("find object {id}"))?;
                let count = gix_pack::data::output::Count::from_data(id, None);
                let data = gix::objs::Data::new(&obj.data, obj.kind, gix::hash::Kind::Sha1);
                let entry = gix_pack::data::output::Entry::from_data(&count, &data)
                    .with_context(|| format!("encode object {id}"))?;
                Ok(entry)
            })
            .collect::<Result<Vec<_>>>();
    }

    let num_workers = crate::gix_util::worker_threads(
        "RIPCLONE_PACK_ENCODE_THREADS",
        crate::gix_util::default_worker_threads(),
    );
    let repo_path = repo.path().to_path_buf();
    let mut entries: Vec<gix_pack::data::output::Entry> =
        crate::gix_util::parallel_map_repo(repo_path, ids, num_workers, |local_repo, id| {
            let obj = local_repo
                .find_object(*id)
                .with_context(|| format!("find object {id}"))?;
            let count = gix_pack::data::output::Count::from_data(*id, None);
            let data = gix::objs::Data::new(&obj.data, obj.kind, gix::hash::Kind::Sha1);
            let entry = gix_pack::data::output::Entry::from_data(&count, &data)
                .with_context(|| format!("encode object {id}"))?;
            Ok(entry)
        })?;
    entries.sort_by_key(|a| a.id);
    Ok(entries)
}

/// Build a packfile + index containing the given object SHAs using gix instead of
/// the `git pack-objects` subprocess. Objects are stored whole (no deltas) so the
/// output is deterministic for a fixed sorted OID input, but not byte-identical
/// to C-git. The resulting files are named `<prefix>-<packhash>.pack` and
/// `<prefix>-<packhash>.idx`.
pub fn pack_objects_to_prefix_gix<P: AsRef<Path>, Q: AsRef<Path>>(
    repo: P,
    object_shas: &[String],
    prefix: Q,
) -> Result<()> {
    if object_shas.is_empty() {
        bail!("no objects to pack");
    }

    let repo = crate::gix_util::open_repo(repo)?;
    let ids: HashSet<gix::hash::ObjectId> = object_shas
        .iter()
        .map(|s| {
            gix::hash::ObjectId::from_hex(s.as_bytes())
                .with_context(|| format!("invalid object id: {s}"))
        })
        .collect::<Result<_>>()?;
    let mut ids: Vec<_> = ids.into_iter().collect();
    ids.sort();

    let num_entries: u32 = ids.len().try_into().context("too many objects for pack")?;

    // Encode objects as base entries. This is undeltified and therefore
    // deterministic for a sorted input, at the cost of a larger pack than
    // C-git would produce.
    // Stream the pack bytes straight to a temp file so peak memory stays flat
    // regardless of pack size; only the object entries are buffered in memory.
    let entries = encode_objects_parallel(&repo, ids).context("collect objects for gix pack")?;

    if let Some(parent) = prefix.as_ref().parent() {
        std::fs::create_dir_all(parent).context("create pack prefix directory")?;
    }
    let parent = prefix
        .as_ref()
        .parent()
        .context("prefix must have a parent directory")?;
    let base = prefix
        .as_ref()
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("pack");

    let mut tmp_pack = tempfile::Builder::new()
        .prefix(&format!("{}-tmp-", base))
        .suffix(".pack")
        .tempfile_in(parent)
        .context("create temp pack file")?;

    let pack_hash = {
        let input = entries
            .into_iter()
            .map(|e| Ok::<_, std::convert::Infallible>(vec![e]));
        let mut buf = std::io::BufWriter::new(tmp_pack.as_file_mut());
        let mut writer = gix_pack::data::output::bytes::FromEntriesIter::new(
            input,
            &mut buf,
            num_entries,
            gix_pack::data::Version::V2,
            gix::hash::Kind::Sha1,
        );
        for _ in writer.by_ref() {}
        let hash = writer
            .digest()
            .context("gix pack writer did not produce a checksum")?
            .to_hex()
            .to_string();
        drop(writer);
        buf.flush().context("flush pack file")?;
        hash
    };

    let pack_out = parent.join(format!("{}-{}.pack", base, pack_hash));
    let idx_out = pack_out.with_extension("idx");
    tmp_pack
        .persist(&pack_out)
        .with_context(|| format!("rename pack to {}", pack_out.display()))?;

    // Generate the matching .idx file from the just-written pack file.
    index_pack(parent.join(".git"), &pack_out)
        .with_context(|| format!("index pack {} -> {}", pack_out.display(), idx_out.display()))?;
    Ok(())
}

/// Build a `.idx` file for an existing `.pack` using gix instead of the
/// `git index-pack` subprocess. The pack must already be named `pack-<hash>.pack`
/// in its directory; gix will detect it and only write the missing `.idx`.
pub fn index_pack<P: AsRef<Path>, Q: AsRef<Path>>(_git_dir: P, pack_path: Q) -> Result<()> {
    let pack_path = pack_path.as_ref();
    let directory = pack_path
        .parent()
        .context("pack path must have a parent directory")?;
    let mut reader = std::io::BufReader::new(
        std::fs::File::open(pack_path)
            .with_context(|| format!("open pack {}", pack_path.display()))?,
    );
    let mut progress = gix::features::progress::Discard;
    let thread_limit = crate::gix_util::worker_threads(
        "RIPCLONE_GIX_INDEX_THREADS",
        crate::gix_util::default_worker_threads(),
    );
    let outcome = gix_pack::Bundle::write_to_directory(
        &mut reader,
        Some(directory),
        &mut progress,
        &AtomicBool::new(false),
        None::<&gix::Repository>,
        gix_pack::bundle::write::Options {
            thread_limit: Some(thread_limit),
            iteration_mode: gix_pack::data::input::Mode::Verify,
            index_version: gix_pack::index::Version::default(),
            object_hash: gix::hash::Kind::Sha1,
        },
    )
    .context("gix index-pack")?;
    if outcome.index_path.is_none() {
        bail!("gix index-pack produced no index (empty pack?)");
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
    crate::gix_util::ls_tree_sizes(repo, commit)
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
    crate::gix_util::ls_tree_entry(repo, commit, path)
}

pub fn cat_file<P: AsRef<Path>>(repo: P, sha: &str) -> Result<Vec<u8>> {
    crate::validation::validate_object_id(sha)
        .with_context(|| format!("invalid object id: {}", sha))?;
    crate::gix_util::cat_file(repo, sha)
}

/// Fetch the contents of many blob SHAs in a single `git cat-file --batch` call.
///
/// Streams one SHA at a time so the pipe never backs up, which avoids the
/// deadlock risk of buffering the entire output in memory before reading.
pub fn cat_file_batch<P: AsRef<Path>>(
    repo: P,
    shas: &[String],
) -> Result<std::collections::HashMap<String, Vec<u8>>> {
    crate::gix_util::cat_file_batch(repo, shas)
}

pub fn object_type<P: AsRef<Path>>(repo: P, sha: &str) -> Result<String> {
    crate::validation::validate_object_id(sha)
        .with_context(|| format!("invalid object id: {}", sha))?;
    crate::gix_util::object_type(repo, sha)
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
    crate::gix_util::parent_commit(repo, commit)
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

    /// Phase 1 parity harness: gix-based enumeration/metadata must match git(1).
    #[test]
    fn gix_enumeration_parity_with_git() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();

        fn git(repo: &Path, args: &[&str]) -> String {
            let out = Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(args)
                .output()
                .unwrap_or_else(|e| panic!("git {:?} failed: {}", args, e));
            assert!(
                out.status.success(),
                "git {:?} stderr: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8(out.stdout).unwrap().trim().to_string()
        }

        git(repo, &["init", "-q", "-b", "main"]);
        git(repo, &["config", "user.email", "t@t"]);
        git(repo, &["config", "user.name", "t"]);

        std::fs::create_dir_all(repo.join("dir")).unwrap();
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        std::fs::write(repo.join("dir/b.txt"), "b\n").unwrap();
        std::fs::write(repo.join("run.sh"), "#!/bin/sh\necho hi\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut p = std::fs::metadata(repo.join("run.sh"))
                .unwrap()
                .permissions();
            p.set_mode(0o755);
            std::fs::set_permissions(repo.join("run.sh"), p).unwrap();
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink("a.txt", repo.join("link-a")).unwrap();

        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", "c1"]);

        std::fs::write(repo.join("c.txt"), "c\n").unwrap();
        git(repo, &["add", "c.txt"]);
        git(repo, &["commit", "-q", "-m", "c2"]);

        // resolve / default branch / parent / last_commits
        assert_eq!(
            resolve_commit(repo, "HEAD").unwrap(),
            git(repo, &["rev-parse", "HEAD"])
        );
        assert_eq!(default_branch(repo).unwrap(), "main");
        assert_eq!(
            parent_commit(repo, "HEAD").unwrap(),
            Some(git(repo, &["rev-parse", "HEAD^"]))
        );
        assert_eq!(
            last_commits(repo, "HEAD", 2).unwrap(),
            git(repo, &["log", "--format=%H", "--first-parent", "-n", "2"])
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );

        // Object sets must include commit + tree/blob closure.
        let expect_full: HashSet<String> = git(
            repo,
            &["rev-list", "--objects", "--no-object-names", "HEAD"],
        )
        .lines()
        .map(|s| s.to_string())
        .collect();
        let got_full: HashSet<String> = list_object_shas(repo, "HEAD")
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(got_full, expect_full, "full object set mismatch");

        let expect_depth1: HashSet<String> = git(
            repo,
            &[
                "rev-list",
                "-n",
                "1",
                "--objects",
                "--no-object-names",
                "HEAD",
            ],
        )
        .lines()
        .map(|s| s.to_string())
        .collect();
        let got_depth1: HashSet<String> = list_object_shas_with_depth(repo, "HEAD", Some(1))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(got_depth1, expect_depth1, "depth=1 object set mismatch");

        // Tree entries.
        let expect_entries: HashSet<(String, String, String, String)> = {
            let out = git(repo, &["ls-tree", "-r", "-z", "HEAD"]);
            out.split('\0')
                .filter(|r| !r.is_empty())
                .map(|record| {
                    let tab = record.rfind('\t').unwrap();
                    let path = record[tab + 1..].to_string();
                    let meta: Vec<&str> = record[..tab].split_whitespace().collect();
                    (
                        path,
                        meta[0].to_string(),
                        meta[2].to_string(),
                        meta[1].to_string(),
                    )
                })
                .collect()
        };
        let got_entries: HashSet<(String, String, String, String)> =
            list_tree_entries(repo, "HEAD")
                .unwrap()
                .into_iter()
                .collect();
        assert_eq!(got_entries, expect_entries, "tree entry set mismatch");

        // Modes for special file types are preserved by the gix walk.
        let entry_map: HashMap<String, String> = got_entries
            .iter()
            .map(|(path, mode, _, _)| (path.clone(), mode.clone()))
            .collect();
        assert_eq!(entry_map.get("run.sh").map(String::as_str), Some("100755"));
        assert_eq!(entry_map.get("link-a").map(String::as_str), Some("120000"));

        // ls-tree sizes.
        let expect_sizes: HashMap<String, u64> = {
            let out = git(repo, &["ls-tree", "-r", "-l", "-z", "HEAD"]);
            out.split('\0')
                .filter(|r| !r.is_empty())
                .filter_map(|record| {
                    let tab = record.rfind('\t').unwrap();
                    let path = record[tab + 1..].to_string();
                    let meta: Vec<&str> = record[..tab].split_whitespace().collect();
                    if meta.len() < 4 || meta[1] != "blob" {
                        return None;
                    }
                    meta[3].parse::<u64>().ok().map(|s| (path, s))
                })
                .collect()
        };
        assert_eq!(ls_tree_sizes(repo, "HEAD").unwrap(), expect_sizes);

        // Single entry lookup.
        assert_eq!(
            ls_tree_entry(repo, "HEAD", "dir/b.txt").unwrap(),
            Some((
                "100644".to_string(),
                git(repo, &["rev-parse", "HEAD:dir/b.txt"])
            ))
        );

        // Type/size/classify.
        let types = classify_objects(repo, &expect_full).unwrap();
        for sha in &expect_full {
            let want_type = git(repo, &["cat-file", "-t", sha]);
            assert_eq!(object_type(repo, sha).unwrap(), want_type);
            assert_eq!(types.get(sha).unwrap(), &want_type);
        }
        let sizes = object_sizes(repo, &expect_full.iter().cloned().collect::<Vec<_>>()).unwrap();
        for sha in &expect_full {
            let want_size = git(repo, &["cat-file", "-s", sha])
                .trim()
                .parse::<u64>()
                .unwrap();
            assert_eq!(sizes.get(sha).copied().unwrap(), want_size);
        }

        // Blob content.
        let blob_sha = git(repo, &["rev-parse", "HEAD:dir/b.txt"]);
        assert_eq!(cat_file(repo, &blob_sha).unwrap(), b"b\n"[..]);
        let batch = cat_file_batch(repo, std::slice::from_ref(&blob_sha)).unwrap();
        assert_eq!(batch.get(&blob_sha).unwrap(), &b"b\n"[..]);
    }

    /// Submodule entries (mode 160000) must not appear in the worktree file list.
    #[test]
    fn gix_list_tree_entries_skips_submodules() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init_bare(tmp.path()).unwrap();
        let sig = git2::Signature::now("test", "test@example.com").unwrap();
        let zero = git2::IndexTime::new(0, 0);

        let blob_oid = repo.blob(b"submodule-readme").unwrap();
        let mut idx = repo.index().unwrap();
        idx.add(&git2::IndexEntry {
            ctime: zero,
            mtime: zero,
            dev: 0,
            ino: 0,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            file_size: 16,
            id: blob_oid,
            flags: 0,
            flags_extended: 0,
            path: b"README.md".to_vec(),
        })
        .unwrap();
        idx.write().unwrap();
        let sub_tree = repo.find_tree(idx.write_tree().unwrap()).unwrap();
        let sub_commit = repo
            .commit(None, &sig, &sig, "submodule", &sub_tree, &[])
            .unwrap();

        let file_blob = repo.blob(b"file").unwrap();
        let empty_tree = repo.treebuilder(None).unwrap().write().unwrap();
        let empty_tree = repo.find_tree(empty_tree).unwrap();
        let mut builder = git2::build::TreeUpdateBuilder::new();
        builder.upsert("file.txt", file_blob, git2::FileMode::Blob);
        builder.upsert("vendor/sub", sub_commit, git2::FileMode::Commit);
        let tree_oid = builder.create_updated(&repo, &empty_tree).unwrap();

        let commit = repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "main",
                &repo.find_tree(tree_oid).unwrap(),
                &[],
            )
            .unwrap();

        let entries = list_tree_entries(tmp.path(), &commit.to_string()).unwrap();
        assert_eq!(entries.len(), 1, "submodule entry must be skipped");
        assert_eq!(entries[0].0, "file.txt");
        assert_eq!(entries[0].1, "100644");
    }

    /// Negative case: asking for a non-existent object must fail cleanly.
    #[test]
    fn gix_cat_file_rejects_missing_object() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git").arg("init").arg(repo).output().unwrap();
        assert!(
            cat_file(repo, "0000000000000000000000000000000000000000").is_err(),
            "missing object should error"
        );
    }

    /// gix pack encode produces a deterministic, valid pack+idx pair.
    #[test]
    fn gix_pack_encode_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "config", "user.email", "t@t"])
            .output()
            .unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "config", "user.name", "t"])
            .output()
            .unwrap();
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "add", "a.txt"])
            .output()
            .unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "commit", "-q", "-m", "c1"])
            .output()
            .unwrap();

        let objects: Vec<String> = {
            let out = Command::new("git")
                .args([
                    "-C",
                    repo.to_str().unwrap(),
                    "rev-list",
                    "--objects",
                    "--no-object-names",
                    "HEAD",
                ])
                .output()
                .unwrap();
            String::from_utf8(out.stdout)
                .unwrap()
                .lines()
                .map(|s| s.to_string())
                .collect()
        };

        let out_dir = tempfile::tempdir().unwrap();
        let prefix = out_dir.path().join("pack");
        pack_objects_to_prefix_gix(repo, &objects, &prefix).unwrap();

        let mut pack_file = None;
        let mut idx_file = None;
        for entry in std::fs::read_dir(out_dir.path()).unwrap() {
            let path = entry.unwrap().path();
            match path.extension().and_then(|e| e.to_str()) {
                Some("pack") => pack_file = Some(path),
                Some("idx") => idx_file = Some(path),
                _ => {}
            }
        }
        assert!(pack_file.is_some(), "pack file written");
        assert!(idx_file.is_some(), "idx file written");

        // Determinism: the same input produces the same pack hash.
        let out_dir2 = tempfile::tempdir().unwrap();
        let prefix2 = out_dir2.path().join("pack");
        pack_objects_to_prefix_gix(repo, &objects, &prefix2).unwrap();
        let hash1 = crate::archive::sha1_bytes(&std::fs::read(pack_file.unwrap()).unwrap());
        let pack2 = std::fs::read_dir(out_dir2.path())
            .unwrap()
            .find(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .and_then(|e| e.to_str())
                    == Some("pack")
            })
            .unwrap()
            .unwrap()
            .path();
        let hash2 = crate::archive::sha1_bytes(&std::fs::read(&pack2).unwrap());
        assert_eq!(hash1, hash2, "gix pack encode must be deterministic");

        // Validity: gix can open the generated bundle and iterate every object.
        let bundle = gix_pack::Bundle::at(idx_file.unwrap(), gix::hash::Kind::Sha1).unwrap();
        assert_eq!(bundle.index.num_objects() as usize, objects.len());
        for oid in &objects {
            let id = gix::hash::ObjectId::from_hex(oid.as_bytes()).unwrap();
            assert!(
                bundle.index.lookup(id).is_some(),
                "generated index must contain {oid}"
            );
        }
    }

    /// gix pack encode fails with a useful error when an object is missing.
    #[test]
    fn gix_pack_encode_fails_on_missing_object() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git").arg("init").arg(repo).output().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let prefix = out_dir.path().join("pack");
        let bad = vec!["0000000000000000000000000000000000000000".to_string()];
        assert!(pack_objects_to_prefix_gix(repo, &bad, &prefix).is_err());
    }

    /// gix pack encode deduplicates duplicate OIDs and rejects invalid hex.
    #[test]
    fn gix_pack_encode_dedup_and_invalid_hex() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();
        for (key, val) in [("user.email", "t@t"), ("user.name", "t")] {
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", key, val])
                .output()
                .unwrap();
        }
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "add", "a.txt"])
            .output()
            .unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "commit", "-q", "-m", "c"])
            .output()
            .unwrap();
        let out = Command::new("git")
            .args([
                "-C",
                repo.to_str().unwrap(),
                "rev-list",
                "--objects",
                "--no-object-names",
                "HEAD",
            ])
            .output()
            .unwrap();
        let objects: Vec<String> = String::from_utf8(out.stdout)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect();
        assert!(!objects.is_empty());
        // Duplicate every OID.
        let duplicated: Vec<String> = objects
            .iter()
            .cloned()
            .chain(objects.iter().cloned())
            .collect();

        let out_dir = tempfile::tempdir().unwrap();
        let prefix = out_dir.path().join("pack");
        pack_objects_to_prefix_gix(repo, &duplicated, &prefix).unwrap();
        let idx_file = std::fs::read_dir(out_dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().and_then(|e| e.to_str()) == Some("idx"))
            .unwrap();
        let bundle = gix_pack::Bundle::at(&idx_file, gix::hash::Kind::Sha1).unwrap();
        assert_eq!(bundle.index.num_objects() as usize, objects.len());

        assert!(pack_objects_to_prefix_gix(repo, &["zzzz".to_string()], &prefix).is_err());
    }

    /// gix index generation for an existing pack (Phase 3) produces an idx that
    /// git can use to read objects.
    #[test]
    fn gix_index_pack_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();
        for (key, val) in [("user.email", "t@t"), ("user.name", "t")] {
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", key, val])
                .output()
                .unwrap();
        }
        std::fs::write(repo.join("file.txt"), "hello\n").unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "add", "file.txt"])
            .output()
            .unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "commit", "-q", "-m", "c"])
            .output()
            .unwrap();

        // Build a pack with C-git, then index it with gix.
        let pack_dir = tmp.path().join("packs");
        std::fs::create_dir(&pack_dir).unwrap();
        let pack_prefix = pack_dir.join("objects");
        let out = Command::new("git")
            .args([
                "-C",
                repo.to_str().unwrap(),
                "rev-list",
                "--objects",
                "--no-object-names",
                "HEAD",
            ])
            .output()
            .unwrap();
        let objects: Vec<String> = String::from_utf8(out.stdout)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect();
        let mut child = std::process::Command::new("git")
            .args([
                "-C",
                repo.to_str().unwrap(),
                "pack-objects",
                "--window=0",
                pack_prefix.to_str().unwrap(),
            ])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        {
            use std::io::Write;
            let stdin = child.stdin.as_mut().unwrap();
            for oid in &objects {
                writeln!(stdin, "{}", oid).unwrap();
            }
        }
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());

        let pack_file = std::fs::read_dir(&pack_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().and_then(|e| e.to_str()) == Some("pack"))
            .unwrap();
        index_pack(repo.join(".git"), &pack_file).unwrap();

        let idx_file = pack_file.with_extension("idx");
        assert!(idx_file.exists(), "gix index_pack must write .idx");

        let bundle = gix_pack::Bundle::at(&idx_file, gix::hash::Kind::Sha1).unwrap();
        assert_eq!(bundle.index.num_objects() as usize, objects.len());
        for oid in &objects {
            let id = gix::hash::ObjectId::from_hex(oid.as_bytes()).unwrap();
            assert!(
                bundle.index.lookup(id).is_some(),
                "gix-generated index must contain {oid}"
            );
        }
    }

    /// gix index generation rejects a corrupt/truncated pack.
    #[test]
    fn gix_index_pack_rejects_truncated() {
        let tmp = tempfile::tempdir().unwrap();
        let pack_dir = tmp.path().join("packs");
        std::fs::create_dir(&pack_dir).unwrap();
        let pack_file = pack_dir.join("pack-0000000000000000000000000000000000000000.pack");
        std::fs::write(&pack_file, b"PKT\x00\x00").unwrap();
        assert!(index_pack(pack_dir.join(".git"), &pack_file).is_err());
    }

    /// Setting and clearing skip-worktree via gix (Phase 5) round-trips through
    /// a real git index.
    #[test]
    fn gix_index_skip_worktree_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();
        for (key, val) in [("user.email", "t@t"), ("user.name", "t")] {
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", key, val])
                .output()
                .unwrap();
        }
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        std::fs::write(repo.join("b.txt"), "b\n").unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "add", "."])
            .output()
            .unwrap();

        set_skip_worktree_all(repo).unwrap();
        assert_skip_worktree_flag(repo, "a.txt", true);
        assert_skip_worktree_flag(repo, "b.txt", true);

        let cleared = clear_skip_worktree_all(repo).unwrap();
        assert_eq!(cleared, 2);
        assert_skip_worktree_flag(repo, "a.txt", false);
        assert_skip_worktree_flag(repo, "b.txt", false);
    }

    fn assert_skip_worktree_flag(repo: &Path, path: &str, expected: bool) {
        let out = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "ls-files", "-v", "--", path])
            .output()
            .unwrap();
        let line = String::from_utf8(out.stdout).unwrap();
        let flag = line.starts_with('S');
        assert_eq!(
            flag, expected,
            "skip-worktree for {}: expected {}, got {} ({:?})",
            path, expected, flag, line
        );
    }

    /// Clearing skip-worktree and refreshing stats via gix (Phase 5) writes
    /// real stat metadata back into the index.
    #[test]
    fn gix_index_clear_skip_worktree_with_stats() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();
        for (key, val) in [("user.email", "t@t"), ("user.name", "t")] {
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", key, val])
                .output()
                .unwrap();
        }
        std::fs::write(repo.join("a.txt"), "aaaa\n").unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "add", "a.txt"])
            .output()
            .unwrap();

        set_skip_worktree_all(repo).unwrap();

        let stats = vec![materialized_path_stat_from_metadata(
            "a.txt".to_string(),
            &std::fs::symlink_metadata(repo.join("a.txt")).unwrap(),
        )];
        clear_skip_worktree_index_with_stats(repo, &["a.txt".to_string()], &stats).unwrap();

        assert_skip_worktree_flag(repo, "a.txt", false);

        // Index should now reflect the real on-disk size.
        let index = open_index_file(&repo.join(".git").join("index")).unwrap();
        let entry = index
            .entry_by_path_and_stage(
                gix::bstr::BStr::new(b"a.txt"),
                gix::index::entry::Stage::Unconflicted,
            )
            .expect("a.txt in index");
        assert_eq!(entry.stat.size, 5);
    }

    /// update_index_sizes via gix (Phase 5) updates cached sizes and leaves
    /// non-targeted entries untouched.
    #[test]
    fn gix_update_index_sizes() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();
        for (key, val) in [("user.email", "t@t"), ("user.name", "t")] {
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", key, val])
                .output()
                .unwrap();
        }
        std::fs::write(repo.join("a.txt"), "aaaa\n").unwrap();
        std::fs::write(repo.join("b.txt"), "bbbbbbbb\n").unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "add", "."])
            .output()
            .unwrap();

        let mut sizes = std::collections::HashMap::new();
        sizes.insert("a.txt".to_string(), 42u64);
        update_index_sizes(repo.join(".git"), &sizes).unwrap();

        let index = open_index_file(&repo.join(".git").join("index")).unwrap();
        let a = index
            .entry_by_path_and_stage(
                gix::bstr::BStr::new(b"a.txt"),
                gix::index::entry::Stage::Unconflicted,
            )
            .unwrap();
        let b = index
            .entry_by_path_and_stage(
                gix::bstr::BStr::new(b"b.txt"),
                gix::index::entry::Stage::Unconflicted,
            )
            .unwrap();
        assert_eq!(a.stat.size, 42);
        assert_eq!(b.stat.size, 9); // untouched
    }

    /// Basic metadata queries via gix (Phase 1).
    #[test]
    fn gix_basic_metadata_queries() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();
        for (key, val) in [("user.email", "t@t"), ("user.name", "t")] {
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", key, val])
                .output()
                .unwrap();
        }
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "add", "a.txt"])
            .output()
            .unwrap();
        let _ = Command::new("git")
            .args(["-C", repo.to_str().unwrap(), "commit", "-q", "-m", "c1"])
            .output()
            .unwrap();

        assert_eq!(default_branch(repo).unwrap(), "main");

        let commit = resolve_commit(repo, "HEAD").unwrap();
        assert_eq!(resolve_commit(repo, "main").unwrap(), commit);

        assert_eq!(parent_commit(repo, &commit).unwrap(), None);

        let commits = last_commits(repo, "main", 1).unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0], commit);

        let empty = last_commits(repo, "main", 0).unwrap();
        assert!(empty.is_empty());

        // ls-tree entry and symlink detection.
        let entry = ls_tree_entry(repo, "HEAD", "a.txt").unwrap();
        assert!(entry.is_some(), "ls_tree_entry should find a.txt");
        let (mode, sha) = entry.unwrap();
        assert_eq!(mode, "100644");
        assert!(!sha.is_empty());

        let missing = ls_tree_entry(repo, "HEAD", "does-not-exist.txt").unwrap();
        assert!(missing.is_none());

        let symlinks = symlink_blob_shas(repo, "HEAD").unwrap();
        assert!(symlinks.is_empty());

        let oids = list_object_shas(repo, &commit).unwrap();
        assert!(!oids.is_empty());
    }

    /// Negative cases for gix metadata queries (Phase 1).
    #[test]
    fn gix_metadata_negative_cases() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();

        assert!(resolve_commit(repo, "not-a-real-ref").is_err());
        assert!(resolve_commit(repo, "HEAD").is_err()); // no commits yet
        assert!(last_commits(repo, "main", 1).is_err());
        assert!(ls_tree_entry(repo, "HEAD", "x").is_err());

        let not_repo = tempfile::tempdir().unwrap();
        assert!(crate::gix_util::open_repo(not_repo.path()).is_err());
    }

    /// A commit chain: parent_commit and last_commits order (Phase 1).
    #[test]
    fn gix_commit_chain_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let _ = Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(repo)
            .output()
            .unwrap();
        for (key, val) in [("user.email", "t@t"), ("user.name", "t")] {
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "config", key, val])
                .output()
                .unwrap();
        }
        let mut shas = Vec::new();
        for i in 0..3 {
            std::fs::write(repo.join("f.txt"), format!("{i}\n")).unwrap();
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "add", "f.txt"])
                .output()
                .unwrap();
            let out = Command::new("git")
                .args([
                    "-C",
                    repo.to_str().unwrap(),
                    "commit",
                    "-q",
                    "-m",
                    &format!("c{i}"),
                ])
                .output()
                .unwrap();
            assert!(out.status.success());
            let out = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "rev-parse", "HEAD"])
                .output()
                .unwrap();
            shas.push(String::from_utf8(out.stdout).unwrap().trim().to_string());
        }

        assert_eq!(
            parent_commit(repo, &shas[2]).unwrap().as_deref(),
            Some(shas[1].as_str())
        );
        assert_eq!(parent_commit(repo, &shas[0]).unwrap(), None);

        let last = last_commits(repo, "main", 2).unwrap();
        assert_eq!(last, vec![shas[2].clone(), shas[1].clone()]);

        let range = list_object_shas_in_range(repo, Some(shas[1].as_str()), &shas[2]).unwrap();
        assert!(!range.is_empty());
        assert!(range.contains(&shas[2]));
    }
}
