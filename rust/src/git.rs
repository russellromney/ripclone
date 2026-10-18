use crate::provider::{ProviderInstance, RepoId};
use anyhow::{Context, Result, bail};
use secrecy::ExposeSecret;
use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

const SKIP_WORKTREE_FLAG: gix::index::entry::Flags = gix::index::entry::Flags::SKIP_WORKTREE;

/// `git` for a child of this process. Every git the server runs starts here.
///
/// The command must stay on std's `posix_spawn` path: no `pre_exec`, `uid`,
/// `gid`, `groups`, `setsid`, `env_clear`, or `PATH` change. Any of those
/// makes std `fork()` the whole server, which copies its page tables on every
/// spawn and turns a Head build's hundreds of `pack-objects` into seconds of
/// overhead at a few GB of RSS. `process_group` and `current_dir` keep
/// `posix_spawn` (glibc 2.29+, musl, macOS).
/// `tests::server_git_spawns_do_not_fork` checks this.
///
/// A server killed with SIGKILL leaves its git children running until their
/// current command ends, on every OS.
pub fn command() -> Command {
    Command::new("git")
}

/// Skip git's own fsyncs. Packs and indexes the server builds are scratch
/// files until the CAS installs them, and a durable CAS syncs them itself.
const NO_FSYNC: [&str; 2] = ["-c", "core.fsync=none"];

/// Abort an HTTP transfer that moves under 1000 bytes/s for 60 s. A stalled
/// upstream then fails the fetch or ls-remote instead of holding it open.
const LOW_SPEED_ARGS: [&str; 4] = [
    "-c",
    "http.lowSpeedLimit=1000",
    "-c",
    "http.lowSpeedTime=60",
];

/// A git child run by [`run_tracked`].
struct Tracked {
    ended: crate::cancel::Ended,
    stderr: Vec<u8>,
}

/// Run `command` in its own process group with stdin closed, stdout
/// discarded and stderr captured to a file (so a long diagnostic cannot
/// block the child). The group is killed when the current build is
/// cancelled or `timeout` passes.
fn run_tracked(command: &mut Command, timeout: Option<Duration>) -> std::io::Result<Tracked> {
    let mut stderr = tempfile::tempfile()?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr.try_clone()?));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn()?;
    let ended = crate::cancel::wait_child(&mut child, timeout)?;
    stderr.rewind()?;
    let mut bytes = Vec::new();
    stderr.read_to_end(&mut bytes)?;
    Ok(Tracked {
        ended,
        stderr: bytes,
    })
}

/// [`run_tracked`] for local maintenance: an error carries git's stderr, a
/// timeout or a cancelled build is an error too.
fn run_quiet_bounded(command: &mut Command, what: &str, timeout: Duration) -> Result<()> {
    run_checked(command, what, Some(timeout))
}

/// [`run_tracked`] with its outcome as a `Result`: an error carries git's
/// stderr, and a timeout or a cancelled build is an error too.
fn run_checked(command: &mut Command, what: &str, timeout: Option<Duration>) -> Result<()> {
    use crate::cancel::Ended;
    let run = run_tracked(command, timeout).with_context(|| format!("run git {what}"))?;
    match run.ended {
        Ended::Exited(status) if status.success() => Ok(()),
        Ended::Exited(_) => bail!(
            "git {what} failed: {}",
            String::from_utf8_lossy(&run.stderr).trim()
        ),
        Ended::TimedOut => bail!("git {what} timed out after {timeout:?}"),
        Ended::Cancelled => Err(anyhow::Error::new(crate::cancel::Cancelled)),
    }
}

/// Fail before starting work that requires the system Git executable.
pub fn require_system_git() -> Result<()> {
    let status = command()
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("system Git is required; install `git` and ensure it is on PATH")?;
    if !status.success() {
        bail!(
            "system Git is required; `git --version` exited with {status}; install Git and ensure it is on PATH"
        );
    }
    Ok(())
}

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

/// Write the index of `commit`'s tree to `index_path`, read from the bare
/// mirror `repo` (`git read-tree` with `GIT_INDEX_FILE`, no work tree). Every
/// entry is skip-worktree, so git treats the paths as present before the
/// client writes them. Each file entry carries the blob's size and fixed stat
/// times, so `git status` can trust stat(2) once the client has written the
/// file and cleared the bit. Paths stay raw bytes, as git stores them.
pub fn write_prebuilt_index(repo: &Path, commit: &str, index_path: &Path) -> Result<()> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {commit}"))?;
    let mut read_tree = command();
    read_tree
        .args(NO_FSYNC)
        .arg("-C")
        .arg(repo)
        .env("GIT_INDEX_FILE", index_path)
        .args(["read-tree", "--end-of-options", commit]);
    run_checked(&mut read_tree, "read-tree", None)?;

    let objects = crate::gix_util::open_repo(repo)?;
    let mut index = open_index_file(index_path)?;
    for entry in index.entries_mut() {
        if entry.mode != gix::index::entry::Mode::COMMIT {
            let size = objects
                .find_header(entry.id)
                .with_context(|| format!("find header {}", entry.id))?
                .size();
            entry.stat = gix::index::entry::Stat {
                size: clamp_u64_to_u32(size),
                ctime: gix::index::entry::stat::Time { secs: 1, nsecs: 0 },
                mtime: gix::index::entry::stat::Time { secs: 1, nsecs: 0 },
                ..Default::default()
            };
        }
        entry
            .flags
            .insert(SKIP_WORKTREE_FLAG | gix::index::entry::Flags::EXTENDED);
    }
    write_index_file(&mut index)
}

pub fn clear_skip_worktree_index_with_stats_byte_iter<'a, P, I>(
    repo_dir: P,
    paths: I,
    stats: &[MaterializedPathStat],
) -> Result<()>
where
    P: AsRef<Path>,
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut paths = paths.into_iter().peekable();
    if paths.peek().is_none() {
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

    let stats_by_path: HashMap<&[u8], &IndexStat> =
        stats.iter().map(|s| (s.path.as_slice(), &s.stat)).collect();
    let mut changed = false;
    for path in paths {
        let Some(entry) = index.entry_mut_by_path_and_stage(
            gix::bstr::BStr::new(path),
            gix::index::entry::Stage::Unconflicted,
        ) else {
            continue;
        };
        let stat = if let Some(stat) = stats_by_path.get(path) {
            **stat
        } else {
            let full_path = repo_dir.join(index_path_from_bytes(path));
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

#[derive(Debug, PartialEq, Eq)]
pub struct MaterializedPathStat {
    pub path: Vec<u8>,
    stat: IndexStat,
}

type IndexStat = gix::index::entry::Stat;

pub fn materialized_path_stat_from_metadata(
    path: Vec<u8>,
    metadata: &std::fs::Metadata,
) -> MaterializedPathStat {
    MaterializedPathStat {
        path,
        stat: index_stat_from_metadata(metadata),
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
            nsecs: clamp_i64_to_u32(metadata.ctime_nsec()),
        },
        mtime: gix::index::entry::stat::Time {
            secs: clamp_i64_to_u32(metadata.mtime()),
            nsecs: clamp_i64_to_u32(metadata.mtime_nsec()),
        },
        dev: clamp_u64_to_u32(metadata.dev()),
        ino: clamp_u64_to_u32(metadata.ino()),
        uid: metadata.uid(),
        gid: metadata.gid(),
        size: clamp_u64_to_u32(metadata.len()),
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
        size: clamp_u64_to_u32(metadata.len()),
    }
}

fn clamp_i64_to_u32(value: i64) -> u32 {
    u32::try_from(value).unwrap_or(if value.is_negative() { 0 } else { u32::MAX })
}

fn clamp_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Run a git command with its output captured, so nothing it prints reaches
/// the server log. On failure the error carries git's stderr.
fn run_quiet(command: &mut Command, what: &str) -> Result<()> {
    let output = command
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("spawn git {what}"))?;
    if !output.status.success() {
        bail!(
            "git {what} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Run a git command in a repo and return stdout as String.
pub fn run_git<P: AsRef<Path>>(repo: P, args: &[&str]) -> Result<String> {
    let output = command()
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

#[derive(Debug)]
pub struct UpstreamGitError {
    op: &'static str,
    retryable: bool,
    /// A sanitized snippet of git's own stderr, so the surfaced error names the
    /// real cause (auth failure, missing repo, HTTP status) instead of a terse
    /// "clone mirror failed". Any credential is redacted before it lands here.
    detail: Option<String>,
}

impl UpstreamGitError {
    /// Test-only constructor for the detail-free case (the classify_build_error
    /// unit tests only care about the retryable flag, not git's stderr).
    #[cfg(test)]
    pub(crate) fn new(op: &'static str, retryable: bool) -> Self {
        Self {
            op,
            retryable,
            detail: None,
        }
    }

    pub(crate) fn with_detail(op: &'static str, retryable: bool, detail: Option<String>) -> Self {
        Self {
            op,
            retryable,
            detail,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl std::fmt::Display for UpstreamGitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.detail {
            Some(detail) => write!(f, "{} failed: {}", self.op, detail),
            None => write!(f, "{} failed", self.op),
        }
    }
}

impl std::error::Error for UpstreamGitError {}

pub fn resolve_commit<P: AsRef<Path>>(repo: P, rev: &str) -> Result<String> {
    crate::validation::validate_git_rev(rev).with_context(|| format!("invalid rev: {}", rev))?;
    crate::gix_util::resolve_commit(repo, rev)
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
    let output = command()
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

/// Whether `ancestor` is an ancestor of (or equal to) `commit`. An error means
/// git could not answer, for example because `ancestor` is not in the repo.
pub fn is_ancestor<P: AsRef<Path>>(repo: P, ancestor: &str, commit: &str) -> Result<bool> {
    crate::validation::validate_git_rev(ancestor)
        .with_context(|| format!("invalid commit: {ancestor}"))?;
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {commit}"))?;
    let output = command()
        .arg("-C")
        .arg(repo.as_ref().as_os_str())
        .args(["merge-base", "--is-ancestor", "--end-of-options"])
        .args([ancestor, commit])
        .stdin(Stdio::null())
        .output()
        .context("run git merge-base --is-ancestor")?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => bail!(
            "git merge-base --is-ancestor {ancestor} {commit} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

/// The commits of `commits` that no other commit of the list reaches: the
/// newest ones.
pub fn independent_commits<P: AsRef<Path>>(
    repo: P,
    commits: &[String],
) -> Result<std::collections::HashSet<String>> {
    for commit in commits {
        crate::validation::validate_git_rev(commit)
            .with_context(|| format!("invalid commit: {commit}"))?;
    }
    let mut args = vec!["merge-base", "--independent", "--end-of-options"];
    args.extend(commits.iter().map(String::as_str));
    Ok(run_git(repo, &args)?
        .lines()
        .map(|line| line.trim().to_string())
        .collect())
}

/// Number of commits reachable from `commit` but not from `base`.
pub fn count_commits_since<P: AsRef<Path>>(repo: P, base: &str, commit: &str) -> Result<u64> {
    crate::validation::validate_git_rev(base).with_context(|| format!("invalid commit: {base}"))?;
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {commit}"))?;
    let count = run_git(
        repo,
        &[
            "rev-list",
            "--count",
            "--end-of-options",
            commit,
            &format!("^{base}"),
        ],
    )?;
    count
        .parse()
        .with_context(|| format!("parse rev-list count {count:?}"))
}

/// The objects of `commit`'s depth-1 closure in path order. See
/// [`crate::gix_util::list_head_objects`].
pub fn list_head_objects<P: AsRef<Path>>(repo: P, commit: &str) -> Result<Vec<String>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    crate::gix_util::list_head_objects(repo, commit)
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
    run_quiet(
        command()
            .arg("-C")
            .arg(repo_dir.as_ref().as_os_str())
            .args(["multi-pack-index", "write"]),
        "multi-pack-index write",
    )
}

/// Write a commit-graph over all reachable commits so the many `rev-list` /
/// graph walks during a build (skeleton + layered packs) don't re-parse commit
/// objects from the packfile each time. A fresh mirror has none. Best-effort:
/// a failure only loses the speedup.
///
/// Uses `--split`: only the new commits are written into a new graph layer
/// instead of rewriting the whole graph, so this stays O(new commits). Safe
/// beside readers (see `tests::concurrent_prep_and_reads_stay_safe`).
pub fn write_commit_graph<P: AsRef<Path>>(repo_dir: P, timeout: Duration) -> Result<()> {
    run_quiet_bounded(
        command()
            .arg("-C")
            .arg(repo_dir.as_ref().as_os_str())
            .args([
                "commit-graph",
                "write",
                "--reachable",
                "--split",
                "--no-progress",
            ]),
        "commit-graph write",
        timeout,
    )
}

/// Packs in a mirror above which [`compact_mirror`] repacks.
const MIRROR_PACK_LIMIT: usize = 16;
/// Loose refs under `refs/ripclone/revs` above which [`compact_mirror`]
/// packs refs.
const MIRROR_LOOSE_REF_LIMIT: usize = 64;

/// What [`compact_mirror`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MirrorCompaction {
    pub repacked: bool,
    pub packed_refs: bool,
}

/// Keep a mirror's pack and loose ref counts bounded. Every admitted commit
/// is fetched as one pack and kept under one loose ref, and auto-gc is off,
/// so both grow with every push. Past [`MIRROR_PACK_LIMIT`] packs, a
/// geometric repack merges the small packs (the large base pack is rarely
/// rewritten) and writes a multi-pack-index; past
/// [`MIRROR_LOOSE_REF_LIMIT`] loose refs, they are packed.
///
/// `repack -d` deletes packs a reader may be about to open, so the caller
/// must hold the mirror exclusively: no build reading it, no fetch. Both
/// steps share one `timeout`; a step killed by it leaves the mirror valid.
pub fn compact_mirror(mirror_dir: &Path, timeout: Duration) -> Result<MirrorCompaction> {
    let deadline = std::time::Instant::now() + timeout;
    let remaining = || deadline.saturating_duration_since(std::time::Instant::now());
    let mut done = MirrorCompaction::default();
    let packs = count_entries(&mirror_dir.join("objects").join("pack"), |name| {
        name.ends_with(".pack")
    });
    if packs > MIRROR_PACK_LIMIT {
        run_quiet_bounded(
            command().arg("-C").arg(mirror_dir.as_os_str()).args([
                "repack",
                "--geometric=2",
                "-d",
                "--write-midx",
                "--no-write-bitmap-index",
                "-q",
            ]),
            "repack --geometric",
            remaining(),
        )?;
        done.repacked = true;
    }
    let loose_refs = count_entries(
        &mirror_dir.join("refs").join("ripclone").join("revs"),
        |_| true,
    );
    if loose_refs > MIRROR_LOOSE_REF_LIMIT {
        run_quiet_bounded(
            command()
                .arg("-C")
                .arg(mirror_dir.as_os_str())
                .args(["pack-refs", "--all"]),
            "pack-refs",
            remaining(),
        )?;
        done.packed_refs = true;
    }
    Ok(done)
}

/// Entries of `dir` whose names `keep` accepts; 0 when `dir` is missing.
fn count_entries(dir: &Path, keep: impl Fn(&str) -> bool) -> usize {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries
            .flatten()
            .filter(|entry| entry.file_name().to_str().is_some_and(&keep))
            .count()
    })
}

/// Build a multi-pack-index over `packs` (each a `(pack, idx)` pair of file
/// paths) in a new bare repo at `dir`, and return the index file's path. The
/// packs are hard-linked in (copied only where linking fails) with the same
/// `pack-<trailer>.{pack,idx}` filenames the client uses, so the MIDX
/// references them by those names and is byte-for-byte usable on the client
/// once it installs the same packs — no client-side `git multi-pack-index
/// write` needed. Git reads the idx files, never the whole packs.
pub fn write_multi_pack_index_over(packs: &[(PathBuf, PathBuf)], dir: &Path) -> Result<PathBuf> {
    use std::io::SeekFrom;
    if packs.is_empty() {
        anyhow::bail!("no packs to index");
    }
    // Minimal bare object store so `git multi-pack-index write` has a repo and
    // an `objects/pack` directory laid out exactly like the client's.
    run_quiet(
        command()
            .args(NO_FSYNC)
            .args(["init", "--bare", "-q"])
            .arg(dir.as_os_str()),
        "init --bare for midx",
    )?;
    let pack_dir = dir.join("objects").join("pack");
    std::fs::create_dir_all(&pack_dir)?;
    let link = |from: &Path, to: &Path| -> Result<()> {
        if std::fs::hard_link(from, to).is_err() {
            std::fs::copy(from, to).with_context(|| format!("copy {} for midx", from.display()))?;
        }
        Ok(())
    };
    for (pack, idx) in packs {
        // Git names packs by the 20-byte trailer sha; match the client exactly.
        let mut file =
            std::fs::File::open(pack).with_context(|| format!("open {}", pack.display()))?;
        let mut trailer = [0u8; 20];
        file.seek(SeekFrom::End(-20))
            .and_then(|_| file.read_exact(&mut trailer))
            .context("pack too short to name by trailer")?;
        let name = hex::encode(trailer);
        link(pack, &pack_dir.join(format!("pack-{name}.pack")))?;
        link(idx, &pack_dir.join(format!("pack-{name}.idx")))?;
    }
    run_quiet(
        command()
            .args(NO_FSYNC)
            .arg("-C")
            .arg(dir.as_os_str())
            .args(["multi-pack-index", "write"]),
        "multi-pack-index write (server pregen)",
    )?;
    Ok(pack_dir.join("multi-pack-index"))
}

/// Return the raw (uncompressed) size of each object via
/// `git cat-file --batch-check`. Used to partition objects into evenly-sized
/// pack batches.
pub fn object_sizes<P: AsRef<Path>>(repo: P, oids: &[String]) -> Result<HashMap<String, u64>> {
    crate::gix_util::object_sizes(repo, oids)
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

/// Pack the given objects into `<prefix>-<packhash>.pack` and `.idx`, every
/// object stored whole — no new delta
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

/// Pack the objects reachable from `tip` but not from `exclude` (all of
/// `tip`'s history when `exclude` is `None`) into one deltified pack under
/// `prefix`, reusing the deltas git already has instead of recomputing them.
///
/// `--revs` reads the rev-list input from stdin (`tip`, then `^exclude`), so
/// git walks the range itself, the same walk as `rev-list --objects tip
/// ^exclude`. The pack is not thin: an object outside the range is never a
/// delta base.
///
/// No `--max-pack-size`: split packs cannot retain deltas whose bases land in
/// another output pack, turning this I/O-oriented reuse path back into
/// expensive re-deltification. Even a 4 GiB split made Linux re-deltify for
/// minutes; 64 MiB more than doubled Bun's cold-history bytes and made the
/// build 30x slower. Remote storage handles large files
/// with multipart upload instead of imposing its transport boundary on Git's
/// object representation.
///
/// An empty range writes a pack with no objects.
pub fn pack_objects_range_to_prefix<P: AsRef<Path>, Q: AsRef<Path>>(
    repo: P,
    tip: &str,
    exclude: Option<&str>,
    prefix: Q,
) -> Result<()> {
    // Each stdin line is a rev-list argument: a `-`-leading or range value
    // would change the packed set, so only plain revisions get through.
    crate::validation::validate_git_rev(tip).with_context(|| format!("invalid commit: {tip}"))?;
    let exclude = exclude
        .map(|rev| {
            crate::validation::validate_git_rev(rev)
                .with_context(|| format!("invalid commit: {rev}"))?;
            anyhow::Ok(format!("^{rev}"))
        })
        .transpose()?;

    if let Some(parent) = prefix.as_ref().parent() {
        std::fs::create_dir_all(parent).context("create pack prefix directory")?;
    }

    let mut command = command();
    command.args(NO_FSYNC).arg("-C").arg(repo.as_ref()).args([
        "pack-objects",
        "--delta-base-offset",
        "--revs",
    ]);
    command.arg(prefix.as_ref());
    run_pack_objects(
        command,
        std::iter::once(tip).chain(exclude.as_deref()),
        "pack-objects (range)",
    )
}

/// Run `pack-objects` with one line per `lines` item on stdin, as a tracked
/// child: a cancelled build kills it. Stderr goes to a file so a long
/// diagnostic cannot block the stdin writes.
fn run_pack_objects<'a>(
    mut command: Command,
    lines: impl IntoIterator<Item = &'a str>,
    what: &str,
) -> Result<()> {
    use crate::cancel::Ended;
    let mut stderr = tempfile::tempfile().context("create pack-objects stderr file")?;
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            stderr
                .try_clone()
                .context("clone pack-objects stderr file")?,
        ));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn git {what}"))?;
    let written = {
        let mut stdin = child
            .stdin
            .take()
            .with_context(|| format!("open git {what} stdin"))?;
        lines.into_iter().try_for_each(|line| {
            stdin.write_all(line.as_bytes())?;
            stdin.write_all(b"\n")
        })
    };
    let ended = crate::cancel::wait_child(&mut child, None)
        .with_context(|| format!("wait for git {what}"))?;
    match ended {
        Ended::Exited(status) if status.success() => {}
        Ended::Cancelled => return Err(anyhow::Error::new(crate::cancel::Cancelled)),
        Ended::Exited(_) | Ended::TimedOut => {
            let mut text = String::new();
            stderr.rewind().context("rewind pack-objects stderr")?;
            stderr
                .read_to_string(&mut text)
                .context("read pack-objects stderr")?;
            bail!("{what} failed: {}", text.trim());
        }
    }
    written.with_context(|| format!("write to git {what} stdin"))
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

    if let Some(parent) = prefix.as_ref().parent() {
        std::fs::create_dir_all(parent).context("create pack prefix directory")?;
    }

    // Pack-objects prints each pack's hash on stdout, which we do not need
    // (the packs are written by base name).
    let mut command = command();
    command
        .args(NO_FSYNC)
        .arg("-C")
        .arg(repo.as_ref())
        .args(["pack-objects", "--delta-base-offset"])
        .args(extra_args)
        .arg(prefix.as_ref());
    run_pack_objects(
        command,
        object_shas.iter().map(String::as_str),
        "pack-objects",
    )
}

/// The empty tree's object id, which git resolves without storing it.
const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

/// Configure a bare mirror for the server, persisted into the mirror's own
/// config so it covers every later command, not just one fetch.
///
/// Builds read the mirror's packs while a fetch appends to it. Auto-gc, which a
/// fetch can trigger, repacks or prunes those packs out from under a live reader
/// and corrupts the read. The server compacts the mirror itself, with no reader
/// ([`compact_mirror`]). `fetch.unpackLimit=1` keeps every fetch a pack, so
/// small pushes do not pile up loose objects that nothing would pack.
///
/// `attr.tree` names the empty tree. Git 2.43 and later read attributes from
/// `HEAD` in a bare repository, and `pack-objects --revs` asks for each named
/// object's `delta` attribute. A mirror's `HEAD` names a branch the mirror
/// never fetches, and git resolves that missing `HEAD` again for every
/// object: on git/git this tripled a cold history pack (5 s to 15 s, mostly
/// system time). No attributes is what git used before. Older git ignores the
/// key. Idempotent and cheap.
pub fn configure_mirror(mirror_dir: &Path) -> Result<()> {
    for (key, value) in [
        ("gc.auto", "0"),
        ("gc.autoPackLimit", "0"),
        ("maintenance.auto", "false"),
        ("fetch.unpackLimit", "1"),
        ("attr.tree", EMPTY_TREE),
    ] {
        run_quiet(
            command()
                .arg("-C")
                .arg(mirror_dir.as_os_str())
                .args(["config", key, value]),
            &format!("config {key} (mirror)"),
        )?;
    }
    Ok(())
}

/// Build the upstream URL and the credential-injection git config args for a
/// repo. The secret rides in `http.extraHeader`, never in the URL, so it stays
/// out of argv, `.git/config`, and logs. `origin_base` (the
/// `RIPCLONE_ORIGIN_BASE` override, see [`origin_base`]) replaces the github
/// default instance's host so offline e2e tests can use local `file://` origins.
///
/// Whenever the credential header is attached, redirect following is turned off
/// (`http.followRedirects=false`). git's default follows the smart-HTTP
/// ref-advertisement redirect, and curl resends a custom-named credential header
/// to the redirect target host. A trusted provider with an open redirect on its
/// smart-HTTP path could 302 an authenticated fetch to an attacker host and
/// deliver the provider token (or the cloud install token). curl only strips the
/// `Authorization` header cross-origin, not a configured custom header name, so
/// refusing the redirect is the only reliable stop: a redirect fails loudly
/// instead of leaking the credential. Unauthenticated requests keep following
/// redirects normally.
fn upstream_url_and_auth(
    provider: &ProviderInstance,
    repo_id: &RepoId,
    credential: Option<&secrecy::SecretString>,
    origin_base: Option<&str>,
) -> (String, Vec<String>) {
    let url = upstream_url_with(provider, repo_id, origin_base);

    let mut git_args: Vec<String> = Vec::new();
    if let Some(token) = credential
        && let Some((name, value)) = provider.auth_header(token.expose_secret())
    {
        git_args.push("-c".to_string());
        git_args.push(format!("http.extraHeader={}: {}", name, value));
        git_args.push("-c".to_string());
        git_args.push("http.followRedirects=false".to_string());
    }
    (url, git_args)
}

/// The upstream URL of `repo_id`, without credentials. The server fetches
/// from it, and a client tops up from it using its own provider config.
pub fn upstream_url(provider: &ProviderInstance, repo_id: &RepoId) -> String {
    upstream_url_with(provider, repo_id, origin_base().as_deref())
}

fn upstream_url_with(
    provider: &ProviderInstance,
    repo_id: &RepoId,
    origin_base: Option<&str>,
) -> String {
    if repo_id.is_github_default()
        && let Some(base) = origin_base.filter(|b| !b.is_empty())
    {
        format!("{}/{}.git", base.trim_end_matches('/'), repo_id.path)
    } else {
        provider.clone_url(&repo_id.path)
    }
}

/// `RIPCLONE_ORIGIN_BASE`: when set and non-empty, github default-instance
/// repos are fetched from `<base>/<owner>/<repo>.git` instead of github.com.
pub fn origin_base() -> Option<String> {
    std::env::var("RIPCLONE_ORIGIN_BASE")
        .ok()
        .filter(|b| !b.is_empty())
}

/// `RIPCLONE_LS_REMOTE_TIMEOUT_SECS`, default 30 s.
pub fn ls_remote_timeout() -> Result<Duration> {
    crate::env::number("RIPCLONE_LS_REMOTE_TIMEOUT_SECS", 30).map(Duration::from_secs)
}

/// `RIPCLONE_FETCH_TIMEOUT_SECS`, default 3600 s. The wall-clock cap on one
/// mirror fetch. A stalled transfer fails sooner, through git's low-speed
/// limit.
pub fn fetch_timeout() -> Result<Duration> {
    crate::env::number("RIPCLONE_FETCH_TIMEOUT_SECS", 3600).map(Duration::from_secs)
}

fn upstream_auth_header(
    provider: &ProviderInstance,
    credential: Option<&secrecy::SecretString>,
) -> Option<(String, String)> {
    credential.and_then(|token| provider.auth_header(token.expose_secret()))
}

fn upstream_git_error(
    op: &'static str,
    _url: &str,
    auth_header: Option<(String, String)>,
    stderr: &[u8],
) -> anyhow::Error {
    let detail = sanitize_git_stderr(stderr, auth_header.as_ref());
    // Classify the result of this Git operation. Never issue a second provider
    // request to guess whether the first one was retryable: admission owns one
    // bounded ls-remote process and its result is the whole probe boundary.
    let retryable = upstream_failure_is_retryable(stderr);
    anyhow::Error::new(UpstreamGitError::with_detail(op, retryable, detail))
}

/// Distil git's stderr into a short, secret-free snippet for the surfaced error.
/// Redacts any credential value that might echo back, keeps only the last few
/// non-empty lines (git's real diagnostic is at the end), and caps the length so
/// a huge progress dump can't bloat the message.
fn sanitize_git_stderr(stderr: &[u8], auth_header: Option<&(String, String)>) -> Option<String> {
    let mut text = String::from_utf8_lossy(stderr).into_owned();
    if let Some((_, value)) = auth_header
        && !value.is_empty()
    {
        text = text.replace(value.as_str(), "***");
    }
    let tail: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    let joined = tail
        .iter()
        .rev()
        .take(3)
        .rev()
        .copied()
        .collect::<Vec<_>>()
        .join("; ");
    if joined.is_empty() {
        return None;
    }
    const MAX: usize = 400;
    if joined.len() > MAX {
        Some(joined.chars().take(MAX).collect::<String>() + "…")
    } else {
        Some(joined)
    }
}

fn upstream_failure_is_retryable(stderr: &[u8]) -> bool {
    let detail = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    let statuses: Vec<u16> = detail
        .split("returned error: ")
        .skip(1)
        .filter_map(|suffix| suffix.get(..3))
        .filter_map(|status| status.parse::<u16>().ok())
        .collect();
    if statuses
        .iter()
        .any(|status| *status == 408 || *status == 429 || (500..600).contains(status))
    {
        return true;
    }
    if statuses.iter().any(|status| (400..500).contains(status)) {
        return false;
    }

    // These are target/authentication failures for which replaying the same
    // immutable job cannot help. Keep this list deliberately narrower than the
    // transport list below: an unfamiliar curl/TLS failure should retain the
    // queue's bounded retry behavior rather than becoming terminal.
    if [
        "authentication failed",
        "invalid username or password",
        "could not read username",
        "repository not found",
        "does not appear to be a git repository",
        "not our ref",
        "unadvertised object",
        "couldn't find remote ref",
        "remote ref does not exist",
        "invalid refspec",
        "invalid ref name",
    ]
    .iter()
    .any(|needle| detail.contains(needle))
    {
        return false;
    }
    // Smart HTTP answers a missing repository with a bare 404, which git
    // reports as `repository '<url>' not found`.
    if detail
        .split("repository '")
        .skip(1)
        .any(|rest| rest.contains("' not found"))
    {
        return false;
    }

    if [
        "operation timed out",
        "connection timed out",
        "connection reset",
        "failed to connect",
        "could not resolve host",
        "couldn't connect to server",
        "remote end hung up unexpectedly",
        "http/2 stream",
        "internal_error",
        "tls connection was non-properly terminated",
        "tls connection terminated unexpectedly",
        "empty reply from server",
        "failure receiving data from the peer",
        "recv failure",
        "ssl_error_syscall",
    ]
    .iter()
    .any(|needle| detail.contains(needle))
    {
        return true;
    }

    // The removed provider re-probe treated any request-level failure without
    // an HTTP response as retryable. Preserve that conservative behavior from
    // the one Git result: bounded queue attempts still cap an unknown failure.
    true
}

/// The commit an upstream ref name points at, by the selector rule of
/// [`ls_remote_tip_async`].
pub async fn ls_remote_commit_async(
    provider: &ProviderInstance,
    repo_id: &RepoId,
    ref_name: &str,
    credential: Option<&secrecy::SecretString>,
) -> Result<Option<String>> {
    Ok(ls_remote_tip_async(provider, repo_id, ref_name, credential)
        .await?
        .map(|tip| tip.commit))
}

/// What one `ls-remote` advertisement says a ref name points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTip {
    pub commit: String,
    /// The full ref that matched: `HEAD`, `refs/heads/<n>` or `refs/tags/<n>`.
    pub ref_name: String,
    /// The branch symbolic `HEAD` points at. Only asked for when resolving
    /// `HEAD`.
    pub default_branch: Option<String>,
}

/// Resolve a ref name upstream with one bounded `ls-remote`. `HEAD` and full
/// `refs/...` names match exactly. A short name matches `refs/heads/<n>`, then
/// `refs/tags/<n>` (an annotated tag peeled to its commit). `Ok(None)` when
/// nothing matches.
///
/// The child owns a fresh process group. Timeout and arbitrary future
/// cancellation kill that whole group; a reaper always waits for the direct
/// child. Temporary files drain output without pipe backpressure or helper
/// descendants keeping a pipe open.
pub async fn ls_remote_tip_async(
    provider: &ProviderInstance,
    repo_id: &RepoId,
    ref_name: &str,
    credential: Option<&secrecy::SecretString>,
) -> Result<Option<RemoteTip>> {
    ls_remote_tip_async_with(
        provider,
        repo_id,
        ref_name,
        credential,
        origin_base().as_deref(),
        ls_remote_timeout()?,
    )
    .await
}

/// [`ls_remote_tip_async`] with the origin base and timeout given.
async fn ls_remote_tip_async_with(
    provider: &ProviderInstance,
    repo_id: &RepoId,
    ref_name: &str,
    credential: Option<&secrecy::SecretString>,
    origin_base: Option<&str>,
    timeout: Duration,
) -> Result<Option<RemoteTip>> {
    let (url, git_args, auth_header, candidates) =
        prepare_ls_remote(provider, repo_id, ref_name, credential, origin_base)?;
    if url.is_empty() {
        return Ok(None);
    }
    let mut stdout = tempfile::tempfile().context("create ls-remote stdout file")?;
    let child_stdout = stdout.try_clone().context("clone ls-remote stdout file")?;
    let mut stderr = tempfile::tempfile().context("create ls-remote stderr file")?;
    let child_stderr = stderr.try_clone().context("clone ls-remote stderr file")?;
    let mut command = tokio::process::Command::from(command());
    command
        .args(&git_args)
        .args(LOW_SPEED_ARGS)
        .args(["ls-remote", "--symref", "--", &url])
        .args(&candidates)
        .stdout(Stdio::from(child_stdout))
        .stderr(Stdio::from(child_stderr));
    // Its own process group, so a timeout can kill git's HTTP helper too.
    #[cfg(unix)]
    command.process_group(0);
    let child = command.spawn().context("spawn git ls-remote")?;
    let mut managed = ManagedLsRemoteChild::new(child);
    let status = match tokio::time::timeout(timeout, managed.child_mut().wait()).await {
        Ok(status) => status.context("wait for git ls-remote")?,
        Err(_) => {
            managed.terminate();
            let reap = managed.child_mut().wait().await;
            managed.child.take();
            reap.context("reap timed-out git ls-remote")?;
            anyhow::bail!("git ls-remote timed out after {timeout:?}");
        }
    };
    // The direct Git process can exit before an HTTP helper. Terminate any
    // residual group member before releasing ownership, then mark the already-
    // waited direct child as consumed so Drop does not schedule another wait.
    managed.terminate();
    managed.child.take();

    stdout.rewind().context("rewind ls-remote stdout")?;
    let mut stdout_bytes = Vec::new();
    stdout
        .read_to_end(&mut stdout_bytes)
        .context("read ls-remote stdout")?;
    stderr.rewind().context("rewind ls-remote stderr")?;
    let mut stderr_bytes = Vec::new();
    stderr
        .read_to_end(&mut stderr_bytes)
        .context("read ls-remote stderr")?;
    if !status.success() {
        return Err(upstream_git_error(
            "ls-remote",
            &url,
            auth_header,
            &stderr_bytes,
        ));
    }
    parse_ls_remote_output(&stdout_bytes, &candidates)
}

struct ManagedLsRemoteChild {
    child: Option<tokio::process::Child>,
    process_group: Option<i32>,
}

impl ManagedLsRemoteChild {
    fn new(child: tokio::process::Child) -> Self {
        Self {
            process_group: child.id().and_then(|pid| i32::try_from(pid).ok()),
            child: Some(child),
        }
    }

    fn child_mut(&mut self) -> &mut tokio::process::Child {
        self.child
            .as_mut()
            .expect("managed ls-remote child present")
    }

    fn terminate(&mut self) {
        #[cfg(unix)]
        if let Some(group) = self.process_group {
            // SAFETY: the child starts a new process group whose id is its pid.
            // ESRCH only means every group member already exited.
            unsafe {
                libc::kill(-group, libc::SIGKILL);
            }
        }
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

impl Drop for ManagedLsRemoteChild {
    fn drop(&mut self) {
        if self.child.is_none() {
            return;
        }
        self.terminate();
        let mut child = self.child.take().expect("managed child checked above");
        // A dropped request future cannot await cleanup itself. Keep ownership
        // in the runtime until wait(2) observes the direct child; the process
        // group was synchronously killed above, so descendants cannot outlive
        // this cancellation boundary.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.wait().await;
            });
        }
    }
}

/// The upstream URL, git args, auth header, and the refs `ref_name` can mean
/// in preference order. These refs are also the `ls-remote` patterns. An empty
/// URL means the name can match nothing.
fn prepare_ls_remote(
    provider: &ProviderInstance,
    repo_id: &RepoId,
    ref_name: &str,
    credential: Option<&secrecy::SecretString>,
    origin_base: Option<&str>,
) -> Result<(String, Vec<String>, Option<(String, String)>, Vec<String>)> {
    crate::validation::validate_repo_path(provider, repo_id)
        .with_context(|| format!("invalid repo path: {}", repo_id.storage_key()))?;
    if ref_name != "HEAD" {
        crate::validation::validate_git_rev(ref_name)
            .with_context(|| format!("invalid ref: {ref_name}"))?;
        if ref_name.contains(['*', '?', '[']) {
            return Ok((String::new(), Vec::new(), None, Vec::new()));
        }
    }
    let (url, git_args) = upstream_url_and_auth(provider, repo_id, credential, origin_base);
    let auth_header = upstream_auth_header(provider, credential);
    let candidates = if ref_name == "HEAD" || ref_name.starts_with("refs/") {
        vec![ref_name.to_string()]
    } else {
        // A tag's own line names the tag object when it is annotated; the
        // peeled `^{}` line names the commit.
        vec![
            format!("refs/heads/{ref_name}"),
            format!("refs/tags/{ref_name}^{{}}"),
            format!("refs/tags/{ref_name}"),
        ]
    };
    Ok((url, git_args, auth_header, candidates))
}

/// The first of `candidates` the advertisement lists, and symbolic `HEAD`'s
/// branch when listed.
fn parse_ls_remote_output(stdout: &[u8], candidates: &[String]) -> Result<Option<RemoteTip>> {
    let stdout = String::from_utf8_lossy(stdout);
    let mut default_branch = None;
    let mut advertised: Vec<(&str, &str)> = Vec::new();
    for line in stdout.lines() {
        if let Some(symbolic) = line.strip_prefix("ref: refs/heads/")
            && let Some(branch) = symbolic
                .strip_suffix("\tHEAD")
                .or_else(|| symbolic.strip_suffix(" HEAD"))
            && !branch.is_empty()
            && crate::validation::validate_git_rev(branch).is_ok()
        {
            default_branch = Some(branch.to_string());
            continue;
        }
        if line.starts_with("ref: ") {
            return Err(anyhow::anyhow!(
                "git ls-remote returned malformed symbolic ref"
            ));
        }
        let Some((sha, remote_ref)) = line.split_once('\t') else {
            return Err(anyhow::anyhow!(
                "git ls-remote returned malformed advertisement"
            ));
        };
        let sha = sha.trim();
        if crate::validation::validate_object_id(sha).is_err() {
            return Err(anyhow::anyhow!(
                "git ls-remote returned malformed object id"
            ));
        }
        advertised.push((remote_ref.trim(), sha));
    }
    Ok(candidates.iter().find_map(|candidate| {
        let (_, sha) = advertised.iter().find(|(name, _)| name == candidate)?;
        Some(RemoteTip {
            commit: sha.to_string(),
            ref_name: candidate.trim_end_matches("^{}").to_string(),
            default_branch: default_branch.clone(),
        })
    }))
}

/// Cross-process advisory lock guarding a repo's bare mirror directory.
///
/// The in-process per-repo lock only serializes syncs within one process. When
/// several processes share a repo root,
/// two of them can try to `git clone --mirror` / `git fetch` into the same bare
/// dir at once — git's own lock files don't cover the initial clone, so the
/// clones collide ("could not lock config file ...: File exists"). This flock
/// serializes that mutation across processes. It is released when dropped, and by
/// the OS if the holding process dies, so it can't wedge.
#[cfg(unix)]
struct MirrorLock {
    _file: std::fs::File,
}

#[cfg(unix)]
impl MirrorLock {
    /// Acquire the exclusive lock, blocking until any other process syncing the
    /// same mirror finishes (after which our fetch simply fast-forwards).
    fn acquire(mirror_dir: &Path) -> Result<Self> {
        use std::os::unix::io::AsRawFd;
        // The lock file sits next to the mirror dir so it survives a fresh clone
        // (which creates the mirror dir itself).
        let lock_path = {
            let mut p = mirror_dir.as_os_str().to_os_string();
            p.push(".lock");
            std::path::PathBuf::from(p)
        };
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create mirror lock dir {}", parent.display()))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("open mirror lock {}", lock_path.display()))?;
        // SAFETY: `file` owns a live descriptor for the duration of the call;
        // `flock` neither retains the descriptor nor dereferences Rust memory.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            return Err(anyhow::Error::new(std::io::Error::last_os_error()))
                .with_context(|| format!("flock mirror lock {}", lock_path.display()));
        }
        Ok(Self { _file: file })
    }
}

#[cfg(not(unix))]
struct MirrorLock;

#[cfg(not(unix))]
impl MirrorLock {
    fn acquire(_mirror_dir: &Path) -> Result<Self> {
        Ok(Self)
    }
}

/// Fetch one admitted immutable commit without consulting or transferring the
/// moving branch. The object is retained under an internal ref so a subsequent
/// branch advance or deletion cannot change this job's source acquisition.
pub fn sync_bare_mirror_admitted<P: AsRef<Path>>(
    mirror_dir: P,
    provider: &ProviderInstance,
    repo_id: &RepoId,
    commit: &str,
    credential: Option<&secrecy::SecretString>,
) -> Result<()> {
    sync_bare_mirror_admitted_from(
        mirror_dir.as_ref(),
        provider,
        repo_id,
        commit,
        credential,
        origin_base().as_deref(),
        fetch_timeout()?,
    )
}

/// [`sync_bare_mirror_admitted`] with the origin base and timeout given.
fn sync_bare_mirror_admitted_from(
    mirror_dir: &Path,
    provider: &ProviderInstance,
    repo_id: &RepoId,
    commit: &str,
    credential: Option<&secrecy::SecretString>,
    origin_base: Option<&str>,
    timeout: Duration,
) -> Result<()> {
    crate::validation::validate_repo_path(provider, repo_id)
        .with_context(|| format!("invalid repo path: {}", repo_id.storage_key()))?;
    crate::validation::validate_object_id(commit)
        .with_context(|| format!("invalid admitted commit: {commit}"))?;
    if !is_full_hex_object_id(commit) {
        bail!("admitted commit must be a full object id: {commit}");
    }

    let _mirror_lock = MirrorLock::acquire(mirror_dir)?;
    // A commit already fetched (an earlier attempt, or the Files and Full
    // phases after Head) needs no upstream round trip. Auto-gc is off, so it
    // stays.
    if mirror_fetched_commit(mirror_dir, commit) {
        return Ok(());
    }
    let (url, git_args) = upstream_url_and_auth(provider, repo_id, credential, origin_base);
    let auth_header = upstream_auth_header(provider, credential);
    sync_bare_mirror_rev(mirror_dir, &url, &git_args, auth_header, commit, timeout)
}

/// True when a fetch of `commit` into the mirror completed. git writes the
/// fetch's ref only after the pack is in place and the commit's history
/// passed its connectivity check, so the ref proves the mirror holds all of
/// it. The commit object alone does not: a fetch still running, or one that
/// failed after writing its pack, can leave the commit without objects it
/// reaches.
pub fn mirror_fetched_commit(mirror_dir: &Path, commit: &str) -> bool {
    let fetched = format!(
        "refs/ripclone/revs/{}^{{commit}}",
        rev_internal_ref_name(commit)
    );
    mirror_dir.exists()
        && command()
            .arg("-C")
            .arg(mirror_dir.as_os_str())
            .args(["rev-parse", "--verify", "-q", &fetched])
            .stderr(Stdio::null())
            .output()
            .is_ok_and(|out| {
                out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == commit
            })
}

/// The file naming the pack that the first fetch into an empty mirror wrote,
/// and the commit it fetched: `<commit> <pack name>`. A fetch of one commit
/// with nothing to negotiate gets exactly that commit's history, so the pack
/// is the commit's whole history, ready to store as a cold Full.
const FETCHED_PACK_MARKER: &str = "ripclone-fetched-pack";

/// True when the mirror holds no objects: no packs and no loose objects.
fn mirror_is_empty(mirror_dir: &Path) -> bool {
    let objects = mirror_dir.join("objects");
    count_entries(&objects.join("pack"), |name| name.ends_with(".pack")) == 0
        && count_entries(&objects, |name| {
            name.len() == 2 && name.bytes().all(|b| b.is_ascii_hexdigit())
        }) == 0
}

/// After the first fetch into an empty mirror, record its pack. The mirror
/// fetches with `fetch.unpackLimit=1`, so the objects land in one pack; any
/// other shape is left unrecorded.
fn mark_fetched_pack(mirror_dir: &Path, commit: &str) -> Result<()> {
    let pack_dir = mirror_dir.join("objects").join("pack");
    let mut packs = Vec::new();
    for entry in std::fs::read_dir(&pack_dir).context("list mirror packs")? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if let Some(stem) = name.strip_suffix(".pack") {
            packs.push(stem.to_string());
        }
    }
    let [pack] = packs.as_slice() else {
        return Ok(());
    };
    let tmp = mirror_dir.join(format!("{FETCHED_PACK_MARKER}.tmp"));
    std::fs::write(&tmp, format!("{commit} {pack}\n")).context("write fetched-pack marker")?;
    std::fs::rename(&tmp, mirror_dir.join(FETCHED_PACK_MARKER))
        .context("install fetched-pack marker")
}

/// The `.pack` and `.idx` paths of the pack holding exactly `commit`'s
/// history, when the mirror's first fetch was of `commit` and that pack is
/// still there (a repack may have merged it away).
pub fn fetched_history_pack(mirror_dir: &Path, commit: &str) -> Option<(PathBuf, PathBuf)> {
    let marker = std::fs::read_to_string(mirror_dir.join(FETCHED_PACK_MARKER)).ok()?;
    let (marked, pack) = marker.trim_end().split_once(' ')?;
    if marked != commit || pack.contains(['/', '\\']) {
        return None;
    }
    let dir = mirror_dir.join("objects").join("pack");
    let (pack, idx) = (
        dir.join(format!("{pack}.pack")),
        dir.join(format!("{pack}.idx")),
    );
    (pack.is_file() && idx.is_file()).then_some((pack, idx))
}

fn ensure_bare_origin(mirror_dir: &Path, url: &str) -> Result<()> {
    if !mirror_dir.exists() {
        std::fs::create_dir_all(mirror_dir.parent().unwrap_or(Path::new("")))?;
        // `-q` and captured output: `init` prints its default-branch hint and
        // an "Initialized" line, which would land in the server log.
        run_quiet(
            command()
                .args(["init", "--bare", "-q"])
                .arg(mirror_dir.as_os_str()),
            "init --bare (mirror)",
        )?;
        configure_mirror(mirror_dir)?;
        run_quiet(
            command()
                .arg("-C")
                .arg(mirror_dir.as_os_str())
                .args(["remote", "add", "origin", url]),
            "remote add origin",
        )?;
        run_quiet(
            command().arg("-C").arg(mirror_dir.as_os_str()).args([
                "config",
                "remote.origin.fetch",
                "+refs/heads/*:refs/heads/*",
            ]),
            "config remote.origin.fetch",
        )?;
    } else {
        configure_mirror(mirror_dir)?;
    }
    Ok(())
}

/// Fetch `rev` from upstream, bounded twice: git's low-speed limit fails a
/// stalled transfer, and `timeout` caps the whole fetch. Either way the
/// fetch's process group (with its HTTP helper) is killed and the error is
/// retryable. A cancelled build kills it too.
fn sync_bare_mirror_rev(
    mirror_dir: &Path,
    url: &str,
    git_args: &[String],
    auth_header: Option<(String, String)>,
    rev: &str,
    timeout: Duration,
) -> Result<()> {
    use crate::cancel::Ended;
    ensure_bare_origin(mirror_dir, url)?;
    let empty = mirror_is_empty(mirror_dir);

    let refspec = format!("{rev}:refs/ripclone/revs/{}", rev_internal_ref_name(rev));

    let run = run_tracked(
        command()
            .args(git_args)
            .args(LOW_SPEED_ARGS)
            .arg("-C")
            .arg(mirror_dir.as_os_str())
            .args(["fetch", "--no-tags", "origin", &refspec]),
        Some(timeout),
    )
    .context("git fetch rev")?;
    match run.ended {
        Ended::Exited(status) if status.success() => {
            if empty {
                mark_fetched_pack(mirror_dir, rev)?;
            }
            Ok(())
        }
        Ended::Exited(_) => Err(upstream_git_error(
            "fetch rev",
            url,
            auth_header,
            &run.stderr,
        )),
        Ended::TimedOut => Err(anyhow::Error::new(UpstreamGitError::with_detail(
            "fetch rev",
            true,
            Some(format!("timed out after {timeout:?}")),
        ))),
        Ended::Cancelled => Err(anyhow::Error::new(crate::cancel::Cancelled)),
    }
}

fn rev_internal_ref_name(rev: &str) -> String {
    rev.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn is_full_hex_object_id(rev: &str) -> bool {
    (rev.len() == 40 || rev.len() == 64) && rev.chars().all(|c| c.is_ascii_hexdigit())
}

pub fn parent_commit<P: AsRef<Path>>(repo: P, commit: &str) -> Result<Option<String>> {
    crate::validation::validate_git_rev(commit)
        .with_context(|| format!("invalid commit: {}", commit))?;
    crate::gix_util::parent_commit(repo, commit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::scope;
    use std::collections::HashSet;
    use std::io::Read;

    /// Git's output is captured, not printed, and a failure still carries
    /// git's own error text.
    #[test]
    fn captured_git_failures_carry_git_stderr() {
        let dir = tempfile::tempdir().unwrap();
        let err = format!(
            "{:#}",
            write_commit_graph(dir.path().join("missing"), Duration::from_secs(30)).unwrap_err()
        );
        assert!(
            err.starts_with("git commit-graph write failed: fatal: cannot change to"),
            "{err}"
        );

        let repo = crate::test_fixture::init_bare(dir.path());
        drop(repo);
        let missing = "1".repeat(40);
        let err = format!(
            "{:#}",
            pack_objects_undeltified_to_prefix(
                dir.path(),
                std::slice::from_ref(&missing),
                dir.path().join("p/pack")
            )
            .unwrap_err()
        );
        assert!(err.starts_with("pack-objects failed: fatal:"), "{err}");
        assert!(err.contains(&missing), "{err}");
    }

    #[test]
    fn upstream_failure_classification_uses_the_git_result() {
        for detail in [
            "fatal: unable to access: The requested URL returned error: 408",
            "fatal: unable to access: The requested URL returned error: 429",
            "fatal: unable to access: The requested URL returned error: 501",
            "fatal: unable to access: The requested URL returned error: 503",
            "fatal: unable to access: Failed to connect to host",
            "fatal: unable to access: Could not resolve host",
            "RPC failed; curl 92 HTTP/2 stream 5 was not closed cleanly: INTERNAL_ERROR",
            "gnutls_handshake() failed: The TLS connection was non-properly terminated",
            "fatal: unable to access: Empty reply from server",
            "RPC failed; curl 56 Failure receiving data from the peer",
            "OpenSSL SSL_read: SSL_ERROR_SYSCALL, errno 54",
            "an unfamiliar transport failure without an HTTP response",
        ] {
            assert!(
                upstream_failure_is_retryable(detail.as_bytes()),
                "{detail} should be retryable"
            );
        }

        for detail in [
            "fatal: unable to access: The requested URL returned error: 400",
            "fatal: unable to access: The requested URL returned error: 401",
            "fatal: unable to access: The requested URL returned error: 403",
            "fatal: unable to access: The requested URL returned error: 404",
            "fatal: Authentication failed for repository",
            "fatal: couldn't find remote ref refs/heads/missing",
            "fatal: invalid refspec 'refs/heads/bad..ref'",
            "fatal: remote error: upload-pack: not our ref deadbeef",
            "Server does not allow request for unadvertised object deadbeef",
            "fatal: repository 'http://127.0.0.1:1/git/acme/gone.git/' not found",
        ] {
            assert!(
                !upstream_failure_is_retryable(detail.as_bytes()),
                "{detail} should be terminal"
            );
        }
    }

    /// A provider instance that attaches a custom-named credential header. curl
    /// strips a cross-origin `Authorization` header on its own, but not a custom
    /// name like `PRIVATE-TOKEN`, so this is the shape that can leak on redirect.
    fn leaky_provider(host: &str) -> ProviderInstance {
        ProviderInstance {
            id: crate::provider::ProviderInstanceId::new("selfhost"),
            kind: crate::provider::ProviderKind::Generic,
            host: host.to_string(),
            auth_template: Some("{token}".to_string()),
            auth_header_name: Some("PRIVATE-TOKEN".to_string()),
        }
    }

    /// The credential-injection args must also refuse redirects. Without this,
    /// git follows the smart-HTTP ref-advertisement redirect and curl resends the
    /// custom credential header to the redirect target host.
    #[test]
    fn authenticated_git_args_refuse_redirects() {
        let provider = leaky_provider("http://127.0.0.1:1/");
        let repo_id = RepoId {
            provider: crate::provider::ProviderInstanceId::new("selfhost"),
            path: "repo".to_string(),
        };
        let token = secrecy::SecretString::new("SECRETTOKEN".to_string().into());

        let (_url, git_args) = upstream_url_and_auth(&provider, &repo_id, Some(&token), None);
        assert!(
            git_args.iter().any(|a| a == "http.followRedirects=false"),
            "authenticated git args must disable redirect following, got {git_args:?}"
        );
        assert!(
            git_args
                .iter()
                .any(|a| a.starts_with("http.extraHeader=PRIVATE-TOKEN: ")),
            "credential still rides in extraHeader, got {git_args:?}"
        );

        // No credential -> no header and no redirect override; public clones may
        // still legitimately follow redirects.
        let (_url, no_auth) = upstream_url_and_auth(&provider, &repo_id, None, None);
        assert!(
            no_auth.is_empty(),
            "unauthenticated args stay empty, got {no_auth:?}"
        );
    }

    /// End-to-end guard: an authenticated ls-remote whose upstream 302-redirects
    /// to a different host must not deliver the credential header to that host.
    /// The redirect must fail loudly instead.
    #[test]
    fn authenticated_ls_remote_does_not_leak_credential_on_redirect() {
        use std::sync::mpsc;

        // Attacker: captures the raw request line + headers of anything it gets.
        let attacker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let attacker_addr = attacker.local_addr().unwrap();
        let (tx, rx) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = attacker.accept() {
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        });

        // Provider: 302-redirects the info/refs request to the attacker host.
        let provider_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let provider_addr = provider_listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = provider_listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let _ = write!(
                    stream,
                    "HTTP/1.1 302 Found\r\nLocation: http://{attacker_addr}/repo.git/info/refs?service=git-upload-pack\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        });

        let provider = leaky_provider(&format!("http://{provider_addr}"));
        let repo_id = RepoId {
            provider: crate::provider::ProviderInstanceId::new("selfhost"),
            path: "repo".to_string(),
        };
        let token = secrecy::SecretString::new("SECRETTOKEN".to_string().into());
        let (url, git_args) = upstream_url_and_auth(&provider, &repo_id, Some(&token), None);

        // Mirror the real ls-remote invocation shape.
        let status = Command::new("git")
            .args(&git_args)
            .args(["ls-remote", &url])
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(
            !status.success(),
            "a cross-host redirect on an authenticated fetch must fail, not succeed"
        );

        // The attacker must never have received the credential. It either got no
        // request at all (redirect refused) or, defensively, a request without
        // the credential header.
        match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(request) => {
                let lower = request.to_ascii_lowercase();
                assert!(
                    !lower.contains("private-token") && !request.contains("SECRETTOKEN"),
                    "credential leaked to redirect target:\n{request}"
                );
            }
            Err(_) => { /* redirect refused; attacker never reached */ }
        }
    }

    /// Auto-gc must be persisted off on the mirror, or a fetch could repack
    /// packs out from under a concurrent read. Attributes come from the empty
    /// tree, not the mirror's unborn `HEAD`.
    #[test]
    fn configure_mirror_persists_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        assert!(
            Command::new("git")
                .args(["init", "-q", "--bare"])
                .arg(repo)
                .status()
                .unwrap()
                .success()
        );

        configure_mirror(repo).unwrap();

        let read = |key: &str| -> String {
            let out = Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(["config", "--get", key])
                .output()
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        assert_eq!(read("gc.auto"), "0");
        assert_eq!(read("gc.autoPackLimit"), "0");
        assert_eq!(read("maintenance.auto"), "false");
        assert_eq!(read("fetch.unpackLimit"), "1");
        assert_eq!(read("attr.tree"), EMPTY_TREE);

        // Idempotent: a second call on an already-configured mirror is fine.
        configure_mirror(repo).unwrap();
        assert_eq!(read("gc.auto"), "0");
    }

    thread_local! {
        static FORKS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }

    extern "C" fn count_fork() {
        FORKS.with(|forks| forks.set(forks.get() + 1));
    }

    /// `fork()` calls made by this thread. `fork()` runs `pthread_atfork`
    /// handlers on glibc, musl and macOS; `posix_spawn` does not.
    fn forks_on_this_thread() -> u32 {
        static REGISTER: std::sync::Once = std::sync::Once::new();
        REGISTER.call_once(|| {
            // SAFETY: the handler only bumps a const-initialized thread local
            // in the forking parent.
            assert_eq!(
                unsafe { libc::pthread_atfork(Some(count_fork), None, None) },
                0
            );
        });
        FORKS.with(std::cell::Cell::get)
    }

    /// Server git spawns use `posix_spawn`, never `fork()`. A `fork()` copies
    /// the server's page tables: at 1 GB of RSS a spawn cost 23 ms instead
    /// of 0.5 ms, and a Linux Head build spawns hundreds of `pack-objects`.
    /// This counts forks, so it holds at any RSS and on any machine.
    #[tokio::test(flavor = "current_thread")]
    async fn server_git_spawns_do_not_fork() {
        use std::os::unix::process::CommandExt;
        let base = tempfile::tempdir().unwrap();
        let forks = forks_on_this_thread();

        // The control: a `pre_exec` hook does fork, and the counter sees it.
        let mut forked = Command::new("git");
        // SAFETY: the hook does nothing.
        unsafe { forked.pre_exec(|| Ok(())) };
        assert!(forked.arg("--version").output().unwrap().status.success());
        assert_eq!(
            forks_on_this_thread(),
            forks + 1,
            "the fork counter is broken"
        );

        // `git::command()` with the options server callers add.
        let output = command()
            .arg("--version")
            .current_dir(base.path())
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(output.status.success());
        let repo_dir = base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(repo_dir.parent().unwrap()).unwrap();
        let repo = crate::test_fixture::init_bare(&repo_dir);
        let tip = crate::test_fixture::commit(&repo, &[("README.md", b"one")]);
        let packed = pack_objects_undeltified_to_prefix(
            &repo_dir,
            std::slice::from_ref(&tip),
            base.path().join("p/pack"),
        );
        assert!(packed.is_ok(), "{packed:?}");

        // The API path's ls-remote, in its own process group.
        let registry = crate::provider::ProviderRegistry::new();
        let found = ls_remote_tip_async_with(
            registry.get("github").unwrap(),
            &crate::provider::RepoId::github("acme/widget"),
            "HEAD",
            None,
            Some(base.path().to_str().unwrap()),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
        assert_eq!(found.map(|t| t.commit), Some(tip));

        assert_eq!(
            forks_on_this_thread(),
            forks + 1,
            "a server git spawn used fork(); see git::command()"
        );
    }

    /// ls-remote reads the upstream tip with no object transfer. A short name
    /// is a branch, then a tag peeled to its commit; an absent ref is `None`.
    #[tokio::test]
    async fn ls_remote_resolves_head_branches_and_tags_without_objects() {
        use crate::provider::{ProviderRegistry, RepoId};
        let base = tempfile::tempdir().unwrap();
        let repo_dir = base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(repo_dir.parent().unwrap()).unwrap();
        let repo = crate::test_fixture::init_bare(&repo_dir);
        let first = crate::test_fixture::commit(&repo, &[("README.md", b"one")]);
        let second = crate::test_fixture::commit(&repo, &[("README.md", b"two")]);
        let git = |args: &[&str]| {
            let status = Command::new("git")
                .arg("-C")
                .arg(&repo_dir)
                .args(["-c", "user.name=t", "-c", "user.email=t@t"])
                .args(args)
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        };
        git(&["tag", "-a", "-m", "v1", "v1", &first]);
        git(&["tag", "light", &first]);
        // A tag named like a branch loses to the branch.
        git(&["tag", "main", &first]);

        let registry = ProviderRegistry::new();
        let provider = registry.get("github").unwrap();
        let repo_id = RepoId::github("acme/widget");
        let tip = |ref_name: &'static str| {
            let repo_id = repo_id.clone();
            let base = base.path().to_str().unwrap().to_string();
            async move {
                ls_remote_tip_async_with(
                    provider,
                    &repo_id,
                    ref_name,
                    None,
                    Some(&base),
                    Duration::from_secs(30),
                )
                .await
                .unwrap()
                .map(|t| (t.commit, t.ref_name, t.default_branch))
            }
        };

        assert_eq!(
            tip("HEAD").await,
            Some((second.clone(), "HEAD".into(), Some("main".into())))
        );
        assert_eq!(
            tip("main").await,
            Some((second.clone(), "refs/heads/main".into(), None))
        );
        assert_eq!(
            tip("v1").await,
            Some((first.clone(), "refs/tags/v1".into(), None)),
            "an annotated tag resolves to its commit"
        );
        assert_eq!(
            tip("light").await,
            Some((first.clone(), "refs/tags/light".into(), None))
        );
        assert_eq!(tip("no-such-branch").await, None);
        // A glob in the ref name must not turn the query into a pattern match.
        assert_eq!(tip("wid*").await, None);
    }

    #[tokio::test]
    async fn ls_remote_timeout_kills_reaps_and_closes_the_fixture_connection() {
        use std::sync::mpsc;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("ls-remote connected");
            accepted_tx.send(()).unwrap();
            // Do not send an HTTP response. The process-group kill must close the
            // client socket, which lets this bounded fixture thread return.
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let _ = stream.read(&mut buf);
        });

        let registry = crate::provider::ProviderRegistry::new();
        let provider = registry.get("github").unwrap();
        let result = ls_remote_tip_async_with(
            provider,
            &crate::provider::RepoId::github("acme/timeout"),
            "HEAD",
            None,
            Some(&format!("http://{addr}")),
            Duration::from_secs(1),
        )
        .await;

        accepted_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bounded ls-remote fixture accepted the request");
        assert!(
            result
                .expect_err("the hanging advertisement must time out")
                .to_string()
                .contains("timed out"),
            "timeout must be surfaced after the child is killed and reaped"
        );
        server
            .join()
            .expect("the fixture connection closed after process-group kill");
    }

    #[test]
    fn sanitize_git_stderr_redacts_credential_and_keeps_tail() {
        let auth = Some((
            "Authorization".to_string(),
            "Bearer ghp_supersecret".to_string(),
        ));
        let stderr = b"Cloning into bare repository...\nremote: token Bearer ghp_supersecret rejected\nfatal: could not read Username for 'https://github.com': terminal prompts disabled\n";
        let out = sanitize_git_stderr(stderr, auth.as_ref()).unwrap();
        assert!(
            !out.contains("ghp_supersecret"),
            "credential value must be redacted, got: {out}"
        );
        assert!(out.contains("***"), "redaction marker present");
        assert!(
            out.contains("could not read Username"),
            "the real diagnostic (tail line) is surfaced: {out}"
        );
        assert!(
            sanitize_git_stderr(b"   \n\n", None).is_none(),
            "blank stderr yields no detail"
        );
    }

    #[test]
    fn admitted_fetch_transfers_only_the_pinned_commit_not_the_moving_branch() {
        use crate::provider::{ProviderRegistry, RepoId};
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(origin.parent().unwrap()).unwrap();
        let src = crate::test_fixture::init_bare(&origin);
        let commit_b = crate::test_fixture::commit(&src, &[("README.md", b"B")]);
        let commit_c = crate::test_fixture::commit(&src, &[("README.md", b"C")]);
        let mirror = base.path().join("mirror-admitted.git");
        let registry = ProviderRegistry::new();
        let provider = registry.get("github").unwrap();
        let repo_id = RepoId::github("acme/widget");

        sync_bare_mirror_admitted_from(
            &mirror,
            provider,
            &repo_id,
            &commit_b,
            None,
            base.path().to_str(),
            Duration::from_secs(60),
        )
        .unwrap();

        assert_eq!(resolve_commit(&mirror, &commit_b).unwrap(), commit_b);
        assert!(
            resolve_commit(&mirror, &commit_c).is_err(),
            "the later branch tip must not be transferred by B's exact fetch"
        );
        assert!(
            resolve_commit(&mirror, "main").is_err(),
            "an immutable fetch must not update the moving branch ref"
        );
    }

    /// The first fetch into an empty mirror writes one pack holding exactly
    /// the fetched commit's history. Its cold Full stores that pack as it is;
    /// any other range, or a mirror whose pack was repacked away, is packed
    /// with `pack-objects`.
    #[test]
    fn cold_full_stores_the_first_fetched_pack() {
        use crate::provider::{ProviderRegistry, RepoId};
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("acme").join("cold.git");
        std::fs::create_dir_all(origin.parent().unwrap()).unwrap();
        let src = crate::test_fixture::init_bare(&origin);
        let a = crate::test_fixture::commit(&src, &[("a", b"a"), ("b", b"b")]);
        let b = crate::test_fixture::commit(&src, &[("a", b"a2"), ("b", b"b")]);
        let c = crate::test_fixture::commit(&src, &[("a", b"a3"), ("c", b"c")]);
        let mirror = base.path().join("mirror.git");
        let registry = ProviderRegistry::new();
        let provider = registry.get("github").unwrap();
        let repo_id = RepoId::github("acme/cold");
        let fetch = |commit: &str| {
            sync_bare_mirror_admitted_from(
                &mirror,
                provider,
                &repo_id,
                commit,
                None,
                base.path().to_str(),
                Duration::from_secs(60),
            )
            .unwrap()
        };
        let objects = |args: &[&str]| -> std::collections::BTreeSet<String> {
            let out = Command::new("git")
                .arg("-C")
                .arg(&mirror)
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?}");
            String::from_utf8(out.stdout)
                .unwrap()
                .lines()
                .map(|line| line.split_whitespace().next().unwrap().to_string())
                .collect()
        };

        fetch(&b);
        let (pack, idx) = fetched_history_pack(&mirror, &b).expect("b's fetched pack");
        assert!(fetched_history_pack(&mirror, &a).is_none());
        let idx_objects: std::collections::BTreeSet<String> = {
            let out = Command::new("git")
                .arg("show-index")
                .stdin(std::fs::File::open(&idx).unwrap())
                .output()
                .unwrap();
            String::from_utf8(out.stdout)
                .unwrap()
                .lines()
                .map(|line| line.split_whitespace().nth(1).unwrap().to_string())
                .collect()
        };
        assert_eq!(idx_objects, objects(&["rev-list", "--objects", &b]));

        let cas_dir = tempfile::tempdir().unwrap();
        let cas = crate::cas::Cas::new(cas_dir.path()).unwrap();
        let builder = crate::pack::PackBuilder::new(&mirror, &cas);
        let stored = builder.build_history_tail(&b, None).unwrap().packs;
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].0, crate::cas::hash_file(&pack).unwrap().0);
        assert!(pack.is_file(), "the mirror keeps its pack");
        // pack-objects can write the same bytes; a stored pack shares its inode.
        let inode =
            |path: &Path| std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(path).unwrap());
        assert_eq!(inode(&cas.path(&stored[0].0)), inode(&pack));

        // A later fetch is not into an empty mirror: b stays marked, and c's
        // history is packed by pack-objects.
        fetch(&c);
        assert_eq!(fetched_history_pack(&mirror, &b), Some((pack.clone(), idx)));
        assert!(fetched_history_pack(&mirror, &c).is_none());
        let c_history = builder.build_history_tail(&c, None).unwrap().packs;
        assert_ne!(c_history[0].0, stored[0].0);

        // After a repack the marked pack is gone, and b is packed again.
        let repack = Command::new("git")
            .arg("-C")
            .arg(&mirror)
            .args(["repack", "-a", "-d", "-q"])
            .status()
            .unwrap();
        assert!(repack.success());
        assert!(fetched_history_pack(&mirror, &b).is_none());
        let repacked = builder.build_history_tail(&b, None).unwrap().packs;
        assert_eq!(repacked.len(), 1);
        assert_eq!(repacked[0].0, stored[0].0, "the same history, packed again");
    }

    #[test]
    fn admitted_fetch_survives_branch_deletion_when_commit_remains_available() {
        use crate::provider::{ProviderRegistry, RepoId};
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("acme").join("deleted.git");
        std::fs::create_dir_all(origin.parent().unwrap()).unwrap();
        let src = crate::test_fixture::init_bare(&origin);
        let commit_b = crate::test_fixture::commit(&src, &[("README.md", b"B")]);
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&origin)
                .args(["update-ref", "refs/keep/admitted-b", &commit_b])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&origin)
                .args(["update-ref", "-d", "refs/heads/main"])
                .status()
                .unwrap()
                .success()
        );

        let mirror = base.path().join("mirror-deleted.git");
        let registry = ProviderRegistry::new();
        let provider = registry.get("github").unwrap();
        let repo_id = RepoId::github("acme/deleted");
        sync_bare_mirror_admitted_from(
            &mirror,
            provider,
            &repo_id,
            &commit_b,
            None,
            base.path().to_str(),
            Duration::from_secs(60),
        )
        .unwrap();

        assert_eq!(resolve_commit(&mirror, &commit_b).unwrap(), commit_b);
        assert!(resolve_commit(&mirror, "main").is_err());
    }

    /// Reads and fetches of one mirror stay correct while the commit-graph is
    /// rewritten beside them, with auto-gc off. One thread fetches and writes
    /// `commit-graph --split` for a fixed number of new commits. Readers walk
    /// every object. With gc off, writers only add packs and atomically
    /// replace the commit-graph, so no reader or fetch may fail: builds read
    /// while the mirror is written, and only `compact_mirror`, which deletes
    /// packs, needs the mirror to itself.
    #[test]
    fn concurrent_prep_and_reads_stay_safe() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        const ROUNDS: u64 = 8;

        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("origin.git");
        let src = crate::test_fixture::init_bare(&origin);
        crate::test_fixture::commit(&src, &[("f0", b"0")]);

        let git = |args: &[&str]| {
            let out = Command::new("git").args(args).output().unwrap();
            assert!(
                out.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
            out
        };

        // A non-bare clone used to advance the origin during the run.
        let pusher = base.path().join("pusher");
        git(&[
            "clone",
            "-q",
            origin.to_str().unwrap(),
            pusher.to_str().unwrap(),
        ]);
        let p = pusher.to_str().unwrap().to_string();
        git(&["-C", &p, "config", "user.email", "t@t"]);
        git(&["-C", &p, "config", "user.name", "t"]);

        // The mirror under test, cloned with gc off (the production path).
        let mirror = base.path().join("mirror.git");
        git(&[
            "clone",
            "--mirror",
            "--config",
            "gc.auto=0",
            origin.to_str().unwrap(),
            mirror.to_str().unwrap(),
        ]);
        let m = mirror.to_string_lossy().to_string();
        // Keep fetched objects as packfiles (production fetches from GitHub arrive
        // as packs); a local file:// fetch would otherwise unpack to loose
        // objects.
        git(&["-C", &m, "config", "transfer.unpackLimit", "1"]);
        git(&["-C", &m, "config", "fetch.unpackLimit", "1"]);
        // A local clone hardlinks loose objects; a production mirror starts
        // from a pack.
        git(&["-C", &m, "repack", "-a", "-d", "-q"]);

        /// Run git; true on success, else print why.
        fn run(label: &str, args: &[&str]) -> bool {
            let out = Command::new("git").args(args).output().unwrap();
            let text = String::from_utf8_lossy(&out.stdout);
            let ok = out.status.success() && !text.contains("missing");
            if !ok {
                eprintln!(
                    "{label} FAILED: git {args:?}: {:?} {}",
                    out.status,
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            ok
        }

        let prep_done = Arc::new(AtomicBool::new(false));
        let prep_err = Arc::new(AtomicU64::new(0));
        let read_ok = Arc::new(AtomicU64::new(0));
        let read_err = Arc::new(AtomicU64::new(0));

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (m, prep_done, read_ok, read_err) = (
                    m.clone(),
                    prep_done.clone(),
                    read_ok.clone(),
                    read_err.clone(),
                );
                std::thread::spawn(move || {
                    loop {
                        let done = prep_done.load(Ordering::Relaxed);
                        for args in [
                            &["-C", &m, "cat-file", "--batch-check", "--batch-all-objects"][..],
                            &["-C", &m, "rev-list", "--objects", "--all"],
                        ] {
                            let counter = if run("read", args) {
                                &read_ok
                            } else {
                                &read_err
                            };
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                        if done {
                            break;
                        }
                    }
                })
            })
            .collect();

        // The locked prep: advance the origin, fetch, rewrite the commit-graph.
        for i in 1..=ROUNDS {
            let file = pusher_path_write(&p, i);
            git(&["-C", &p, "add", &file]);
            git(&["-C", &p, "commit", "-q", "-m", &format!("c{i}")]);
            git(&["-C", &p, "push", "-q", "origin"]);
            for args in [
                &["-C", &m, "fetch", "-q", "origin"][..],
                &[
                    "-C",
                    &m,
                    "commit-graph",
                    "write",
                    "--reachable",
                    "--split",
                    "--no-progress",
                ],
            ] {
                if !run("prep", args) {
                    prep_err.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        prep_done.store(true, Ordering::Relaxed);
        for reader in readers {
            reader.join().unwrap();
        }

        let (prep_err, read_ok, read_err) = (
            prep_err.load(Ordering::Relaxed),
            read_ok.load(Ordering::Relaxed),
            read_err.load(Ordering::Relaxed),
        );
        eprintln!("prep_err={prep_err} read_ok={read_ok} read_err={read_err}");
        assert_eq!(
            prep_err, 0,
            "a fetch or commit-graph write failed beside reads"
        );
        assert_eq!(read_err, 0, "a read failed beside fetch and commit-graph");
        // Every object the readers saw is still there and connected at the end.
        git(&["-C", &m, "fsck", "--connectivity-only", "--no-progress"]);
    }

    /// Write a unique file into the pusher worktree (for the test above).
    fn pusher_path_write(pusher: &str, i: u64) -> String {
        let name = format!("f{i}");
        std::fs::write(std::path::Path::new(pusher).join(&name), format!("{i}")).unwrap();
        name
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

        // resolve / parent
        assert_eq!(
            resolve_commit(repo, "HEAD").unwrap(),
            git(repo, &["rev-parse", "HEAD"])
        );
        assert_eq!(
            parent_commit(repo, "HEAD").unwrap(),
            Some(git(repo, &["rev-parse", "HEAD^"]))
        );
        // Object sets must include commit + tree/blob closure.
        let expect_full: HashSet<String> = git(
            repo,
            &["rev-list", "--objects", "--no-object-names", "HEAD"],
        )
        .lines()
        .map(|s| s.to_string())
        .collect();
        let got_full: HashSet<String> = list_object_shas_with_depth(repo, "HEAD", None)
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

        // Sizes.
        let sizes = object_sizes(repo, &expect_full.iter().cloned().collect::<Vec<_>>()).unwrap();
        for sha in &expect_full {
            let want_size = git(repo, &["cat-file", "-s", sha])
                .trim()
                .parse::<u64>()
                .unwrap();
            assert_eq!(sizes.get(sha).copied().unwrap(), want_size);
        }
    }

    /// Submodule entries (mode 160000) must not appear in the worktree file list.
    #[test]
    fn gix_list_tree_entries_skips_submodules() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());

        let sub_tmp = tempfile::tempdir().unwrap();
        let sub_repo = crate::test_fixture::init_bare(sub_tmp.path());
        let sub_commit =
            crate::test_fixture::commit(&sub_repo, &[("README.md", b"submodule-readme")]);

        let parent_commit = crate::test_fixture::commit_with_modes(
            &repo,
            &[
                ("file.txt", 0o100644, b"file"),
                ("vendor/sub", 0o160000, sub_commit.as_bytes()),
            ],
        );

        let entries = list_tree_entries(tmp.path(), &parent_commit).unwrap();
        assert_eq!(entries.len(), 1, "submodule entry must be skipped");
        assert_eq!(entries[0].0, "file.txt");
        assert_eq!(entries[0].1, "100644");
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
        let _ = Command::new("git")
            .args([
                "-C",
                repo.to_str().unwrap(),
                "update-index",
                "--skip-worktree",
                "a.txt",
            ])
            .output()
            .unwrap();
        assert_skip_worktree_flag(repo, "a.txt", true);

        let stats = vec![materialized_path_stat_from_metadata(
            b"a.txt".to_vec(),
            &std::fs::symlink_metadata(repo.join("a.txt")).unwrap(),
        )];
        clear_skip_worktree_index_with_stats_byte_iter(repo, [b"a.txt".as_slice()], &stats)
            .unwrap();

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

        let commit = resolve_commit(repo, "HEAD").unwrap();
        assert_eq!(resolve_commit(repo, "main").unwrap(), commit);

        assert_eq!(parent_commit(repo, &commit).unwrap(), None);

        let oids = list_object_shas_with_depth(repo, &commit, None).unwrap();
        assert!(!oids.is_empty());
    }

    #[test]
    fn parent_commit_is_none_for_a_merge() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();
        let run = |args: &[&str]| {
            let output = Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        run(&["init", "-q", "-b", "main"]);
        run(&["config", "user.email", "t@t"]);
        run(&["config", "user.name", "t"]);
        std::fs::write(repo.join("base"), b"base\n").unwrap();
        run(&["add", "base"]);
        run(&["commit", "-q", "-m", "base"]);
        run(&["checkout", "-q", "-b", "side"]);
        std::fs::write(repo.join("side"), b"side\n").unwrap();
        run(&["add", "side"]);
        run(&["commit", "-q", "-m", "side"]);
        run(&["checkout", "-q", "main"]);
        std::fs::write(repo.join("main"), b"main\n").unwrap();
        run(&["add", "main"]);
        run(&["commit", "-q", "-m", "main"]);
        run(&["merge", "-q", "--no-ff", "side", "-m", "merge"]);
        let merge = resolve_commit(repo, "HEAD").unwrap();
        assert_eq!(parent_commit(repo, &merge).unwrap(), None);
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

        let not_repo = tempfile::tempdir().unwrap();
        assert!(crate::gix_util::open_repo(not_repo.path()).is_err());
    }

    /// A commit chain: parent_commit and range listing (Phase 1).
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
            if i == 0 {
                std::fs::write(repo.join("stable.txt"), b"unchanged\n").unwrap();
            }
            let _ = Command::new("git")
                .args(["-C", repo.to_str().unwrap(), "add", "-A"])
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
    }

    /// Regression: object lookups for ≥PARALLEL_LOOKUP_THRESHOLD objects must not
    /// deadlock when called from a rayon context (cold builds hit this path).
    #[test]
    fn gix_object_sizes_parallel_path_no_deadlock() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = crate::test_fixture::init_bare(tmp.path());
        let files: Vec<(String, Vec<u8>)> = (0..300)
            .map(|i| (format!("f{i:03}.txt"), vec![(i % 256) as u8; 100]))
            .collect();
        let refs: Vec<(&str, &[u8])> = files
            .iter()
            .map(|(p, b)| (p.as_str(), b.as_slice()))
            .collect();
        crate::test_fixture::commit(&repo, &refs);

        let objects =
            crate::gix_util::list_object_shas_with_depth(tmp.path(), "HEAD", None).unwrap();
        assert!(
            objects.len() > crate::gix_util::PARALLEL_LOOKUP_THRESHOLD,
            "test must exercise the parallel path ({} objects)",
            objects.len()
        );

        // Call from inside a rayon context, exactly like cold pack builds do.
        let sizes: HashMap<String, u64> = scope(|_| object_sizes(tmp.path(), &objects)).unwrap();
        assert_eq!(sizes.len(), objects.len());

        // Sanity: all blobs are 100 bytes, trees/commits are small.
        let total: u64 = sizes.values().sum();
        assert!(
            total >= 30_000,
            "expected at least 300 * 100 bytes of blob data"
        );
    }

    #[test]
    fn resolve_commit_peels_annotated_tag() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path();

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
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        git(repo, &["add", "a.txt"]);
        git(repo, &["commit", "-q", "-m", "c1"]);

        let head = git(repo, &["rev-parse", "HEAD"]);
        git(repo, &["tag", "-a", "v1.0", "-m", "release"]);

        assert_eq!(
            resolve_commit(repo, "v1.0").unwrap(),
            head,
            "annotated tag should peel to its target commit"
        );
        assert_eq!(
            resolve_commit(repo, "v1.0^{commit}").unwrap(),
            head,
            "explicit peel syntax should still work"
        );
    }
}
