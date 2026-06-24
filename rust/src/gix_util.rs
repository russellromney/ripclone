use anyhow::{Context, Result};
use gix::objs::tree::EntryMode;
use gix::traverse::tree::{Visit, visit::Action};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Global ceiling on the number of worker threads spawned by any gix parallel
/// helper. Individual operations can be tuned with their own env vars, but they
/// are always clamped to this value to avoid saturating the host.
pub const DEFAULT_THREAD_CAP: usize = 64;

/// Threading environment variables:
///
/// * `RIPCLONE_MAX_THREADS`        - global ceiling for all gix worker pools (default: host cores).
/// * `RIPCLONE_HASH_THREADS`       - blob hashing in archive files-table builds.
/// * `RIPCLONE_PACK_ENCODE_THREADS`- object read/encode in gix pack encoding.
/// * `RIPCLONE_GIX_INDEX_THREADS`  - gix pack index verification/writing.
/// * `RIPCLONE_LOOKUP_THREADS`     - bulk object size/type lookups.
/// * `RIPCLONE_WRITE_THREADS`      - worktree materialization writers (in extract.rs).
/// * `RIPCLONE_FETCH_THREADS`      - archive chunk fetchers (in extract.rs).
/// * `RAYON_NUM_THREADS`           - zstd frame compression pool (in archive.rs).
///
/// All values are clamped to `[1, DEFAULT_THREAD_CAP]`.
///
/// Read a parallelism setting from `env_var`. If missing or unparseable, use
/// `fallback`. The result is clamped to at least 1 and at most the global cap.
pub fn worker_threads(env_var: &str, fallback: usize) -> usize {
    let fallback = fallback.clamp(1, DEFAULT_THREAD_CAP);
    std::env::var(env_var)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.clamp(1, DEFAULT_THREAD_CAP))
        .unwrap_or(fallback)
}

/// Number of threads to use when no operation-specific override is set.
pub fn default_worker_threads() -> usize {
    worker_threads(
        "RIPCLONE_MAX_THREADS",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
    )
}

/// Open a gix repository for single-threaded use.
pub fn open_repo<P: AsRef<Path>>(path: P) -> Result<gix::Repository> {
    gix::open(path.as_ref())
        .with_context(|| format!("opening gix repo at {}", path.as_ref().display()))
}

/// Open a gix repository that can safely be shared across threads.
pub fn open_sync_repo<P: AsRef<Path>>(path: P) -> Result<gix::ThreadSafeRepository> {
    gix::ThreadSafeRepository::open(path.as_ref()).with_context(|| {
        format!(
            "opening thread-safe gix repo at {}",
            path.as_ref().display()
        )
    })
}

/// Resolve a revision expression to a full hex sha.
pub fn resolve_commit<P: AsRef<Path>>(repo_path: P, rev: &str) -> Result<String> {
    let repo = open_repo(repo_path)?;
    let id = repo
        .rev_parse_single(rev)
        .with_context(|| format!("resolving rev '{}'", rev))?;
    Ok(id.to_string())
}

/// Return the name of the current branch (e.g. `main`).
pub fn default_branch<P: AsRef<Path>>(repo_path: P) -> Result<String> {
    let repo = open_repo(repo_path)?;
    let head = repo.head().context("reading HEAD")?;
    let name = head
        .referent_name()
        .context("detached HEAD has no branch name")?
        .shorten()
        .to_string();
    Ok(name)
}

/// Return the `count` most recent commits on `branch`, first-parent only.
pub fn last_commits<P: AsRef<Path>>(
    repo_path: P,
    branch: &str,
    count: usize,
) -> Result<Vec<String>> {
    let repo = open_repo(repo_path)?;
    let id = repo
        .rev_parse_single(branch)
        .with_context(|| format!("resolving branch '{}'", branch))?;
    repo.rev_walk([id])
        .first_parent_only()
        .all()
        .context("rev-walk")?
        .take(count)
        .map(|info| info.map(|i| i.id.to_string()))
        .collect::<Result<Vec<_>, _>>()
        .context("iterate rev-walk")
}

/// Return the immediate parent of `commit`, if any.
pub fn parent_commit<P: AsRef<Path>>(repo_path: P, commit: &str) -> Result<Option<String>> {
    let repo = open_repo(repo_path)?;
    let id = repo
        .rev_parse_single(commit)
        .with_context(|| format!("resolving commit '{}'", commit))?;
    let commit_obj = repo.find_commit(id).context("find commit")?;
    Ok(commit_obj.parent_ids().next().map(|pid| pid.to_string()))
}

/// List every object reachable from `commit`, optionally limiting the number of
/// commits walked (`max_depth`). The returned list includes each walked commit
/// plus the full tree/blob closure reachable from those commits, deduplicated
/// and sorted. This matches `git rev-list --objects --no-object-names`.
pub fn list_object_shas_with_depth<P: AsRef<Path>>(
    repo_path: P,
    commit: &str,
    max_depth: Option<usize>,
) -> Result<Vec<String>> {
    let repo = open_repo(repo_path)?;
    let id = repo
        .rev_parse_single(commit)
        .with_context(|| format!("resolving commit '{}'", commit))?;
    let walk = repo.rev_walk([id]);
    let infos: Vec<gix::revision::walk::Info<'_>> = if let Some(d) = max_depth {
        walk.all()?.take(d).collect::<Result<Vec<_>, _>>()?
    } else {
        walk.all()?.collect::<Result<Vec<_>, _>>()?
    };

    let mut oids = HashSet::with_capacity(infos.len() * 4);
    for info in &infos {
        oids.insert(info.id);
    }
    for info in &infos {
        let commit_obj = repo
            .find_commit(info.id)
            .with_context(|| format!("find commit {}", info.id))?;
        collect_tree_objects(&repo, commit_obj.tree_id()?.detach(), &mut oids)
            .with_context(|| format!("collecting tree closure for {}", info.id))?;
    }

    let mut out: Vec<String> = oids.into_iter().map(|oid| oid.to_string()).collect();
    out.sort();
    Ok(out)
}

/// List objects reachable from `to` but not from `from`.
pub fn list_object_shas_in_range<P: AsRef<Path>>(
    repo_path: P,
    from: Option<&str>,
    to: &str,
) -> Result<Vec<String>> {
    let repo = open_repo(repo_path)?;
    let to_id = repo
        .rev_parse_single(to)
        .with_context(|| format!("resolving to '{}'", to))?;
    let mut walk = repo.rev_walk([to_id]);
    if let Some(f) = from {
        let from_id = repo
            .rev_parse_single(f)
            .with_context(|| format!("resolving from '{}'", f))?;
        walk = walk.with_hidden([from_id]);
    }
    let infos: Vec<_> = walk.all()?.collect::<Result<Vec<_>, _>>()?;

    let mut oids = HashSet::with_capacity(infos.len() * 4);
    for info in &infos {
        oids.insert(info.id);
    }
    for info in &infos {
        let commit_obj = repo
            .find_commit(info.id)
            .with_context(|| format!("find commit {}", info.id))?;
        collect_tree_objects(&repo, commit_obj.tree_id()?.detach(), &mut oids)
            .with_context(|| format!("collecting tree closure for {}", info.id))?;
    }

    let mut out: Vec<String> = oids.into_iter().map(|oid| oid.to_string()).collect();
    out.sort();
    Ok(out)
}

/// List every tree entry reachable from `commit`.
pub fn list_tree_entries<P: AsRef<Path>>(
    repo_path: P,
    commit: &str,
) -> Result<Vec<(String, String, String, String)>> {
    let repo = open_repo(repo_path)?;
    let id = repo
        .rev_parse_single(commit)
        .with_context(|| format!("resolving commit '{}'", commit))?;
    let tree_id = repo
        .find_commit(id)
        .context("find commit")?
        .tree_id()
        .context("read tree id")?
        .detach();

    let mut recorder = gix::traverse::tree::Recorder::default();
    gix::traverse::tree::depthfirst(
        tree_id,
        gix::traverse::tree::depthfirst::State::default(),
        &repo.objects,
        &mut recorder,
    )
    .context("tree traversal")?;

    let mut out = Vec::with_capacity(recorder.records.len());
    for entry in recorder.records {
        // Trees are recursed into; submodule commit entries are not worktree files.
        if entry.mode.is_tree() || entry.mode.is_commit() {
            continue;
        }
        let path = String::from_utf8_lossy(&entry.filepath).to_string();
        let mode = format!("{:o}", entry.mode);
        let kind = mode_to_object_type(&entry.mode);
        out.push((path, mode, entry.oid.to_string(), kind));
    }
    Ok(out)
}

/// Return the raw (uncompressed) size of each object.
const PARALLEL_LOOKUP_THRESHOLD: usize = 256;

pub fn object_sizes<P: AsRef<Path>>(repo_path: P, oids: &[String]) -> Result<HashMap<String, u64>> {
    if oids.len() < PARALLEL_LOOKUP_THRESHOLD {
        let repo = open_repo(repo_path)?;
        let mut map = HashMap::with_capacity(oids.len());
        for oid_str in oids {
            let id = parse_oid(oid_str)?;
            let header = repo
                .find_header(id)
                .with_context(|| format!("find header {}", oid_str))?;
            map.insert(oid_str.clone(), header.size());
        }
        return Ok(map);
    }

    let sync_repo = open_sync_repo(repo_path)?;
    let num_threads =
        worker_threads("RIPCLONE_LOOKUP_THREADS", default_worker_threads()).min(oids.len());
    let chunk_size = oids.len().div_ceil(num_threads);

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(num_threads);
        for chunk in oids.chunks(chunk_size.max(1)) {
            let sync = &sync_repo;
            handles.push(scope.spawn(move || {
                let repo = sync.to_thread_local();
                let mut local = HashMap::with_capacity(chunk.len());
                for oid_str in chunk {
                    let id = parse_oid(oid_str)?;
                    let header = repo
                        .find_header(id)
                        .with_context(|| format!("find header {}", oid_str))?;
                    local.insert(oid_str.clone(), header.size());
                }
                Ok::<_, anyhow::Error>(local)
            }));
        }

        let mut map = HashMap::with_capacity(oids.len());
        for handle in handles {
            map.extend(
                handle
                    .join()
                    .map_err(|e| anyhow::anyhow!("object_sizes worker panicked: {:?}", e))??,
            );
        }
        Ok(map)
    })
}

/// Classify many objects by type.
pub fn classify_objects<P: AsRef<Path>>(
    repo_path: P,
    shas: &HashSet<String>,
) -> Result<HashMap<String, String>> {
    let shas: Vec<String> = shas.iter().cloned().collect();
    if shas.len() < PARALLEL_LOOKUP_THRESHOLD {
        let repo = open_repo(repo_path)?;
        let mut map = HashMap::with_capacity(shas.len());
        for sha in &shas {
            let id = parse_oid(sha)?;
            let header = repo
                .find_header(id)
                .with_context(|| format!("find header {}", sha))?;
            map.insert(sha.clone(), kind_to_str(header.kind()).to_string());
        }
        return Ok(map);
    }

    let sync_repo = open_sync_repo(repo_path)?;
    let num_threads =
        worker_threads("RIPCLONE_LOOKUP_THREADS", default_worker_threads()).min(shas.len());
    let chunk_size = shas.len().div_ceil(num_threads);

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(num_threads);
        for chunk in shas.chunks(chunk_size.max(1)) {
            let sync = &sync_repo;
            handles.push(scope.spawn(move || {
                let repo = sync.to_thread_local();
                let mut local = HashMap::with_capacity(chunk.len());
                for sha in chunk {
                    let id = parse_oid(sha)?;
                    let header = repo
                        .find_header(id)
                        .with_context(|| format!("find header {}", sha))?;
                    local.insert(sha.clone(), kind_to_str(header.kind()).to_string());
                }
                Ok::<_, anyhow::Error>(local)
            }));
        }

        let mut map = HashMap::with_capacity(shas.len());
        for handle in handles {
            map.extend(
                handle
                    .join()
                    .map_err(|e| anyhow::anyhow!("classify_objects worker panicked: {:?}", e))??,
            );
        }
        Ok(map)
    })
}

/// Return the type of an object.
pub fn object_type<P: AsRef<Path>>(repo_path: P, sha: &str) -> Result<String> {
    let repo = open_repo(repo_path)?;
    let id = parse_oid(sha)?;
    let header = repo
        .find_header(id)
        .with_context(|| format!("find header {}", sha))?;
    Ok(kind_to_str(header.kind()).to_string())
}

/// Read the content bytes of an object (what `git cat-file -p` prints).
pub fn cat_file<P: AsRef<Path>>(repo_path: P, sha: &str) -> Result<Vec<u8>> {
    let repo = open_repo(repo_path)?;
    let id = parse_oid(sha)?;
    Ok(repo
        .find_object(id)
        .with_context(|| format!("read object {}", sha))?
        .data
        .clone())
}

/// Read the content bytes of many objects.
pub fn cat_file_batch<P: AsRef<Path>>(
    repo_path: P,
    shas: &[String],
) -> Result<HashMap<String, Vec<u8>>> {
    let repo = open_repo(repo_path)?;
    let mut map = HashMap::with_capacity(shas.len());
    for sha in shas {
        let id = parse_oid(sha)?;
        let data = repo
            .find_object(id)
            .with_context(|| format!("read object {}", sha))?
            .data
            .clone();
        map.insert(sha.clone(), data);
    }
    Ok(map)
}

/// Return a map from path to blob size for every blob in the commit tree.
pub fn ls_tree_sizes<P: AsRef<Path>>(repo_path: P, commit: &str) -> Result<HashMap<String, u64>> {
    let repo = open_repo(repo_path)?;
    let id = repo
        .rev_parse_single(commit)
        .with_context(|| format!("resolving commit '{}'", commit))?;
    let tree_id = repo
        .find_commit(id)
        .context("find commit")?
        .tree_id()
        .context("read tree id")?
        .detach();

    let mut recorder = gix::traverse::tree::Recorder::default();
    gix::traverse::tree::depthfirst(
        tree_id,
        gix::traverse::tree::depthfirst::State::default(),
        &repo.objects,
        &mut recorder,
    )
    .context("tree traversal")?;

    let mut map = HashMap::new();
    for entry in recorder.records {
        if !entry.mode.is_blob_or_symlink() {
            continue;
        }
        let size = repo
            .find_header(entry.oid)
            .with_context(|| format!("find header for {}", entry.oid))?
            .size();
        map.insert(String::from_utf8_lossy(&entry.filepath).to_string(), size);
    }
    Ok(map)
}

/// Look up a single tree entry by path.
pub fn ls_tree_entry<P: AsRef<Path>>(
    repo_path: P,
    commit: &str,
    path: &str,
) -> Result<Option<(String, String)>> {
    let repo = open_repo(repo_path)?;
    let id = repo
        .rev_parse_single(commit)
        .with_context(|| format!("resolving commit '{}'", commit))?;
    let tree_id = repo
        .find_commit(id)
        .context("find commit")?
        .tree_id()
        .context("read tree id")?;
    let tree = repo.find_tree(tree_id).context("find tree")?;
    let entry = tree.lookup_entry_by_path(path).context("lookup entry")?;
    Ok(entry.map(|e| {
        let mode = format!("{:o}", e.mode());
        (mode, e.object_id().to_string())
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_oid(s: &str) -> Result<gix::hash::ObjectId> {
    gix::hash::ObjectId::from_hex(s.as_bytes()).with_context(|| format!("invalid object id: {}", s))
}

fn kind_to_str(kind: gix::objs::Kind) -> &'static str {
    match kind {
        gix::objs::Kind::Commit => "commit",
        gix::objs::Kind::Tree => "tree",
        gix::objs::Kind::Blob => "blob",
        gix::objs::Kind::Tag => "tag",
    }
}

fn mode_to_object_type(mode: &EntryMode) -> String {
    use gix::objs::tree::EntryKind;
    match mode.kind() {
        EntryKind::Tree => "tree".to_string(),
        EntryKind::Blob | EntryKind::BlobExecutable | EntryKind::Link => "blob".to_string(),
        EntryKind::Commit => "commit".to_string(),
    }
}

struct OidCollector {
    oids: HashSet<gix::hash::ObjectId>,
}

impl OidCollector {
    fn new() -> Self {
        Self {
            oids: HashSet::new(),
        }
    }
}

impl Visit for OidCollector {
    fn pop_back_tracked_path_and_set_current(&mut self) {}
    fn pop_front_tracked_path_and_set_current(&mut self) {}
    fn push_back_tracked_path_component(&mut self, _component: &gix::bstr::BStr) {}
    fn push_path_component(&mut self, _component: &gix::bstr::BStr) {}
    fn pop_path_component(&mut self) {}

    fn visit_tree(&mut self, entry: &gix::objs::tree::EntryRef<'_>) -> Action {
        if self.oids.insert(entry.oid.to_owned()) {
            Action::Continue(true)
        } else {
            Action::Continue(false)
        }
    }

    fn visit_nontree(&mut self, entry: &gix::objs::tree::EntryRef<'_>) -> Action {
        self.oids.insert(entry.oid.to_owned());
        Action::Continue(true)
    }
}

fn collect_tree_objects(
    repo: &gix::Repository,
    root_tree_id: gix::hash::ObjectId,
    oids: &mut HashSet<gix::hash::ObjectId>,
) -> Result<()> {
    oids.insert(root_tree_id);
    let mut collector = OidCollector::new();
    gix::traverse::tree::depthfirst(
        root_tree_id,
        gix::traverse::tree::depthfirst::State::default(),
        &repo.objects,
        &mut collector,
    )
    .context("tree traversal")?;
    oids.extend(collector.oids);
    Ok(())
}
