//! The repository's published refs: the tags artifact, and the branches
//! artifact with its side pack, built from the mirror and pointed at by the
//! control store so every clone response carries them
//! (docs/internal/rebuild/TAGS.md).
//!
//! Head publishes the tags once `ensure_commit` returns. That proves the
//! fetch of its commit ended, and the fetch is atomic, so its tags are in
//! `refs/tags/` with it: a Head that is ready never points clones at older
//! tags.
//!
//! A refresh, after any push webhook or when the poller sees upstream's
//! refs change, fetches every upstream branch and tag into the mirror and
//! publishes all of it, without fetching a build's commit or admitting a
//! build.

use super::ServerState;
use super::worker::repo_lock;
use crate::branches::MirrorRefs;
use crate::cancel;
use crate::cas::Cas;
use crate::control::{PublishedTags, RepoBranches, SidePack};
use crate::git::{self, UpstreamRefs};
use crate::pins::PinSet;
use crate::provider::RepoId;
use anyhow::{Context, Result};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::Ordering;
use tracing::warn;

/// Whether the poller should refresh: upstream's refs differ from the ones
/// the last refresh published. A listing that names no default branch
/// leaves the mirror's `HEAD` where it was, so it compares with the last
/// refresh's default branch.
pub(super) async fn changed(
    state: &ServerState,
    repo_id: &RepoId,
    upstream: &UpstreamRefs,
) -> Result<bool> {
    let stored = state.control.get_refs(repo_id).await?;
    let Some(branches) = stored.and_then(|row| row.branches) else {
        return Ok(true);
    };
    let kept = (branches.default_branch.as_deref())
        .filter(|kept| upstream.branches.iter().any(|(name, _)| name == kept));
    let default_branch = upstream.default_branch.as_deref().or(kept);
    let hash = crate::branches::refs_hash(default_branch, &upstream.branches, &upstream.tags);
    Ok(branches.refs_hash != hash)
}

/// Refresh the repo's refs. Requests that arrive while one runs are folded
/// into one more pass of it, which runs after a failed pass too. The error
/// returned is the last pass's: a pass fetches every ref, so a later
/// success covers an earlier failure.
pub(super) async fn refresh(state: &ServerState, repo_id: &RepoId) -> Result<()> {
    let lock = repo_lock(state, repo_id).await;
    lock.refresh_wanted.store(true, Ordering::SeqCst);
    let mut last = Ok(());
    loop {
        let Ok(running) = lock.refresh.try_lock() else {
            return last;
        };
        while lock.refresh_wanted.swap(false, Ordering::SeqCst) {
            last = refresh_once(state, repo_id).await;
            if let Err(error) = &last
                && lock.refresh_wanted.load(Ordering::SeqCst)
            {
                warn!(
                    "refs refresh of {} failed, and runs again for a later request: {error:#}",
                    repo_id.storage_key()
                );
            }
        }
        drop(running);
        if !lock.refresh_wanted.load(Ordering::SeqCst) {
            return last;
        }
    }
}

/// Refresh in the background, logging a failure.
pub(super) fn spawn_refresh(state: &ServerState, repo_id: &RepoId, why: &'static str) {
    let (state, repo_id) = (state.clone(), repo_id.clone());
    tokio::spawn(async move {
        if let Err(error) = refresh(&state, &repo_id).await {
            warn!(
                "{why}: refs refresh of {} failed: {error:#}",
                repo_id.storage_key()
            );
        }
    });
}

/// Return once the repo's branches are published: the first build's Full
/// is ready only with the refs a full clone writes. A refresh that is
/// running is waited for, since the one a first build's Head starts runs
/// after its fetch; with none running, this refreshes.
pub(super) async fn first_branches(state: &ServerState, repo_id: &RepoId) -> Result<()> {
    let published = || async {
        let refs = state.control.get_refs(repo_id).await?;
        anyhow::Ok(refs.is_some_and(|refs| refs.branches.is_some()))
    };
    if published().await? {
        return Ok(());
    }
    tracing::info!(
        "Full of {} waits for the repo's first refs",
        repo_id.storage_key()
    );
    let lock = repo_lock(state, repo_id).await;
    let idle = lock.refresh.try_lock().is_ok();
    if idle {
        refresh(state, repo_id).await?;
    }
    drop(lock.refresh.lock().await);
    anyhow::ensure!(
        published().await?,
        "the refs refresh of {} published no branches",
        repo_id.storage_key()
    );
    Ok(())
}

/// Fetch upstream's branches and tags into the mirror, point its `HEAD` at
/// upstream's default branch, and publish. A mirror with no fetch yet is
/// left to its first build, which publishes and then refreshes.
async fn refresh_once(state: &ServerState, repo_id: &RepoId) -> Result<()> {
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    if !mirror_dir.exists() {
        return Ok(());
    }
    let provider = state
        .provider_registry
        .get(repo_id.provider.as_str())
        .cloned()
        .with_context(|| format!("unknown provider {}", repo_id.provider.as_str()))?;
    let credential = state
        .credentials
        .get(repo_id)
        .await
        .with_context(|| format!("fetch credential for {}", repo_id.storage_key()))?;
    // The fetch cannot name the default branch; one listing of `HEAD` does.
    let default_branch = {
        let _permit = super::ls_remote_semaphore()
            .acquire()
            .await
            .expect("ls-remote semaphore never closed");
        git::ls_remote_tip_async(&provider, repo_id, "HEAD", credential.as_ref())
            .await?
            .and_then(|tip| tip.default_branch)
            .filter(|branch| crate::validation::validate_checkout_name(branch).is_ok())
    };
    let lock = repo_lock(state, repo_id).await;
    // Held through the publish: compaction must not delete the packs the
    // side pack is built from.
    let _reading = lock.read.read().await;
    let fetched = {
        let _writing = lock.write.lock().await;
        let (dir, repo_id) = (mirror_dir.clone(), repo_id.clone());
        cancel::spawn_blocking(move || {
            git::refresh_mirror_refs(
                &dir,
                &provider,
                &repo_id,
                credential.as_ref(),
                default_branch.as_deref(),
            )
        })
        .await
        .context("mirror refs task")??
    };
    if fetched {
        publish(state, repo_id, Scope::All).await?;
    }
    Ok(())
}

/// What a publish rebuilds.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Scope {
    /// The tags only, after a build's fetch. A Head waits on it, so it
    /// waits on nothing but another publish's tags.
    Tags,
    /// The tags, the branches and the side pack, after a refresh.
    All,
}

/// Publish the mirror's refs unless the control store already points at
/// them. A tags publish before the repo's first refresh starts one.
///
/// The branches and side pack are built before taking `lock.refs`, which
/// a Head's tags publish waits on. Only a refresh builds them, and
/// refreshes run one at a time, so no other publish writes them meanwhile.
/// Branches it replaces stay as `ready` when their base's Full is ready.
///
/// What it stores is pinned until the refs that name it are written, and
/// whatever it stored that they do not name is queued for deletion. A Head
/// publishes with [`publish_pinned`] instead, pinning into its build, whose
/// end queues what it stored even when the build's failure cancels this.
pub(super) async fn publish(state: &ServerState, repo_id: &RepoId, scope: Scope) -> Result<()> {
    let pins = PinSet::new(&state.pins);
    let published = publish_pinned(state, repo_id, scope, &state.cas.with_pins(&pins)).await;
    let named = published
        .as_ref()
        .map_or_else(|_| BTreeSet::new(), Clone::clone);
    if let Err(error) = super::purge::queue_unnamed(state, &pins, &named).await {
        warn!(
            "queue the unnamed refs artifacts of {} for deletion: {error:#}",
            repo_id.storage_key()
        );
    }
    published.map(|_| ())
}

/// Publish, storing through `cas`, and return the artifacts the repo's refs
/// name afterwards: none when the repo was purged meanwhile.
pub(super) async fn publish_pinned(
    state: &ServerState,
    repo_id: &RepoId,
    scope: Scope,
    cas: &Cas,
) -> Result<BTreeSet<String>> {
    let lock = repo_lock(state, repo_id).await;
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let branches = match scope {
        Scope::Tags => None,
        Scope::All => {
            let refs = read_refs(&mirror_dir).await?;
            let stored = state.control.get_refs(repo_id).await?.unwrap_or_default();
            match stored.branches {
                Some(previous) if previous.refs_hash == refs.hash() => Some(previous),
                previous => {
                    let tags = if stored.tags_hash == refs.tags_hash {
                        PublishedTags {
                            artifact: stored.tags_artifact,
                            len: stored.tags_len,
                            count: stored.tags_count,
                            hash: stored.tags_hash,
                        }
                    } else {
                        build_tags(state, cas, &mirror_dir, &refs).await?
                    };
                    Some(publish_branches(state, cas, &mirror_dir, refs, previous, tags).await?)
                }
            }
        }
    };
    let _publishing = lock.refs.lock().await;
    // Read again under the lock: a build's fetch may have added tags since.
    let refs = read_refs(&mirror_dir).await?;
    let stored = state.control.get_refs(repo_id).await?;
    let mut row = stored.clone().unwrap_or_default();
    if stored.is_none() || row.tags_hash != refs.tags_hash {
        let tags = match &branches {
            Some(branches) if branches.tags.hash == refs.tags_hash => branches.tags.clone(),
            _ => build_tags(state, cas, &mirror_dir, &refs).await?,
        };
        row.tags_artifact = tags.artifact;
        row.tags_len = tags.len;
        row.tags_count = tags.count;
        row.tags_hash = refs.tags_hash.clone();
    }
    if let Some(branches) = branches
        && row.branches.as_ref() != Some(&branches)
    {
        if let Some(replaced) = row.branches.take()
            && full_ready(state, repo_id, replaced.base.as_deref()).await?
        {
            row.ready = Some(replaced);
        }
        row.branches = Some(branches);
    }
    if stored.as_ref() != Some(&row) {
        if !state.control.set_refs(repo_id, &row).await? {
            return Ok(BTreeSet::new());
        }
        if row.branches != stored.and_then(|stored| stored.branches) {
            tracing::info!(
                "published refs of {}: base {}",
                repo_id.storage_key(),
                (row.branches.as_ref())
                    .and_then(|branches| branches.base.as_deref())
                    .unwrap_or("none")
            );
        }
    }
    if row.branches.is_none() {
        spawn_refresh(state, repo_id, "first build");
    }
    Ok(row.artifacts().into_iter().map(str::to_string).collect())
}

/// Whether `commit`'s Full is ready.
async fn full_ready(state: &ServerState, repo_id: &RepoId, commit: Option<&str>) -> Result<bool> {
    let Some(commit) = commit else {
        return Ok(false);
    };
    let row = state.control.status(repo_id, commit).await?;
    Ok(row.is_some_and(|row| row.full.ready().is_some()))
}

/// Build and store the tags artifact of `refs`.
async fn build_tags(
    state: &ServerState,
    cas: &Cas,
    mirror_dir: &Path,
    refs: &MirrorRefs,
) -> Result<PublishedTags> {
    let (dir, tags) = (mirror_dir.to_path_buf(), refs.tags.clone());
    let (artifact, len) = put_bytes(state, cas, move || crate::tags::build(&dir, &tags))
        .await
        .context("tags artifact")?;
    Ok(PublishedTags {
        artifact,
        len,
        count: refs.tags.len() as u64,
        hash: refs.tags_hash.clone(),
    })
}

async fn read_refs(mirror_dir: &Path) -> Result<MirrorRefs> {
    let dir = mirror_dir.to_path_buf();
    cancel::spawn_blocking(move || MirrorRefs::read(&dir))
        .await
        .context("refs listing task")?
}

/// Side pack layers at most: a refresh that would add one more builds the
/// whole side pack again as one.
const SIDE_LAYERS: usize = 8;

/// The file in the mirror listing the side tips the published layers
/// complete, so the next refresh's layer can leave out what they hold.
const SIDE_TIPS_FILE: &str = "ripclone-side-tips";

/// Build and store the branches artifact and side pack of `refs`.
///
/// While the default branch only moves forward, what the previous
/// refresh's side pack leaves out is still in its history, so the side
/// pack is kept: as it is when the refs it completes are the same, and with
/// one more layer when they changed. The layer holds what the new refs
/// reach that the old refs and the default branch do not, so a push to a
/// side branch or a new tag stores about what it added. After a force-push
/// of the default branch, at [`SIDE_LAYERS`], or when the old refs are not
/// known, the side pack is built whole as one layer.
async fn publish_branches(
    state: &ServerState,
    cas: &Cas,
    mirror_dir: &Path,
    refs: MirrorRefs,
    previous: Option<RepoBranches>,
    tags: PublishedTags,
) -> Result<RepoBranches> {
    let built = refs.build()?;
    let (artifact, len) = put_bytes(state, cas, move || Ok(built))
        .await
        .context("branches artifact")?;
    let refs_hash = refs.hash();
    let base = refs.base().map(str::to_string);
    let side_tips = refs.side_tips_hash();
    let tip_ids: Vec<String> = refs.side_tips().into_iter().map(str::to_string).collect();
    let default_branch = refs.default_branch.clone();
    let forward = match (&previous, &base) {
        (Some(previous), Some(base)) => match &previous.side_base {
            Some(side_base) => moved_forward(mirror_dir, side_base, base).await?,
            None => false,
        },
        _ => false,
    };
    let previous = previous.filter(|_| forward);
    let (side_base, side_packs) = match previous {
        Some(previous) if previous.side_tips == side_tips => {
            (previous.side_base, previous.side_packs)
        }
        Some(previous) if previous.side_packs.len() < SIDE_LAYERS => {
            match read_side_tips(mirror_dir, &previous.side_tips).await {
                Some(older) => {
                    let layer = build_side_pack(state, cas, mirror_dir, refs.clone(), older).await;
                    match layer {
                        Ok(layer) => {
                            let mut layers = previous.side_packs;
                            layers.extend(layer);
                            (base.clone(), layers)
                        }
                        // Such as an old tip a repack dropped.
                        Err(error) => {
                            warn!("side pack layer failed, building it whole: {error:#}");
                            (
                                base.clone(),
                                whole_side_pack(state, cas, mirror_dir, refs).await?,
                            )
                        }
                    }
                }
                None => (
                    base.clone(),
                    whole_side_pack(state, cas, mirror_dir, refs).await?,
                ),
            }
        }
        _ => (
            base.clone(),
            whole_side_pack(state, cas, mirror_dir, refs).await?,
        ),
    };
    write_side_tips(mirror_dir, &tip_ids).await;
    Ok(RepoBranches {
        artifact,
        len,
        refs_hash,
        default_branch,
        base,
        side_base,
        side_tips,
        side_packs,
        tags,
    })
}

/// Whether `old` is in `new`'s history.
async fn moved_forward(mirror_dir: &Path, old: &str, new: &str) -> Result<bool> {
    let (dir, old, new) = (mirror_dir.to_path_buf(), old.to_string(), new.to_string());
    cancel::spawn_blocking(move || crate::gix_util::is_ancestor(&dir, &old, &new))
        .await
        .context("ancestry task")?
}

/// The side tips the mirror's file lists, when they are the ones `hash`
/// names: the published layers' tips.
async fn read_side_tips(mirror_dir: &Path, hash: &str) -> Option<Vec<String>> {
    let listed = tokio::fs::read_to_string(mirror_dir.join(SIDE_TIPS_FILE))
        .await
        .ok()?;
    (crate::cas::hash(listed.as_bytes()) == hash)
        .then(|| listed.lines().map(str::to_string).collect())
}

/// List `tips` in the mirror's file. A failed write only costs the next
/// refresh that adds a layer a whole side pack.
async fn write_side_tips(mirror_dir: &Path, tips: &[String]) {
    let (path, listed) = (mirror_dir.join(SIDE_TIPS_FILE), tips.join("\n"));
    let tmp = path.with_extension("tmp");
    let written = async {
        tokio::fs::write(&tmp, listed).await?;
        tokio::fs::rename(&tmp, &path).await
    };
    if let Err(error) = written.await {
        warn!("write {}: {error}", path.display());
    }
}

/// Build and store the side pack of `refs` whole, as one layer.
async fn whole_side_pack(
    state: &ServerState,
    cas: &Cas,
    mirror_dir: &Path,
    refs: MirrorRefs,
) -> Result<Vec<SidePack>> {
    let layer = build_side_pack(state, cas, mirror_dir, refs, Vec::new()).await?;
    Ok(layer.into_iter().collect())
}

/// Build and store the side pack layer of `refs` above layers completing
/// `older`.
async fn build_side_pack(
    state: &ServerState,
    cas: &Cas,
    mirror_dir: &Path,
    refs: MirrorRefs,
    older: Vec<String>,
) -> Result<Option<SidePack>> {
    let scratch = tempfile::tempdir_in(&state.repo_root).context("create side pack dir")?;
    let (dir, out) = (mirror_dir.to_path_buf(), scratch.path().to_path_buf());
    let files = cancel::spawn_blocking(move || refs.side_pack(&dir, &out, &older))
        .await
        .context("side pack task")??;
    let Some((pack, idx)) = files else {
        return Ok(None);
    };
    let (pack, pack_len) = put_file(state, cas, pack).await.context("side pack")?;
    let (idx, idx_len) = put_file(state, cas, idx).await.context("side pack idx")?;
    Ok(Some(SidePack {
        pack,
        pack_len,
        idx,
        idx_len,
    }))
}

/// Store the bytes `build` makes in `cas` and the artifact store.
async fn put_bytes(
    state: &ServerState,
    cas: &Cas,
    build: impl FnOnce() -> Result<Vec<u8>> + Send + 'static,
) -> Result<(String, u64)> {
    let cas = cas.clone();
    let (artifact, len, path) = cancel::spawn_blocking(move || -> Result<_> {
        let bytes = build()?;
        let artifact = cas.put(&bytes)?;
        let path = cas.path(&artifact);
        Ok((artifact, bytes.len() as u64, path))
    })
    .await
    .context("artifact task")??;
    state
        .storage
        .put_file_async(&artifact, &path)
        .await
        .with_context(|| format!("upload artifact {artifact}"))?;
    Ok((artifact, len))
}

/// Store the file at `source` in `cas` and the artifact store.
async fn put_file(
    state: &ServerState,
    cas: &Cas,
    source: std::path::PathBuf,
) -> Result<(String, u64)> {
    let cas = cas.clone();
    let (artifact, len, path) = cancel::spawn_blocking(move || -> Result<_> {
        let (artifact, _) = crate::cas::hash_file(&source)?;
        let len = cas.put_file_with_hash(&artifact, &source)?;
        let path = cas.path(&artifact);
        Ok((artifact, len, path))
    })
    .await
    .context("artifact task")??;
    state
        .storage
        .put_file_async(&artifact, &path)
        .await
        .with_context(|| format!("upload artifact {artifact}"))?;
    Ok((artifact, len))
}
