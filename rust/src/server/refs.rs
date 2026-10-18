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
use crate::control::{RepoBranches, SidePack};
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
///
/// What it stores is pinned until the refs that name it are written, and
/// whatever it stored that they do not name is queued for deletion.
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

/// Publish, returning the artifacts the repo's refs name afterwards: none
/// when the repo was purged meanwhile.
async fn publish_pinned(
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
            let previous = state
                .control
                .get_refs(repo_id)
                .await?
                .and_then(|row| row.branches);
            match previous {
                Some(previous) if previous.refs_hash == refs.hash() => Some(previous),
                previous => Some(publish_branches(state, cas, &mirror_dir, refs, previous).await?),
            }
        }
    };
    let _publishing = lock.refs.lock().await;
    // Read again under the lock: a build's fetch may have added tags since.
    let refs = read_refs(&mirror_dir).await?;
    let stored = state.control.get_refs(repo_id).await?;
    let mut row = stored.clone().unwrap_or_default();
    if stored.is_none() || row.tags_hash != refs.tags_hash {
        let (dir, tags) = (mirror_dir.clone(), refs.tags.clone());
        let (artifact, len) = put_bytes(state, cas, move || crate::tags::build(&dir, &tags))
            .await
            .context("tags artifact")?;
        row.tags_artifact = artifact;
        row.tags_len = len;
        row.tags_count = refs.tags.len() as u64;
        row.tags_hash = refs.tags_hash.clone();
    }
    if branches.is_some() {
        row.branches = branches;
    }
    if stored.as_ref() != Some(&row) && !state.control.set_refs(repo_id, &row).await? {
        return Ok(BTreeSet::new());
    }
    if row.branches.is_none() {
        spawn_refresh(state, repo_id, "first build");
    }
    Ok(row.artifacts().into_iter().map(str::to_string).collect())
}

async fn read_refs(mirror_dir: &Path) -> Result<MirrorRefs> {
    let dir = mirror_dir.to_path_buf();
    cancel::spawn_blocking(move || MirrorRefs::read(&dir))
        .await
        .context("refs listing task")?
}

/// Build and store the branches artifact and side pack of `refs`. The
/// previous refresh's side pack is kept when the refs it completes are the
/// same and the default branch only moved forward: what it leaves out is
/// still in the default branch's history.
async fn publish_branches(
    state: &ServerState,
    cas: &Cas,
    mirror_dir: &Path,
    refs: MirrorRefs,
    previous: Option<RepoBranches>,
) -> Result<RepoBranches> {
    let built = refs.build()?;
    let (artifact, len) = put_bytes(state, cas, move || Ok(built))
        .await
        .context("branches artifact")?;
    let refs_hash = refs.hash();
    let base = refs.base().map(str::to_string);
    let side_tips = refs.side_tips_hash();
    let default_branch = refs.default_branch.clone();
    let kept = match (previous, &base) {
        (Some(previous), Some(base)) if previous.side_tips == side_tips => {
            match previous.side_base.clone() {
                Some(side_base) if moved_forward(mirror_dir, &side_base, base).await? => {
                    Some((Some(side_base), previous.side_pack))
                }
                _ => None,
            }
        }
        _ => None,
    };
    let (side_base, side_pack) = match kept {
        Some(kept) => kept,
        None => (
            base.clone(),
            build_side_pack(state, cas, mirror_dir, refs).await?,
        ),
    };
    Ok(RepoBranches {
        artifact,
        len,
        refs_hash,
        default_branch,
        base,
        side_base,
        side_tips,
        side_pack,
    })
}
/// Whether `old` is in `new`'s history.
async fn moved_forward(mirror_dir: &Path, old: &str, new: &str) -> Result<bool> {
    let (dir, old, new) = (mirror_dir.to_path_buf(), old.to_string(), new.to_string());
    cancel::spawn_blocking(move || git::is_ancestor(&dir, &old, &new))
        .await
        .context("ancestry task")?
}

/// Build and store the side pack of `refs`.
async fn build_side_pack(
    state: &ServerState,
    cas: &Cas,
    mirror_dir: &Path,
    refs: MirrorRefs,
) -> Result<Option<SidePack>> {
    let scratch = tempfile::tempdir_in(&state.repo_root).context("create side pack dir")?;
    let (dir, out) = (mirror_dir.to_path_buf(), scratch.path().to_path_buf());
    let files = cancel::spawn_blocking(move || refs.side_pack(&dir, &out))
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
