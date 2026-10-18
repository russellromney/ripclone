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
use crate::control::{RepoBranches, SidePack};
use crate::git::{self, UpstreamRefs};
use crate::provider::RepoId;
use anyhow::{Context, Result};
use std::path::Path;
use std::sync::atomic::Ordering;
use tracing::warn;

/// Whether the poller should refresh: upstream's refs differ from the ones
/// the last refresh published.
pub(super) async fn changed(
    state: &ServerState,
    repo_id: &RepoId,
    upstream: &UpstreamRefs,
) -> Result<bool> {
    let stored = state.control.get_refs(repo_id).await?;
    Ok(stored
        .and_then(|row| row.branches)
        .is_none_or(|branches| branches.refs_hash != upstream.hash()))
}

/// Refresh the repo's refs. Requests that arrive while one runs are folded
/// into one more pass of it.
pub(super) async fn refresh(state: &ServerState, repo_id: &RepoId) -> Result<()> {
    let lock = repo_lock(state, repo_id).await;
    lock.refresh_wanted.store(true, Ordering::SeqCst);
    loop {
        let Ok(running) = lock.refresh.try_lock() else {
            return Ok(());
        };
        while lock.refresh_wanted.swap(false, Ordering::SeqCst) {
            refresh_once(state, repo_id).await?;
        }
        drop(running);
        if !lock.refresh_wanted.load(Ordering::SeqCst) {
            return Ok(());
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
    /// The tags only, after a build's fetch. Cheap, so a Head waits on it.
    Tags,
    /// The tags, the branches and the side pack, after a refresh.
    All,
}

/// Publish the mirror's refs unless the control store already points at
/// them. A tags publish before the repo's first refresh starts one.
pub(super) async fn publish(state: &ServerState, repo_id: &RepoId, scope: Scope) -> Result<()> {
    let lock = repo_lock(state, repo_id).await;
    let _publishing = lock.refs.lock().await;
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let dir = mirror_dir.clone();
    let refs = cancel::spawn_blocking(move || MirrorRefs::read(&dir))
        .await
        .context("refs listing task")??;
    let stored = state.control.get_refs(repo_id).await?;
    let mut row = stored.clone().unwrap_or_default();
    if stored.is_none() || row.tags_hash != refs.tags_hash {
        let (dir, tags) = (mirror_dir.clone(), refs.tags.clone());
        let (artifact, len) = put_bytes(state, move || crate::tags::build(&dir, &tags))
            .await
            .context("tags artifact")?;
        row.tags_artifact = artifact;
        row.tags_len = len;
        row.tags_count = refs.tags.len() as u64;
        row.tags_hash = refs.tags_hash.clone();
    }
    let refs_hash = refs.hash();
    if scope == Scope::All
        && row
            .branches
            .as_ref()
            .is_none_or(|branches| branches.refs_hash != refs_hash)
    {
        row.branches = Some(publish_branches(state, &mirror_dir, refs, refs_hash).await?);
    }
    if stored.as_ref() != Some(&row) {
        state.control.set_refs(repo_id, &row).await?;
    }
    if row.branches.is_none() {
        spawn_refresh(state, repo_id, "first build");
    }
    Ok(())
}

/// Build and store the branches artifact and side pack of `refs`.
async fn publish_branches(
    state: &ServerState,
    mirror_dir: &Path,
    refs: MirrorRefs,
    refs_hash: String,
) -> Result<RepoBranches> {
    let built = refs.build()?;
    let (artifact, len) = put_bytes(state, move || Ok(built))
        .await
        .context("branches artifact")?;
    let scratch = tempfile::tempdir_in(&state.repo_root).context("create side pack dir")?;
    let (dir, out) = (mirror_dir.to_path_buf(), scratch.path().to_path_buf());
    let files = cancel::spawn_blocking(move || refs.side_pack(&dir, &out))
        .await
        .context("side pack task")??;
    let side_pack = match files {
        None => None,
        Some((pack, idx)) => {
            let (pack, pack_len) = put_file(state, pack).await.context("side pack")?;
            let (idx, idx_len) = put_file(state, idx).await.context("side pack idx")?;
            Some(SidePack {
                pack,
                pack_len,
                idx,
                idx_len,
            })
        }
    };
    Ok(RepoBranches {
        artifact,
        len,
        refs_hash,
        side_pack,
    })
}

/// Store the bytes `build` makes in the CAS and the artifact store.
async fn put_bytes(
    state: &ServerState,
    build: impl FnOnce() -> Result<Vec<u8>> + Send + 'static,
) -> Result<(String, u64)> {
    let cas = state.cas.clone();
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

/// Store the file at `source` in the CAS and the artifact store.
async fn put_file(state: &ServerState, source: std::path::PathBuf) -> Result<(String, u64)> {
    let cas = state.cas.clone();
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
