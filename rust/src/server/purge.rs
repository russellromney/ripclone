//! Purging a repository: its rows go at once, its mirror once no build
//! holds it, and each artifact only it referenced once no clone can still be
//! reading it.
//!
//! A purge queues those artifacts for deletion after twice the signed URL
//! lifetime, so a clone that read the repo before the purge finishes. A
//! build that ends without publishing queues what it stored for deletion at
//! once. A sweep deletes a queued artifact only when it is due, no
//! repository references it, and no running build has pinned it.

use super::ServerState;
use super::worker::repo_lock;
use crate::control::{Clock, Purged, SystemClock};
use crate::pins::PinSet;
use crate::provider::RepoId;
use anyhow::{Context, Result};
use futures::StreamExt;
use std::collections::{BTreeSet, HashSet};
use std::time::Duration;
use tracing::{info, warn};

/// Queued artifacts read per batch of a sweep.
const SWEEP_BATCH: usize = 1000;
/// Deletions running at once.
const SWEEP_PARALLEL: usize = 8;

/// Remove `repo`'s rows and queue its artifacts, then remove its mirror in
/// the background.
pub(super) async fn purge(state: &ServerState, repo: &RepoId) -> Result<Purged> {
    let grace = i64::try_from(state.signed_url_ttl.as_secs().saturating_mul(2)).unwrap_or(i64::MAX);
    let due_at = SystemClock.now().saturating_add(grace);
    let purged = state.control.purge_repo(repo, due_at).await?;
    info!(
        repo = %repo.storage_key(),
        builds = purged.builds,
        queued = purged.queued,
        "purged repository"
    );
    let (state, repo) = (state.clone(), repo.clone());
    tokio::spawn(async move {
        if let Err(error) = remove_mirror(&state, &repo).await {
            warn!(
                "remove the mirror of purged {}: {error:#}",
                repo.storage_key()
            );
        }
    });
    Ok(purged)
}

/// Remove `repo`'s mirror once no build reads or fetches it, unless the
/// repo was added or admitted again meanwhile.
async fn remove_mirror(state: &ServerState, repo: &RepoId) -> Result<()> {
    let lock = repo_lock(state, repo).await;
    let _read = lock.read.write().await;
    let _write = lock.write.lock().await;
    if state.control.knows_repo(repo).await? {
        return Ok(());
    }
    remove_dir(state.repo_root.join(repo.mirror_dir_name())).await
}

/// Remove every mirror of a repository the control store no longer knows:
/// the purges that a stop interrupted. Runs before the workers start.
pub(super) async fn remove_orphan_mirrors(state: &ServerState) -> Result<()> {
    let known: HashSet<String> = state
        .control
        .known_repos()
        .await?
        .iter()
        .map(RepoId::mirror_dir_name)
        .collect();
    let entries = std::fs::read_dir(&state.repo_root)
        .with_context(|| format!("list {}", state.repo_root.display()))?;
    for entry in entries {
        let entry = entry.context("list mirrors")?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".git") || known.contains(&name) {
            continue;
        }
        info!(mirror = %name, "removing the mirror of an unknown repository");
        remove_dir(entry.path()).await?;
    }
    Ok(())
}

async fn remove_dir(dir: std::path::PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&dir) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            Err(error).with_context(|| format!("remove {}", dir.display()))
        }
        _ => Ok(()),
    })
    .await
    .context("remove task")?
}

/// Queue each artifact `pins` holds that `keep` does not name for deletion
/// now. Deletion keeps those that any repository names.
pub(super) async fn queue_unnamed(
    state: &ServerState,
    pins: &PinSet,
    keep: &BTreeSet<String>,
) -> Result<()> {
    let mut hashes = pins.hashes();
    hashes.retain(|hash| !keep.contains(hash));
    if hashes.is_empty() {
        return Ok(());
    }
    state
        .control
        .queue_deletions(&hashes, SystemClock.now())
        .await
}

/// Sweep every `interval`. A zero interval turns sweeping off, as it does
/// local cache retention.
pub(super) fn spawn_sweeper(state: ServerState, interval: Duration) {
    if interval.is_zero() {
        warn!("artifact deletion disabled: RIPCLONE_RETENTION_INTERVAL_SECS is 0");
        return;
    }
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            match sweep(&state).await {
                Ok(0) => {}
                Ok(deleted) => info!(deleted, "deleted unreferenced artifacts"),
                Err(error) => {
                    state.metrics.record_error();
                    warn!("artifact deletion failed: {error:#}");
                }
            }
        }
    });
}

/// Delete the due, unreferenced, unpinned artifacts. Returns how many.
/// Each batch is read and dequeued in one statement; only the checks that
/// must follow an artifact's deletion mark run per artifact.
pub(super) async fn sweep(state: &ServerState) -> Result<u64> {
    let mut deleted = 0;
    loop {
        let now = SystemClock.now();
        let due = state.control.due_deletions(now, SWEEP_BATCH).await?;
        let full = due.len() == SWEEP_BATCH;
        let mut settled = Vec::with_capacity(due.len());
        let mut failure = None;
        let mut results = futures::stream::iter(due)
            .map(|hash| async move {
                let swept = sweep_one(state, &hash, now).await;
                (hash, swept)
            })
            .buffer_unordered(SWEEP_PARALLEL);
        while let Some((hash, swept)) = results.next().await {
            match swept {
                Ok(Swept::Deleted) => {
                    deleted += 1;
                    settled.push(hash);
                }
                Ok(Swept::Kept) => settled.push(hash),
                Ok(Swept::Pinned) => {}
                Err(error) => {
                    // Stop at the first failure, such as storage being
                    // down, rather than fail the rest of the batch too.
                    failure = Some(error);
                    break;
                }
            }
        }
        drop(results);
        state.control.dequeue_deletions(&settled, now).await?;
        if let Some(error) = failure {
            return Err(error);
        }
        // A full batch of pinned artifacts would be read again forever.
        if !full || settled.is_empty() {
            return Ok(deleted);
        }
    }
}

enum Swept {
    Deleted,
    /// Referenced again, or due later: dequeued or left for its due time.
    Kept,
    /// A running build relies on it; the next sweep tries again.
    Pinned,
}

/// Delete `hash` if it is still deletable once marked as being deleted: a
/// build that relied on it may have published since the batch was read.
async fn sweep_one(state: &ServerState, hash: &str, now: i64) -> Result<Swept> {
    let Some(_deleting) = state.pins.begin_delete(hash) else {
        return Ok(Swept::Pinned);
    };
    if !state.control.deletable(hash, now).await? {
        return Ok(Swept::Kept);
    }
    state.storage.delete(hash).await?;
    state.cas.remove(hash)?;
    state.metrics.record_artifact_deleted();
    Ok(Swept::Deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::test_state;

    #[tokio::test]
    async fn a_sweep_skips_pinned_artifacts_until_they_are_released() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let free = state.cas.put(b"free").unwrap();
        let pinned = state.cas.put(b"pinned").unwrap();
        let later = state.cas.put(b"later").unwrap();
        let now = SystemClock.now();
        let due = [free.clone(), pinned.clone()];
        state.control.queue_deletions(&due, now - 1).await.unwrap();
        state
            .control
            .queue_deletions(std::slice::from_ref(&later), now + 3600)
            .await
            .unwrap();
        let build = PinSet::new(&state.pins);
        build.pin(&pinned);

        assert_eq!(sweep(&state).await.unwrap(), 1);
        assert!(!state.cas.has(&free));
        assert!(state.cas.has(&pinned), "a build relies on it");
        assert!(state.cas.has(&later), "not due");

        drop(build);
        assert_eq!(sweep(&state).await.unwrap(), 1);
        assert!(!state.cas.has(&pinned));
        assert!(state.cas.has(&later));
        assert_eq!(sweep(&state).await.unwrap(), 0, "the queue is empty");
    }

    #[tokio::test]
    async fn startup_removes_only_mirrors_of_unknown_repos() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let added = RepoId::github("acme/widget");
        let gone = RepoId::github("acme/gone");
        state.control.add_repo(&added).await.unwrap();
        for repo in [&added, &gone] {
            std::fs::create_dir_all(state.repo_root.join(repo.mirror_dir_name()).join("objects"))
                .unwrap();
        }
        let scratch = state.repo_root.join(".tmp-fetch");
        std::fs::create_dir_all(&scratch).unwrap();

        remove_orphan_mirrors(&state).await.unwrap();
        assert!(state.repo_root.join(added.mirror_dir_name()).exists());
        assert!(!state.repo_root.join(gone.mirror_dir_name()).exists());
        assert!(scratch.exists(), "only mirrors are removed");
    }
}
