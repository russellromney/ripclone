//! Polling fallback: admit the upstream HEAD of every added repository.

use super::ServerState;
use super::api::{admit_build, resolve};
use crate::provider::RepoId;
use crate::validation::Selector;
use futures::StreamExt;
use std::time::Duration;
use tracing::{info, warn};

/// Repositories one polling pass resolves at once. Below the ls-remote cap,
/// so a pass over many repositories leaves room for API admissions.
const POLL_CONCURRENCY: usize = 8;

/// One polling pass: for every added repository, cheaply resolve HEAD once
/// (`ls-remote`, under the ls-remote cap) and admit a build for that commit
/// if it is new. Up to [`POLL_CONCURRENCY`] repositories resolve at once, so
/// one slow upstream does not hold up the rest. Catches pushes that arrived
/// without a webhook/Actions trigger, so build-before-clone still holds.
/// Best-effort: per-repo errors are logged and skipped. Returns the number of
/// builds admitted.
pub async fn poll_once(state: &ServerState) -> usize {
    let repos = match state.control.list_repos().await {
        Ok(r) => r,
        Err(e) => {
            warn!("poll: list repos failed: {e}");
            return 0;
        }
    };
    let mut seen_repos = std::collections::HashSet::new();
    let repos: Vec<RepoId> = repos
        .into_iter()
        .map(|added| added.repo_id)
        .filter(|repo_id| seen_repos.insert(repo_id.storage_key()))
        .collect();
    futures::stream::iter(repos)
        .map(|repo_id| poll_repo(state, repo_id))
        .buffer_unordered(POLL_CONCURRENCY)
        .filter(|admitted| std::future::ready(*admitted))
        .count()
        .await
}

/// Resolve one repository's HEAD and admit it. True when a build was
/// admitted.
async fn poll_repo(state: &ServerState, repo_id: RepoId) -> bool {
    let Some(provider) = state
        .provider_registry
        .get(repo_id.provider.as_str())
        .cloned()
    else {
        return false; // unknown provider; skip
    };
    let resolved = match resolve(state, &repo_id, &provider, &Selector::Head).await {
        Ok(resolved) => resolved,
        Err(error) => {
            warn!(
                "poll: resolve HEAD of {} failed: {}",
                repo_id.storage_key(),
                error.error
            );
            return false;
        }
    };
    match admit_build(
        state,
        &repo_id,
        &resolved.commit,
        resolved.ref_name.as_deref(),
        false,
    )
    .await
    {
        Ok((_, true)) => {
            info!(
                "poll: admitted HEAD build for {} at {}",
                repo_id.storage_key(),
                &resolved.commit[..7.min(resolved.commit.len())]
            );
            true
        }
        Ok((_, false)) => false,
        Err(e) => {
            warn!("poll: admit {}@HEAD failed: {e:#}", repo_id.storage_key());
            false
        }
    }
}

/// Spawn the polling-fallback loop. `interval == 0` disables it.
pub(super) fn spawn_poll_loop(state: ServerState, interval: Duration) {
    if interval.is_zero() {
        info!("poll fallback disabled (RIPCLONE_POLL_INTERVAL_SECS=0)");
        return;
    }
    info!("poll fallback enabled every {:?}", interval);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let n = poll_once(&state).await;
            if n > 0 {
                info!("poll fallback: triggered {n} build(s)");
            }
        }
    });
}
