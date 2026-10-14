/// One polling pass: for every added repository, cheaply resolve HEAD once
/// (`ls-remote`, under the fetch cap) and admit a build for that commit if it
/// is new. Catches pushes that arrived without a webhook/Actions trigger,
/// so build-before-clone still holds. Best-effort: per-repo errors are logged and
/// skipped. Returns the number of builds admitted. Exposed for tests.
pub async fn poll_once(state: &ServerState) -> usize {
    let repos = match state.control.list_repos().await {
        Ok(r) => r,
        Err(e) => {
            warn!("poll: list repos failed: {e}");
            return 0;
        }
    };
    let mut triggered = 0;
    let mut seen_repos = std::collections::HashSet::new();
    for added in repos {
        let repo_id = added.repo_id;
        if !seen_repos.insert(repo_id.storage_key()) {
            continue;
        }
        let Some(provider) = state
            .provider_registry
            .get(repo_id.provider.as_str())
            .cloned()
        else {
            continue; // unknown provider; skip
        };
        let resolved = match resolve(state, &repo_id, &provider, &Selector::Head).await {
            Ok(resolved) => resolved,
            Err(error) => {
                warn!(
                    "poll: resolve HEAD of {} failed: {}",
                    repo_id.storage_key(),
                    error.error
                );
                continue;
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
                triggered += 1;
                info!(
                    "poll: admitted HEAD build for {} at {}",
                    repo_id.storage_key(),
                    &resolved.commit[..7.min(resolved.commit.len())]
                );
            }
            Ok((_, false)) => {}
            Err(e) => warn!("poll: admit {}@HEAD failed: {e:#}", repo_id.storage_key()),
        }
    }
    triggered
}

/// Spawn the polling-fallback loop. `interval == 0` disables it.
fn spawn_poll_loop(state: ServerState, interval: Duration) {
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
