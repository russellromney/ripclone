//! What each repository uses: build time per output kind, recorded as each
//! claim ends, and the size of each artifact it names, recorded before the
//! output or refs naming it publish. `GET .../usage` reads both from the
//! control store.

use super::ServerState;
use crate::control::Claim;
use anyhow::{Context, Result};
use std::time::Duration;
use tracing::warn;

/// Add an ended claim's wall time to its repository's usage. A failed write
/// is logged: usage never fails a build.
pub(super) async fn record_build_time(state: &ServerState, claim: &Claim, elapsed: Duration) {
    if let Err(error) = state
        .control
        .add_build_time(&claim.repo, claim.kind(), elapsed)
        .await
    {
        warn!(
            "record the {} build time of {}@{}: {error:#}",
            claim.kind(),
            claim.repo.storage_key(),
            claim.build.commit
        );
    }
}

/// Record the size of each of `hashes` not yet recorded: from the local
/// copy, which a build's new artifacts have, else from storage. A size that
/// cannot be read is logged, and usage counts that artifact as unsized.
pub(super) async fn record_sizes(state: &ServerState, hashes: &[&str]) {
    if let Err(error) = try_record_sizes(state, hashes).await {
        warn!("record artifact sizes: {error:#}");
    }
}

async fn try_record_sizes(state: &ServerState, hashes: &[&str]) -> Result<()> {
    let wanted = state.control.unsized_artifacts(hashes).await?;
    if wanted.is_empty() {
        return Ok(());
    }
    let (cas, storage) = (state.cas.clone(), state.storage.clone());
    // Storage reads block, and remote storage makes a request for each.
    let sizes = tokio::task::spawn_blocking(move || {
        let mut sizes = Vec::with_capacity(wanted.len());
        for hash in wanted {
            match std::fs::metadata(cas.path(&hash)) {
                Ok(metadata) => sizes.push((hash, metadata.len())),
                Err(_) => match storage.size(&hash) {
                    Ok(len) => sizes.push((hash, len)),
                    Err(error) => warn!("read the size of artifact {hash}: {error:#}"),
                },
            }
        }
        sizes
    })
    .await
    .context("artifact size task")?;
    state.control.record_sizes(&sizes).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::test_state;

    #[tokio::test]
    async fn sizes_come_from_the_local_copy_else_from_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp).await;
        state.storage = crate::storage::local(tmp.path().join("bucket")).unwrap();
        let local = state.cas.put(b"local copy").unwrap();
        let remote = crate::cas::hash(b"only in storage");
        state.storage.put(&remote, b"only in storage").unwrap();
        let missing = crate::cas::hash(b"nowhere");
        let hashes = [local.as_str(), remote.as_str(), missing.as_str()];

        record_sizes(&state, &hashes).await;
        assert_eq!(
            state.control.unsized_artifacts(&hashes).await.unwrap(),
            vec![missing.clone()],
            "the others are recorded"
        );
        let sizes = usage_sizes(&state, &[&local, &remote]).await;
        assert_eq!(sizes, [10, 15]);
    }

    /// Each artifact's size as usage reads it, named alone by its own repo.
    async fn usage_sizes(state: &ServerState, hashes: &[&str]) -> Vec<u64> {
        let mut sizes = Vec::new();
        for (n, hash) in hashes.iter().enumerate() {
            let repo = crate::provider::RepoId::github(format!("acme/r{n}"));
            state.control.add_repo(&repo, None).await.unwrap();
            let refs = crate::control::RepoRefs {
                tags_artifact: hash.to_string(),
                ..Default::default()
            };
            assert!(state.control.set_refs(&repo, &refs).await.unwrap());
            let usage = state.control.usage(&repo).await.unwrap().unwrap();
            sizes.push(usage.unique_bytes);
        }
        sizes
    }
}
