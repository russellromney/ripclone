//! Regression: a full (depth=0) editable clonepack for a MULTI-PACK repo must
//! serve an idx bundle whose bytes hash to exactly `manifest.idx_bundle` — the
//! value the client verifies the fetched bundle against.
//!
//! The full clonepack is published in two phases from a detached background task:
//! phase 2a writes the whole `full_clonepack` (manifest + idx_bundle) atomically,
//! phase 2b then re-points only `manifest`/`metadata_chunk` at the archive-bearing
//! variant. Two same-commit builds can overlap (each detached phase 2 keeps
//! running after `/sync` returns, and `should_replace_ref` lets an equal-commit
//! save win). With the old phase-2b partial poke, this interleave left the ref
//! with build A's `manifest` on top of build B's `idx_bundle`: the served
//! `idx_bundle_url` (build B's bundle) and `manifest.idx_bundle` (build A's) then
//! disagreed, so every editable clone failed the idx-bundle hash check. Multi-pack
//! is what makes the two builds' bundles differ (git pack-objects is non-deterministic
//! run to run), which the `RIPCLONE_TEST_PHASE2_RACE` hook reproduces deterministically.
//!
//! Own test binary so the process-global `RIPCLONE_HISTORY_MAX_PACK_BYTES` /
//! `RIPCLONE_TEST_PHASE2_RACE` can't race other tests.

mod common;

use common::*;
use prost::Message;
use ripclone::clonepack::{ClonepackManifest, hash_to_hex};
use ripclone::provider::RepoId;
use ripclone::ref_store::{FileRefStore, RefStore};
use std::sync::Arc;
use std::time::Duration;

/// Deterministic incompressible bytes (xorshift64) so packs don't zlib below the
/// history split threshold.
fn pseudo(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    let mut v = Vec::with_capacity(len);
    while v.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(len);
    v
}

#[tokio::test]
async fn full_idx_bundle_matches_manifest_under_concurrent_multipack_build() {
    // Force git's history pack to split (clamped up to its 1 MiB minimum) and turn
    // on the same-commit race hook. Set before the server/client read them.
    unsafe {
        std::env::set_var("RIPCLONE_HISTORY_MAX_PACK_BYTES", "1");
        std::env::set_var("RIPCLONE_TEST_PHASE2_RACE", "1");
    }
    // LSM on: the cold history split runs through build_history_tail's reuse path,
    // the multi-pack producer.
    init(true);
    let server = start_server().await;
    let origin = make_origin("acme", "idxrace");

    // ~2.8 MiB of incompressible history across 4 commits -> multiple history packs
    // under the 1 MiB split.
    for i in 1..=4u64 {
        let name = format!("big{i}.dat");
        origin.commit_bytes(
            &[(name.as_str(), pseudo(i, 700 * 1024).as_slice())],
            &format!("c{i}"),
        );
    }
    origin.publish();
    register_added_without_build(&server, "acme/idxrace")
        .await
        .expect("register repo");

    // sync1 -> phase-2 build 0 (brackets its editable+files publishes around build 1
    // via the race hook); sync2 -> phase-2 build 1. Both detach after phase 1, so the
    // two full-history builds overlap on the same commit.
    server
        .client()
        .sync_repo("acme/idxrace", None)
        .await
        .expect("sync1");
    server
        .client()
        .sync_repo("acme/idxrace", None)
        .await
        .expect("sync2");

    // Wait out the bracketing delays (build 0: 3s before editable + 8s before files)
    // plus build time, then let the ref settle.
    tokio::time::sleep(Duration::from_secs(16)).await;

    // Read the stored full clonepack directly and decode the manifest it serves.
    let store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&server.repo_root));
    let repo = RepoId::github("acme/idxrace");
    let info = store
        .load_branch(&repo, "main")
        .await
        .expect("load ref")
        .expect("ref exists");
    let fc = &info.full_clonepack;
    assert!(
        !fc.manifest.is_empty() && info.build_status.is_none(),
        "full clonepack must be published (status={:?})",
        info.build_status
    );

    let manifest_bytes = server
        .client()
        .fetch_artifact(&fc.manifest)
        .await
        .expect("fetch served manifest");
    let manifest = ClonepackManifest::decode(manifest_bytes.as_ref()).expect("decode manifest");
    assert!(
        manifest.packs.len() >= 2,
        "fixture must span >1 pack to exercise the multi-pack path, got {}",
        manifest.packs.len()
    );
    let manifest_bundle = hash_to_hex(
        &manifest
            .idx_bundle
            .as_ref()
            .expect("manifest carries an idx bundle")
            .hash,
    );

    // THE INVARIANT: idx_bundle_url is signed over `full_clonepack.idx_bundle`, so
    // the object it serves hashes to exactly that value. The client fetches it and
    // checks it against `manifest.idx_bundle`. They MUST be the same object.
    assert_eq!(
        fc.idx_bundle, manifest_bundle,
        "served idx_bundle ({}) must equal manifest.idx_bundle ({}) — otherwise every \
         editable clone fails: `artifact hash mismatch: expected {manifest_bundle}, got {}`",
        fc.idx_bundle, manifest_bundle, fc.idx_bundle
    );

    // End to end: a full editable clone reconstructs a usable, complete git repo.
    let (_g, d) = clone_full_at(&server, "acme", "idxrace", "4").await;
    assert!(
        git_ok(&d, &["fsck", "--connectivity-only", "HEAD"]),
        "full editable clone must be fsck-clean"
    );
    assert_eq!(git(&d, &["rev-list", "--count", "HEAD"]), "4");
}
