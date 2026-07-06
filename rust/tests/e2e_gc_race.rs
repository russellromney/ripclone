//! Fast, deterministic GC-race test using local storage.
//!
//! This exercises the same safety property as the S3/MinIO test in
//! `e2e_remote_gc_s3.rs` but without S3 setup, signed-URL proxies, or slow
//! cleanup. It is the local-dev counterpart; the S3 test remains the CI gate.

mod common;

use common::*;
use ripclone::remote_gc::{GcConfig, RemoteGc};
use ripclone::server::ArtifactBarrier;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// Race: `RemoteGc` with grace=0 must not corrupt a clone stalled mid-chunk.
/// We use the server-side `ArtifactBarrier` to pause the first artifact body
/// after 16 bytes, run GC while the download is blocked, then release the
/// barrier. The clone either completes with a correct tree or fails cleanly
/// without leaving a partial target directory.
#[tokio::test]
async fn remote_gc_during_local_clone_is_safe() {
    init(false);

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let barrier = ArtifactBarrier {
        after_bytes: 16,
        entered: Arc::new(std::sync::Mutex::new(Some(entered_tx))),
        proceed: Arc::new(std::sync::Mutex::new(Some(proceed_rx))),
        close_on_proceed: false,
        consumed: Arc::new(AtomicBool::new(false)),
    };
    let server = start_server_split_storage_barrier(barrier).await;

    let origin = make_origin("acme", "gcrace-local");
    origin.commit(&[("a.txt", "gc race\n"), ("b.txt", "x\n")], "c1");
    origin.publish();

    server
        .client()
        .sync_repo("acme/gcrace-local", None)
        .await
        .expect("sync");

    // Serialize downloads so the first large artifact GET deterministically
    // hits the barrier rather than racing with concurrent fetches.
    unsafe {
        std::env::set_var("RIPCLONE_EDITABLE_DOWNLOAD_CONCURRENCY", "1");
    }

    let client = server.client();
    let repo_path = "acme/gcrace-local".to_string();
    let clone_task = tokio::spawn(async move {
        let out = tempfile::tempdir().expect("clone temp dir");
        let target = out.path().join("clone");
        let result = client
            .install_repo_with_mode_at(
                &repo_path,
                "HEAD",
                None,
                &target,
                ripclone::mode::CloneMode::Files,
                Some("full"),
                None,
            )
            .await;
        (result, out, target)
    });

    // Wait until the server has sent the first bytes and is stalled mid-body.
    entered_rx.await.expect("barrier entered");

    // Run remote GC against the same wrapped-local storage the server uses.
    // `RemoteLocalStorage` reports `is_remote() = true` so `RemoteGc::run`
    // actually scans and deletes instead of short-circuiting.
    let storage: ripclone::storage::StorageRef = Arc::new(RemoteLocalStorage::new(
        ripclone::storage::local(&server.storage_dir).unwrap(),
    ));
    let ref_store: Arc<dyn ripclone::ref_store::RefStore> =
        Arc::new(ripclone::ref_store::FileRefStore::new(&server.repo_root));
    let gc = RemoteGc::new(
        storage,
        ref_store,
        GcConfig {
            grace_period: Duration::ZERO,
            dry_run: false,
            ..Default::default()
        },
    );
    let report = gc.run().await.expect("remote gc run during clone");
    eprintln!("GC during clone: {report:?}");

    // Release the barrier and let the clone finish (or fail cleanly).
    proceed_tx.send(()).expect("release barrier");

    let (result, _out, target) = clone_task.await.expect("clone task joined");
    unsafe {
        std::env::remove_var("RIPCLONE_EDITABLE_DOWNLOAD_CONCURRENCY");
    }

    match result {
        Ok(_) => {
            assert!(target.exists(), "successful clone must materialize target");
            assert_eq!(
                std::fs::read_to_string(target.join("a.txt")).unwrap_or_default(),
                "gc race\n",
                "clone content must be intact"
            );
            assert_eq!(
                std::fs::read_to_string(target.join("b.txt")).unwrap_or_default(),
                "x\n",
                "clone content must be intact"
            );
        }
        Err(_) => {
            assert!(
                !target.exists(),
                "failed clone must not leave a partial tree at target"
            );
        }
    }
}
