//! Fast, deterministic GC-race test using local storage.
//!
//! This exercises the same safety property as the S3/MinIO test in
//! `e2e_remote_gc_s3.rs` but without S3 setup, signed-URL proxies, or slow
//! cleanup. It is the local-dev counterpart; the S3 test remains the CI gate.

use crate::common::*;
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
        target: ripclone::server::BarrierTarget::FirstLargeBody,
        entered: Arc::new(std::sync::Mutex::new(Some(entered_tx))),
        proceed: Arc::new(std::sync::Mutex::new(Some(proceed_rx))),
        close_on_proceed: false,
        consumed: Arc::new(AtomicBool::new(false)),
    };
    let server = start_server_split_storage_barrier(barrier).await;

    let origin = make_origin("acme", "gcrace-local");
    origin.commit(&[("a.txt", "gc race\n"), ("b.txt", "x\n")], "c1");
    origin.publish();

    register_added_without_build(&server, "acme/gcrace-local")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/gcrace-local", None)
        .await
        .expect("sync");

    // Serialize downloads so the first large artifact GET deterministically
    // hits the barrier rather than racing with concurrent fetches.
    unsafe {
        std::env::set_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY", "1");
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
    let ref_store: Arc<dyn ripclone::ref_store::RefStore> = server_ref_store(&server).await;
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
        std::env::remove_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY");
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

/// A production-shaped eviction retains B's artifact pointers while deleting
/// the referenced objects. Re-admission must replace that evicted publication,
/// clear the marker, and make both Full and Files cloneable again at exact B.
#[tokio::test]
async fn completed_exact_result_rebuilds_after_real_gc_eviction() {
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "gc-rebuild-exact");
    let b = origin.commit(
        &[("a.txt", "rebuilt B\n"), ("nested/b.txt", "files B\n")],
        "B",
    );
    origin.publish();
    register_added_without_build(&server, "acme/gc-rebuild-exact")
        .await
        .expect("register exact GC fixture");
    server
        .client()
        .sync_repo_at("acme/gc-rebuild-exact", Some(&b), None)
        .await
        .expect("build completed exact B");
    let (_before_full_guard, before_full) = clone_only_at(
        &server,
        "acme",
        "gc-rebuild-exact",
        Some(&b),
        0,
        ripclone::mode::CloneMode::Editable,
    )
    .await
    .expect("completed B is Full-cloneable before eviction");
    assert_eq!(read(&before_full, "a.txt"), "rebuilt B\n");
    let (_before_files_guard, before_files) = clone_only_at(
        &server,
        "acme",
        "gc-rebuild-exact",
        Some(&b),
        0,
        ripclone::mode::CloneMode::Files,
    )
    .await
    .expect("completed B is Files-cloneable before eviction");
    assert_eq!(read(&before_files, "nested/b.txt"), "files B\n");

    let repo_id = ripclone::provider::RepoId::github("acme/gc-rebuild-exact");
    let ref_store = server_ref_store(&server).await;
    let mut completed = ref_store
        .load_result(&repo_id, &b)
        .await
        .unwrap()
        .expect("load completed exact B");
    let completed_full = completed.full.as_ref().expect("Full(B) ready");
    let completed_files = completed.files.as_ref().expect("Files(B) ready");
    let old_manifest = completed_full.clonepack.manifest.clone();
    let old_archive = completed_files.archive_chunks[0].clone();

    // Age only the completed row; RemoteGc performs the real eviction status
    // mutation and removes the now-unreachable artifact objects.
    completed.last_accessed_at = Some(1);
    ref_store.delete_result(&repo_id, &b).await.unwrap();
    ref_store.save_result(&repo_id, &completed).await.unwrap();
    let storage: ripclone::storage::StorageRef = Arc::new(RemoteLocalStorage::new(
        ripclone::storage::local(&server.storage_dir).unwrap(),
    ));
    let report = RemoteGc::new(
        storage,
        Arc::clone(&ref_store),
        GcConfig {
            grace_period: Duration::ZERO,
            warm_ttl: Duration::from_secs(1),
            dry_run: false,
        },
    )
    .run()
    .await
    .expect("run real warm-TTL eviction and object removal");
    assert!(report.objects_deleted > 0, "GC must remove B artifacts");
    assert!(!server.storage_path(&old_manifest).exists());
    assert!(!server.storage_path(&old_archive).exists());
    let evicted = ref_store.load_result(&repo_id, &b).await.unwrap().unwrap();
    assert!(evicted.head.is_none() && evicted.full.is_none() && evicted.files.is_none());

    let (_full_guard, full) = clone_only_at(
        &server,
        "acme",
        "gc-rebuild-exact",
        Some(&b),
        0,
        ripclone::mode::CloneMode::Editable,
    )
    .await
    .expect("Full(B) rebuilds after real eviction");
    assert_eq!(git(&full, &["rev-parse", "HEAD"]), b);
    assert_eq!(read(&full, "a.txt"), "rebuilt B\n");
    assert_repo_usable(&full, "1");

    let (_files_guard, files) = clone_only_at(
        &server,
        "acme",
        "gc-rebuild-exact",
        Some(&b),
        0,
        ripclone::mode::CloneMode::Files,
    )
    .await
    .expect("Files(B) rebuilds after real eviction");
    assert_eq!(read(&files, "a.txt"), "rebuilt B\n");
    assert_eq!(read(&files, "nested/b.txt"), "files B\n");
    assert!(!files.join(".git").exists());

    let rebuilt = ref_store.load_result(&repo_id, &b).await.unwrap().unwrap();
    assert_eq!(rebuilt.commit, b);
    assert!(rebuilt.head.is_some(), "rebuild restores Head");
    assert!(rebuilt.full.is_some(), "rebuild restores Full");
    assert!(rebuilt.files.is_some(), "rebuild restores Files");
}
