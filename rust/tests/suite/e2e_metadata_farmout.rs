//! The realistic farm-out config: BOTH the queue and the metadata store on SQL,
//! with a separate `ripclone-worker` process. The worker writes the ref into the
//! shared sqlite metadata DB; the server reads it back. Also checks cross-process
//! freshness — a second sync of a new commit must return the fresh ref, not a
//! cached one (the SQL `/sync` path invalidates the server's ref cache).

use crate::common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn metadata_and_queue_on_sqlite_with_real_worker() {
    let qdir = tempfile::tempdir().expect("queue dir");
    let mdir = tempfile::tempdir().expect("metadata dir");
    let queue_url = qdir.path().join("queue.db").to_string_lossy().to_string();
    let meta_url = mdir.path().join("meta.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &queue_url);
        std::env::set_var("RIPCLONE_METADATA", "sqlite");
        std::env::set_var("RIPCLONE_METADATA_DB_URL", &meta_url);
        std::env::set_var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "10");
    }
    init(false);

    let server = start_server().await;
    // Real separate worker, sharing queue + metadata + storage via disk.
    let _worker = spawn_worker(&server.cas_dir, &server.repo_root);

    let origin = make_origin("acme", "mfarm");
    origin.commit(&[("a.txt", "1\n")], "c1");
    let commit2 = origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();

    // The worker builds and writes the ref into the shared sqlite metadata DB;
    // the server reads it back across the process boundary.
    register_added_without_build(&server, "acme/mfarm")
        .await
        .expect("add repo");
    let resp = server
        .client()
        .sync_repo("acme/mfarm", None)
        .await
        .expect("farm-out sync with sqlite metadata");
    assert_eq!(resp.commit, commit2);

    let (g0, c) = clone_only(&server, "acme", "mfarm", 0, CloneMode::Editable)
        .await
        .expect("clone");
    assert_eq!(std::fs::read_to_string(c.join("a.txt")).unwrap(), "2\n");
    drop(g0);

    // Push a new commit: the second sync must reflect it (the worker writes the
    // new ref to SQL; the server must not serve a stale cached ref).
    let commit3 = origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();
    let resp2 = server
        .client()
        .sync_repo("acme/mfarm", None)
        .await
        .expect("second farm-out sync");
    assert_ne!(commit3, commit2);
    assert_eq!(
        resp2.commit, commit3,
        "cross-process metadata must be fresh after the worker's build"
    );

    let (_g, c2) = clone_only(&server, "acme", "mfarm", 0, CloneMode::Editable)
        .await
        .expect("clone after second sync");
    assert_eq!(std::fs::read_to_string(c2.join("a.txt")).unwrap(), "3\n");
}
