//! True two-process farm-out e2e with the **real `ripclone-worker` binary** and
//! the `sqlite` queue. The HTTP server runs in-process; the worker runs as a
//! separate OS process sharing storage + repo root + the queue file via disk —
//! exactly how a user runs farm-out. Covers the positive path (sync → worker
//! builds → clone) and the negative path (a build that fails → `/sync` errors).

mod common;

use common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn worker_binary_farm_out_sqlite() {
    let qdir = tempfile::tempdir().expect("queue dir");
    let db_path = qdir.path().join("queue.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &db_path);
        // Fail fast on the negative case instead of retrying for 80s.
        std::env::set_var("RIPCLONE_SYNC_MAX_ATTEMPTS", "8");
    }
    enable_async_build();
    init(false);

    let server = start_server().await;
    // Real, separate worker process. The server enqueues only.
    let _worker = spawn_worker(&server.cas_dir, &server.repo_root);

    // --- Positive: a published repo is built by the worker and clones cleanly.
    let origin = make_origin("acme", "wp");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();

    let resp = server
        .client()
        .sync_repo("acme/wp", None)
        .await
        .expect("farm-out sync should succeed");
    assert!(!resp.commit.is_empty());

    let (_g, c) = clone_only(&server, "acme", "wp", 0, CloneMode::Editable)
        .await
        .expect("clone after farm-out build");
    assert_eq!(std::fs::read_to_string(c.join("a.txt")).unwrap(), "hello\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    // --- Negative: a repo that was never published → the worker's build fails →
    // `/sync` must surface an error to the user, not hang or 200.
    let result = server.client().sync_repo("acme/does-not-exist", None).await;
    assert!(
        result.is_err(),
        "sync of a missing upstream must fail, got {result:?}"
    );
}
