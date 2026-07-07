//! Diskless multi-machine farm-out regression test.
//!
//! The whole point of the `postgres`/`mysql`/`libsql` queues is workers on *other*
//! machines: they share storage + the queue + the metadata store, but NOT the
//! bare-mirror `repo_root`. The other worker e2e tests run the worker with the
//! server's own `repo_root`, so the server always has the mirror — they cannot
//! catch a server that must answer `/sync` without one.
//!
//! Here the worker runs with a SEPARATE `repo_root` from the server (same CAS /
//! storage, same SQLite queue, same SQLite metadata store). After the worker
//! builds, the server has no local mirror to map a requested `HEAD` to the
//! concrete default branch `do_sync` stored the ref under — so `/sync HEAD` must
//! resolve purely from the shared metadata store (the `HEAD` ref alias). Without
//! that, the server returns a 200 carrying an empty placeholder ref.

mod common;

use common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn diskless_worker_head_sync_returns_real_ref() {
    let qdir = tempfile::tempdir().expect("queue dir");
    let db_path = qdir.path().join("queue.db").to_string_lossy().to_string();
    let mdir = tempfile::tempdir().expect("metadata dir");
    let meta_path = mdir.path().join("meta.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &db_path);
        // Shared metadata store is mandatory here: a separate repo_root means a
        // file metadata store would NOT be shared between server and worker.
        std::env::set_var("RIPCLONE_METADATA", "sqlite");
        std::env::set_var("RIPCLONE_METADATA_DB_URL", &meta_path);
        std::env::set_var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "8");
    }
    init(false);

    let server = start_server().await;
    // Worker shares the server's CAS (storage) but gets its OWN repo_root, so the
    // server never sees the bare mirror — the diskless multi-machine shape.
    let worker_repos = tempfile::tempdir().expect("worker repo root");
    let _worker = spawn_worker(&server.cas_dir, worker_repos.path());

    let origin = make_origin("acme", "diskless");
    origin.commit(&[("a.txt", "1\n")], "c1");
    let commit = origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();

    // `/sync HEAD` is built by the worker (in its own repo_root) and the server —
    // which has no mirror — must still return the real ref, not an empty
    // placeholder. This is the regression: it asserts a non-empty commit equal to
    // the published tip.
    register_added_without_build(&server, "acme/diskless")
        .await
        .expect("add repo");
    let resp = server
        .client()
        .sync_repo("acme/diskless", None)
        .await
        .expect("diskless HEAD sync should return the ref");
    assert!(
        !resp.commit.is_empty(),
        "diskless HEAD sync must return a real (non-empty) commit"
    );
    assert_eq!(
        resp.commit, commit,
        "diskless HEAD sync must resolve the published tip, not a stale/empty ref"
    );

    // Clone from the diskless server. The resolve path fetches the bare mirror
    // onto the server on demand and reuses the worker's pre-built artifacts
    // (matched by commit) from shared storage — so the expensive build stays
    // offloaded to the worker while clone still works without a pre-existing
    // mirror on the server.
    let (_g, c) = clone_only(&server, "acme", "diskless", 0, CloneMode::Editable)
        .await
        .expect("diskless clone should succeed");
    assert_eq!(std::fs::read_to_string(c.join("a.txt")).unwrap(), "2\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));
}
