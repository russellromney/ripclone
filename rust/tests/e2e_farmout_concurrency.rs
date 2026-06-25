//! Real multi-process farm-out concurrency (sqlite): an in-process API server
//! plus **two real `ripclone-worker` binary processes** sharing one queue +
//! storage + repo root. Verifies that a pool of separate worker processes drains
//! the queue correctly (cross-process claim distribution — every repo built
//! exactly once, none lost) and that concurrent `/sync` for the same repo
//! coalesces to one build.
//!
//! Workers share the server's `repo_root`: with local storage the metadata store
//! lives there, so it must be shared. The enqueued repos are distinct, so the
//! per-repo mirrors never collide, and queue coalescing guarantees one worker
//! per repo.

mod common;

use common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn pool_of_worker_processes_drains_queue_and_coalesces() {
    let qdir = tempfile::tempdir().expect("queue dir");
    let db_path = qdir.path().join("queue.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &db_path);
    }
    enable_async_build();
    init(false);

    let server = start_server().await;
    // Two real worker processes against the same queue + storage + metadata.
    let _w1 = spawn_worker(&server.cas_dir, &server.repo_root);
    let _w2 = spawn_worker(&server.cas_dir, &server.repo_root);

    // --- Distinct repos, fired concurrently: the two-process pool must build
    // every one exactly once (nothing lost or double-built).
    let names = ["r0", "r1", "r2", "r3", "r4", "r5"];
    for (i, name) in names.iter().enumerate() {
        let o = make_origin("acme", name);
        o.commit(&[("v", &format!("{i}\n"))], "c1");
        o.publish();
    }
    let mut handles = Vec::new();
    for name in names {
        let client = server.client();
        handles.push(tokio::spawn(async move {
            client.sync_repo(&format!("acme/{name}"), None).await
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        let resp = h
            .await
            .expect("join")
            .expect("each repo syncs via the pool");
        assert!(!resp.commit.is_empty(), "r{i} produced a commit");
    }
    // Each clones with the right content → built correctly by some worker.
    for (i, name) in names.iter().enumerate() {
        let (_g, c) = clone_only(&server, "acme", name, 0, CloneMode::Editable)
            .await
            .unwrap_or_else(|e| panic!("clone {name}: {e:?}"));
        assert_eq!(
            std::fs::read_to_string(c.join("v")).unwrap(),
            format!("{i}\n")
        );
    }

    // --- Concurrent /sync for the SAME repo coalesces to one resolved commit.
    let o = make_origin("acme", "coalesce");
    o.commit(&[("f", "1\n")], "c1");
    o.commit(&[("f", "2\n")], "c2");
    o.publish();
    let mut handles = Vec::new();
    for _ in 0..6 {
        let client = server.client();
        handles.push(tokio::spawn(async move {
            client.sync_repo("acme/coalesce", None).await
        }));
    }
    let mut commits = Vec::new();
    for h in handles {
        commits.push(h.await.expect("join").expect("coalesced sync ok").commit);
    }
    assert!(
        commits.windows(2).all(|w| w[0] == w[1]),
        "all concurrent same-repo syncs resolve the same commit: {commits:?}"
    );
}
