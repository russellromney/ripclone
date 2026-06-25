//! End-to-end test for the pluggable SQL queue: `/sync` enqueues a build into a
//! shared libsql (local-file) jobs table and a *separate* worker (here, a
//! background task standing in for the `ripclone-worker` process) claims it,
//! runs `process_build_job`, and acks. The server observes completion by polling
//! the job's status — proving sync can be farmed out to a process that shares
//! only storage + metadata + the queue.
//!
//! Uses the SQLite backend for reliable cross-process access; the libsql
//! (remote) binding shares the same orchestration and is covered by the unit
//! tests for the SQL logic.

mod common;

use common::*;
use ripclone::backends::{self, Backends};
use ripclone::metrics::Metrics;
use ripclone::mode::CloneMode;
use ripclone::queue::{BuildJob, JobQueueRef};
use ripclone::server::{ServerState, process_build_job};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn sql_queue_farm_out_sync_then_clone() {
    // Select the SQLite queue before the server starts.
    let qdir = tempfile::tempdir().expect("queue dir");
    let db_path = qdir.path().join("queue.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &db_path);
    }
    enable_async_build();
    init(false);

    // The server uses the SQL queue and spawns NO in-process worker.
    let server = start_server().await;

    // Stand up a worker that shares the server's storage + repo root + queue —
    // the same wiring `ripclone-worker` does, just in-process for the test.
    let queue = Arc::new(backends::connect_sql_queue().await.expect("worker queue"));
    let metrics = Metrics::new();
    let wb =
        Backends::from_env(&server.cas_dir, &server.repo_root, &metrics)
            .await
            .expect("worker backends");
    let state = ServerState::for_worker(wb, queue.clone() as JobQueueRef, metrics)
        .expect("worker state");
    let worker_queue = queue.clone();
    let worker = tokio::spawn(async move {
        loop {
            match worker_queue.claim("test-worker").await.expect("claim") {
                Some(c) => {
                    let job = BuildJob {
                        repo_id: c.repo_id(),
                        branch: c.branch,
                        rev: None,
                        credential: None,
                    };
                    let result = process_build_job(&state, &job).await;
                    worker_queue.ack(c.id, result).await.expect("ack");
                }
                None => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    });

    let origin = make_origin("acme", "sq");
    origin.commit(&[("a.txt", "1\n")], "c1");
    let commit1 = origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();

    // `/sync` enqueues into SQLite; the separate worker builds; `/sync` polls the
    // job status and returns the ref once it is done.
    let resp1 = server
        .client()
        .sync_repo("acme/sq", None)
        .await
        .expect("sql-queue sync");
    assert_eq!(resp1.commit, commit1, "first sync resolves the latest commit");

    // Clone — this also populates the SERVER's ref caches with commit1.
    let (g0, c) = clone_only(&server, "acme", "sq", 0, CloneMode::Editable)
        .await
        .expect("clone");
    assert_eq!(std::fs::read_to_string(c.join("a.txt")).unwrap(), "2\n");
    assert_eq!(git(&c, &["rev-list", "--count", "HEAD"]), "2");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));
    drop(g0);

    // Push a NEW commit, then sync again. The build runs in the worker process,
    // so the server must invalidate its own (now-stale) ref caches before
    // answering — otherwise it would return the cached commit1. This is the
    // cross-process stale-cache regression test.
    let commit2 = origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();
    let resp2 = server
        .client()
        .sync_repo("acme/sq", None)
        .await
        .expect("second sql-queue sync");
    assert_ne!(commit2, commit1);
    assert_eq!(
        resp2.commit, commit2,
        "second sync must return the fresh commit, not a stale cached one"
    );

    let (_g, c2) = clone_only(&server, "acme", "sq", 0, CloneMode::Editable)
        .await
        .expect("clone after second sync");
    assert_eq!(std::fs::read_to_string(c2.join("a.txt")).unwrap(), "3\n");
    assert_eq!(git(&c2, &["rev-list", "--count", "HEAD"]), "3");

    worker.abort();
}
