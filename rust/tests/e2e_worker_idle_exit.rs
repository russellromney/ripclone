//! Worker lifecycle: `--idle-exit-secs` (scale-to-zero drain) and `--max-jobs`
//! (one-shot). Real `ripclone-worker` binary against the sqlite queue.
//!
//! Specs: docs/internal/DISPATCHER.md — exit only on empty claim after N seconds;
//! a job in the exit window is picked up on the next worker start; max-jobs exits
//! after N builds. Crash safety is unchanged (reclaim_stale).
//!
//! These tests set process-global `RIPCLONE_QUEUE_DB_URL` and spawn a worker that
//! inherits it, so they serialize on `SERIAL` (same pattern as e2e_equivalence).

mod common;

use common::*;
use ripclone::backends;
use ripclone::mode::CloneMode;
use ripclone::provider::RepoId;
use ripclone::queue::{BuildJob, JobQueue, JobState};
use std::time::Duration;

/// Queue URL + worker env is process-global; cargo runs tests in parallel.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn setup_sqlite_queue() -> tempfile::TempDir {
    let qdir = tempfile::tempdir().expect("queue dir");
    let db_path = qdir.path().join("queue.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &db_path);
    }
    init(false);
    qdir
}

async fn enqueue(path: &str) -> (ripclone::queue::SqlJobQueue, i64) {
    let queue = backends::connect_sql_queue().await.expect("queue");
    let enq = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github(path),
            branch: "main".into(),
            rev: None,
            credential: None,
            recheck: 0,
        })
        .await
        .expect("enqueue");
    (queue, enq.job_id.expect("job id"))
}

async fn wait_done(queue: &ripclone::queue::SqlJobQueue, id: i64, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match queue.job_status(id).await.expect("status") {
            JobState::Done => return,
            JobState::Failed(e) => panic!("job {id} failed: {e}"),
            JobState::Pending | JobState::Unknown => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "job {id} did not finish within {timeout:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Drain a job, then idle-exit. Proves the worker leaves after the queue is empty.
#[tokio::test]
async fn idle_exit_drains_then_exits() {
    let _guard = SERIAL.lock().await;
    let _qdir = setup_sqlite_queue();
    let server = start_server().await;

    let origin = make_origin("acme", "idle-drain");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.publish();

    // Enqueue first so the worker has work the moment it starts — avoids racing
    // idle-exit against origin setup.
    let (queue, job_id) = enqueue("acme/idle-drain").await;

    let mut worker = spawn_worker_args(
        &server.cas_dir,
        &server.repo_root,
        &["--idle-exit-secs", "1"],
    );

    wait_done(&queue, job_id, Duration::from_secs(90)).await;
    assert!(
        worker.wait_exit(Duration::from_secs(15)),
        "worker must idle-exit after the queue drains"
    );
    assert_eq!(queue.depth().await, 0, "queue should be empty after drain");
}

/// A job that arrives after idle-exit (the reconcile / next-start path) is built
/// by a fresh worker. Models the cloud reconcile cron covering the exit window.
#[tokio::test]
async fn job_after_idle_exit_picked_up_on_next_start() {
    let _guard = SERIAL.lock().await;
    let _qdir = setup_sqlite_queue();
    let server = start_server().await;

    // Empty queue → first worker idle-exits with nothing to do.
    let mut w1 = spawn_worker_args(
        &server.cas_dir,
        &server.repo_root,
        &["--idle-exit-secs", "1"],
    );
    assert!(
        w1.wait_exit(Duration::from_secs(15)),
        "empty-queue worker must idle-exit"
    );
    drop(w1);

    let origin = make_origin("acme", "idle-next");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();

    register_added_without_build(&server, "acme/idle-next")
        .await
        .expect("add repo");

    // Job lands with no live worker (exit window / lost dispatch). Next start
    // drains it.
    let _w2 = spawn_worker(&server.cas_dir, &server.repo_root);
    let resp = server
        .client()
        .sync_repo("acme/idle-next", None)
        .await
        .expect("next worker start must pick up the job");
    assert!(!resp.commit.is_empty());

    let (_g, c) = clone_only(&server, "acme", "idle-next", 0, CloneMode::Editable)
        .await
        .expect("clone after next-start build");
    assert_eq!(std::fs::read_to_string(c.join("a.txt")).unwrap(), "hello\n");
}

/// `--max-jobs 1` runs exactly one build then exits; a second queued job waits
/// for another worker (one-shot platforms).
#[tokio::test]
async fn max_jobs_one_then_exits() {
    let _guard = SERIAL.lock().await;
    let _qdir = setup_sqlite_queue();
    let server = start_server().await;

    let o1 = make_origin("acme", "one-shot-a");
    o1.commit(&[("a.txt", "a\n")], "c1");
    o1.publish();
    let o2 = make_origin("acme", "one-shot-b");
    o2.commit(&[("b.txt", "b\n")], "c1");
    o2.publish();

    // Enqueue both before the one-shot worker starts so both are waiting.
    let queue = backends::connect_sql_queue().await.expect("queue");
    let enq_a = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github("acme/one-shot-a"),
            branch: "main".into(),
            rev: None,
            credential: None,
            recheck: 0,
        })
        .await
        .expect("enqueue a");
    let enq_b = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github("acme/one-shot-b"),
            branch: "main".into(),
            rev: None,
            credential: None,
            recheck: 0,
        })
        .await
        .expect("enqueue b");
    let id_a = enq_a.job_id.expect("job a id");
    let id_b = enq_b.job_id.expect("job b id");
    assert_eq!(queue.depth().await, 2);

    let mut worker = spawn_worker_args(&server.cas_dir, &server.repo_root, &["--max-jobs", "1"]);
    assert!(
        worker.wait_exit(Duration::from_secs(90)),
        "max-jobs 1 worker must exit after one build"
    );

    // Exactly one of the two jobs finished; the other is still pending.
    let mut done = 0u8;
    let mut pending = 0u8;
    for id in [id_a, id_b] {
        match queue.job_status(id).await.expect("status") {
            JobState::Done => done += 1,
            JobState::Pending => pending += 1,
            other => panic!("unexpected job state: {other:?}"),
        }
    }
    assert_eq!(done, 1, "exactly one job should be done");
    assert_eq!(pending, 1, "the other job must still be pending");
    assert_eq!(queue.depth().await, 1, "one queued job remains");

    // Next start drains the leftover.
    let _w2 = spawn_worker(&server.cas_dir, &server.repo_root);
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let sa = queue.job_status(id_a).await.unwrap();
        let sb = queue.job_status(id_b).await.unwrap();
        if matches!(sa, JobState::Done) && matches!(sb, JobState::Done) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "second worker should finish the remaining job (a={sa:?}, b={sb:?})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
