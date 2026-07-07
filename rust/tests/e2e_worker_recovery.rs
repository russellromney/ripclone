//! Real worker-crash recovery e2e.
//!
//! This uses the production sqlite queue plus real `ripclone-worker` binaries.
//! The test fails if a claimed build killed mid-flight is never reclaimed, if
//! the replacement worker publishes a partial/corrupt tree, or if `/sync`
//! reports success before durable bytes are actually cloneable.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use sqlx::{Row, sqlite::SqlitePoolOptions};
use std::path::Path;
use std::time::Duration;

async fn wait_for_job_status(db_path: &Path, status: &str) -> (i64, i64) {
    let url = format!("sqlite://{}?mode=rw", db_path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect queue db");
    let mut last = String::from("<no rows>");
    for _ in 0..200 {
        if let Some(row) = sqlx::query("SELECT id, status, attempts FROM jobs ORDER BY id LIMIT 1")
            .fetch_optional(&pool)
            .await
            .expect("read jobs")
        {
            let id: i64 = row.get("id");
            let got: String = row.get("status");
            let attempts: i64 = row.get("attempts");
            if got == status {
                pool.close().await;
                return (id, attempts);
            }
            last = format!("id={id} status={got} attempts={attempts}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("queue job never reached {status:?} (last: {last})");
}

#[tokio::test]
async fn killed_worker_claim_is_reclaimed_and_rebuilds_cleanly() {
    let qdir = tempfile::tempdir().expect("queue dir");
    let db_path = qdir.path().join("queue.db");
    let db_url = db_path.to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &db_url);
        std::env::set_var("RIPCLONE_QUEUE_STALE_SECS", "1");
        std::env::set_var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "120");
        // Keep worker #1 inside the full-history phase long enough to kill it
        // after it owns the queue claim. Worker #2 is spawned after this env is
        // removed, so recovery is fast.
        std::env::set_var("RIPCLONE_TEST_ARCHIVE_DELAY_MS", "10000");
    }
    init(false);

    let server = start_server().await;
    let mut worker = spawn_worker(&server.cas_dir, &server.repo_root);

    let origin = make_origin("acme", "worker-crash");
    origin.commit(&[("a.txt", "one\n")], "c1");
    origin.commit(&[("a.txt", "two\n"), ("nested/b.txt", "bee\n")], "c2");
    origin.publish();

    register_added_without_build(&server, "acme/worker-crash")
        .await
        .expect("add repo");
    let client = server.client();
    let sync_task = tokio::spawn(async move { client.sync_repo("acme/worker-crash", None).await });

    let (job_id, attempts) = wait_for_job_status(&db_path, "claimed").await;
    assert_eq!(attempts, 1, "first worker owns the first claim");
    worker.kill_now();
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_ARCHIVE_DELAY_MS");
    }

    let _replacement = spawn_worker(&server.cas_dir, &server.repo_root);
    let sync = sync_task
        .await
        .expect("sync task join")
        .expect("sync recovers after worker crash");
    assert_eq!(
        sync.commit,
        git(&origin.work, &["rev-parse", "HEAD"]),
        "recovered sync must publish the upstream tip"
    );

    let (same_job, recovered_attempts) = wait_for_job_status(&db_path, "done").await;
    assert_eq!(same_job, job_id, "the original claimed job was reclaimed");
    assert!(
        recovered_attempts >= 2,
        "replacement worker should reclaim and retry the killed claim, attempts={recovered_attempts}"
    );

    let info = wait_repo_cloneable(&server, "acme/worker-crash", Some("full")).await;
    assert_eq!(info.commit, sync.commit);

    let (_g, clone) = clone_only(&server, "acme", "worker-crash", 0, CloneMode::Editable)
        .await
        .expect("clone after recovered worker build");
    assert_eq!(read(&clone, "a.txt"), "two\n");
    assert_eq!(read(&clone, "nested/b.txt"), "bee\n");
    assert_repo_usable(&clone, "2");
}
