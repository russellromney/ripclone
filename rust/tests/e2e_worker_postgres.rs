//! Real two-process farm-out e2e on the **Postgres** queue backend, with the
//! real `ripclone-worker` binary as a separate process. Runs only when
//! `RIPCLONE_TEST_PG_URL` points at a reachable Postgres (or any Postgres-wire
//! server) — see scripts/test-queue-sql.sh; skips otherwise.
//!
//! Uses a unique repo name per run so it never collides with rows left in the
//! shared `jobs` table by previous runs (integration tests can't use sqlx to
//! drop the table; the unit tests in src/queue/sql.rs do that).

mod common;

use common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn worker_farm_out_postgres() {
    let Ok(url) = std::env::var("RIPCLONE_TEST_PG_URL") else {
        eprintln!("SKIP worker_farm_out_postgres: RIPCLONE_TEST_PG_URL unset");
        return;
    };
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "postgres");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &url);
        std::env::set_var("RIPCLONE_SYNC_MAX_ATTEMPTS", "8");
    }
    enable_async_build();
    init(false);

    let server = start_server().await;
    let _worker = spawn_worker(&server.cas_dir, &server.repo_root);

    // Unique repo names so we don't coalesce onto a leftover job from a prior run.
    let pid = std::process::id();
    let good = format!("pg{pid}");
    let missing = format!("pgmissing{pid}");

    // Positive: published repo is built by the worker (over postgres) and clones.
    let origin = make_origin("acme", &good);
    origin.commit(&[("a.txt", "via-postgres\n")], "c1");
    origin.publish();
    let resp = server
        .client()
        .sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("postgres farm-out sync should succeed");
    assert!(!resp.commit.is_empty());

    let (_g, c) = clone_only(&server, "acme", &good, 0, CloneMode::Editable)
        .await
        .expect("clone after postgres farm-out build");
    assert_eq!(std::fs::read_to_string(c.join("a.txt")).unwrap(), "via-postgres\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    // Negative: missing upstream → build fails → /sync errors.
    let result = server.client().sync_repo(&format!("acme/{missing}"), None).await;
    assert!(
        result.is_err(),
        "sync of a missing upstream over postgres must fail, got {result:?}"
    );
}
