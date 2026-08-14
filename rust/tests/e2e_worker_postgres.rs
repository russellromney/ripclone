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
use ripclone::backends;
use ripclone::mode::CloneMode;
use ripclone::queue::BuildError;

#[tokio::test]
async fn worker_farm_out_postgres() {
    let Ok(url) = std::env::var("RIPCLONE_TEST_PG_URL") else {
        eprintln!("SKIP worker_farm_out_postgres: RIPCLONE_TEST_PG_URL unset");
        return;
    };
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "postgres");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &url);
        std::env::set_var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "8");
        std::env::set_var("RIPCLONE_QUEUE_RETRY_BACKOFF_MS", "0");
    }
    init(false);

    let server = start_server().await;

    // Unique repo names so we don't coalesce onto a leftover job from a prior run.
    let pid = std::process::id();
    let good = format!("pg{pid}");
    let missing = format!("pgmissing{pid}");

    // Admit B before a worker exists, then claim it through the real Postgres
    // adapter. The duplicate must coalesce against the claimed exact row.
    let origin = make_origin("acme", &good);
    let commit_b = origin.commit(&[("a.txt", "via-postgres-b\n")], "b");
    origin.publish();
    register_added_without_build(&server, &format!("acme/{good}"))
        .await
        .expect("register postgres farm-out repo");
    let admitted_b = server
        .client()
        .admit_sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("admit postgres B");
    assert!(admitted_b.accepted);
    assert_eq!(admitted_b.commit, commit_b);

    let probe_queue = backends::connect_sql_queue()
        .await
        .expect("connect Postgres lifecycle probe");
    let claimed_b = claim_exact_sql_job(&probe_queue, "postgres-lifecycle-probe", &commit_b).await;
    assert_eq!(
        claimed_b.admitted_commit.as_deref(),
        Some(commit_b.as_str())
    );

    let duplicate_b = server
        .client()
        .admit_sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("duplicate claimed B admission");
    assert_eq!(duplicate_b.commit, commit_b);
    assert_eq!(duplicate_b.status, "coalesced");

    let commit_c = origin.commit(&[("a.txt", "via-postgres-c\n")], "c");
    origin.publish();
    let admitted_c = server
        .client()
        .admit_sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("admit distinct Postgres C");
    assert!(admitted_c.accepted);
    assert_eq!(admitted_c.commit, commit_c);
    assert_eq!(admitted_c.status, "queued");

    assert!(
        probe_queue
            .ack(
                claimed_b.id,
                "postgres-lifecycle-probe",
                Err(BuildError::retryable("release B for real worker")),
            )
            .await
            .expect("requeue B")
    );

    // The real worker builds the requeued exact B and distinct C; the ordinary
    // branch settles at C.
    let _worker = spawn_worker(&server.cas_dir, &server.repo_root);
    let resp = server
        .client()
        .sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("postgres farm-out sync should succeed");
    assert_eq!(resp.commit, commit_c);

    let (_g, c) = clone_only(&server, "acme", &good, 0, CloneMode::Editable)
        .await
        .expect("clone after postgres farm-out build");
    assert_eq!(
        std::fs::read_to_string(c.join("a.txt")).unwrap(),
        "via-postgres-c\n"
    );
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    // Negative: missing upstream → build fails → /sync errors.
    let result = server.client().add_repo(&format!("acme/{missing}")).await;
    assert!(
        result.is_err(),
        "add of a missing upstream over postgres must fail, got {result:?}"
    );
}
