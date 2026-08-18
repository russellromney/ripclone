//! Real two-process farm-out e2e on the **MySQL** queue backend, with the real
//! `ripclone-worker` binary as a separate process. Runs only when
//! `RIPCLONE_TEST_MYSQL_URL` points at a reachable MySQL (or any MySQL-wire
//! server) — see scripts/test-queue-sql.sh; skips otherwise.
//!
//! Uses a unique repo name per run so it never collides with rows left in the
//! shared `jobs` table by previous runs.

use crate::common::*;
use ripclone::backends;
use ripclone::mode::CloneMode;
use ripclone::queue::BuildError;

#[tokio::test]
async fn worker_farm_out_mysql() {
    let Ok(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL") else {
        eprintln!("SKIP worker_farm_out_mysql: RIPCLONE_TEST_MYSQL_URL unset");
        return;
    };
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "mysql");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &url);
        std::env::set_var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "8");
        std::env::set_var("RIPCLONE_QUEUE_RETRY_BACKOFF_MS", "0");
    }
    init(false);

    let server = start_server().await;

    let pid = std::process::id();
    let good = format!("my{pid}");
    let missing = format!("mymissing{pid}");

    // Admit B before a worker exists, then claim it through the real MySQL
    // adapter. The duplicate must coalesce against the claimed exact row.
    let origin = make_origin("acme", &good);
    let commit_b = origin.commit(&[("a.txt", "via-mysql-b\n")], "b");
    origin.publish();
    register_added_without_build(&server, &format!("acme/{good}"))
        .await
        .expect("register mysql farm-out repo");
    let admitted_b = server
        .client()
        .admit_sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("admit mysql B");
    assert!(admitted_b.accepted);
    assert_eq!(admitted_b.commit, commit_b);

    let probe_queue = backends::connect_sql_queue()
        .await
        .expect("connect MySQL lifecycle probe");
    let claimed_b = claim_exact_sql_job(&probe_queue, "mysql-lifecycle-probe", &commit_b).await;
    assert_eq!(claimed_b.admitted_commit, commit_b);

    let duplicate_b = server
        .client()
        .admit_sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("duplicate claimed B admission");
    assert_eq!(duplicate_b.commit, commit_b);
    assert_eq!(duplicate_b.status, "coalesced");

    let commit_c = origin.commit(&[("a.txt", "via-mysql-c\n")], "c");
    origin.publish();
    let admitted_c = server
        .client()
        .admit_sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("admit distinct MySQL C");
    assert!(admitted_c.accepted);
    assert_eq!(admitted_c.commit, commit_c);
    assert_eq!(admitted_c.status, "queued");

    assert!(
        probe_queue
            .ack(
                claimed_b.id,
                "mysql-lifecycle-probe",
                Err(BuildError::retryable("release B for real worker")),
            )
            .await
            .expect("requeue B")
    );

    let _worker = spawn_worker(&server.cas_dir, &server.repo_root);
    let resp = server
        .client()
        .sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("mysql farm-out sync should succeed");
    assert_eq!(resp.commit, commit_c);

    let (_g, c) = clone_only(&server, "acme", &good, 0, CloneMode::Editable)
        .await
        .expect("clone after mysql farm-out build");
    assert_eq!(
        std::fs::read_to_string(c.join("a.txt")).unwrap(),
        "via-mysql-c\n"
    );
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    let result = server.client().add_repo(&format!("acme/{missing}")).await;
    assert!(
        result.is_err(),
        "add of a missing upstream over mysql must fail, got {result:?}"
    );
}
