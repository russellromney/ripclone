//! Real two-process farm-out e2e on the **MySQL** queue backend, with the real
//! `ripclone-worker` binary as a separate process. Runs only when
//! `RIPCLONE_TEST_MYSQL_URL` points at a reachable MySQL (or any MySQL-wire
//! server) — see scripts/test-queue-sql.sh; skips otherwise.
//!
//! Uses a unique repo name per run so it never collides with rows left in the
//! shared `jobs` table by previous runs.

mod common;

use common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn worker_farm_out_mysql() {
    let Ok(url) = std::env::var("RIPCLONE_TEST_MYSQL_URL") else {
        eprintln!("SKIP worker_farm_out_mysql: RIPCLONE_TEST_MYSQL_URL unset");
        return;
    };
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "mysql");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &url);
        std::env::set_var("RIPCLONE_SYNC_MAX_ATTEMPTS", "8");
    }
    init(false);

    let server = start_server().await;
    let _worker = spawn_worker(&server.cas_dir, &server.repo_root);

    let pid = std::process::id();
    let good = format!("my{pid}");
    let missing = format!("mymissing{pid}");

    let origin = make_origin("acme", &good);
    origin.commit(&[("a.txt", "via-mysql\n")], "c1");
    origin.publish();
    let resp = server
        .client()
        .sync_repo(&format!("acme/{good}"), None)
        .await
        .expect("mysql farm-out sync should succeed");
    assert!(!resp.commit.is_empty());

    let (_g, c) = clone_only(&server, "acme", &good, 0, CloneMode::Editable)
        .await
        .expect("clone after mysql farm-out build");
    assert_eq!(
        std::fs::read_to_string(c.join("a.txt")).unwrap(),
        "via-mysql\n"
    );
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    let result = server
        .client()
        .sync_repo(&format!("acme/{missing}"), None)
        .await;
    assert!(
        result.is_err(),
        "sync of a missing upstream over mysql must fail, got {result:?}"
    );
}
