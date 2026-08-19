//! Real end-to-end test of the **libsql remote backend** against a real local
//! `sqld` (libsql server) over the actual hrana/HTTP wire — no Turso Cloud creds
//! needed. The in-process API server enqueues over libsql; the real
//! `ripclone-worker` binary (separate process) claims/builds/acks over libsql.
//! This exercises the libsql param/row binding that sqlite/turso tests can't.
//!
//! Skips (passes as a no-op) if `sqld` is not installed, so CI without it stays
//! green; run locally with `sqld` on PATH for full coverage.

use crate::common::*;
use ripclone::backends;
use ripclone::mode::CloneMode;
use ripclone::queue::BuildError;
use secrecy::ExposeSecret;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct Proc(Child);
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn sqld_available() -> bool {
    Command::new("sqld")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Start a local `sqld` and wait until it accepts connections.
fn start_sqld(port: u16, data: &Path) -> Proc {
    let child = Command::new("sqld")
        .arg("--http-listen-addr")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--db-path")
        .arg(data)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sqld");
    let mut ready = false;
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(ready, "sqld did not become ready on port {port}");
    Proc(child)
}

// The worker is spawned via `common::spawn_worker`; `Proc`/`start_sqld` above
// manage the local sqld server this test needs.

#[tokio::test]
async fn worker_farm_out_libsql_against_real_sqld() {
    if !sqld_available() {
        eprintln!("SKIP: sqld not installed; install it to run the libsql e2e");
        return;
    }

    crate::reset_database_backend_env();

    let data = tempfile::tempdir().expect("sqld data dir");
    let port = free_port();
    let _sqld = start_sqld(port, data.path());

    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "libsql");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", format!("http://127.0.0.1:{port}"));
        // sqld runs without auth here; the backend requires a non-empty token.
        std::env::set_var("RIPCLONE_QUEUE_DB_TOKEN", "dev");
        std::env::set_var("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "10");
        std::env::set_var("RIPCLONE_QUEUE_RETRY_BACKOFF_MS", "0");
        // Also drive the METADATA store over the same libsql server — this is the
        // only runtime coverage of the libsql metadata adapter (it's otherwise
        // remote-only and compile-checked), and exercises queue + metadata on
        // libsql together.
        std::env::set_var("RIPCLONE_METADATA", "libsql");
        std::env::set_var(
            "RIPCLONE_METADATA_DB_URL",
            format!("http://127.0.0.1:{port}"),
        );
        std::env::set_var("RIPCLONE_METADATA_DB_TOKEN", "dev");
    }
    init(false);

    let server = start_server().await;

    // Admit B before a worker exists, then claim it through the real libsql
    // adapter. The duplicate must coalesce against the claimed exact row.
    let origin = make_origin("acme", "lq");
    let commit_b = origin.commit(&[("a.txt", "via-libsql-b\n")], "b");
    origin.publish();
    register_added_without_build(&server, "acme/lq")
        .await
        .expect("register libsql farm-out repo");
    let admitted_b = server
        .client()
        .with_upstream_token("first-libsql-credential")
        .admit_sync_repo("acme/lq", None)
        .await
        .expect("admit libsql B");
    assert!(admitted_b.accepted);
    assert_eq!(admitted_b.commit, commit_b);

    let probe_queue = backends::connect_sql_queue()
        .await
        .expect("connect libsql lifecycle probe");
    let claimed_b = claim_exact_sql_job(&probe_queue, "libsql-lifecycle-probe", &commit_b).await;
    assert_eq!(claimed_b.admitted_commit, commit_b);
    assert_eq!(
        claimed_b
            .credential
            .as_ref()
            .map(|credential| credential.expose_secret()),
        Some("first-libsql-credential")
    );

    let duplicate_b = server
        .client()
        .with_upstream_token("claimed-duplicate-decoy")
        .admit_sync_repo("acme/lq", None)
        .await
        .expect("duplicate claimed B admission");
    assert_eq!(duplicate_b.commit, commit_b);
    assert_eq!(duplicate_b.status, "coalesced");
    eprintln!("libsql active_rows_B=1 claimed_rows=1 queued_rows=0 credential_owner=first");

    let commit_c = origin.commit(&[("a.txt", "via-libsql-c\n")], "c");
    origin.publish();
    let admitted_c = server
        .client()
        .admit_sync_repo("acme/lq", None)
        .await
        .expect("admit distinct libsql C");
    assert!(admitted_c.accepted);
    assert_eq!(admitted_c.commit, commit_c);
    assert_eq!(admitted_c.status, "queued");

    assert!(
        probe_queue
            .ack(
                claimed_b.id,
                "libsql-lifecycle-probe",
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
        .sync_repo("acme/lq", None)
        .await
        .expect("libsql farm-out sync should succeed against real sqld");
    assert_eq!(resp.commit, commit_c);

    let (_g, c) = clone_only(&server, "acme", "lq", 0, CloneMode::Editable)
        .await
        .expect("clone after libsql farm-out build");
    assert_eq!(
        std::fs::read_to_string(c.join("a.txt")).unwrap(),
        "via-libsql-c\n"
    );
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    // Negative: a missing upstream → the worker's build fails → /add errors.
    let result = server.client().add_repo("acme/missing-libsql").await;
    assert!(
        result.is_err(),
        "add of a missing upstream over libsql must fail, got {result:?}"
    );
}
