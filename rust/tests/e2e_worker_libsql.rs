//! Real end-to-end test of the **libsql remote backend** against a real local
//! `sqld` (libsql server) over the actual hrana/HTTP wire — no Turso Cloud creds
//! needed. The in-process API server enqueues over libsql; the real
//! `ripclone-worker` binary (separate process) claims/builds/acks over libsql.
//! This exercises the libsql param/row binding that sqlite/turso tests can't.
//!
//! Skips (passes as a no-op) if `sqld` is not installed, so CI without it stays
//! green; run locally with `sqld` on PATH for full coverage.

mod common;

use common::*;
use ripclone::mode::CloneMode;
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

    let data = tempfile::tempdir().expect("sqld data dir");
    let port = free_port();
    let _sqld = start_sqld(port, data.path());

    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "libsql");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", format!("http://127.0.0.1:{port}"));
        // sqld runs without auth here; the backend requires a non-empty token.
        std::env::set_var("RIPCLONE_QUEUE_DB_TOKEN", "dev");
        std::env::set_var("RIPCLONE_SYNC_MAX_ATTEMPTS", "10");
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
    enable_async_build();
    init(false);

    let server = start_server().await;
    let _worker = spawn_worker(&server.cas_dir, &server.repo_root);

    // Positive: a published repo is built by the worker (over libsql) and clones.
    let origin = make_origin("acme", "lq");
    origin.commit(&[("a.txt", "via-libsql\n")], "c1");
    origin.publish();

    let resp = server
        .client()
        .sync_repo("acme/lq", None)
        .await
        .expect("libsql farm-out sync should succeed against real sqld");
    assert!(!resp.commit.is_empty());

    let (_g, c) = clone_only(&server, "acme", "lq", 0, CloneMode::Editable)
        .await
        .expect("clone after libsql farm-out build");
    assert_eq!(
        std::fs::read_to_string(c.join("a.txt")).unwrap(),
        "via-libsql\n"
    );
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    // Negative: a missing upstream → the worker's build fails → /sync errors.
    let result = server.client().sync_repo("acme/missing-libsql", None).await;
    assert!(
        result.is_err(),
        "sync of a missing upstream over libsql must fail, got {result:?}"
    );
}
