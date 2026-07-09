//! End-to-end coverage for `ripclone clone --verify-upstream`.
//!
//! These tests exercise the real CLI binary against a real in-process server and
//! real `file://` git origins. No mocks.

use crate::common::*;

use std::path::Path;
use std::process::{Command, Output, Stdio};

/// Run the CLI `clone` subcommand against `server` with optional extra args.
/// The clone is written into `target`; `target.parent()` is used as `HOME` so
/// config/token-store lookups are isolated.
///
/// stdout/stderr are redirected to temp files rather than captured with
/// `Command::output`, because the CLI may invoke `git credential fill` helpers
/// that spawn long-lived grandchildren. Holding pipe FDs open would make the
/// capture hang even after the CLI itself exits.
///
/// The wait is run on the blocking pool so a `#[tokio::test]` single-threaded
/// runtime is not blocked; the in-process server needs the runtime to make
/// progress while the CLI runs.
async fn run_clone(server: &Server, repo: &str, target: &Path, args: &[&str]) -> Output {
    run_clone_with_token(server, repo, target, None, args).await
}

/// Like `run_clone`, but passes `--token <token>` before the subcommand so the
/// CLI resolves an upstream credential for verification.
async fn run_clone_with_token(
    server: &Server,
    repo: &str,
    target: &Path,
    token: Option<&str>,
    args: &[&str],
) -> Output {
    let server_url = server.url.clone();
    let repo = repo.to_string();
    let target = target.to_path_buf();
    let token = token.map(|s| s.to_string());
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let dir = target.parent().unwrap().to_path_buf();
        let stdout_tmp = tempfile::NamedTempFile::new_in(&dir).expect("stdout temp file");
        let stderr_tmp = tempfile::NamedTempFile::new_in(&dir).expect("stderr temp file");

        // Prefer runtime CARGO_BIN_EXE_* (set by CI when running prebuilt tests).
        let ripclone_bin = std::env::var_os("CARGO_BIN_EXE_ripclone")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_BIN_EXE_ripclone")));
        let mut cmd = Command::new(ripclone_bin);
        cmd.arg("--server").arg(&server_url);
        if let Some(token) = &token {
            cmd.arg("--token").arg(token);
        }
        cmd.arg("clone")
            .arg(&repo)
            .arg(&target)
            .env("RIPCLONE_SERVER_TOKEN_HASH", token_hash())
            .env("HOME", &dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(stdout_tmp.reopen().expect("reopen stdout temp"))
            .stderr(stderr_tmp.reopen().expect("reopen stderr temp"));
        for a in &args {
            cmd.arg(a);
        }
        let mut child = cmd.spawn().expect("spawn ripclone clone");
        let status = child.wait().expect("wait for ripclone clone");
        let stdout = std::fs::read(stdout_tmp.path()).expect("read stdout temp");
        let stderr = std::fs::read(stderr_tmp.path()).expect("read stderr temp");
        Output {
            status,
            stdout,
            stderr,
        }
    })
    .await
    .expect("spawn_blocking run_clone")
}

/// Wait until the server has finished building the artifacts the CLI will ask
/// for. `sync_repo` returns as soon as the sync job is enqueued/accepted, so a
/// directly following CLI clone can otherwise poll the 202 path for many
/// seconds.
async fn wait_for_warm(server: &Server, repo: &str, clonepack_kind: Option<&str>) {
    server
        .client()
        .resolve_ref_with_clonepack(repo, "HEAD", clonepack_kind, None)
        .await
        .expect("warm repo");
}

#[tokio::test]
async fn verify_upstream_default_succeeds_for_public() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-default");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-default")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-default", None)
        .await
        .expect("sync");
    wait_for_warm(&server, "acme/verify-default", Some("shallow")).await;

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(&server, "acme/verify-default", &target, &[]).await;

    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "hello\n"
    );
    // Editable clone: .git exists.
    assert!(target.join(".git").is_dir());
    // Default auto mode verified the public upstream; no skip warning was emitted.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("skipped"),
        "expected verification to run, got stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_succeeds_for_shallow_clone_with_history() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-shallow-history");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-shallow-history")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-shallow-history", None)
        .await
        .expect("sync");
    wait_for_warm(&server, "acme/verify-shallow-history", Some("shallow")).await;

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    // Default depth is 1; the installed repo is shallow. Verification must
    // still pass when the upstream tip matches.
    let out = run_clone(
        &server,
        "acme/verify-shallow-history",
        &target,
        &["--verify-upstream"],
    )
    .await;

    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "2\n"
    );
    assert!(target.join(".git").is_dir());
}

#[tokio::test]
async fn verify_upstream_auto_detects_stale_tip_on_public_repo() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-auto-stale");
    origin.commit(&[("a.txt", "a\n")], "a");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-auto-stale")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-auto-stale", None)
        .await
        .expect("sync");
    wait_for_warm(&server, "acme/verify-auto-stale", Some("shallow")).await;

    // Advance upstream without re-syncing the server.
    origin.commit(&[("a.txt", "b\n")], "b");
    origin.publish();

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(
        &server,
        "acme/verify-auto-stale",
        &target,
        &["--verify-upstream=auto"],
    )
    .await;

    assert!(
        !out.status.success(),
        "expected verification to fail, got stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("upstream verification failed"),
        "stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_always_detects_stale_tip() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-stale");
    origin.commit(&[("a.txt", "a\n")], "a");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-stale")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-stale", None)
        .await
        .expect("sync");
    wait_for_warm(&server, "acme/verify-stale", Some("shallow")).await;

    // Advance upstream without re-syncing the server. The server still serves
    // the older commit, but the upstream tip has moved.
    origin.commit(&[("a.txt", "b\n")], "b");
    origin.publish();

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(
        &server,
        "acme/verify-stale",
        &target,
        &["--verify-upstream"],
    )
    .await;

    assert!(
        !out.status.success(),
        "expected verification to fail, got stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("upstream verification failed"),
        "stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_always_fails_when_unreachable() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-unreachable");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-unreachable")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-unreachable", None)
        .await
        .expect("sync");
    wait_for_warm(&server, "acme/verify-unreachable", Some("shallow")).await;

    // Remove the origin so the upstream ls-remote fails. The server's cached
    // mirror is still fresh, so the clone itself would succeed without
    // verification.
    std::fs::remove_dir_all(&origin.bare).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(
        &server,
        "acme/verify-unreachable",
        &target,
        &["--verify-upstream"],
    )
    .await;

    assert!(
        !out.status.success(),
        "expected verification to fail, got stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("upstream verification failed"),
        "stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_never_silently_skips() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-never");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-never")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-never", None)
        .await
        .expect("sync");
    wait_for_warm(&server, "acme/verify-never", Some("shallow")).await;

    // Make upstream unreachable. With --verify-upstream=never the clone must
    // still succeed and emit no verification messages.
    std::fs::remove_dir_all(&origin.bare).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(
        &server,
        "acme/verify-never",
        &target,
        &["--verify-upstream=never"],
    )
    .await;

    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "hello\n"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("verify-upstream"),
        "expected silent skip, got stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_auto_warns_and_skips_unreachable() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-auto-unreachable");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-auto-unreachable")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-auto-unreachable", None)
        .await
        .expect("sync");
    wait_for_warm(&server, "acme/verify-auto-unreachable", Some("shallow")).await;

    // Remove the origin so the anonymous upstream probe fails. Default auto mode
    // (no credential) must degrade with a warning rather than failing the clone.
    std::fs::remove_dir_all(&origin.bare).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(&server, "acme/verify-auto-unreachable", &target, &[]).await;

    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "hello\n"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("warning: --verify-upstream skipped"),
        "expected degrade warning, got stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_auto_with_token_warns_and_skips_unreachable() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-auto-token-unreachable");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-auto-token-unreachable")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-auto-token-unreachable", None)
        .await
        .expect("sync");
    wait_for_warm(
        &server,
        "acme/verify-auto-token-unreachable",
        Some("shallow"),
    )
    .await;

    // Remove the origin. Even with a credential, auto mode must warn and skip an
    // unreachable upstream rather than hard-failing the clone.
    std::fs::remove_dir_all(&origin.bare).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone_with_token(
        &server,
        "acme/verify-auto-token-unreachable",
        &target,
        Some("fake-token"),
        &["--verify-upstream=auto"],
    )
    .await;

    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "hello\n"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("warning: --verify-upstream skipped"),
        "expected degrade warning, got stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_files_mode_warns_and_skips() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-files");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-files")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/verify-files", None)
        .await
        .expect("sync");
    wait_for_warm(&server, "acme/verify-files", Some("full")).await;

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(
        &server,
        "acme/verify-files",
        &target,
        &["--mode", "files", "--verify-upstream"],
    )
    .await;

    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "hello\n"
    );
    // Files mode: no .git object database, so verification is skipped.
    assert!(!target.join(".git").exists());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not supported for files-mode clones"),
        "stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_auto_skips_non_tip_rev() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-at-auto");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-at-auto")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo_at("acme/verify-at-auto", Some("HEAD~1"), None)
        .await
        .expect("sync at HEAD~1");
    server
        .client()
        .resolve_ref_with_clonepack("acme/verify-at-auto", "HEAD~1", Some("full"), None)
        .await
        .expect("warm full at HEAD~1");

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(
        &server,
        "acme/verify-at-auto",
        &target,
        &["--at", "HEAD~1", "--verify-upstream=auto"],
    )
    .await;

    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "1\n"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("skipped for --at HEAD~1"),
        "stderr: {stderr}"
    );
}

#[tokio::test]
async fn verify_upstream_always_fails_non_tip_rev() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-at-always");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();
    server
        .client()
        .add_repo("acme/verify-at-always")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo_at("acme/verify-at-always", Some("HEAD~1"), None)
        .await
        .expect("sync at HEAD~1");
    server
        .client()
        .resolve_ref_with_clonepack("acme/verify-at-always", "HEAD~1", Some("full"), None)
        .await
        .expect("warm full at HEAD~1");

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(
        &server,
        "acme/verify-at-always",
        &target,
        &["--at", "HEAD~1", "--verify-upstream"],
    )
    .await;

    assert!(
        !out.status.success(),
        "expected verification to fail, got stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("cannot verify a non-tip rev"),
        "stderr: {stderr}"
    );
}
