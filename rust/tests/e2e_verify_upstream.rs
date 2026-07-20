//! End-to-end coverage for `ripclone clone --verify-upstream`.
//!
//! These tests exercise the real CLI binary against a real in-process server and
//! real `file://` git origins. No mocks.

mod common;

use common::*;

use std::path::Path;
use std::process::{Child, Command, Output, Stdio};

struct ProbeBarrier {
    _dir: tempfile::TempDir,
    wrapper_dir: std::path::PathBuf,
    entered: std::path::PathBuf,
    proceed: std::path::PathBuf,
    count: std::path::PathBuf,
    snapshot: std::path::PathBuf,
    real_git: String,
}

impl ProbeBarrier {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("probe barrier directory");
        let wrapper_dir = dir.path().join("bin");
        std::fs::create_dir_all(&wrapper_dir).unwrap();
        let wrapper = wrapper_dir.join("git");
        std::fs::write(
            &wrapper,
            r#"#!/bin/sh
is_probe=0
for arg in "$@"; do
  if [ "$arg" = "ls-remote" ]; then is_probe=1; fi
done
if [ "$is_probe" = "1" ]; then
  "$RIPCLONE_TEST_REAL_GIT" "$@" >"$RIPCLONE_TEST_PROBE_SNAPSHOT"
  status=$?
  printf '1\n' >>"$RIPCLONE_TEST_PROBE_COUNT"
  : >"$RIPCLONE_TEST_PROBE_ENTERED"
  waited=0
  while [ ! -f "$RIPCLONE_TEST_PROBE_PROCEED" ]; do
    if [ "$waited" -ge 400 ]; then exit 124; fi
    sleep 0.05
    waited=$((waited + 1))
  done
  cat "$RIPCLONE_TEST_PROBE_SNAPSHOT"
  exit "$status"
fi
exec "$RIPCLONE_TEST_REAL_GIT" "$@"
"#,
        )
        .expect("write git probe wrapper");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755))
                .expect("make git probe wrapper executable");
        }
        let real_git = String::from_utf8(
            Command::new("sh")
                .args(["-c", "command -v git"])
                .output()
                .expect("locate real git")
                .stdout,
        )
        .expect("real git path is utf8")
        .trim()
        .to_string();
        Self {
            wrapper_dir,
            entered: dir.path().join("entered"),
            proceed: dir.path().join("proceed"),
            count: dir.path().join("count"),
            snapshot: dir.path().join("snapshot"),
            real_git,
            _dir: dir,
        }
    }

    async fn wait_entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while !self.entered.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("CLI upstream probe reached deterministic barrier");
    }

    fn release(&self) {
        std::fs::write(&self.proceed, b"go").expect("release upstream probe");
    }

    fn assert_one_probe(&self) {
        let count = std::fs::read_to_string(&self.count).expect("probe count");
        assert_eq!(count.lines().count(), 1, "exactly one upstream probe");
    }
}

fn spawn_clone_at_probe_barrier(
    server: &Server,
    repo: &str,
    target: &Path,
    barrier: &ProbeBarrier,
) -> Child {
    let binary = cargo_bin("ripclone");
    if let Some(dir) = std::env::var_os("RIPCLONE_BIN_DIR") {
        assert_eq!(
            binary.canonicalize().expect("canonical selected CLI"),
            std::path::PathBuf::from(dir)
                .join("ripclone")
                .canonicalize()
                .expect("canonical requested release CLI")
        );
    }
    let path = format!(
        "{}:{}",
        barrier.wrapper_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    Command::new(binary)
        .arg("--server")
        .arg(&server.url)
        .arg("clone")
        .arg(repo)
        .arg(target)
        .args([
            "--branch",
            "main",
            "--depth",
            "0",
            "--verify-upstream=always",
            "--no-metrics",
        ])
        .env("PATH", path)
        .env("RIPCLONE_TEST_REAL_GIT", &barrier.real_git)
        .env("RIPCLONE_TEST_PROBE_ENTERED", &barrier.entered)
        .env("RIPCLONE_TEST_PROBE_PROCEED", &barrier.proceed)
        .env("RIPCLONE_TEST_PROBE_COUNT", &barrier.count)
        .env("RIPCLONE_TEST_PROBE_SNAPSHOT", &barrier.snapshot)
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_NO_METRICS", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn release CLI at probe barrier")
}

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
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll ripclone clone") {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().expect("kill timed-out ripclone clone");
                let _ = child.wait().expect("reap timed-out ripclone clone");
                panic!("ripclone clone timed out after 60s; killed and reaped");
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        };
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
async fn upstream_snapshot_a_survives_branch_movement_and_installs_a() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-snapshot-a");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/verify-snapshot-a")
        .await
        .expect("register verification fixture");
    server
        .client()
        .sync_repo("acme/verify-snapshot-a", None)
        .await
        .expect("sync A");
    wait_for_warm(&server, "acme/verify-snapshot-a", Some("full")).await;

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let barrier = ProbeBarrier::new();
    let child = spawn_clone_at_probe_barrier(&server, "acme/verify-snapshot-a", &target, &barrier);
    barrier.wait_entered().await;
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    assert_ne!(a, b);
    barrier.release();
    let output = wait_child_output_bounded(child, std::time::Duration::from_secs(60))
        .await
        .expect("positive verification clone bounded and reaped");
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    barrier.assert_one_probe();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), a);
    assert_eq!(read(&target, "value.txt"), "A\n");
}

#[tokio::test]
async fn upstream_snapshot_a_rejects_later_pinned_install_b_without_repinning() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "verify-snapshot-mismatch");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/verify-snapshot-mismatch")
        .await
        .expect("register verification mismatch fixture");
    server
        .client()
        .sync_repo("acme/verify-snapshot-mismatch", None)
        .await
        .expect("sync A");
    wait_for_warm(&server, "acme/verify-snapshot-mismatch", Some("full")).await;

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let barrier = ProbeBarrier::new();
    let child =
        spawn_clone_at_probe_barrier(&server, "acme/verify-snapshot-mismatch", &target, &barrier);
    barrier.wait_entered().await;
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    server
        .client()
        .sync_repo("acme/verify-snapshot-mismatch", None)
        .await
        .expect("publish B before release CLI resolves its ref");
    wait_for_warm(&server, "acme/verify-snapshot-mismatch", Some("full")).await;
    let ready_b = server
        .client()
        .resolve_ref_with_clonepack("acme/verify-snapshot-mismatch", "main", Some("full"), None)
        .await
        .expect("full B ready");
    assert_eq!(ready_b.commit, b);
    barrier.release();
    let output = wait_child_output_bounded(child, std::time::Duration::from_secs(60))
        .await
        .expect("negative verification clone bounded and reaped");
    assert!(!output.status.success(), "verification mismatch must fail");
    barrier.assert_one_probe();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("upstream verification failed"), "{stderr}");
    assert!(stderr.contains(&a), "snapshot A missing: {stderr}");
    assert!(stderr.contains(&b), "installed B missing: {stderr}");
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(read(&target, "value.txt"), "B\n");
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
    server
        .client()
        .resolve_ref_with_clonepack("acme/verify-auto-stale", "main", Some("full"), None)
        .await
        .expect("cache A on the concrete branch");

    // Advance upstream without re-syncing the server.
    origin.commit(&[("a.txt", "b\n")], "b");
    origin.publish();

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_clone(
        &server,
        "acme/verify-auto-stale",
        &target,
        &["--branch", "main", "--verify-upstream=auto"],
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
    server
        .client()
        .resolve_ref_with_clonepack("acme/verify-stale", "main", Some("full"), None)
        .await
        .expect("cache A on the concrete branch");

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
        &["--branch", "main", "--verify-upstream"],
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
        .resolve_ref_with_clonepack("acme/verify-at-auto", "HEAD", Some("full"), Some("HEAD~1"))
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
        .resolve_ref_with_clonepack(
            "acme/verify-at-always",
            "HEAD",
            Some("full"),
            Some("HEAD~1"),
        )
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
