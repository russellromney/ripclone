//! Operator-facing contracts for the optimized `ripclone` CLI binary.

use crate::common::*;
use axum::Router;
use axum::body::Body;
use axum::http::{Response, StatusCode};
use axum::routing::any;
use ripclone::client::Client;
use ripclone::mode::CloneMode;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{Duration, Instant, UNIX_EPOCH};

fn cli_binary() -> PathBuf {
    std::env::var_os("RIPCLONE_TEST_CLI_BIN")
        .or_else(|| std::env::var_os("CARGO_BIN_EXE_ripclone"))
        .map(PathBuf::from)
        .expect("RIPCLONE_TEST_CLI_BIN or CARGO_BIN_EXE_ripclone")
}

async fn run_cli(
    server: &str,
    cwd: &Path,
    home: &Path,
    args: &[&str],
    envs: &[(&str, &str)],
) -> (Output, Duration) {
    let started = Instant::now();
    let mut command = tokio::process::Command::new(cli_binary());
    command
        .args(args)
        .current_dir(cwd)
        .env("HOME", home)
        .env("RIPCLONE_SERVER", server)
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_TESTING", "1")
        .env("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2")
        .env("RIPCLONE_TEST_REF_POLL_MS", "50")
        .env("RIPCLONE_TEST_SYNC_MAX_ATTEMPTS", "2")
        .env("RIPCLONE_TEST_SYNC_POLL_MS", "50")
        .kill_on_drop(true);
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = tokio::time::timeout(Duration::from_secs(8), command.output())
        .await
        .expect("release CLI remained bounded")
        .expect("spawn release CLI");
    (output, started.elapsed())
}

fn output_text(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn snapshot_files(root: &Path) -> Vec<(PathBuf, u64, u128, String)> {
    fn visit(root: &Path, path: &Path, rows: &mut Vec<(PathBuf, u64, u128, String)>) {
        if !path.exists() {
            return;
        }
        for entry in std::fs::read_dir(path).expect("snapshot read_dir") {
            let entry = entry.expect("snapshot entry");
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).expect("snapshot metadata");
            if metadata.is_dir() {
                visit(root, &path, rows);
            } else if metadata.is_file() {
                let bytes = std::fs::read(&path).expect("snapshot bytes");
                let modified = metadata
                    .modified()
                    .unwrap_or(UNIX_EPOCH)
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                rows.push((
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    metadata.len(),
                    modified,
                    hex::encode(Sha256::digest(bytes)),
                ));
            }
        }
    }
    let mut rows = Vec::new();
    visit(root, root, &mut rows);
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

async fn pending_metadata_server(commit: &str) -> String {
    let commit = commit.to_string();
    let app = Router::new().fallback(any(move || {
        let commit = commit.clone();
        async move {
            Response::builder()
                .status(StatusCode::ACCEPTED)
                .header("content-type", "application/json")
                .header("content-location", "main")
                .body(Body::from(
                    serde_json::json!({
                        "code": "artifact_pending",
                        "commit": commit,
                        "branch": "main",
                        "status": "building",
                        "queue_depth": 1,
                        "top_up_supported": true
                    })
                    .to_string(),
                ))
                .unwrap()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pending metadata server");
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    url
}

#[derive(Clone, Copy)]
enum IdentityResponseStatus {
    Pending,
    Unavailable,
    Ready,
}

async fn identity_response_server(
    status: IdentityResponseStatus,
    commit: &str,
    body_branch: Option<&str>,
    content_location: Option<&str>,
) -> String {
    let commit = commit.to_string();
    let body_branch = body_branch.map(str::to_string);
    let content_location = content_location.map(str::to_string);
    let app = Router::new().fallback(any(move || {
        let commit = commit.clone();
        let body_branch = body_branch.clone();
        let content_location = content_location.clone();
        async move {
            let mut body = match status {
                IdentityResponseStatus::Pending => serde_json::json!({
                    "code": "artifact_pending",
                    "commit": commit,
                    "status": "building",
                    "queue_depth": 1
                }),
                IdentityResponseStatus::Unavailable => serde_json::json!({
                    "error": "queue unavailable",
                    "commit": commit
                }),
                IdentityResponseStatus::Ready => serde_json::json!({
                    "owner": "acme",
                    "repo": "identity",
                    "provider": "github",
                    "host": "github.com",
                    "origin_url": "https://github.com/acme/identity.git",
                    "commit": commit,
                    "parent_commit": null,
                    "clonepack_manifest": "manifest",
                    "metadata_chunk": "metadata",
                    "result": "full"
                }),
            };
            if let Some(branch) = body_branch {
                body["branch"] = serde_json::Value::String(branch);
            }
            let http_status = match status {
                IdentityResponseStatus::Pending => StatusCode::ACCEPTED,
                IdentityResponseStatus::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                IdentityResponseStatus::Ready => StatusCode::OK,
            };
            let mut response = Response::builder()
                .status(http_status)
                .header("content-type", "application/json");
            if let Some(location) = content_location {
                response = response.header("content-location", location);
            }
            response.body(Body::from(body.to_string())).unwrap()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind identity response server");
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    url
}

#[tokio::test]
async fn explicit_sha_is_the_initial_sync_and_clone_pin_for_every_response_status() {
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "cccccccccccccccccccccccccccccccccccccccc";

    for status in [
        IdentityResponseStatus::Pending,
        IdentityResponseStatus::Unavailable,
        IdentityResponseStatus::Ready,
    ] {
        let server = identity_response_server(status, C, Some("main"), Some("main")).await;
        let client = Client::new(server);
        let sync_error = client
            .sync_repo_at("acme/identity", Some(B), None)
            .await
            .expect_err("sync --at B must reject an initial response pinned to C");
        assert!(
            format!("{sync_error:#}").contains("integrity error"),
            "unexpected sync mismatch: {sync_error:#}"
        );

        let output = tempfile::tempdir().expect("clone identity output");
        let target = output.path().join("target");
        let clone_error = client
            .install_repo_with_mode_at(
                "acme/identity",
                "HEAD",
                Some(B),
                &target,
                CloneMode::Editable,
                None,
                None,
            )
            .await
            .expect_err("clone --at B must reject an initial response pinned to C");
        assert!(
            format!("{clone_error:#}").contains("integrity error"),
            "unexpected clone mismatch: {clone_error:#}"
        );
        assert!(!target.exists());
    }
}

#[tokio::test]
async fn ordinary_clone_requires_pending_body_branch_and_matching_content_location() {
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    for (body_branch, content_location, expected) in [
        (None, Some("main"), "missing field `branch`"),
        (
            Some("main"),
            Some("release"),
            "disagrees with Content-Location",
        ),
    ] {
        let server = identity_response_server(
            IdentityResponseStatus::Pending,
            B,
            body_branch,
            content_location,
        )
        .await;
        let client = Client::new(server);
        let output = tempfile::tempdir().expect("pending identity output");
        let target = output.path().join("target");
        let error = client
            .install_repo_with_mode_at(
                "acme/identity",
                "HEAD",
                None,
                &target,
                CloneMode::Editable,
                None,
                None,
            )
            .await
            .expect_err("contradictory pending identity must fail closed");
        assert!(
            format!("{error:#}").contains(expected),
            "expected {expected:?}, got {error:#}"
        );
        assert!(!target.exists());
    }
}

#[tokio::test]
async fn ordinary_clone_rejects_initial_concrete_branch_change() {
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    for status in [
        IdentityResponseStatus::Pending,
        IdentityResponseStatus::Ready,
    ] {
        let server = identity_response_server(status, B, Some("main"), Some("main")).await;
        let client = Client::new(server);
        let output = tempfile::tempdir().expect("branch identity output");
        let target = output.path().join("target");
        let error = client
            .install_repo_with_mode_at(
                "acme/identity",
                "release",
                None,
                &target,
                CloneMode::Editable,
                None,
                None,
            )
            .await
            .expect_err("release must reject an initial response for main");
        assert!(
            format!("{error:#}").contains("integrity error"),
            "unexpected branch mismatch: {error:#}"
        );
        assert!(!target.exists());
    }
}

fn hanging_origin() -> (
    String,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Receiver<bool>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind hanging origin");
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    let (closed_tx, closed_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept hanging Git request");
        stream
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        accepted_tx.send(()).unwrap();
        let mut buffer = [0_u8; 4096];
        let closed = loop {
            match stream.read(&mut buffer) {
                Ok(0) => break true,
                Ok(_) => continue,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    break false;
                }
                Err(_) => break true,
            }
        };
        closed_tx.send(closed).unwrap();
    });
    (format!("http://{address}"), accepted_rx, closed_rx)
}

fn git_processes_for(origin: &str) -> Vec<String> {
    let output = std::process::Command::new("ps")
        .args(["-axo", "command="])
        .output()
        .expect("list processes");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.contains("git ls-remote") && line.contains(origin))
        .map(str::to_string)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_cli_sync_clone_failure_and_cleanup_contract() {
    setup(false);
    let home = tempfile::tempdir().expect("CLI home");
    let work = tempfile::tempdir().expect("CLI work");

    // Ready sync: one real provider probe, clear output, and no durable writes.
    let origin = make_http_origin_with_auth("acme/cli-ready", "token cli-token");
    let b = origin.commit(&[("README.md", "ready B\n")], "B");
    origin.publish();
    let providers_json = serde_json::json!({
        "providers": [{
            "id": "cli-http", "kind": "generic", "host": origin.url,
            "token": "cli-token", "auth_template": "token {token}"
        }]
    })
    .to_string();
    let server = start_server_env(&[("RIPCLONE_PROVIDERS", &providers_json)]).await;
    register_added_without_build_for_provider(&server, "cli-http", "acme/cli-ready")
        .await
        .expect("register ready CLI repo");
    server
        .client_with_provider("cli-http", Some("cli-token"))
        .sync_repo("acme/cli-ready", None)
        .await
        .expect("publish ready B");
    server
        .client_with_provider("cli-http", Some("cli-token"))
        .resolve_exact_result(
            "acme/cli-ready",
            &b,
            ripclone::ExactResultKind::Full,
            Some(&b),
        )
        .await
        .expect("settle exact B before the no-write ready probe");
    let durable_before = (
        snapshot_files(&server.cas_dir),
        snapshot_files(&server.repo_root),
    );
    origin.clear_auth_log();
    let (ready, ready_elapsed) = run_cli(
        &server.url,
        work.path(),
        home.path(),
        &["--provider", "cli-http", "sync", "acme/cli-ready"],
        &[("RIPCLONE_PROVIDERS", &providers_json)],
    )
    .await;
    assert!(ready.status.success(), "{}", output_text(&ready));
    assert!(
        output_text(&ready).contains(&format!("already current at {b}")),
        "{}",
        output_text(&ready)
    );
    assert!(ready_elapsed < Duration::from_secs(5));
    assert_eq!(
        (
            snapshot_files(&server.cas_dir),
            snapshot_files(&server.repo_root)
        ),
        durable_before,
        "ready CLI sync mutated durable state"
    );
    let ready_log = origin.auth_log_text();
    assert!(!ready_log.is_empty());
    assert!(
        ready_log.lines().all(|line| {
            let path = line.split('\t').nth(2).unwrap_or("");
            path.contains("/info/refs") || path.ends_with("/HEAD")
        }),
        "ready sync transferred objects:\n{ready_log}"
    );

    // A Full clone with no safe base names its immutable pin and removes every
    // target-adjacent staging directory on bounded exhaustion.
    let pending_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let pending_server = pending_metadata_server(pending_b).await;
    let target = work.path().join("pending-target");
    let before_entries = std::fs::read_dir(work.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    let (pending, pending_elapsed) = run_cli(
        &pending_server,
        work.path(),
        home.path(),
        &[
            "--provider",
            "github",
            "--token",
            "unused",
            "clone",
            "acme/pending",
            target.to_str().unwrap(),
            "--mode",
            "editable",
        ],
        &[],
    )
    .await;
    let pending_text = output_text(&pending);
    assert!(
        !pending.status.success(),
        "pending clone succeeded: {pending_text}"
    );
    assert!(pending_text.contains(pending_b), "{pending_text}");
    assert!(
        pending_text.to_ascii_lowercase().contains("pending"),
        "{pending_text}"
    );
    assert!(pending_elapsed < Duration::from_secs(3));
    assert!(!target.exists());
    let after_entries = std::fs::read_dir(work.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(
        after_entries, before_entries,
        "pending clone leaked staging"
    );

    // An unresponsive provider is bounded by the admission timeout, closes its
    // connection, never reaches the queue, and leaves no Git process behind.
    let (hanging_url, accepted, closed) = hanging_origin();
    let timeout_providers = serde_json::json!({
        "providers": [{
            "id": "hanging", "kind": "generic", "host": hanging_url,
            "token": "hang-token", "auth_template": "token {token}"
        }]
    })
    .to_string();
    let timeout_server = start_server_env(&[("RIPCLONE_PROVIDERS", &timeout_providers)]).await;
    register_added_without_build_for_provider(&timeout_server, "hanging", "acme/timeout")
        .await
        .expect("register timeout repo");
    assert!(git_processes_for(&hanging_url).is_empty());
    unsafe { std::env::set_var("RIPCLONE_LS_REMOTE_TIMEOUT_SECS", "1") };
    let (timed_out, timeout_elapsed) = run_cli(
        &timeout_server.url,
        work.path(),
        home.path(),
        &["--provider", "hanging", "sync", "acme/timeout"],
        &[("RIPCLONE_PROVIDERS", &timeout_providers)],
    )
    .await;
    unsafe { std::env::remove_var("RIPCLONE_LS_REMOTE_TIMEOUT_SECS") };
    let timeout_text = output_text(&timed_out);
    assert!(
        !timed_out.status.success(),
        "timeout sync succeeded: {timeout_text}"
    );
    assert!(
        timeout_text.to_ascii_lowercase().contains("timed out"),
        "{timeout_text}"
    );
    assert!(
        timeout_elapsed < Duration::from_secs(4),
        "timeout took {timeout_elapsed:?}"
    );
    accepted
        .recv_timeout(Duration::from_secs(2))
        .expect("Git reached hanging origin");
    assert!(
        closed
            .recv_timeout(Duration::from_secs(2))
            .expect("hanging origin observed cancellation"),
        "timed-out Git connection remained open"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        git_processes_for(&hanging_url).is_empty(),
        "timed-out git ls-remote survived: {:?}",
        git_processes_for(&hanging_url)
    );
    let timeout_status: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/hanging/acme/timeout/status",
            timeout_server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("timeout status request")
        .error_for_status()
        .expect("timeout status response")
        .json()
        .await
        .expect("timeout status JSON");
    assert_eq!(timeout_status["refs"], serde_json::json!([]));
    assert!(snapshot_files(&timeout_server.cas_dir).is_empty());

    println!(
        "CLI_CONTRACT_EVIDENCE ready_ms={} pending_ms={} timeout_ms={} timeout_exact_results=0",
        ready_elapsed.as_millis(),
        pending_elapsed.as_millis(),
        timeout_elapsed.as_millis()
    );
}
