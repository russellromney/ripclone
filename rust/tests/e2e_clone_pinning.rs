//! Exact-commit clone orchestration proof using both a scripted ref endpoint
//! and the real server/ref store.

mod common;

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use common::*;
use ripclone::client::{ArtifactPending, Client};
use ripclone::mode::CloneMode;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[derive(Clone)]
struct ScriptState {
    requests: Arc<Mutex<Vec<String>>>,
    responses: Arc<Mutex<Vec<(StatusCode, serde_json::Value)>>>,
}

async fn scripted_ref(
    State(state): State<ScriptState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    state
        .requests
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(uri.to_string());
    let (status, body) = state
        .responses
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(0);
    (status, Json(body))
}

async fn scripted_server(
    responses: Vec<(StatusCode, serde_json::Value)>,
) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = ScriptState {
        requests: Arc::clone(&requests),
        responses: Arc::new(Mutex::new(responses)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted ref server");
    let addr = listener.local_addr().expect("scripted server address");
    let app = Router::new()
        .route("/v1/repos/{*path}", get(scripted_ref))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("scripted ref server");
    });
    (format!("http://{addr}"), requests, task)
}

fn pending(commit: &str) -> (StatusCode, serde_json::Value) {
    (
        StatusCode::ACCEPTED,
        json!({
            "code": "artifact_pending",
            "commit": commit,
            "status": "building",
            "queue_depth": 1
        }),
    )
}

fn ready(commit: &str) -> (StatusCode, serde_json::Value) {
    (
        StatusCode::OK,
        json!({
            "owner": "acme",
            "repo": "demo",
            "provider": "github",
            "host": "example.invalid",
            "origin_url": "https://example.invalid/acme/demo.git",
            "branch": "main",
            "default_branch": "main",
            "commit": commit,
            "parent_commit": null,
            "full_pack": "",
            "clonepack_manifest": "manifest",
            "metadata_chunk": "metadata",
            "shallow": false,
            "archive_ready": true
        }),
    )
}

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[derive(Clone)]
struct RefBarrierState {
    upstream: String,
    held: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
    force_first_archive_pending: bool,
    entered: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    proceed: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
}

async fn ref_barrier_proxy(
    State(state): State<RefBarrierState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let is_ref = uri.path().contains("/refs/");
    if is_ref {
        state
            .requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(uri.to_string());
    }
    let url = format!("{}{}", state.upstream, uri);
    let mut request = reqwest::Client::new().request(method, url).body(body);
    for (name, value) in headers.iter() {
        if name != axum::http::header::HOST {
            request = request.header(name, value);
        }
    }
    let response = request.send().await.expect("forward proxy request");
    let status = response.status();
    let response_headers = response.headers().clone();
    let mut bytes = response.bytes().await.expect("forward proxy body");

    let first_ref = is_ref && !state.held.swap(true, Ordering::SeqCst);
    if first_ref {
        if let Some(entered) = state
            .entered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            entered.send(()).expect("signal fetched ref");
        }
        if let Some(proceed) = state.proceed.lock().await.take() {
            proceed.await.expect("release fetched ref");
        }
        if state.force_first_archive_pending {
            let mut body: serde_json::Value =
                serde_json::from_slice(&bytes).expect("ready ref JSON");
            body["archive_ready"] = serde_json::Value::Bool(false);
            bytes = Bytes::from(serde_json::to_vec(&body).expect("encode pending archive ref"));
        }
    }

    let mut output = axum::http::Response::builder().status(status);
    for name in [
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderName::from_static("x-ripclone-clone-id"),
    ] {
        if let Some(value) = response_headers.get(&name) {
            output = output.header(name, value);
        }
    }
    output.body(Body::from(bytes)).expect("proxy response")
}

async fn start_ref_barrier_proxy(
    upstream: &str,
    force_first_archive_pending: bool,
) -> (
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    Arc<Mutex<Vec<String>>>,
    tokio::task::JoinHandle<()>,
) {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = RefBarrierState {
        upstream: upstream.to_string(),
        held: Arc::new(AtomicBool::new(false)),
        requests: Arc::clone(&requests),
        force_first_archive_pending,
        entered: Arc::new(Mutex::new(Some(entered_tx))),
        proceed: Arc::new(tokio::sync::Mutex::new(Some(proceed_rx))),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ref barrier proxy");
    let address = listener.local_addr().expect("ref barrier address");
    let app = Router::new()
        .route("/{*path}", axum::routing::any(ref_barrier_proxy))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("ref barrier proxy");
    });
    (
        format!("http://{address}"),
        entered_rx,
        proceed_tx,
        requests,
        task,
    )
}

fn selected_cli_binary() -> std::path::PathBuf {
    let binary = cargo_bin("ripclone");
    if let Some(dir) = std::env::var_os("RIPCLONE_BIN_DIR") {
        assert_eq!(
            binary.canonicalize().expect("canonical selected CLI"),
            std::path::PathBuf::from(dir)
                .join("ripclone")
                .canonicalize()
                .expect("canonical requested CLI")
        );
    }
    let version = std::process::Command::new(&binary)
        .arg("--version")
        .output()
        .expect("selected CLI version");
    assert!(version.status.success());
    binary
}

fn mutate_stored_refs(root: &std::path::Path, mut mutate: impl FnMut(&mut ripclone::RefInfo)) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("json")
                && let Ok(bytes) = std::fs::read(&path)
                && let Ok(mut info) = serde_json::from_slice::<ripclone::RefInfo>(&bytes)
            {
                mutate(&mut info);
                std::fs::write(&path, serde_json::to_vec_pretty(&info).unwrap()).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn first_pending_response_pins_every_later_lookup() {
    let _guard = env_lock().lock().await;
    let (url, requests, task) = scripted_server(vec![pending(A), ready(A)]).await;
    let client = Client::new(url);
    let info = client
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect("pinned resolve");
    task.abort();
    assert_eq!(info.commit, A);
    let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(requests.len(), 2);
    assert!(!requests[0].contains("pinned="), "first request is moving");
    assert!(
        requests[1].contains(&format!("pinned={A}")),
        "second request is exact: {:?}",
        *requests
    );
}

#[tokio::test]
async fn pending_exhaustion_is_typed_and_keeps_the_commit() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }
    let (url, requests, task) = scripted_server(vec![pending(A), pending(A)]).await;
    let error = Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect_err("artifact remains pending");
    task.abort();
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
    let pending = error
        .downcast_ref::<ArtifactPending>()
        .expect("typed artifact pending error");
    assert_eq!(pending.commit, A);
    assert_eq!(pending.mode, "full");
    assert!(
        requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .skip(1)
            .all(|request| request.contains(&format!("pinned={A}")))
    );
}

#[tokio::test]
async fn changing_pending_commit_is_an_integrity_error() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }
    let (url, requests, task) = scripted_server(vec![pending(A), pending(B)]).await;
    let error = Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect_err("changing pending commit must fail");
    task.abort();
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
    assert!(format!("{error:#}").contains("integrity error"));
    let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
    assert!(requests[1].contains(&format!("pinned={A}")));
}

#[tokio::test]
async fn malformed_pending_commit_is_a_protocol_error() {
    let _guard = env_lock().lock().await;
    let (url, requests, task) = scripted_server(vec![pending("not-an-object-id")]).await;
    let error = Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect_err("malformed pending commit must fail");
    task.abort();
    assert!(format!("{error:#}").contains("invalid pending commit"));
    let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(requests.len(), 1, "malformed response never retries");
    assert!(!requests[0].contains("pinned="));
}

#[tokio::test]
async fn service_unavailable_switches_to_exact_only_after_a_pin_exists() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }
    let unavailable = || (StatusCode::SERVICE_UNAVAILABLE, json!({"error": "busy"}));

    let (url, pre_pin_requests, pre_pin_task) =
        scripted_server(vec![unavailable(), ready(A)]).await;
    Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect("pre-pin 503 may retry moving selector");
    pre_pin_task.abort();
    {
        let pre_pin_requests = pre_pin_requests.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            pre_pin_requests
                .iter()
                .all(|request| !request.contains("pinned="))
        );
    }

    let (url, post_pin_requests, post_pin_task) =
        scripted_server(vec![pending(A), unavailable(), ready(A)]).await;
    Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect("post-pin 503 retries exact selector");
    post_pin_task.abort();
    let post_pin_requests = post_pin_requests.lock().unwrap_or_else(|e| e.into_inner());
    assert!(!post_pin_requests[0].contains("pinned="));
    assert!(
        post_pin_requests
            .iter()
            .skip(1)
            .all(|request| request.contains(&format!("pinned={A}")))
    );
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
}

#[tokio::test]
async fn ready_response_cannot_change_an_established_pin() {
    let _guard = env_lock().lock().await;
    let (url, requests, task) = scripted_server(vec![pending(A), ready(B)]).await;
    let error = Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect_err("ready response cannot change pin");
    task.abort();
    assert!(format!("{error:#}").contains("integrity error"));
    assert!(requests.lock().unwrap_or_else(|e| e.into_inner())[1].contains(&format!("pinned={A}")));
}

#[tokio::test]
async fn overwritten_branch_metadata_returns_pending_for_the_pin_without_upstream() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "overwritten-pin");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/overwritten-pin")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/overwritten-pin", None)
        .await
        .expect("sync A");
    let a = server
        .client()
        .resolve_ref_with_clonepack("acme/overwritten-pin", "HEAD", Some("full"), None)
        .await
        .expect("full A ready")
        .commit;

    origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    server
        .client()
        .sync_repo("acme/overwritten-pin", None)
        .await
        .expect("sync B");
    let b = server
        .client()
        .resolve_ref_with_clonepack("acme/overwritten-pin", "HEAD", Some("full"), None)
        .await
        .expect("full B ready")
        .commit;
    assert_ne!(a, b);

    std::fs::rename(&origin.bare, origin.bare.with_extension("offline"))
        .expect("make upstream unavailable");
    let counts = server.work_counts.as_ref().expect("counting fixture");
    let before = counts.snapshot();
    let response = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/overwritten-pin/refs/HEAD?clonepack=full&pinned={a}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "2")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("metadata-only pinned lookup");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value = response.json().await.expect("pending json");
    assert_eq!(body["code"], "artifact_pending");
    assert_eq!(body["commit"], a);
    let after = counts.snapshot();
    assert_eq!(after.pinned_requests - before.pinned_requests, 1);
    let reads = after.ref_point_reads - before.ref_point_reads;
    assert!((1..=3).contains(&reads), "bounded point reads: {reads}");
    assert_eq!(after.upstream_accesses, before.upstream_accesses);
    assert_eq!(after.enqueues, before.enqueues);
    assert_eq!(after.source_acquisitions, before.source_acquisitions);
    assert_eq!(after.builder_entries, before.builder_entries);
}

#[tokio::test]
async fn pinned_input_is_validated_and_scoped_to_the_authorized_repository() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server().await;
    register_added_without_build(&server, "acme/pin-scope-a")
        .await
        .expect("register repo A");
    let origin_b = make_origin("acme", "pin-scope-b");
    origin_b.commit(&[("secret.txt", "repo B\n")], "B");
    origin_b.publish();
    register_added_without_build(&server, "acme/pin-scope-b")
        .await
        .expect("register repo B");
    server
        .client()
        .sync_repo("acme/pin-scope-b", None)
        .await
        .expect("sync repo B");
    let b = server
        .client()
        .resolve_ref_with_clonepack("acme/pin-scope-b", "HEAD", Some("full"), None)
        .await
        .expect("repo B ready")
        .commit;
    let http = reqwest::Client::new();
    let request = |pin: &str| {
        http.get(format!(
            "{}/v1/repos/github/acme/pin-scope-a/refs/HEAD?clonepack=full&pinned={pin}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "2")
    };
    let malformed = request("not-a-sha")
        .send()
        .await
        .expect("malformed request");
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    let cross_repo = request(&b).send().await.expect("cross-repo request");
    assert_eq!(cross_repo.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value = cross_repo.json().await.expect("pending body");
    assert_eq!(body["commit"], b);
    assert_eq!(body["code"], "artifact_pending");
}

#[tokio::test]
async fn protocol_two_never_substitutes_the_other_clonepack_variant() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "strict-variant");
    origin.commit(&[("value.txt", "ready\n")], "ready");
    origin.publish();
    register_added_without_build(&server, "acme/strict-variant")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/strict-variant", None)
        .await
        .expect("sync repo");
    let commit = server
        .client()
        .resolve_ref_with_clonepack("acme/strict-variant", "HEAD", Some("full"), None)
        .await
        .expect("full ready")
        .commit;
    let ref_root = server.repo_root.join(".ripclone-refs");
    let http = reqwest::Client::new();
    let request = |variant: &str| {
        http.get(format!(
            "{}/v1/repos/github/acme/strict-variant/refs/HEAD?clonepack={variant}&pinned={commit}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "2")
    };

    mutate_stored_refs(&ref_root, |info| {
        info.shallow_clonepack = Default::default()
    });
    let shallow = request("shallow").send().await.expect("shallow request");
    assert_eq!(shallow.status(), StatusCode::ACCEPTED);
    assert_eq!(
        shallow.json::<serde_json::Value>().await.unwrap()["commit"],
        commit
    );

    mutate_stored_refs(&ref_root, |info| {
        info.shallow_clonepack.commit = commit.clone();
        info.shallow_clonepack.manifest = "present".to_string();
        info.full_clonepack = Default::default();
    });
    let full = request("full").send().await.expect("full request");
    assert_eq!(full.status(), StatusCode::ACCEPTED);
    assert_eq!(
        full.json::<serde_json::Value>().await.unwrap()["commit"],
        commit
    );
}

#[tokio::test]
async fn bounded_warm_clone_smoke_covers_files_shallow_and_full() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "pin-warm-smoke");
    origin.commit(&[("value.txt", "one\n")], "one");
    origin.commit(&[("value.txt", "two\n")], "two");
    origin.publish();

    for (depth, mode) in [
        (0, CloneMode::Files),
        (1, CloneMode::Editable),
        (0, CloneMode::Editable),
    ] {
        let (_guard, target) = tokio::time::timeout(
            Duration::from_secs(90),
            sync_and_clone(&server, &origin, depth, mode),
        )
        .await
        .expect("bounded warm clone");
        assert_eq!(
            std::fs::read_to_string(target.join("value.txt")).unwrap(),
            "two\n"
        );
        if mode == CloneMode::Editable {
            assert!(git_ok(&target, &["fsck", "--connectivity-only", "HEAD"]));
        }
    }
}

#[tokio::test]
async fn release_cli_installs_the_fetched_snapshot_after_branch_movement() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server().await;
    let binary = selected_cli_binary();

    for (name, mode, depth) in [
        ("files", CloneMode::Files, 0usize),
        ("shallow", CloneMode::Editable, 1usize),
        ("full", CloneMode::Editable, 0usize),
    ] {
        let repo = format!("release-pin-{name}");
        let origin = make_origin("acme", &repo);
        origin.commit(&[("value.txt", "base\n")], "base");
        origin.commit(&[("value.txt", "A\n")], "A");
        origin.publish();
        register_added_without_build(&server, &format!("acme/{repo}"))
            .await
            .expect("register release fixture");
        server
            .client()
            .sync_repo(&format!("acme/{repo}"), None)
            .await
            .expect("sync release fixture");
        let variant = if mode == CloneMode::Files {
            "full"
        } else {
            ripclone::mode::clonepack_kind_for_depth(depth)
        };
        let pinned = server
            .client()
            .resolve_ref_with_clonepack(&format!("acme/{repo}"), "HEAD", Some(variant), None)
            .await
            .expect("selected variant ready")
            .commit;

        let (proxy, entered, proceed, requests, proxy_task) =
            start_ref_barrier_proxy(&server.url, mode == CloneMode::Files).await;
        let out = tempfile::tempdir().expect("release clone output");
        let target = out.path().join("clone");
        let mut command = std::process::Command::new(&binary);
        command
            .arg("--server")
            .arg(&proxy)
            .arg("clone")
            .arg(format!("acme/{repo}"))
            .arg(&target)
            .arg("--depth")
            .arg(depth.to_string())
            .arg("--no-metrics")
            .arg("--verify-upstream=never")
            .env("RIPCLONE_SERVER_TOKEN", TOKEN)
            .env("RIPCLONE_NO_METRICS", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if mode == CloneMode::Files {
            command.args(["--mode", "files"]);
        }
        let child = command.spawn().expect("spawn selected CLI");
        tokio::time::timeout(Duration::from_secs(20), entered)
            .await
            .expect("CLI ref request reached barrier")
            .expect("ref barrier alive");
        origin.commit(&[("value.txt", "B\n")], "B");
        origin.publish();
        assert_ne!(git(&origin.bare, &["rev-parse", "HEAD"]), pinned);
        proceed.send(()).expect("release fetched A response");
        let output = tokio::time::timeout(
            Duration::from_secs(60),
            tokio::task::spawn_blocking(move || child.wait_with_output()),
        )
        .await
        .expect("release CLI clone bounded")
        .expect("release CLI wait task")
        .expect("release CLI output");
        proxy_task.abort();
        assert!(
            output.status.success(),
            "release {name} clone failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(target.join("value.txt")).unwrap(),
            "A\n"
        );
        if mode == CloneMode::Editable {
            assert_eq!(git(&target, &["rev-parse", "HEAD"]), pinned);
            assert!(git_ok(&target, &["fsck", "--connectivity-only", "HEAD"]));
            assert_eq!(target.join(".git/shallow").exists(), depth == 1);
        } else {
            assert!(!target.join(".git").exists());
            let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
            assert!(
                requests.len() >= 2,
                "Files readiness must poll: {requests:?}"
            );
            assert!(!requests[0].contains("pinned="));
            assert!(
                requests
                    .iter()
                    .skip(1)
                    .all(|request| request.contains(&format!("pinned={pinned}"))),
                "Files readiness repinned or moved: {requests:?}"
            );
        }
    }
}
