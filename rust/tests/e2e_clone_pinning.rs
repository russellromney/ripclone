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
use ripclone::client::Client;
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
    let (status, mut body) = state
        .responses
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(0);
    let content_location = body
        .as_object_mut()
        .and_then(|object| object.remove("__content_location"))
        .and_then(|value| value.as_str().map(str::to_string));
    let mut response = (status, Json(body)).into_response();
    if let Some(content_location) = content_location {
        response.headers_mut().insert(
            axum::http::header::CONTENT_LOCATION,
            urlencoding::encode(&content_location)
                .parse()
                .expect("valid encoded branch hint"),
        );
    }
    response
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

async fn abort_server_task(mut task: tokio::task::JoinHandle<()>) {
    task.abort();
    let joined = tokio::time::timeout(Duration::from_secs(5), &mut task)
        .await
        .expect("aborted test server joined within five seconds");
    assert!(
        joined.is_err(),
        "aborted test server unexpectedly succeeded"
    );
}

fn pending(commit: &str) -> (StatusCode, serde_json::Value) {
    (
        StatusCode::ACCEPTED,
        json!({
            "code": "artifact_pending",
            "commit": commit,
            "branch": "main",
            "status": "building",
            "queue_depth": 1
        }),
    )
}

fn pending_on(commit: &str, branch: &str) -> (StatusCode, serde_json::Value) {
    let (status, mut body) = pending(commit);
    body["branch"] = json!(branch);
    body["__content_location"] = json!(branch);
    (status, body)
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
            "commit": commit,
            "parent_commit": null,
            "clonepack_manifest": "manifest",
            "metadata_chunk": "metadata",
            "result": "full"
        }),
    )
}

fn ready_on(commit: &str, branch: &str) -> (StatusCode, serde_json::Value) {
    let (status, mut body) = ready(commit);
    body["branch"] = json!(branch);
    (status, body)
}

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(unix)]
#[tokio::test]
async fn bounded_child_reaps_descendants_that_inherit_output_pipes() {
    let mut command = std::process::Command::new("sh");
    command
        .args(["-c", "(sleep 30) & exit 0"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = spawn_bounded_child(&mut command).expect("spawn descendant fixture");
    let started = std::time::Instant::now();
    let output = wait_child_output_bounded(child, Duration::from_secs(2))
        .await
        .expect("child group and inherited pipes are bounded");
    assert!(output.status.success());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "pipe readers waited for the 30-second descendant"
    );
}

#[derive(Clone)]
struct RefBarrierState {
    upstream: String,
    held: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
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
    let bytes = response.bytes().await.expect("forward proxy body");

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
            tokio::time::timeout(Duration::from_secs(20), proceed)
                .await
                .expect("ref barrier released within 20 seconds")
                .expect("ref barrier sender remained alive");
        }
    }

    let mut output = axum::http::Response::builder().status(status);
    for name in [
        axum::http::header::CONTENT_TYPE,
        axum::http::header::CONTENT_LOCATION,
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
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect_err("changing pending commit must fail");
    abort_server_task(task).await;
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
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect_err("malformed pending commit must fail");
    abort_server_task(task).await;
    assert!(format!("{error:#}").contains("invalid pending commit"));
    let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(requests.len(), 1, "malformed response never retries");
    assert!(!requests[0].contains("pinned="));
}

#[tokio::test]
async fn pinned_top_up_requires_the_current_pending_shape() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }
    let (url, requests, task) = scripted_server(vec![pending(A), pending(A)]).await;
    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let error = Client::new(url)
        .install_repo_with_mode_at(
            "acme/demo",
            "main",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("a pinned Full top-up must declare the current response shape");
    abort_server_task(task).await;
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
    let error = format!("{error:#}");
    assert!(error.contains("top-up support was not declared"), "{error}");
    assert!(
        !target.exists(),
        "invalid response must not publish a target"
    );
    let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains(&format!("pinned={A}")));
    assert!(requests[1].contains("top_up=true"));
}

#[tokio::test]
async fn exact_service_unavailable_establishes_and_preserves_the_pin() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "3");
    }
    let exact_unavailable = || {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "busy", "commit": A, "branch": "main"}),
        )
    };

    let (url, pre_pin_requests, pre_pin_task) =
        scripted_server(vec![exact_unavailable(), ready(A)]).await;
    Client::new(url)
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect("the first exact 503 pins subsequent polling");
    abort_server_task(pre_pin_task).await;
    {
        let pre_pin_requests = pre_pin_requests.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(pre_pin_requests.len(), 2);
        assert!(!pre_pin_requests[0].contains("pinned="));
        assert!(pre_pin_requests[1].contains(&format!("pinned={A}")));
    }

    let (url, post_pin_requests, post_pin_task) =
        scripted_server(vec![pending(A), exact_unavailable(), ready(A)]).await;
    Client::new(url)
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect("post-pin 503 retries exact selector");
    abort_server_task(post_pin_task).await;
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
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
    }
}

#[tokio::test]
async fn unidentified_503_fails_instead_of_repinning_after_branch_moves() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2");
    }
    // The proxy-style generic 503 represents an operation that selected B but
    // lost the server's structured identity. Ready(C) is queued as the next
    // response to prove this operation must not retry and accept the moved tip.
    let (url, requests, task) = scripted_server(vec![
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "proxy unavailable"}),
        ),
        ready(B),
    ])
    .await;
    let error = Client::new(url)
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect_err("unidentified 503 cannot establish an operation pin");
    abort_server_task(task).await;
    assert!(format!("{error:#}").contains("invalid unavailable response"));
    assert_eq!(
        requests.lock().unwrap_or_else(|e| e.into_inner()).len(),
        1,
        "the original operation must not resolve or accept C"
    );
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
    }
}

#[tokio::test]
async fn absolute_checkout_name_is_rejected_before_filesystem_use() {
    let _guard = env_lock().lock().await;
    let output = tempfile::tempdir().unwrap();
    let victim = output.path().join("victim");
    std::fs::write(&victim, b"unchanged\n").unwrap();
    let malicious = victim.to_string_lossy().to_string();
    let (url, requests, task) = scripted_server(vec![ready_on(A, &malicious)]).await;
    let target = output.path().join("clone");
    let error = Client::new(url)
        .install_repo_with_mode_at(
            "acme/demo",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("absolute checkout name must fail closed");
    abort_server_task(task).await;
    assert!(format!("{error:#}").contains("invalid ready branch"));
    assert_eq!(std::fs::read(&victim).unwrap(), b"unchanged\n");
    assert!(
        !target.exists(),
        "invalid identity cannot publish a partial target"
    );
    assert_eq!(requests.lock().unwrap_or_else(|e| e.into_inner()).len(), 1);
}

#[tokio::test]
async fn empty_checkout_name_is_rejected_for_moving_head() {
    let _guard = env_lock().lock().await;
    let output = tempfile::tempdir().unwrap();
    let (url, requests, task) = scripted_server(vec![ready_on(A, "")]).await;
    let target = output.path().join("clone");
    let error = Client::new(url)
        .install_repo_with_mode_at(
            "acme/demo",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("only an exact full-object-ID operation may detach");
    abort_server_task(task).await;
    assert!(format!("{error:#}").contains("checkout name is empty"));
    assert!(
        !target.exists(),
        "invalid identity cannot publish a partial target"
    );
    assert_eq!(requests.lock().unwrap_or_else(|e| e.into_inner()).len(), 1);
}

#[tokio::test]
async fn ready_response_cannot_change_an_established_pin() {
    let _guard = env_lock().lock().await;
    let (url, requests, task) = scripted_server(vec![pending(A), ready(B)]).await;
    let error = Client::new(url)
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect_err("ready response cannot change pin");
    abort_server_task(task).await;
    assert!(format!("{error:#}").contains("integrity error"));
    assert!(requests.lock().unwrap_or_else(|e| e.into_inner())[1].contains(&format!("pinned={A}")));
}

#[tokio::test]
async fn pending_historical_head_keeps_rev_on_concrete_pinned_polls() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }
    assert!(
        std::process::Command::new("git")
            .args(["check-ref-format", "refs/heads/release#one"])
            .status()
            .expect("validate delimiter-bearing branch")
            .success(),
        "release#one must remain a valid Git branch fixture"
    );
    for concrete in ["rélease/東京", "release#one", "percent%branch"] {
        let (url, requests, task) =
            scripted_server(vec![pending_on(A, concrete), ready_on(A, concrete)]).await;
        Client::new(url)
            .resolve_exact_result(
                "acme/demo",
                "HEAD",
                ripclone::ExactResultKind::Full,
                Some("HEAD~1"),
            )
            .await
            .expect("pending rev continues at its concrete branch");
        abort_server_task(task).await;
        let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
        assert!(requests[0].contains("/refs/HEAD?"));
        assert!(requests[0].contains("rev=HEAD~1"));
        assert!(
            requests[1].contains(&format!("/refs/{}?", urlencoding::encode(concrete))),
            "concrete branch must remain one encoded request path: {:?}",
            requests[1]
        );
        let decoded = urlencoding::decode(&requests[1]).expect("decode concrete branch request");
        assert!(decoded.contains(&format!("/refs/{concrete}?")));
        assert!(requests[1].contains(&format!("pinned={A}")));
        assert!(
            requests[1].contains("rev=HEAD~1"),
            "historical pinned polls must retain their explicit rev lane: {:?}",
            requests[1]
        );
    }
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
}

#[tokio::test]
async fn ready_ref_requires_http_200() {
    let _guard = env_lock().lock().await;
    let (_, ready_body) = ready(A);
    let (url, requests, task) = scripted_server(vec![(StatusCode::CREATED, ready_body)]).await;
    let error = Client::new(url)
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect_err("201 with a ready-shaped body is not protocol success");
    abort_server_task(task).await;
    let error = format!("{error:#}");
    assert!(error.contains("ref lookup failed"), "{error}");
    assert_eq!(requests.lock().unwrap_or_else(|e| e.into_inner()).len(), 1);
}

#[tokio::test]
async fn pinned_refresh_distinguishes_authorization_from_server_failure() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }

    let (url, _, forbidden_task) = scripted_server(vec![
        pending(A),
        (StatusCode::FORBIDDEN, json!({"error": "access revoked"})),
    ])
    .await;
    let forbidden = Client::new(url)
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect_err("403 pinned refresh must fail");
    abort_server_task(forbidden_task).await;
    let forbidden = format!("{forbidden:#}");
    assert!(forbidden.contains(&format!("refresh of pinned commit {A} was not authorized")));
    assert!(forbidden.contains("access revoked"));

    let (url, _, server_task) = scripted_server(vec![
        pending(A),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "ref store unavailable"}),
        ),
    ])
    .await;
    let server_failure = Client::new(url)
        .resolve_exact_result("acme/demo", "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect_err("500 pinned refresh must fail");
    abort_server_task(server_task).await;
    let server_failure = format!("{server_failure:#}");
    assert!(server_failure.contains(&format!("refresh of pinned commit {A} failed")));
    assert!(server_failure.contains("ref store unavailable"));
    assert!(!server_failure.contains("not authorized"));

    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
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
        .resolve_exact_result(
            "acme/pin-scope-b",
            "HEAD",
            ripclone::ExactResultKind::Full,
            None,
        )
        .await
        .expect("repo B ready")
        .commit;
    let http = reqwest::Client::new();
    let request = |pin: &str| {
        http.get(format!(
            "{}/v1/repos/github/acme/pin-scope-a/refs/HEAD?result=full&pinned={pin}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
    };
    let malformed = request("not-a-sha")
        .send()
        .await
        .expect("malformed request");
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    let cross_repo = request(&b).send().await.expect("cross-repo request");
    assert_eq!(cross_repo.status(), StatusCode::CONFLICT);
    let body: serde_json::Value = cross_repo.json().await.expect("conflict body");
    assert_eq!(body["commit"], b);
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("no job is active"))
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
        let settled = sync_until_files_ready(&server, "acme", &repo).await;
        let result = if mode == CloneMode::Files {
            ripclone::ExactResultKind::Files
        } else if depth == 1 {
            ripclone::ExactResultKind::Head
        } else {
            ripclone::ExactResultKind::Full
        };
        let pinned = server
            .client()
            .resolve_exact_result(&format!("acme/{repo}"), "HEAD", result, None)
            .await
            .expect("selected variant ready")
            .commit;
        assert_eq!(pinned, settled.commit);
        let (proxy, entered, proceed, requests, proxy_task) =
            start_ref_barrier_proxy(&server.url).await;
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
        let child = spawn_bounded_child(&mut command).expect("spawn selected CLI");
        if !matches!(
            tokio::time::timeout(Duration::from_secs(20), entered).await,
            Ok(Ok(()))
        ) {
            let output = wait_child_output_bounded(child, Duration::from_secs(1)).await;
            abort_server_task(proxy_task).await;
            panic!("release CLI never reached ref barrier: {output:?}");
        }
        origin.commit(&[("value.txt", "B\n")], "B");
        origin.publish();
        let newer = git(&origin.bare, &["rev-parse", "HEAD"]);
        assert_ne!(newer, pinned);
        server
            .client()
            .sync_repo(&format!("acme/{repo}"), None)
            .await
            .expect("publish exact B");
        proceed.send(()).expect("release fetched A response");
        let output = wait_child_output_bounded(child, Duration::from_secs(60))
            .await
            .expect("release CLI clone bounded, killed, and reaped on timeout");
        abort_server_task(proxy_task).await;
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
        }
        let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            requests.len(),
            1,
            "a ready response already fetched for A must install without a metadata reread: {requests:?}"
        );
        assert!(!requests[0].contains("pinned="));
    }
}
