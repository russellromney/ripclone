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
use ripclone::provider::RepoId;
use ripclone::ref_store::{FileRefStore, RefStore};
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
    force_first_pending: bool,
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
    let mut status = response.status();
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
        if state.force_first_pending {
            let body: serde_json::Value =
                serde_json::from_slice(&bytes).expect("ready ref JSON for pending response");
            let commit = body["commit"]
                .as_str()
                .expect("ready response commit")
                .to_string();
            status = StatusCode::ACCEPTED;
            bytes = Bytes::from(
                serde_json::to_vec(&json!({
                    "code": "artifact_pending",
                    "commit": commit,
                    "status": "building",
                    "queue_depth": 1
                }))
                .expect("encode pending ref"),
            );
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
    force_first_pending: bool,
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
        force_first_pending,
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
async fn cold_pending_real_server_installs_pinned_commit_after_branch_moves() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let binary = selected_cli_binary();
    let origin = make_origin("acme", "cold-pin-ready");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/cold-pin-ready")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/cold-pin-ready", None)
        .await
        .expect("sync A");
    let pinned = server
        .client()
        .resolve_ref_with_clonepack("acme/cold-pin-ready", "HEAD", Some("full"), None)
        .await
        .expect("full A ready")
        .commit;
    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/cold-pin-ready");
    let exact_a = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load A ref")
        .expect("A ref present");
    store
        .save_branch(&repo_id, &format!("main#{pinned}"), &exact_a)
        .await
        .expect("publish exact A fixture");

    let (proxy, entered, proceed, requests, proxy_task) =
        start_ref_barrier_proxy(&server.url, false, true).await;
    let output = tempfile::tempdir().expect("clone output");
    let target = output.path().join("clone");
    let child = std::process::Command::new(binary)
        .arg("--server")
        .arg(&proxy)
        .arg("clone")
        .arg("acme/cold-pin-ready")
        .arg(&target)
        .arg("--depth")
        .arg("0")
        .arg("--no-metrics")
        .arg("--verify-upstream=never")
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_NO_METRICS", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn release CLI");
    if !matches!(
        tokio::time::timeout(Duration::from_secs(20), entered).await,
        Ok(Ok(()))
    ) {
        let output = wait_child_output_bounded(child, Duration::from_secs(1)).await;
        proxy_task.abort();
        panic!("release CLI never reached moving-response barrier: {output:?}");
    }

    origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    server
        .client()
        .sync_repo("acme/cold-pin-ready", None)
        .await
        .expect("publish B");
    std::fs::rename(&origin.bare, origin.bare.with_extension("offline"))
        .expect("make upstream unavailable");
    proceed.send(()).expect("return pending A");

    let output = wait_child_output_bounded(child, Duration::from_secs(60))
        .await
        .expect("release CLI bounded, killed, and reaped on timeout");
    proxy_task.abort();
    assert!(
        output.status.success(),
        "cold pinned clone failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), pinned);
    assert_eq!(
        std::fs::read_to_string(target.join("value.txt")).unwrap(),
        "A\n"
    );
    assert!(git_ok(&target, &["fsck", "--connectivity-only", "HEAD"]));
    let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
    assert!(
        requests.len() >= 2,
        "pending A must be polled: {requests:?}"
    );
    assert!(!requests[0].contains("pinned="), "first request is moving");
    assert!(
        requests
            .iter()
            .skip(1)
            .all(|request| request.contains(&format!("pinned={pinned}"))),
        "every post-pin request must name A: {requests:?}"
    );
}

#[tokio::test]
async fn real_server_pending_exhaustion_is_typed_and_leaves_no_target() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "cold-pin-pending");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/cold-pin-pending")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/cold-pin-pending", None)
        .await
        .expect("sync A");
    let pinned = server
        .client()
        .resolve_ref_with_clonepack("acme/cold-pin-pending", "HEAD", Some("full"), None)
        .await
        .expect("full A initially ready")
        .commit;
    mutate_stored_refs(&server.repo_root.join(".ripclone-refs"), |info| {
        info.full_clonepack = Default::default();
        info.clonepack_manifest.clear();
    });

    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }
    let (proxy, entered, proceed, requests, proxy_task) =
        start_ref_barrier_proxy(&server.url, false, false).await;
    let target_root = tempfile::tempdir().expect("pending target root");
    let target = target_root.path().join("clone");
    let client = Client::new_with_token(proxy, Some(token_hash()));
    let target_for_clone = target.clone();
    let install = tokio::spawn(async move {
        client
            .install_repo_with_mode_at(
                "acme/cold-pin-pending",
                "HEAD",
                None,
                &target_for_clone,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("pending response reached barrier")
        .expect("barrier alive");
    proceed.send(()).expect("release pending response");
    let error = install
        .await
        .expect("join pending install")
        .expect_err("exact A remains pending");
    proxy_task.abort();
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
    let pending = error
        .downcast_ref::<ArtifactPending>()
        .expect("typed artifact pending error");
    assert_eq!(pending.commit, pinned);
    assert_eq!(pending.mode, "full");
    assert!(!target.exists(), "pending clone must not publish a target");
    assert!(
        requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .skip(1)
            .all(|request| request.contains(&format!("pinned={pinned}")))
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
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect_err("403 pinned refresh must fail");
    forbidden_task.abort();
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
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect_err("500 pinned refresh must fail");
    server_task.abort();
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
async fn mismatched_variant_never_enters_the_moving_response_cache() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "guarded-cache");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/guarded-cache")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/guarded-cache", None)
        .await
        .expect("sync A");

    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/guarded-cache");
    let mut valid_a = None;
    for _ in 0..200 {
        if let Ok(Some(info)) = store.load_branch(&repo_id, "main").await
            && info.build_status.is_none()
            && !info.full_clonepack.manifest.is_empty()
        {
            valid_a = Some(info);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let valid_a = valid_a.expect("full A publication settled");
    assert_eq!(valid_a.commit, a);

    let mut mismatched = valid_a.clone();
    mismatched.full_clonepack.commit = B.to_string();
    store
        .save_branch(&repo_id, "main", &mismatched)
        .await
        .expect("publish target-A/artifact-B row");

    let http = reqwest::Client::new();
    let request = || {
        http.get(format!(
            "{}/v1/repos/github/acme/guarded-cache/refs/main?clonepack=full",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "2")
    };
    let rejected = request().send().await.expect("mismatched lookup");
    assert_eq!(rejected.status(), StatusCode::ACCEPTED);
    let rejected: serde_json::Value = rejected.json().await.expect("pending response");
    assert_eq!(rejected["commit"], a);

    store
        .save_branch(&repo_id, "main", &valid_a)
        .await
        .expect("restore guarded A row");
    let ready = request().send().await.expect("guarded lookup");
    assert_eq!(ready.status(), StatusCode::OK);
    let ready: serde_json::Value = ready.json().await.expect("ready A response");
    assert_eq!(ready["commit"], a);

    // Change the durable row out of band after the successful response. The
    // next lookup is a real response-cache hit and must contain only guarded A,
    // never the earlier rejected target-A/artifact-B snapshot.
    store
        .save_branch(&repo_id, "main", &mismatched)
        .await
        .expect("restore mismatched durable row");
    let cached = request().send().await.expect("cached lookup");
    assert_eq!(cached.status(), StatusCode::OK);
    let cached: serde_json::Value = cached.json().await.expect("cached A response");
    assert_eq!(cached["commit"], a);
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

    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }
    let (proxy, entered, proceed, requests, proxy_task) =
        start_ref_barrier_proxy(&server.url, true, false).await;
    let target_root = tempfile::tempdir().expect("overwrite target root");
    let target = target_root.path().join("clone");
    let target_for_clone = target.clone();
    let client = Client::new_with_token(proxy, Some(token_hash()));
    let mut install = tokio::spawn(async move {
        client
            .install_repo_with_mode_at(
                "acme/overwritten-pin",
                "HEAD",
                None,
                &target_for_clone,
                CloneMode::Files,
                Some("full"),
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("moving A response reached barrier")
        .expect("barrier alive");

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    assert_ne!(a, b);
    server
        .client()
        .sync_repo("acme/overwritten-pin", None)
        .await
        .expect("sync B");

    // Let B's archive publication finish before arming the request-path
    // adapter, so background ref-store reads cannot contaminate the exact
    // three-candidate count below.
    let durable = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/overwritten-pin");
    let mut settled = false;
    for _ in 0..200 {
        if matches!(
            durable.load_branch(&repo_id, "HEAD").await,
            Ok(Some(info)) if info.build_status.is_none()
        ) {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(settled, "B fixture background publication settled");

    std::fs::rename(&origin.bare, origin.bare.with_extension("offline"))
        .expect("make upstream unavailable");
    let probe = server
        .pinned_path_probe
        .as_ref()
        .expect("pinned-path test adapter");
    probe.arm();
    proceed.send(()).expect("release ready A metadata");
    let error = match tokio::time::timeout(Duration::from_secs(20), &mut install).await {
        Ok(joined) => joined
            .expect("join overwritten install")
            .expect_err("overwritten A metadata must exhaust as pending"),
        Err(_) => {
            install.abort();
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut install).await;
            panic!("overwritten metadata install did not finish within 20 seconds");
        }
    };
    proxy_task.abort();
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
    let pending = error
        .downcast_ref::<ArtifactPending>()
        .expect("overwritten metadata ends in typed pending");
    assert_eq!(pending.commit, a);
    assert_eq!(pending.mode, "files");
    assert!(!target.exists(), "pending clone must not publish a target");
    let requests = requests.lock().unwrap_or_else(|e| e.into_inner());
    assert!(!requests[0].contains("pinned="));
    assert!(
        requests
            .iter()
            .skip(1)
            .all(|request| request.contains(&format!("pinned={a}"))),
        "every request after moving A must stay pinned to A: {requests:?}"
    );
    let observed = probe.snapshot();
    assert_eq!(
        observed.branch_reads,
        3 * (requests.len() - 1),
        "each exact poll performs only the three bounded point reads"
    );
    assert_eq!(observed.enqueues, 0);
}

#[tokio::test]
async fn pinned_head_reads_pre_upgrade_default_branch_exact_row() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "baseline-layout-pin");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/baseline-layout-pin")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/baseline-layout-pin", None)
        .await
        .expect("sync A");
    let a = server
        .client()
        .resolve_ref_with_clonepack("acme/baseline-layout-pin", "HEAD", Some("full"), None)
        .await
        .expect("full A ready")
        .commit;

    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/baseline-layout-pin");
    let exact_a = store
        .load_branch(&repo_id, "HEAD")
        .await
        .expect("load A HEAD row")
        .expect("A HEAD row present");
    assert_eq!(exact_a.commit, a);
    let mut encoded = serde_json::to_value(exact_a).expect("serialize baseline-layout row");
    encoded["full_clonepack"]
        .as_object_mut()
        .expect("full clonepack object")
        .remove("commit");
    let exact_a: ripclone::RefInfo =
        serde_json::from_value(encoded).expect("deserialize pre-variant-commit layout");
    assert!(exact_a.full_clonepack.commit.is_empty());
    store
        .save_branch(&repo_id, &format!("main#{a}"), &exact_a)
        .await
        .expect("seed pre-upgrade main exact row");
    assert!(
        store
            .load_branch(&repo_id, &format!("HEAD#{a}"))
            .await
            .expect("check duplicate alias")
            .is_none(),
        "baseline layout must not contain a HEAD exact alias"
    );

    origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    server
        .client()
        .sync_repo("acme/baseline-layout-pin", None)
        .await
        .expect("publish B");
    let stored_exact = store
        .load_branch(&repo_id, &format!("main#{a}"))
        .await
        .expect("reload exact A")
        .expect("exact A remains present");
    assert_eq!(stored_exact.commit, a);
    assert!(stored_exact.full_clonepack.commit.is_empty());
    assert!(!stored_exact.full_clonepack.manifest.is_empty());
    let moving_head = store
        .load_branch(&repo_id, "HEAD")
        .await
        .expect("reload moving HEAD")
        .expect("moving HEAD remains present");
    assert_eq!(moving_head.default_branch, "main");

    let response = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/baseline-layout-pin/refs/HEAD?clonepack=full&pinned={a}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "2")
        .send()
        .await
        .expect("pinned baseline-layout lookup");
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.expect("ready response");
    assert_eq!(body["commit"], a);
}

#[tokio::test]
async fn pinned_lookup_uses_exact_a_while_phase_one_b_is_paused() {
    let _guard = env_lock().lock().await;
    init(false);
    let (server, barrier, entered, proceed) = start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "phase-one-pin");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/phase-one-pin")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/phase-one-pin", None)
        .await
        .expect("sync A");
    let a = server
        .client()
        .resolve_ref_with_clonepack("acme/phase-one-pin", "main", Some("full"), None)
        .await
        .expect("full A ready")
        .commit;

    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/phase-one-pin");
    let exact_a = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load A")
        .expect("A row");
    assert_eq!(exact_a.commit, a);
    store
        .save_branch(&repo_id, &format!("main#{a}"), &exact_a)
        .await
        .expect("publish exact A fixture");

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/phase-one-pin", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase-one publication")
        .expect("phase-one barrier alive");

    let moving_b = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load paused B")
        .expect("paused B row");
    assert_eq!(moving_b.commit, b);
    assert_eq!(moving_b.full_clonepack.commit, a);
    assert_ne!(
        moving_b
            .packs
            .iter()
            .map(|pack| pack.pack.as_str())
            .collect::<Vec<_>>(),
        exact_a
            .packs
            .iter()
            .map(|pack| pack.pack.as_str())
            .collect::<Vec<_>>()
    );

    let probe = server
        .pinned_path_probe
        .as_ref()
        .expect("pinned-path test adapter");
    probe.arm();
    let pinned_url = format!(
        "{}/v1/repos/github/acme/phase-one-pin/refs/main?clonepack=full&pinned={a}",
        server.url
    );
    let response = reqwest::Client::new()
        .get(&pinned_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "2")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("pinned lookup while B phase one is paused");
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.expect("ready A response");
    assert_eq!(body["commit"], a);
    assert_eq!(body["clonepack_manifest"], exact_a.full_clonepack.manifest);
    let exact_observed = probe.snapshot();
    assert_eq!(exact_observed.branch_reads, 2);
    assert_eq!(exact_observed.enqueues, 0);

    // With the exact row absent, the paused moving B row still carries
    // Full(A). It must not satisfy pin A because its enclosing/top-level fields
    // belong to B. This makes the moving-row guard independently non-vacuous.
    store
        .delete_branch(&repo_id, &format!("main#{a}"))
        .await
        .expect("remove exact A fixture");
    probe.arm();
    let pending = reqwest::Client::new()
        .get(&pinned_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "2")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("pinned fallback lookup while B phase one is paused");
    assert_eq!(pending.status(), StatusCode::ACCEPTED);
    let pending: serde_json::Value = pending.json().await.expect("pending A response");
    assert_eq!(pending["code"], "artifact_pending");
    assert_eq!(pending["commit"], a);
    let fallback_observed = probe.snapshot();
    assert_eq!(fallback_observed.branch_reads, 2);
    assert_eq!(fallback_observed.enqueues, 0);

    proceed.send(()).expect("release B phase-one publication");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("sync B completed after barrier release")
        .expect("join sync B")
        .expect("sync B");
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
        if mode == CloneMode::Files {
            let exact_store = FileRefStore::new(&server.repo_root);
            let repo_id = RepoId::github(format!("acme/{repo}"));
            let exact_a = exact_store
                .load_branch(&repo_id, "HEAD")
                .await
                .expect("load file-store A ref")
                .expect("file-store A ref present");
            exact_store
                .save_branch(&repo_id, &format!("main#{pinned}"), &exact_a)
                .await
                .expect("publish exact file-store A fixture");
        }

        let (proxy, entered, proceed, requests, proxy_task) =
            start_ref_barrier_proxy(&server.url, mode == CloneMode::Files, false).await;
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
        if !matches!(
            tokio::time::timeout(Duration::from_secs(20), entered).await,
            Ok(Ok(()))
        ) {
            let output = wait_child_output_bounded(child, Duration::from_secs(1)).await;
            proxy_task.abort();
            panic!("release CLI never reached ref barrier: {output:?}");
        }
        origin.commit(&[("value.txt", "B\n")], "B");
        origin.publish();
        let newer = git(&origin.bare, &["rev-parse", "HEAD"]);
        assert_ne!(newer, pinned);
        if mode == CloneMode::Files {
            server
                .client()
                .sync_repo(&format!("acme/{repo}"), None)
                .await
                .expect("publish B through file ref store");
            let published_b = server
                .client()
                .resolve_ref_with_clonepack(&format!("acme/{repo}"), "HEAD", Some("full"), None)
                .await
                .expect("file-store branch row B ready")
                .commit;
            assert_eq!(published_b, newer);
        }
        proceed.send(()).expect("release fetched A response");
        let output = wait_child_output_bounded(child, Duration::from_secs(60))
            .await
            .expect("release CLI clone bounded, killed, and reaped on timeout");
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
