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
use prost::Message;
use ripclone::client::{ArtifactPending, Client};
use ripclone::mode::CloneMode;
use ripclone::provider::RepoId;
use ripclone::ref_store::{FileRefStore, RefStore, exact_ref_key};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct EnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl EnvGuard {
    fn capture(keys: &[&'static str]) -> Self {
        Self(
            keys.iter()
                .map(|key| (*key, std::env::var_os(key)))
                .collect(),
        )
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.0 {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

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

fn ready_on(commit: &str, branch: &str) -> (StatusCode, serde_json::Value) {
    let (status, mut body) = ready(commit);
    body["branch"] = json!(branch);
    body["default_branch"] = json!(branch);
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
    let mut pending_content_location = None;

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
            pending_content_location = Some(
                body["branch"]
                    .as_str()
                    .expect("ready response concrete branch")
                    .to_string(),
            );
            status = StatusCode::ACCEPTED;
            bytes = Bytes::from(
                serde_json::to_vec(&json!({
                    "code": "artifact_pending",
                    "commit": commit,
                    "branch": pending_content_location.as_deref(),
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
        axum::http::header::CONTENT_LOCATION,
        axum::http::HeaderName::from_static("x-ripclone-clone-id"),
    ] {
        if let Some(value) = response_headers.get(&name) {
            output = output.header(name, value);
        }
    }
    if let Some(branch) = pending_content_location {
        output = output.header(
            axum::http::header::CONTENT_LOCATION,
            urlencoding::encode(&branch).as_ref(),
        );
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

fn content_hashes(value: &serde_json::Value, out: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(value)
            if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            out.insert(value.clone());
        }
        serde_json::Value::Array(values) => {
            for value in values {
                content_hashes(value, out);
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                if key == "raw_hash" {
                    continue;
                }
                content_hashes(value, out);
            }
        }
        _ => {}
    }
}

fn artifact_snapshot(
    storage_root: &Path,
    cas_root: &Path,
    info: &ripclone::RefInfo,
) -> BTreeMap<String, Vec<u8>> {
    let mut hashes = BTreeSet::new();
    content_hashes(
        &serde_json::to_value(info).expect("serialize artifact ref"),
        &mut hashes,
    );
    let storage = ripclone::storage::local(storage_root).expect("open artifact storage");
    let cas = ripclone::cas::Cas::new(cas_root).expect("open artifact CAS");
    hashes
        .into_iter()
        .map(|hash| {
            let bytes = storage
                .get(&hash)
                .or_else(|_| cas.get(&hash))
                .unwrap_or_else(|error| panic!("load artifact {hash}: {error:#}"));
            (hash, bytes)
        })
        .collect()
}

async fn run_pinned_branch_advance_proof(advance_again: bool) {
    let _guard = env_lock().lock().await;
    init(false);
    let _env = EnvGuard::capture(&[
        "RIPCLONE_RECHECK_MAX",
        "RIPCLONE_TESTING",
        "RIPCLONE_TEST_REF_MAX_ATTEMPTS",
        "RIPCLONE_TEST_REF_POLL_MS",
    ]);
    unsafe {
        std::env::set_var("RIPCLONE_RECHECK_MAX", "0");
        std::env::set_var("RIPCLONE_TESTING", "1");
    }

    let (server, phase_one, phase_one_entered, phase_one_proceed) =
        start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "pinned-advance-proof");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/pinned-advance-proof")
        .await
        .expect("register pin proof repo");
    server
        .client()
        .sync_repo("acme/pinned-advance-proof", None)
        .await
        .expect("publish A");

    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    phase_one.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let b_admission = server
        .client()
        .admit_sync_repo("acme/pinned-advance-proof", None)
        .await
        .expect("admit B");
    assert!(b_admission.accepted);
    assert_eq!(b_admission.commit, b);
    tokio::time::timeout(Duration::from_secs(20), phase_one_entered)
        .await
        .expect("B phase-one publication entered")
        .expect("phase-one barrier sender alive");

    let binary = selected_cli_binary();
    let (proxy, proxy_entered, proxy_proceed, requests, proxy_task) =
        start_ref_barrier_proxy(&server.url, false, false).await;
    let home = tempfile::tempdir().expect("CLI home");
    let output_root = tempfile::tempdir().expect("clone output root");
    let target = output_root.path().join("clone");
    let mut command = std::process::Command::new(binary);
    command
        .arg("--server")
        .arg(&proxy)
        .arg("clone")
        .arg("acme/pinned-advance-proof")
        .arg(&target)
        .arg("--depth")
        .arg("0")
        .arg("--no-metrics")
        .arg("--verify-upstream=never")
        .env("HOME", home.path())
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_NO_METRICS", "1")
        .env("RIPCLONE_TESTING", "1")
        .env("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "100")
        .env("RIPCLONE_TEST_REF_POLL_MS", "10")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = spawn_bounded_child(&mut command).expect("spawn pinned clone CLI");
    tokio::time::timeout(Duration::from_secs(20), proxy_entered)
        .await
        .expect("CLI reached the first moving ref response")
        .expect("ref proxy barrier sender alive");

    git(&origin.work, &["reset", "--hard", &a]);
    let c = origin.commit(&[("value.txt", "C\n")], "C-divergent");
    origin.publish();
    let c_admission = server
        .client()
        .admit_sync_repo("acme/pinned-advance-proof", None)
        .await
        .expect("admit C");
    assert!(c_admission.accepted);
    assert_eq!(c_admission.commit, c);
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("C full publication");

    let store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&server.repo_root));
    let repo_id = RepoId::github("acme/pinned-advance-proof");
    let mut latest_commit = c.clone();
    let mut moving_before = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load C moving row")
        .expect("C moving row exists");
    assert_eq!(moving_before.commit, c);

    if advance_again {
        let d = origin.commit(&[("value.txt", "D\n")], "D");
        origin.publish();
        let d_admission = server
            .client()
            .admit_sync_repo("acme/pinned-advance-proof", None)
            .await
            .expect("admit D");
        assert!(d_admission.accepted);
        assert_eq!(d_admission.commit, d);
        tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(2))
            .await
            .expect("D full publication");
        latest_commit = d;
        moving_before = store
            .load_branch(&repo_id, "main")
            .await
            .expect("load D moving row")
            .expect("D moving row exists");
        assert_eq!(moving_before.commit, latest_commit);
    }
    let moving_json = serde_json::to_value(&moving_before).expect("serialize moving snapshot");
    let moving_artifacts = artifact_snapshot(&server.storage_dir, &server.cas_dir, &moving_before);
    let expected_jobs = if advance_again { 3 } else { 2 };
    assert_eq!(
        probe.queue_inserts.load(Ordering::SeqCst),
        expected_jobs,
        "only A-free B/C/D admissions exist before pinned polls"
    );
    let tip_probes_before_polls = probe.tip_probes.load(Ordering::SeqCst);
    probe.http_trace.lock().unwrap().clear();

    proxy_proceed.send(()).expect("release B response to CLI");
    let required_pinned_requests: usize = if advance_again { 3 } else { 2 };
    let first_poll =
        tokio::time::timeout(Duration::from_secs(20), probe.wait_until_http_trace_len(1)).await;
    if first_poll.is_err() {
        let _ = phase_one_proceed.send(());
        let output = wait_child_output_bounded(child, Duration::from_secs(10))
            .await
            .expect("collect failed pin proof CLI");
        abort_server_task(proxy_task).await;
        panic!(
            "pinned polls did not arrive: proxy={:?} trace={:?} stdout={} stderr={}",
            requests.lock().unwrap(),
            probe.http_trace.lock().unwrap(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let current_pinned_requests = probe.http_trace.lock().unwrap().len();
    let extra_polls = required_pinned_requests.saturating_sub(current_pinned_requests);
    for _ in 0..extra_polls {
        let response = reqwest::Client::new()
            .get(format!(
                "{}/v1/repos/github/acme/pinned-advance-proof/refs/main?clonepack=full&pinned={b}",
                server.url
            ))
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
            .send()
            .await
            .expect("pinned B poll");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body: serde_json::Value = response.json().await.expect("pinned B poll body");
        assert_eq!(body["commit"], b);
    }
    tokio::time::timeout(
        Duration::from_secs(20),
        probe.wait_until_http_trace_len(required_pinned_requests),
    )
    .await
    .expect("pinned polls reached the server while B was held");
    let trace = probe.http_trace.lock().unwrap().clone();
    assert!(
        trace
            .iter()
            .all(|event| event.contains(&format!("pinned={b}"))),
        "every post-admission server request remains pinned to B: {trace:?}"
    );
    assert_eq!(
        probe.tip_probes.load(Ordering::SeqCst),
        tip_probes_before_polls,
        "pinned polls do not probe the moving provider branch"
    );
    assert_eq!(
        probe.queue_inserts.load(Ordering::SeqCst),
        expected_jobs,
        "pinned polls do not enqueue C or D"
    );

    phase_one_proceed
        .send(())
        .expect("release B phase-one publication");
    tokio::time::timeout(
        Duration::from_secs(60),
        probe.wait_until_full_published(expected_jobs),
    )
    .await
    .expect("B exact full publication");
    let output = wait_child_output_bounded(child, Duration::from_secs(60))
        .await
        .expect("pinned clone CLI completed");
    abort_server_task(proxy_task).await;
    assert!(
        output.status.success(),
        "pinned B clone failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("value.txt")).expect("read pinned clone"),
        "B\n"
    );

    let proxy_requests = requests.lock().unwrap().clone();
    assert!(!proxy_requests.is_empty(), "CLI made a ref request");
    assert!(!proxy_requests[0].contains("pinned="));
    assert!(
        proxy_requests
            .iter()
            .skip(1)
            .all(|request| request.contains(&format!("pinned={b}"))),
        "the CLI never returned to the moving branch after pinning: {proxy_requests:?}"
    );
    assert_eq!(
        probe.queue_inserts.load(Ordering::SeqCst),
        expected_jobs,
        "the clone's pinned polls did not admit another job"
    );

    let moving_after = store
        .load_branch(&repo_id, "main")
        .await
        .expect("reload moving row after B")
        .expect("moving row remains present");
    assert_eq!(moving_after.commit, latest_commit);
    assert_eq!(
        serde_json::to_value(&moving_after).expect("serialize moving row after B"),
        moving_json,
        "late B publication cannot move the latest moving row backward"
    );
    assert_eq!(
        artifact_snapshot(&server.storage_dir, &server.cas_dir, &moving_after),
        moving_artifacts,
        "late B publication cannot replace the latest artifacts"
    );
    let exact_b = store
        .load_branch(&repo_id, &format!(":main#{b}"))
        .await
        .expect("load exact B result")
        .expect("exact B result remains addressable");
    assert_eq!(exact_b.commit, b);
    assert_eq!(exact_b.full_clonepack.commit, b);
    assert!(!exact_b.full_clonepack.manifest.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_pin_finishes_b_after_branch_advances_to_c() {
    run_pinned_branch_advance_proof(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continued_branch_advance_never_repins_b_clone() {
    run_pinned_branch_advance_proof(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evicted_pin_rebuilds_b_without_branch_probe() {
    let _guard = env_lock().lock().await;
    init(false);
    let _env = EnvGuard::capture(&["RIPCLONE_RECHECK_MAX", "RIPCLONE_TESTING"]);
    unsafe {
        std::env::set_var("RIPCLONE_RECHECK_MAX", "0");
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "evicted-pin");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/evicted-pin")
        .await
        .expect("register evicted pin repo");
    server
        .client()
        .sync_repo("acme/evicted-pin", None)
        .await
        .expect("publish A");
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    server
        .client()
        .sync_repo("acme/evicted-pin", None)
        .await
        .expect("publish B");
    let c = origin.commit(&[("value.txt", "C\n")], "C");
    origin.publish();
    server
        .client()
        .sync_repo("acme/evicted-pin", None)
        .await
        .expect("publish C");

    let store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&server.repo_root));
    let repo_id = RepoId::github("acme/evicted-pin");
    let exact_key = format!(":main#{b}");
    let exact_b = store
        .load_branch(&repo_id, &exact_key)
        .await
        .expect("load B before eviction")
        .expect("B exact row before eviction");
    assert!(exact_b.internal_exact_result);
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    for key in store.list_branches(&repo_id).await.expect("list refs") {
        let mut info = store.load_branch(&repo_id, &key).await.unwrap().unwrap();
        info.last_accessed_at = Some(if key == exact_key { 1 } else { now + 3600 });
        store.save_branch(&repo_id, &key, &info).await.unwrap();
    }
    let storage: ripclone::storage::StorageRef = Arc::new(RemoteLocalStorage::new(
        ripclone::storage::local(&server.storage_dir).unwrap(),
    ));
    ripclone::remote_gc::RemoteGc::new(
        storage,
        Arc::clone(&store),
        ripclone::remote_gc::GcConfig {
            grace_period: Duration::from_secs(0),
            warm_ttl: Duration::from_secs(1),
            dry_run: false,
        },
    )
    .run()
    .await
    .expect("evict idle ordinary exact B");
    assert_eq!(
        store
            .load_branch(&repo_id, &exact_key)
            .await
            .unwrap()
            .unwrap()
            .build_status
            .as_deref(),
        Some("evicted")
    );
    let moving_before = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load C before exact rebuild")
        .expect("C moving row before exact rebuild");
    assert_eq!(moving_before.commit, c);
    let moving_json = serde_json::to_value(&moving_before).expect("serialize C before rebuild");
    let moving_artifacts = artifact_snapshot(&server.storage_dir, &server.cas_dir, &moving_before);
    let tip_probes_before = probe.tip_probes.load(Ordering::SeqCst);
    let queue_inserts_before = probe.queue_inserts.load(Ordering::SeqCst);
    probe.fetch_entry.arm();
    let response = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/evicted-pin/refs/main?clonepack=full&pinned={b}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("evicted exact lookup");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value = response.json().await.expect("evicted pending response");
    assert_eq!(body["commit"], b);
    tokio::time::timeout(
        Duration::from_secs(20),
        probe.fetch_entry.wait_until_entered(1),
    )
    .await
    .expect("exact B rebuild reached fetch barrier");
    assert_eq!(
        probe.tip_probes.load(Ordering::SeqCst),
        tip_probes_before,
        "evicted exact lookup did not probe moving main"
    );
    assert_eq!(
        probe.queue_inserts.load(Ordering::SeqCst),
        queue_inserts_before + 1,
        "one exact B recovery job was admitted"
    );
    assert_eq!(
        probe
            .fetch_targets
            .lock()
            .unwrap()
            .last()
            .map(String::as_str),
        Some(b.as_str()),
        "recovery fetches B, never the moving C target"
    );
    probe.fetch_entry.release();
    probe.fetch_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(3))
        .await
        .expect("exact B recovery publication");
    let moving_after = store
        .load_branch(&repo_id, "main")
        .await
        .expect("reload C after exact rebuild")
        .expect("C moving row after exact rebuild");
    assert_eq!(moving_after.commit, c);
    assert_eq!(
        serde_json::to_value(&moving_after).expect("serialize C after rebuild"),
        moving_json
    );
    assert_eq!(
        artifact_snapshot(&server.storage_dir, &server.cas_dir, &moving_after),
        moving_artifacts
    );
    let rebuilt = store
        .load_branch(&repo_id, &exact_key)
        .await
        .expect("load rebuilt B")
        .expect("rebuilt B exact row");
    assert_eq!(rebuilt.commit, b);
    assert_eq!(rebuilt.full_clonepack.commit, b);
    assert!(rebuilt.build_status.is_none() || rebuilt.build_status.as_deref() == Some("done"));
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
async fn pinned_release_cli_completes_exact_a_after_branch_moves() {
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
    let branches = store.list_branches(&repo_id).await.expect("list A refs");
    assert!(
        branches
            .iter()
            .any(|branch| branch == &format!(":main#{pinned}")),
        "ordinary sync publishes the exact A result"
    );
    assert!(
        branches
            .iter()
            .all(|branch| branch != &format!(":HEAD#{pinned}")),
        "ordinary sync keeps only the concrete exact key"
    );

    let _env = EnvGuard::capture(&[
        "RIPCLONE_TESTING",
        "RIPCLONE_TEST_REF_MAX_ATTEMPTS",
        "RIPCLONE_TEST_REF_POLL_MS",
    ]);
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }

    let (proxy, entered, proceed, requests, proxy_task) =
        start_ref_barrier_proxy(&server.url, false, true).await;
    let output = tempfile::tempdir().expect("clone output");
    let target = output.path().join("clone");
    let mut command = std::process::Command::new(binary);
    command
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
        .stderr(std::process::Stdio::piped());
    let child = spawn_bounded_child(&mut command).expect("spawn release CLI");
    if !matches!(
        tokio::time::timeout(Duration::from_secs(20), entered).await,
        Ok(Ok(()))
    ) {
        let output = wait_child_output_bounded(child, Duration::from_secs(1)).await;
        abort_server_task(proxy_task).await;
        panic!("release CLI never reached moving-response barrier: {output:?}");
    }

    // The spawned CLI captured the bounded polling configuration. Restore the
    // server-side sync client to its normal wait budget before the
    // independent B sync.
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
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
    abort_server_task(proxy_task).await;
    assert!(output.status.success(), "exact A remains clonable after B");
    assert_eq!(
        std::fs::read_to_string(target.join("value.txt")).expect("read exact A"),
        "A\n"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("pending"),
        "exact A must not remain pending: {stderr}"
    );
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
async fn exact_ready_commit_bypasses_stale_moving_projection_for_clone_and_sync() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "exact-ready-return");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/exact-ready-return")
        .await
        .expect("register exact-ready fixture");
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let ready_b = sync_until_archive_ready(&server, "acme", "exact-ready-return").await;
    assert_eq!(ready_b.commit, b);

    let c = origin.commit(&[("value.txt", "C\n")], "C");
    origin.publish();
    let ready_c = sync_until_archive_ready(&server, "acme", "exact-ready-return").await;
    assert_eq!(ready_c.commit, c);
    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/exact-ready-return");
    let moving_c = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load moving C")
        .expect("moving C row");
    assert_eq!(moving_c.commit, c);
    let moving_c_json = serde_json::to_value(&moving_c).expect("serialize moving C");

    git(&origin.work, &["reset", "--hard", &b]);
    origin.publish();
    let _test_env = EnvGuard::capture(&["RIPCLONE_TESTING"]);
    unsafe { std::env::set_var("RIPCLONE_TESTING", "1") };
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let sync = server
        .client()
        .sync_repo("acme/exact-ready-return", None)
        .await
        .expect("exact-ready B sync");
    assert_eq!(sync.commit, b);

    let output_root = tempfile::tempdir().expect("exact-ready output");
    let target = output_root.path().join("clone");
    let mut command = std::process::Command::new(selected_cli_binary());
    command
        .arg("--server")
        .arg(&server.url)
        .arg("clone")
        .arg("acme/exact-ready-return")
        .arg(&target)
        .arg("--depth")
        .arg("0")
        .arg("--no-metrics")
        .arg("--verify-upstream=never")
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_NO_METRICS", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = spawn_bounded_child(&mut command).expect("spawn exact-ready release CLI");
    let output = wait_child_output_bounded(child, Duration::from_secs(60))
        .await
        .expect("exact-ready release CLI completed");
    assert!(
        output.status.success(),
        "exact-ready release clone failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(target.join("value.txt")).unwrap(),
        "B\n"
    );
    assert_eq!(probe.tip_probes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.enqueue_attempts.load(Ordering::SeqCst), 0);
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), 0);
    assert_eq!(probe.exact_fetches.load(Ordering::SeqCst), 0);
    assert_eq!(probe.builder_entries.load(Ordering::SeqCst), 0);
    let moving_after = store
        .load_branch(&repo_id, "main")
        .await
        .expect("reload moving C")
        .expect("moving C remains");
    assert_eq!(
        serde_json::to_value(moving_after).expect("serialize moving C after reads"),
        moving_c_json,
        "exact-ready B reads do not rewrite moving C"
    );
}

#[tokio::test]
async fn warm_ttl_collects_superseded_exact_result_without_deleting_current_shared_artifacts() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "exact-result-gc");
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    register_added_without_build(&server, "acme/exact-result-gc")
        .await
        .expect("register exact GC fixture");
    assert_eq!(
        sync_until_archive_ready(&server, "acme", "exact-result-gc")
            .await
            .commit,
        b
    );
    let c = origin.commit(&[("value.txt", "C\n")], "C");
    origin.publish();
    assert_eq!(
        sync_until_archive_ready(&server, "acme", "exact-result-gc")
            .await
            .commit,
        c
    );

    let store: Arc<dyn RefStore> = Arc::new(FileRefStore::new(&server.repo_root));
    let repo_id = RepoId::github("acme/exact-result-gc");
    let exact_b_key = format!(":main#{b}");
    let exact_b = store
        .load_branch(&repo_id, &exact_b_key)
        .await
        .expect("load exact B")
        .expect("exact B row");
    let moving_c = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load moving C")
        .expect("moving C row");
    assert!(exact_b.internal_exact_result);
    assert_eq!(moving_c.commit, c);
    let mut b_hashes = BTreeSet::new();
    let mut c_hashes = BTreeSet::new();
    content_hashes(&serde_json::to_value(&exact_b).unwrap(), &mut b_hashes);
    content_hashes(&serde_json::to_value(&moving_c).unwrap(), &mut c_hashes);
    let b_only: Vec<_> = b_hashes.difference(&c_hashes).cloned().collect();
    let shared: Vec<_> = b_hashes.intersection(&c_hashes).cloned().collect();
    assert!(!b_only.is_empty(), "B fixture needs exact-only artifacts");
    assert!(!shared.is_empty(), "B and C fixture needs shared artifacts");

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    for key in store.list_branches(&repo_id).await.expect("list GC refs") {
        let mut info = store.load_branch(&repo_id, &key).await.unwrap().unwrap();
        info.last_accessed_at = Some(if key == exact_b_key { 1 } else { now + 3600 });
        store.save_branch(&repo_id, &key, &info).await.unwrap();
    }
    let storage: ripclone::storage::StorageRef = Arc::new(RemoteLocalStorage::new(
        ripclone::storage::local(&server.storage_dir).unwrap(),
    ));
    let gc = ripclone::remote_gc::RemoteGc::new(
        storage,
        Arc::clone(&store),
        ripclone::remote_gc::GcConfig {
            grace_period: Duration::from_secs(0),
            warm_ttl: Duration::from_secs(1),
            dry_run: false,
        },
    );
    gc.run().await.expect("collect superseded exact B");

    let exact_b = store
        .load_branch(&repo_id, &exact_b_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact_b.build_status.as_deref(), Some("evicted"));
    assert!(
        b_only
            .iter()
            .all(|hash| !server.storage_path(hash).exists())
    );
    assert!(shared.iter().all(|hash| server.storage_path(hash).exists()));
    let moving_c = store.load_branch(&repo_id, "main").await.unwrap().unwrap();
    assert_eq!(moving_c.commit, c);
    assert_ne!(moving_c.build_status.as_deref(), Some("evicted"));
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
    let mut install = tokio::spawn(async move {
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
    let error = match tokio::time::timeout(Duration::from_secs(20), &mut install).await {
        Ok(joined) => joined
            .expect("join pending install")
            .expect_err("exact A remains pending"),
        Err(_) => {
            install.abort();
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut install).await;
            panic!("pending install did not finish within 20 seconds");
        }
    };
    abort_server_task(proxy_task).await;
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
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
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
async fn service_unavailable_switches_to_exact_only_after_a_pin_exists() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }
    let unavailable = || (StatusCode::SERVICE_UNAVAILABLE, json!({"error": "busy"}));
    let exact_unavailable = || {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "busy", "commit": A, "branch": "main"}),
        )
    };

    let (url, pre_pin_requests, pre_pin_task) =
        scripted_server(vec![unavailable(), ready(A)]).await;
    Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
        .await
        .expect("pre-pin 503 may retry moving selector");
    abort_server_task(pre_pin_task).await;
    {
        let pre_pin_requests = pre_pin_requests.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            pre_pin_requests
                .iter()
                .all(|request| !request.contains("pinned="))
        );
    }

    let (url, post_pin_requests, post_pin_task) =
        scripted_server(vec![pending(A), exact_unavailable(), ready(A)]).await;
    Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
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
            .resolve_ref_with_clonepack("acme/demo", "HEAD", Some("full"), Some("HEAD~1"))
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
async fn sha_suffixed_real_branch_resolves_reports_polls_and_advances() {
    let _guard = env_lock().lock().await;
    init(false);
    let _env = EnvGuard::capture(&["RIPCLONE_TESTING"]);
    unsafe { std::env::set_var("RIPCLONE_TESTING", "1") };
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server_env(&[("RIPCLONE_POLL_INTERVAL_SECS", "1")]).await;
    let origin = make_origin("acme", "delimiter-branch");
    let branch = "release#0123456789abcdef0123456789abcdef01234567";
    assert!(
        std::process::Command::new("git")
            .args(["check-ref-format", &format!("refs/heads/{branch}")])
            .status()
            .expect("validate SHA-suffixed branch")
            .success(),
        "SHA-suffixed branch must be a valid Git ref"
    );
    git(&origin.work, &["branch", "-m", branch]);
    let commit = origin.commit(&[("value.txt", "A\n")], "A");
    git(
        &origin.work,
        &["push", "-q", "--force", origin.bare_str(), branch],
    );
    git(
        &origin.bare,
        &["symbolic-ref", "HEAD", &format!("refs/heads/{branch}")],
    );
    register_added_without_build(&server, "acme/delimiter-branch")
        .await
        .expect("register delimiter branch repo");
    let settled = sync_until_archive_ready(&server, "acme", "delimiter-branch").await;
    assert_eq!(settled.commit, commit);
    let ready = server
        .client()
        .resolve_ref_with_clonepack("acme/delimiter-branch", "HEAD", Some("full"), None)
        .await
        .expect("delimiter branch is ready before forcing the 202");
    assert_eq!(ready.commit, commit);
    assert_eq!(ready.branch, branch);
    let status = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/delimiter-branch/status",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("status for delimiter branch");
    assert_eq!(status.status(), StatusCode::OK);
    let status: serde_json::Value = status.json().await.expect("delimiter status response");
    assert!(
        status["refs"]
            .as_array()
            .expect("status refs")
            .iter()
            .any(|entry| entry["branch"] == branch && entry["commit"] == commit),
        "a SHA-suffixed real source branch must remain visible in status: {status:?}"
    );
    let branch_status = status["refs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["branch"] == branch)
        .expect("SHA-suffixed branch status entry");
    assert!(branch_status["bytes"].as_u64().unwrap() > 0);
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }

    let (proxy, entered, proceed, requests, proxy_task) =
        start_ref_barrier_proxy(&server.url, false, true).await;
    let resolve = tokio::spawn(async move {
        Client::new_with_token(proxy, Some(token_hash()))
            .resolve_ref_with_clonepack("acme/delimiter-branch", "HEAD", Some("full"), None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("real ready response reached pending barrier")
        .expect("pending barrier alive");
    proceed.send(()).expect("release forced pending response");
    let response = tokio::time::timeout(Duration::from_secs(20), resolve)
        .await
        .expect("delimiter exact poll completed")
        .expect("join delimiter exact poll")
        .expect("resolve delimiter branch");
    abort_server_task(proxy_task).await;

    assert_eq!(response.commit, commit);
    assert_eq!(response.branch, branch);
    let requests = requests.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(requests.len(), 2, "one moving and one exact request");
    assert!(requests[0].contains("/refs/HEAD?"));
    assert!(
        requests[1].contains(&format!("/refs/{}?", urlencoding::encode(branch))),
        "delimiter must be part of the encoded path, not a fragment: {requests:?}"
    );
    assert!(requests[1].contains(&format!("pinned={commit}")));

    let queue_before = probe.queue_inserts.load(Ordering::SeqCst);
    let fetches_before = probe
        .fetch_targets
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .len();
    let builders_before = probe
        .builder_targets
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .len();
    let next = origin.commit(&[("value.txt", "B\n")], "B");
    git(
        &origin.work,
        &["push", "-q", "--force", origin.bare_str(), branch],
    );

    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/delimiter-branch");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if matches!(
                store.load_branch(&repo_id, branch).await,
                Ok(Some(info))
                    if info.commit == next
                        && info.build_status.is_none()
                        && info.full_clonepack.commit == next
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("polling admits and completes the SHA-suffixed branch advance");

    assert_eq!(
        probe.queue_inserts.load(Ordering::SeqCst) - queue_before,
        1,
        "HEAD and concrete polling coalesce to one immutable job"
    );
    let fetch_targets = probe
        .fetch_targets
        .lock()
        .unwrap_or_else(|error| error.into_inner())[fetches_before..]
        .to_vec();
    assert_eq!(fetch_targets.as_slice(), std::slice::from_ref(&next));
    let builder_targets = probe
        .builder_targets
        .lock()
        .unwrap_or_else(|error| error.into_inner())[builders_before..]
        .to_vec();
    assert_eq!(builder_targets.as_slice(), std::slice::from_ref(&next));

    let advanced = server
        .client()
        .resolve_ref_with_clonepack("acme/delimiter-branch", "HEAD", Some("full"), None)
        .await
        .expect("advanced SHA-suffixed branch resolves through HEAD");
    assert_eq!(advanced.branch, branch);
    assert_eq!(advanced.commit, next);
    let final_status = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/delimiter-branch/status",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("final SHA-suffixed status")
        .error_for_status()
        .expect("final status 2xx")
        .json::<serde_json::Value>()
        .await
        .expect("final status JSON");
    assert!(
        final_status["refs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| {
                entry["branch"] == branch
                    && entry["commit"] == next
                    && entry["bytes"].as_u64() > Some(0)
            })
    );
    assert!(
        final_status["refs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["branch"] != "release"),
        "the real SHA-suffixed branch must not be reinterpreted as another branch's history"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
}

#[tokio::test]
async fn exact_main_b_and_real_main_hash_b_branch_do_not_collide() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "exact-key-collision");
    let b = origin.commit(&[("value.txt", "main B\n")], "main B");
    origin.publish();
    register_added_without_build(&server, "acme/exact-key-collision")
        .await
        .expect("register collision fixture");
    let main = server
        .client()
        .sync_branch("acme/exact-key-collision", "main")
        .await
        .expect("build main B");
    assert_eq!(main.commit, b);

    let colliding_branch = format!("main#{b}");
    assert!(
        std::process::Command::new("git")
            .args([
                "check-ref-format",
                &format!("refs/heads/{colliding_branch}")
            ])
            .status()
            .expect("validate colliding branch")
            .success()
    );
    git(&origin.work, &["checkout", "-q", "-b", &colliding_branch]);
    let branch_commit = origin.commit(&[("value.txt", "real branch\n")], "real branch");
    git(
        &origin.work,
        &[
            "push",
            "-q",
            "--force",
            origin.bare_str(),
            &colliding_branch,
        ],
    );
    git(&origin.work, &["checkout", "-q", "main"]);

    let branch = server
        .client()
        .sync_branch("acme/exact-key-collision", &colliding_branch)
        .await
        .expect("build real colliding branch");
    assert_eq!(branch.commit, branch_commit);
    assert_eq!(branch.branch, colliding_branch);

    let repo_id = RepoId::github("acme/exact-key-collision");
    let store = FileRefStore::new(&server.repo_root);
    let exact_main = store
        .load_branch(&repo_id, &exact_ref_key("main", &b))
        .await
        .expect("load exact main B")
        .expect("exact main B exists");
    assert_eq!(exact_main.commit, b);
    assert!(exact_main.internal_exact_result);
    let public_branch = store
        .load_branch(&repo_id, &colliding_branch)
        .await
        .expect("load real colliding branch")
        .expect("real colliding branch exists");
    assert_eq!(public_branch.commit, branch_commit);
    assert!(!public_branch.internal_exact_result);
    let exact_branch = store
        .load_branch(&repo_id, &exact_ref_key(&colliding_branch, &branch_commit))
        .await
        .expect("load exact real-branch result")
        .expect("exact real-branch result exists");
    assert_eq!(exact_branch.commit, branch_commit);
    assert!(exact_branch.internal_exact_result);

    let status = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/exact-key-collision/status",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("collision status")
        .error_for_status()
        .expect("collision status 2xx")
        .json::<serde_json::Value>()
        .await
        .expect("collision status JSON");
    let public_refs = status["refs"].as_array().expect("public refs");
    assert!(
        public_refs
            .iter()
            .any(|entry| entry["branch"] == "main" && entry["commit"] == b)
    );
    assert!(
        public_refs.iter().any(|entry| {
            entry["branch"] == colliding_branch && entry["commit"] == branch_commit
        })
    );
    assert!(public_refs.iter().all(|entry| {
        !entry["branch"]
            .as_str()
            .unwrap_or_default()
            .starts_with(':')
    }));

    let binary = selected_cli_binary();
    for (branch, expected) in [
        ("main", "main B\n"),
        (colliding_branch.as_str(), "real branch\n"),
    ] {
        let output = tempfile::tempdir().expect("clone output");
        let target = output.path().join("clone");
        let mut command = std::process::Command::new(&binary);
        command
            .arg("--server")
            .arg(&server.url)
            .arg("clone")
            .arg("acme/exact-key-collision")
            .arg(&target)
            .args([
                "--branch",
                branch,
                "--depth",
                "0",
                "--verify-upstream=never",
                "--no-metrics",
            ])
            .env("RIPCLONE_SERVER_TOKEN", TOKEN)
            .env("RIPCLONE_NO_METRICS", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let child = spawn_bounded_child(&mut command).expect("spawn collision clone");
        let output = wait_child_output_bounded(child, Duration::from_secs(30))
            .await
            .expect("collision clone completed");
        assert!(
            output.status.success(),
            "clone {branch} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(target.join("value.txt")).expect("installed value"),
            expected
        );
    }
}

#[tokio::test]
async fn ready_ref_requires_http_200() {
    let _guard = env_lock().lock().await;
    let (_, ready_body) = ready(A);
    let (url, requests, task) = scripted_server(vec![(StatusCode::CREATED, ready_body)]).await;
    let error = Client::new(url)
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
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
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
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
        .resolve_ref_with_clonepack("acme/demo", "main", Some("full"), None)
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
    let exact_key = format!("main#{a}");
    let mut valid_a = None;
    for _ in 0..200 {
        if let Ok(Some(info)) = store.load_branch(&repo_id, &exact_key).await
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
        .save_branch(&repo_id, &exact_key, &mismatched)
        .await
        .expect("publish target-A/artifact-B exact row");

    let http = reqwest::Client::new();
    let request = || {
        http.get(format!(
            "{}/v1/repos/github/acme/guarded-cache/refs/main?clonepack=full",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
    };
    for attempt in 1..=2 {
        let rejected = request().send().await.expect("mismatched lookup");
        assert_eq!(
            rejected.status(),
            StatusCode::ACCEPTED,
            "stored mismatch request {attempt} must remain pending"
        );
        let rejected: serde_json::Value = rejected.json().await.expect("pending response");
        assert_eq!(rejected["code"], "artifact_pending");
        assert_eq!(rejected["commit"], a);
    }

    store
        .save_branch(&repo_id, &exact_key, &valid_a)
        .await
        .expect("restore guarded exact A row");
    let ready = request().send().await.expect("guarded lookup");
    assert_eq!(ready.status(), StatusCode::OK);
    let ready: serde_json::Value = ready.json().await.expect("ready A response");
    assert_eq!(ready["commit"], a);

    // Change the durable row out of band after the successful response. The
    // next lookup is a real response-cache hit and must contain only guarded A,
    // never the earlier rejected target-A/artifact-B snapshot.
    store
        .save_branch(&repo_id, &exact_key, &mismatched)
        .await
        .expect("restore mismatched durable exact row");
    let cached = request().send().await.expect("cached lookup");
    assert_eq!(cached.status(), StatusCode::OK);
    let cached: serde_json::Value = cached.json().await.expect("cached A response");
    assert_eq!(
        cached, ready,
        "cache hit must retain the guarded A snapshot"
    );
}

#[tokio::test]
async fn cached_mismatched_manifest_is_rejected_on_every_use() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "manifest-cache-guard");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/manifest-cache-guard")
        .await
        .expect("register manifest fixture");
    server
        .client()
        .sync_repo("acme/manifest-cache-guard", None)
        .await
        .expect("sync manifest fixture");
    let (pinned, bad_manifest) =
        replace_full_manifest_commit(&server, "acme/manifest-cache-guard", B).await;

    let root = tempfile::tempdir().expect("manifest test root");
    let cache_dir = root.path().join("cache");
    let target = root.path().join("clone");
    let client =
        Client::new_with_token_and_cache(server.url.clone(), Some(token_hash()), Some(&cache_dir));
    let first = client
        .install_repo_with_mode_at(
            "acme/manifest-cache-guard",
            "main",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("manifest B must be rejected under pin A");
    let first = format!("{first:#}");
    assert!(first.contains(&format!("manifest commit {B}")));
    assert!(first.contains(&format!("pinned commit {pinned}")));
    assert!(
        !target.exists(),
        "integrity failure must not publish a target"
    );
    let cache = ripclone::cas::Cas::new(&cache_dir).expect("open client cache");
    assert!(
        cache.get(&bad_manifest).is_ok(),
        "hash-valid immutable bytes may remain in the artifact CAS"
    );

    // Remove the only network copy. A second attempt now necessarily reads the
    // immutable cached bytes and must repeat the per-use semantic rejection.
    std::fs::remove_file(server.storage_path(&bad_manifest))
        .expect("remove mismatched manifest from test storage");
    let second = client
        .install_repo_with_mode_at(
            "acme/manifest-cache-guard",
            "main",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("cached manifest B must still be rejected under pin A");
    let second = format!("{second:#}");
    assert!(
        second.contains(&format!("manifest commit {B}"))
            && second.contains(&format!("pinned commit {pinned}")),
        "cached bytes bypassed per-use identity validation: {second}"
    );
    assert!(!target.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn worktree_rejects_mismatched_manifest_before_git_registration() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "worktree-manifest-guard");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/worktree-manifest-guard")
        .await
        .expect("register worktree fixture");
    server
        .client()
        .sync_repo("acme/worktree-manifest-guard", None)
        .await
        .expect("sync worktree fixture");
    let (pinned, _) =
        replace_full_manifest_commit(&server, "acme/worktree-manifest-guard", B).await;

    let root = tempfile::tempdir().expect("worktree root");
    let main_repo = root.path().join("main");
    let clone_status = std::process::Command::new("git")
        .args([
            "clone",
            origin.bare.to_str().unwrap(),
            main_repo.to_str().unwrap(),
        ])
        .status()
        .expect("clone main worktree fixture");
    assert!(clone_status.success());
    let target = root.path().join("linked");
    let error = server
        .client()
        .add_worktree("acme/worktree-manifest-guard", "main", &main_repo, &target)
        .await
        .expect_err("worktree must reject manifest B under pin A");
    let error = format!("{error:#}");
    assert!(error.contains(&format!("manifest commit {B}")));
    assert!(error.contains(&format!("pinned commit {pinned}")));
    assert!(!target.exists());
    let registrations = git(&main_repo, &["worktree", "list", "--porcelain"]);
    assert!(!registrations.contains(target.to_str().unwrap()));
}

#[tokio::test]
async fn cancelling_real_clone_waits_for_midx_writer_before_staging_cleanup() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "cancel-midx");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/cancel-midx")
        .await
        .expect("register cancellation fixture");
    server
        .client()
        .sync_repo("acme/cancel-midx", None)
        .await
        .expect("sync cancellation fixture");

    // Incremental shallow manifests may omit the pregenerated MIDX when their
    // base packs are remote. Reproduce that current shape so the public install
    // path reaches its blocking local MIDX writer after worktree staging.
    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/cancel-midx");
    let mut info = None;
    for _ in 0..200 {
        if let Ok(Some(candidate)) = store.load_branch(&repo_id, "main").await
            && candidate.build_status.is_none()
            && !candidate.full_clonepack.manifest.is_empty()
        {
            info = Some(candidate);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let moving = info.expect("full cancellation fixture settled");
    let exact_key = format!("main#{}", moving.commit);
    let mut info = store
        .load_branch(&repo_id, &exact_key)
        .await
        .expect("load exact cancellation fixture")
        .expect("exact cancellation fixture exists");
    let storage = ripclone::storage::local(&server.storage_dir).expect("open test storage");
    let bytes = storage
        .get(&info.full_clonepack.manifest)
        .expect("read full manifest");
    let mut manifest = ripclone::clonepack::ClonepackManifest::decode(bytes.as_slice())
        .expect("decode full manifest");
    manifest.midx = None;
    let bytes = manifest.encode_to_vec();
    let hash = ripclone::cas::hash(&bytes);
    storage.put(&hash, &bytes).expect("write no-MIDX manifest");
    info.full_clonepack.manifest = hash.clone();
    info.full_clonepack.midx.clear();
    store
        .save_branch(&repo_id, &exact_key, &info)
        .await
        .expect("publish no-MIDX exact fixture");

    let root = tempfile::tempdir().expect("cancellation output");
    let wrapper_dir = root.path().join("bin");
    std::fs::create_dir_all(&wrapper_dir).unwrap();
    let entered = root.path().join("entered");
    let proceed = root.path().join("proceed");
    let real_git = String::from_utf8(
        std::process::Command::new("sh")
            .args(["-c", "command -v git"])
            .output()
            .expect("locate git")
            .stdout,
    )
    .expect("git path utf8")
    .trim()
    .to_string();
    let wrapper = wrapper_dir.join("git");
    std::fs::write(
        &wrapper,
        r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "multi-pack-index" ]; then
    : >"$RIPCLONE_TEST_MIDX_ENTERED"
    waited=0
    while [ ! -f "$RIPCLONE_TEST_MIDX_PROCEED" ]; do
      if [ "$waited" -ge 400 ]; then exit 124; fi
      sleep 0.05
      waited=$((waited + 1))
    done
    break
  fi
done
exec "$RIPCLONE_TEST_REAL_GIT" "$@"
"#,
    )
    .expect("write MIDX wrapper");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let original_path = std::env::var_os("PATH").unwrap_or_default();
    let _env_guard = EnvGuard::capture(&[
        "PATH",
        "RIPCLONE_TEST_REAL_GIT",
        "RIPCLONE_TEST_MIDX_ENTERED",
        "RIPCLONE_TEST_MIDX_PROCEED",
    ]);
    unsafe {
        std::env::set_var(
            "PATH",
            format!(
                "{}:{}",
                wrapper_dir.display(),
                original_path.to_string_lossy()
            ),
        );
        std::env::set_var("RIPCLONE_TEST_REAL_GIT", &real_git);
        std::env::set_var("RIPCLONE_TEST_MIDX_ENTERED", &entered);
        std::env::set_var("RIPCLONE_TEST_MIDX_PROCEED", &proceed);
    }

    let target = root.path().join("clone");
    let target_for_install = target.clone();
    let client = server.client();
    let mut install = tokio::spawn(async move {
        client
            .install_repo_with_mode_at(
                "acme/cancel-midx",
                "main",
                None,
                &target_for_install,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), async {
        while !entered.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real clone reached blocking MIDX fallback");

    install.abort();
    tokio::time::timeout(Duration::from_secs(2), &mut install)
        .await
        .expect("cancelled public install task joined")
        .expect_err("public install was cancelled");
    let staged = std::fs::read_dir(root.path())
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("clone.") && name.ends_with(".tmp"))
        })
        .expect("staging remains while cancelled blocking writer runs");
    assert!(!target.exists());
    std::fs::write(&proceed, b"go").expect("release MIDX writer");
    tokio::time::timeout(Duration::from_secs(5), async {
        while staged.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cancellation reaper removed staging after writer exit");
    assert!(!target.exists());
}

#[tokio::test]
async fn overwritten_branch_metadata_keeps_exact_pin_addressable_without_upstream() {
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

    // The install has already captured its bounded two-attempt poll config.
    // Let the independent sync use its normal build wait, then
    // restore the short config before releasing the pinned install.
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    assert_ne!(a, b);
    server
        .client()
        .sync_repo("acme/overwritten-pin", None)
        .await
        .expect("sync B");
    unsafe {
        std::env::set_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS", "2");
        std::env::set_var("RIPCLONE_TEST_REF_POLL_MS", "0");
    }

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
    let outcome = match tokio::time::timeout(Duration::from_secs(20), &mut install).await {
        Ok(joined) => joined
            .expect("join overwritten install")
            .expect("exact A remains addressable after moving B"),
        Err(_) => {
            install.abort();
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut install).await;
            panic!("overwritten metadata install did not finish within 20 seconds");
        }
    };
    abort_server_task(proxy_task).await;
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_TEST_REF_MAX_ATTEMPTS");
        std::env::remove_var("RIPCLONE_TEST_REF_POLL_MS");
    }
    assert_eq!(outcome.commit, a);
    assert_eq!(outcome.mode, "files");
    assert!(target.exists(), "exact A must publish a target");
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
        requests.len() - 1,
        "each concrete-branch pinned lookup performs one exact-row point read"
    );
    assert_eq!(observed.enqueues, 0);
    assert_eq!(observed.builder_entries, 0);
}

#[tokio::test]
async fn pinned_lookup_serves_exact_a_while_phase_one_b_is_paused() {
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
        .load_branch(&repo_id, &format!(":main#{a}"))
        .await
        .expect("load A")
        .expect("A row");
    assert_eq!(exact_a.commit, a);
    assert!(
        store
            .load_branch(&repo_id, &format!(":main#{a}"))
            .await
            .expect("load exact A")
            .is_some(),
        "ordinary A publication creates the exact result"
    );
    let pinned_url = format!(
        "{}/v1/repos/github/acme/phase-one-pin/refs/main?clonepack=full&pinned={a}",
        server.url
    );
    let exact_snapshot = reqwest::Client::new()
        .get(&pinned_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("baseline exact A lookup");
    assert_eq!(exact_snapshot.status(), StatusCode::OK);
    let exact_snapshot: serde_json::Value =
        exact_snapshot.json().await.expect("baseline A response");
    assert_eq!(exact_snapshot["commit"], a);

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

    let moving_a = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load moving row while B is paused")
        .expect("moving A row");
    assert_eq!(moving_a.commit, a, "moving main must not publish B yet");
    let exact_b = store
        .load_branch(&repo_id, &format!("main#{b}"))
        .await
        .expect("load paused exact B")
        .expect("paused exact B row");
    assert_eq!(exact_b.commit, b, "exact B must be durable at the barrier");
    assert_eq!(exact_b.full_clonepack.commit, a);
    assert_ne!(
        exact_b
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

    // Pinning B with the explicit Full top-up opt-in derives A solely from the
    // carried manifest. Exact Full(B) is still blocked, so this remains a 202
    // whose public identity is B.
    let top_up = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/phase-one-pin/refs/main?clonepack=full&pinned={b}&top_up=true",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("top-up lookup while B phase one is paused");
    assert_eq!(top_up.status(), StatusCode::ACCEPTED);
    let top_up: serde_json::Value = top_up.json().await.expect("top-up pending response");
    assert_eq!(top_up["code"], "artifact_pending");
    assert_eq!(top_up["commit"], b);
    assert_eq!(top_up["top_up_supported"], true);
    assert_eq!(top_up["top_up_base"]["commit"], a);
    assert_eq!(
        top_up["top_up_base"]["clonepack_manifest"],
        exact_b.full_clonepack.manifest
    );
    assert_ne!(
        top_up["top_up_base"]["metadata_chunk"], exact_b.shallow_clonepack.metadata_chunk,
        "the response must not mix B's shallow metadata into carried A"
    );

    let probe = server
        .pinned_path_probe
        .as_ref()
        .expect("pinned-path test adapter");
    probe.arm();
    let response = reqwest::Client::new()
        .get(&pinned_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("pinned lookup while B phase one is paused");
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.expect("exact A response");
    assert_eq!(body["commit"], a);
    let exact_observed = probe.snapshot();
    assert_eq!(
        exact_observed.branch_reads, 1,
        "pinned lookup reads only the exact A key"
    );
    assert_eq!(exact_observed.enqueues, 0);
    assert_eq!(exact_observed.builder_entries, 0);

    // Repeat the lookup while B is paused to prove the exact A row remains
    // authoritative instead of depending on B's carried Full(A).
    probe.arm();
    let pending = reqwest::Client::new()
        .get(&pinned_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("repeated exact lookup while B phase one is paused");
    assert_eq!(pending.status(), StatusCode::OK);
    let pending: serde_json::Value = pending.json().await.expect("exact A response");
    assert_eq!(pending["commit"], a);
    let repeated_observed = probe.snapshot();
    assert_eq!(repeated_observed.branch_reads, 1);
    assert_eq!(repeated_observed.enqueues, 0);
    assert_eq!(repeated_observed.builder_entries, 0);

    proceed.send(()).expect("release B phase-one publication");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("sync B completed after barrier release")
        .expect("join sync B")
        .expect("sync B");
    let moving_b = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load moving B after release")
        .expect("moving B row");
    assert_eq!(moving_b.commit, b);
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
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
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
async fn exact_lookup_never_substitutes_the_other_clonepack_variant() {
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
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
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
        let settled = sync_until_archive_ready(&server, "acme", &repo).await;
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
        assert_eq!(pinned, settled.commit);
        let (proxy, entered, proceed, requests, proxy_task) =
            start_ref_barrier_proxy(&server.url, false, false).await;
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
            .expect("publish B through moving ref");
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
