//! End-to-end coverage for the CLI's post-clone metrics report.
//!
//! These are real round-trips, not mocks. A real in-process `ripclone` server
//! builds real artifacts; a thin local gateway sits in front of it and — exactly
//! like a hosted gateway — injects the `X-Ripclone-Clone-Id` header on
//! the ref-resolve response and accepts the CLI's fire-and-forget metrics POST.
//! The real `Client` clones through the gateway and then reports, so we exercise
//! the actual capture-header → build-payload → POST path.
//!
//! Covered:
//!   1. A server that returns `X-Ripclone-Clone-Id` ⇒ the CLI POSTs the metric
//!      with the right body.
//!   2. No header (self-host / older server) ⇒ no POST at all.
//!   3. A failing metrics endpoint ⇒ the clone still succeeds (report swallows it).

mod common;

use common::*;

use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    response::Response,
    routing::any,
};
use ripclone::client::Client;
use ripclone::mode::CloneMode;

/// Hop-by-hop headers we must not blindly copy when re-emitting a proxied
/// response with a fully-buffered body (axum recomputes content-length).
const SKIP_RESP_HEADERS: &[&str] = &[
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
];

#[derive(Clone)]
struct GwState {
    /// Base URL of the real ripclone server we proxy to.
    upstream: String,
    http: reqwest::Client,
    /// Bodies of every metrics POST the gateway received.
    captured: Arc<Mutex<Vec<serde_json::Value>>>,
    /// `Authorization` header of every metrics POST (None if absent). The cloud
    /// rejects an unauthenticated post with a silent 401, so this is load-bearing.
    auth_headers: Arc<Mutex<Vec<Option<String>>>>,
    /// Count of metrics POSTs (even ones with an unparseable body).
    hits: Arc<Mutex<u32>>,
    /// Inject `X-Ripclone-Clone-Id` on ref-resolve responses (the cloud does;
    /// a self-hosted server does not).
    inject_clone_id: bool,
    clone_id: String,
    /// HTTP status the metrics endpoint returns (202 = accepted, 500 = failure).
    metrics_status: u16,
}

/// Capture the CLI's metrics POST and return the configured status.
async fn metrics_handler(State(st): State<GwState>, req: Request) -> Response {
    let auth = req
        .headers()
        .get(reqwest::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = axum::body::to_bytes(req.into_body(), 1 << 20)
        .await
        .unwrap_or_default();
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
        st.captured.lock().unwrap().push(v);
    }
    st.auth_headers.lock().unwrap().push(auth);
    *st.hits.lock().unwrap() += 1;
    Response::builder()
        .status(st.metrics_status)
        .body(Body::empty())
        .unwrap()
}

/// Proxy everything else to the real server, adding the clone-id header to a
/// successful ref-resolve response when configured.
async fn proxy(State(st): State<GwState>, req: Request) -> Response {
    let method = req.method().clone();
    let path_q = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_default();
    let is_ref = req.uri().path().contains("/refs/");

    let mut headers = req.headers().clone();
    headers.remove(reqwest::header::HOST);
    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap_or_default();

    let url = format!("{}{}", st.upstream, path_q);
    let resp = st
        .http
        .request(method, &url)
        .headers(headers)
        .body(body.to_vec())
        .send()
        .await;

    match resp {
        Ok(r) => {
            let status = r.status();
            let mut builder = Response::builder().status(status.as_u16());
            for (k, v) in r.headers() {
                if SKIP_RESP_HEADERS.contains(&k.as_str()) {
                    continue;
                }
                builder = builder.header(k, v);
            }
            if is_ref && status.is_success() && st.inject_clone_id {
                builder = builder.header("X-Ripclone-Clone-Id", &st.clone_id);
            }
            let bytes = r.bytes().await.unwrap_or_default();
            builder.body(Body::from(bytes)).unwrap()
        }
        Err(e) => Response::builder()
            .status(502)
            .body(Body::from(format!("proxy error: {e}")))
            .unwrap(),
    }
}

struct Gateway {
    url: String,
    captured: Arc<Mutex<Vec<serde_json::Value>>>,
    auth_headers: Arc<Mutex<Vec<Option<String>>>>,
    hits: Arc<Mutex<u32>>,
    clone_id: String,
}

/// Spin up the gateway in front of `upstream` and return its URL plus the
/// capture buffers.
async fn spawn_gateway(upstream: &str, inject_clone_id: bool, metrics_status: u16) -> Gateway {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let auth_headers = Arc::new(Mutex::new(Vec::new()));
    let hits = Arc::new(Mutex::new(0u32));
    let clone_id = "test-clone-id-0001".to_string();
    let state = GwState {
        upstream: upstream.to_string(),
        http: reqwest::Client::new(),
        captured: captured.clone(),
        auth_headers: auth_headers.clone(),
        hits: hits.clone(),
        inject_clone_id,
        clone_id: clone_id.clone(),
        metrics_status,
    };
    let app = Router::new()
        .route("/v1/clones/{cloneId}/metrics", any(metrics_handler))
        .fallback(any(proxy))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Gateway {
        url: format!("http://{addr}"),
        captured,
        auth_headers,
        hits,
        clone_id,
    }
}

/// A client pointed at the gateway, authenticated with the shared test token.
fn gateway_client(gw_url: &str) -> Client {
    Client::new_with_token(gw_url.to_string(), Some(token_hash())).with_provider("github")
}

/// Sync a fresh single-commit repo on the real server and return (owner, repo).
async fn seed_repo(server: &Server, owner: &str, repo: &str) -> String {
    let origin = make_origin(owner, repo);
    let commit = origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo(&format!("{owner}/{repo}"))
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo(&format!("{owner}/{repo}"), None)
        .await
        .expect("sync");
    commit
}

#[tokio::test]
async fn clone_id_header_triggers_metrics_post_with_correct_body() {
    init(false);
    let server = start_server().await;
    let commit = seed_repo(&server, "acme", "metrics-on").await;

    let gw = spawn_gateway(&server.url, true, 202).await;
    let client = gateway_client(&gw.url);

    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let outcome = client
        .install_repo_with_mode_at(
            "acme/metrics-on",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("shallow"),
            None,
        )
        .await
        .expect("clone through gateway");

    // The header flowed through to the outcome.
    assert_eq!(outcome.clone_id.as_deref(), Some(gw.clone_id.as_str()));
    assert_eq!(outcome.mode, "depth1");
    assert!(!outcome.cold, "warm repo, no 202 poll");
    assert!(outcome.bytes > 0, "downloaded some bytes");

    // The clone really materialized.
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "hello\n"
    );

    // Fire the report (the CLI does this after printing success).
    client.report_clone_metrics(&outcome, 4242).await;

    // The POST must carry the ripclone auth token — the cloud rejects an
    // unauthenticated metrics post with a silent 401, so a regression that drops
    // auth (e.g. switching to the no-auth client) must fail this test.
    let auth = gw.auth_headers.lock().unwrap();
    assert_eq!(auth.len(), 1, "one captured auth header");
    let header = auth[0].as_deref().expect("Authorization header present");
    assert!(
        header.starts_with("Ripclone "),
        "metrics POST sends the Ripclone token, got {header:?}"
    );

    let captured = gw.captured.lock().unwrap();
    assert_eq!(captured.len(), 1, "exactly one metrics POST");
    let body = &captured[0];
    assert_eq!(body["cloneId"], gw.clone_id);
    assert_eq!(body["repo"]["provider"], "github");
    assert_eq!(body["repo"]["owner"], "acme");
    assert_eq!(body["repo"]["name"], "metrics-on");
    assert_eq!(body["commit"], commit);
    assert_eq!(body["mode"], "depth1");
    assert_eq!(body["cold"], false);
    assert_eq!(body["totalMs"], 4242);
    assert!(body["bytes"].as_u64().unwrap() > 0);
    // v1 always omits downloadMs (pure download time isn't cleanly isolated).
    assert!(body.get("downloadMs").is_none());
    assert!(body["client"]["os"].is_string());
    assert!(body["client"]["arch"].is_string());
    assert_eq!(body["client"]["ripcloneVersion"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn missing_clone_id_header_means_no_post() {
    init(false);
    let server = start_server().await;
    seed_repo(&server, "acme", "metrics-off").await;

    // Self-hosted shape: gateway proxies but never injects the clone-id header.
    let gw = spawn_gateway(&server.url, false, 202).await;
    let client = gateway_client(&gw.url);

    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let outcome = client
        .install_repo_with_mode_at(
            "acme/metrics-off",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("shallow"),
            None,
        )
        .await
        .expect("clone through gateway");

    assert!(outcome.clone_id.is_none(), "no clone id without the header");

    client.report_clone_metrics(&outcome, 100).await;

    assert_eq!(*gw.hits.lock().unwrap(), 0, "no metrics POST fired");
    assert!(gw.captured.lock().unwrap().is_empty());
}

#[tokio::test]
async fn metrics_endpoint_failure_does_not_fail_the_clone() {
    init(false);
    let server = start_server().await;
    seed_repo(&server, "acme", "metrics-500").await;

    // Gateway injects the header but the metrics endpoint always 500s.
    let gw = spawn_gateway(&server.url, true, 500).await;
    let client = gateway_client(&gw.url);

    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let outcome = client
        .install_repo_with_mode_at(
            "acme/metrics-500",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("shallow"),
            None,
        )
        .await
        .expect("clone succeeds despite a doomed metrics endpoint");

    // The clone itself is whole.
    assert_eq!(
        std::fs::read_to_string(target.join("a.txt")).unwrap(),
        "hello\n"
    );

    // The report must return normally even though the endpoint 500s — it can
    // never surface an error that looks like the clone failed.
    client.report_clone_metrics(&outcome, 1).await;

    // It did try (and got a 500), but nothing propagated.
    assert_eq!(*gw.hits.lock().unwrap(), 1, "report attempted exactly once");
}

/// Run the CLI `clone` subcommand against `server` (or a gateway) with optional
/// extra args. stdout/stderr go to temp files so long-lived git credential
/// grandchildren don't keep capture pipes open, and the blocking wait runs on
/// the blocking pool so the single-threaded tokio test runtime keeps driving the
/// in-process server.
async fn run_cli_clone(
    server_url: &str,
    repo: &str,
    target: &Path,
    args: &[&str],
    extra_env: &[(&str, &str)],
) -> Output {
    let server_url = server_url.to_string();
    let repo = repo.to_string();
    let target = target.to_path_buf();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let extra_env: Vec<(String, String)> = extra_env
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    tokio::task::spawn_blocking(move || {
        let dir = target.parent().unwrap().to_path_buf();
        let stdout_tmp = tempfile::NamedTempFile::new_in(&dir).expect("stdout temp file");
        let stderr_tmp = tempfile::NamedTempFile::new_in(&dir).expect("stderr temp file");

        let mut cmd = Command::new(env!("CARGO_BIN_EXE_ripclone"));
        cmd.arg("--server")
            .arg(&server_url)
            .arg("clone")
            .arg(&repo)
            .arg(&target)
            .env("RIPCLONE_SERVER_TOKEN_HASH", token_hash())
            .env("HOME", &dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(stdout_tmp.reopen().expect("reopen stdout temp"))
            .stderr(stderr_tmp.reopen().expect("reopen stderr temp"));
        for (k, v) in &extra_env {
            cmd.env(k, v);
        }
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
    .expect("spawn_blocking run_cli_clone")
}

/// Poll the gateway (proxying to the OSS server) until the requested clonepack
/// is warm, so the CLI doesn't spend the test waiting on 202s.
async fn warm_through_gateway(gw_url: &str, repo: &str, clonepack_kind: Option<&str>) {
    gateway_client(gw_url)
        .resolve_ref_with_clonepack(repo, "HEAD", clonepack_kind, None)
        .await
        .expect("warm repo through gateway");
}

#[tokio::test]
async fn no_metrics_flag_suppresses_post() {
    init(false);
    let server = start_server().await;
    seed_repo(&server, "acme", "metrics-no-flag").await;

    let gw = spawn_gateway(&server.url, true, 202).await;
    warm_through_gateway(&gw.url, "acme/metrics-no-flag", Some("shallow")).await;

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_cli_clone(
        &gw.url,
        "acme/metrics-no-flag",
        &target,
        &["--no-metrics"],
        &[],
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
    assert_eq!(
        *gw.hits.lock().unwrap(),
        0,
        "--no-metrics must suppress the metrics POST"
    );
}

#[tokio::test]
async fn no_metrics_env_var_suppresses_post() {
    init(false);
    let server = start_server().await;
    seed_repo(&server, "acme", "metrics-no-env").await;

    let gw = spawn_gateway(&server.url, true, 202).await;
    warm_through_gateway(&gw.url, "acme/metrics-no-env", Some("shallow")).await;

    let tmp = tempfile::tempdir().unwrap();
    let target = tmp.path().join("clone");
    let out = run_cli_clone(
        &gw.url,
        "acme/metrics-no-env",
        &target,
        &[],
        &[("RIPCLONE_NO_METRICS", "1")],
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
    assert_eq!(
        *gw.hits.lock().unwrap(),
        0,
        "RIPCLONE_NO_METRICS must suppress the metrics POST"
    );
}

#[tokio::test]
async fn oss_server_accepts_metrics_post() {
    init(false);
    let server = start_server().await;

    // The OSS server exposes an accept-and-drop metrics sink so self-hosted
    // clients don't get 404s.
    let url = format!("{}/v1/clones/test-id/metrics", server.url);
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Ripclone {}", token_hash()),
        )
        .header(
            "x-ripclone-protocol",
            ripclone::PROTOCOL_VERSION.to_string(),
        )
        .json(&serde_json::json!({"cloneId": "test-id"}))
        .send()
        .await
        .expect("POST metrics");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::ACCEPTED,
        "OSS metrics sink should accept and drop"
    );
}
