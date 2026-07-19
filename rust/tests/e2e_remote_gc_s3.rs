//! Real S3/Tigris end-to-end tests for remote GC and storage usage accounting.
//!
//! These tests are ignored by default because they need credentials for an
//! S3-compatible store. Run them explicitly with:
//!
//!   RIPCLONE_S3_ENDPOINT=https://... RIPCLONE_S3_BUCKET=... \
//!     AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!     cargo test --test e2e_remote_gc_s3 -- --ignored

mod common;

use anyhow::{Context, Result};
use common::*;
use ripclone::auth::access::{AccessDecision, AccessVerifier};
use ripclone::mode::CloneMode;
use ripclone::provider::RepoId;
use ripclone::ref_store::{CachingRefStore, RefStore, S3RefStore};
use ripclone::remote_gc::{GcConfig, RemoteGc};
use ripclone::server::{ServerState, build_app, run_server};
use ripclone::storage::{S3Storage, StorageBackend};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;

struct ToggleAccessVerifier {
    allowed: std::sync::atomic::AtomicBool,
    calls: AtomicU64,
}

impl ToggleAccessVerifier {
    fn new(allowed: bool) -> Self {
        Self {
            allowed: std::sync::atomic::AtomicBool::new(allowed),
            calls: AtomicU64::new(0),
        }
    }

    fn set_allowed(&self, allowed: bool) {
        self.allowed.store(allowed, Ordering::SeqCst);
    }

    fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl AccessVerifier for ToggleAccessVerifier {
    async fn verify(
        &self,
        _provider: &ripclone::provider::ProviderInstance,
        _repo_path: &str,
        _credential: Option<&secrecy::SecretString>,
    ) -> AccessDecision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.allowed.load(Ordering::SeqCst) {
            AccessDecision::PrivateAuthorized
        } else {
            AccessDecision::Denied
        }
    }
}

#[derive(Clone)]
struct S3Env {
    endpoint: String,
    region: String,
    bucket: String,
}

/// Serializes server startup and env-var mutation across tests in this binary.
static SERVER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static PREFIX_COUNTER: AtomicU64 = AtomicU64::new(0);

fn s3_env() -> Option<S3Env> {
    let required = std::env::var_os("RIPCLONE_REQUIRE_MINIO").is_some();
    let endpoint = std::env::var("RIPCLONE_S3_ENDPOINT")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::env::var("AWS_ENDPOINT_URL_S3")
                .ok()
                .filter(|s| !s.is_empty())
        });
    let bucket = std::env::var("RIPCLONE_S3_BUCKET")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("BUCKET_NAME").ok().filter(|s| !s.is_empty()));
    if required {
        assert!(endpoint.is_some(), "RIPCLONE_S3_ENDPOINT is required");
        assert!(bucket.is_some(), "RIPCLONE_S3_BUCKET is required");
    }
    let endpoint = endpoint?;
    let bucket = bucket?;
    let region = std::env::var("RIPCLONE_S3_REGION")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("AWS_REGION").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| "us-east-1".to_string());
    Some(S3Env {
        endpoint,
        region,
        bucket,
    })
}

fn unique_prefix() -> String {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    let seq = PREFIX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("e2e-remote-gc/{ns}-{pid}-{seq}/")
}

fn repo_suffix(prefix: &str) -> String {
    prefix
        .trim_start_matches("e2e-remote-gc/")
        .trim_end_matches('/')
        .to_string()
}

fn required_ripclone_bin() -> std::path::PathBuf {
    let binary = cargo_bin("ripclone");
    if std::env::var_os("RIPCLONE_REQUIRE_MINIO").is_some() {
        let dir = std::env::var_os("RIPCLONE_BIN_DIR")
            .map(std::path::PathBuf::from)
            .expect("RIPCLONE_BIN_DIR is required for the MinIO pinning proof");
        assert_eq!(
            binary.canonicalize().expect("canonical release binary"),
            dir.join("ripclone")
                .canonicalize()
                .expect("canonical RIPCLONE_BIN_DIR binary"),
            "CLI-spawning proof must use RIPCLONE_BIN_DIR"
        );
    }
    let version = std::process::Command::new(&binary)
        .arg("--version")
        .output()
        .expect("run selected ripclone --version");
    assert!(
        version.status.success(),
        "selected ripclone reports version"
    );
    binary
}

fn wait_child_output_bounded(
    mut child: std::process::Child,
    timeout: Duration,
) -> Result<std::process::Output> {
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().context("poll CLI child")? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("CLI child exceeded {timeout:?}; killed and reaped");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_end(&mut stdout).context("read CLI stdout")?;
    }
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_end(&mut stderr).context("read CLI stderr")?;
    }
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

async fn wait_child_output(child: std::process::Child) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(65),
        tokio::task::spawn_blocking(move || {
            wait_child_output_bounded(child, Duration::from_secs(60))
        }),
    )
    .await
    .expect("CLI wait task remained bounded")
    .expect("join CLI wait task")
    .expect("wait for bounded CLI child")
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_for_server(port: u16) {
    for _ in 0..400 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("server on port {port} did not become ready");
}

/// A selective TCP delay proxy for forcing S3 signed-URL expiry in tests.
///
/// Listens on a local port and forwards requests to `target_endpoint`. Regular
/// S3 API traffic is tunneled with keep-alive. Every GET/HEAD whose path/query
/// looks like a presigned S3 URL is delayed for `delay` and forced to close
/// after the response, so each signed-URL fetch is held long enough for a short
/// TTL to expire. The raw-byte forwarding preserves the Host header, so MinIO
/// validates the signature minted for the proxy endpoint.
pub struct DelayProxy {
    pub url: String,
    _handle: tokio::task::JoinHandle<()>,
}

impl Drop for DelayProxy {
    fn drop(&mut self) {
        self._handle.abort();
    }
}

fn target_host_port(endpoint: &str) -> String {
    let url = url::Url::parse(endpoint).expect("valid S3 endpoint URL");
    let host = url.host_str().expect("endpoint host");
    let port = url
        .port_or_known_default()
        .expect("endpoint port or known scheme default");
    format!("{host}:{port}")
}

/// True when the request bytes look like a presigned S3 GET/HEAD.
fn is_signed_get(head: &[u8]) -> bool {
    let s = std::str::from_utf8(head).unwrap_or("");
    let Some(line) = s.lines().next() else {
        return false;
    };
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return false;
    }
    let method = parts[0];
    let path_query = parts[1];
    (method == "GET" || method == "HEAD")
        && (path_query.contains("X-Amz-Signature=") || path_query.contains("Signature="))
}

/// Rewrite the request so the backend closes the connection after the response.
fn force_connection_close(buf: &mut Vec<u8>) {
    let s = match std::str::from_utf8(buf) {
        Ok(s) => s,
        Err(_) => return,
    };
    let Some(end_headers) = s.find("\r\n\r\n") else {
        return;
    };
    let before = &s[..end_headers];
    let after = &s[end_headers + 4..];
    let new_headers: Vec<&str> = before
        .lines()
        .filter(|l| !l.to_lowercase().starts_with("connection:"))
        .collect();
    let new = format!(
        "{}\r\nConnection: close\r\n\r\n{}",
        new_headers.join("\r\n"),
        after
    );
    *buf = new.into_bytes();
}

/// Replace the Host header so the S3 backend validates the signature minted for
/// the direct endpoint, while the client still sends requests to the proxy.
fn replace_host_header(buf: &mut Vec<u8>, new_host: &str) {
    let s = match std::str::from_utf8(buf) {
        Ok(s) => s,
        Err(_) => return,
    };
    let Some(end_headers) = s.find("\r\n\r\n") else {
        return;
    };
    let before = &s[..end_headers];
    let after = &s[end_headers + 4..];
    let new_headers: Vec<String> = before
        .lines()
        .map(|l| {
            if l.to_lowercase().starts_with("host:") {
                format!("Host: {new_host}")
            } else {
                l.to_string()
            }
        })
        .collect();
    let new = format!("{}\r\n\r\n{}", new_headers.join("\r\n"), after);
    *buf = new.into_bytes();
}

/// Read until the HTTP header block is complete.
async fn read_request_header(client: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    loop {
        let n = client.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            return Some(buf);
        }
        if buf.len() > 64 * 1024 {
            return None;
        }
    }
}

/// Handle a signed GET by delaying it, forcing a close, and copying the response
/// until the backend closes. GETs have no body, so we only need the header.
async fn proxy_signed_get(
    client: &mut tokio::net::TcpStream,
    target: &str,
    mut header: Vec<u8>,
    delay: Duration,
) {
    sleep(delay).await;
    let Ok(mut backend) = tokio::net::TcpStream::connect(target).await else {
        return;
    };
    force_connection_close(&mut header);
    replace_host_header(&mut header, target);
    if backend.write_all(&header).await.is_err() {
        return;
    }
    let mut buf = [0u8; 4096];
    loop {
        match backend.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if client.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn proxy_one_connection(mut client: tokio::net::TcpStream, target: String, delay: Duration) {
    let Some(header) = read_request_header(&mut client).await else {
        return;
    };

    if is_signed_get(&header) {
        proxy_signed_get(&mut client, &target, header, delay).await;
        return;
    }

    // Not a signed GET: open a backend connection and tunnel the rest. The
    // already-read header bytes are forwarded, then we full-duplex copy.
    let Ok(mut backend) = tokio::net::TcpStream::connect(&target).await else {
        return;
    };
    if backend.write_all(&header).await.is_err() {
        return;
    }
    let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
}

pub async fn start_delay_proxy(target_endpoint: &str, delay: Duration) -> DelayProxy {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind delay proxy");
    let port = listener.local_addr().expect("proxy local addr").port();
    let target = target_host_port(target_endpoint);

    let handle = tokio::spawn(async move {
        loop {
            let (client, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let target = target.clone();
            tokio::spawn(async move {
                proxy_one_connection(client, target, delay).await;
            });
        }
    });

    DelayProxy {
        url: format!("http://127.0.0.1:{port}"),
        _handle: handle,
    }
}

/// Deterministic mid-download barrier for signed-URL GETs.
///
/// The first presigned GET whose response body is larger than `after_bytes` is
/// forwarded until exactly `after_bytes` have been sent, then the proxy signals
/// `entered` and waits on `proceed`. After the test releases the barrier the
/// proxy either closes the connection (`close_on_proceed = true`) or copies the
/// remainder (`false`).
struct BarrierState {
    after_bytes: usize,
    close_on_proceed: bool,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    proceed: Option<tokio::sync::oneshot::Receiver<()>>,
    consumed: std::sync::atomic::AtomicBool,
    signed_headers: Vec<String>,
}

pub struct BarrierProxy {
    pub url: String,
    state: Arc<std::sync::Mutex<BarrierState>>,
    _handle: tokio::task::JoinHandle<()>,
}

impl BarrierProxy {
    fn signed_headers(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .signed_headers
            .clone()
    }
}

impl Drop for BarrierProxy {
    fn drop(&mut self) {
        self._handle.abort();
    }
}

/// Read until the HTTP response header block is complete.
async fn read_response_header(backend: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    loop {
        let n = backend.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            return Some(buf);
        }
        if buf.len() > 64 * 1024 {
            return None;
        }
    }
}

async fn proxy_signed_get_barrier(
    client: &mut tokio::net::TcpStream,
    target: &str,
    mut header: Vec<u8>,
    barrier: Arc<std::sync::Mutex<BarrierState>>,
) {
    eprintln!("BARRIER PROXY: signed GET received");
    barrier
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .signed_headers
        .push(String::from_utf8_lossy(&header).into_owned());
    let Ok(mut backend) = tokio::net::TcpStream::connect(target).await else {
        return;
    };
    replace_host_header(&mut header, target);
    force_connection_close(&mut header);
    if backend.write_all(&header).await.is_err() {
        return;
    }
    let Some(resp_header) = read_response_header(&mut backend).await else {
        return;
    };
    eprintln!("BARRIER PROXY: response header received, forwarding");
    // `read_response_header` stops at the end of the header block, but its
    // buffered reads may have already pulled body bytes past the CRLFCRLF
    // boundary. Forward ONLY the header now, and carry any trailing bytes as the
    // first body bytes the barrier accounts for. Forwarding the whole buffer
    // would deliver a small artifact's entire body in one shot (header + body
    // arriving in the same TCP read), so the "barrier" would hold an
    // already-drained connection and the clone would complete — the exact
    // TCP-segmentation nondeterminism that made this test flaky.
    let header_end = resp_header
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(resp_header.len());
    let (head, leftover) = resp_header.split_at(header_end);
    if client.write_all(head).await.is_err() {
        return;
    }
    let mut pending_body: Vec<u8> = leftover.to_vec();

    let (after_bytes, close_on_proceed, entered, proceed) = {
        let mut b = barrier.lock().unwrap();
        if !b.consumed.load(std::sync::atomic::Ordering::SeqCst) {
            b.consumed.store(true, std::sync::atomic::Ordering::SeqCst);
            (
                b.after_bytes,
                b.close_on_proceed,
                b.entered.take(),
                b.proceed.take(),
            )
        } else {
            (usize::MAX, false, None, None)
        }
    };

    if entered.is_none() {
        // Barrier already consumed; just copy the rest (buffered body first).
        if !pending_body.is_empty() && client.write_all(&pending_body).await.is_err() {
            return;
        }
        let _ = tokio::io::copy(&mut backend, client).await;
        return;
    }

    // Forward at most `after_bytes` body bytes — from the already-buffered
    // leftover first, then the backend — then HOLD, keeping the rest of the
    // artifact undelivered. This stalls the clone deterministically regardless of
    // how the response was segmented, so the credentials can expire before the
    // client is forced to retry.
    let mut buf = [0u8; 4096];
    let mut copied = 0usize;
    while copied < after_bytes {
        if pending_body.is_empty() {
            let need = after_bytes - copied;
            let to_read = buf.len().min(need);
            let n = match backend.read(&mut buf[..to_read]).await {
                Ok(0) => break,
                Err(_) => return,
                Ok(n) => n,
            };
            pending_body.extend_from_slice(&buf[..n]);
        }
        let take = pending_body.len().min(after_bytes - copied);
        if client.write_all(&pending_body[..take]).await.is_err() {
            return;
        }
        pending_body.drain(..take);
        copied += take;
    }

    if let Some(entered) = entered {
        eprintln!("BARRIER PROXY: entered barrier after {copied} bytes");
        let _ = entered.send(());
    }
    let should_continue = if let Some(proceed) = proceed {
        proceed.await.is_ok() && !close_on_proceed
    } else {
        false
    };
    if !should_continue {
        return;
    }

    // Released without closing: deliver the held body, then the remainder.
    if !pending_body.is_empty() && client.write_all(&pending_body).await.is_err() {
        return;
    }
    loop {
        match backend.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if client.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn proxy_one_connection_barrier(
    mut client: tokio::net::TcpStream,
    target: String,
    barrier: Arc<std::sync::Mutex<BarrierState>>,
) {
    let Some(header) = read_request_header(&mut client).await else {
        return;
    };

    if is_signed_get(&header) {
        proxy_signed_get_barrier(&mut client, &target, header, barrier).await;
        return;
    }

    let Ok(mut backend) = tokio::net::TcpStream::connect(&target).await else {
        return;
    };
    if backend.write_all(&header).await.is_err() {
        return;
    }
    let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
}

pub async fn start_barrier_proxy(
    target_endpoint: &str,
    after_bytes: usize,
    close_on_proceed: bool,
    entered: tokio::sync::oneshot::Sender<()>,
    proceed: tokio::sync::oneshot::Receiver<()>,
) -> BarrierProxy {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind barrier proxy");
    let port = listener.local_addr().expect("proxy local addr").port();
    let target = target_host_port(target_endpoint);

    let state = Arc::new(std::sync::Mutex::new(BarrierState {
        after_bytes,
        close_on_proceed,
        entered: Some(entered),
        proceed: Some(proceed),
        consumed: std::sync::atomic::AtomicBool::new(false),
        signed_headers: Vec::new(),
    }));
    let observable_state = Arc::clone(&state);

    let handle = tokio::spawn(async move {
        loop {
            let (client, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let state = state.clone();
            let target = target.clone();
            tokio::spawn(async move {
                proxy_one_connection_barrier(client, target, state).await;
            });
        }
    });

    BarrierProxy {
        url: format!("http://127.0.0.1:{port}"),
        state: observable_state,
        _handle: handle,
    }
}

async fn start_s3_server(env: &S3Env, prefix: &str) -> Server {
    start_s3_server_faulting(env, prefix, 0).await
}

/// Start the in-process server backed by the real S3-compatible store, failing
/// the first `fail_first` artifact GETs via `RIPCLONE_TEST_FAIL_FIRST_FETCHES`.
///
/// This helper does NOT take `SERVER_LOCK`; every caller already holds it for the
/// whole test body. It reads and mutates process-global request-time env vars, so
/// callers must be serialized on `SERVER_LOCK` to keep those vars race-free. The
/// tokio Mutex is not reentrant, so re-locking here would deadlock.
async fn start_s3_server_faulting(env: &S3Env, prefix: &str, fail_first: usize) -> Server {
    unsafe {
        std::env::set_var("RIPCLONE_S3_ENDPOINT", &env.endpoint);
        std::env::set_var("RIPCLONE_S3_BUCKET", &env.bucket);
        std::env::set_var("RIPCLONE_S3_REGION", &env.region);
        std::env::set_var("RIPCLONE_S3_PREFIX", prefix);
        std::env::set_var("RIPCLONE_REMOTE_GC_INTERVAL_SECS", "0");
        std::env::set_var("RIPCLONE_RETENTION_INTERVAL_SECS", "999999");
        // Disable the server's in-memory ref cache. These tests drive GC and ref
        // eviction/pinning out-of-band through a separate ref-store handle, so a
        // cached ref on the server would otherwise serve a stale (pre-eviction /
        // pre-pin) view and its now-deleted artifacts. TTL=0 makes every server
        // read go through to the durable store, keeping /status and /ref resolve
        // coherent with the out-of-band writes.
        std::env::set_var("RIPCLONE_REF_CACHE_TTL_SECS", "0");
        std::env::set_var("RIPCLONE_TEST_MIRROR_FRESH_TTL_MS", "0");
        // Fast re-attach when a build outlives the server's ~25s wait window.
        // Production clients keep the 2s default (this var unset).
        std::env::set_var("RIPCLONE_TEST_SYNC_POLL_MS", "100");
        if fail_first > 0 {
            std::env::set_var("RIPCLONE_TEST_FAIL_FIRST_FETCHES", fail_first.to_string());
        }
    }
    common::init(false);

    let dir = tempfile::tempdir().expect("server temp dir");
    let cas_dir = dir.path().join("cas");
    let repo_root = dir.path().join("repos");
    std::fs::create_dir_all(&cas_dir).unwrap();
    std::fs::create_dir_all(&repo_root).unwrap();
    unsafe {
        std::env::set_var("RIPCLONE_S3_CACHE_DIR", cas_dir.to_str().unwrap());
    }

    let port = free_port();
    let (cas_dir2, repo_root2) = (cas_dir.clone(), repo_root.clone());
    tokio::spawn(async move {
        let _ = run_server(&cas_dir2, &repo_root2, "127.0.0.1", port).await;
    });
    wait_for_server(port).await;

    if fail_first > 0 {
        unsafe {
            std::env::remove_var("RIPCLONE_TEST_FAIL_FIRST_FETCHES");
        }
    }

    Server {
        url: format!("http://127.0.0.1:{port}"),
        cas_dir: cas_dir.clone(),
        storage_dir: cas_dir,
        repo_root,
        pinned_path_probe: None,
        _dir: dir,
    }
}

/// Start the existing S3-backed fixture with repository authorization enforced
/// through a controllable test verifier. This uses the same production
/// `ServerState`, S3 storage/ref store, local queue, and build worker as the
/// ordinary fixture; only the authorization adapter is injected by the test.
async fn start_s3_server_authorized(
    env: &S3Env,
    prefix: &str,
    verifier: Arc<dyn AccessVerifier>,
) -> Server {
    unsafe {
        std::env::set_var("RIPCLONE_S3_ENDPOINT", &env.endpoint);
        std::env::set_var("RIPCLONE_S3_BUCKET", &env.bucket);
        std::env::set_var("RIPCLONE_S3_REGION", &env.region);
        std::env::set_var("RIPCLONE_S3_PREFIX", prefix);
        std::env::set_var("RIPCLONE_REMOTE_GC_INTERVAL_SECS", "0");
        std::env::set_var("RIPCLONE_RETENTION_INTERVAL_SECS", "999999");
        std::env::set_var("RIPCLONE_REF_CACHE_TTL_SECS", "0");
        std::env::set_var("RIPCLONE_TEST_MIRROR_FRESH_TTL_MS", "0");
        std::env::set_var("RIPCLONE_TEST_SYNC_POLL_MS", "100");
    }
    common::init(false);

    let dir = tempfile::tempdir().expect("authorized S3 server temp dir");
    let cas_dir = dir.path().join("cas");
    let repo_root = dir.path().join("repos");
    std::fs::create_dir_all(&cas_dir).unwrap();
    std::fs::create_dir_all(&repo_root).unwrap();
    unsafe {
        std::env::set_var("RIPCLONE_S3_CACHE_DIR", cas_dir.to_str().unwrap());
    }

    let metrics = ripclone::metrics::Metrics::new();
    let backends = ripclone::backends::Backends::from_env(&cas_dir, &repo_root, &metrics)
        .await
        .expect("authorized S3 backends");
    let (local_queue, mut rx, depth) = ripclone::queue::LocalJobQueue::new(16);
    let build_queue: ripclone::queue::JobQueueRef = Arc::new(local_queue);
    let provider_registry = ripclone::provider::ProviderRegistry::new();
    let broker: Arc<dyn ripclone::auth::broker::CredentialBroker> = Arc::new(
        ripclone::auth::broker::StaticBroker::new(provider_registry.clone()),
    );
    let state = ServerState {
        cas: backends.cas,
        repo_config: Arc::new(ripclone::repo_config::RepoConfigStore::new(
            backends.storage.clone(),
        )),
        storage: backends.storage,
        repo_root: repo_root.clone(),
        ref_store: backends.ref_store,
        provider_registry,
        broker,
        token_hash: Some(token_hash()),
        jwt: None,
        metrics,
        rate_limiter: ripclone::server::RateLimiter::new(1_000_000, 1_000_000.0),
        retention: backends.retention,
        build_queue,
        worker_queue: None,
        build_queue_depth: depth,
        build_waiters: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        oidc_verifier: None,
        webhook_config: Arc::new(ripclone::webhook::WebhookConfig::empty()),
        sync_locks: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        mirror_freshness: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        mirror_fresh_ttl: Duration::from_secs(0),
        ref_response_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        artifact_fetch_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        fail_first_fetches: 0,
        artifact_barrier: None,
        readyz_cache: Arc::new(std::sync::Mutex::new(None)),
        access_verifier: verifier,
        require_repo_auth: true,
    };

    let worker_state = state.clone();
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let state = worker_state.clone();
            tokio::spawn(async move {
                let key = format!(
                    "{}/{}#{}",
                    job.repo_id.storage_key(),
                    job.branch,
                    job.rev.as_deref().unwrap_or("")
                );
                let result = ripclone::server::process_build_job(&state, &job).await;
                state
                    .build_queue_depth
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                if let Some(senders) = state.build_waiters.lock().await.remove(&key) {
                    for sender in senders {
                        let _ = sender.send(result.clone());
                    }
                }
            });
        }
    });

    let port = free_port();
    let app = build_app(state);
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind authorized S3 server");
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .expect("serve authorized S3 server");
    });
    wait_for_server(port).await;

    Server {
        url: format!("http://127.0.0.1:{port}"),
        cas_dir: cas_dir.clone(),
        storage_dir: cas_dir,
        repo_root,
        pinned_path_probe: None,
        _dir: dir,
    }
}

fn make_s3_storage(env: &S3Env, prefix: &str) -> Result<Arc<S3Storage>> {
    let s3 = S3Storage::new(
        &env.endpoint,
        &env.region,
        &env.bucket,
        Some(prefix),
        s3::Auth::from_env().context("S3 auth from env")?,
        None,
    )
    .context("create S3 storage")?;
    Ok(Arc::new(s3))
}

fn make_s3_ref_store(storage: Arc<S3Storage>) -> Arc<dyn RefStore> {
    Arc::new(CachingRefStore::new(S3RefStore::new(storage)))
}

/// Build the tiny shallow fixture on local storage, then copy its real CAS
/// objects and ref rows into the existing S3 fixture. The signed-URL tests need
/// real S3 reads, signing, mutation, and client installation; they do not need
/// to spend minutes rebuilding full history through S3 before that journey can
/// start.
async fn seed_shallow_s3_fixture(
    env: &S3Env,
    prefix: &str,
    repo: &str,
) -> (String, Arc<dyn RefStore>, ripclone::RefInfo, Server) {
    let fixture = start_server_split_storage().await;
    add_acme_repo(&fixture, repo).await;
    fixture
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("build local shallow fixture");
    let pinned = fixture
        .client()
        .resolve_ref_with_clonepack(&format!("acme/{repo}"), "HEAD", Some("shallow"), None)
        .await
        .expect("local shallow fixture ready")
        .commit;

    let repo_id = RepoId::github(format!("acme/{repo}"));
    let local_refs = ripclone::ref_store::FileRefStore::new(&fixture.repo_root);
    let info = local_refs
        .load_branch(&repo_id, "HEAD")
        .await
        .expect("load local A ref")
        .expect("local A ref present");
    assert_eq!(info.shallow_clonepack.commit, pinned);
    assert!(!info.shallow_clonepack.manifest.is_empty());

    let local_storage =
        ripclone::storage::local(&fixture.storage_dir).expect("open local fixture storage");
    let s3_storage = make_s3_storage(env, prefix).expect("open S3 fixture storage");
    for entry in local_storage
        .list_hashes()
        .expect("list local fixture artifacts")
    {
        let data = local_storage
            .get(&entry.hash)
            .expect("read local fixture artifact");
        s3_storage
            .put_async(&entry.hash, &data)
            .await
            .expect("upload fixture artifact to S3");
    }

    let s3_refs: Arc<dyn RefStore> = Arc::new(S3RefStore::new(s3_storage));
    let default_branch = info.default_branch.clone();
    s3_refs
        .save_branch(&repo_id, "HEAD", &info)
        .await
        .expect("publish S3 HEAD fixture");
    s3_refs
        .save_branch(&repo_id, &default_branch, &info)
        .await
        .expect("publish S3 default-branch fixture");
    s3_refs
        .save_branch(&repo_id, &format!("{default_branch}#{pinned}"), &info)
        .await
        .expect("publish exact S3 A fixture");
    (pinned, s3_refs, info, fixture)
}

async fn publish_moving_s3_row(
    ref_store: &Arc<dyn RefStore>,
    repo_id: &RepoId,
    previous: &ripclone::RefInfo,
    commit: &str,
) {
    let info = ripclone::RefInfo {
        commit: commit.to_string(),
        default_branch: previous.default_branch.clone(),
        build_status: Some("building".to_string()),
        synced_at: Some(previous.synced_at.unwrap_or(0).saturating_add(1)),
        generation: Some(previous.generation.unwrap_or(0).saturating_add(1)),
        ..Default::default()
    };
    ref_store
        .save_branch(repo_id, "HEAD", &info)
        .await
        .expect("publish moving S3 HEAD row");
    ref_store
        .save_branch(repo_id, &info.default_branch, &info)
        .await
        .expect("publish moving S3 default-branch row");
}

/// Cleanup client with the same timeout/retry posture as production S3Storage.
/// The s3 crate default (~10s, few retries) flakes on MinIO `delete_objects`
/// batches under CI load — not a credentials failure (would be 403, not timeout).
fn cleanup_s3_client(env: &S3Env) -> Result<s3::Client> {
    s3::Client::builder(&env.endpoint)
        .context("create S3 cleanup builder")?
        .region(&env.region)
        .auth(s3::Auth::from_env().context("S3 auth for cleanup")?)
        .timeout(Duration::from_secs(10))
        .max_attempts(1)
        .build()
        .context("build cleanup S3 client")
}

async fn delete_key_batches(env: &S3Env, client: &s3::Client, keys: Vec<String>) -> Result<()> {
    // Smaller batches: a single DeleteObjects of 1000 under a slow MinIO can
    // exceed a tight transport timeout and fail the whole cleanup.
    for chunk in keys.chunks(100) {
        let chunk: Vec<String> = chunk.to_vec();
        if chunk.is_empty() {
            continue;
        }
        let mut last_error = None;
        for attempt in 1..=3 {
            match client
                .objects()
                .delete_objects(&env.bucket)
                .objects(&chunk)
                .context("build cleanup delete batch")?
                .quiet(true)
                .send()
                .await
            {
                Ok(_) => {
                    last_error = None;
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 3 {
                        sleep(Duration::from_millis(100 * attempt)).await;
                    }
                }
            }
        }
        if let Some(error) = last_error {
            return Err(error).context("S3 cleanup delete_objects after 3 attempts");
        }
    }
    Ok(())
}

async fn cleanup_prefix(env: &S3Env, prefix: &str) -> Result<()> {
    let client = cleanup_s3_client(env)?;

    let mut keys = Vec::new();
    let mut continuation = None::<String>;
    loop {
        let mut req = client
            .objects()
            .list_v2(&env.bucket)
            .prefix(prefix)
            .context("set cleanup list prefix")?;
        if let Some(token) = continuation.take() {
            req = req
                .continuation_token(token)
                .context("set cleanup continuation token")?;
        }
        let output = req.send().await.context("S3 list for cleanup")?;
        for obj in output.contents {
            keys.push(obj.key);
        }
        if !output.is_truncated {
            break;
        }
        continuation = output.next_continuation_token;
        if continuation.is_none() {
            break;
        }
    }

    delete_key_batches(env, &client, keys).await
}

async fn cleanup_repo_refs(env: &S3Env, owner: &str, repo: &str) -> Result<()> {
    let repo_id = ripclone::provider::RepoId::github(format!("{owner}/{repo}"));
    let storage_key = repo_id.storage_key();
    let client = cleanup_s3_client(env)?;

    // Refs live under the per-test RIPCLONE_S3_PREFIX when the server is S3-backed.
    // Prefer listing via the env prefix if set; also try unscoped keys for safety.
    let prefix = std::env::var("RIPCLONE_S3_PREFIX").unwrap_or_default();
    let head_key = format!("{prefix}refs/{storage_key}.json");
    let branch_prefix = format!("{prefix}refs/{storage_key}/");
    let mut keys = vec![head_key];
    let mut continuation = None::<String>;
    loop {
        let mut req = client
            .objects()
            .list_v2(&env.bucket)
            .prefix(&branch_prefix)
            .context("set cleanup ref list prefix")?;
        if let Some(token) = continuation.take() {
            req = req
                .continuation_token(token)
                .context("set cleanup ref continuation token")?;
        }
        let output = req.send().await.context("S3 list refs for cleanup")?;
        for obj in output.contents {
            keys.push(obj.key);
        }
        if !output.is_truncated {
            break;
        }
        continuation = output.next_continuation_token;
        if continuation.is_none() {
            break;
        }
    }

    delete_key_batches(env, &client, keys).await
}

/// Ensures the S3 prefix (and optional ref JSON) are deleted even if a test
/// panics. Call `disable()` after an explicit successful cleanup to avoid
/// running twice.
struct CleanupGuard {
    env: S3Env,
    prefix: String,
    owner_repo: Option<(String, String)>,
    disabled: bool,
}

impl CleanupGuard {
    fn new(env: S3Env, prefix: String) -> Self {
        Self {
            env,
            prefix,
            owner_repo: None,
            disabled: false,
        }
    }

    fn track_repo(&mut self, owner: &str, repo: &str) {
        self.owner_repo = Some((owner.to_string(), repo.to_string()));
    }

    fn disable(&mut self) {
        self.disabled = true;
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if self.disabled {
            return;
        }
        let env = self.env.clone();
        let prefix = self.prefix.clone();
        let owner_repo = self.owner_repo.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("cleanup runtime");
            if let Err(e) = rt.block_on(cleanup_prefix(&env, &prefix)) {
                eprintln!("cleanup_prefix failed: {e:#}");
            }
            if let Some((owner, repo)) = owner_repo
                && let Err(e) = rt.block_on(cleanup_repo_refs(&env, &owner, &repo))
            {
                eprintln!("cleanup_repo_refs failed: {e:#}");
            }
        })
        .join()
        .ok();
    }
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Poll until `grace` has elapsed since `start`, timing out at 10 s past the
/// grace window. This replaces fixed sleeps with a bounded poll so tests don't
/// wait longer than necessary on fast backends.
async fn wait_for_grace_since(start: Instant, grace: Duration) {
    let deadline = start + grace + Duration::from_secs(10);
    while Instant::now() < start + grace && Instant::now() < deadline {
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        Instant::now() >= start + grace,
        "grace {grace:?} never elapsed since {start:?}"
    );
}

async fn get_status(
    server: &Server,
    owner: &str,
    repo: &str,
    query: Option<&str>,
) -> serde_json::Value {
    let mut url = format!("{}/v1/repos/github/{owner}/{repo}/status", server.url);
    if let Some(q) = query {
        url.push('?');
        url.push_str(q);
    }
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("status request");
    let status = resp.status();
    let body = resp.text().await.expect("status text");
    if !status.is_success() {
        eprintln!("status endpoint returned {status}: {body}");
    }
    assert!(status.is_success(), "status 2xx");
    serde_json::from_str(&body).expect("status json")
}

/// Block until the background full-history build has settled.
///
/// `sync_repo` returns as soon as the depth=1 clonepack is published; phase 2
/// (the full clonepack + archive) finishes on a detached task and keeps writing
/// the concrete default-branch ref. A test that ages/pins/GCs the ref before
/// that lands races the build and observes a half-built repo. Wait until the
/// concrete default branch reports a completed build (`build_status` cleared,
/// full clonepack present) so the artifact set is stable before we touch it.
///
/// This polls the durable S3 ref store directly rather than the server's
/// `/status` endpoint on purpose: `/status` reads through the server's
/// `CachingRefStore`, and polling it for the length of the build would keep the
/// ref hot in that cache. A test that then writes the ref out-of-band (to age or
/// pin it) would be invisible to a subsequent `/status` read until the cache
/// entry expired. Reading the store directly lets the server's cache lapse on
/// its own TTL, so the later `/status` assertions observe the out-of-band write.
async fn wait_for_full_build(env: &S3Env, prefix: &str, owner: &str, repo: &str) {
    let storage = make_s3_storage(env, prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage);
    let repo_id = RepoId::github(format!("{owner}/{repo}"));
    // 50ms poll (was 200ms): phase-2 settlement is the multi-minute sink on
    // these tests; tighter polling only shaves seconds but costs almost nothing
    // against local MinIO and keeps the suite responsive once the build lands.
    // 300s ceiling unchanged (6000 * 50ms).
    for _ in 0..6000 {
        if let Ok(branches) = ref_store.list_branches(&repo_id).await {
            for branch in &branches {
                if branch == "HEAD" {
                    continue;
                }
                ref_store.invalidate(&repo_id, branch).await;
                if let Ok(Some(info)) = ref_store.load_branch(&repo_id, branch).await
                    && info.build_status.is_none()
                    && !info.full_clonepack.manifest.is_empty()
                {
                    return;
                }
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("full build never settled for {owner}/{repo}");
}

async fn add_acme_repo(server: &Server, repo: &str) {
    server
        .client()
        .add_repo(&format!("acme/{repo}"))
        .await
        .expect("add repo");
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn remote_gc_deletes_orphans_on_s3() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcorphan-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "hello world\n")], "c1");
    origin.publish();

    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let storage = make_s3_storage(&env, &prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage.clone());
    let reachable_data = b"i-am-reachable";
    let reachable_hash = sha256_hex(reachable_data);
    storage
        .put(&reachable_hash, reachable_data)
        .expect("put reachable");
    let reachable_repo = RepoId::github(format!("acme/{repo}-gc-reachable"));
    let reachable_info = ripclone::RefInfo {
        commit: "reachable".to_string(),
        default_branch: "HEAD".to_string(),
        metadata_chunk: reachable_hash.clone(),
        ..Default::default()
    };
    ref_store
        .save(&reachable_repo, &reachable_info)
        .await
        .expect("save reachable ref");

    // Age the reachable object relative to the orphan we are about to inject.
    let reachable_at = Instant::now();
    wait_for_grace_since(reachable_at, Duration::from_secs(1)).await;

    let orphan_data = b"i-am-an-orphan";
    let orphan_hash = sha256_hex(orphan_data);
    storage.put(&orphan_hash, orphan_data).expect("put orphan");
    let orphan_at = Instant::now();

    // Make sure the orphan is older than the grace period we will use.
    wait_for_grace_since(orphan_at, Duration::from_secs(1)).await;

    let gc = RemoteGc::new(
        storage.clone(),
        ref_store,
        GcConfig {
            grace_period: Duration::from_secs(1),
            dry_run: false,
            ..Default::default()
        },
    );
    // First pass tombstones the orphan in the ledger; it is never deleted on the
    // pass that first sees it unreferenced.
    let first = gc.run().await.expect("remote gc first run");
    let tombstoned_at = Instant::now();
    assert_eq!(
        first.objects_deleted, 0,
        "first pass must only tombstone, got {first:?}"
    );
    assert!(
        storage.size(&orphan_hash).is_ok(),
        "orphan must survive the tombstoning pass"
    );

    // After the (1s) grace elapses, a second pass collects it.
    wait_for_grace_since(tombstoned_at, Duration::from_secs(1)).await;
    let report = gc.run().await.expect("remote gc second run");

    // The orphan plus every reachable CAS object were scanned.
    assert!(
        report.objects_scanned >= 2,
        "expected at least reachable + orphan, got {report:?}"
    );
    assert!(
        report.objects_deleted >= 1,
        "expected at least one orphan deleted, got {report:?}"
    );

    // Orphan is gone.
    assert!(
        storage.size(&orphan_hash).is_err(),
        "orphan should have been deleted"
    );

    assert!(
        storage.size(&reachable_hash).is_ok(),
        "reachable object should survive GC"
    );

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn remote_gc_dry_run_does_not_delete_on_s3() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcdryrun-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "dry run\n")], "c1");
    origin.publish();

    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let storage = make_s3_storage(&env, &prefix).expect("storage");
    let orphan_data = b"dry-run-orphan";
    let orphan_hash = sha256_hex(orphan_data);
    storage.put(&orphan_hash, orphan_data).expect("put orphan");
    let orphan_at = Instant::now();

    // Make sure the orphan is older than the grace period we will use.
    wait_for_grace_since(orphan_at, Duration::from_secs(1)).await;

    let ref_store = make_s3_ref_store(storage.clone());
    let gc = RemoteGc::new(
        storage.clone(),
        ref_store,
        GcConfig {
            grace_period: Duration::from_secs(1),
            dry_run: true,
            ..Default::default()
        },
    );
    // First dry-run pass tombstones (would_delete=0); after grace a second pass
    // reports it as a would-delete candidate without removing it.
    let _ = gc.run().await.expect("remote gc dry run first");
    let tombstoned_at = Instant::now();
    wait_for_grace_since(tombstoned_at, Duration::from_secs(1)).await;
    let report = gc.run().await.expect("remote gc dry run second");
    assert!(
        report.objects_deleted >= 1,
        "dry-run should report at least one deletion, got {report:?}"
    );

    // The orphan must still be present.
    assert!(
        storage.size(&orphan_hash).is_ok(),
        "dry-run must not delete objects"
    );

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

/// Race: RemoteGc with grace=0 must not corrupt a clone that is stalled
/// mid-chunk. We deterministically stall the first signed-URL GET in a proxy
/// after it has sent a few bytes, run GC while the download is blocked, then
/// release the barrier. The clone either completes with a correct tree or fails
/// cleanly without leaving a partial target directory.
#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn remote_gc_during_faulting_clone_is_safe() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcrace-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());

    // Stall the first signed-URL GET mid-body; GC will run while the proxy is
    // blocked. close_on_proceed=false so the clone can finish after release.
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let proxy = start_barrier_proxy(&env.endpoint, 16, false, entered_tx, proceed_rx).await;
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "gc race\n"), ("b.txt", "x\n")], "c1");
    origin.publish();

    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    // Redirect only the presigned artifact URLs through the barrier proxy.
    // Serialize editable downloads so the first large signed-URL GET deterministically
    // hits the barrier rather than racing with other concurrent fetches.
    unsafe {
        std::env::set_var("RIPCLONE_TEST_SIGNED_URL_PROXY", &proxy.url);
        std::env::set_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY", "1");
    }

    // Start the clone on a faulting server and let it begin resolving/downloading.
    let client = server.client();
    let repo_path = format!("acme/{repo}");
    let clone_task = tokio::spawn(async move {
        let out = tempfile::tempdir().expect("clone temp dir");
        let target = out.path().join("clone");
        let result = client
            .install_repo_with_mode_at(
                &repo_path,
                "HEAD",
                Some("HEAD"),
                &target,
                CloneMode::Editable,
                Some("shallow"),
                None,
            )
            .await;
        (result, out, target)
    });

    // Wait until the proxy has forwarded the response headers and a few body
    // bytes, so we know the clone is truly mid-download before running GC.
    tokio::time::timeout(Duration::from_secs(30), entered_rx)
        .await
        .expect("proxy barrier entered within 30s")
        .expect("proxy barrier entered");

    let storage = make_s3_storage(&env, &prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage.clone());
    let gc = RemoteGc::new(
        storage.clone(),
        ref_store,
        GcConfig {
            grace_period: Duration::ZERO,
            dry_run: false,
            ..Default::default()
        },
    );
    let report = gc.run().await.expect("remote gc run during clone");
    eprintln!("GC during clone: {report:?}");

    // Release the barrier and let the clone finish (or fail cleanly).
    proceed_tx.send(()).expect("release barrier");

    let (result, _out, target) = clone_task.await.expect("clone task joined");
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_SIGNED_URL_PROXY");
        std::env::remove_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY");
    }
    match result {
        Ok(_) => {
            assert!(target.exists(), "successful clone must materialize target");
            assert_eq!(
                std::fs::read_to_string(target.join("a.txt")).unwrap_or_default(),
                "gc race\n",
                "clone content must be intact"
            );
            assert_eq!(
                std::fs::read_to_string(target.join("b.txt")).unwrap_or_default(),
                "x\n",
                "clone content must be intact"
            );
        }
        Err(_) => {
            assert!(
                !target.exists(),
                "failed clone must not leave a partial tree at target"
            );
        }
    }

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

/// Signed URLs with a TTL shorter than the request latency must fail cleanly
/// with an actionable stale-URL error, never a partial tree. A local MinIO is
/// too fast for a 10 MiB download to outlive a 1-second TTL, so we insert a
/// delay proxy that holds every GET for longer than the TTL before forwarding
/// it to storage.
#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn expired_signed_url_fails_clone_cleanly() {
    let direct_env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;

    // All direct S3 cleanup must talk to MinIO, not the proxy.
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("sigurl-{suffix}");
    let mut guard = CleanupGuard::new(direct_env.clone(), prefix.clone());

    // Hold signed-URL GETs for longer than the TTL so they expire mid-request.
    // The server uses MinIO directly for storage API traffic; only the presigned
    // URLs are rewritten to point at this proxy.
    let proxy = start_delay_proxy(&direct_env.endpoint, Duration::from_secs(4)).await;
    let server = start_s3_server(&direct_env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "signed-url race\n")], "c1");
    origin.publish();

    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    // Short signed-URL TTL plus serial editable fetches. The TTL is read when
    // the ref response is built, so it must be set before the clone resolves.
    // Redirect only the presigned URLs through the delay proxy.
    unsafe {
        std::env::set_var("RIPCLONE_SIGNED_URL_TTL_SECS", "1");
        std::env::set_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY", "1");
        std::env::set_var("RIPCLONE_TEST_SIGNED_URL_PROXY", &proxy.url);
    }

    let client = server.client();
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let res = client
        .install_repo_with_mode_at(
            &format!("acme/{repo}"),
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            None,
            None,
        )
        .await;
    unsafe {
        std::env::remove_var("RIPCLONE_SIGNED_URL_TTL_SECS");
        std::env::remove_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY");
        std::env::remove_var("RIPCLONE_TEST_SIGNED_URL_PROXY");
    }

    assert!(
        res.is_err(),
        "clone with expired signed URLs must fail, got {res:?}"
    );
    assert!(
        ripclone::client::is_stale_signed_url(&res.unwrap_err()),
        "expected StaleSignedUrl in error chain"
    );
    assert!(
        !target.exists(),
        "failed clone must not leave a partial tree at target"
    );

    cleanup_prefix(&direct_env, &prefix)
        .await
        .expect("cleanup prefix");
    cleanup_repo_refs(&direct_env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn expired_signed_url_retry_stays_on_pinned_commit() {
    let direct_env = match s3_env() {
        Some(env) => env,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("pinrefresh-{suffix}");
    let mut guard = CleanupGuard::new(direct_env.clone(), prefix.clone());

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let proxy = start_barrier_proxy(&direct_env.endpoint, 16, true, entered_tx, proceed_rx).await;
    let verifier = Arc::new(ToggleAccessVerifier::new(true));
    let server = start_s3_server_authorized(
        &direct_env,
        &prefix,
        Arc::clone(&verifier) as Arc<dyn AccessVerifier>,
    )
    .await;
    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("value.txt", "A\n"), ("stable.txt", "stable\n")], "A");
    origin.publish();
    let (pinned, exact_store, exact_a, _fixture) =
        seed_shallow_s3_fixture(&direct_env, &prefix, &repo).await;
    let repo_id = RepoId::github(format!("acme/{repo}"));
    add_acme_repo(&server, &repo).await;

    unsafe {
        std::env::set_var("RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS", "1");
        std::env::set_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY", "1");
        std::env::set_var("RIPCLONE_TEST_SIGNED_URL_PROXY", &proxy.url);
    }
    let out = tempfile::tempdir().expect("clone out");
    let target = out.path().join("clone");
    let binary = required_ripclone_bin();
    let mut child = std::process::Command::new(&binary)
        .arg("--server")
        .arg(&server.url)
        .arg("clone")
        .arg(format!("acme/{repo}"))
        .arg(&target)
        .arg("--depth")
        .arg("1")
        .arg("--no-metrics")
        .arg("--verify-upstream=never")
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_NO_METRICS", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn release CLI clone");

    if !matches!(
        tokio::time::timeout(Duration::from_secs(30), entered_rx).await,
        Ok(Ok(()))
    ) {
        let _ = child.kill();
        let output =
            wait_child_output_bounded(child, Duration::from_secs(5)).expect("wait failed clone");
        panic!(
            "CLI clone never reached the signed-URL barrier\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    origin.commit(&[("value.txt", "B\n"), ("stable.txt", "stable\n")], "B");
    origin.publish();
    let newer = git(&origin.bare, &["rev-parse", "HEAD"]);
    assert_ne!(pinned, newer);
    publish_moving_s3_row(&exact_store, &repo_id, &exact_a, &newer).await;
    let published_b = exact_store
        .load_branch(&repo_id, "HEAD")
        .await
        .expect("load published S3 B row")
        .expect("published S3 B row present")
        .commit;
    assert_eq!(published_b, newer);
    let authorization_calls_before_refresh = verifier.calls();
    sleep(Duration::from_secs(2)).await;
    proceed_tx
        .send(())
        .expect("expire and close first signed request");

    let output = wait_child_output(child).await;
    unsafe {
        std::env::remove_var("RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS");
        std::env::remove_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY");
        std::env::remove_var("RIPCLONE_TEST_SIGNED_URL_PROXY");
    }
    assert!(
        output.status.success(),
        "pinned refresh clone failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), pinned);
    assert_eq!(
        std::fs::read_to_string(target.join("value.txt")).unwrap(),
        "A\n"
    );
    assert!(git_ok(&target, &["fsck", "--connectivity-only", "HEAD"]));
    assert!(
        verifier.calls() > authorization_calls_before_refresh,
        "pinned signed-URL refresh must re-enter repository authorization"
    );
    let headers = proxy.signed_headers();
    assert!(
        headers.len() >= 2,
        "stale attempt plus refreshed signed request"
    );
    assert!(
        headers
            .iter()
            .all(|header| !header.to_ascii_lowercase().contains("authorization:")),
        "artifact-host requests must not carry Ripclone authorization: {headers:?}"
    );

    cleanup_prefix(&direct_env, &prefix)
        .await
        .expect("cleanup prefix");
    cleanup_repo_refs(&direct_env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn revoked_authorization_blocks_pinned_refresh() {
    let direct_env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;

    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("comboexp-{suffix}");
    let mut guard = CleanupGuard::new(direct_env.clone(), prefix.clone());

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let proxy = start_barrier_proxy(&direct_env.endpoint, 16, true, entered_tx, proceed_rx).await;
    let verifier = Arc::new(ToggleAccessVerifier::new(true));
    let server = start_s3_server_authorized(
        &direct_env,
        &prefix,
        Arc::clone(&verifier) as Arc<dyn AccessVerifier>,
    )
    .await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("value.txt", "A\n"), ("stable.txt", "stable\n")], "A");
    origin.publish();
    let (pinned, exact_store, exact_a, _fixture) =
        seed_shallow_s3_fixture(&direct_env, &prefix, &repo).await;
    let repo_id = RepoId::github(format!("acme/{repo}"));
    add_acme_repo(&server, &repo).await;

    unsafe {
        std::env::set_var("RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS", "1");
        std::env::set_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY", "1");
        std::env::set_var("RIPCLONE_TEST_SIGNED_URL_PROXY", &proxy.url);
    }

    let out = tempfile::tempdir().expect("clone out");
    let target = out.path().join("clone");
    let ripclone_bin = required_ripclone_bin();
    let mut child = std::process::Command::new(&ripclone_bin)
        .arg("--server")
        .arg(&server.url)
        .arg("clone")
        .arg(format!("acme/{repo}"))
        .arg(&target)
        .arg("--depth")
        .arg("1")
        .arg("--no-metrics")
        .arg("--verify-upstream=never")
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_NO_METRICS", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn ripclone clone");

    if !matches!(
        tokio::time::timeout(Duration::from_secs(30), entered_rx).await,
        Ok(Ok(()))
    ) {
        let _ = child.kill();
        let output =
            wait_child_output_bounded(child, Duration::from_secs(5)).expect("wait failed clone");
        panic!(
            "CLI clone never reached the signed-URL barrier\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    origin.commit(&[("value.txt", "B\n"), ("stable.txt", "stable\n")], "B");
    origin.publish();
    let newer = git(&origin.bare, &["rev-parse", "HEAD"]);
    assert_ne!(pinned, newer);
    publish_moving_s3_row(&exact_store, &repo_id, &exact_a, &newer).await;
    verifier.set_allowed(false);
    sleep(Duration::from_secs(2)).await;
    proceed_tx.send(()).expect("release signed-URL barrier");

    let output = wait_child_output(child).await;
    unsafe {
        std::env::remove_var("RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS");
        std::env::remove_var("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY");
        std::env::remove_var("RIPCLONE_TEST_SIGNED_URL_PROXY");
    }

    assert!(
        !output.status.success(),
        "revoked authorization must fail the pinned refresh, stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("403") || combined.to_lowercase().contains("access denied"),
        "retry after stale signed URL must fail at repository authorization, got:\n{combined}"
    );
    assert!(
        combined.contains(&pinned),
        "authorization error must identify pinned commit {pinned}, got:\n{combined}"
    );
    assert!(
        !target.exists(),
        "denied pinned refresh must not leave a partial checkout"
    );
    assert!(
        proxy
            .signed_headers()
            .iter()
            .all(|header| !header.to_ascii_lowercase().contains("authorization:")),
        "artifact-host requests must not carry Ripclone authorization"
    );

    cleanup_prefix(&direct_env, &prefix)
        .await
        .expect("cleanup prefix");
    cleanup_repo_refs(&direct_env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn status_reports_bytes_from_s3() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("billings3-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "bill me\n")], "c1");
    origin.publish();

    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let status = get_status(&server, "acme", &repo, None).await;
    assert_eq!(status["owner"], "acme");
    assert_eq!(status["repo"], repo);
    assert!(status["refs"][0]["bytes"].as_u64().unwrap() > 0);
    assert_eq!(
        status["refs"][0]["bytes"],
        status["refs"][0]["unique_bytes"]
    );
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    assert_eq!(status["total_bytes"], status["total_unique_bytes"]);
    assert!(!status["regions"].as_array().unwrap().is_empty());
    assert!(status["regions"][0]["unique_bytes"].as_u64().unwrap() > 0);

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

/// Age *every* ref of a repo — the literal `HEAD` alias and the concrete default
/// branch — so the whole repo is uniformly idle. Warm-TTL eviction is
/// repo-scoped: a repo is only evicted when all of its refs are idle past the
/// TTL, so aging only the `HEAD` alias leaves the sibling default-branch ref
/// (written by the detached phase-2 build, and holding the full-history
/// artifacts) up to build timing. Enumerate the refs and age them all, reading
/// through the cache (invalidate first) so the durable ref is what we mutate.
///
/// When `pin` is true, also set `warm_pinned` on every ref. The pin is
/// repo-scoped for GC, but `/status` only surfaces refs that carry clonepack
/// manifests — a pin written only on a thin `HEAD` alias (no manifests) is
/// invisible in the status response even though GC honors it. Pinning every
/// ref makes the status assertion deterministic.
async fn age_all_refs(env: &S3Env, prefix: &str, owner: &str, repo: &str) {
    mutate_all_refs(env, prefix, owner, repo, false).await;
}

async fn age_and_pin_all_refs(env: &S3Env, prefix: &str, owner: &str, repo: &str) {
    mutate_all_refs(env, prefix, owner, repo, true).await;
}

async fn mutate_all_refs(env: &S3Env, prefix: &str, owner: &str, repo: &str, pin: bool) {
    let storage = make_s3_storage(env, prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage);
    let repo_id = RepoId::github(format!("{owner}/{repo}"));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let aged = now.saturating_sub(86400);
    let branches = ref_store
        .list_branches(&repo_id)
        .await
        .expect("list branches to mutate");
    assert!(!branches.is_empty(), "repo has at least one ref to mutate");
    for branch in branches {
        ref_store.invalidate(&repo_id, &branch).await;
        let Some(mut info) = ref_store
            .load_branch(&repo_id, &branch)
            .await
            .expect("load ref to mutate")
        else {
            continue;
        };
        info.last_accessed_at = Some(aged);
        info.synced_at = Some(aged);
        if pin {
            info.warm_pinned = true;
        }
        ref_store
            .save_branch(&repo_id, &branch, &info)
            .await
            .expect("save mutated ref");
    }
}

async fn run_gc(
    env: &S3Env,
    prefix: &str,
    warm_ttl: Duration,
    dry_run: bool,
) -> ripclone::remote_gc::GcReport {
    let storage = make_s3_storage(env, prefix).expect("storage");
    let ref_store = make_s3_ref_store(storage.clone());
    let gc = RemoteGc::new(
        storage,
        ref_store,
        GcConfig {
            grace_period: Duration::from_secs(0),
            warm_ttl,
            dry_run,
        },
    );
    gc.run().await.expect("gc run")
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn warm_ttl_evicts_idle_ref_and_status_reports_cold() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcwarm-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "warm me\n")], "c1");
    origin.publish();
    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let status = get_status(&server, "acme", &repo, None).await;
    assert!(status["refs"][0]["warm"].as_bool().unwrap());

    // Settle phase 2 before aging. The detached full-history build writes a
    // second ref (the concrete default branch) that holds the full artifacts and,
    // while it is mid-flight ("archive building"), shares the very chunks the
    // `HEAD` alias points at. Warm-TTL eviction is repo-scoped, so if that sibling
    // ref is still fresh, evicting the aged `HEAD` alone deletes nothing. Wait for
    // the build to finish, then age *every* ref so the whole repo is uniformly
    // idle and the eviction is deterministic.
    wait_for_full_build(&env, &prefix, "acme", &repo).await;
    age_all_refs(&env, &prefix, "acme", &repo).await;

    let report = run_gc(&env, &prefix, Duration::from_secs(1), false).await;
    assert!(
        report.objects_deleted > 0,
        "GC should delete idle artifacts"
    );

    let status = get_status(&server, "acme", &repo, None).await;
    assert!(!status["refs"][0]["warm"].as_bool().unwrap());
    assert_eq!(status["refs"][0]["bytes"], 0);

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn warm_ttl_keeps_pinned_ref() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcpin-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "pin me\n")], "c1");
    origin.publish();
    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    // Let phase 2 finish before aging/pinning, so the concrete default-branch
    // ref that holds the full-history artifacts is stable (and not still being
    // rewritten by the detached build) when GC runs.
    //
    // Age *and pin* every ref. Pinning only HEAD used to flake: `/status` skips
    // refs with empty clonepack manifests (a thin HEAD alias often has none), so
    // the status response could list only the concrete default branch — which
    // was never pinned — and `a ref reports the pin` failed even though GC
    // correctly honored the repo-scoped pin on HEAD.
    wait_for_full_build(&env, &prefix, "acme", &repo).await;
    age_and_pin_all_refs(&env, &prefix, "acme", &repo).await;

    // grace_period=0: any genuinely-orphaned object is deleted this pass. The pin
    // is repo-scoped, so *no* ref may be evicted. A two-phase build also leaves
    // one unreferenced byproduct (the editable clonepack manifest, superseded by
    // the files manifest); reclaiming that is correct GC and unrelated to the
    // pin, so we assert the repo's refs survive rather than a literal zero-delete
    // count.
    run_gc(&env, &prefix, Duration::from_secs(1), false).await;

    let status = get_status(&server, "acme", &repo, None).await;
    let refs = status["refs"].as_array().expect("status refs");
    assert!(!refs.is_empty(), "pinned repo still has refs");
    for r in refs {
        assert!(
            r["warm"].as_bool().unwrap(),
            "pinned repo ref {} must not be evicted: {r}",
            r["branch"]
        );
        assert!(
            r["bytes"].as_u64().unwrap() > 0,
            "pinned repo ref {} must keep its artifacts: {r}",
            r["branch"]
        );
    }
    let pinned_ref = refs
        .iter()
        .find(|r| r["pinned"].as_bool().unwrap_or(false))
        .expect("a ref reports the pin");
    assert!(pinned_ref["warm"].as_bool().unwrap());
    assert!(pinned_ref["bytes"].as_u64().unwrap() > 0);

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn warm_ttl_marks_evicted_ref_cold() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("gcrebuild-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "rebuild me\n")], "c1");
    origin.publish();
    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    // Settle phase 2 before aging, then age *every* ref (the `HEAD` alias and
    // the concrete default branch) so the whole repo is uniformly idle:
    // eviction is repo-scoped, so a single fresh sibling ref would keep the repo
    // warm and leave `refs[0]` reporting warm below.
    wait_for_full_build(&env, &prefix, "acme", &repo).await;
    age_all_refs(&env, &prefix, "acme", &repo).await;

    run_gc(&env, &prefix, Duration::from_secs(1), false).await;

    let status = get_status(&server, "acme", &repo, None).await;
    assert!(!status["refs"][0]["warm"].as_bool().unwrap());

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}

#[ignore = "requires S3 credentials"]
#[tokio::test]
async fn public_fork_status_is_free_on_s3() {
    let env = match s3_env() {
        Some(e) => e,
        None => {
            eprintln!("SKIP: RIPCLONE_S3_ENDPOINT/BUCKET not set");
            return;
        }
    };
    let _server_lock = SERVER_LOCK.lock().await;
    let prefix = unique_prefix();
    let suffix = repo_suffix(&prefix);
    let repo = format!("forks3-{suffix}");
    let mut guard = CleanupGuard::new(env.clone(), prefix.clone());
    let server = start_s3_server(&env, &prefix).await;

    let origin = make_origin("acme", &repo);
    guard.track_repo("acme", &repo);
    origin.commit(&[("a.txt", "fork me\n")], "c1");
    origin.publish();

    add_acme_repo(&server, &repo).await;
    server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync");

    let status = get_status(
        &server,
        "acme",
        &repo,
        Some("public=true&fork_of=upstream/repo"),
    )
    .await;
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
    assert_eq!(status["total_unique_bytes"], 0);
    assert_eq!(status["refs"][0]["unique_bytes"], 0);
    assert_eq!(status["regions"][0]["unique_bytes"], 0);

    cleanup_prefix(&env, &prefix).await.expect("cleanup prefix");
    cleanup_repo_refs(&env, "acme", &repo)
        .await
        .expect("cleanup refs");
    guard.disable();
}
