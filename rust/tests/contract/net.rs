//! Network fixtures that inject faults from outside the product: a smart-HTTP
//! git origin that can hold fetches, and a reverse proxy in front of the
//! server that can corrupt, cut, hold, or rewrite responses.
//!
//! Both run on their own thread with their own Tokio runtime, so tests stay
//! plain blocking code.
#![allow(dead_code)]

use crate::harness::{Endpoint, WAIT};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::Response;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, watch};

/// Serve `router` on a free local port from a background thread.
fn serve(router: Router) -> (String, oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fixture");
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let server = axum::serve(listener, router);
            tokio::select! {
                _ = server => {}
                _ = stop_rx => {}
            }
        });
        // Held requests must not keep the thread alive after the test.
        runtime.shutdown_timeout(Duration::from_millis(100));
    });
    (url, stop_tx)
}

/// A gate that requests wait on while it is closed.
struct Gate {
    open: watch::Sender<bool>,
    waiting: AtomicUsize,
}

impl Gate {
    fn new() -> Self {
        Self {
            open: watch::Sender::new(true),
            waiting: AtomicUsize::new(0),
        }
    }

    async fn pass(&self) {
        let mut rx = self.open.subscribe();
        if *rx.borrow() {
            return;
        }
        self.waiting.fetch_add(1, Ordering::SeqCst);
        let _ = rx.wait_for(|open| *open).await;
        self.waiting.fetch_sub(1, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Gated smart-HTTP origin
// ---------------------------------------------------------------------------

struct GitState {
    root: PathBuf,
    gate: Gate,
    fetches: AtomicUsize,
    requests: Mutex<Vec<OriginRequest>>,
    /// Accepted `name: value` headers; empty accepts every request.
    accepted: Mutex<Vec<(String, String)>>,
    /// A fetch carrying this header passes a closed gate.
    unheld: Mutex<Option<(String, String)>>,
    /// Fetches of these repos wait on `repo_gate`.
    held_repos: Mutex<Vec<String>>,
    repo_gate: Gate,
    /// Ref advertisements (`GET .../info/refs`) wait on this.
    listing_gate: Gate,
}

/// One request the origin received.
#[derive(Clone, Debug)]
pub struct OriginRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
}

impl OriginRequest {
    /// The value of header `name` (case-insensitive), if sent.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// `git http-backend` over HTTP for a directory of bare repos. While held, it
/// parks every upload-pack fetch; ref listings (v0 `info/refs`, v2 `ls-refs`)
/// still answer, so a server can resolve a branch but not build it. Ref
/// listings have their own hold.
pub struct GitHttp {
    url: String,
    state: Arc<GitState>,
    stop: Option<oneshot::Sender<()>>,
}

impl GitHttp {
    pub fn start(root: PathBuf) -> Self {
        let state = Arc::new(GitState {
            root,
            gate: Gate::new(),
            fetches: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            accepted: Mutex::new(Vec::new()),
            unheld: Mutex::new(None),
            held_repos: Mutex::new(Vec::new()),
            repo_gate: Gate::new(),
            listing_gate: Gate::new(),
        });
        let router = Router::new()
            .fallback(git_backend)
            .with_state(Arc::clone(&state));
        let (url, stop) = serve(router);
        Self {
            url,
            state,
            stop: Some(stop),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Park upload-pack fetches until [`GitHttp::release_fetches`].
    pub fn hold_fetches(&self) {
        self.state.gate.open.send_replace(false);
    }

    /// Park upload-pack fetches except those carrying `name: value`, so one
    /// caller's fetches pass while every other caller's wait.
    pub fn hold_fetches_without(&self, name: &str, value: &str) {
        *self.state.unheld.lock().unwrap() = Some((name.to_string(), value.to_string()));
        self.hold_fetches();
    }

    /// Park upload-pack fetches of one repo (`owner/name`) only; every other
    /// repo's fetches pass.
    pub fn hold_fetches_of(&self, repo: &str) {
        let path = format!("/{repo}.git/");
        self.state.held_repos.lock().unwrap().push(path);
        self.state.repo_gate.open.send_replace(false);
    }

    pub fn release_fetches(&self) {
        self.state.held_repos.lock().unwrap().clear();
        self.state.repo_gate.open.send_replace(true);
        self.state.gate.open.send_replace(true);
    }

    /// Park ref advertisements, the first request of every `ls-remote` and
    /// fetch, until [`GitHttp::release_listings`].
    pub fn hold_listings(&self) {
        self.state.listing_gate.open.send_replace(false);
    }

    pub fn release_listings(&self) {
        self.state.listing_gate.open.send_replace(true);
    }

    /// Wait until at least `n` ref advertisements are parked.
    pub fn wait_listings_held(&self, n: usize) {
        crate::harness::wait_for(&format!("{n} held ref listing(s)"), || {
            (self.state.listing_gate.waiting.load(Ordering::SeqCst) >= n).then_some(())
        });
    }

    /// Wait until at least `n` fetches are parked.
    pub fn wait_held(&self, n: usize) {
        crate::harness::wait_for(&format!("{n} held origin fetch(es)"), || {
            (self.held() >= n).then_some(())
        });
    }

    /// Fetches parked now.
    pub fn held(&self) -> usize {
        self.state.gate.waiting.load(Ordering::SeqCst)
            + self.state.repo_gate.waiting.load(Ordering::SeqCst)
    }

    /// Upload-pack fetch requests seen so far, held or not.
    pub fn fetches(&self) -> usize {
        self.state.fetches.load(Ordering::SeqCst)
    }

    /// Answer 403 to any request that carries none of the required headers.
    /// Each call accepts one more `name: value`.
    pub fn require_header(&self, name: &str, value: &str) {
        let header = (name.to_string(), value.to_string());
        self.state.accepted.lock().unwrap().push(header);
    }

    /// Every request received so far, in order.
    pub fn requests(&self) -> Vec<OriginRequest> {
        self.state.requests.lock().unwrap().clone()
    }
}

impl Drop for GitHttp {
    fn drop(&mut self) {
        self.release_fetches();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

async fn git_backend(State(state): State<Arc<GitState>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    let path = parts.uri.path().to_string();
    let request = OriginRequest {
        method: parts.method.to_string(),
        path: path.clone(),
        headers: parts
            .headers
            .iter()
            .map(|(n, v)| {
                (
                    n.to_string(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect(),
    };
    let carries = |(name, value): &(String, String)| request.header(name) == Some(value);
    let allowed = {
        let accepted = state.accepted.lock().unwrap();
        accepted.is_empty() || accepted.iter().any(carries)
    };
    let unheld = state.unheld.lock().unwrap().as_ref().is_some_and(carries);
    let repo_held = state
        .held_repos
        .lock()
        .unwrap()
        .iter()
        .any(|repo| path.contains(repo.as_str()));
    state.requests.lock().unwrap().push(request);
    if !allowed {
        return plain(StatusCode::FORBIDDEN, "missing required header".into());
    }
    // A v2 fetch and a v0 negotiation are both POSTs to git-upload-pack; a v2
    // `ls-refs` is too, but it only lists refs. Large request bodies may be
    // gzipped, and only fetches are large.
    let is_fetch = parts.method == Method::POST
        && path.ends_with("/git-upload-pack")
        && !contains(&body, b"command=ls-refs");
    if parts.method == Method::GET && path.ends_with("/info/refs") {
        state.listing_gate.pass().await;
    }
    if is_fetch {
        state.fetches.fetch_add(1, Ordering::SeqCst);
        if !unheld {
            state.gate.pass().await;
        }
        if repo_held {
            state.repo_gate.pass().await;
        }
    }

    let mut cmd = tokio::process::Command::new("git");
    cmd.arg("http-backend")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "uploadpack.allowAnySHA1InWant")
        .env("GIT_CONFIG_VALUE_0", "true")
        .env("GIT_PROJECT_ROOT", &state.root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REQUEST_METHOD", parts.method.as_str())
        .env("PATH_INFO", &path)
        .env("QUERY_STRING", parts.uri.query().unwrap_or(""))
        .env("CONTENT_LENGTH", body.len().to_string())
        .env("REMOTE_ADDR", "127.0.0.1");
    for (cgi, name) in [
        ("CONTENT_TYPE", header::CONTENT_TYPE.as_str()),
        ("HTTP_CONTENT_ENCODING", header::CONTENT_ENCODING.as_str()),
        ("HTTP_GIT_PROTOCOL", "git-protocol"),
    ] {
        if let Some(value) = parts.headers.get(name).and_then(|v| v.to_str().ok()) {
            cmd.env(cgi, value);
        }
    }
    let mut child = match cmd
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, format!("spawn: {e}")),
    };
    let mut stdin = child.stdin.take().unwrap();
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let _ = stdin.write_all(&body).await;
    });
    let out = match child.wait_with_output().await {
        Ok(out) => out,
        Err(e) => return plain(StatusCode::INTERNAL_SERVER_ERROR, format!("wait: {e}")),
    };
    cgi_response(&out.stdout)
}

/// Turn CGI output (headers, blank line, body) into a response.
fn cgi_response(out: &[u8]) -> Response {
    let (head, body) = match find(out, b"\r\n\r\n") {
        Some(i) => (&out[..i], &out[i + 4..]),
        None => match find(out, b"\n\n") {
            Some(i) => (&out[..i], &out[i + 2..]),
            None => return plain(StatusCode::BAD_GATEWAY, "malformed CGI output".into()),
        },
    };
    let mut builder = Response::builder().status(StatusCode::OK);
    for line in String::from_utf8_lossy(head).lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("status") {
            let code = value.split_whitespace().next().unwrap_or("500");
            builder = builder.status(code.parse::<u16>().unwrap_or(500));
        } else {
            builder = builder.header(name.trim(), value);
        }
    }
    builder.body(Body::from(body.to_vec())).unwrap()
}

fn plain(status: StatusCode, text: String) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(text))
        .unwrap()
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    find(haystack, needle).is_some()
}

// ---------------------------------------------------------------------------
// Fault proxy
// ---------------------------------------------------------------------------

/// What the proxy does to a matching artifact response.
#[derive(Clone, Debug)]
pub enum Fault {
    /// Promise the full Content-Length, send this many bytes, then drop the
    /// connection.
    CutAfter(usize),
    /// Like `CutAfter`, but stall this long before dropping the connection.
    StallThenCut(usize, Duration),
    /// Flip one byte in the middle of the body.
    FlipByte,
    /// Send only this many bytes, with a matching Content-Length.
    Truncate(usize),
    /// Replace the response with this status and an empty body.
    Status(u16),
    /// Do not answer until [`FaultProxy::release`].
    Hold,
}

struct Rule {
    /// Only requests with this method, or any method.
    method: Option<Method>,
    prefix: String,
    fault: Fault,
    min_len: usize,
    remaining: usize,
}

struct Rewrite {
    when: String,
    from: String,
    to: String,
}

/// One request the proxy answered.
#[derive(Clone, Debug)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub query: String,
    pub range: Option<String>,
    pub status: u16,
    /// A fault rule fired on this response.
    pub faulted: bool,
}

struct ProxyState {
    upstream: String,
    http: reqwest::Client,
    rules: Mutex<Vec<Rule>>,
    rewrites: Mutex<Vec<Rewrite>>,
    /// Request headers set (replaced) before forwarding.
    set_headers: Mutex<Vec<(header::HeaderName, header::HeaderValue)>>,
    ignore_range: AtomicBool,
    preserve_host: AtomicBool,
    gate: Gate,
    seen: Mutex<Vec<Seen>>,
}

/// A reverse proxy in front of a server. The CLI talks to it in place of the
/// server; with local storage every artifact download passes through it.
pub struct FaultProxy {
    url: String,
    state: Arc<ProxyState>,
    stop: Option<oneshot::Sender<()>>,
}

impl FaultProxy {
    pub fn start(upstream: &impl Endpoint) -> Self {
        let state = Arc::new(ProxyState {
            upstream: upstream.url(),
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            rules: Mutex::new(Vec::new()),
            rewrites: Mutex::new(Vec::new()),
            set_headers: Mutex::new(Vec::new()),
            ignore_range: AtomicBool::new(false),
            preserve_host: AtomicBool::new(false),
            gate: Gate::new(),
            seen: Mutex::new(Vec::new()),
        });
        let router = Router::new().fallback(proxy).with_state(Arc::clone(&state));
        let (url, stop) = serve(router);
        Self {
            url,
            state,
            stop: Some(stop),
        }
    }

    /// Apply `fault` to the next `times` artifact responses whose body is at
    /// least `min_len` bytes.
    pub fn fault_artifacts(&self, fault: Fault, min_len: usize, times: usize) {
        self.add_rule(None, "/v1/artifacts/", fault, min_len, times);
    }

    /// Apply `fault` to the next `times` responses to any request whose path
    /// starts with `prefix`.
    pub fn fault_requests(&self, prefix: &str, fault: Fault, times: usize) {
        self.add_rule(None, prefix, fault, 0, times);
    }

    /// Apply `fault` to the responses of every `PUT` whose path starts with
    /// `prefix`, as a store that rejects writes.
    pub fn fail_puts(&self, prefix: &str, fault: Fault) {
        self.add_rule(Some(Method::PUT), prefix, fault, 0, usize::MAX);
    }

    /// Hold the responses to the next `times` `PUT`s whose path starts with
    /// `prefix`, until [`FaultProxy::release`]. Later writes pass.
    pub fn hold_puts(&self, prefix: &str, times: usize) {
        self.add_rule(Some(Method::PUT), prefix, Fault::Hold, 0, times);
    }

    /// Apply `fault` to the next `times` responses of at least `min_len`
    /// bytes to requests whose path starts with `prefix`.
    pub fn fault_large(&self, prefix: &str, fault: Fault, min_len: usize, times: usize) {
        self.add_rule(None, prefix, fault, min_len, times);
    }

    /// Forward the client's Host header instead of the upstream's, so
    /// signatures over the host (S3 presigned URLs) still verify.
    pub fn preserve_host(&self) {
        self.state.preserve_host.store(true, Ordering::SeqCst);
    }

    fn add_rule(
        &self,
        method: Option<Method>,
        prefix: &str,
        fault: Fault,
        min_len: usize,
        times: usize,
    ) {
        if matches!(fault, Fault::Hold) {
            self.state.gate.open.send_replace(false);
        }
        self.state.rules.lock().unwrap().push(Rule {
            method,
            prefix: prefix.to_string(),
            fault,
            min_len,
            remaining: times,
        });
    }

    /// Answer every held response and drop all fault rules.
    pub fn release(&self) {
        self.state.rules.lock().unwrap().clear();
        self.state.gate.open.send_replace(true);
    }

    /// Wait until at least `n` responses are held.
    pub fn wait_held(&self, n: usize) {
        crate::harness::wait_for(&format!("{n} held response(s)"), || {
            (self.state.gate.waiting.load(Ordering::SeqCst) >= n).then_some(())
        });
    }

    /// Forward every request with header `name` set to `value`, replacing
    /// what the client sent.
    pub fn set_request_header(&self, name: &str, value: &str) {
        let name = header::HeaderName::from_bytes(name.as_bytes()).unwrap();
        let value = header::HeaderValue::from_str(value).unwrap();
        self.state.set_headers.lock().unwrap().push((name, value));
    }

    /// Strip `Range` from requests, as a server without Range support would
    /// ignore it.
    pub fn ignore_range(&self) {
        self.state.ignore_range.store(true, Ordering::SeqCst);
    }

    /// In responses to requests whose path and query contain `when`, replace
    /// `from` with `to` in the body.
    pub fn rewrite(&self, when: &str, from: &str, to: &str) {
        self.state.rewrites.lock().unwrap().push(Rewrite {
            when: when.to_string(),
            from: from.to_string(),
            to: to.to_string(),
        });
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.state.seen.lock().unwrap().clone()
    }

    /// Artifact GETs, in order.
    pub fn artifact_gets(&self) -> Vec<Seen> {
        self.gets("/v1/artifacts/")
    }

    /// GETs whose path starts with `prefix`, in order.
    pub fn gets(&self, prefix: &str) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|s| s.method == "GET" && s.path.starts_with(prefix))
            .collect()
    }

    /// Wait until `check` holds for the requests seen so far.
    pub fn wait_seen(&self, what: &str, check: impl Fn(&[Seen]) -> bool) {
        crate::harness::wait_for(what, || check(&self.seen()).then_some(()));
    }
}

impl Endpoint for FaultProxy {
    fn url(&self) -> String {
        self.url.clone()
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.release();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

const HOP_HEADERS: [header::HeaderName; 4] = [
    header::HOST,
    header::CONNECTION,
    header::CONTENT_LENGTH,
    header::TRANSFER_ENCODING,
];

async fn proxy(State(state): State<Arc<ProxyState>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    let path = parts.uri.path().to_string();
    let query = parts.uri.query().unwrap_or("").to_string();
    let range = parts
        .headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let mut headers = HeaderMap::new();
    for (name, value) in &parts.headers {
        let drop_range = name == header::RANGE && state.ignore_range.load(Ordering::SeqCst);
        let keep_host = name == header::HOST && state.preserve_host.load(Ordering::SeqCst);
        if (keep_host || !HOP_HEADERS.contains(name)) && !drop_range {
            headers.append(name, value.clone());
        }
    }
    for (name, value) in state.set_headers.lock().unwrap().iter() {
        headers.insert(name.clone(), value.clone());
    }
    let target = format!("{}{}", state.upstream, parts.uri);
    let upstream = state
        .http
        .request(parts.method.clone(), target)
        .headers(headers)
        .body(body)
        .timeout(WAIT)
        .send()
        .await;
    let upstream = match upstream {
        Ok(resp) => resp,
        Err(e) => return plain(StatusCode::BAD_GATEWAY, format!("proxy: {e}")),
    };
    let mut status = upstream.status();
    let mut resp_headers = HeaderMap::new();
    for (name, value) in upstream.headers() {
        if !HOP_HEADERS.contains(name) {
            resp_headers.append(name, value.clone());
        }
    }
    // A HEAD response has no body; its length is the object's.
    let head_len = (parts.method == Method::HEAD)
        .then(|| upstream.headers().get(header::CONTENT_LENGTH).cloned())
        .flatten();
    let mut data = upstream.bytes().await.unwrap_or_default().to_vec();

    let target = format!("{path}?{query}");
    for rewrite in state.rewrites.lock().unwrap().iter() {
        if target.contains(&rewrite.when) {
            let text = String::from_utf8_lossy(&data).replace(&rewrite.from, &rewrite.to);
            data = text.into_bytes();
        }
    }

    let fault = {
        let mut rules = state.rules.lock().unwrap();
        rules
            .iter_mut()
            .find(|r| {
                r.remaining > 0
                    && r.method.as_ref().is_none_or(|m| *m == parts.method)
                    && path.starts_with(&r.prefix)
                    && data.len() >= r.min_len
            })
            .map(|r| {
                r.remaining -= 1;
                r.fault.clone()
            })
    };

    let faulted = fault.is_some();
    let mut cut = None;
    match fault {
        None => {}
        Some(Fault::Hold) => state.gate.pass().await,
        Some(Fault::FlipByte) => {
            let mid = data.len() / 2;
            data[mid] ^= 0xff;
        }
        Some(Fault::Truncate(n)) => data.truncate(n),
        Some(Fault::Status(code)) => {
            status = StatusCode::from_u16(code).unwrap();
            data.clear();
            resp_headers.remove(header::CONTENT_RANGE);
        }
        // hyper flushes written bytes when the body is pending, so pause
        // before failing; otherwise the cut can discard the head.
        Some(Fault::CutAfter(n)) => cut = Some((n.min(data.len()), Duration::from_millis(20))),
        Some(Fault::StallThenCut(n, stall)) => cut = Some((n.min(data.len()), stall)),
    }

    state.seen.lock().unwrap().push(Seen {
        method: parts.method.to_string(),
        path,
        query,
        range,
        status: status.as_u16(),
        faulted,
    });

    let mut builder = Response::builder().status(status);
    for (name, value) in &resp_headers {
        builder = builder.header(name, value);
    }
    builder = match head_len {
        Some(len) => builder.header(header::CONTENT_LENGTH, len),
        None => builder.header(header::CONTENT_LENGTH, data.len()),
    };
    match cut {
        None => builder.body(Body::from(data)).unwrap(),
        Some((n, stall)) => {
            use futures::StreamExt;
            let head = Bytes::from(data).slice(..n);
            let cut = futures::stream::once(async move {
                tokio::time::sleep(stall).await;
                Err(std::io::Error::other("fault proxy cut"))
            });
            let body = futures::stream::once(async move { Ok(head) }).chain(cut);
            builder.body(Body::from_stream(body)).unwrap()
        }
    }
}
