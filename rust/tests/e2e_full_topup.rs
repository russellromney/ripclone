//! One-commit Full-clone top-up through real phase-one publication.

mod common;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use axum::routing::any;
use common::*;
use prost::Message;
use ripclone::cas::Cas;
use ripclone::client::ArtifactPending;
use ripclone::clonepack::ClonepackManifest;
use ripclone::mode::CloneMode;
use ripclone::provider::{
    ProviderConfig, ProviderInstance, ProviderInstanceId, ProviderKind, ProviderRegistry, RepoId,
};
use ripclone::ref_store::{FileRefStore, RefStore};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct ScopedEnvVar {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl ScopedEnvVar {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ScopedEnvVar {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

async fn start_failing_upstream_decoy() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind failing upstream decoy");
    let url = format!("http://{}", listener.local_addr().expect("decoy address"));
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            observed.fetch_add(1, Ordering::SeqCst);
            let mut request = [0_u8; 1024];
            let _ = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut request)).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
        }
    });
    (url, requests, task)
}

async fn start_authenticated_redirect_source() -> (
    String,
    Arc<std::sync::Mutex<Option<String>>>,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let target = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind redirect target");
    let target_url = format!("http://{}", target.local_addr().expect("target address"));
    let target_requests = Arc::new(AtomicUsize::new(0));
    let target_observed = target_requests.clone();
    let target_task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = target.accept().await else {
                return;
            };
            target_observed.fetch_add(1, Ordering::SeqCst);
            let mut request = [0_u8; 4096];
            let _ = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut request)).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
        }
    });

    let source = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind redirect source");
    let source_url = format!("http://{}", source.local_addr().expect("source address"));
    let source_request = Arc::new(std::sync::Mutex::new(None));
    let source_observed = source_request.clone();
    let source_task = tokio::spawn(async move {
        let Ok((mut stream, _)) = source.accept().await else {
            return;
        };
        // A single TCP read is not guaranteed to contain the complete request
        // headers. Preserve the existing timeout and 4 KiB cap, but capture a
        // complete header block before asserting credential delivery.
        let request = tokio::time::timeout(Duration::from_secs(5), async {
            let mut request = Vec::with_capacity(4096);
            let mut chunk = [0_u8; 1024];
            while request.len() < 4096 && !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let chunk_len = (4096 - request.len()).min(chunk.len());
                let bytes = stream.read(&mut chunk[..chunk_len]).await?;
                if bytes == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..bytes]);
            }
            Ok::<_, std::io::Error>(request)
        })
        .await
        .expect("redirect source request completed before timeout")
        .expect("read redirect source request");
        *source_observed
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            Some(String::from_utf8_lossy(&request).into_owned());
        let response = format!(
            "HTTP/1.1 302 Found\r\nLocation: {target_url}/acme/full-topup-redirect.git/info/refs?service=git-upload-pack\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(response.as_bytes()).await;
    });
    (
        source_url,
        source_request,
        target_requests,
        source_task,
        target_task,
    )
}

async fn wait_for_archive_settled(server: &Server, repo: &str, commit: &str) {
    let url = format!("{}/v1/repos/github/{repo}/status", server.url);
    let client = reqwest::Client::new();
    let mut last = String::new();
    for _ in 0..360 {
        let response = client
            .get(&url)
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .header("x-ripclone-protocol", "2")
            .send()
            .await
            .expect("read-only archive status request");
        let status = response.status();
        let text = response
            .text()
            .await
            .expect("read-only archive status body");
        last = format!("{status} {text}");
        if status.is_success() {
            let body: serde_json::Value = serde_json::from_str(&text).expect("archive status json");
            if body["refs"].as_array().is_some_and(|refs| {
                refs.iter().any(|reference| {
                    reference["branch"] != "HEAD"
                        && reference["commit"] == commit
                        && (reference["build_status"].is_null()
                            || reference["build_status"] == "done")
                        && reference["warm"] == true
                        && reference["manifest"]
                            .as_str()
                            .is_some_and(|manifest| !manifest.is_empty())
                })
            }) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("MinIO archive build did not settle for {repo}@{commit}: {last}");
}

struct MinioAuditProxy {
    url: String,
    signed_requests: Arc<std::sync::atomic::AtomicUsize>,
    ripclone_auth_requests: Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

struct CloneIdProxy {
    url: String,
    authenticated_pinned_requests: Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CloneIdProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct CloneIdProxyState {
    upstream: String,
    pending_sequence: Arc<std::sync::atomic::AtomicUsize>,
    authenticated_pinned_requests: Arc<std::sync::atomic::AtomicUsize>,
    pinned_pause: Option<Arc<PinnedRequestPause>>,
}

struct PinnedRequestPause {
    at: usize,
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    proceed: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

async fn clone_id_proxy(
    State(state): State<CloneIdProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    let path_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or(request.uri().path());
    let has_ripclone_auth = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("Ripclone "));
    if has_ripclone_auth && path_query.contains("pinned=") {
        let pinned_request = state
            .authenticated_pinned_requests
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        if let Some(pause) = &state.pinned_pause
            && pinned_request == pause.at
        {
            if let Some(entered) = pause
                .entered
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _ = entered.send(());
            }
            if let Some(proceed) = pause.proceed.lock().await.take() {
                tokio::time::timeout(Duration::from_secs(10), proceed)
                    .await
                    .expect("pinned refresh proxy barrier released within 10 seconds")
                    .expect("pinned refresh proxy barrier sender remained alive");
            }
        }
    }
    let mut outgoing = reqwest::Client::new().request(
        request.method().clone(),
        format!("{}{}", state.upstream, path_query),
    );
    for (name, value) in request.headers() {
        if name != axum::http::header::HOST && name != axum::http::header::CONTENT_LENGTH {
            outgoing = outgoing.header(name, value);
        }
    }
    let body = axum::body::to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("read clone-ID proxy request");
    let upstream = outgoing
        .body(body)
        .send()
        .await
        .expect("forward clone-ID proxy request");
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let bytes = upstream
        .bytes()
        .await
        .expect("read clone-ID proxy response");
    let mut output = Response::builder().status(status);
    for (name, value) in &headers {
        if name != axum::http::header::CONTENT_LENGTH
            && name != axum::http::header::TRANSFER_ENCODING
            && name != axum::http::header::CONNECTION
        {
            output = output.header(name, value);
        }
    }
    if status == StatusCode::ACCEPTED {
        let sequence = state
            .pending_sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        output = output.header("x-ripclone-clone-id", format!("pending-clone-{sequence}"));
    }
    output.body(Body::from(bytes)).expect("clone-ID response")
}

async fn start_clone_id_proxy_inner(
    upstream: &str,
    pinned_pause: Option<Arc<PinnedRequestPause>>,
) -> CloneIdProxy {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind clone-ID proxy");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("clone-ID proxy address")
    );
    let authenticated_pinned_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let state = CloneIdProxyState {
        upstream: upstream.trim_end_matches('/').to_string(),
        pending_sequence: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        authenticated_pinned_requests: Arc::clone(&authenticated_pinned_requests),
        pinned_pause,
    };
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .fallback(any(clone_id_proxy))
                .with_state(state),
        )
        .await
        .expect("serve clone-ID proxy");
    });
    CloneIdProxy {
        url,
        authenticated_pinned_requests,
        task,
    }
}

async fn start_clone_id_proxy_with_pinned_pause(
    upstream: &str,
    pause_at: usize,
) -> (
    CloneIdProxy,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let pause = Arc::new(PinnedRequestPause {
        at: pause_at,
        entered: std::sync::Mutex::new(Some(entered_tx)),
        proceed: tokio::sync::Mutex::new(Some(proceed_rx)),
    });
    (
        start_clone_id_proxy_inner(upstream, Some(pause)).await,
        entered_rx,
        proceed_tx,
    )
}

impl Drop for MinioAuditProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_http_head(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut request = Vec::with_capacity(2048);
    let mut chunk = [0u8; 2048];
    loop {
        let count = stream.read(&mut chunk).await.ok()?;
        if count == 0 {
            return None;
        }
        request.extend_from_slice(&chunk[..count]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return Some(request);
        }
        if request.len() > 64 * 1024 {
            return None;
        }
    }
}

fn rewrite_proxy_head(request: &mut Vec<u8>, backend_host: &str) {
    let Ok(text) = std::str::from_utf8(request) else {
        return;
    };
    let Some(end) = text.find("\r\n\r\n") else {
        return;
    };
    let body = &text[end + 4..];
    let mut headers = Vec::new();
    for line in text[..end].lines() {
        if line.to_ascii_lowercase().starts_with("host:") {
            headers.push(format!("Host: {backend_host}"));
        } else if !line.to_ascii_lowercase().starts_with("connection:") {
            headers.push(line.to_string());
        }
    }
    *request = format!(
        "{}\r\nConnection: close\r\n\r\n{body}",
        headers.join("\r\n")
    )
    .into_bytes();
}

async fn start_minio_audit_proxy(endpoint: &str) -> MinioAuditProxy {
    let endpoint = url::Url::parse(endpoint).expect("valid MinIO endpoint");
    let backend_host = format!(
        "{}:{}",
        endpoint.host_str().expect("MinIO host"),
        endpoint.port_or_known_default().expect("MinIO port")
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind MinIO audit proxy");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("MinIO audit address")
    );
    let signed_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ripclone_auth_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let signed_for_task = Arc::clone(&signed_requests);
    let auth_for_task = Arc::clone(&ripclone_auth_requests);
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut client, _)) = listener.accept().await else {
                break;
            };
            let backend_host = backend_host.clone();
            let signed = Arc::clone(&signed_for_task);
            let auth = Arc::clone(&auth_for_task);
            tokio::spawn(async move {
                let Some(mut request) = read_http_head(&mut client).await else {
                    return;
                };
                let text = String::from_utf8_lossy(&request).to_ascii_lowercase();
                if text.contains("x-amz-signature=") {
                    signed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                if text.contains("authorization: ripclone ") {
                    auth.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                rewrite_proxy_head(&mut request, &backend_host);
                let Ok(mut backend) = tokio::net::TcpStream::connect(&backend_host).await else {
                    return;
                };
                if backend.write_all(&request).await.is_ok() {
                    let _ = tokio::io::copy(&mut backend, &mut client).await;
                }
            });
        }
    });
    MinioAuditProxy {
        url,
        signed_requests,
        ripclone_auth_requests,
        task,
    }
}

fn hanging_origin() -> (
    String,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Receiver<bool>,
) {
    use std::io::Read;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    let (closed_tx, closed_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept hanging Git request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        accepted_tx.send(()).unwrap();
        let mut buffer = [0u8; 4096];
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

fn commit_all(repo: &std::path::Path, message: &str) -> String {
    git(repo, &["add", "-A"]);
    git(
        repo,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            message,
        ],
    );
    git(repo, &["rev-parse", "HEAD"])
}

#[tokio::test]
async fn blocked_full_b_tops_up_carried_direct_parent_a_and_publishes_exact_b() {
    let _guard = env_lock().lock().await;
    init(false);
    let upstream_token = "full-topup-client-token";
    let origin = make_http_origin_with_auth("acme/full-topup", "token full-topup-client-token");
    let provider = ProviderInstance {
        id: ProviderInstanceId::new("counting"),
        kind: ProviderKind::Generic,
        host: origin.url.clone(),
        auth_template: Some("token {token}".to_string()),
        auth_header_name: None,
    };
    let mut registry = ProviderRegistry::new();
    registry
        .merge_one(ProviderConfig {
            id: "counting".to_string(),
            kind: Some("generic".to_string()),
            host: Some(origin.url.clone()),
            token: Some(upstream_token.to_string()),
            auth_template: Some("token {token}".to_string()),
            auth_header_name: None,
        })
        .expect("configure counting upstream");
    let (server, barrier, entered, proceed) =
        start_server_split_storage_phase_one_barrier_with_registry(registry).await;

    // SplitMix64 gives this blob deterministic high entropy. Repeated bytes
    // compress too well to detect an accidental retransmission of unchanged
    // content, so keep this large enough to make the byte budget load-bearing.
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    let mut large = Vec::with_capacity(12 * 1024 * 1024);
    while large.len() < large.capacity() {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        large.extend_from_slice(&value.to_le_bytes());
    }
    for (path, bytes) in [
        ("modified.txt", b"before\n".as_slice()),
        ("deleted.txt", b"delete me\n".as_slice()),
        ("renamed-old.txt", b"rename me\n".as_slice()),
        ("file-to-dir", b"was a file\n".as_slice()),
        ("dir-to-file/child.txt", b"was a directory\n".as_slice()),
        ("executable.sh", b"#!/bin/sh\necho before\n".as_slice()),
    ] {
        let full = origin.work.join(path);
        std::fs::create_dir_all(full.parent().expect("fixture parent")).unwrap();
        std::fs::write(full, bytes).unwrap();
    }
    std::fs::write(origin.work.join("unchanged.bin"), &large).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("modified.txt", origin.work.join("link")).unwrap();
    let a = commit_all(&origin.work, "A");
    let unchanged_blob = git(&origin.work, &["hash-object", "unchanged.bin"]);
    origin.publish();

    register_added_without_build_for_provider(&server, "counting", "acme/full-topup")
        .await
        .expect("register repo");
    server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .sync_repo("acme/full-topup", None)
        .await
        .expect("publish full A");
    let ready_a = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .resolve_ref_with_clonepack("acme/full-topup", "main", Some("full"), None)
        .await
        .expect("resolve full A");
    assert_eq!(ready_a.commit, a);
    let base_manifest = ready_a.clonepack_manifest.clone();
    assert!(!base_manifest.is_empty());

    std::fs::write(origin.work.join("modified.txt"), b"after\n").unwrap();
    std::fs::write(origin.work.join("added.txt"), b"added\n").unwrap();
    std::fs::remove_file(origin.work.join("deleted.txt")).unwrap();
    std::fs::rename(
        origin.work.join("renamed-old.txt"),
        origin.work.join("renamed-new.txt"),
    )
    .unwrap();
    std::fs::remove_file(origin.work.join("file-to-dir")).unwrap();
    std::fs::create_dir(origin.work.join("file-to-dir")).unwrap();
    std::fs::write(
        origin.work.join("file-to-dir/child.txt"),
        b"now a directory\n",
    )
    .unwrap();
    std::fs::remove_dir_all(origin.work.join("dir-to-file")).unwrap();
    std::fs::write(origin.work.join("dir-to-file"), b"now a file\n").unwrap();
    std::fs::write(
        origin.work.join("executable.sh"),
        b"#!/bin/sh\necho after\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            origin.work.join("executable.sh"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        std::fs::remove_file(origin.work.join("link")).unwrap();
        std::os::unix::fs::symlink("added.txt", origin.work.join("link")).unwrap();
    }
    let b = commit_all(&origin.work, "B");
    origin.publish();

    barrier.arm();
    let sync_client = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token);
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase-one publication")
        .expect("phase-one barrier alive");

    origin.clear_auth_log();

    let probe = server
        .pinned_path_probe
        .as_ref()
        .expect("pinned-path probe");
    probe.arm();
    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let staging_barrier = output.path().join("staging-barrier");
    let top_up_metrics = output.path().join("top-up-metrics.txt");
    let managed_git_log = output.path().join("managed-git.log");
    let manifest_reads = output.path().join("manifest-reads.txt");
    let source_probe = output.path().join("source-probe");
    std::fs::create_dir_all(&source_probe).unwrap();
    let source_log = source_probe.join("server-source.log");
    std::fs::write(&source_log, b"").unwrap();
    let real_git = String::from_utf8(
        std::process::Command::new("sh")
            .args(["-c", "command -v git"])
            .output()
            .expect("locate real Git")
            .stdout,
    )
    .expect("real Git path is UTF-8")
    .trim()
    .to_string();
    let git_wrapper = source_probe.join("git");
    std::fs::write(
        &git_wrapper,
        format!(
            r#"#!/bin/sh
server_root='{}'
source_log='{}'
real_git='{}'
for arg in "$@"; do
  if [ "$arg" = "fetch" ] || [ "$arg" = "clone" ]; then
    case " $* " in
      *"$server_root"*)
        printf '%s\n' "$*" >>"$source_log"
        if [ "$RIPCLONE_TEST_SOURCE_FORBIDDEN" = "1" ]; then exit 97; fi
        ;;
    esac
    break
  fi
done
exec "$real_git" "$@"
"#,
            server.repo_root.display(),
            source_log.display(),
            real_git
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&git_wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let original_path = std::env::var_os("PATH").unwrap_or_default();
    let _path_guard = ScopedEnvVar::set(
        "PATH",
        format!(
            "{}:{}",
            source_probe.display(),
            original_path.to_string_lossy()
        ),
    );
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");

    // Phase one has already published Shallow(B), while Full(B) remains
    // stopped at the production barrier. Make the real server Git boundary
    // fail: this ordinary, unpinned read must still serve B from
    // authenticated metadata without attempting source acquisition.
    let source_forbidden = ScopedEnvVar::set("RIPCLONE_TEST_SOURCE_FORBIDDEN", "1");
    let shallow_response = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/counting/acme/full-topup/refs/main?clonepack=shallow",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("X-Upstream-Token", upstream_token)
        .header("x-ripclone-protocol", "2")
        .send()
        .await
        .expect("phase-one shallow metadata request");
    assert_eq!(shallow_response.status(), StatusCode::OK);
    let shallow: serde_json::Value = shallow_response
        .json()
        .await
        .expect("phase-one shallow response");
    assert_eq!(shallow["commit"], b);
    assert_eq!(shallow["shallow"], true);
    assert!(
        shallow["clonepack_manifest"]
            .as_str()
            .is_some_and(|manifest| !manifest.is_empty()),
        "phase one must publish a usable shallow manifest"
    );
    assert!(
        std::fs::read_to_string(&source_log)
            .expect("phase-one shallow source log")
            .is_empty(),
        "published Shallow(B) must not reacquire upstream while Full(B) is active"
    );
    assert!(
        !sync_b.is_finished(),
        "Full(B) remains blocked during shallow read"
    );
    drop(source_forbidden);

    let _unchanged_path = ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_UNCHANGED_PATH", "unchanged.bin");
    let _metrics_log = ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_METRICS_LOG", &top_up_metrics);
    let _git_log = ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_GIT_LOG", &managed_git_log);
    let _manifest_log =
        ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_MANIFEST_READ_LOG", &manifest_reads);
    let _staging_barrier =
        ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_STAGING_BARRIER_DIR", &staging_barrier);
    let total_top_up_started = std::time::Instant::now();
    let target_for_install = target.clone();
    let install_client = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token);
    let mut install = tokio::spawn(async move {
        install_client
            .install_repo_with_mode_at(
                "acme/full-topup",
                "HEAD",
                None,
                &target_for_install,
                CloneMode::Editable,
                None,
                None,
            )
            .await
    });
    for _ in 0..800 {
        if staging_barrier.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        staging_barrier.join("entered").exists(),
        "the client must acquire the carried-A top-up plan before branch movement"
    );
    // This is deliberately after the server has issued the carried-A plan and
    // the client has created its private staging root. The production-generated
    // B row remains untouched; exact fetch must not follow the now-advanced C.
    let c = origin.commit(&[("c-only.txt", "C\n")], "C");
    origin.publish();
    assert_ne!(c, b);
    std::fs::write(staging_barrier.join("proceed"), b"continue\n").unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(15), &mut install).await;
    let total_top_up_us = total_top_up_started.elapsed().as_micros();
    let outcome = outcome
        .expect("top-up completed while Full(B) stayed blocked")
        .expect("top-up clone task joined")
        .expect("top-up clone succeeded");
    assert!(!sync_b.is_finished(), "Full(B) must still be blocked");
    let observed = probe.snapshot();
    assert_eq!(observed.enqueues, 0);
    assert_eq!(observed.builder_entries, 0);
    probe.disarm();
    assert_eq!(outcome.commit, b);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(git(&target, &["rev-list", "--count", "HEAD"]), "2");
    assert_eq!(
        git(
            &target,
            &["status", "--porcelain=v1", "--untracked-files=all"]
        ),
        ""
    );
    assert!(git_ok(&target, &["fsck", "--connectivity-only", "HEAD"]));
    assert_eq!(
        std::fs::read(target.join("modified.txt")).unwrap(),
        b"after\n"
    );
    assert_eq!(std::fs::read(target.join("added.txt")).unwrap(), b"added\n");
    assert!(!target.join("c-only.txt").exists());
    assert!(!target.join("deleted.txt").exists());
    assert!(!target.join("renamed-old.txt").exists());
    assert_eq!(
        std::fs::read(target.join("renamed-new.txt")).unwrap(),
        b"rename me\n"
    );
    assert!(target.join("file-to-dir").is_dir());
    assert_eq!(
        std::fs::read(target.join("file-to-dir/child.txt")).unwrap(),
        b"now a directory\n"
    );
    assert!(target.join("dir-to-file").is_file());
    assert_eq!(std::fs::read(target.join("unchanged.bin")).unwrap(), large);
    let metrics = std::fs::read_to_string(top_up_metrics).unwrap();
    let metric = |name: &str| {
        metrics
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}=")))
            .expect("top-up metric present")
            .parse::<u128>()
            .expect("numeric top-up metric")
    };
    assert_eq!(
        metric("before_mtime_ns"),
        metric("after_mtime_ns"),
        "Git's A-to-B update must not rewrite the large unchanged file"
    );
    let top_up_phase_us = metric("top_up_phase_us");
    assert!(top_up_phase_us > 0);
    assert!(total_top_up_us >= top_up_phase_us);
    let managed_git = std::fs::read_to_string(&managed_git_log).expect("managed Git timing log");
    let command_timings = |command: &str| {
        managed_git
            .lines()
            .filter_map(|line| {
                let mut fields = line.split('\t');
                let actual = fields.next()?.strip_prefix("command=")?;
                let duration = fields.next()?.strip_prefix("duration_us=")?;
                (actual == command).then(|| duration.parse::<u128>().expect("Git duration"))
            })
            .collect::<Vec<_>>()
    };
    let exact_fetch_timings = command_timings("fetch");
    assert_eq!(
        exact_fetch_timings.len(),
        1,
        "one top-up must launch exactly one exact Git fetch"
    );
    let git_update_timings = command_timings("reset");
    assert_eq!(git_update_timings.len(), 1);
    let git_update_us = git_update_timings[0];
    assert!(git_update_us > 0);
    let manifest_read_hashes = std::fs::read_to_string(&manifest_reads)
        .expect("carried-manifest read log")
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(
        manifest_read_hashes,
        vec![base_manifest.clone()],
        "one top-up plan must read exactly Full(A)'s carried manifest"
    );
    let upstream_requests = origin.auth_success_count();
    let upstream_bytes = origin.auth_success_get_body_bytes();
    assert!(
        upstream_requests > 0,
        "the exact B fetch must reach the counting upstream"
    );
    assert!(
        upstream_bytes > 0,
        "the counting upstream must observe exact-fetch response bytes"
    );
    assert!(
        upstream_bytes < (large.len() / 8) as u64,
        "top-up transferred {upstream_bytes} bytes for a {}-byte unchanged blob",
        large.len()
    );
    assert_eq!(origin.auth_reject_count(), 0);
    let server_source_acquisitions = std::fs::read_to_string(&source_log)
        .expect("server source acquisition log")
        .lines()
        .count();
    assert_eq!(
        server_source_acquisitions, 0,
        "the pinned top-up metadata request must not acquire server source"
    );
    println!(
        "TOP_UP_EVIDENCE target={b} base={a} manifest={base_manifest} advanced={c} \
manifest_reads={} exact_fetch_commands={} upstream_requests={upstream_requests} \
upstream_bytes={upstream_bytes} git_update_us={git_update_us} \
top_up_phase_us={top_up_phase_us} total_top_up_us={total_top_up_us} \
before_mtime_ns={} after_mtime_ns={} server_source_acquisitions={server_source_acquisitions} \
server_enqueues={} server_builder_entries={} full_b_blocked=true",
        manifest_read_hashes.len(),
        exact_fetch_timings.len(),
        metric("before_mtime_ns"),
        metric("after_mtime_ns"),
        observed.enqueues,
        observed.builder_entries,
    );

    // Non-vacuity control: an empty Git repository fetching the same pinned B
    // must transfer A's incompressible unchanged blob. This proves the HTTP
    // byte counter can see the regression excluded by the top-up ceiling.
    origin.clear_auth_log();
    let fresh = tempfile::tempdir().expect("fresh-fetch control repo");
    git(fresh.path(), &["init", "-q"]);
    let auth_arg = format!("http.extraHeader=Authorization: token {upstream_token}");
    let remote = format!("{}/acme/full-topup.git", origin.url);
    git(
        fresh.path(),
        &["-c", &auth_arg, "fetch", "--no-tags", "--", &remote, &b],
    );
    assert_eq!(
        git(fresh.path(), &["cat-file", "-t", &unchanged_blob]),
        "blob"
    );
    let fresh_fetch_bytes = origin.auth_success_get_body_bytes();
    assert!(
        fresh_fetch_bytes > (large.len() * 3 / 4) as u64,
        "fresh-fetch control observed only {fresh_fetch_bytes} bytes for a {}-byte incompressible blob",
        large.len()
    );
    assert!(
        upstream_bytes * 8 < fresh_fetch_bytes,
        "top-up {upstream_bytes} bytes was not a small fraction of fresh fetch {fresh_fetch_bytes}"
    );
    println!(
        "TOP_UP_BYTE_CONTROL unchanged_blob_bytes={} top_up_bytes={upstream_bytes} fresh_fetch_bytes={fresh_fetch_bytes} ratio_x={:.1}",
        large.len(),
        fresh_fetch_bytes as f64 / upstream_bytes as f64
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::read_link(target.join("link")).unwrap(),
            std::path::PathBuf::from("added.txt")
        );
        assert_ne!(
            std::fs::metadata(target.join("executable.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
    }
    assert_eq!(
        git(&target, &["config", "--get", "remote.origin.url"]),
        format!("{}/acme/full-topup.git", origin.url)
    );

    // Restore B only so the intentionally blocked phase-two task can finish
    // during fixture teardown. Exact-B precedence is proven separately while
    // Full(A) is still carried by B's moving phase-one row.
    git(&origin.work, &["reset", "--hard", &b]);
    origin.publish();
    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join B sync")
        .expect("sync B");

    // Non-vacuity control for the server-source counter: an ordinary sync on
    // the same server must cross the wrapped mirror clone/fetch boundary.
    let d = origin.commit(&[("source-control.txt", "D\n")], "D source control");
    origin.publish();
    server
        .client()
        .with_provider_instance(provider)
        .with_upstream_token(upstream_token)
        .sync_repo("acme/full-topup", None)
        .await
        .expect("ordinary sync reaches the server source boundary");
    assert_ne!(d, b);
    assert!(
        std::fs::read_to_string(&source_log)
            .expect("source control log")
            .lines()
            .count()
            > server_source_acquisitions,
        "server source counter control did not observe ordinary mirror acquisition"
    );
}

#[tokio::test]
async fn exact_full_b_precedes_available_carried_a_without_manifest_or_upstream() {
    let _guard = env_lock().lock().await;
    init(false);
    let upstream_token = "exact-precedence-token";
    let origin = make_http_origin_with_auth(
        "acme/full-topup-exact-precedence",
        "token exact-precedence-token",
    );
    let provider = ProviderInstance {
        id: ProviderInstanceId::new("counting"),
        kind: ProviderKind::Generic,
        host: origin.url.clone(),
        auth_template: Some("token {token}".to_string()),
        auth_header_name: None,
    };
    let mut registry = ProviderRegistry::new();
    registry
        .merge_one(ProviderConfig {
            id: "counting".to_string(),
            kind: Some("generic".to_string()),
            host: Some(origin.url.clone()),
            token: Some(upstream_token.to_string()),
            auth_template: Some("token {token}".to_string()),
            auth_header_name: None,
        })
        .expect("configure counting upstream");
    let (server, barrier, entered, proceed) =
        start_server_split_storage_phase_one_barrier_with_registry(registry).await;

    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build_for_provider(
        &server,
        "counting",
        "acme/full-topup-exact-precedence",
    )
    .await
    .expect("register repo");
    server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .sync_repo("acme/full-topup-exact-precedence", None)
        .await
        .expect("publish Full(A)");
    let ready_a = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .resolve_ref_with_clonepack(
            "acme/full-topup-exact-precedence",
            "main",
            Some("full"),
            None,
        )
        .await
        .expect("resolve exact Full(A)");
    assert_eq!(ready_a.commit, a);

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token);
    let mut sync_b = tokio::spawn(async move {
        sync_client
            .sync_repo("acme/full-topup-exact-precedence", None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase-one publication")
        .expect("phase-one barrier alive");

    origin.clear_auth_log();
    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let pin_barrier = output.path().join("pin-barrier");
    let manifest_reads = output.path().join("manifest-reads.txt");
    std::fs::write(&manifest_reads, "").unwrap();
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let _pin_barrier = ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_PIN_BARRIER_DIR", &pin_barrier);
    let _manifest_log =
        ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_MANIFEST_READ_LOG", &manifest_reads);
    let (decoy_url, decoy_requests, decoy_task) = start_failing_upstream_decoy().await;
    let decoy_provider = ProviderInstance {
        host: decoy_url,
        ..provider.clone()
    };
    let install_client = server
        .client()
        .with_provider_instance(decoy_provider)
        .with_upstream_token(upstream_token);
    let target_for_install = target.clone();
    let mut install = tokio::spawn(async move {
        install_client
            .install_repo_with_mode_at(
                "acme/full-topup-exact-precedence",
                "HEAD",
                None,
                &target_for_install,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });
    for _ in 0..800 {
        if pin_barrier.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        pin_barrier.join("entered").exists(),
        "the first ordinary pending response must pin B before exact B publishes"
    );

    // Full(A) is still a coherent carried base on B's phase-one row. Publish
    // Full(B) before the client issues its pinned top-up request, then prove the
    // exact row wins without consulting either the carried manifest or upstream.
    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finishes")
        .expect("join Full(B) sync")
        .expect("sync Full(B)");
    let ready_b = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .resolve_ref_with_clonepack(
            "acme/full-topup-exact-precedence",
            "main",
            Some("full"),
            None,
        )
        .await
        .expect("wait for exact Full(B) publication");
    assert_eq!(ready_b.commit, b);
    // Phase-two publication itself legitimately fetched from the real source.
    // From this point onward, the paused client's exact-B response must use
    // only server artifacts and never either upstream endpoint.
    origin.clear_auth_log();
    std::fs::write(pin_barrier.join("proceed"), b"continue\n").unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(15), &mut install)
        .await
        .expect("exact B clone completes")
        .expect("exact B clone task joins")
        .expect("exact B clone succeeds");
    decoy_task.abort();

    assert_eq!(outcome.commit, b);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(
        std::fs::read_to_string(&manifest_reads).unwrap(),
        "",
        "exact Full(B) must win before the carried Full(A) manifest is read"
    );
    assert_eq!(
        origin.auth_success_count(),
        0,
        "exact Full(B) must not fetch from the original upstream"
    );
    assert_eq!(origin.auth_reject_count(), 0);
    assert_eq!(
        decoy_requests.load(Ordering::SeqCst),
        0,
        "exact Full(B) must not contact the decoy top-up provider"
    );
}

#[tokio::test]
async fn surrogate_stale_carried_default_branch_uses_resolved_b_branch() {
    let _guard = env_lock().lock().await;
    init(false);
    let upstream_token = "stale-default-token";
    let origin =
        make_http_origin_with_auth("acme/full-topup-stale-default", "token stale-default-token");
    let provider = ProviderInstance {
        id: ProviderInstanceId::new("counting"),
        kind: ProviderKind::Generic,
        host: origin.url.clone(),
        auth_template: Some("token {token}".to_string()),
        auth_header_name: None,
    };
    let mut registry = ProviderRegistry::new();
    registry
        .merge_one(ProviderConfig {
            id: "counting".to_string(),
            kind: Some("generic".to_string()),
            host: Some(origin.url.clone()),
            token: Some(upstream_token.to_string()),
            auth_template: Some("token {token}".to_string()),
            auth_header_name: None,
        })
        .expect("configure counting upstream");
    let (server, barrier, entered, proceed) =
        start_server_split_storage_phase_one_barrier_with_registry(registry).await;
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build_for_provider(&server, "counting", "acme/full-topup-stale-default")
        .await
        .expect("register repo");
    server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .sync_repo("acme/full-topup-stale-default", None)
        .await
        .expect("publish Full(A)");
    let ready_a = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .resolve_ref_with_clonepack("acme/full-topup-stale-default", "main", Some("full"), None)
        .await
        .expect("wait for exact Full(A)");
    assert_eq!(ready_a.commit, a);

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token);
    let mut sync_b = tokio::spawn(async move {
        sync_client
            .sync_repo("acme/full-topup-stale-default", None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase-one publication")
        .expect("phase-one barrier alive");

    // This is intentionally a surrogate for an upstream default-branch rename:
    // mutate only the carried A manifest after real phase-one publication.
    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId {
        provider: ProviderInstanceId::new("counting"),
        path: "acme/full-topup-stale-default".to_string(),
    };
    let mut moving_row = None;
    for candidate in store.list_branches(&repo_id).await.expect("list B rows") {
        if let Some(info) = store
            .load_branch(&repo_id, &candidate)
            .await
            .expect("load candidate B row")
            && info.commit == b
            && info.full_clonepack.commit == a
        {
            moving_row = Some((candidate, info));
            break;
        }
    }
    let (moving_branch, mut moving) = moving_row.expect("moving B row");
    let storage = Cas::new(&server.storage_dir).expect("open split storage CAS");
    let bytes = storage
        .get(&moving.full_clonepack.manifest)
        .expect("read carried Full(A) manifest");
    let mut carried = ClonepackManifest::decode(bytes.as_slice()).expect("decode carried manifest");
    carried.default_branch = "master".to_string();
    moving.full_clonepack.manifest = storage
        .put(&carried.encode_to_vec())
        .expect("store renamed-default carried manifest");
    store
        .save_branch(&repo_id, &moving_branch, &moving)
        .await
        .expect("publish carried manifest with obsolete default branch");

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let outcome = server
        .client()
        .with_provider_instance(provider)
        .with_upstream_token(upstream_token)
        .install_repo_with_mode_at(
            "acme/full-topup-stale-default",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("surrogate top-up succeeds");
    assert_eq!(outcome.commit, b);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(
        git(&target, &["symbolic-ref", "--short", "HEAD"]),
        "main",
        "a HEAD top-up must use B's resolved branch, not A's stale default branch"
    );

    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join Full(B) sync")
        .expect("sync Full(B)");
}

#[tokio::test]
async fn missing_local_provider_fails_before_base_artifacts_download() {
    let _guard = env_lock().lock().await;
    init(false);
    let upstream_token = "missing-provider-token";
    let origin = make_http_origin_with_auth(
        "acme/full-topup-missing-provider",
        "token missing-provider-token",
    );
    let provider = ProviderInstance {
        id: ProviderInstanceId::new("gitea"),
        kind: ProviderKind::Generic,
        host: origin.url.clone(),
        auth_template: Some("token {token}".to_string()),
        auth_header_name: None,
    };
    let mut registry = ProviderRegistry::new();
    registry
        .merge_one(ProviderConfig {
            id: "gitea".to_string(),
            kind: Some("generic".to_string()),
            host: Some(origin.url.clone()),
            token: Some(upstream_token.to_string()),
            auth_template: Some("token {token}".to_string()),
            auth_header_name: None,
        })
        .expect("configure server provider");
    let (server, barrier, entered, proceed) =
        start_server_split_storage_phase_one_barrier_with_registry(registry).await;
    let a = origin.commit(&[("value.txt", "A\\n")], "A");
    origin.publish();
    register_added_without_build_for_provider(&server, "gitea", "acme/full-topup-missing-provider")
        .await
        .expect("register gitea provider repo");
    server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .sync_repo("acme/full-topup-missing-provider", None)
        .await
        .expect("publish Full(A)");
    let ready_a = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token)
        .resolve_ref_with_clonepack(
            "acme/full-topup-missing-provider",
            "main",
            Some("full"),
            None,
        )
        .await
        .expect("wait for exact Full(A)");
    assert_eq!(ready_a.commit, a);

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\\n")], "B");
    origin.publish();
    let sync_client = server
        .client()
        .with_provider_instance(provider)
        .with_upstream_token(upstream_token);
    let mut sync_b = tokio::spawn(async move {
        sync_client
            .sync_repo("acme/full-topup-missing-provider", None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase-one publication")
        .expect("phase-one barrier alive");

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let cache = output.path().join("cache");
    let error = ripclone::client::Client::new_with_token_and_cache(
        server.url.clone(),
        Some(token_hash()),
        Some(&cache),
    )
    .with_provider("gitea")
    .with_upstream_token(upstream_token)
    .install_repo_with_mode_at(
        "acme/full-topup-missing-provider",
        "HEAD",
        None,
        &target,
        CloneMode::Editable,
        Some("full"),
        None,
    )
    .await
    .expect_err("missing local provider configuration must reject top-up");
    assert!(format!("{error:#}").contains("no local client configuration"));
    assert!(
        !target.exists(),
        "rejected top-up must not publish a target"
    );
    assert!(
        std::fs::read_dir(&cache).unwrap().next().is_none(),
        "provider validation must run before any carried Full(A) artifact download"
    );

    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join Full(B) sync")
        .expect("sync Full(B)");
    assert_ne!(a, b);
}

#[tokio::test]
async fn server_named_decoy_is_ignored_for_local_provider_exact_fetch() {
    let _guard = env_lock().lock().await;
    init(false);
    let decoy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    decoy.set_nonblocking(true).unwrap();
    let decoy_url = format!("http://{}", decoy.local_addr().unwrap());
    let mut registry = ProviderRegistry::new();
    registry
        .merge_one(ProviderConfig {
            id: "github".to_string(),
            kind: Some("github".to_string()),
            host: Some(decoy_url),
            token: Some("server-only-secret".to_string()),
            auth_template: None,
            auth_header_name: None,
        })
        .expect("configure server-returned decoy");
    let (server, barrier, entered, proceed) =
        start_server_split_storage_phase_one_barrier_with_registry(registry).await;
    let origin = make_origin("acme", "full-topup-decoy");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/full-topup-decoy")
        .await
        .expect("register decoy repo");
    server
        .client()
        .with_upstream_token("local-only-secret")
        .sync_repo("acme/full-topup-decoy", None)
        .await
        .expect("publish decoy A through local origin override");

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server.client().with_upstream_token("local-only-secret");
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-decoy", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("decoy B reached phase one")
        .expect("decoy phase-one barrier alive");

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let outcome = server
        .client()
        .with_provider_instance(ProviderInstance {
            id: ProviderInstanceId::new("github"),
            kind: ProviderKind::GitHub,
            host: "github.com".to_string(),
            auth_template: None,
            auth_header_name: None,
        })
        .with_upstream_token("local-only-secret")
        .install_repo_with_mode_at(
            "acme/full-topup-decoy",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("local provider fetch ignores server-returned decoy");
    assert_eq!(outcome.commit, b);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert!(
        matches!(decoy.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "server-returned decoy must receive no connection or credential"
    );

    proceed.send(()).expect("release decoy Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("decoy Full(B) finished after release")
        .expect("join decoy B sync")
        .expect("sync decoy B");
    assert_ne!(a, b);
}

#[tokio::test]
async fn new_server_without_a_safe_carried_base_returns_pending_b_immediately() {
    let _guard = env_lock().lock().await;
    init(false);
    let (server, barrier, entered, proceed) = start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "full-topup-no-base");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/full-topup-no-base")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/full-topup-no-base", None)
        .await
        .expect("publish A");

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-no-base", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase one")
        .expect("phase-one barrier alive");

    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/full-topup-no-base");
    let mut moving = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load moving B")
        .expect("moving B row");
    assert_eq!(moving.commit, b);
    let carried = moving.full_clonepack.clone();
    assert_eq!(carried.commit, a);
    moving.full_clonepack = Default::default();
    moving.clonepack_manifest.clear();
    store
        .save_branch(&repo_id, "main", &moving)
        .await
        .expect("remove carried base");

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let started = std::time::Instant::now();
    let error = server
        .client()
        .install_repo_with_mode_at(
            "acme/full-topup-no-base",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("unsafe/missing base stays pending");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "new-server no-base response must not enter the old long poll"
    );
    let pending = error
        .downcast_ref::<ArtifactPending>()
        .expect("typed pending error");
    assert_eq!(pending.commit, b);
    assert!(!target.exists());

    // A present manifest whose authenticated bytes claim the wrong commit is
    // equally ineligible: the server must not sign or return it as a base.
    let storage = Cas::new(&server.storage_dir).expect("open split storage CAS");
    let bytes = storage
        .get(&carried.manifest)
        .expect("read carried manifest");
    let mut bad = ClonepackManifest::decode(bytes.as_slice()).expect("decode carried manifest");
    bad.commit = b.clone();
    let bad_hash = storage
        .put(&bad.encode_to_vec())
        .expect("store bad manifest");
    moving.full_clonepack = carried;
    moving.full_clonepack.manifest = bad_hash;
    store
        .save_branch(&repo_id, "main", &moving)
        .await
        .expect("install mismatched carried manifest");
    let bad_target = output.path().join("bad-clone");
    let bad_error = server
        .client()
        .install_repo_with_mode_at(
            "acme/full-topup-no-base",
            "HEAD",
            None,
            &bad_target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("mismatched manifest stays pending");
    assert_eq!(
        bad_error
            .downcast_ref::<ArtifactPending>()
            .expect("typed mismatch pending")
            .commit,
        b
    );
    assert!(!bad_target.exists());

    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join B sync")
        .expect("sync B");
}

#[tokio::test]
async fn rolling_upgrade_parent_hint_cannot_top_up_an_unrelated_target() {
    let _guard = env_lock().lock().await;
    init(false);
    let (server, barrier, entered, proceed) = start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "full-topup-unrelated");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/full-topup-unrelated")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/full-topup-unrelated", None)
        .await
        .expect("publish A");

    git(&origin.work, &["checkout", "-q", "--orphan", "rewritten"]);
    git(&origin.work, &["rm", "-q", "-rf", "."]);
    let x = origin.commit(&[("rewritten.txt", "X\n")], "X");
    let b = origin.commit(&[("target.txt", "B\n")], "B");
    git(&origin.work, &["branch", "-M", "main"]);
    origin.publish();
    assert_ne!(a, x);

    barrier.arm();
    let sync_client = server.client();
    let mut sync_b = tokio::spawn(async move {
        sync_client
            .sync_repo("acme/full-topup-unrelated", None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase one")
        .expect("phase-one barrier alive");

    let store = FileRefStore::new(&server.repo_root);
    let repo_id = RepoId::github("acme/full-topup-unrelated");
    let mut moving = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load moving B")
        .expect("moving B row");
    assert_eq!(moving.commit, b);
    assert_eq!(moving.parent_commit.as_deref(), Some(x.as_str()));
    assert_eq!(moving.full_clonepack.commit, a);
    // Simulate a transient row written by an older binary that reported only a
    // first-parent-style hint. The fetched commit object remains authoritative.
    moving.parent_commit = Some(a.clone());
    store
        .save_branch(&repo_id, "main", &moving)
        .await
        .expect("install rolling-upgrade parent hint");

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let error = server
        .client()
        .install_repo_with_mode_at(
            "acme/full-topup-unrelated",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("unrelated B must be rejected after exact fetch");
    let message = format!("{error:#}");
    assert!(message.contains(&b));
    assert!(message.contains("not a single-parent child"));
    assert!(!target.exists());

    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join B sync")
        .expect("sync B");
}

#[tokio::test]
async fn merge_target_has_no_top_up_base_and_returns_typed_pending_b() {
    let _guard = env_lock().lock().await;
    init(false);
    let (server, barrier, entered, proceed) = start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "full-topup-merge");
    let a = origin.commit(&[("base.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/full-topup-merge")
        .await
        .expect("register merge repo");
    server
        .client()
        .sync_repo("acme/full-topup-merge", None)
        .await
        .expect("publish merge base A");

    git(&origin.work, &["checkout", "-q", "-b", "side"]);
    origin.commit(&[("side.txt", "side\n")], "side");
    git(&origin.work, &["checkout", "-q", "main"]);
    origin.commit(&[("main.txt", "main\n")], "main");
    git(
        &origin.work,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "merge",
            "--no-ff",
            "-q",
            "side",
            "-m",
            "merge B",
        ],
    );
    let b = git(&origin.work, &["rev-parse", "HEAD"]);
    assert_ne!(a, b);
    origin.publish();

    barrier.arm();
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-merge", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("merge B reached phase one")
        .expect("merge phase-one barrier alive");

    let store = FileRefStore::new(&server.repo_root);
    let moving = store
        .load_branch(&RepoId::github("acme/full-topup-merge"), "main")
        .await
        .expect("load merge B row")
        .expect("merge B row");
    assert_eq!(moving.commit, b);
    assert_eq!(moving.full_clonepack.commit, a);
    assert_eq!(
        moving.parent_commit, None,
        "merge B has no safe sole parent"
    );

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let started = std::time::Instant::now();
    let error = server
        .client()
        .install_repo_with_mode_at(
            "acme/full-topup-merge",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("merge B must not top up from first parent A");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        error
            .downcast_ref::<ArtifactPending>()
            .expect("merge yields typed pending")
            .commit,
        b
    );
    assert!(!target.exists());

    proceed.send(()).expect("release merge Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("merge Full(B) finished after release")
        .expect("join merge B sync")
        .expect("sync merge B");
}

#[tokio::test]
async fn removed_pinned_b_fails_without_following_the_branch_back_to_a() {
    let _guard = env_lock().lock().await;
    init(false);
    let (server, barrier, entered, proceed) = start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "full-topup-removed");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/full-topup-removed")
        .await
        .expect("register removed-target repo");
    server
        .client()
        .sync_repo("acme/full-topup-removed", None)
        .await
        .expect("publish removed-target A");

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-removed", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("removed B reached phase one")
        .expect("removed phase-one barrier alive");

    // Make the advertised branch point back to A and physically prune B from
    // the source repository. The client must request B by object ID and fail;
    // following main would silently publish the wrong commit.
    git(&origin.work, &["reset", "--hard", &a]);
    origin.publish();
    git(&origin.bare, &["reflog", "expire", "--expire=now", "--all"]);
    git(&origin.bare, &["gc", "--prune=now"]);
    assert!(!git_ok(
        &origin.bare,
        &["cat-file", "-e", &format!("{b}^{{commit}}")]
    ));

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let error = tokio::time::timeout(
        Duration::from_secs(15),
        server.client().install_repo_with_mode_at(
            "acme/full-topup-removed",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        ),
    )
    .await
    .expect("removed B failure is bounded")
    .expect_err("removed B must not fall back to current main/A");
    assert!(format!("{error:#}").contains(&b));
    assert!(!target.exists());

    proceed.send(()).expect("release removed Full(B)");
    // The readiness waiter is pinned to B and must never follow the
    // rewound branch to A. If the post-build recheck replaces the mutable
    // branch row before the next exact poll, the documented result is a
    // bounded typed pending error; either outcome is acceptable here because
    // the install assertion above is the source-removal proof.
    match tokio::time::timeout(Duration::from_secs(20), &mut sync_b).await {
        Ok(joined) => match joined.expect("join removed B sync") {
            Ok(response) => assert_eq!(response.commit, b),
            Err(error) => assert!(
                format!("{error:#}").contains(&b),
                "removed-B readiness failure lost its B pin: {error:#}"
            ),
        },
        Err(_) => {
            sync_b.abort();
            tokio::time::timeout(Duration::from_secs(5), &mut sync_b)
                .await
                .expect("aborted removed-B readiness waiter joined")
                .expect_err("removed-B readiness waiter was not aborted");
        }
    }
}

#[tokio::test]
async fn cancellation_and_timeout_reap_git_helpers_before_staging_cleanup() {
    let _guard = env_lock().lock().await;
    init(false);
    let (server, barrier, entered, proceed) = start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "full-topup-cancel");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/full-topup-cancel")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/full-topup-cancel", None)
        .await
        .expect("publish A");
    barrier.arm();
    origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-cancel", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase one")
        .expect("phase-one barrier alive");

    let output = tempfile::tempdir().unwrap();
    let cancel_target = output.path().join("cancelled");
    let (cancel_host, cancel_accepted, cancel_closed) = hanging_origin();
    let cancel_client = server.client().with_provider_instance(ProviderInstance {
        id: ProviderInstanceId::new("github"),
        kind: ProviderKind::GitHub,
        host: cancel_host,
        auth_template: None,
        auth_header_name: None,
    });
    let cancel_target_task = cancel_target.clone();
    let cancelled = tokio::spawn(async move {
        cancel_client
            .install_repo_with_mode_at(
                "acme/full-topup-cancel",
                "HEAD",
                None,
                &cancel_target_task,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });
    tokio::task::spawn_blocking(move || {
        cancel_accepted
            .recv_timeout(Duration::from_secs(10))
            .expect("managed Git reached hanging origin")
    })
    .await
    .unwrap();
    cancelled.abort();
    assert!(cancelled.await.is_err());
    let cancellation_closed = tokio::task::spawn_blocking(move || {
        cancel_closed
            .recv_timeout(Duration::from_secs(5))
            .expect("hanging origin observed cancellation")
    })
    .await
    .unwrap();
    assert!(
        cancellation_closed,
        "remote helper connection remained alive"
    );
    for _ in 0..100 {
        if std::fs::read_dir(output.path()).unwrap().next().is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!cancel_target.exists());
    assert!(
        std::fs::read_dir(output.path()).unwrap().next().is_none(),
        "cancelled attempt leaked staging"
    );

    let timeout_target = output.path().join("timed-out");
    let (timeout_host, timeout_accepted, timeout_closed) = hanging_origin();
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let _timeout = ScopedEnvVar::set("RIPCLONE_TEST_GIT_TIMEOUT_MS", "1000");
    let timeout_result = server
        .client()
        .with_provider_instance(ProviderInstance {
            id: ProviderInstanceId::new("github"),
            kind: ProviderKind::GitHub,
            host: timeout_host,
            auth_template: None,
            auth_header_name: None,
        })
        .install_repo_with_mode_at(
            "acme/full-topup-cancel",
            "HEAD",
            None,
            &timeout_target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await;
    let timeout_error = timeout_result.expect_err("hanging Git fetch must time out");
    assert!(format!("{timeout_error:#}").contains("timed out"));
    timeout_accepted
        .recv_timeout(Duration::from_secs(2))
        .expect("timed fetch reached hanging origin");
    assert!(
        timeout_closed
            .recv_timeout(Duration::from_secs(5))
            .expect("hanging origin observed timeout"),
        "timed-out remote helper connection remained alive"
    );
    assert!(!timeout_target.exists());
    assert!(
        std::fs::read_dir(output.path()).unwrap().next().is_none(),
        "timed-out attempt leaked staging"
    );

    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join B sync")
        .expect("sync B");
}

#[tokio::test]
async fn locally_configured_gitea_token_is_used_only_for_the_exact_fetch() {
    let _guard = env_lock().lock().await;
    init(false);
    let secret = "topup-client-token";
    let origin = make_http_origin_with_auth("acme/full-topup-private", &format!("token {secret}"));
    let mut registry = ProviderRegistry::new();
    registry
        .merge_one(ProviderConfig {
            id: "gitea".to_string(),
            kind: Some("gitea".to_string()),
            host: Some(origin.url.clone()),
            token: Some(secret.to_string()),
            auth_template: None,
            auth_header_name: None,
        })
        .expect("configure private Gitea provider");
    let (server, barrier, entered, proceed) =
        start_server_split_storage_phase_one_barrier_with_registry(registry).await;
    let provider = ProviderInstance {
        id: ProviderInstanceId::new("gitea"),
        kind: ProviderKind::Gitea,
        host: origin.url.clone(),
        auth_template: None,
        auth_header_name: None,
    };
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build_for_provider(&server, "gitea", "acme/full-topup-private")
        .await
        .expect("register private provider repo");
    server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(secret)
        .sync_repo("acme/full-topup-private", None)
        .await
        .expect("publish private A");

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(secret);
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-private", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("private B reached phase one")
        .expect("phase-one barrier alive");

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let outcome = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(secret)
        .install_repo_with_mode_at(
            "acme/full-topup-private",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("locally authenticated private top-up");
    assert_eq!(outcome.commit, b);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert_eq!(
        git(&target, &["config", "--get", "remote.origin.url"]),
        format!("{}/acme/full-topup-private.git", origin.url)
    );
    assert_eq!(origin.auth_reject_count(), 0);

    let missing_target = output.path().join("missing-token");
    let missing = server
        .client()
        .with_provider_instance(provider)
        .install_repo_with_mode_at(
            "acme/full-topup-private",
            "HEAD",
            None,
            &missing_target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("private top-up without local token must fail closed");
    assert!(format!("{missing:#}").contains(&b));
    assert!(!missing_target.exists());
    assert!(origin.auth_reject_count() > 0);

    proceed.send(()).expect("release private Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("private Full(B) finished after release")
        .expect("join private B sync")
        .expect("sync private B");
}

#[tokio::test]
async fn authenticated_top_up_fetch_refuses_redirect_without_credential_leak() {
    let _guard = env_lock().lock().await;
    init(false);
    let secret = "redirect-top-up-token";
    let origin = make_http_origin("acme/full-topup-redirect");
    let provider = ProviderInstance {
        id: ProviderInstanceId::new("redirect"),
        kind: ProviderKind::Generic,
        host: origin.url.clone(),
        auth_template: Some("{token}".to_string()),
        auth_header_name: Some("PRIVATE-TOKEN".to_string()),
    };
    let mut registry = ProviderRegistry::new();
    registry
        .merge_one(ProviderConfig {
            id: "redirect".to_string(),
            kind: Some("generic".to_string()),
            host: Some(origin.url.clone()),
            token: Some(secret.to_string()),
            auth_template: Some("{token}".to_string()),
            auth_header_name: Some("PRIVATE-TOKEN".to_string()),
        })
        .expect("configure redirect provider");
    let (server, barrier, entered, proceed) =
        start_server_split_storage_phase_one_barrier_with_registry(registry).await;
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build_for_provider(&server, "redirect", "acme/full-topup-redirect")
        .await
        .expect("register redirect provider repo");
    server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(secret)
        .sync_repo("acme/full-topup-redirect", None)
        .await
        .expect("publish Full(A)");

    barrier.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(secret);
    let mut sync_b = tokio::spawn(async move {
        sync_client
            .sync_repo("acme/full-topup-redirect", None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached phase-one publication")
        .expect("phase-one barrier alive");

    let (redirect_url, source_request, target_requests, source_task, target_task) =
        start_authenticated_redirect_source().await;
    let redirect_provider = ProviderInstance {
        host: redirect_url,
        ..provider
    };
    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let error = server
        .client()
        .with_provider_instance(redirect_provider)
        .with_upstream_token(secret)
        .install_repo_with_mode_at(
            "acme/full-topup-redirect",
            "HEAD",
            None,
            &target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect_err("an authenticated redirect must fail closed");
    source_task.await.expect("redirect source task joined");
    target_task.abort();

    let request = source_request
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
        .expect("redirect source received the top-up Git request");
    assert!(
        request
            .to_ascii_lowercase()
            .contains("private-token: redirect-top-up-token"),
        "the configured credential must reach only the configured source: {request}"
    );
    assert_eq!(
        target_requests.load(Ordering::SeqCst),
        0,
        "the redirect target must receive neither a request nor the credential"
    );
    assert!(!target.exists());
    assert!(
        format!("{error:#}").contains(&b),
        "the failure must retain the pinned B identity"
    );

    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join Full(B) sync")
        .expect("sync Full(B)");
}

#[tokio::test]
#[ignore = "requires digest-pinned MinIO runner"]
async fn minio_signed_base_stale_url_refresh_remains_pinned_to_b() {
    let _guard = env_lock().lock().await;
    assert_eq!(
        std::env::var("RIPCLONE_REQUIRE_MINIO").as_deref(),
        Ok("1"),
        "run through scripts/e2e_clone_pinning_minio.sh"
    );
    init(false);
    let controls = tempfile::tempdir().unwrap();
    let direct_minio = std::env::var("RIPCLONE_S3_ENDPOINT").expect("MinIO endpoint");
    let audit = start_minio_audit_proxy(&direct_minio).await;
    // The server keeps using the direct S3 endpoint. Only client-facing signed
    // URLs are rewritten through this audit hop, which records the artifact
    // request headers before forwarding them byte-for-byte to MinIO.
    let _signed_url_proxy = ScopedEnvVar::set("RIPCLONE_TEST_SIGNED_URL_PROXY", &audit.url);
    // Production's optional local cache keeps server-side build reads off the
    // single-threaded fixture runtime. Client artifact reads still use real
    // MinIO presigned URLs through `audit`.
    let server_cache_dir =
        ScopedEnvVar::set("RIPCLONE_S3_CACHE_DIR", controls.path().join("s3-cache"));
    let server = start_server().await;
    let (clone_proxy, refresh_entered, refresh_proceed) =
        start_clone_id_proxy_with_pinned_pause(&server.url, 2).await;
    drop(server_cache_dir);
    let origin = make_origin("acme", "full-topup-minio");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    server
        .client()
        .add_repo("acme/full-topup-minio")
        .await
        .expect("register and publish MinIO A");
    let ready_a = server
        .client()
        .resolve_ref_with_clonepack("acme/full-topup-minio", "main", Some("full"), None)
        .await
        .expect("wait for MinIO Full(A)");
    assert_eq!(ready_a.commit, a);
    let server_cas =
        Cas::new(controls.path().join("s3-cache")).expect("open production S3 local cache");
    let client_cache_dir = controls.path().join("client-cache");
    let client_cache = Cas::new(&client_cache_dir).expect("open explicit client cache");
    for hash in [&ready_a.clonepack_manifest, &ready_a.metadata_chunk] {
        let bytes = server_cas.get(hash).expect("read base setup artifact");
        client_cache
            .put_with_hash(hash, &bytes)
            .expect("prime base setup artifact in explicit client cache");
    }

    let phase_barrier = controls.path().join("phase-two");
    let staging_barrier = controls.path().join("staging-barrier");
    let _phase_two_barrier = ScopedEnvVar::set("RIPCLONE_TEST_PHASE2_BARRIER_DIR", &phase_barrier);
    let _signed_url_ttl = ScopedEnvVar::set("RIPCLONE_SIGNED_URL_TTL_SECS", "1");
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let _staging_barrier =
        ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_STAGING_BARRIER_DIR", &staging_barrier);
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-minio", None).await });
    for _ in 0..800 {
        if phase_barrier.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(phase_barrier.join("entered").exists());

    let plan = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/full-topup-minio/refs/main?clonepack=full&pinned={b}&top_up=true",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", "2")
        .send()
        .await
        .expect("request MinIO top-up plan");
    assert_eq!(plan.status(), reqwest::StatusCode::ACCEPTED);
    let plan: serde_json::Value = plan.json().await.expect("decode MinIO top-up plan");
    assert_eq!(plan["commit"], b);
    assert_eq!(plan["top_up_base"]["commit"], a);
    let signed_manifest = plan["top_up_base"]["clonepack_manifest_url"]
        .as_str()
        .expect("signed base manifest URL");
    assert!(signed_manifest.starts_with(&audit.url));
    assert!(signed_manifest.contains("X-Amz-Signature="));

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let target_for_install = target.clone();
    let clone_server = clone_proxy.url.clone();
    let mut install = tokio::spawn(async move {
        ripclone::client::Client::new_with_token_and_cache(
            clone_server,
            Some(token_hash()),
            Some(&client_cache_dir),
        )
        .install_repo_with_mode_at(
            "acme/full-topup-minio",
            "HEAD",
            None,
            &target_for_install,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
    });
    for _ in 0..400 {
        if staging_barrier.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let stale_staging = std::path::PathBuf::from(
        std::fs::read_to_string(staging_barrier.join("entered"))
            .expect("first top-up staging barrier entered")
            .trim(),
    );
    assert!(
        stale_staging.exists(),
        "the stale attempt must own a real staging directory"
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    std::fs::write(staging_barrier.join("proceed"), b"expire\n").unwrap();
    tokio::time::timeout(Duration::from_secs(20), refresh_entered)
        .await
        .expect("stale base URL reached pinned refresh")
        .expect("pinned refresh barrier remained alive");
    assert!(
        !stale_staging.exists(),
        "pinned refresh began before the stale attempt staging was drained"
    );
    refresh_proceed
        .send(())
        .expect("release refreshed pinned-B plan");
    let outcome = tokio::time::timeout(Duration::from_secs(30), &mut install)
        .await
        .expect("refreshed top-up completed")
        .expect("top-up install task joined");
    let outcome = outcome.expect("fresh pinned-B plan succeeds after stale base URL");
    assert_eq!(outcome.commit, b);
    assert!(outcome.cold, "the first 202 must survive the stale retry");
    assert_eq!(
        outcome.clone_id.as_deref(),
        Some("pending-clone-1"),
        "later top-up and stale-refresh responses must not replace the first clone ID"
    );
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert!(
        !stale_staging.exists(),
        "stale attempt staging reappeared after publication"
    );
    assert!(!sync_b.is_finished(), "Full(B) must still be blocked");
    assert!(
        audit
            .signed_requests
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 2,
        "the artifact host must see the expired request and refreshed base-A download"
    );
    assert_eq!(
        audit
            .ripclone_auth_requests
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the Ripclone session credential must never reach MinIO"
    );
    assert!(
        clone_proxy
            .authenticated_pinned_requests
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 2,
        "both the initial plan and stale refresh must authenticate and stay pinned"
    );
    println!(
        "MINIO_TOP_UP_EVIDENCE target={b} base={a} stale_staging_drained_before_refresh=true \
signed_requests={} authenticated_pinned_requests={} ripclone_auth_requests={}",
        audit
            .signed_requests
            .load(std::sync::atomic::Ordering::SeqCst),
        clone_proxy
            .authenticated_pinned_requests
            .load(std::sync::atomic::Ordering::SeqCst),
        audit
            .ripclone_auth_requests
            .load(std::sync::atomic::Ordering::SeqCst),
    );

    std::fs::write(phase_barrier.join("proceed"), b"release\n").unwrap();
    tokio::time::timeout(Duration::from_secs(30), &mut sync_b)
        .await
        .expect("MinIO Full(B) finished after release")
        .expect("join MinIO B sync")
        .expect("sync MinIO B");
    // `sync_repo` completes after phase one. Keep the local source and the
    // server alive until the detached archive worker reports B fully settled.
    wait_for_archive_settled(&server, "acme/full-topup-minio", &b).await;
}
