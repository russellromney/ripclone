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
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
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
    requests: Arc<std::sync::atomic::AtomicUsize>,
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
    force_old_pending: Arc<std::sync::atomic::AtomicUsize>,
    requests: Arc<std::sync::atomic::AtomicUsize>,
}

async fn clone_id_proxy(
    State(state): State<CloneIdProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    state
        .requests
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        state
            .authenticated_pinned_requests
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    let mut status = upstream.status();
    let headers = upstream.headers().clone();
    let mut bytes = upstream
        .bytes()
        .await
        .expect("read clone-ID proxy response");
    let mut content_location = None;
    if status == StatusCode::OK
        && state
            .force_old_pending
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
    {
        let ready: serde_json::Value =
            serde_json::from_slice(&bytes).expect("ready response for old-server proxy");
        let commit = ready["commit"].as_str().expect("ready commit");
        content_location = ready["branch"].as_str().map(str::to_string);
        status = StatusCode::ACCEPTED;
        bytes = serde_json::to_vec(&serde_json::json!({
            "code": "artifact_pending",
            "commit": commit,
            "status": "building",
            "queue_depth": 1
        }))
        .expect("encode old-server pending")
        .into();
    }
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
    if let Some(branch) = content_location {
        output = output.header(
            axum::http::header::CONTENT_LOCATION,
            urlencoding::encode(&branch).into_owned(),
        );
    }
    output.body(Body::from(bytes)).expect("clone-ID response")
}

async fn start_clone_id_proxy(upstream: &str, force_old_pending: usize) -> CloneIdProxy {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind clone-ID proxy");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("clone-ID proxy address")
    );
    let authenticated_pinned_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let state = CloneIdProxyState {
        upstream: upstream.trim_end_matches('/').to_string(),
        pending_sequence: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        authenticated_pinned_requests: Arc::clone(&authenticated_pinned_requests),
        force_old_pending: Arc::new(std::sync::atomic::AtomicUsize::new(force_old_pending)),
        requests: Arc::clone(&requests),
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
        requests,
        task,
    }
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

    let large = vec![b'u'; 2 * 1024 * 1024];
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

    // Advance the real branch again after the server has pinned/published B's
    // phase-one row. The client must exact-fetch B, never follow main to C.
    let c = origin.commit(&[("c-only.txt", "C\n")], "C");
    origin.publish();
    assert_ne!(c, b);

    let probe = server
        .pinned_path_probe
        .as_ref()
        .expect("pinned-path probe");
    probe.arm();
    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let top_up_metrics = output.path().join("top-up-metrics.txt");
    let manifest_reads = output.path().join("manifest-reads.txt");
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_TOP_UP_UNCHANGED_PATH", "unchanged.bin");
        std::env::set_var("RIPCLONE_TEST_TOP_UP_METRICS_LOG", &top_up_metrics);
        std::env::set_var("RIPCLONE_TEST_TOP_UP_MANIFEST_READ_LOG", &manifest_reads);
    }
    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        server
            .client()
            .with_provider_instance(provider.clone())
            .with_upstream_token(upstream_token)
            .install_repo_with_mode_at(
                "acme/full-topup",
                "HEAD",
                None,
                &target,
                CloneMode::Editable,
                Some("full"),
                None,
            ),
    )
    .await;
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_TOP_UP_METRICS_LOG");
        std::env::remove_var("RIPCLONE_TEST_TOP_UP_MANIFEST_READ_LOG");
        std::env::remove_var("RIPCLONE_TEST_TOP_UP_UNCHANGED_PATH");
        std::env::remove_var("RIPCLONE_TESTING");
    }
    let outcome = outcome
        .expect("top-up completed while Full(B) stayed blocked")
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
    assert_eq!(metric("exact_fetches"), 1);
    assert!(metric("top_up_ms") > 0);
    assert_eq!(
        std::fs::read_to_string(&manifest_reads)
            .expect("carried-manifest read log")
            .lines()
            .count(),
        1,
        "one top-up plan must perform one carried-manifest storage read"
    );
    assert!(
        origin.auth_success_count() > 0,
        "the exact B fetch must reach the counting upstream"
    );
    assert!(
        origin.auth_success_bytes() > 0,
        "the counting upstream must observe exact-fetch response bytes"
    );
    assert_eq!(origin.auth_reject_count(), 0);
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

    // Restore the branch to B for the separate exact-precedence control. The
    // successful top-up above already proved that C was live during the plan.
    git(&origin.work, &["reset", "--hard", &b]);
    origin.publish();
    proceed.send(()).expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join B sync")
        .expect("sync B");

    let ready_b = server
        .client()
        .with_provider_instance(provider)
        .with_upstream_token(upstream_token)
        .resolve_ref_with_clonepack("acme/full-topup", "main", Some("full"), None)
        .await
        .expect("wait for exact Full(B)");
    assert_eq!(ready_b.commit, b);

    // Once exact Full(B) is ready it wins before any top-up/provider work. A
    // local decoy provider would fail immediately if the client contacted it.
    let exact_root = tempfile::tempdir().unwrap();
    let exact_target = exact_root.path().join("clone");
    std::fs::write(&manifest_reads, "").unwrap();
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_TOP_UP_MANIFEST_READ_LOG", &manifest_reads);
    }
    let exact = server
        .client()
        .with_provider_instance(ProviderInstance {
            id: ProviderInstanceId::new("counting"),
            kind: ProviderKind::Generic,
            host: "http://127.0.0.1:9".to_string(),
            auth_template: Some("token {token}".to_string()),
            auth_header_name: None,
        })
        .install_repo_with_mode_at(
            "acme/full-topup",
            "HEAD",
            None,
            &exact_target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("exact Full(B) ignores decoy upstream");
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_TOP_UP_MANIFEST_READ_LOG");
        std::env::remove_var("RIPCLONE_TESTING");
    }
    assert_eq!(exact.commit, b);
    assert_eq!(git(&exact_target, &["rev-parse", "HEAD"]), b);
    assert_eq!(
        std::fs::read_to_string(&manifest_reads).unwrap(),
        "",
        "exact Full(B) must win without reading the carried-A manifest"
    );
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
async fn old_server_pending_shape_keeps_bounded_exact_polling_compatible() {
    let _guard = env_lock().lock().await;
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "full-topup-old-server");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/full-topup-old-server")
        .await
        .expect("register old-server repo");
    server
        .client()
        .sync_repo("acme/full-topup-old-server", None)
        .await
        .expect("publish old-server A");
    let ready = server
        .client()
        .resolve_ref_with_clonepack("acme/full-topup-old-server", "main", Some("full"), None)
        .await
        .expect("wait for old-server Full(A)");
    assert_eq!(ready.commit, a);

    // Rewrite the first ordinary response and the first pinned opt-in response
    // to the pre-top-up 202 shape. The new client must ignore absent extension
    // fields, continue its existing bounded exact poll, and preserve identity.
    let proxy = start_clone_id_proxy(&server.url, 2).await;
    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        ripclone::client::Client::new_with_token(proxy.url.clone(), Some(token_hash()))
            .install_repo_with_mode_at(
                "acme/full-topup-old-server",
                "HEAD",
                None,
                &target,
                CloneMode::Editable,
                Some("full"),
                None,
            ),
    )
    .await
    .expect("old-server compatibility poll is bounded")
    .expect("old-server compatibility clone succeeds once exact A is returned");
    assert_eq!(outcome.commit, a);
    assert!(outcome.cold);
    assert_eq!(outcome.clone_id.as_deref(), Some("pending-clone-1"));
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), a);
    assert!(
        proxy.requests.load(std::sync::atomic::Ordering::SeqCst) >= 3,
        "old-server response must be polled rather than treated as new-server no-base"
    );
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
    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("removed Full(B) finished after release")
        .expect("join removed B sync")
        .expect("sync removed B from server mirror");
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
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_GIT_TIMEOUT_MS", "1000");
    }
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
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_GIT_TIMEOUT_MS");
        std::env::remove_var("RIPCLONE_TESTING");
    }
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
    unsafe {
        std::env::set_var("RIPCLONE_TEST_SIGNED_URL_PROXY", &audit.url);
        // Production's optional local cache keeps server-side build reads off
        // the single-threaded fixture runtime. Client artifact reads still use
        // real MinIO presigned URLs through `audit`.
        std::env::set_var("RIPCLONE_S3_CACHE_DIR", controls.path().join("s3-cache"));
    }
    let server = start_server().await;
    let clone_proxy = start_clone_id_proxy(&server.url, 0).await;
    unsafe {
        std::env::remove_var("RIPCLONE_S3_CACHE_DIR");
    }
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

    let phase_barrier = controls.path().join("phase-two");
    let delay_marker = controls.path().join("delayed-once");
    unsafe {
        std::env::set_var("RIPCLONE_TEST_PHASE2_BARRIER_DIR", &phase_barrier);
        std::env::set_var("RIPCLONE_SIGNED_URL_TTL_SECS", "1");
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_TEST_TOP_UP_PLAN_DELAY_MS", "1500");
        std::env::set_var("RIPCLONE_TEST_TOP_UP_PLAN_DELAY_MARKER", &delay_marker);
    }
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
    let outcome =
        ripclone::client::Client::new_with_token(clone_proxy.url.clone(), Some(token_hash()))
            .install_repo_with_mode_at(
                "acme/full-topup-minio",
                "HEAD",
                None,
                &target,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await;
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_TOP_UP_PLAN_DELAY_MARKER");
        std::env::remove_var("RIPCLONE_TEST_TOP_UP_PLAN_DELAY_MS");
        std::env::remove_var("RIPCLONE_TESTING");
        std::env::remove_var("RIPCLONE_SIGNED_URL_TTL_SECS");
        std::env::remove_var("RIPCLONE_TEST_PHASE2_BARRIER_DIR");
        std::env::remove_var("RIPCLONE_TEST_SIGNED_URL_PROXY");
    }
    let outcome = outcome.expect("fresh pinned-B plan succeeds after stale base URL");
    assert_eq!(outcome.commit, b);
    assert!(outcome.cold, "the first 202 must survive the stale retry");
    assert_eq!(
        outcome.clone_id.as_deref(),
        Some("pending-clone-1"),
        "later top-up and stale-refresh responses must not replace the first clone ID"
    );
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert!(delay_marker.exists(), "first plan was deliberately expired");
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

    std::fs::write(phase_barrier.join("proceed"), b"release\n").unwrap();
    tokio::time::timeout(Duration::from_secs(30), &mut sync_b)
        .await
        .expect("MinIO Full(B) finished after release")
        .expect("join MinIO B sync")
        .expect("sync MinIO B");
}
