//! One-commit Full-clone top-up through real Head publication.

mod common;

use axum::http::StatusCode;
use common::*;
use prost::Message;
use ripclone::cas::Cas;
use ripclone::client::ArtifactPending;
use ripclone::clonepack::ClonepackManifest;
use ripclone::mode::CloneMode;
use ripclone::provider::{
    ProviderConfig, ProviderInstance, ProviderInstanceId, ProviderKind, ProviderRegistry, RepoId,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn wait_for_exact_full(client: ripclone::client::Client, repo: &str, commit: &str) {
    let ready = client
        .resolve_exact_result(repo, "main", ripclone::ExactResultKind::Full, None)
        .await
        .expect("exact Full became ready");
    assert_eq!(ready.commit, commit);
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

async fn wait_for_files_job_settled(server: &Server, repo: &str, commit: &str) {
    let url = format!("{}/v1/repos/github/{repo}/status", server.url);
    let client = reqwest::Client::new();
    let mut last = String::new();
    for _ in 0..360 {
        let response = client
            .get(&url)
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
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
                        && reference["head"] == true
                        && reference["full"] == true
                        && reference["files"] == true
                        && reference["job"] == "done"
                })
            }) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("MinIO Files job did not settle for {repo}@{commit}: {last}");
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
        start_server_split_storage_head_publish_barrier_with_registry(registry).await;

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
        .resolve_exact_result(
            "acme/full-topup",
            "main",
            ripclone::ExactResultKind::Full,
            None,
        )
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

    barrier.arm_for(&b);
    let sync_client = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(upstream_token);
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached Head publication")
        .expect("Head publication barrier alive");

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

    // Head(B) is already published, while Full(B) remains
    // stopped at the production barrier. Make the real server Git boundary
    // fail: this exact pinned read must still serve B from
    // authenticated metadata without attempting source acquisition.
    let source_forbidden = ScopedEnvVar::set("RIPCLONE_TEST_SOURCE_FORBIDDEN", "1");
    let shallow_response = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/counting/acme/full-topup/refs/main?result=head&pinned={b}",
            server.url,
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("X-Upstream-Token", upstream_token)
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("Head metadata request");
    assert_eq!(shallow_response.status(), StatusCode::OK);
    let shallow: serde_json::Value = shallow_response.json().await.expect("Head response");
    assert_eq!(shallow["commit"], b);
    assert_eq!(shallow["result"], "head");
    assert!(
        shallow["clonepack_manifest"]
            .as_str()
            .is_some_and(|manifest| !manifest.is_empty()),
        "Head must publish a usable manifest"
    );
    assert!(
        std::fs::read_to_string(&source_log)
            .expect("Head source log")
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
    let source_acquisitions_after_pin = std::fs::read_to_string(&source_log)
        .expect("source log after operation pin")
        .lines()
        .count();
    assert_eq!(
        source_acquisitions_after_pin, 0,
        "request-time selection must not start a second mirror fetch"
    );
    assert!(
        origin.auth_success_count() > 0,
        "the initial request must perform its bounded upstream selection"
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
        server_source_acquisitions, source_acquisitions_after_pin,
        "no server source acquisition is allowed after the top-up pin exists"
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

    // Restore B only so the intentionally blocked Full/Files work can finish
    // during fixture teardown. Exact-B precedence is proven separately while
    // Full(A) remains available through B's exact Head parent.
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
        start_server_split_storage_head_publish_barrier_with_registry(registry).await;

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
        .resolve_exact_result(
            "acme/full-topup-exact-precedence",
            "main",
            ripclone::ExactResultKind::Full,
            None,
        )
        .await
        .expect("resolve exact Full(A)");
    assert_eq!(ready_a.commit, a);

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    barrier.arm_for(&b);
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
        .expect("B reached Head publication")
        .expect("Head publication barrier alive");

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

    // Full(A) is still a coherent exact-parent base for B. Publish
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
        .resolve_exact_result(
            "acme/full-topup-exact-precedence",
            "main",
            ripclone::ExactResultKind::Full,
            None,
        )
        .await
        .expect("wait for exact Full(B) publication");
    assert_eq!(ready_b.commit, b);
    // Full(B) publication itself legitimately fetched from the real source.
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
        start_server_split_storage_head_publish_barrier_with_registry(registry).await;
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
        .resolve_exact_result(
            "acme/full-topup-missing-provider",
            "main",
            ripclone::ExactResultKind::Full,
            None,
        )
        .await
        .expect("wait for exact Full(A)");
    assert_eq!(ready_a.commit, a);

    let b = origin.commit(&[("value.txt", "B\\n")], "B");
    origin.publish();
    barrier.arm_for(&b);
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
        .expect("B reached Head publication")
        .expect("Head publication barrier alive");

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
        start_server_split_storage_head_publish_barrier_with_registry(registry).await;
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
    wait_for_exact_full(
        server.client().with_upstream_token("local-only-secret"),
        "acme/full-topup-decoy",
        &a,
    )
    .await;

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    barrier.arm_for(&b);
    let sync_client = server.client().with_upstream_token("local-only-secret");
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-decoy", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("decoy B reached Head publication")
        .expect("decoy Head barrier alive");

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
    let (server, barrier, entered, proceed) =
        start_server_split_storage_head_publish_barrier().await;
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
    wait_for_exact_full(server.client(), "acme/full-topup-no-base", &a).await;

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    barrier.arm_for(&b);
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-no-base", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached Head publication")
        .expect("Head publication barrier alive");

    let store = server_ref_store(&server).await;
    let repo_id = RepoId::github("acme/full-topup-no-base");
    let exact = store
        .load_result(&repo_id, &b)
        .await
        .expect("load exact B")
        .expect("exact B row");
    assert_eq!(exact.commit, b);
    let mut exact_a = store
        .load_result(&repo_id, &a)
        .await
        .expect("load exact A")
        .expect("exact A row");
    let carried = exact_a.full.take().expect("Full(A) ready").clonepack;
    assert_eq!(carried.commit, a);
    store
        .save_result(&repo_id, &exact_a)
        .await
        .expect("remove exact Full(A)");

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
    exact_a.full = Some(ripclone::FullResult {
        clonepack: ripclone::ClonepackArtifacts {
            manifest: bad_hash,
            ..carried
        },
        ..Default::default()
    });
    store
        .save_result(&repo_id, &exact_a)
        .await
        .expect("install mismatched exact Full(A)");
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
async fn malformed_parent_hint_cannot_top_up_an_unrelated_target() {
    let _guard = env_lock().lock().await;
    init(false);
    let (server, barrier, entered, proceed) =
        start_server_split_storage_head_publish_barrier().await;
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
    wait_for_exact_full(server.client(), "acme/full-topup-unrelated", &a).await;

    git(&origin.work, &["checkout", "-q", "--orphan", "rewritten"]);
    git(&origin.work, &["rm", "-q", "-rf", "."]);
    let x = origin.commit(&[("rewritten.txt", "X\n")], "X");
    let b = origin.commit(&[("target.txt", "B\n")], "B");
    git(&origin.work, &["branch", "-M", "main"]);
    origin.publish();
    assert_ne!(a, x);

    barrier.arm_for(&b);
    let sync_client = server.client();
    let mut sync_b = tokio::spawn(async move {
        sync_client
            .sync_repo("acme/full-topup-unrelated", None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached Head publication")
        .expect("Head publication barrier alive");

    let store = server_ref_store(&server).await;
    let repo_id = RepoId::github("acme/full-topup-unrelated");
    let mut exact = store
        .load_result(&repo_id, &b)
        .await
        .expect("load exact B")
        .expect("exact B row");
    assert_eq!(exact.commit, b);
    assert_eq!(
        exact.head.as_ref().unwrap().parent_commit.as_deref(),
        Some(x.as_str())
    );
    assert!(
        exact.full.is_none(),
        "an unrelated history must not carry Full(A) automatically"
    );
    let exact_a = store
        .load_result(&repo_id, &a)
        .await
        .expect("load exact A")
        .expect("exact A row");
    assert_eq!(exact_a.full.as_ref().unwrap().clonepack.commit, a);
    // Forge both the first-parent hint and a carried Full(A). The fetched B
    // commit object remains authoritative and must reject this metadata.
    exact.head.as_mut().unwrap().parent_commit = Some(a.clone());
    store
        .save_result(&repo_id, &exact)
        .await
        .expect("install malformed parent hint");

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
    let (server, barrier, entered, proceed) =
        start_server_split_storage_head_publish_barrier().await;
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
    wait_for_exact_full(server.client(), "acme/full-topup-merge", &a).await;

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

    barrier.arm_for(&b);
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-merge", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("merge B reached Head publication")
        .expect("merge Head barrier alive");

    let store = server_ref_store(&server).await;
    let exact = store
        .load_result(&RepoId::github("acme/full-topup-merge"), &b)
        .await
        .expect("load merge B row")
        .expect("merge B row");
    assert_eq!(exact.commit, b);
    assert!(
        exact.full.is_none(),
        "a merge target must not carry an arbitrary first-parent Full artifact"
    );
    assert_eq!(
        exact.head.as_ref().unwrap().parent_commit,
        None,
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
    let (server, barrier, entered, proceed) =
        start_server_split_storage_head_publish_barrier().await;
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
    wait_for_exact_full(server.client(), "acme/full-topup-removed", &a).await;

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    barrier.arm_for(&b);
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-removed", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("removed B reached Head publication")
        .expect("removed Head barrier alive");

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let staging_barrier = output.path().join("staging-barrier");
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    let _staging_barrier =
        ScopedEnvVar::set("RIPCLONE_TEST_TOP_UP_STAGING_BARRIER_DIR", &staging_barrier);
    let install_client = server.client();
    let target_for_install = target.clone();
    let mut install = tokio::spawn(async move {
        install_client
            .install_repo_with_mode_at(
                "acme/full-topup-removed",
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
        if staging_barrier.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        staging_barrier.join("entered").exists(),
        "ordinary Full clone must acquire the B-from-A top-up plan"
    );

    // The ordinary clone has pinned B and privately staged Full(A). Rewind the
    // advertised branch and physically prune B before allowing its exact fetch.
    // Following main would silently publish A; fetching B must fail instead.
    git(&origin.work, &["reset", "--hard", &a]);
    origin.publish();
    git(&origin.bare, &["reflog", "expire", "--expire=now", "--all"]);
    git(&origin.bare, &["gc", "--prune=now"]);
    assert!(!git_ok(
        &origin.bare,
        &["cat-file", "-e", &format!("{b}^{{commit}}")]
    ));

    std::fs::write(staging_barrier.join("proceed"), b"continue\n").unwrap();
    let error = tokio::time::timeout(Duration::from_secs(15), &mut install)
        .await
        .expect("removed B failure is bounded")
        .expect("removed B clone task joins")
        .expect_err("removed B must not fall back to current main/A");
    let error = format!("{error:#}");
    assert!(error.contains("exact upstream fetch"), "{error}");
    assert!(error.contains(&b), "{error}");
    assert!(!target.exists());

    proceed.send(()).expect("release removed Full(B)");
    // The readiness waiter is pinned to B and must never follow the rewound
    // branch to A. A bounded exact-B error remains acceptable here because the
    // install assertion above is the source-removal proof.
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
    let (server, barrier, entered, proceed) =
        start_server_split_storage_head_publish_barrier().await;
    let origin = make_origin("acme", "full-topup-cancel");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/full-topup-cancel")
        .await
        .expect("register repo");
    server
        .client()
        .sync_repo("acme/full-topup-cancel", None)
        .await
        .expect("publish A");
    wait_for_exact_full(server.client(), "acme/full-topup-cancel", &a).await;
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    barrier.arm_for(&b);
    let sync_client = server.client();
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-cancel", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("B reached Head publication")
        .expect("Head publication barrier alive");

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
    assert!(cancellation_closed, "upstream connection remained alive");
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
        "timed-out upstream connection remained alive"
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
        start_server_split_storage_head_publish_barrier_with_registry(registry).await;
    let provider = ProviderInstance {
        id: ProviderInstanceId::new("gitea"),
        kind: ProviderKind::Gitea,
        host: origin.url.clone(),
        auth_template: None,
        auth_header_name: None,
    };
    let a = origin.commit(&[("value.txt", "A\n")], "A");
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
    wait_for_exact_full(
        server
            .client()
            .with_provider_instance(provider.clone())
            .with_upstream_token(secret),
        "acme/full-topup-private",
        &a,
    )
    .await;

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    barrier.arm_for(&b);
    let sync_client = server
        .client()
        .with_provider_instance(provider.clone())
        .with_upstream_token(secret);
    let mut sync_b =
        tokio::spawn(async move { sync_client.sync_repo("acme/full-topup-private", None).await });
    tokio::time::timeout(Duration::from_secs(20), entered)
        .await
        .expect("private B reached Head publication")
        .expect("Head publication barrier alive");

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
        start_server_split_storage_head_publish_barrier_with_registry(registry).await;
    let a = origin.commit(&[("value.txt", "A\n")], "A");
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
    wait_for_exact_full(
        server
            .client()
            .with_provider_instance(provider.clone())
            .with_upstream_token(secret),
        "acme/full-topup-redirect",
        &a,
    )
    .await;

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    barrier.arm_for(&b);
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
        .expect("B reached Head publication")
        .expect("Head publication barrier alive");

    let plan = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/redirect/acme/full-topup-redirect/refs/main?result=full&pinned={b}&top_up=true",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("X-Upstream-Token", secret)
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("read pinned top-up plan");
    assert_eq!(plan.status(), StatusCode::ACCEPTED);
    let plan: serde_json::Value = plan.json().await.expect("top-up plan JSON");
    assert_eq!(plan["top_up_supported"], true);
    let store = server_ref_store(&server).await;
    let repo_id = RepoId {
        provider: ProviderInstanceId::new("redirect"),
        path: "acme/full-topup-redirect".to_string(),
    };
    let exact_b = store
        .load_result(&repo_id, &b)
        .await
        .expect("load exact B")
        .expect("exact B exists");
    assert_eq!(
        exact_b.head.as_ref().unwrap().parent_commit.as_deref(),
        Some(a.as_str())
    );
    let exact_a = store
        .load_result(&repo_id, &a)
        .await
        .expect("load exact A")
        .expect("exact A exists");
    assert!(exact_a.full.is_some(), "exact Full(A) exists: {plan}");
    assert_eq!(plan["top_up_base"]["commit"], a, "plan: {plan}");

    let (redirect_url, source_request, target_requests, source_task, target_task) =
        start_authenticated_redirect_source().await;
    let redirect_provider = ProviderInstance {
        host: redirect_url,
        ..provider
    };
    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let install_client = server
        .client()
        .with_provider_instance(redirect_provider)
        .with_upstream_token(secret);
    let install_target = target.clone();
    let install = tokio::spawn(async move {
        install_client
            .install_repo_with_mode_at(
                "acme/full-topup-redirect",
                "HEAD",
                None,
                &install_target,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });
    source_task.await.expect("redirect source task joined");
    proceed.send(()).expect("release Full(B)");
    let error = install
        .await
        .expect("redirect install task joined")
        .expect_err("an authenticated redirect must fail closed");
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

    tokio::time::timeout(Duration::from_secs(20), &mut sync_b)
        .await
        .expect("Full(B) finished after release")
        .expect("join Full(B) sync")
        .expect("sync Full(B)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires digest-pinned MinIO runner"]
async fn minio_interrupted_history_pack_expires_refreshes_exact_b_and_resumes() {
    let _guard = env_lock().lock().await;
    assert_eq!(
        std::env::var("RIPCLONE_REQUIRE_MINIO").as_deref(),
        Ok("1"),
        "run through scripts/e2e_full_topup_minio.sh"
    );
    init(false);
    let controls = tempfile::tempdir().expect("MinIO resume controls");
    let interrupt = controls.path().join("interrupt");
    let audit = controls.path().join("download-audit.log");
    let _testing = ScopedEnvVar::set("RIPCLONE_TESTING", "1");
    // Keep enough lifetime on the refreshed URL set for the remaining local
    // MinIO transfers. A one-second TTL can expire twice at a wall-clock second
    // boundary on a loaded CI runner, which proves repeated refresh rather than
    // the intended one-refresh/reuse behavior.
    let _private_ttl = ScopedEnvVar::set("RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS", "5");
    let _public_ttl = ScopedEnvVar::set("RIPCLONE_SIGNED_URL_TTL_SECS", "5");
    let _backoff = ScopedEnvVar::set("RIPCLONE_FETCH_BACKOFF_MS", "0");
    let probe = Arc::new(ripclone::server::AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;

    let origin = make_origin("acme", "resume-minio");
    let noise = |seed: u64, len: usize| {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/";
        let mut state = seed | 1;
        let mut bytes = Vec::with_capacity(len);
        while bytes.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.push(ALPHABET[(state & 0x3f) as usize]);
        }
        String::from_utf8(bytes).expect("noise is ascii")
    };
    let a_text = noise(211, 512 * 1024);
    origin.commit(&[("large.txt", a_text.as_str())], "A");
    let b_text = noise(223, 512 * 1024);
    let b = origin.commit(&[("large.txt", b_text.as_str())], "B");
    origin.publish();
    server
        .client()
        .add_repo("acme/resume-minio")
        .await
        .expect("register and publish MinIO B");
    let ready_b = server
        .client()
        .resolve_exact_result(
            "acme/resume-minio",
            "main",
            ripclone::ExactResultKind::Full,
            None,
        )
        .await
        .expect("wait for MinIO Full(B)");
    assert_eq!(ready_b.commit, b);
    let (manifest, _) = server
        .client()
        .fetch_clonepack(&ready_b)
        .await
        .expect("fetch B manifest from MinIO");
    let history = manifest
        .packs
        .iter()
        .find(|pack| pack.history_only)
        .and_then(|pack| pack.pack.as_ref())
        .expect("B publishes a history-only pack");
    assert!(history.len > 64 * 1024, "fixture history pack is large");
    let history_hash = ripclone::clonepack::hash_to_hex(&history.hash);
    let _interrupt_hash = ScopedEnvVar::set("RIPCLONE_TEST_INTERRUPT_ARTIFACT", &history_hash);
    let _interrupt_after = ScopedEnvVar::set("RIPCLONE_TEST_INTERRUPT_AFTER_BYTES", "65536");
    let _interrupt_dir = ScopedEnvVar::set("RIPCLONE_TEST_INTERRUPT_DIR", &interrupt);
    let _audit = ScopedEnvVar::set("RIPCLONE_TEST_DOWNLOAD_AUDIT", &audit);

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    let target_for_install = target.clone();
    let clone_client = server.client();
    let install = tokio::spawn(async move {
        clone_client
            .install_repo_with_mode_at(
                "acme/resume-minio",
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
        if interrupt.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let saved = std::fs::read_to_string(interrupt.join("entered"))
        .expect("client interruption reached")
        .parse::<u64>()
        .expect("saved byte count");
    assert!(saved >= 65_536 && saved < history.len);

    // Advance the real branch and build C while the B clone is paused. The URL
    // refresh that follows must remain an exact-B metadata read.
    let c_text = noise(227, 512 * 1024);
    // More than CDC_MAX guarantees multiple archive frames; high-entropy
    // printable bytes keep the compressed download above the 4 MiB bundle
    // target so the Files clone has genuinely queued MinIO chunks.
    let c_archive_text = noise(229, 17 * 1024 * 1024);
    let c = origin.commit(
        &[
            ("large.txt", c_text.as_str()),
            ("archive-large.txt", c_archive_text.as_str()),
        ],
        "C",
    );
    origin.publish();
    let ready_c = server
        .client()
        .sync_repo("acme/resume-minio", None)
        .await
        .expect("build branch advance C");
    assert_eq!(ready_c.commit, c);
    wait_for_files_job_settled(&server, "acme/resume-minio", &c).await;
    let counters_before_refresh = (
        probe.tip_probes.load(Ordering::SeqCst),
        probe.exact_fetches.load(Ordering::SeqCst),
        probe.builder_entries.load(Ordering::SeqCst),
        probe.head_builds.load(Ordering::SeqCst),
        probe.full_builds.load(Ordering::SeqCst),
        probe.files_builds.load(Ordering::SeqCst),
        probe.queue_inserts.load(Ordering::SeqCst),
    );
    let trace_before_refresh = probe
        .http_trace
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .len();
    tokio::time::sleep(Duration::from_millis(6_200)).await;
    std::fs::write(interrupt.join("proceed"), b"retry expired URL\n").unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(60), install)
        .await
        .expect("resumed MinIO clone completed")
        .expect("MinIO clone task joined")
        .expect("fresh exact-B URL resumed the failed artifact");
    assert_eq!(outcome.commit, b);
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    assert!(git_ok(&target, &["fsck", "--connectivity-only", "HEAD"]));
    assert_eq!(
        counters_before_refresh,
        (
            probe.tip_probes.load(Ordering::SeqCst),
            probe.exact_fetches.load(Ordering::SeqCst),
            probe.builder_entries.load(Ordering::SeqCst),
            probe.head_builds.load(Ordering::SeqCst),
            probe.full_builds.load(Ordering::SeqCst),
            probe.files_builds.load(Ordering::SeqCst),
            probe.queue_inserts.load(Ordering::SeqCst),
        ),
        "URL refresh must create no source fetch, build, or job"
    );

    let trace = probe
        .http_trace
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let refresh_trace = &trace[trace_before_refresh..];
    let pinned_b: Vec<_> = refresh_trace
        .iter()
        .filter(|event| event.contains(&format!("pinned={b}")))
        .collect();
    assert_eq!(
        pinned_b.len(),
        1,
        "one exact-B URL refresh: {refresh_trace:?}"
    );

    let audit_text = std::fs::read_to_string(&audit).expect("read client download audit");
    let all_requests: Vec<&str> = audit_text
        .lines()
        .filter(|line| line.starts_with("hash="))
        .collect();
    let requests: Vec<&str> = all_requests
        .iter()
        .copied()
        .filter(|line| line.contains(&format!("hash={history_hash}")))
        .collect();
    assert_eq!(
        requests.len(),
        3,
        "initial, expired, refreshed: {requests:?}"
    );
    assert!(requests[0].contains("offset=0"));
    assert!(requests[1].contains(&format!("offset={saved}")));
    assert!(requests[2].contains(&format!("offset={saved}")));
    let mut per_artifact = std::collections::HashMap::<&str, usize>::new();
    for request in &all_requests {
        let hash = request
            .split_whitespace()
            .next()
            .and_then(|field| field.strip_prefix("hash="))
            .expect("audit request hash");
        *per_artifact.entry(hash).or_default() += 1;
        assert!(
            request.contains("signed=true ripclone_authorization=false"),
            "MinIO request must use the credential-free HTTP client: {request}"
        );
    }
    assert!(
        per_artifact
            .iter()
            .filter(|entry| *entry.0 != history_hash.as_str())
            .all(|(_, count)| *count == 1),
        "completed artifacts must not be requested again: {per_artifact:?}"
    );
    println!(
        "MINIO_RESUME_EVIDENCE target={b} branch_now={c} saved={saved} \
requests=3 exact_b_refreshes=1 no_build_or_fetch_on_refresh=true no_repeated_completed_bytes=true \
signed_requests_without_ripclone_auth={}",
        all_requests
            .iter()
            .all(|request| request.contains("signed=true ripclone_authorization=false")),
    );

    // Buffered Files chunks and Head packs restart only themselves from byte
    // zero after obtaining a fresh exact-C URL. Both pauses occur before the
    // real MinIO request, so the initial signed URL genuinely expires.
    let ready_c_files = server
        .client()
        .resolve_exact_result(
            "acme/resume-minio",
            "main",
            ripclone::ExactResultKind::Files,
            Some(&c),
        )
        .await
        .expect("resolve exact Files(C)");
    let (c_files_manifest, _) = server
        .client()
        .fetch_clonepack(&ready_c_files)
        .await
        .expect("fetch Files(C) manifest");
    assert!(
        c_files_manifest.archive_chunks.len() > 1,
        "Files(C) must publish multiple queued archive chunks"
    );
    let archive_hashes: Vec<String> = c_files_manifest
        .archive_chunks
        .iter()
        .map(|chunk| ripclone::clonepack::hash_to_hex(&chunk.hash))
        .collect();
    let archive_hash = ripclone::clonepack::hash_to_hex(
        &c_files_manifest
            .archive_chunks
            .first()
            .expect("Files(C) publishes an archive chunk")
            .hash,
    );
    {
        let pause = controls.path().join("buffered-files-pause");
        let buffered_audit = controls.path().join("buffered-files-audit.log");
        let _pause_hash = ScopedEnvVar::set("RIPCLONE_TEST_PAUSE_BUFFERED_ARTIFACT", &archive_hash);
        let _pause_dir = ScopedEnvVar::set("RIPCLONE_TEST_PAUSE_BUFFERED_DIR", &pause);
        let _buffered_audit = ScopedEnvVar::set("RIPCLONE_TEST_DOWNLOAD_AUDIT", &buffered_audit);
        let _serial_downloads = ScopedEnvVar::set("RIPCLONE_TEST_DOWNLOAD_CONCURRENCY", "1");
        let files_output = tempfile::tempdir().expect("buffered Files output");
        let files_target = files_output.path().join("clone");
        let files_target_task = files_target.clone();
        let files_client = server.client();
        let files_commit = c.clone();
        let files_clone = tokio::spawn(async move {
            files_client
                .install_repo_with_mode_at(
                    "acme/resume-minio",
                    "main",
                    Some(&files_commit),
                    files_target_task,
                    CloneMode::Files,
                    Some("full"),
                    None,
                )
                .await
        });
        for _ in 0..800 {
            if pause.join("entered").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(pause.join("entered").exists(), "archive chunk paused");
        tokio::time::sleep(Duration::from_millis(6_200)).await;
        let trace_start = probe
            .http_trace
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len();
        std::fs::write(pause.join("proceed"), b"request expired archive URL\n").unwrap();
        let files_outcome = tokio::time::timeout(Duration::from_secs(30), files_clone)
            .await
            .expect("buffered Files clone completed")
            .expect("buffered Files task joined")
            .expect("expired archive URL refreshed");
        assert_eq!(files_outcome.commit, c);
        assert_eq!(
            std::fs::read_to_string(files_target.join("large.txt")).unwrap(),
            c_text
        );
        assert_eq!(
            std::fs::metadata(files_target.join("archive-large.txt"))
                .expect("materialize large archive fixture")
                .len(),
            c_archive_text.len() as u64
        );
        let trace = probe
            .http_trace
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        assert_eq!(
            trace[trace_start..]
                .iter()
                .filter(|event| event.contains(&format!("pinned={c}")))
                .count(),
            1,
            "one exact-C archive URL refresh: {:?}",
            &trace[trace_start..]
        );
        let audit_text = std::fs::read_to_string(&buffered_audit).expect("Files audit");
        let requests: Vec<_> = audit_text
            .lines()
            .filter(|line| line.contains(&format!("hash={archive_hash}")))
            .collect();
        assert_eq!(
            requests.len(),
            2,
            "expired and refreshed archive: {requests:?}"
        );
        assert!(requests.iter().all(|request| request.contains("offset=0")));
        for hash in archive_hashes.iter().skip(1) {
            let queued_requests: Vec<_> = audit_text
                .lines()
                .filter(|line| line.contains(&format!("hash={hash}")))
                .collect();
            assert_eq!(
                queued_requests.len(),
                1,
                "queued chunk must use the retained refreshed URL set: {queued_requests:?}"
            );
            assert!(queued_requests[0].contains("offset=0"));
        }
    }

    let ready_c_head = server
        .client()
        .resolve_exact_result(
            "acme/resume-minio",
            "main",
            ripclone::ExactResultKind::Head,
            Some(&c),
        )
        .await
        .expect("resolve exact Head(C)");
    let (c_head_manifest, _) = server
        .client()
        .fetch_clonepack(&ready_c_head)
        .await
        .expect("fetch Head(C) manifest");
    let head_pack_hash = c_head_manifest
        .packs
        .iter()
        .find(|pack| !pack.history_only)
        .and_then(|pack| pack.pack.as_ref())
        .map(|pack| ripclone::clonepack::hash_to_hex(&pack.hash))
        .expect("Head(C) publishes a head pack");
    {
        let pause = controls.path().join("buffered-head-pause");
        let buffered_audit = controls.path().join("buffered-head-audit.log");
        let _pause_hash =
            ScopedEnvVar::set("RIPCLONE_TEST_PAUSE_BUFFERED_ARTIFACT", &head_pack_hash);
        let _pause_dir = ScopedEnvVar::set("RIPCLONE_TEST_PAUSE_BUFFERED_DIR", &pause);
        let _buffered_audit = ScopedEnvVar::set("RIPCLONE_TEST_DOWNLOAD_AUDIT", &buffered_audit);
        let head_output = tempfile::tempdir().expect("buffered Head output");
        let head_target = head_output.path().join("clone");
        let head_target_task = head_target.clone();
        let head_client = server.client();
        let head_commit = c.clone();
        let head_clone = tokio::spawn(async move {
            head_client
                .install_repo_with_mode_at(
                    "acme/resume-minio",
                    "main",
                    Some(&head_commit),
                    head_target_task,
                    CloneMode::Editable,
                    Some("shallow"),
                    None,
                )
                .await
        });
        for _ in 0..800 {
            if pause.join("entered").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(pause.join("entered").exists(), "head pack paused");
        tokio::time::sleep(Duration::from_millis(6_200)).await;
        let trace_start = probe
            .http_trace
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len();
        std::fs::write(pause.join("proceed"), b"request expired head URL\n").unwrap();
        let head_outcome = tokio::time::timeout(Duration::from_secs(30), head_clone)
            .await
            .expect("buffered Head clone completed")
            .expect("buffered Head task joined")
            .expect("expired head-pack URL refreshed");
        assert_eq!(head_outcome.commit, c);
        assert_eq!(git(&head_target, &["rev-parse", "HEAD"]), c);
        let trace = probe
            .http_trace
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        assert_eq!(
            trace[trace_start..]
                .iter()
                .filter(|event| event.contains(&format!("pinned={c}")))
                .count(),
            1,
            "one exact-C head-pack URL refresh: {:?}",
            &trace[trace_start..]
        );
        let audit_text = std::fs::read_to_string(&buffered_audit).expect("Head audit");
        let requests: Vec<_> = audit_text
            .lines()
            .filter(|line| line.contains(&format!("hash={head_pack_hash}")))
            .collect();
        assert_eq!(
            requests.len(),
            2,
            "expired and refreshed head pack: {requests:?}"
        );
        assert!(requests.iter().all(|request| request.contains("offset=0")));
    }
    println!(
        "MINIO_BUFFERED_REFRESH_EVIDENCE files_restart_offset=0 head_restart_offset=0 \
files_chunks={} files_ref_refreshes=1 queued_chunks_reused=true \
exact_c_refreshes=2 no_whole_clone_restart=true",
        archive_hashes.len()
    );

    // Negative row: the signed transfer starts while repository access is
    // valid, but that access is actively revoked before the expired URL is
    // refreshed. The private staging tree must disappear without publishing a
    // target.
    let c_manifest = server
        .client()
        .resolve_exact_result(
            "acme/resume-minio",
            "main",
            ripclone::ExactResultKind::Full,
            Some(&c),
        )
        .await
        .expect("resolve exact C for revocation fixture");
    let (c_manifest, _) = server
        .client()
        .fetch_clonepack(&c_manifest)
        .await
        .expect("fetch C manifest");
    let c_history = c_manifest
        .packs
        .iter()
        .find(|pack| pack.history_only)
        .and_then(|pack| pack.pack.as_ref())
        .expect("C publishes a history pack");
    let c_history_hash = ripclone::clonepack::hash_to_hex(&c_history.hash);
    let revoked_interrupt = controls.path().join("revoked-interrupt");
    let revoked_audit = controls.path().join("revoked-audit.log");
    let _revoked_hash = ScopedEnvVar::set("RIPCLONE_TEST_INTERRUPT_ARTIFACT", &c_history_hash);
    let _revoked_dir = ScopedEnvVar::set("RIPCLONE_TEST_INTERRUPT_DIR", &revoked_interrupt);
    let _revoked_audit = ScopedEnvVar::set("RIPCLONE_TEST_DOWNLOAD_AUDIT", &revoked_audit);
    let revoked_output = tempfile::tempdir().expect("revoked clone output");
    let revoked_target = revoked_output.path().join("clone");
    let revoked_target_task = revoked_target.clone();
    let revoked_client = server.client();
    let revoked_commit = c.clone();
    let revoked_clone = tokio::spawn(async move {
        revoked_client
            .install_repo_with_mode_at(
                "acme/resume-minio",
                "main",
                Some(&revoked_commit),
                revoked_target_task,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });
    for _ in 0..800 {
        if revoked_interrupt.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        revoked_interrupt.join("entered").exists(),
        "revoked clone reached its deterministic interruption"
    );
    let revoked_saved = std::fs::read_to_string(revoked_interrupt.join("entered"))
        .expect("read revoked interruption offset")
        .parse::<u64>()
        .expect("revoked interruption offset");
    probe.deny_repo_reads();
    tokio::time::sleep(Duration::from_millis(6_200)).await;
    std::fs::write(
        revoked_interrupt.join("proceed"),
        b"retry after revocation\n",
    )
    .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(30), revoked_clone)
        .await
        .expect("revoked clone stopped")
        .expect("revoked clone task joined")
        .expect_err("expired access must refuse exact-C URL refresh");
    assert!(
        format!("{error:#}").contains(&format!("refresh of pinned commit {c} was not authorized")),
        "unexpected revocation error: {error:#}"
    );
    assert!(
        !revoked_target.exists(),
        "revoked clone publishes no target"
    );
    assert!(
        std::fs::read_dir(revoked_output.path())
            .expect("read revoked clone parent")
            .next()
            .is_none(),
        "revoked clone removes private staging"
    );
    let revoked_audit_text =
        std::fs::read_to_string(&revoked_audit).expect("read revoked download audit");
    let revoked_requests: Vec<_> = revoked_audit_text
        .lines()
        .filter(|line| line.contains(&format!("hash={c_history_hash}")))
        .collect();
    assert_eq!(
        revoked_requests.len(),
        2,
        "revoked clone makes the initial and expired-URL requests only: {revoked_requests:?}"
    );
    assert!(revoked_requests[0].contains("offset=0"));
    assert!(
        revoked_requests[1].contains(&format!("offset={revoked_saved}")),
        "expired request must begin at the saved byte count: {revoked_requests:?}"
    );
    probe.allow_repo_reads();
    println!(
        "MINIO_REVOCATION_EVIDENCE commit={c} active_revocation=true no_target=true cleanup=true"
    );

    // Top-up race: acquire a Full(A) plan while Full(B) is pending, interrupt
    // A's real signed history response, then let Full(B) become ready before
    // the expired URL is refreshed. Refresh must address immutable A directly;
    // ready B must neither replace it nor make the clone fail.
    let topup_server = start_server().await;
    let topup_origin = make_origin("acme", "topup-refresh-minio");
    let topup_a1 = noise(307, 512 * 1024);
    topup_origin.commit(&[("large.txt", topup_a1.as_str())], "A1");
    let topup_a2 = noise(311, 512 * 1024);
    topup_origin.commit(&[("large.txt", topup_a2.as_str())], "A2");
    let topup_a3 = noise(313, 512 * 1024);
    let topup_a = topup_origin.commit(&[("large.txt", topup_a3.as_str())], "A3");
    topup_origin.publish();
    topup_server
        .client()
        .add_repo("acme/topup-refresh-minio")
        .await
        .expect("publish MinIO Full(A)");
    let ready_topup_a = topup_server
        .client()
        .resolve_exact_result(
            "acme/topup-refresh-minio",
            "main",
            ripclone::ExactResultKind::Full,
            Some(&topup_a),
        )
        .await
        .expect("resolve exact Full(A)");
    let (topup_a_manifest, _) = topup_server
        .client()
        .fetch_clonepack(&ready_topup_a)
        .await
        .expect("fetch Full(A) manifest");
    let topup_a_history = topup_a_manifest
        .packs
        .iter()
        .find(|pack| pack.history_only)
        .and_then(|pack| pack.pack.as_ref())
        .expect("Full(A) publishes a history pack");
    assert!(topup_a_history.len > 64 * 1024);
    let topup_a_history_hash = ripclone::clonepack::hash_to_hex(&topup_a_history.hash);

    let topup_b_text = noise(317, 512 * 1024);
    let topup_b = topup_origin.commit(&[("large.txt", topup_b_text.as_str())], "B");
    topup_origin.publish();
    let topup_head_barrier = controls.path().join("topup-head-barrier");
    let _topup_head_barrier =
        ScopedEnvVar::set("RIPCLONE_TEST_AFTER_HEAD_BARRIER_DIR", &topup_head_barrier);
    let _topup_head_commit = ScopedEnvVar::set("RIPCLONE_TEST_AFTER_HEAD_BARRIER_COMMIT", &topup_b);
    let topup_sync_client = topup_server.client();
    let topup_b_for_sync = topup_b.clone();
    let topup_sync = tokio::spawn(async move {
        let ready = topup_sync_client
            .sync_repo("acme/topup-refresh-minio", None)
            .await?;
        anyhow::ensure!(ready.commit == topup_b_for_sync);
        Ok::<_, anyhow::Error>(ready)
    });
    for _ in 0..1_200 {
        if topup_head_barrier.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        topup_head_barrier.join("entered").exists(),
        "Full(B) reached Head publication"
    );

    let topup_interrupt = controls.path().join("topup-interrupt");
    let topup_audit = controls.path().join("topup-audit.log");
    let _topup_hash = ScopedEnvVar::set("RIPCLONE_TEST_INTERRUPT_ARTIFACT", &topup_a_history_hash);
    let _topup_dir = ScopedEnvVar::set("RIPCLONE_TEST_INTERRUPT_DIR", &topup_interrupt);
    let _topup_audit = ScopedEnvVar::set("RIPCLONE_TEST_DOWNLOAD_AUDIT", &topup_audit);
    let topup_output = tempfile::tempdir().expect("top-up refresh output");
    let topup_target = topup_output.path().join("clone");
    let topup_target_task = topup_target.clone();
    let topup_client = topup_server.client();
    let topup_clone = tokio::spawn(async move {
        topup_client
            .install_repo_with_mode_at(
                "acme/topup-refresh-minio",
                "HEAD",
                None,
                topup_target_task,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
    });
    for _ in 0..800 {
        if topup_interrupt.join("entered").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        topup_interrupt.join("entered").exists(),
        "top-up clone must interrupt Full(A)'s signed history pack"
    );
    let topup_saved = std::fs::read_to_string(topup_interrupt.join("entered"))
        .expect("read top-up interruption offset")
        .parse::<u64>()
        .expect("top-up interruption offset");

    std::fs::write(topup_head_barrier.join("proceed"), b"release Full(B)\n")
        .expect("release Full(B)");
    tokio::time::timeout(Duration::from_secs(30), topup_sync)
        .await
        .expect("Full(B) completed before refresh")
        .expect("Full(B) sync task joined")
        .expect("Full(B) sync succeeded");
    wait_for_files_job_settled(&topup_server, "acme/topup-refresh-minio", &topup_b).await;
    let topup_counters_before_refresh = (
        probe.tip_probes.load(Ordering::SeqCst),
        probe.exact_fetches.load(Ordering::SeqCst),
        probe.builder_entries.load(Ordering::SeqCst),
        probe.head_builds.load(Ordering::SeqCst),
        probe.full_builds.load(Ordering::SeqCst),
        probe.files_builds.load(Ordering::SeqCst),
        probe.queue_inserts.load(Ordering::SeqCst),
    );
    let topup_trace_before_refresh = probe
        .http_trace
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .len();
    tokio::time::sleep(Duration::from_millis(6_200)).await;
    std::fs::write(
        topup_interrupt.join("proceed"),
        b"refresh immutable A after Full(B) became ready\n",
    )
    .unwrap();
    let topup_outcome = tokio::time::timeout(Duration::from_secs(60), topup_clone)
        .await
        .expect("top-up refresh clone completed")
        .expect("top-up refresh clone task joined")
        .expect("ready Full(B) did not replace refreshed Full(A)");
    assert_eq!(topup_outcome.commit, topup_b);
    assert_eq!(git(&topup_target, &["rev-parse", "HEAD"]), topup_b);
    assert!(git_ok(
        &topup_target,
        &["fsck", "--connectivity-only", "HEAD"]
    ));
    assert_eq!(
        topup_counters_before_refresh,
        (
            probe.tip_probes.load(Ordering::SeqCst),
            probe.exact_fetches.load(Ordering::SeqCst),
            probe.builder_entries.load(Ordering::SeqCst),
            probe.head_builds.load(Ordering::SeqCst),
            probe.full_builds.load(Ordering::SeqCst),
            probe.files_builds.load(Ordering::SeqCst),
            probe.queue_inserts.load(Ordering::SeqCst),
        ),
        "refreshing immutable A must create no source fetch, build, or job"
    );
    let topup_trace = probe
        .http_trace
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let topup_refresh_trace = &topup_trace[topup_trace_before_refresh..];
    assert_eq!(
        topup_refresh_trace
            .iter()
            .filter(|event| event.contains(&format!("pinned={topup_a}")))
            .count(),
        1,
        "top-up URL refresh must read exact A once: {topup_refresh_trace:?}"
    );
    assert!(
        topup_refresh_trace
            .iter()
            .all(|event| !event.contains(&format!("pinned={topup_b}"))),
        "top-up URL refresh must not depend on ready B: {topup_refresh_trace:?}"
    );
    let topup_audit_text =
        std::fs::read_to_string(&topup_audit).expect("read top-up download audit");
    let topup_history_requests: Vec<_> = topup_audit_text
        .lines()
        .filter(|line| line.contains(&format!("hash={topup_a_history_hash}")))
        .collect();
    assert_eq!(
        topup_history_requests.len(),
        3,
        "{topup_history_requests:?}"
    );
    assert!(topup_history_requests[0].contains("offset=0"));
    assert!(
        topup_history_requests[1].contains(&format!("offset={topup_saved}"))
            && topup_history_requests[2].contains(&format!("offset={topup_saved}"))
    );
    println!(
        "MINIO_TOPUP_REFRESH_EVIDENCE target={topup_b} artifact={topup_a} \
saved={topup_saved} full_b_ready_before_refresh=true exact_a_refreshes=1 \
exact_b_refreshes=0 no_build_or_fetch_on_refresh=true"
    );
}
