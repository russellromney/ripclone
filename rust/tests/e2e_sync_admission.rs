//! Deterministic direct proof for immutable sync admission.
//!
//! Every concurrency assertion is gated by a production-boundary notification
//! barrier. The fixture never infers overlap from a sleep or from a tiny build.

mod common;

use common::*;
use hmac::{Hmac, KeyInit, Mac};
use prost::Message;
use ripclone::clonepack::{ClonepackManifest, hash_to_hex, manifest_chunk_refs};
use ripclone::server::AdmissionTestProbe;
use serde_json::Value;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

const WEBHOOK_SECRET: &str = "immutable-admission-webhook-secret";

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn tree_snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                visit(root, &path, out);
            } else if file_type.is_file() {
                if let Ok(bytes) = std::fs::read(&path) {
                    out.insert(path.strip_prefix(root).unwrap().to_path_buf(), bytes);
                }
            }
        }
    }

    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}

async fn wait_entered(barrier: &ripclone::server::AdmissionTestBarrier, count: usize) {
    tokio::time::timeout(Duration::from_secs(20), barrier.wait_until_entered(count))
        .await
        .expect("admission barrier entered within 20 seconds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_b_and_c_admissions_create_independent_exact_work() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe {
        std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "1");
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server_env(&[("RIPCLONE_WEBHOOK_SECRET_GITHUB", WEBHOOK_SECRET)]).await;
    let origin = make_origin("acme", "immutable");
    origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/immutable")
        .await
        .unwrap();
    server
        .client()
        .sync_repo("acme/immutable", None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("A reaches Full");

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    probe.before_claim.arm();
    wait_entered(&probe.before_claim, 1).await;
    probe.hold_inside_admission_transaction(&b);

    // B owns the immediate transaction while C resolves and prepares against
    // the still-visible A projection. Once B commits, C must rediscover B
    // inside its own immediate transaction instead of extending stale A.
    let b_request = post_sync(&server, None);
    let coordinate = async {
        wait_entered(&probe.inside_admission_tx, 1).await;
        let c = origin.commit(&[("value.txt", "C\n")], "C");
        origin.publish();
        probe.hold_admission_transaction(&c);
        let c_request = post_sync(&server, None);
        let release_transactions = async {
            wait_entered(&probe.before_admission_tx, 1).await;
            probe.inside_admission_tx.release();
            probe.before_admission_tx.release();
        };
        let (c_result, ()) = tokio::join!(c_request, release_transactions);
        (c, c_result)
    };
    let (b_result, (c, c_result)) = tokio::join!(b_request, coordinate);
    let (b_status, b_body, _) = b_result;
    let (c_status, c_body, _) = c_result;
    assert_eq!(b_status, reqwest::StatusCode::ACCEPTED, "B: {b_body}");
    assert_eq!(c_status, reqwest::StatusCode::ACCEPTED, "C: {c_body}");
    assert_eq!(response_commit(&b_body), b);
    assert_eq!(response_commit(&c_body), c);

    // Inspect the two independently admitted exact results before either worker may claim.
    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/immutable");
    let exact_b = store.load_result(&repo_id, &b).await.unwrap().unwrap();
    assert_eq!(exact_b.commit, b);
    let exact_c = store.load_result(&repo_id, &c).await.unwrap().unwrap();
    assert_eq!(exact_c.commit, c);

    let full_barrier = tempfile::tempdir().unwrap();
    unsafe {
        std::env::set_var("RIPCLONE_TEST_PHASE2_BARRIER_DIR", full_barrier.path());
        std::env::set_var("RIPCLONE_TEST_PHASE2_BARRIER_COMMIT", &c);
    }
    probe.inside_admission_tx.disarm();
    probe.before_admission_tx.disarm();
    probe.before_claim.release();
    probe.before_claim.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(2))
        .await
        .expect("B completes before held C Full");
    tokio::time::timeout(Duration::from_secs(60), async {
        while !full_barrier.path().join("entered").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("C reaches held Full");
    std::fs::write(full_barrier.path().join("proceed"), b"proceed\n").unwrap();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(3))
        .await
        .expect("C completes after B");

    assert_eq!(
        store
            .load_result(&repo_id, &b)
            .await
            .unwrap()
            .unwrap()
            .commit,
        b
    );
    assert_eq!(
        store
            .load_result(&repo_id, &c)
            .await
            .unwrap()
            .unwrap()
            .commit,
        c
    );
    unsafe {
        std::env::remove_var("RIPCLONE_TEST_PHASE2_BARRIER_DIR");
        std::env::remove_var("RIPCLONE_TEST_PHASE2_BARRIER_COMMIT");
        std::env::remove_var("RIPCLONE_BUILD_CONCURRENCY");
        std::env::remove_var("RIPCLONE_TESTING");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_rechecks_ready_result_inside_immediate_transaction() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe { std::env::set_var("RIPCLONE_TESTING", "1") };
    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    let origin = make_origin("acme", "immutable");
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    register_added_without_build(&server, "acme/immutable")
        .await
        .unwrap();
    server
        .client()
        .sync_repo("acme/immutable", None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("initial B result completes");

    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/immutable");
    store
        .update_build_status(&repo_id, &b, "failed: injected retry fixture")
        .await
        .unwrap();
    let jobs_before = probe.queue_inserts.load(Ordering::SeqCst);
    let fetches_before = probe.exact_fetches.load(Ordering::SeqCst);
    let builders_before = probe.builder_entries.load(Ordering::SeqCst);
    let uploads_before = probe.artifact_uploads.load(Ordering::SeqCst);
    probe.hold_admission_transaction(&b);

    let retry = post_sync(&server, None);
    let make_ready = async {
        wait_entered(&probe.before_admission_tx, 1).await;
        store
            .update_build_status(&repo_id, &b, "done")
            .await
            .expect("first retry makes B ready before second transaction");
        probe.before_admission_tx.release();
    };
    let ((status, body, _), ()) = tokio::join!(retry, make_ready);
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "retry response: {body}"
    );
    assert_eq!(response_commit(&body), b);
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), jobs_before);
    assert_eq!(probe.exact_fetches.load(Ordering::SeqCst), fetches_before);
    assert_eq!(
        probe.builder_entries.load(Ordering::SeqCst),
        builders_before
    );
    assert_eq!(
        probe.artifact_uploads.load(Ordering::SeqCst),
        uploads_before
    );
    assert_eq!(
        store
            .load_result(&repo_id, &b)
            .await
            .unwrap()
            .unwrap()
            .build_status
            .as_deref(),
        Some("done")
    );
    probe.before_admission_tx.disarm();
    unsafe { std::env::remove_var("RIPCLONE_TESTING") };
}

async fn post_sync(
    server: &Server,
    branch: Option<&str>,
) -> (reqwest::StatusCode, Value, Duration) {
    let mut url = format!("{}/v1/repos/github/acme/immutable/sync", server.url);
    if let Some(branch) = branch {
        url.push_str("?branch=");
        url.push_str(branch);
    }
    let started = Instant::now();
    let response = reqwest::Client::new()
        .post(url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("sync request");
    let status = response.status();
    let body = response.json().await.expect("sync response json");
    (status, body, started.elapsed())
}

async fn admit_repo(server: &Server, repo: &str) -> Value {
    let response = reqwest::Client::new()
        .post(format!("{}/v1/repos/github/{repo}/sync", server.url))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("admission request");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    response.json().await.expect("admission response json")
}

fn response_commit(body: &Value) -> &str {
    body.get("commit")
        .and_then(Value::as_str)
        .expect("accepted response includes exact commit")
}

fn sign_webhook(body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(WEBHOOK_SECRET.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

async fn post_webhook(
    server: &Server,
    branch: &str,
    commit: &str,
) -> (reqwest::StatusCode, Duration) {
    let body = serde_json::json!({
        "ref": format!("refs/heads/{branch}"),
        "after": commit,
        "deleted": false,
        "repository": {
            "name": "immutable",
            "owner": {"login": "acme"},
            "default_branch": "main",
            "private": false
        }
    })
    .to_string()
    .into_bytes();
    let started = Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{}/webhooks/github", server.url))
        .header("X-GitHub-Event", "push")
        .header("X-Hub-Signature-256", sign_webhook(&body))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("webhook request");
    (response.status(), started.elapsed())
}

fn hanging_origin() -> (
    String,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Receiver<bool>,
    Arc<std::sync::atomic::AtomicUsize>,
    std::thread::JoinHandle<()>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("hanging origin listener");
    let addr = listener.local_addr().expect("hanging origin address");
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    let (closed_tx, closed_rx) = std::sync::mpsc::channel();
    let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let thread_connections = Arc::clone(&connections);
    let thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("bounded ls-remote connected");
        thread_connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        accepted_tx
            .send(())
            .expect("report hanging origin connection");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("bound hanging origin read");
        let mut buf = [0u8; 4096];
        let closed_by_client = loop {
            match stream.read(&mut buf) {
                Ok(0) => break true,
                Ok(_) => {}
                Err(_) => break false,
            }
        };
        closed_tx
            .send(closed_by_client)
            .expect("report hanging origin closure");
        // Keep observing the provider boundary briefly after Git closes the
        // first request. A hidden retryability request would establish a
        // second real connection here even though the internal probe counter
        // still says one.
        listener.set_nonblocking(true).unwrap();
        let observe_until = Instant::now() + Duration::from_millis(750);
        while Instant::now() < observe_until {
            match listener.accept() {
                Ok((_extra, _)) => {
                    thread_connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("observe provider connections: {error}"),
            }
        }
    });
    (
        format!("http://{addr}"),
        accepted_rx,
        closed_rx,
        connections,
        thread,
    )
}

fn matching_processes(marker: &str) -> Vec<String> {
    let output = std::process::Command::new("ps")
        .args(["-axo", "pid=,ppid=,state=,command="])
        .output()
        .expect("inspect process table");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.contains(marker) && !line.contains("ps -axo"))
        .map(str::to_string)
        .collect()
}

async fn wait_for_no_matching_process(marker: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let matches = matching_processes(marker);
            if matches.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "ls-remote process tree survived: {:?}",
            matching_processes(marker)
        )
    });
}

async fn run_cli(server: &Server, args: &[&str]) -> (std::process::Output, Duration) {
    let home = tempfile::tempdir().expect("CLI home");
    let started = Instant::now();
    let mut command = std::process::Command::new(cargo_bin("ripclone"));
    command
        .arg("--server")
        .arg(&server.url)
        .args(args)
        .env("HOME", home.path())
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = spawn_bounded_child(&mut command).expect("spawn ripclone CLI");
    let output = wait_child_output_bounded(child, Duration::from_secs(20))
        .await
        .expect("CLI completed within bounded admission window");
    (output, started.elapsed())
}

fn reset_probe(probe: &AdmissionTestProbe) {
    probe
        .enqueue_attempts
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .queue_inserts
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .coalesces
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .tip_probes
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .exact_fetches
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .builder_entries
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .full_publishes
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .ref_store_writes
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .artifact_uploads
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .embedded_notification_wakes
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe
        .embedded_fallback_polls
        .store(0, std::sync::atomic::Ordering::SeqCst);
    probe.fetch_targets.lock().unwrap().clear();
    probe.builder_targets.lock().unwrap().clear();
    probe.failure_targets.lock().unwrap().clear();
    probe.http_trace.lock().unwrap().clear();
}

fn assert_full_artifacts(
    storage: &ripclone::storage::StorageRef,
    info: &ripclone::RefInfo,
    commit: &str,
) {
    assert_eq!(info.commit, commit, "ref identity");
    assert_eq!(info.full_clonepack.commit, commit, "full artifact identity");
    assert!(
        !info.full_clonepack.manifest.is_empty(),
        "full manifest is present"
    );
    let bytes = storage
        .get(&info.full_clonepack.manifest)
        .expect("load exact full manifest bytes");
    assert_eq!(
        ripclone::cas::hash(&bytes),
        info.full_clonepack.manifest,
        "full manifest hash"
    );
    let manifest = ClonepackManifest::decode(bytes.as_slice()).expect("decode exact full manifest");
    assert_eq!(manifest.commit, commit, "manifest commit identity");
    let chunks = manifest_chunk_refs(&manifest);
    assert!(!chunks.is_empty(), "full manifest names artifact chunks");
    for chunk in chunks {
        let hash = hash_to_hex(&chunk.hash);
        let bytes = storage.get(&hash).expect("load exact manifest artifact");
        assert_eq!(bytes.len() as u64, chunk.len, "artifact length for {hash}");
        assert_eq!(
            ripclone::cas::hash(&bytes),
            hash,
            "artifact hash for {hash}"
        );
    }
}

fn content_hashes(value: &Value, out: &mut BTreeSet<String>) {
    match value {
        Value::String(value)
            if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            out.insert(value.clone());
        }
        Value::Array(values) => {
            for value in values {
                content_hashes(value, out);
            }
        }
        Value::Object(values) => {
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
    storage: &ripclone::storage::StorageRef,
    info: &ripclone::RefInfo,
) -> BTreeMap<String, Vec<u8>> {
    let mut hashes = BTreeSet::new();
    content_hashes(
        &serde_json::to_value(info).expect("serialize artifact snapshot"),
        &mut hashes,
    );
    hashes
        .into_iter()
        .map(|hash| {
            let bytes = storage
                .get(&hash)
                .unwrap_or_else(|error| panic!("load artifact {hash}: {error:#}"));
            (hash, bytes)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_admission_wakes_idle_embedded_worker_before_fallback_poll() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "1");
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    setup(false);

    let probe = Arc::new(AdmissionTestProbe::default());
    probe.embedded_idle_wait.arm();
    probe.builder_entry.arm();
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    wait_entered(&probe.embedded_idle_wait, 1).await;

    let origin = make_origin("acme", "immediate-wake");
    origin.commit(&[("value.txt", "wake\n")], "wake");
    origin.publish();
    register_added_without_build(&server, "acme/immediate-wake")
        .await
        .expect("register immediate-wake repo");

    admit_repo(&server, "acme/immediate-wake").await;
    assert_eq!(probe.embedded_notification_wakes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.embedded_fallback_polls.load(Ordering::SeqCst), 0);

    // The fallback sleep is not created until this barrier releases. The
    // post-commit Notify permit is already stored, so the notification side of
    // the select must win without advancing that timer.
    probe.embedded_idle_wait.disarm();
    wait_entered(&probe.builder_entry, 1).await;
    assert_eq!(probe.embedded_notification_wakes.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.embedded_fallback_polls.load(Ordering::SeqCst),
        0,
        "normal committed admission did not use the recovery poll"
    );
    probe.builder_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("notified job settles before fixture teardown");

    unsafe {
        std::env::remove_var("RIPCLONE_BUILD_CONCURRENCY");
        std::env::remove_var("RIPCLONE_TESTING");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn held_full_jobs_release_foreground_slots_for_another_head() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "2");
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    setup(false);

    let probe = Arc::new(AdmissionTestProbe::default());
    probe.phase2_entry.arm();
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;

    let mut commits = Vec::new();
    for repo in ["capacity-a", "capacity-b", "capacity-c"] {
        let origin = make_origin("acme", repo);
        let commit = origin.commit(&[("value.txt", repo)], repo);
        origin.publish();
        register_added_without_build(&server, &format!("acme/{repo}"))
            .await
            .expect("register capacity fixture");
        commits.push((repo, commit, origin));
    }

    for (repo, _, _) in &commits[..2] {
        admit_repo(&server, &format!("acme/{repo}")).await;
    }
    wait_entered(&probe.phase2_entry, 2).await;

    let (third_repo, third_commit, _) = &commits[2];
    admit_repo(&server, &format!("acme/{third_repo}")).await;
    wait_entered(&probe.phase2_entry, 3).await;

    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github(format!("acme/{third_repo}"));
    let exact = store
        .load_result(&repo_id, third_commit)
        .await
        .expect("load third exact Head")
        .expect("third exact Head exists");
    assert_eq!(exact.commit, *third_commit);
    assert_eq!(exact.shallow_clonepack.commit, *third_commit);
    assert!(!exact.shallow_clonepack.manifest.is_empty());
    assert_eq!(exact.build_status.as_deref(), Some("full history building"));

    // The third admission and exact Head publication committed
    // while the first two Full barriers were held. Those writes directly prove
    // that Full work retains no SQLite transaction or write lock.
    probe.phase2_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(3))
        .await
        .expect("all held Full jobs settle after release");

    unsafe {
        std::env::remove_var("RIPCLONE_BUILD_CONCURRENCY");
        std::env::remove_var("RIPCLONE_TESTING");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn incomplete_parent_forces_cold_files_build() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "2");
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    setup(false);
    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    let origin = make_origin("acme", "incomplete-parent");

    origin.commit(
        &[("same-size.txt", "PPPP\n"), ("stable.txt", "stable\n")],
        "P",
    );
    origin.publish();
    register_added_without_build(&server, "acme/incomplete-parent")
        .await
        .expect("register incomplete-parent fixture");
    server
        .client()
        .sync_repo("acme/incomplete-parent", None)
        .await
        .expect("P ready");
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("P Full/Files complete");

    probe.phase2_entry.arm();
    let _a = origin.commit(&[("same-size.txt", "AAAA\n")], "A equal-length edit");
    origin.publish();
    admit_repo(&server, "acme/incomplete-parent").await;
    wait_entered(&probe.phase2_entry, 1).await;

    let b = origin.commit(&[("stable.txt", "B-only\n")], "B while A is phase one");
    origin.publish();
    admit_repo(&server, "acme/incomplete-parent").await;
    wait_entered(&probe.phase2_entry, 2).await;
    probe.phase2_entry.release();
    probe.phase2_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(3))
        .await
        .expect("A and B complete in either order");

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("files-b");
    server
        .client()
        .install_repo_with_mode_at(
            "acme/incomplete-parent",
            "HEAD",
            Some(&b),
            &target,
            ripclone::mode::CloneMode::Files,
            Some("full"),
            None,
        )
        .await
        .expect("Files(B) passes archive integrity verification");
    assert_eq!(
        std::fs::read_to_string(target.join("same-size.txt")).unwrap(),
        "AAAA\n"
    );
    assert_eq!(
        std::fs::read_to_string(target.join("stable.txt")).unwrap(),
        "B-only\n"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_BUILD_CONCURRENCY");
        std::env::remove_var("RIPCLONE_TESTING");
    }
}

/// Direct local composition: ready no-op, fast accepted response, duplicate
/// coalescing before and after claim, separate C identity, exact B fetch after
/// the origin moves, phase-one/full coalescing, and final C publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_sync_admission() {
    let _guard = env_lock().lock().await;
    // One local worker makes the B/C queue order and detached Full barrier
    // observable without relying on build duration.
    unsafe {
        std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "1");
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    setup(false);

    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server_env(&[("RIPCLONE_WEBHOOK_SECRET_GITHUB", WEBHOOK_SECRET)]).await;
    let origin = make_origin("acme", "immutable");

    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/immutable")
        .await
        .expect("register immutable repo");
    server
        .client()
        .sync_repo("acme/immutable", None)
        .await
        .expect("initial A readiness");
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("initial A full publication");
    assert_eq!(
        probe
            .full_publishes
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    reset_probe(&probe);

    // A signed replay for a complete branch-scoped B/A is a read-only no-op:
    // the trusted exact path does not probe the upstream or touch the queue.
    let replay_refs = tree_snapshot(&server.repo_root);
    let replay_storage = tree_snapshot(&server.storage_dir);
    let (replay_status, replay_elapsed) = post_webhook(&server, "main", &a).await;
    assert_eq!(replay_status, reqwest::StatusCode::OK);
    assert!(replay_elapsed < Duration::from_secs(5));
    assert_eq!(
        probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        probe
            .enqueue_attempts
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        replay_refs,
        tree_snapshot(&server.repo_root),
        "complete webhook replay wrote ref metadata"
    );
    assert_eq!(
        replay_storage,
        tree_snapshot(&server.storage_dir),
        "complete webhook replay wrote artifacts"
    );

    // A correctly signed push with a malformed immutable target is ignored
    // before probing, queueing, or mutating durable state. This is a
    // non-vacuous control: the valid signed replay immediately above exercised
    // the same authenticated route and exact-admission parser.
    let malformed_refs = tree_snapshot(&server.repo_root);
    let malformed_storage = tree_snapshot(&server.storage_dir);
    let malformed_probe_count = probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst);
    let malformed_enqueue_count = probe
        .enqueue_attempts
        .load(std::sync::atomic::Ordering::SeqCst);
    let (malformed_status, _) = post_webhook(&server, "main", "not-an-object-id").await;
    assert_eq!(malformed_status, reqwest::StatusCode::OK);
    assert_eq!(
        probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst),
        malformed_probe_count,
        "malformed signed target performed a tip probe"
    );
    assert_eq!(
        probe
            .enqueue_attempts
            .load(std::sync::atomic::Ordering::SeqCst),
        malformed_enqueue_count,
        "malformed signed target reached admission"
    );
    assert_eq!(
        malformed_refs,
        tree_snapshot(&server.repo_root),
        "malformed signed target wrote ref metadata"
    );
    assert_eq!(
        malformed_storage,
        tree_snapshot(&server.storage_dir),
        "malformed signed target wrote artifacts"
    );

    // A complete unchanged sync performs exactly one admission probe and one
    // metadata read, while the durable ref/artifact trees stay byte-identical.
    let refs_before = tree_snapshot(&server.repo_root);
    let storage_before = tree_snapshot(&server.storage_dir);
    let (status, body, ready_elapsed) = post_sync(&server, None).await;
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "ready sync response: {body}"
    );
    assert_eq!(body["status"], "no-op");
    assert_eq!(response_commit(&body), a);
    assert_eq!(
        probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        probe
            .enqueue_attempts
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        probe
            .exact_fetches
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        probe
            .builder_entries
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        probe
            .full_publishes
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        probe
            .ref_store_writes
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        probe
            .artifact_uploads
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        refs_before,
        tree_snapshot(&server.repo_root),
        "ready sync wrote ref state"
    );
    assert_eq!(
        storage_before,
        tree_snapshot(&server.storage_dir),
        "ready sync changed artifact storage"
    );
    eprintln!("ready_noop_ms={}", ready_elapsed.as_millis());

    // Authorization and absent-ref failures stop before admission/source work.
    let unauthorized = reqwest::Client::new()
        .post(format!(
            "{}/v1/repos/github/acme/immutable/sync",
            server.url
        ))
        .header("Authorization", "Ripclone definitely-wrong")
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("unauthorized sync request");
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    let (missing_status, _, _) = post_sync(&server, Some("missing-branch")).await;
    assert_eq!(missing_status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        probe
            .enqueue_attempts
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        probe
            .exact_fetches
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );

    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    probe.before_claim.arm();
    probe.after_claim.arm();
    probe.fetch_entry.arm();
    probe.builder_entry.arm();
    probe.phase2_entry.arm();
    // The worker is now stopped on the pre-receive side of the real local
    // claim boundary. B will remain in the channel until this gate is released.
    wait_entered(&probe.before_claim, 1).await;
    let (b_status, b_body, b_elapsed) = post_sync(&server, None).await;
    assert_eq!(
        b_status,
        reqwest::StatusCode::ACCEPTED,
        "B admission: {b_body}"
    );
    assert_eq!(response_commit(&b_body), b);
    assert_eq!(probe.after_claim.entered(), 0, "B has not been claimed");
    assert!(
        b_elapsed < Duration::from_secs(5),
        "B response waited for blocked worker: {b_elapsed:?}"
    );

    // Concurrent duplicates before claim: the production barrier keeps B in the
    // queue while both real HTTP handlers race their one independent probe.
    let (
        (b_dup_one_status, b_dup_one, b_dup_one_elapsed),
        (b_dup_two_status, b_dup_two, b_dup_two_elapsed),
    ) = tokio::join!(post_sync(&server, None), post_sync(&server, None),);
    assert_eq!(b_dup_one_status, reqwest::StatusCode::ACCEPTED);
    assert_eq!(response_commit(&b_dup_one), b);
    assert_eq!(b_dup_two_status, reqwest::StatusCode::ACCEPTED);
    assert_eq!(response_commit(&b_dup_two), b);
    let (cli_sync, cli_sync_elapsed) = run_cli(&server, &["sync", "acme/immutable"]).await;
    assert!(
        cli_sync.status.success(),
        "CLI sync failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&cli_sync.stdout),
        String::from_utf8_lossy(&cli_sync.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&cli_sync.stdout).trim(),
        format!("accepted {b}"),
        "normal CLI sync reports admission identity"
    );
    assert!(
        cli_sync_elapsed < Duration::from_secs(5),
        "CLI sync waited for the blocked worker: {cli_sync_elapsed:?}"
    );
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(probe.coalesces.load(std::sync::atomic::Ordering::SeqCst), 3);

    assert_eq!(
        probe.queue_inserts.load(Ordering::SeqCst),
        1,
        "one durable B job remains admitted"
    );
    probe.before_claim.release();
    probe.before_claim.disarm();
    wait_entered(&probe.after_claim, 1).await;

    // Duplicate after the worker has claimed B but before source work still
    // coalesces to the same full immutable key.
    let (b_dup_claimed_status, b_dup_claimed, b_dup_claimed_elapsed) =
        post_sync(&server, None).await;
    assert_eq!(b_dup_claimed_status, reqwest::StatusCode::ACCEPTED);
    assert_eq!(response_commit(&b_dup_claimed), b);
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(probe.coalesces.load(std::sync::atomic::Ordering::SeqCst), 4);

    probe.after_claim.release();
    probe.after_claim.disarm();
    wait_entered(&probe.fetch_entry, 1).await;

    // Move upstream only after B is admitted and blocked at exact mirror-fetch
    // entry. C gets its own accepted job while B remains blocked.
    let c = origin.commit(&[("value.txt", "C\n")], "C");
    origin.publish();
    let (c_status, c_body, c_elapsed) = post_sync(&server, None).await;
    assert_eq!(
        c_status,
        reqwest::StatusCode::ACCEPTED,
        "C admission: {c_body}"
    );
    assert_eq!(response_commit(&c_body), c);
    assert!(
        c_elapsed < Duration::from_secs(5),
        "C response waited for blocked B: {c_elapsed:?}"
    );
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(probe.coalesces.load(std::sync::atomic::Ordering::SeqCst), 4);

    // Release B's exact fetch and observe both exact targets before releasing
    // the real builder boundary. No branch-tip substitution is possible here.
    probe.fetch_entry.release();
    probe.fetch_entry.disarm();
    wait_entered(&probe.builder_entry, 1).await;
    assert_eq!(
        probe.fetch_targets.lock().unwrap().as_slice(),
        [b.as_str()],
        "B fetch target changed before C's job ran"
    );
    probe.builder_entry.release();
    probe.builder_entry.disarm();
    wait_entered(&probe.phase2_entry, 1).await;

    // Phase one has published B, while detached Full(B) is held. A signed
    // replay of B is admitted directly by the webhook path and coalesces
    // without probing the moving upstream tip.
    let tip_probes_before_webhook = probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst);
    let (b_phase1_status, b_phase1_elapsed) = post_webhook(&server, "main", &b).await;
    assert_eq!(b_phase1_status, reqwest::StatusCode::OK);
    assert!(
        b_phase1_elapsed < Duration::from_secs(5),
        "webhook replay waited for blocked Full(B): {b_phase1_elapsed:?}"
    );
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(probe.coalesces.load(std::sync::atomic::Ordering::SeqCst), 5);
    assert_eq!(
        probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst),
        tip_probes_before_webhook,
        "signed webhook replay must not perform a moving-tip probe"
    );
    eprintln!(
        "admission_latencies_ms replay={} ready={} B={} dup_before_claim=[{},{}] cli_sync={} dup_after_claim={} C={} webhook_phase1={}",
        replay_elapsed.as_millis(),
        ready_elapsed.as_millis(),
        b_elapsed.as_millis(),
        b_dup_one_elapsed.as_millis(),
        b_dup_two_elapsed.as_millis(),
        cli_sync_elapsed.as_millis(),
        b_dup_claimed_elapsed.as_millis(),
        c_elapsed.as_millis(),
        b_phase1_elapsed.as_millis(),
    );

    // Full(B) is held, but its foreground slot is already free for C. Releasing
    // the shared Full barrier lets both durable claims settle; exact B must
    // remain addressable after the moving branch advances to C.
    probe.phase2_entry.release();
    probe.phase2_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(2))
        .await
        .expect("B and C full publications completed");

    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/immutable");
    let local_storage = ripclone::storage::local(&server.storage_dir)
        .expect("open local artifact storage for exact proof");

    let fetch_targets = probe.fetch_targets.lock().unwrap().clone();
    let mut sorted_fetch_targets = fetch_targets.clone();
    sorted_fetch_targets.sort();
    let mut expected_targets = vec![b.clone(), c.clone()];
    expected_targets.sort();
    assert_eq!(
        sorted_fetch_targets, expected_targets,
        "exact fetch targets"
    );
    let builder_targets = probe.builder_targets.lock().unwrap().clone();
    let mut sorted_builder_targets = builder_targets.clone();
    sorted_builder_targets.sort();
    assert_eq!(
        sorted_builder_targets, expected_targets,
        "exact builder targets"
    );
    assert_eq!(
        probe
            .exact_fetches
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(
        probe
            .builder_entries
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(
        probe
            .full_publishes
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );

    let exact_b = store
        .load_result(&repo_id, &b)
        .await
        .expect("load exact B after C")
        .expect("exact B remains addressable after C");
    assert_full_artifacts(&local_storage, &exact_b, &b);
    assert_eq!(
        store
            .load_result(&repo_id, &c)
            .await
            .expect("load exact C")
            .expect("exact C remains addressable")
            .commit,
        c
    );

    // A real HTTP admission whose one ls-remote hangs is bounded and leaves no
    // queue/source/build side effect. The fixture also proves the killed child
    // closed its socket before the test proceeds.
    let timeout_refs = tree_snapshot(&server.repo_root);
    let timeout_storage = tree_snapshot(&server.storage_dir);
    let timeout_inserts = probe
        .queue_inserts
        .load(std::sync::atomic::Ordering::SeqCst);
    let timeout_fetches = probe
        .exact_fetches
        .load(std::sync::atomic::Ordering::SeqCst);
    let timeout_builders = probe
        .builder_entries
        .load(std::sync::atomic::Ordering::SeqCst);
    let (hanging_base, accepted_rx, closed_rx, hanging_connections, hanging_thread) =
        hanging_origin();
    let old_origin = std::env::var_os("RIPCLONE_ORIGIN_BASE");
    let old_timeout = std::env::var_os("RIPCLONE_LS_REMOTE_TIMEOUT_SECS");
    unsafe {
        std::env::set_var("RIPCLONE_ORIGIN_BASE", &hanging_base);
        std::env::set_var("RIPCLONE_LS_REMOTE_TIMEOUT_SECS", "1");
    }
    let (timeout_status, _, timeout_elapsed) = post_sync(&server, None).await;
    unsafe {
        match old_origin {
            Some(value) => std::env::set_var("RIPCLONE_ORIGIN_BASE", value),
            None => std::env::remove_var("RIPCLONE_ORIGIN_BASE"),
        }
        match old_timeout {
            Some(value) => std::env::set_var("RIPCLONE_LS_REMOTE_TIMEOUT_SECS", value),
            None => std::env::remove_var("RIPCLONE_LS_REMOTE_TIMEOUT_SECS"),
        }
    }
    assert_eq!(timeout_status, reqwest::StatusCode::BAD_GATEWAY);
    assert!(
        timeout_elapsed < Duration::from_secs(5),
        "bounded tip timeout exceeded request bound: {timeout_elapsed:?}"
    );
    accepted_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("timeout fixture received ls-remote");
    assert!(
        closed_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("timeout fixture closed in bounded time"),
        "timed-out ls-remote child did not close the fixture connection"
    );
    hanging_thread
        .join()
        .expect("timeout fixture thread reaped");
    assert_eq!(
        hanging_connections.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "one admission probe must make exactly one provider connection"
    );
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        timeout_inserts
    );
    assert_eq!(
        probe
            .exact_fetches
            .load(std::sync::atomic::Ordering::SeqCst),
        timeout_fetches
    );
    assert_eq!(
        probe
            .builder_entries
            .load(std::sync::atomic::Ordering::SeqCst),
        timeout_builders
    );
    assert_eq!(timeout_refs, tree_snapshot(&server.repo_root));
    assert_eq!(timeout_storage, tree_snapshot(&server.storage_dir));
    eprintln!(
        "admission_counts probes={} inserts={} coalesces={} fetches={} builders={} fulls={} ref_writes={} artifact_uploads={}",
        probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst),
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        probe.coalesces.load(std::sync::atomic::Ordering::SeqCst),
        probe
            .exact_fetches
            .load(std::sync::atomic::Ordering::SeqCst),
        probe
            .builder_entries
            .load(std::sync::atomic::Ordering::SeqCst),
        probe
            .full_publishes
            .load(std::sync::atomic::Ordering::SeqCst),
        probe
            .ref_store_writes
            .load(std::sync::atomic::Ordering::SeqCst),
        probe
            .artifact_uploads
            .load(std::sync::atomic::Ordering::SeqCst),
    );

    // Cancellation is a separate ownership boundary from the wall timeout:
    // drop the real HTTP request while its Git child is connected, then require
    // the socket, direct process, and any helper descendant to be gone long
    // before the configured 30-second timeout could fire.
    reset_probe(&probe);
    let (cancel_base, cancel_accepted, cancel_closed, cancel_connections, cancel_thread) =
        hanging_origin();
    let cancel_marker = cancel_base.clone();
    let old_origin = std::env::var_os("RIPCLONE_ORIGIN_BASE");
    let old_timeout = std::env::var_os("RIPCLONE_LS_REMOTE_TIMEOUT_SECS");
    unsafe {
        std::env::set_var("RIPCLONE_ORIGIN_BASE", &cancel_base);
        std::env::set_var("RIPCLONE_LS_REMOTE_TIMEOUT_SECS", "30");
    }
    let cancel_url = format!("{}/v1/repos/github/acme/immutable/sync", server.url);
    let cancel_started = Instant::now();
    let cancel_request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(cancel_url)
            .header("Authorization", format!("Ripclone {}", token_hash()))
            .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
            .send()
            .await
    });
    cancel_accepted
        .recv_timeout(Duration::from_secs(3))
        .expect("cancelled request reached ls-remote");
    cancel_request.abort();
    let _ = cancel_request.await;
    assert!(
        cancel_closed
            .recv_timeout(Duration::from_secs(5))
            .expect("cancelled ls-remote closed its socket"),
        "request cancellation left the ls-remote connection open"
    );
    cancel_thread.join().expect("cancellation fixture reaped");
    assert_eq!(
        cancel_connections.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "cancelled admission must not start a second provider request"
    );
    wait_for_no_matching_process(&cancel_marker).await;
    assert!(
        cancel_started.elapsed() < Duration::from_secs(10),
        "cancellation cleanup fell through to the 30-second wall timeout"
    );
    unsafe {
        match old_origin {
            Some(value) => std::env::set_var("RIPCLONE_ORIGIN_BASE", value),
            None => std::env::remove_var("RIPCLONE_ORIGIN_BASE"),
        }
        match old_timeout {
            Some(value) => std::env::set_var("RIPCLONE_LS_REMOTE_TIMEOUT_SECS", value),
            None => std::env::remove_var("RIPCLONE_LS_REMOTE_TIMEOUT_SECS"),
        }
    }
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "cancelled resolution admitted work"
    );

    // A caller performs one moving POST, learns the exact target,
    // and then waits exclusively through authenticated pinned metadata GETs.
    let wait_commit = origin.commit(&[("value.txt", "wait\n")], "pinned wait");
    origin.publish();
    reset_probe(&probe);
    probe.phase2_entry.arm();
    let wait_client = server.client();
    let wait_task = tokio::spawn(async move {
        wait_client
            .sync_repo("acme/immutable", None)
            .await
            .expect("sync becomes ready")
    });
    wait_entered(&probe.phase2_entry, 1).await;
    tokio::time::timeout(Duration::from_secs(10), probe.wait_until_http_trace_len(2))
        .await
        .expect("caller reached pinned GET");
    let trace = probe.http_trace.lock().unwrap().clone();
    assert_eq!(
        trace.first().map(String::as_str),
        Some("POST /v1/repos/github/acme/immutable/sync")
    );
    assert_eq!(
        trace.iter().filter(|event| event.contains("/sync")).count(),
        1,
        "pinned wait repeated moving sync: {trace:?}"
    );
    assert!(
        trace.iter().skip(1).all(|event| event.starts_with(&format!(
            "GET /v1/repos/{}/refs/main?pinned={wait_commit}&clonepack=full",
            repo_id.storage_key()
        ))),
        "pinned wait used an unpinned or mutating follow-up: {trace:?}"
    );
    probe.phase2_entry.release();
    probe.phase2_entry.disarm();
    let waited = tokio::time::timeout(Duration::from_secs(60), wait_task)
        .await
        .expect("pinned wait completed")
        .expect("pinned task joined");
    assert_eq!(waited.commit, wait_commit);
    let settled_wait = sync_until_archive_ready(&server, "acme", "immutable").await;
    assert_eq!(settled_wait.commit, wait_commit);

    // Admit an exact target, make only that object unreachable before the
    // claimed worker may fetch, and prove the real local queue/worker reports
    // that target's failure without building the now-visible older tip.
    let unavailable = origin.commit(&[("value.txt", "unavailable\n")], "unavailable");
    origin.publish();
    let before_unavailable = store
        .load_result(&repo_id, &wait_commit)
        .await
        .expect("load ref before unavailable target")
        .expect("prior ready ref exists");
    let unavailable_storage = tree_snapshot(&server.storage_dir);
    reset_probe(&probe);
    probe.after_claim.arm();
    let (unavailable_status, unavailable_body, _) = post_sync(&server, None).await;
    assert_eq!(unavailable_status, reqwest::StatusCode::ACCEPTED);
    assert_eq!(response_commit(&unavailable_body), unavailable);
    wait_entered(&probe.after_claim, 1).await;
    git(&origin.work, &["reset", "--hard", &wait_commit]);
    origin.publish();
    git(&origin.bare, &["reflog", "expire", "--expire=now", "--all"]);
    git(&origin.bare, &["gc", "--prune=now"]);
    assert!(
        !git_ok(
            &origin.bare,
            &["cat-file", "-e", &format!("{unavailable}^{{commit}}")]
        ),
        "unavailable target fixture still exposes the admitted object"
    );
    probe.after_claim.release();
    probe.after_claim.disarm();
    tokio::time::timeout(Duration::from_secs(30), probe.wait_until_failure(1))
        .await
        .expect("unavailable exact target failed clearly");
    let failures = probe.failure_targets.lock().unwrap().clone();
    assert_eq!(failures.len(), 1, "one unavailable-target failure");
    assert_eq!(
        failures[0].0, unavailable,
        "failure retains admitted identity"
    );
    assert!(!failures[0].1.is_empty(), "failure includes a clear cause");
    assert_eq!(
        probe.fetch_targets.lock().unwrap().as_slice(),
        [unavailable.as_str()],
        "worker attempted only the unavailable admitted object"
    );
    assert!(
        probe.builder_targets.lock().unwrap().is_empty(),
        "unavailable job fell forward into a builder"
    );
    let after_unavailable = store
        .load_result(&repo_id, &wait_commit)
        .await
        .expect("load ref after unavailable target")
        .expect("prior ready ref remains");
    assert_eq!(after_unavailable.commit, wait_commit);
    assert_eq!(
        after_unavailable.build_status,
        before_unavailable.build_status
    );
    assert_eq!(
        unavailable_storage,
        tree_snapshot(&server.storage_dir),
        "unavailable exact job wrote artifacts"
    );
    let failed = store
        .load_result(&repo_id, &unavailable)
        .await
        .expect("load failed exact row")
        .expect("failed exact row exists before publication");
    assert_eq!(failed.commit, unavailable);
    assert!(
        failed
            .build_status
            .as_deref()
            .is_some_and(|status| status.starts_with("failed: ")),
        "failed exact row records the unavailable target: {failed:?}"
    );
    reset_probe(&probe);
    probe.after_claim.arm();
    let retry_url = format!(
        "{}/v1/repos/github/acme/immutable/refs/main?clonepack=full&pinned={unavailable}",
        server.url
    );
    let retry = reqwest::Client::new()
        .get(&retry_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("retry failed exact target");
    assert_eq!(retry.status(), reqwest::StatusCode::ACCEPTED);
    let retry_body: serde_json::Value = retry.json().await.expect("retry exact response");
    assert_eq!(response_commit(&retry_body), unavailable);
    wait_entered(&probe.after_claim, 1).await;

    let duplicate = reqwest::Client::new()
        .get(&retry_url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("coalesce duplicate failed exact target");
    assert_eq!(duplicate.status(), reqwest::StatusCode::ACCEPTED);
    let duplicate_body: serde_json::Value = duplicate.json().await.expect("duplicate response");
    assert_eq!(response_commit(&duplicate_body), unavailable);
    assert_eq!(
        probe.queue_inserts.load(Ordering::SeqCst),
        1,
        "failed exact target retries as one coalesced job"
    );
    assert_eq!(probe.coalesces.load(Ordering::SeqCst), 1);
    assert_eq!(probe.tip_probes.load(Ordering::SeqCst), 0);
    probe.after_claim.release();
    probe.after_claim.disarm();
    tokio::time::timeout(Duration::from_secs(30), probe.wait_until_failure(1))
        .await
        .expect("exact retry reaches its failure before fixture teardown");
    assert_eq!(
        probe.fetch_targets.lock().unwrap().as_slice(),
        [unavailable.as_str()],
        "retry fetches only the originally admitted exact target"
    );

    // An evicted completed row is not treated as ready and its historical done
    // job does not block one fresh exact admission.
    let evicted_target = origin.commit(&[("value.txt", "evicted\n")], "evicted target");
    origin.publish();
    let mut evicted = store
        .load_result(&repo_id, &wait_commit)
        .await
        .expect("load exact completed row before eviction")
        .expect("exact completed row exists");
    evicted.commit = evicted_target.clone();
    evicted.full_clonepack.commit = evicted_target.clone();
    evicted.shallow_clonepack.commit = evicted_target.clone();
    evicted.build_status = Some("evicted".to_string());
    store
        .save_result(&repo_id, &evicted)
        .await
        .expect("mark exact completed row evicted");
    assert_eq!(
        store
            .load_result(&repo_id, &evicted_target)
            .await
            .unwrap()
            .unwrap()
            .build_status
            .as_deref(),
        Some("evicted")
    );
    reset_probe(&probe);
    probe.before_claim.arm();
    let (evicted_status, evicted_body, evicted_elapsed) = post_sync(&server, None).await;
    assert_eq!(evicted_status, reqwest::StatusCode::ACCEPTED);
    assert_eq!(response_commit(&evicted_body), evicted_target);
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "evicted target admitted exactly one fresh active job"
    );
    wait_entered(&probe.before_claim, 1).await;

    // The normal CLI add path returns after registration and admission while
    // the real worker remains held at the evicted job's before-claim barrier.
    let add_origin = make_origin("acme", "cli-add");
    let add_commit = add_origin.commit(&[("added.txt", "added\n")], "CLI add");
    add_origin.publish();
    let (cli_add, cli_add_elapsed) = run_cli(&server, &["add", "acme/cli-add"]).await;
    assert!(
        cli_add.status.success(),
        "CLI add failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&cli_add.stdout),
        String::from_utf8_lossy(&cli_add.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&cli_add.stdout).trim(),
        format!("added acme/cli-add; accepted {add_commit}")
    );
    wait_entered(&probe.before_claim, 1).await;
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        2,
        "evicted sync and CLI add each admitted one exact job"
    );
    probe.before_claim.release();
    probe.before_claim.disarm();
    eprintln!(
        "closed_gap_timings_ms cancellation={} evicted_readmission={} cli_add={} active_rows_evicted=1 active_rows_cli_add=1",
        cancel_started.elapsed().as_millis(),
        evicted_elapsed.as_millis(),
        cli_add_elapsed.as_millis(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_build_publishes_exact_commit_result() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    let (server, phase_one, phase_one_entered, phase_one_proceed) =
        start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "ordinary-exact-publish");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/ordinary-exact-publish")
        .await
        .expect("register ordinary exact fixture");
    server
        .client()
        .sync_repo("acme/ordinary-exact-publish", None)
        .await
        .expect("publish initial A");

    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    phase_one.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let admission = server
        .client()
        .admit_sync_repo("acme/ordinary-exact-publish", None)
        .await
        .expect("admit B");
    assert!(admission.accepted);
    assert_eq!(admission.commit, b);
    tokio::time::timeout(Duration::from_secs(20), phase_one_entered)
        .await
        .expect("B phase-one publication entered")
        .expect("phase-one barrier sender alive");

    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/ordinary-exact-publish");
    let phase_one_exact = store
        .load_result(&repo_id, &b)
        .await
        .expect("load exact phase-one B")
        .expect("ordinary build published exact phase-one B");
    assert_eq!(phase_one_exact.commit, b);
    assert_eq!(phase_one_exact.full_clonepack.commit, a);
    assert_eq!(
        phase_one_exact.build_status.as_deref(),
        Some("full history building")
    );

    phase_one_proceed
        .send(())
        .expect("release B phase-one publication");
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("B full publication");
    let exact = store
        .load_result(&repo_id, &b)
        .await
        .expect("load exact B")
        .expect("exact B remains addressable");
    let storage = ripclone::storage::local(&server.storage_dir).expect("open exact storage");
    assert_full_artifacts(&storage, &exact, &b);
    let commits = store
        .list_commits(&repo_id)
        .await
        .expect("list ordinary exact results");
    assert!(commits.iter().any(|commit| commit == &b));
    let status = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/acme/ordinary-exact-publish/status",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("ordinary exact status");
    assert_eq!(status.status(), reqwest::StatusCode::OK);
    let status: serde_json::Value = status.json().await.expect("ordinary exact status body");
    let public_refs = status["refs"].as_array().expect("exact results");
    assert!(public_refs.iter().any(|entry| entry["commit"] == b));
    assert!(status["total_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_and_explicit_requests_share_one_exact_job() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.before_claim.arm();
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    let origin = make_origin("acme", "one-exact-job");
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    register_added_without_build(&server, "acme/one-exact-job")
        .await
        .expect("register exact-job fixture");

    let ordinary = server
        .client()
        .admit_sync_repo("acme/one-exact-job", None)
        .await
        .expect("ordinary B admission");
    assert!(ordinary.accepted);
    assert_eq!(ordinary.commit, b);
    wait_entered(&probe.before_claim, 1).await;

    let explicit = reqwest::Client::new()
        .post(format!(
            "{}/v1/repos/github/acme/one-exact-job/sync?branch=main&rev={b}",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("explicit B admission");
    assert_eq!(explicit.status(), reqwest::StatusCode::ACCEPTED);
    let pending: Value = explicit.json().await.expect("typed pending response");
    assert_eq!(pending["commit"], b);
    assert_eq!(pending["branch"], "main");
    assert_eq!(
        probe.queue_inserts.load(Ordering::SeqCst),
        1,
        "ordinary and explicit B must coalesce onto one active job"
    );

    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/one-exact-job");
    let pending_row = store
        .load_result(&repo_id, &b)
        .await
        .expect("load shared exact row")
        .expect("shared exact row exists before claim");
    assert_eq!(pending_row.commit, b);

    probe.before_claim.release();
    probe.before_claim.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("shared B build completes");
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), 1);
    assert_eq!(store.list_commits(&repo_id).await.unwrap(), vec![b.clone()]);
    unsafe {
        std::env::remove_var("RIPCLONE_TESTING");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_names_for_one_commit_share_result_and_job_but_keep_checkout_name() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe { std::env::set_var("RIPCLONE_TESTING", "1") };
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.before_claim.arm();
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    let origin = make_origin("acme", "same-object-names");
    let commit = origin.commit(&[("value.txt", "one object\n")], "one object");
    origin.publish();
    git(&origin.work, &["branch", "release", &commit]);
    git(&origin.work, &["push", "-q", origin.bare_str(), "release"]);
    register_added_without_build(&server, "acme/same-object-names")
        .await
        .expect("register same-object fixture");

    let main = server
        .client()
        .admit_sync_repo("acme/same-object-names", None)
        .await
        .expect("admit main");
    assert_eq!(main.commit, commit);
    assert_eq!(main.branch, "main");
    wait_entered(&probe.before_claim, 1).await;

    let release = reqwest::Client::new()
        .post(format!(
            "{}/v1/repos/github/acme/same-object-names/sync?branch=release",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("admit release");
    assert_eq!(release.status(), reqwest::StatusCode::ACCEPTED);
    let release: Value = release.json().await.unwrap();
    assert_eq!(release["commit"], commit);
    assert_eq!(release["branch"], "release");
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), 1);
    assert_eq!(probe.tip_probes.load(Ordering::SeqCst), 2);
    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/same-object-names");
    assert_eq!(
        store.list_commits(&repo_id).await.unwrap(),
        vec![commit.clone()]
    );

    probe.before_claim.release();
    probe.before_claim.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("shared exact job completes");
    let ready = server
        .client()
        .resolve_ref_with_clonepack("acme/same-object-names", "release", Some("full"), None)
        .await
        .expect("release name reuses exact result");
    assert_eq!(ready.commit, commit);
    assert_eq!(ready.branch, "release");
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), 1);

    let output = tempfile::tempdir().unwrap();
    let target = output.path().join("clone");
    server
        .client()
        .install_repo_with_mode_at(
            "acme/same-object-names",
            "release",
            None,
            &target,
            ripclone::mode::CloneMode::Editable,
            Some("full"),
            None,
        )
        .await
        .expect("install through release name");
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), commit);
    assert_eq!(
        git(&target, &["symbolic-ref", "--short", "HEAD"]),
        "release"
    );
    unsafe { std::env::remove_var("RIPCLONE_TESTING") };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_branch_rename_reuses_commit_and_returns_each_operations_name() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe { std::env::set_var("RIPCLONE_TESTING", "1") };
    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    let origin = make_origin("acme", "renamed-default");
    let commit = origin.commit(&[("value.txt", "same commit\n")], "same commit");
    origin.publish();
    register_added_without_build(&server, "acme/renamed-default")
        .await
        .expect("register rename fixture");
    server
        .client()
        .sync_repo("acme/renamed-default", None)
        .await
        .expect("build main");
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("main Full completes");
    reset_probe(&probe);

    let before = server
        .client()
        .resolve_ref_with_clonepack("acme/renamed-default", "HEAD", Some("full"), None)
        .await
        .expect("resolve old default");
    assert_eq!(before.branch, "main");
    assert_eq!(before.commit, commit);
    git(&origin.work, &["branch", "trunk", &commit]);
    git(&origin.work, &["push", "-q", origin.bare_str(), "trunk"]);
    git(&origin.bare, &["symbolic-ref", "HEAD", "refs/heads/trunk"]);
    let after = server
        .client()
        .resolve_ref_with_clonepack("acme/renamed-default", "HEAD", Some("full"), None)
        .await
        .expect("resolve renamed default");
    assert_eq!(after.branch, "trunk");
    assert_eq!(after.commit, commit);
    assert_eq!(probe.tip_probes.load(Ordering::SeqCst), 2);
    assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), 0);
    let store = server_ref_store(&server).await;
    assert_eq!(
        store
            .list_commits(&ripclone::provider::RepoId::github("acme/renamed-default"))
            .await
            .unwrap(),
        vec![commit]
    );
    unsafe { std::env::remove_var("RIPCLONE_TESTING") };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reclaimed_worker_cannot_publish_before_heartbeat_detects_loss() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
        std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "1");
    }
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.phase2_entry.arm();
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server_env(&[
        ("RIPCLONE_QUEUE_STALE_SECS", "3"),
        ("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS", "3"),
    ])
    .await;
    let origin = make_origin("acme", "lost-claim");
    let commit = origin.commit(&[("value.txt", "old owner\n")], "held Full");
    origin.publish();
    register_added_without_build(&server, "acme/lost-claim")
        .await
        .expect("register lost-claim fixture");
    let admitted = admit_repo(&server, "acme/lost-claim").await;
    assert_eq!(response_commit(&admitted), commit);
    wait_entered(&probe.phase2_entry, 1).await;

    let takeover = ripclone::queue::SqlJobQueue::new(Box::new(
        ripclone::queue::LibsqlDb::connect(&server.control_db.to_string_lossy())
            .await
            .unwrap(),
    ))
    .await
    .unwrap()
    .with_stale_claim_secs(0);
    let transferred = takeover
        .claim("replacement-owner")
        .await
        .unwrap()
        .expect("replacement takes the held durable claim");
    let writes_before_release = probe.ref_store_writes.load(Ordering::SeqCst);
    // Release the old attempt immediately after reclaim, before its next
    // heartbeat can detect ownership loss. The publication itself must reject
    // the stale `(job_id, worker_id)` atomically.
    probe.phase2_entry.release();
    tokio::time::timeout(Duration::from_secs(20), async {
        while probe.ref_store_writes.load(Ordering::SeqCst) == writes_before_release {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old attempt reaches rejected publication");
    assert_eq!(probe.full_publishes.load(Ordering::SeqCst), 0);
    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/lost-claim");
    let exact = store.load_result(&repo_id, &commit).await.unwrap().unwrap();
    assert!(exact.full_clonepack.manifest.is_empty());
    assert_eq!(
        exact.build_status.as_deref(),
        Some("full history building"),
        "stale owner cannot mark the exact result ready or failed"
    );
    assert!(
        takeover
            .ack(
                transferred.id,
                "replacement-owner",
                Err(ripclone::queue::BuildError::permanent(
                    "test settled replacement"
                )),
            )
            .await
            .unwrap()
    );
    unsafe {
        std::env::remove_var("RIPCLONE_BUILD_CONCURRENCY");
        std::env::remove_var("RIPCLONE_TESTING");
    }
}

/// A historical sync's phase-one row is metadata and shallow readiness only.
/// Holding phase two at its entry makes the HTTP readiness decision observable:
/// the first response must be exact pending until Full(commit) is durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn historical_sync_requires_exact_full_before_ready() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    let probe = Arc::new(AdmissionTestProbe::default());
    probe.phase2_entry.arm();
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server().await;
    let origin = make_origin("acme", "historical-full-ready");
    let pinned = origin.commit(&[("value.txt", "B\n")], "B");
    origin.commit(&[("value.txt", "C\n")], "C");
    origin.publish();
    register_added_without_build(&server, "acme/historical-full-ready")
        .await
        .expect("register historical readiness fixture");

    let response = reqwest::Client::new()
        .post(format!(
            "{}/v1/repos/github/acme/historical-full-ready/sync?rev=HEAD~1",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("historical sync phase-one response");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::ACCEPTED,
        "phase one cannot report exact Full readiness"
    );
    let pending: Value = response.json().await.expect("typed exact pending body");
    assert_eq!(pending["commit"], pinned);
    assert_eq!(pending["branch"], "main");

    wait_entered(&probe.phase2_entry, 1).await;
    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/historical-full-ready");
    let phase_one = store
        .load_result(&repo_id, &pinned)
        .await
        .expect("load historical phase-one row")
        .expect("historical phase-one row is durable");
    assert_eq!(phase_one.commit, pinned);
    assert_eq!(
        phase_one.build_status.as_deref(),
        Some("full history building")
    );
    assert!(phase_one.full_clonepack.manifest.is_empty());

    probe.phase2_entry.release();
    let ready = server
        .client()
        .sync_repo_at("acme/historical-full-ready", Some("HEAD~1"), None)
        .await
        .expect("historical sync reaches exact Full readiness");
    assert_eq!(ready.commit, pinned);
    let exact = store
        .load_result(&repo_id, &pinned)
        .await
        .expect("load historical exact result")
        .expect("historical exact result remains durable");
    assert_eq!(exact.full_clonepack.commit, pinned);
    assert!(!exact.full_clonepack.manifest.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_b_exact_publish_does_not_mutate_c() {
    let _guard = env_lock().lock().await;
    setup(false);
    unsafe {
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    let (server, phase_one, phase_one_entered, phase_one_proceed) =
        start_server_split_storage_phase_one_barrier().await;
    let origin = make_origin("acme", "late-exact-publish");
    let a = origin.commit(&[("value.txt", "A\n")], "A");
    origin.publish();
    register_added_without_build(&server, "acme/late-exact-publish")
        .await
        .expect("register late publication fixture");
    server
        .client()
        .sync_repo("acme/late-exact-publish", None)
        .await
        .expect("publish initial A");

    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    phase_one.arm();
    let b = origin.commit(&[("value.txt", "B\n")], "B");
    origin.publish();
    let b_admission = server
        .client()
        .admit_sync_repo("acme/late-exact-publish", None)
        .await
        .expect("admit B");
    assert!(b_admission.accepted);
    assert_eq!(b_admission.commit, b);
    tokio::time::timeout(Duration::from_secs(20), phase_one_entered)
        .await
        .expect("B phase-one publication entered")
        .expect("phase-one barrier sender alive");

    git(&origin.work, &["reset", "--hard", &a]);
    let c = origin.commit(&[("value.txt", "C\n")], "divergent C");
    origin.publish();
    let c_admission = server
        .client()
        .admit_sync_repo("acme/late-exact-publish", None)
        .await
        .expect("admit C");
    assert!(c_admission.accepted);
    assert_eq!(c_admission.commit, c);
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(1))
        .await
        .expect("C full publication while B is held");

    let store = server_ref_store(&server).await;
    let repo_id = ripclone::provider::RepoId::github("acme/late-exact-publish");
    let exact_c_before = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let exact = store
                .load_result(&repo_id, &c)
                .await
                .expect("load C before delayed B")
                .expect("exact C row");
            if exact.build_status.is_none() && !exact.full_clonepack.manifest.is_empty() {
                break exact;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("C job reaches its terminal status while B is held");
    let exact_b_phase_one = store
        .load_result(&repo_id, &b)
        .await
        .expect("load held exact B")
        .expect("held exact B row");
    assert_eq!(exact_b_phase_one.commit, b);
    let c_json = serde_json::to_value(&exact_c_before).expect("serialize C before delayed B");
    let storage = ripclone::storage::local(&server.storage_dir).expect("open late storage");
    let c_artifacts = artifact_snapshot(&storage, &exact_c_before);
    assert_full_artifacts(&storage, &exact_c_before, &c);

    phase_one_proceed
        .send(())
        .expect("release delayed B publication");
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(2))
        .await
        .expect("B and C full publications");
    let final_c = store
        .load_result(&repo_id, &c)
        .await
        .expect("reload exact C after B")
        .expect("exact C after B");
    assert_eq!(final_c.commit, c);
    assert_eq!(
        serde_json::to_value(&final_c).expect("serialize C after delayed B"),
        c_json,
        "late B publication leaves exact C metadata unchanged"
    );
    assert_eq!(
        artifact_snapshot(&storage, &final_c),
        c_artifacts,
        "late B publication leaves C artifacts byte-identical"
    );
    let exact_b = store
        .load_result(&repo_id, &b)
        .await
        .expect("load delayed exact B")
        .expect("delayed exact B row");
    assert_full_artifacts(&storage, &exact_b, &b);
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        2,
        "B and C are the only admitted jobs"
    );
}
