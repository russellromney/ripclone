//! Deterministic direct proof for immutable sync admission.
//!
//! Every concurrency assertion is gated by a production-boundary notification
//! barrier. The fixture never infers overlap from a sleep or from a tiny build.

mod common;

use common::*;
use hmac::{Hmac, KeyInit, Mac};
use ripclone::ref_store::{FileRefStore, RefStore};
use ripclone::server::AdmissionTestProbe;
use serde_json::Value;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const WEBHOOK_SECRET: &str = "immutable-admission-webhook-secret";

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
        .send()
        .await
        .expect("sync request");
    let status = response.status();
    let body = response.json().await.expect("sync response json");
    (status, body, started.elapsed())
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
        .post(format!("{}/v1/webhooks/github", server.url))
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
    std::thread::JoinHandle<()>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("hanging origin listener");
    let addr = listener.local_addr().expect("hanging origin address");
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    let (closed_tx, closed_rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("bounded ls-remote connected");
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
    });
    (format!("http://{addr}"), accepted_rx, closed_rx, thread)
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
    probe.fetch_targets.lock().unwrap().clear();
    probe.builder_targets.lock().unwrap().clear();
}

/// Direct local composition: ready no-op, fast accepted response, duplicate
/// coalescing before and after claim, separate C identity, exact B fetch after
/// the origin moves, phase-one/full coalescing, and final C publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_sync_admission() {
    // One local worker makes the B/C queue order and detached Full barrier
    // observable without relying on build duration.
    unsafe {
        std::env::set_var("RIPCLONE_BUILD_CONCURRENCY", "1");
        std::env::set_var("RIPCLONE_RECHECK_MAX", "0");
        std::env::set_var("RIPCLONE_TESTING", "1");
    }
    setup(false);

    let probe = Arc::new(AdmissionTestProbe::default());
    let _probe_guard = ripclone::server::install_admission_test_probe(Arc::clone(&probe));
    let server = start_server_env(&[("RIPCLONE_WEBHOOK_SECRET", WEBHOOK_SECRET)]).await;
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
    let (b_status, b_body, b_elapsed) = post_sync(&server, None).await;
    assert_eq!(
        b_status,
        reqwest::StatusCode::ACCEPTED,
        "B admission: {b_body}"
    );
    assert_eq!(response_commit(&b_body), b);
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
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(probe.coalesces.load(std::sync::atomic::Ordering::SeqCst), 2);

    wait_entered(&probe.before_claim, 1).await;
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
    assert_eq!(probe.coalesces.load(std::sync::atomic::Ordering::SeqCst), 3);

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
    assert_eq!(probe.coalesces.load(std::sync::atomic::Ordering::SeqCst), 3);

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
    assert_eq!(probe.coalesces.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert_eq!(
        probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst),
        tip_probes_before_webhook,
        "signed webhook replay must not perform a moving-tip probe"
    );
    eprintln!(
        "admission_latencies_ms replay={} ready={} B={} dup_before_claim=[{},{}] dup_after_claim={} C={} webhook_phase1={}",
        replay_elapsed.as_millis(),
        ready_elapsed.as_millis(),
        b_elapsed.as_millis(),
        b_dup_one_elapsed.as_millis(),
        b_dup_two_elapsed.as_millis(),
        b_dup_claimed_elapsed.as_millis(),
        c_elapsed.as_millis(),
        b_phase1_elapsed.as_millis(),
    );

    // Release Full(B), then wait on the full-publication counter for both B and
    // C. The C worker was admitted earlier and remains a distinct queue item.
    probe.phase2_entry.release();
    probe.phase2_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(2))
        .await
        .expect("B and C full publications completed");

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

    let store = FileRefStore::new(&server.repo_root);
    let repo_id = ripclone::provider::RepoId::github("acme/immutable");
    let final_ref = store
        .load_branch(&repo_id, "main")
        .await
        .expect("load final branch")
        .expect("final branch exists");
    assert_eq!(final_ref.commit, c, "ordinary branch settled at C");
    assert!(
        final_ref.full_clonepack.commit == c,
        "final full artifact is C"
    );

    // An older linear webhook may still be admitted as its own immutable
    // target when its branch metadata has already moved to C, but its ordered
    // publication must not move the ordinary branch back to A.
    let tip_probes_before_old_webhook = probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst);
    probe.before_claim.arm();
    probe.fetch_entry.arm();
    let (old_webhook_status, old_webhook_elapsed) = post_webhook(&server, "main", &a).await;
    assert_eq!(old_webhook_status, reqwest::StatusCode::OK);
    assert!(old_webhook_elapsed < Duration::from_secs(5));
    assert_eq!(
        probe
            .queue_inserts
            .load(std::sync::atomic::Ordering::SeqCst),
        3,
        "older webhook gets a separate exact attempt"
    );
    assert_eq!(
        probe.tip_probes.load(std::sync::atomic::Ordering::SeqCst),
        tip_probes_before_old_webhook,
        "older signed webhook must not probe the moving tip"
    );
    wait_entered(&probe.before_claim, 1).await;
    probe.before_claim.release();
    probe.before_claim.disarm();
    wait_entered(&probe.fetch_entry, 1).await;
    assert_eq!(
        probe
            .fetch_targets
            .lock()
            .unwrap()
            .last()
            .map(String::as_str),
        Some(a.as_str()),
        "older webhook fetches its exact admitted commit"
    );
    probe.fetch_entry.release();
    probe.fetch_entry.disarm();
    tokio::time::timeout(Duration::from_secs(60), probe.wait_until_full_published(3))
        .await
        .expect("older webhook exact build completed");
    let final_after_old = store
        .load_branch(&repo_id, "main")
        .await
        .expect("reload final branch")
        .expect("final branch remains present");
    assert_eq!(
        final_after_old.commit, c,
        "older webhook must not move the ordinary branch backward"
    );
    assert_eq!(final_after_old.full_clonepack.commit, c);

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
    let (hanging_base, accepted_rx, closed_rx, hanging_thread) = hanging_origin();
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
}
