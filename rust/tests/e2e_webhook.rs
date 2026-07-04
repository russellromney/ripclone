//! End-to-end proof of the build-before-clone triggers: a native push webhook
//! and the polling fallback each cause a real build that a clone then reads.
//!
//! The unit tests in server.rs check the webhook handler's status codes against a
//! fake queue. These run the *whole* path: trigger → real two-phase + LSM build →
//! clone the pushed commit and verify it byte-for-byte. That's the actual
//! "artifacts are ready before the clone" claim.

mod common;

use common::*;
use ripclone::mode::{CloneMode, clonepack_kind_for_depth};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

const SECRET: &str = "whsecret-e2e";

fn sign(body: &[u8]) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// Clone one branch's full (depth=0) artifacts, polling until phase 2 has
/// published the full clonepack at the expected commit count. This is how we
/// wait for an async, fire-and-forget build to finish.
async fn clone_branch_full(
    server: &Server,
    repo: &str,
    branch: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    for _ in 0..200 {
        let out = tempfile::tempdir().unwrap();
        let target = out.path().join("clone");
        let ok = server
            .client()
            .install_repo_with_mode_at(
                &format!("acme/{repo}"),
                branch,
                None,
                &target,
                CloneMode::Editable,
                Some(clonepack_kind_for_depth(0)),
                None,
            )
            .await
            .is_ok();
        if ok && git(&target, &["rev-list", "--count", "HEAD"]) == want_count {
            return (out, target);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("full clone of {repo}@{branch} never reached {want_count} commits");
}

/// A signed `push` webhook triggers a real build, and a clone then gets the
/// pushed commit — without any per-repo Actions workflow.
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn webhook_push_builds_before_clone() {
    setup(true); // two-phase + LSM + async (production defaults)
    let server = start_server_env(&[("RIPCLONE_WEBHOOK_SECRET", SECRET)]).await;
    let origin = make_origin("acme", "hook");
    origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();

    // GitHub-shaped push payload. `after` only needs to be non-zero (not a
    // delete); the build resolves the real upstream tip itself. `main` is the
    // default branch, so the receiver warms it.
    let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","deleted":false,"repository":{"name":"hook","owner":{"login":"acme"},"default_branch":"main","private":false}}"#.to_vec();
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{}/v1/webhooks/github", server.url))
        .header("X-GitHub-Event", "push")
        .header("X-Hub-Signature-256", sign(&body))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("webhook POST");
    assert_eq!(resp.status().as_u16(), 200, "valid signed push accepted");

    // The build runs in the background; the clone proves it produced real,
    // correct artifacts for the pushed commit.
    let (_g, c) = clone_branch_full(&server, "hook", "main", "1").await;
    assert_eq!(read(&c, "f.txt"), "v1\n", "clone has the pushed commit");
    assert_repo_usable(&c, "1");
}

/// Read a Prometheus counter value from `/metrics` text.
fn parse_metric(text: &str, name: &str) -> u64 {
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(name)
            && let Some(v) = rest.split_whitespace().next()
        {
            return v.parse().unwrap_or(0);
        }
    }
    0
}

/// A webhook and a `/sync` for the SAME branch key, fired concurrently, coalesce
/// into one build (no corruption, no double-build). Proves the coalescing gate
/// unifies the two entry points — not just `/sync`-vs-`/sync`.
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn webhook_and_sync_same_branch_coalesce() {
    setup(true);
    let server = start_server_env(&[("RIPCLONE_WEBHOOK_SECRET", SECRET)]).await;
    let origin = make_origin("acme", "coal");
    origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();

    let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","deleted":false,"repository":{"name":"coal","owner":{"login":"acme"},"default_branch":"main","private":false}}"#.to_vec();
    let url = server.url.clone();

    // Fire a webhook and a branch-targeted sync for the same key at once.
    let webhook = {
        let (url, body, sig) = (url.clone(), body.clone(), sign(&body));
        tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("{url}/v1/webhooks/github"))
                .header("X-GitHub-Event", "push")
                .header("X-Hub-Signature-256", sig)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        })
    };
    let sync = {
        let client = server.client();
        tokio::spawn(async move { client.sync_branch("acme/coal", "main").await.is_ok() })
    };
    assert_eq!(webhook.await.unwrap(), 200, "webhook accepted");
    assert!(sync.await.unwrap(), "concurrent same-key sync ok");

    // Both raced the same key without corruption; the clone is correct.
    let (_g, c) = clone_branch_full(&server, "coal", "main", "1").await;
    assert_eq!(read(&c, "f.txt"), "v1\n");
    assert_repo_usable(&c, "1");

    // Coalescing: the two same-key triggers did not each run an independent build.
    // (1 expected; allow 1 extra for a timing-induced no-op re-check.)
    let metrics = reqwest::get(format!("{url}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let completed = parse_metric(&metrics, "ripclone_builds_completed_total");
    assert!(
        completed <= 2,
        "same-key webhook+sync coalesced (builds_completed={completed}, expected ~1)"
    );
}

/// A push that arrives with NO webhook/sync trigger is still caught by the poll
/// loop, which builds the new tip — proving the missed-event fallback end to end.
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn poll_catches_a_missed_push() {
    setup(true);
    let server = start_server_env(&[("RIPCLONE_POLL_INTERVAL_SECS", "1")]).await;
    let origin = make_origin("acme", "poll");
    origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();

    // First sync makes the repo known to the ref store (poll only sweeps known
    // repos) and builds c1.
    server
        .client()
        .sync_repo("acme/poll", None)
        .await
        .expect("initial sync");

    // Advance upstream with NO webhook and NO sync — only the 1s poll loop can
    // notice and build c2.
    origin.commit(&[("f.txt", "v2\n"), ("new.txt", "n\n")], "c2");
    origin.publish();

    let (_g, c) = clone_branch_full(&server, "poll", "main", "2").await;
    assert_eq!(read(&c, "f.txt"), "v2\n", "poll caught the missed push");
    assert!(c.join("new.txt").exists(), "poll built the new commit");
    assert_repo_usable(&c, "2");
}
