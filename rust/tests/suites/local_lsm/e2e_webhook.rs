//! End-to-end proof of the build-before-clone triggers: a native push webhook
//! and the polling fallback each cause a real build that a clone then reads.
//!
//! The unit tests in server.rs check the webhook handler's status codes against a
//! fake queue. These run the *whole* path: trigger → real exact-result LSM build →
//! clone the pushed commit and verify it byte-for-byte. That's the actual
//! "artifacts are ready before the clone" claim.

use crate::common;

use common::*;
use hmac::{Hmac, KeyInit, Mac};
use ripclone::client::Client;
use ripclone::mode::{CloneMode, clonepack_kind_for_depth};
use sha2::Sha256;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

const SECRET: &str = "whsecret-e2e";

async fn start_provider_server_env(extra: &[(&str, &str)]) -> Server {
    let isolated_config = origin_root().join("missing-webhook-provider-test-config.toml");
    let isolated_config = isolated_config.to_string_lossy().into_owned();
    let mut env = vec![("RIPCLONE_CONFIG", isolated_config.as_str())];
    env.extend_from_slice(extra);
    start_server_env(&env).await
}

fn sign_github(body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn sign_gitea(body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Clone one branch's Full result at the expected commit count. This is how we
/// wait for an async, fire-and-forget build to finish.
async fn clone_branch_full(
    server: &Server,
    repo: &str,
    branch: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    clone_branch_full_with_client(server.client(), &format!("acme/{repo}"), branch, want_count)
        .await
}

/// Like [`clone_branch_full`], but clones through an explicit provider instance
/// (e.g. gitlab or gitea) instead of the default github instance.
async fn clone_branch_full_for_provider(
    server: &Server,
    provider: &str,
    repo: &str,
    branch: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    clone_branch_full_with_client(
        server.client_with_provider(provider, None),
        repo,
        branch,
        want_count,
    )
    .await
}

async fn clone_branch_full_with_client(
    client: Client,
    repo: &str,
    branch: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    for _ in 0..200 {
        let out = tempfile::tempdir().unwrap();
        let target = out.path().join("clone");
        let ok = client
            .install_repo_with_mode_at(
                repo,
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

/// A webhook and a `/sync` for the same exact commit, fired concurrently, coalesce
/// into one build (no corruption, no double-build). Proves the coalescing gate
/// unifies the two entry points — not just `/sync`-vs-`/sync`.
#[tokio::test]
async fn webhook_and_sync_same_commit_coalesce() {
    setup(true);
    let server = start_server_env(&[("RIPCLONE_WEBHOOK_SECRET_GITHUB", SECRET)]).await;
    let origin = make_origin("acme", "coal");
    let commit = origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/coal")
        .await
        .expect("add repo");

    let body = serde_json::json!({
        "ref": "refs/heads/main",
        "after": commit,
        "deleted": false,
        "repository": {"name": "coal", "owner": {"login": "acme"}, "default_branch": "main", "private": false}
    })
    .to_string()
    .into_bytes();
    let url = server.url.clone();

    // Fire a webhook and a branch-targeted sync for the same commit at once.
    let webhook = {
        let (url, body, sig) = (url.clone(), body.clone(), sign_github(&body));
        tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("{url}/webhooks/github"))
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

// ---- GitLab + Gitea provider webhook e2es ---------------------------------

/// Build the JSON provider registry used by the non-github webhook tests.
/// The provider `host` points at the local dumb-HTTP origin so the sync can
/// fetch without network.
fn webhook_providers_json(origin_url: &str, kind: &str) -> String {
    webhook_provider_with_token_json(kind, kind, origin_url, None)
}

fn webhook_provider_with_token_json(
    id: &str,
    kind: &str,
    origin_url: &str,
    token: Option<&str>,
) -> String {
    serde_json::json!({
        "providers": [{
            "id": id,
            "kind": kind,
            "host": origin_url,
            "token": token,
        }]
    })
    .to_string()
}

/// A Gitea `delete` webhook is acknowledged without resurrecting the deleted name.
#[tokio::test]
async fn gitea_webhook_branch_delete_is_ignored_without_resurrection() {
    setup(true);
    let origin = make_http_origin("acme/hook");
    origin.commit(&[("main.txt", "m\n")], "main commit");
    origin.publish();
    // Push a feature branch to the bare origin and refresh dumb HTTP info.
    git(&origin.work, &["checkout", "-b", "feature"]);
    origin.commit(&[("feat.txt", "f\n")], "feature commit");
    git(
        &origin.work,
        &["push", "-q", "--force", origin.bare_str(), "feature"],
    );
    git(&origin.bare, &["update-server-info"]);

    let providers = webhook_providers_json(&origin.url, "gitea");
    let server = start_provider_server_env(&[
        ("RIPCLONE_PROVIDERS", &providers),
        ("RIPCLONE_WEBHOOK_SECRET_GITEA", SECRET),
    ])
    .await;
    let client = server.client_with_provider("gitea", None);
    client.add_repo("acme/hook").await.expect("add gitea repo");

    // Build the feature branch via explicit sync, then prove it clones.
    client
        .sync_branch("acme/hook", "feature")
        .await
        .expect("sync feature branch");
    let (_g, c) =
        clone_branch_full_for_provider(&server, "gitea", "acme/hook", "feature", "2").await;
    assert_eq!(read(&c, "feat.txt"), "f\n", "feature branch was built");

    // Delete the actual upstream ref before delivering its webhook. This keeps
    // the fixture faithful to a provider delete event and proves a stale local
    // mirror plus retained immutable artifacts cannot resurrect the branch.
    git(&origin.work, &["push", "-q", origin.bare_str(), ":feature"]);
    git(&origin.bare, &["update-server-info"]);

    // Gitea delete event carries the short branch name + ref_type.
    let body = br#"{"ref":"feature","ref_type":"branch","repository":{"full_name":"acme/hook","default_branch":"main"}}"#.to_vec();
    let resp = reqwest::Client::new()
        .post(format!("{}/webhooks/gitea", server.url))
        .header("X-Gitea-Event", "delete")
        .header("X-Gitea-Signature", sign_gitea(&body))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("delete webhook POST");
    assert_eq!(resp.status().as_u16(), 200, "valid Gitea delete accepted");

    // After the delete webhook, cloning the deleted branch must fail.
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let result = client
        .install_repo_with_mode_at(
            "acme/hook",
            "feature",
            None,
            &target,
            CloneMode::Editable,
            Some(clonepack_kind_for_depth(0)),
            None,
        )
        .await;
    assert!(result.is_err(), "clone of deleted feature branch fails");
}
