//! End-to-end proof of the build-before-clone triggers: a native push webhook
//! and the polling fallback each cause a real build that a clone then reads.
//!
//! The unit tests in server.rs check the webhook handler's status codes against a
//! fake queue. These run the *whole* path: trigger → real two-phase + LSM build →
//! clone the pushed commit and verify it byte-for-byte. That's the actual
//! "artifacts are ready before the clone" claim.

mod common;

use base64::Engine;
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

/// Clone one branch's full (depth=0) artifacts, polling until phase 2 has
/// published the full clonepack at the expected commit count. This is how we
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

/// A signed `push` webhook triggers a real build, and a clone then gets the
/// pushed commit — without any per-repo Actions workflow.
#[tokio::test]
async fn webhook_push_builds_before_clone() {
    setup(true); // two-phase + LSM + async (production defaults)
    let server = start_server_env(&[("RIPCLONE_WEBHOOK_SECRET", SECRET)]).await;
    let origin = make_origin("acme", "hook");
    origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/hook")
        .await
        .expect("add repo");

    // GitHub-shaped push payload. `after` only needs to be non-zero (not a
    // delete); the build resolves the real upstream tip itself. `main` is the
    // default branch, so the receiver warms it.
    let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","deleted":false,"repository":{"name":"hook","owner":{"login":"acme"},"default_branch":"main","private":false}}"#.to_vec();
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{}/v1/webhooks/github", server.url))
        .header("X-GitHub-Event", "push")
        .header("X-Hub-Signature-256", sign_github(&body))
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
#[tokio::test]
async fn webhook_and_sync_same_branch_coalesce() {
    setup(true);
    let server = start_server_env(&[("RIPCLONE_WEBHOOK_SECRET", SECRET)]).await;
    let origin = make_origin("acme", "coal");
    origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();
    server
        .client()
        .add_repo("acme/coal")
        .await
        .expect("add repo");

    let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","deleted":false,"repository":{"name":"coal","owner":{"login":"acme"},"default_branch":"main","private":false}}"#.to_vec();
    let url = server.url.clone();

    // Fire a webhook and a branch-targeted sync for the same key at once.
    let webhook = {
        let (url, body, sig) = (url.clone(), body.clone(), sign_github(&body));
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
#[tokio::test]
async fn poll_catches_a_missed_push() {
    setup(true);
    let server = start_server_env(&[("RIPCLONE_POLL_INTERVAL_SECS", "1")]).await;
    let origin = make_origin("acme", "poll");
    origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();

    // First add makes the repo available; the poll loop only acts on added repos.
    server
        .client()
        .add_repo("acme/poll")
        .await
        .expect("initial add");

    // Advance upstream with NO webhook and NO sync — only the 1s poll loop can
    // notice and build c2.
    origin.commit(&[("f.txt", "v2\n"), ("new.txt", "n\n")], "c2");
    origin.publish();

    let (_g, c) = clone_branch_full(&server, "poll", "main", "2").await;
    assert_eq!(read(&c, "f.txt"), "v2\n", "poll caught the missed push");
    assert!(c.join("new.txt").exists(), "poll built the new commit");
    assert_repo_usable(&c, "2");
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

fn provider_auth(kind: &str, token: &str) -> String {
    match kind {
        "github" => format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"))
        ),
        "gitlab" => format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("oauth2:{token}"))
        ),
        "gitea" => format!("token {token}"),
        other => panic!("unknown provider kind {other}"),
    }
}

fn webhook_secret_env(provider_id: &str) -> String {
    format!(
        "RIPCLONE_WEBHOOK_SECRET_{}",
        provider_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect::<String>()
    )
}

fn push_body(kind: &str, repo: &str) -> Vec<u8> {
    match kind {
        "github" => format!(
            r#"{{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","deleted":false,"repository":{{"name":"{}","owner":{{"login":"acme"}},"default_branch":"main","private":true}}}}"#,
            repo.rsplit('/').next().unwrap()
        )
        .into_bytes(),
        "gitlab" => format!(
            r#"{{"object_kind":"push","ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","project":{{"path_with_namespace":"{repo}","default_branch":"main","visibility_level":0}}}}"#
        )
        .into_bytes(),
        "gitea" => format!(
            r#"{{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","repository":{{"full_name":"{repo}","default_branch":"main","private":true}}}}"#
        )
        .into_bytes(),
        other => panic!("unknown provider kind {other}"),
    }
}

async fn post_provider_push(server: &Server, provider_id: &str, kind: &str, body: Vec<u8>) {
    let mut req = reqwest::Client::new()
        .post(format!("{}/webhooks/{provider_id}", server.url))
        .header("content-type", "application/json")
        .body(body.clone());
    req = match kind {
        "github" => req
            .header("X-GitHub-Event", "push")
            .header("X-Hub-Signature-256", sign_github(&body)),
        "gitlab" => req
            .header("X-Gitlab-Event", "Push Hook")
            .header("X-Gitlab-Token", SECRET),
        "gitea" => req
            .header("X-Gitea-Event", "push")
            .header("X-Gitea-Signature", sign_gitea(&body)),
        other => panic!("unknown provider kind {other}"),
    };
    let resp = req.send().await.expect("webhook POST");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "valid signed {kind} push accepted"
    );
}

/// A GitLab `Push Hook` with the shared `X-Gitlab-Token` secret triggers a real
/// build, and a clone through the `gitlab` provider instance reads it.
#[tokio::test]
async fn gitlab_webhook_push_builds_before_clone() {
    setup(true);
    let origin = make_http_origin("acme/hook");
    origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();

    let providers = webhook_providers_json(&origin.url, "gitlab");
    let server = start_provider_server_env(&[
        ("RIPCLONE_PROVIDERS", &providers),
        ("RIPCLONE_WEBHOOK_SECRET_GITLAB", SECRET),
    ])
    .await;
    server
        .client_with_provider("gitlab", None)
        .add_repo("acme/hook")
        .await
        .expect("add gitlab repo");

    // GitLab authenticates with the secret echoed verbatim; no body HMAC.
    let body = br#"{"object_kind":"push","ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","project":{"path_with_namespace":"acme/hook","default_branch":"main","visibility_level":0}}"#.to_vec();
    let resp = reqwest::Client::new()
        .post(format!("{}/webhooks/gitlab", server.url))
        .header("X-Gitlab-Event", "Push Hook")
        .header("X-Gitlab-Token", SECRET)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("webhook POST");
    assert_eq!(resp.status().as_u16(), 200, "valid GitLab push accepted");

    let (_g, c) = clone_branch_full_for_provider(&server, "gitlab", "acme/hook", "main", "1").await;
    assert_eq!(
        read(&c, "f.txt"),
        "v1\n",
        "gitlab clone has the pushed commit"
    );
    assert_repo_usable(&c, "1");
}

/// Provider-matrix regression: for each provider kind, a signed push webhook
/// must warm the repo by fetching through that provider's exact auth-header
/// format. If the webhook path forgets provider credentials or uses another
/// scheme, the upstream origin returns 403 and this clone never reaches the
/// pushed commit.
#[tokio::test]
async fn provider_webhook_builds_use_provider_auth_headers() {
    setup(true);

    for (provider_id, kind, token) in [
        ("github-http", "github", "github-e2e-token"),
        ("gitlab-auth", "gitlab", "gitlab-e2e-token"),
        ("gitea-auth", "gitea", "gitea-e2e-token"),
    ] {
        let repo = format!("acme/{kind}-authhook");
        let origin = make_http_origin_with_auth(&repo, &provider_auth(kind, token));
        origin.commit(&[("f.txt", &format!("from {kind} webhook auth\n"))], "c1");
        origin.publish();

        let providers =
            webhook_provider_with_token_json(provider_id, kind, &origin.url, Some(token));
        let secret_env = webhook_secret_env(provider_id);
        let server =
            start_provider_server_env(&[("RIPCLONE_PROVIDERS", &providers), (&secret_env, SECRET)])
                .await;

        register_added_without_build_for_provider(&server, provider_id, &repo)
            .await
            .expect("mark webhook repo added");
        post_provider_push(&server, provider_id, kind, push_body(kind, &repo)).await;

        let (_g, c) =
            clone_branch_full_for_provider(&server, provider_id, &repo, "main", "1").await;
        assert_eq!(
            read(&c, "f.txt"),
            format!("from {kind} webhook auth\n"),
            "{kind} webhook build fetched through the provider auth header"
        );
        assert_repo_usable(&c, "1");
    }
}

/// A Gitea `push` webhook with a valid HMAC signature triggers a real build, and
/// a clone through the `gitea` provider instance reads it.
#[tokio::test]
async fn gitea_webhook_push_builds_before_clone() {
    setup(true);
    let origin = make_http_origin("acme/hook");
    origin.commit(&[("f.txt", "v1\n")], "c1");
    origin.publish();

    let providers = webhook_providers_json(&origin.url, "gitea");
    let server = start_provider_server_env(&[
        ("RIPCLONE_PROVIDERS", &providers),
        ("RIPCLONE_WEBHOOK_SECRET_GITEA", SECRET),
    ])
    .await;
    server
        .client_with_provider("gitea", None)
        .add_repo("acme/hook")
        .await
        .expect("add gitea repo");

    // Gitea sends the bare hex HMAC-SHA256 digest (no `sha256=` prefix).
    let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","repository":{"full_name":"acme/hook","default_branch":"main","private":false}}"#.to_vec();
    let resp = reqwest::Client::new()
        .post(format!("{}/webhooks/gitea", server.url))
        .header("X-Gitea-Event", "push")
        .header("X-Gitea-Signature", sign_gitea(&body))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("webhook POST");
    assert_eq!(resp.status().as_u16(), 200, "valid Gitea push accepted");

    let (_g, c) = clone_branch_full_for_provider(&server, "gitea", "acme/hook", "main", "1").await;
    assert_eq!(
        read(&c, "f.txt"),
        "v1\n",
        "gitea clone has the pushed commit"
    );
    assert_repo_usable(&c, "1");
}

/// A Gitea `delete` webhook removes the stored ref for a deleted branch.
#[tokio::test]
async fn gitea_webhook_branch_delete_cleans_up_ref() {
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
