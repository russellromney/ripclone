//! End-to-end test for non-github providers.
//!
//! Stands up a local git origin served over HTTP, registers it as a `generic`
//! provider, and exercises sync + clone through the explicit-provider URL form.

mod common;

use base64::Engine;
use common::*;
use ripclone::client::Client;
use ripclone::mode::CloneMode;

async fn start_provider_server(providers: &str) -> Server {
    let isolated_config = origin_root().join("missing-provider-test-config.toml");
    let isolated_config = isolated_config.to_string_lossy().into_owned();
    start_server_env(&[
        ("RIPCLONE_CONFIG", &isolated_config),
        ("RIPCLONE_PROVIDERS", providers),
    ])
    .await
}

#[tokio::test]
async fn generic_provider_sync_and_clone_through_http_origin() {
    init(false);

    // Stand up a local HTTP origin.
    let origin = make_http_origin("acme/http");
    let want = origin.commit(&[("README.md", "hello from http origin\n")], "c1");
    origin.publish();

    // Configure a generic provider pointing at the local HTTP server.
    let providers = serde_json::json!({
        "providers": [{
            "id": "localgit",
            "kind": "generic",
            "host": &origin.url,
            "auth_template": "token {token}",
        }]
    });
    let providers_str = providers.to_string();
    let server = start_provider_server(&providers_str).await;

    // Sync through the explicit-provider route.
    let client = server.client_with_provider("localgit", Some("test-token"));
    client.add_repo("acme/http").await.expect("add repo");
    client
        .sync_repo("acme/http", None)
        .await
        .expect("sync generic provider repo");

    let (_out, target) = clone_full_with_provider(&client, "acme/http", &want).await;

    // Verify content and origin remote.
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert_eq!(readme, "hello from http origin\n");

    let origin_url = git(&target, &["remote", "get-url", "origin"]);
    assert_eq!(origin_url, format!("{}/acme/http.git", origin.url));
}

/// Poll a full clone through an explicit-provider client until it reaches the
/// expected upstream commit. The full clonepack builds in the background, so
/// this retries until phase 2 lands.
async fn clone_full_with_provider(
    client: &Client,
    repo_path: &str,
    want: &str,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let mut last = String::from("<no successful clone>");
    let mut found = None;
    for _ in 0..160 {
        let out = tempfile::tempdir().unwrap();
        let target = out.path().join("clone");
        match client
            .install_repo_with_mode_at(
                repo_path,
                "HEAD",
                None,
                &target,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await
        {
            Ok(_)
                if git_ok(&target, &["rev-parse", "--verify", "HEAD"])
                    && git(&target, &["rev-parse", "HEAD"]) == want =>
            {
                found = Some((out, target));
                break;
            }
            Ok(_) => last = "clone not yet current".to_string(),
            Err(e) => last = format!("clone err: {e:#}"),
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    found.unwrap_or_else(|| panic!("provider clone of {repo_path} never current (last: {last})"))
}

fn preset_provider_auth(kind: &str, token: &str) -> String {
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

/// Fails if the GitHub provider preset stops injecting
/// `Authorization: Basic base64(x-access-token:token)` into real upstream git
/// fetches; the origin rejects every other header and the clone byte-checks the
/// fetched commit.
#[tokio::test]
async fn github_provider_injects_basic_x_access_token_auth_header() {
    init(false);

    let token = "github-e2e-token";
    let expected_auth = preset_provider_auth("github", token);
    let origin = make_http_origin_with_auth("acme/http", &expected_auth);
    let want = origin.commit(&[("README.md", "hello from github origin\n")], "c1");
    origin.publish();

    let providers = serde_json::json!({
        "providers": [{
            "id": "github-http",
            "kind": "github",
            "host": &origin.url,
            "token": token,
        }]
    });
    let providers_str = providers.to_string();
    let server = start_provider_server(&providers_str).await;
    let client = server.client_with_provider("github-http", None);
    client.add_repo("acme/http").await.expect("add repo");
    client
        .sync_repo("acme/http", None)
        .await
        .expect("sync github-shaped provider repo");

    let (_out, target) = clone_full_with_provider(&client, "acme/http", &want).await;
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert_eq!(readme, "hello from github origin\n");
}

/// Fails if the GitLab provider preset stops injecting
/// `Authorization: Basic base64(oauth2:token)` into real upstream git fetches;
/// the protected origin must accept the fetch before the byte-checked clone can
/// succeed.
#[tokio::test]
async fn gitlab_provider_injects_basic_oauth2_auth_header() {
    init(false);

    let token = "gitlab-e2e-token";
    let expected_auth = preset_provider_auth("gitlab", token);
    let origin = make_http_origin_with_auth("acme/http", &expected_auth);
    let want = origin.commit(&[("README.md", "hello from gitlab origin\n")], "c1");
    origin.publish();

    let providers = serde_json::json!({
        "providers": [{
            "id": "gitlab",
            "kind": "gitlab",
            "host": &origin.url,
            "token": token,
        }]
    });
    let providers_str = providers.to_string();
    let server = start_provider_server(&providers_str).await;
    let client = server.client_with_provider("gitlab", None);
    client.add_repo("acme/http").await.expect("add repo");
    client
        .sync_repo("acme/http", None)
        .await
        .expect("sync gitlab provider repo");

    let (_out, target) = clone_full_with_provider(&client, "acme/http", &want).await;
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert_eq!(readme, "hello from gitlab origin\n");
}

/// Fails if the Gitea provider preset stops injecting
/// `Authorization: token <token>` into real upstream git fetches; the protected
/// origin must accept the fetch before the byte-checked clone can succeed.
#[tokio::test]
async fn gitea_provider_injects_token_auth_header() {
    init(false);

    let token = "gitea-e2e-token";
    let expected_auth = preset_provider_auth("gitea", token);
    let origin = make_http_origin_with_auth("acme/http", &expected_auth);
    let want = origin.commit(&[("README.md", "hello from gitea origin\n")], "c1");
    origin.publish();

    let providers = serde_json::json!({
        "providers": [{
            "id": "gitea",
            "kind": "gitea",
            "host": &origin.url,
            "token": token,
        }]
    });
    let providers_str = providers.to_string();
    let server = start_provider_server(&providers_str).await;
    let client = server.client_with_provider("gitea", None);
    client.add_repo("acme/http").await.expect("add repo");
    client
        .sync_repo("acme/http", None)
        .await
        .expect("sync gitea provider repo");

    let (_out, target) = clone_full_with_provider(&client, "acme/http", &want).await;
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert_eq!(readme, "hello from gitea origin\n");
}

/// Wrong or absent provider credentials must fail at the upstream boundary. This
/// test would pass vacuously if it only checked route status: it asserts the
/// protected origin rejects the fetch, so no cloneable bytes are published.
#[tokio::test]
async fn provider_upstream_rejects_wrong_or_absent_auth_headers() {
    init(false);

    for (provider_id, kind, good_token, configured_token) in [
        (
            "github-bad",
            "github",
            "github-good-token",
            Some("github-wrong-token"),
        ),
        (
            "gitlab-bad",
            "gitlab",
            "gitlab-good-token",
            Some("gitlab-wrong-token"),
        ),
        (
            "gitea-bad",
            "gitea",
            "gitea-good-token",
            Some("gitea-wrong-token"),
        ),
        ("github-none", "github", "github-good-token", None),
        ("gitlab-none", "gitlab", "gitlab-good-token", None),
        ("gitea-none", "gitea", "gitea-good-token", None),
    ] {
        let origin = make_http_origin_with_auth(
            &format!("acme/{provider_id}"),
            &preset_provider_auth(kind, good_token),
        );
        origin.commit(&[("README.md", "should stay private\n")], "c1");
        origin.publish();

        let providers = serde_json::json!({
            "providers": [{
                "id": provider_id,
                "kind": kind,
                "host": &origin.url,
                "token": configured_token,
            }]
        });
        let providers_str = providers.to_string();
        let server = start_provider_server(&providers_str).await;
        let client = server.client_with_provider(provider_id, None);
        register_added_without_build_for_provider(
            &server,
            provider_id,
            &format!("acme/{provider_id}"),
        )
        .await
        .expect("mark provider repo added");
        let res = client.sync_repo(&format!("acme/{provider_id}"), None).await;
        assert!(
            res.is_err(),
            "{provider_id} with wrong/absent auth must fail upstream, got {res:?}"
        );
        assert!(
            origin.auth_reject_count() > 0,
            "{provider_id} must reach the protected origin and receive a real 403"
        );

        let out = tempfile::tempdir().unwrap();
        let target = out.path().join("clone");
        let clone = client
            .install_repo_with_mode_at(
                &format!("acme/{provider_id}"),
                "HEAD",
                None,
                &target,
                CloneMode::Editable,
                Some("full"),
                None,
            )
            .await;
        assert!(
            clone.is_err(),
            "{provider_id} must not publish cloneable bytes after upstream auth rejection"
        );
    }
}
