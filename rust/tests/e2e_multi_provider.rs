//! End-to-end test for non-github providers.
//!
//! Stands up a local git origin served over HTTP, registers it as a `generic`
//! provider, and exercises sync + clone through the explicit-provider URL form.

mod common;

use base64::Engine;
use common::*;
use ripclone::client::Client;
use ripclone::mode::CloneMode;

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
    let server = start_server_env(&[("RIPCLONE_PROVIDERS", &providers_str)]).await;

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

/// GitLab-shaped provider injects `Authorization: Basic base64(oauth2:token)`.
#[tokio::test]
async fn gitlab_provider_injects_basic_oauth2_auth_header() {
    init(false);

    let token = "gitlab-e2e-token";
    let expected_auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("oauth2:{token}"))
    );
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
    let server = start_server_env(&[("RIPCLONE_PROVIDERS", &providers_str)]).await;
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

/// Gitea-shaped provider injects `Authorization: token <token>`.
#[tokio::test]
async fn gitea_provider_injects_token_auth_header() {
    init(false);

    let token = "gitea-e2e-token";
    let expected_auth = format!("token {token}");
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
    let server = start_server_env(&[("RIPCLONE_PROVIDERS", &providers_str)]).await;
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
