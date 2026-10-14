//! End-to-end test for non-github providers.
//!
//! Stands up a local git origin served over HTTP, registers it as a `generic`
//! provider, and exercises sync + clone through the explicit-provider URL form.

use crate::common;

use base64::Engine;
use common::*;
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
