//! End-to-end test for non-github providers.
//!
//! Stands up a local git origin served over HTTP, registers it as a `generic`
//! provider, and exercises sync + clone through the explicit-provider URL form.

mod common;

use common::*;
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
    unsafe {
        std::env::set_var("RIPCLONE_PROVIDERS", providers.to_string());
    }

    let server = start_server().await;

    // Sync through the explicit-provider route.
    let client = server.client_with_provider("localgit", Some("test-token"));
    client
        .sync_repo("acme/http", None)
        .await
        .expect("sync generic provider repo");

    // Clone the resulting artifacts. The full clonepack builds in the background
    // under two-phase publish, so poll until it reaches the published commit.
    let mut last = String::from("<no successful clone>");
    let mut found = None;
    for _ in 0..160 {
        let out = tempfile::tempdir().unwrap();
        let target = out.path().join("clone");
        match client
            .install_repo_with_mode_at(
                "acme/http",
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
    let (_out, target) =
        found.unwrap_or_else(|| panic!("generic provider clone never current (last: {last})"));

    // Verify content and origin remote.
    let readme = std::fs::read_to_string(target.join("README.md")).unwrap();
    assert_eq!(readme, "hello from http origin\n");

    let origin_url = git(&target, &["remote", "get-url", "origin"]);
    assert_eq!(origin_url, format!("{}/acme/http.git", origin.url));
}
