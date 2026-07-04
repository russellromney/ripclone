//! End-to-end tests for the per-repo build config (ROADMAP §2a): the admin
//! read/write endpoint, branch overrides, validation, and that a configured
//! compression level still produces a correct clone (config drives the build).

mod common;

use common::*;
use ripclone::mode::CloneMode;
use std::path::Path;

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap()
}

fn admin_url(server: &Server, owner: &str, repo: &str, branch: Option<&str>) -> String {
    let mut url = format!("{}/v1/admin/config/{owner}/{repo}", server.url);
    if let Some(b) = branch {
        url.push_str(&format!("?branch={b}"));
    }
    url
}

async fn admin_put(
    server: &Server,
    owner: &str,
    repo: &str,
    branch: Option<&str>,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(admin_url(server, owner, repo, branch))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .json(&body)
        .send()
        .await
        .expect("admin put request")
}

async fn admin_get(
    server: &Server,
    owner: &str,
    repo: &str,
    branch: Option<&str>,
) -> reqwest::Response {
    reqwest::Client::new()
        .get(admin_url(server, owner, repo, branch))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("admin get request")
}

#[tokio::test]
async fn admin_config_round_trips_with_branch_override() {
    init(false);
    let server = start_server().await;

    // Absent until written.
    assert_eq!(
        admin_get(&server, "acme", "cfg", None).await.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    // Write a repo-level config.
    let resp = admin_put(
        &server,
        "acme",
        "cfg",
        None,
        serde_json::json!({ "compression_level": 9, "hot_files": 12 }),
    )
    .await;
    assert!(
        resp.status().is_success(),
        "put repo config: {:?}",
        resp.status()
    );

    // Read it back.
    let got: serde_json::Value = admin_get(&server, "acme", "cfg", None)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(got["compression_level"], 9);
    assert_eq!(got["hot_files"], 12);

    // Branch-level override is stored separately.
    let resp = admin_put(
        &server,
        "acme",
        "cfg",
        Some("release"),
        serde_json::json!({ "compression_level": 19 }),
    )
    .await;
    assert!(resp.status().is_success());
    let branch_cfg: serde_json::Value = admin_get(&server, "acme", "cfg", Some("release"))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(branch_cfg["compression_level"], 19);

    // The repo-level config is unchanged by the branch write.
    let repo_cfg: serde_json::Value = admin_get(&server, "acme", "cfg", None)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(repo_cfg["compression_level"], 9);
}

#[tokio::test]
async fn admin_rejects_invalid_config() {
    init(false);
    let server = start_server().await;

    // Compression level out of range.
    let resp = admin_put(
        &server,
        "acme",
        "bad",
        None,
        serde_json::json!({ "compression_level": 99 }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Three structural variants are beyond what the build can emit today.
    let resp = admin_put(
        &server,
        "acme",
        "bad",
        None,
        serde_json::json!({
            "clonepack_depths": [
                { "name": "shallow", "depth": 1 },
                { "name": "recent", "depth": 50 },
                { "name": "full", "depth": null }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Nothing got stored.
    assert_eq!(
        admin_get(&server, "acme", "bad", None).await.status(),
        reqwest::StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn configured_compression_still_clones_correctly() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "compress");
    origin.commit(&[("a.txt", "one\n"), ("dir/b.txt", "bee\n")], "c1");
    origin.commit(&[("a.txt", "two\n")], "c2");
    origin.publish();

    // Configure a non-default compression level for this repo.
    let resp = admin_put(
        &server,
        "acme",
        "compress",
        None,
        serde_json::json!({ "compression_level": 3 }),
    )
    .await;
    assert!(resp.status().is_success());

    // The build reads the config; the clone must still be byte-correct.
    let (_g, c) = sync_and_clone(&server, &origin, 0, CloneMode::Editable).await;
    assert_eq!(read(&c, "a.txt"), "two\n");
    assert_eq!(read(&c, "dir/b.txt"), "bee\n");
    assert_eq!(git(&c, &["rev-list", "--count", "HEAD"]), "2");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));

    // Files mode (uses the archive built at the configured level) is correct too.
    let (_g2, f) = sync_and_clone(&server, &origin, 0, CloneMode::Files).await;
    assert_eq!(read(&f, "a.txt"), "two\n");
}

#[tokio::test]
async fn unconfigured_repo_clones_like_today() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "default");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();

    // No config stored → default behavior, clone works as before.
    let (_g, c) = sync_and_clone(&server, &origin, 0, CloneMode::Editable).await;
    assert_eq!(read(&c, "a.txt"), "hello\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));
}
