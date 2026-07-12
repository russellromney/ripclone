mod common;

use common::*;

#[tokio::test]
async fn add_registers_builds_and_makes_repo_cloneable() {
    setup(false);
    let origin = make_origin("b5_add", "repo");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.publish();

    let server = start_server().await;
    let client = server.client();
    let repo_path = format!("{}/{}", origin.owner, origin.repo);

    let added = client.add_repo(&repo_path).await.expect("add repo");
    assert_eq!(added.commit, git(&origin.bare, &["rev-parse", "HEAD"]));

    let added_record = server
        .repo_root
        .join(".ripclone-added")
        .join("github")
        .join("b5_add%2Frepo.json");
    assert!(added_record.exists(), "add must persist added-repo state");
    let persisted: ripclone::ref_store::AddedRepo =
        serde_json::from_slice(&std::fs::read(&added_record).expect("read admission row"))
            .expect("parse admission row");
    assert_eq!(
        persisted.state,
        ripclone::ref_store::RepoLifecycleState::Active
    );
    assert_eq!(
        persisted.initialization_target.as_deref(),
        Some(added.commit.as_str())
    );

    let status: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/v1/repos/github/{repo_path}/status", server.url))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("status request")
        .error_for_status()
        .expect("status 2xx")
        .json()
        .await
        .expect("status json");
    assert_eq!(status["added"], true);
    assert_eq!(status["lifecycle"], "active");
    let main = status["refs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["branch"] == "main")
        .expect("main status");
    assert_eq!(main["depth1_ready"], true);
    assert!(main["archive_ready"].is_boolean());
    assert!(
        ["ready", "building"].contains(&main["history"].as_str().unwrap()),
        "unexpected history status: {}",
        main["history"]
    );

    let (_tmp, clone) = clone_only(
        &server,
        &origin.owner,
        &origin.repo,
        1,
        ripclone::mode::CloneMode::Editable,
    )
    .await
    .expect("clone after add");
    assert_eq!(read(&clone, "a.txt"), "1\n");
}

#[tokio::test]
async fn initializing_repo_is_visible_in_status_but_not_cloneable() {
    use ripclone::ref_store::{
        AddedRepo, AddedRepoSource, FileRefStore, RefStore, RepoLifecycleState,
    };

    setup(false);
    let origin = make_origin("b5_initializing", "repo");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.publish();
    let server = start_server().await;
    let repo_id = ripclone::provider::RepoId::github("b5_initializing/repo");
    FileRefStore::new(&server.repo_root)
        .add_repo(&AddedRepo {
            repo_id,
            added_at: 1,
            history_enabled: true,
            source: AddedRepoSource::Api,
            repo_size_bytes: None,
            state: RepoLifecycleState::Initializing,
            initialization_branch: Some("HEAD".into()),
            initialization_target: Some(git(&origin.bare, &["rev-parse", "HEAD"])),
            activated_at: None,
            failure: None,
        })
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let status: serde_json::Value = client
        .get(format!(
            "{}/v1/repos/github/b5_initializing/repo/status",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["added"], false);
    assert_eq!(status["lifecycle"], "initializing");

    let ref_response = client
        .get(format!(
            "{}/v1/repos/github/b5_initializing/repo/refs/HEAD",
            server.url
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .unwrap();
    assert_eq!(ref_response.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = ref_response.json().await.unwrap();
    assert_eq!(body["code"], "repo_not_added");
}

#[tokio::test]
async fn non_added_repo_ref_and_sync_are_rejected() {
    setup(false);
    let origin = make_origin("b5_missing", "repo");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.publish();

    let server = start_server().await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/v1/repos/github/{}/{}/refs/HEAD",
            server.url, origin.owner, origin.repo
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("ref request");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await.expect("error json");
    assert_eq!(body["code"], "repo_not_added");

    let err = server
        .client()
        .sync_repo(&format!("{}/{}", origin.owner, origin.repo), None)
        .await
        .expect_err("sync non-added");
    assert!(
        err.to_string().contains("ripclone add"),
        "unexpected sync error: {err:#}"
    );
}
