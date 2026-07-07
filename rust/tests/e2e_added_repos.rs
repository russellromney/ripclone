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
