use crate::common;

use common::*;

fn snapshot_files(root: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    fn visit(
        root: &std::path::Path,
        current: &std::path::Path,
        files: &mut Vec<(std::path::PathBuf, Vec<u8>)>,
    ) {
        if !current.exists() {
            return;
        }
        for entry in std::fs::read_dir(current).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, files);
            } else if path.is_file() {
                files.push((
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(&path).unwrap(),
                ));
            }
        }
    }

    let mut files = Vec::new();
    visit(root, root, &mut files);
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

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

    let added_record = server_ref_store(&server)
        .await
        .load_added_repo(&ripclone::provider::RepoId::github(&repo_path))
        .await
        .expect("load added-repo state");
    assert!(added_record.is_some(), "add must persist added-repo state");

    let status: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/v1/repos/github/{repo_path}/status", server.url))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
        .send()
        .await
        .expect("status request")
        .error_for_status()
        .expect("status 2xx")
        .json()
        .await
        .expect("status json");
    assert_eq!(status["added"], true);
    let exact = status["refs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["commit"] == added.commit)
        .expect("exact commit status");
    assert!(exact.get("branch").is_none());
    assert_eq!(exact["head"], true);
    assert!(exact["full"].is_boolean());
    assert!(exact["files"].is_boolean());
    assert!(exact["job"].is_string());

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

    let repo_id = ripclone::provider::RepoId::github(&repo_path);
    let store = server_ref_store(&server).await;
    let result_before = serde_json::to_value(
        store
            .load_result(&repo_id, &added.commit)
            .await
            .expect("load exact result before removal")
            .expect("exact result exists before removal"),
    )
    .unwrap();
    let artifacts_before = snapshot_files(&server.cas_dir);
    assert!(!artifacts_before.is_empty());

    server
        .client()
        .remove_repo(&repo_path)
        .await
        .expect("remove repository registration");
    assert!(store.load_added_repo(&repo_id).await.unwrap().is_none());
    let result_after = serde_json::to_value(
        store
            .load_result(&repo_id, &added.commit)
            .await
            .expect("load exact result after removal")
            .expect("exact result exists after removal"),
    )
    .unwrap();
    assert_eq!(
        result_after, result_before,
        "removal must preserve exact results"
    );
    assert_eq!(
        snapshot_files(&server.cas_dir),
        artifacts_before,
        "removal must preserve local artifact bytes"
    );

    let clone_error = server
        .client()
        .resolve_ref(&repo_path, "HEAD")
        .await
        .expect_err("removed repository must not remain cloneable");
    assert!(clone_error.to_string().contains("ripclone add"));
}

#[tokio::test]
async fn remove_isolated_to_one_registration_and_missing_remove_changes_nothing() {
    setup(false);
    let server = start_server().await;
    register_added_without_build(&server, "acme/a")
        .await
        .unwrap();
    register_added_without_build(&server, "acme/b")
        .await
        .unwrap();

    server.client().remove_repo("acme/a").await.unwrap();
    let listed = server.client().list_repos().await.unwrap();
    assert_eq!(listed, vec![ripclone::provider::RepoId::github("acme/b")]);

    let before_missing = server_ref_store(&server)
        .await
        .list_added_repos()
        .await
        .unwrap();
    let error = server
        .client()
        .remove_repo("acme/a")
        .await
        .expect_err("second removal must report not added");
    assert!(error.to_string().contains("repo not added"));
    let after_missing = server_ref_store(&server)
        .await
        .list_added_repos()
        .await
        .unwrap();
    assert_eq!(before_missing, after_missing);
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
            "{}/v1/repos/github/{}/{}/refs/HEAD?result=full",
            server.url, origin.owner, origin.repo
        ))
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .header("x-ripclone-protocol", ripclone::PROTOCOL_VERSION)
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
