//! Failure-injection e2e tests for build/clone fault boundaries.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use std::time::Duration;

// Both cases install process-global test configuration and drive deliberately
// failing background builds. Keep those fault domains isolated from each other.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn repo_status(server: &Server, owner: &str, repo: &str) -> serde_json::Value {
    let url = format!("{}/v1/repos/github/{owner}/{repo}/status", server.url);
    let resp = reqwest::Client::new()
        .get(url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("status request");
    let status = resp.status();
    let text = resp.text().await.expect("status body");
    assert!(
        status.is_success(),
        "status must stay readable after injected failure, got {status}: {text}"
    );
    serde_json::from_str(&text).expect("status json")
}

async fn assert_no_warm_ref_for_commit(server: &Server, owner: &str, repo: &str, commit: &str) {
    let status = repo_status(server, owner, repo).await;
    let refs = status["refs"].as_array().expect("status refs");
    assert!(
        !refs.iter().any(|r| {
            r["commit"] == commit
                && r["warm"] == true
                && r["manifest"].as_str().is_some_and(|m| !m.is_empty())
        }),
        "failed build must not publish a warm ref for {commit}: {status}"
    );
}

#[tokio::test]
async fn storage_upload_failure_mid_build_does_not_publish_partial_ref_and_retry_recovers() {
    let _guard = SERIAL.lock().await;
    // Fails if a durable-storage write error during build can publish a ref whose
    // manifest points at missing objects, if the failed clone leaves a partial
    // worktree, or if retry cannot rebuild the same commit after the fault clears.
    init(false);
    let server = start_server_split_storage_failing_put(1, 1).await;
    let origin = make_origin("acme", "writefail");
    let want = origin.commit(
        &[
            ("a.txt", "good\n"),
            ("dir/b.txt", "also good\n"),
            ("dir/c.txt", "still good\n"),
        ],
        "c1",
    );
    origin.publish();

    register_added_without_build(&server, "acme/writefail")
        .await
        .expect("mark writefail added");
    let first = server.client().sync_repo("acme/writefail", None).await;
    assert!(
        first.is_err(),
        "injected storage upload failure must fail the build, got {first:?}"
    );

    let failed_out = tempfile::tempdir().unwrap();
    let failed_target = failed_out.path().join("clone");
    let failed_clone = tokio::time::timeout(
        Duration::from_secs(5),
        server.client().install_repo_with_mode(
            "acme",
            "writefail",
            "HEAD",
            &failed_target,
            CloneMode::Editable,
            Some("full"),
            None,
        ),
    )
    .await;
    assert!(
        !matches!(failed_clone, Ok(Ok(_))),
        "failed build must not publish cloneable bytes"
    );
    assert!(
        !failed_target.exists(),
        "failed clone must not leave a partial target"
    );
    assert_no_warm_ref_for_commit(&server, "acme", "writefail", &want).await;

    let resp = server
        .client()
        .sync_repo("acme/writefail", None)
        .await
        .expect("retry after injected storage failure");
    assert_eq!(resp.commit, want, "retry rebuilds the intended commit");

    let (_g, clone) = wait_repo_cloneable(&server, "acme", "writefail", "1").await;
    assert_eq!(read(&clone, "a.txt"), "good\n");
    assert_eq!(read(&clone, "dir/b.txt"), "also good\n");
    assert_eq!(read(&clone, "dir/c.txt"), "still good\n");
    assert!(git_ok(&clone, &["fsck", "--connectivity-only", "HEAD"]));
}

#[tokio::test]
async fn ref_store_write_failure_does_not_publish_partial_ref_and_retry_recovers() {
    let _guard = SERIAL.lock().await;
    // Fails if a metadata/DB write error during ref publication leaves a warm
    // ref for the failed commit, if clone can read partial state through that
    // failed publish, or if retry cannot republish cleanly after the fault clears.
    init(false);
    let server = start_server_split_storage_failing_ref_save(0, 1).await;
    let origin = make_origin("acme", "reffail");
    let want = origin.commit(
        &[
            ("a.txt", "metadata survives\n"),
            ("nested/b.txt", "retry repairs\n"),
        ],
        "c1",
    );
    origin.publish();

    register_added_without_build(&server, "acme/reffail")
        .await
        .expect("mark reffail added");
    let first = server.client().sync_repo("acme/reffail", None).await;
    assert!(
        first.is_err(),
        "injected ref-store failure must fail the initial build, got {first:?}"
    );
    assert_no_warm_ref_for_commit(&server, "acme", "reffail", &want).await;

    let failed_out = tempfile::tempdir().unwrap();
    let failed_target = failed_out.path().join("clone");
    let failed_clone = tokio::time::timeout(
        Duration::from_secs(5),
        server.client().install_repo_with_mode(
            "acme",
            "reffail",
            "HEAD",
            &failed_target,
            CloneMode::Editable,
            Some("full"),
            None,
        ),
    )
    .await;
    assert!(
        !matches!(failed_clone, Ok(Ok(_))),
        "failed ref publish must not expose cloneable bytes"
    );
    assert!(
        !failed_target.exists(),
        "failed clone must not leave a partial target"
    );

    let resp = server
        .client()
        .sync_repo("acme/reffail", None)
        .await
        .expect("retry after injected ref-store failure");
    assert_eq!(resp.commit, want, "retry republishes the intended commit");

    let (_g, clone) = wait_repo_cloneable(&server, "acme", "reffail", "1").await;
    assert_eq!(read(&clone, "a.txt"), "metadata survives\n");
    assert_eq!(read(&clone, "nested/b.txt"), "retry repairs\n");
    assert!(git_ok(&clone, &["fsck", "--connectivity-only", "HEAD"]));
}
