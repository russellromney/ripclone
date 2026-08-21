//! Failure-injection e2e tests for build/clone fault boundaries.

use crate::common;

use common::*;

#[tokio::test]
async fn storage_upload_failure_mid_build_does_not_publish_partial_ref_and_retry_recovers() {
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
    // The first exact-B attempt fails, then the same client operation retries
    // only B after the one-shot storage fault clears.
    let resp = server
        .client()
        .sync_repo("acme/writefail", None)
        .await
        .expect("exact retry after injected storage failure");
    assert_eq!(resp.commit, want, "retry rebuilds only the intended commit");
    let store = server_ref_store(&server).await;
    let commits = store
        .list_commits(&ripclone::provider::RepoId::github("acme/writefail"))
        .await
        .expect("list exact retry results");
    assert_eq!(commits, vec![want.clone()], "retry created an extra result");

    let (_g, clone) = wait_repo_cloneable(&server, "acme", "writefail", "1").await;
    assert_eq!(read(&clone, "a.txt"), "good\n");
    assert_eq!(read(&clone, "dir/b.txt"), "also good\n");
    assert_eq!(read(&clone, "dir/c.txt"), "still good\n");
    assert!(git_ok(&clone, &["fsck", "--connectivity-only", "HEAD"]));
}

#[tokio::test]
async fn ref_store_write_failure_does_not_publish_partial_ref_and_retry_recovers() {
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
    // The first exact-B publication fails, then the same request retries B
    // after the one-shot metadata fault clears.
    let resp = server
        .client()
        .sync_repo("acme/reffail", None)
        .await
        .expect("exact retry after injected ref-store failure");
    assert_eq!(
        resp.commit, want,
        "retry republishes only the intended commit"
    );
    let store = server_ref_store(&server).await;
    let commits = store
        .list_commits(&ripclone::provider::RepoId::github("acme/reffail"))
        .await
        .expect("list exact retry results");
    assert_eq!(commits, vec![want.clone()], "retry created an extra result");

    let (_g, clone) = wait_repo_cloneable(&server, "acme", "reffail", "1").await;
    assert_eq!(read(&clone, "a.txt"), "metadata survives\n");
    assert_eq!(read(&clone, "nested/b.txt"), "retry repairs\n");
    assert!(git_ok(&clone, &["fsck", "--connectivity-only", "HEAD"]));
}
