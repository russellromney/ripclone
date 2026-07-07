//! Failure-injection e2e tests for build/clone fault boundaries.

mod common;

use common::*;
use ripclone::mode::CloneMode;

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

    let first = server.client().sync_repo("acme/writefail", None).await;
    assert!(
        first.is_err(),
        "injected storage upload failure must fail the build, got {first:?}"
    );

    let failed_out = tempfile::tempdir().unwrap();
    let failed_target = failed_out.path().join("clone");
    let failed_clone = server
        .client()
        .install_repo_with_mode(
            "acme",
            "writefail",
            "HEAD",
            &failed_target,
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await;
    assert!(
        failed_clone.is_err(),
        "failed build must not publish cloneable bytes"
    );
    assert!(
        !failed_target.exists(),
        "failed clone must not leave a partial target"
    );

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
