//! End-to-end tests for the LSM incremental history build (RIPCLONE_LSM=1, with
//! a 1-byte seal threshold so every non-empty tail seals a level). Drives real
//! server sync + client full clones across multiple seal cycles and a rewrite.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use std::path::Path;

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap()
}

async fn full_clone(server: &Server, origin: &Origin) -> (tempfile::TempDir, std::path::PathBuf) {
    sync_and_clone(server, origin, 0, CloneMode::Editable).await
}

/// Across three seal cycles (each sync adds commits and seals a new level), the
/// full clone stays complete, fsck-clean, and materializes the latest worktree.
/// This exercises seal -> reuse-by-hash -> seal -> reuse end to end.
#[tokio::test]
async fn lsm_multi_seal_full_clone_stays_complete() {
    init(true);
    let server = start_server().await;
    let origin = make_origin("acme", "lsm-multi");

    // Cycle 1.
    origin.commit(&[("a.txt", "v1\n")], "c1");
    origin.publish();
    let (_g1, c1) = full_clone(&server, &origin).await;
    assert_eq!(read(&c1, "a.txt"), "v1\n");
    assert_eq!(git(&c1, &["rev-list", "--count", "HEAD"]), "1");
    assert!(git_ok(&c1, &["fsck", "--connectivity-only", "HEAD"]));

    // Cycle 2: change an existing file (its old blob must survive in level 0) +
    // add a file. New tail since the sealed tip.
    origin.commit(&[("a.txt", "v2\n"), ("b.txt", "bee\n")], "c2");
    origin.commit(&[("a.txt", "v3\n")], "c3");
    origin.publish();
    let (_g2, c2) = full_clone(&server, &origin).await;
    assert_eq!(read(&c2, "a.txt"), "v3\n");
    assert_eq!(read(&c2, "b.txt"), "bee\n");
    assert_eq!(
        git(&c2, &["rev-list", "--count", "HEAD"]),
        "3",
        "all 3 commits"
    );
    assert!(
        git_ok(&c2, &["rev-list", "--objects", "HEAD"]),
        "complete object closure after a reuse+seal cycle"
    );
    assert!(git_ok(&c2, &["fsck", "--connectivity-only", "HEAD"]));
    // Old version of a.txt (from c1, current at the first seal point) must exist.
    let v1_blob = git(&origin.work, &["rev-parse", "HEAD~2:a.txt"]);
    assert!(
        git_ok(&c2, &["cat-file", "-e", &v1_blob]),
        "blob from a sealed level must be present"
    );

    // Cycle 3.
    origin.commit(&[("a.txt", "v4\n"), ("c.txt", "see\n")], "c4");
    origin.publish();
    let (_g3, c3) = full_clone(&server, &origin).await;
    assert_eq!(read(&c3, "a.txt"), "v4\n");
    assert_eq!(read(&c3, "c.txt"), "see\n");
    assert_eq!(git(&c3, &["rev-list", "--count", "HEAD"]), "4");
    assert!(git_ok(&c3, &["fsck", "--connectivity-only", "HEAD"]));
    assert_eq!(git(&c3, &["status", "--porcelain"]), "");
}

/// A history rewrite (force-push that makes HEAD not a descendant of the sealed
/// tip) still yields a complete, connectivity-clean clone — the sealed level's
/// now-unreachable objects are harmless dangling, never a missing object.
#[tokio::test]
async fn lsm_force_push_full_clone_stays_complete() {
    init(true);
    let server = start_server().await;
    let origin = make_origin("acme", "lsm-rewrite");

    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();
    let (_g1, _c1) = full_clone(&server, &origin).await; // seal level 0 @ c2

    // Rewrite the tip: amend c2 into a sibling commit (same parent c1), so the
    // old c2 is no longer reachable from the new HEAD.
    std::fs::write(origin.work.join("a.txt"), "2-rewritten\n").unwrap();
    git(&origin.work, &["add", "-A"]);
    git(
        &origin.work,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "--amend",
            "-m",
            "c2b",
        ],
    );
    origin.publish();

    let (_g2, c2) = full_clone(&server, &origin).await;
    assert_eq!(read(&c2, "a.txt"), "2-rewritten\n");
    assert!(
        git_ok(&c2, &["rev-list", "--objects", "HEAD"]),
        "rewrite must still produce a complete object closure"
    );
    assert!(
        git_ok(&c2, &["fsck", "--connectivity-only", "HEAD"]),
        "fsck connectivity from HEAD must be clean after a rewrite"
    );
    assert_eq!(git(&c2, &["status", "--porcelain"]), "");
}

/// Re-syncing at the same HEAD under LSM (empty tail) reuses the sealed level and
/// still clones a complete repo.
#[tokio::test]
async fn lsm_resync_same_head_reuses_level() {
    init(true);
    let server = start_server().await;
    let origin = make_origin("acme", "lsm-noop");
    origin.commit(&[("a", "1\n")], "c1");
    origin.commit(&[("a", "2\n")], "c2");
    origin.publish();

    let client = server.client();
    client
        .sync_repo("acme", "lsm-noop", None, None)
        .await
        .unwrap(); // seal
    client
        .sync_repo("acme", "lsm-noop", None, None)
        .await
        .unwrap(); // empty tail, reuse

    let (_g, c) = full_clone(&server, &origin).await;
    assert_eq!(read(&c, "a"), "2\n");
    assert_eq!(git(&c, &["rev-list", "--count", "HEAD"]), "2");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));
}
