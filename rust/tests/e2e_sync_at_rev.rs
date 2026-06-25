//! Sync + clone at an explicit rev (HEAD~N) to exercise the incremental build
//! path deterministically, without upstream HEAD actually advancing. Sync at an
//! older commit then a newer one, and clone each at its rev to verify the
//! artifacts built for that exact commit.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

/// Clone the full (depth=0) artifacts built for `rev`, polling until phase 2
/// publishes the full clonepack at the expected commit count.
async fn clone_full_rev(
    server: &Server,
    repo: &str,
    rev: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    for _ in 0..200 {
        if let Ok((g, d)) =
            clone_only_at(server, "acme", repo, Some(rev), 0, CloneMode::Editable).await
            && git(&d, &["rev-list", "--count", "HEAD"]) == want_count
        {
            return (g, d);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("depth=0 clone at {rev} never reached {want_count} commits");
}

#[tokio::test]
async fn sync_at_rev_builds_and_clones_older_then_newer() {
    setup(true, true, true); // two-phase + LSM + async (production defaults)
    let server = start_server().await;
    let origin = make_origin("acme", "atrev");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("b.txt", "B\n")], "c2");
    origin.commit(&[("a.txt", "3\n"), ("c.txt", "C\n")], "c3");
    origin.publish();

    let client = server.client();

    // Build at HEAD~2 (= c1); clone at that rev must be exactly c1.
    client
        .sync_repo_at("acme/atrev", Some("HEAD~2"), None)
        .await
        .expect("sync at HEAD~2");
    let (_g1, c1dir) = clone_full_rev(&server, "atrev", "HEAD~2", "1").await;
    assert_eq!(read(&c1dir, "a.txt"), "1\n");
    assert!(!c1dir.join("b.txt").exists(), "c1 has no b.txt");
    assert_repo_usable(&c1dir, "1");

    // Build at HEAD~1 (= c2): a controlled incremental step (synced commit
    // advances c1 -> c2, exercising files-table by-diff + history tail without
    // upstream moving). Clone at that rev must be exactly c2.
    client
        .sync_repo_at("acme/atrev", Some("HEAD~1"), None)
        .await
        .expect("sync at HEAD~1");
    let (_g2, c2dir) = clone_full_rev(&server, "atrev", "HEAD~1", "2").await;
    assert_eq!(read(&c2dir, "a.txt"), "2\n");
    assert_eq!(read(&c2dir, "b.txt"), "B\n");
    assert!(!c2dir.join("c.txt").exists(), "c2 has no c.txt");
    assert_repo_usable(&c2dir, "2");

    // Build at the tip (c3) and verify the full latest state (depth=0 + depth=1).
    client
        .sync_repo_at("acme/atrev", None, None)
        .await
        .expect("sync at tip");
    let (_g3, c3dir) = clone_full_at(&server, "acme", "atrev", "3", true).await;
    assert_eq!(read(&c3dir, "a.txt"), "3\n");
    assert_eq!(read(&c3dir, "c.txt"), "C\n");
    assert_repo_usable(&c3dir, "3");
    let (_g3d1, c3d1) = clone_only(&server, "acme", "atrev", 1, CloneMode::Editable)
        .await
        .expect("depth=1 at tip");
    assert_eq!(read(&c3d1, "a.txt"), "3\n");
}

/// Regression (adversarial review): a `sync --at <older rev>` must NOT clobber
/// the real branch entry that normal tip clients depend on. After a normal tip
/// sync, an at-rev sync of an OLDER commit, then a plain tip clone must still
/// serve the tip correctly. (Rev builds use a rolling key isolated from the
/// branch entry.)
#[tokio::test]
async fn sync_at_rev_does_not_clobber_tip() {
    setup(true, true, true);
    let server = start_server().await;
    let origin = make_origin("acme", "noclob");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();
    let client = server.client();

    // Normal tip sync (builds the real branch entry at c3).
    client.sync_repo("acme/noclob", None).await.unwrap();
    let (_g0, tip0) = clone_full_at(&server, "acme", "noclob", "3", true).await;
    assert_eq!(read(&tip0, "a.txt"), "3\n");

    // Now sync at an OLDER rev. Under the buggy (clobbering) behavior this would
    // overwrite the branch entry with c1 and break the next tip clone.
    client
        .sync_repo_at("acme/noclob", Some("HEAD~2"), None)
        .await
        .unwrap();

    // A plain tip clone must STILL serve c3 correctly.
    let (_g1, tip1) = clone_full_at(&server, "acme", "noclob", "3", true).await;
    assert_eq!(
        read(&tip1, "a.txt"),
        "3\n",
        "tip clone unaffected by at-rev sync"
    );
    assert_repo_usable(&tip1, "3");
}
