//! End-to-end tests for two-phase publish (RIPCLONE_TWO_PHASE=1): a sync
//! publishes the depth=1 clonepack in the foreground and builds full history in
//! the background, so depth=1 is clonable immediately and depth=0 shortly after.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use std::path::Path;
use std::time::Duration;

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap()
}

/// After a single two-phase sync: depth=1 is immediately clonable + correct, and
/// depth=0 becomes a complete, fsck-clean full clone once phase 2 finishes.
#[tokio::test]
async fn two_phase_depth1_immediate_then_full() {
    enable_two_phase();
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tp");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("b.txt", "x\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();

    // Sync returns after phase 1 (depth=1 published; full builds in background).
    server
        .client()
        .sync_repo("acme", "tp", None, None)
        .await
        .expect("sync");

    // depth=1 clonable immediately.
    let (_g1, c1) = clone_only(&server, "acme", "tp", 1, CloneMode::Editable)
        .await
        .expect("depth=1 clone right after sync");
    assert_eq!(read(&c1, "a.txt"), "3\n");
    assert_eq!(read(&c1, "b.txt"), "x\n");
    assert!(c1.join(".git/shallow").exists(), "depth=1 is shallow");
    assert_eq!(git(&c1, &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(git(&c1, &["status", "--porcelain"]), "");

    // depth=0 becomes available once phase 2 finishes (poll up to ~30s).
    let mut full: Option<(tempfile::TempDir, std::path::PathBuf)> = None;
    for _ in 0..120 {
        if let Ok((g, d)) = clone_only(&server, "acme", "tp", 0, CloneMode::Editable).await
            && git(&d, &["rev-list", "--count", "HEAD"]) == "3"
        {
            full = Some((g, d));
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let (_g0, c0) = full.expect("depth=0 full clone available after phase 2");
    assert_eq!(read(&c0, "a.txt"), "3\n");
    assert!(!c0.join(".git/shallow").exists(), "full clone not shallow");
    assert_eq!(git(&c0, &["rev-list", "--count", "HEAD"]), "3");
    assert!(
        git_ok(&c0, &["rev-list", "--objects", "HEAD"]),
        "full object closure complete"
    );
    assert!(git_ok(&c0, &["fsck", "--connectivity-only", "HEAD"]));
    assert_eq!(git(&c0, &["status", "--porcelain"]), "");
}

/// Files mode works under two-phase: the zstd archive is deferred to phase 2,
/// so a files-mode clone of the full variant materializes the worktree from the
/// frames built in the background.
#[tokio::test]
async fn two_phase_files_mode_after_phase2() {
    enable_two_phase();
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tpf");
    origin.commit(&[("a.txt", "hello\n"), ("nested/b.txt", "world\n")], "c1");
    origin.commit(&[("a.txt", "hello2\n")], "c2");
    origin.publish();
    server
        .client()
        .sync_repo("acme", "tpf", None, None)
        .await
        .expect("sync");

    // Poll until phase 2 publishes the full variant, then clone files mode.
    let mut materialized = false;
    for _ in 0..120 {
        if let Ok((_g, d)) = clone_only(&server, "acme", "tpf", 0, CloneMode::Files).await
            && d.join("a.txt").exists()
        {
            assert_eq!(read(&d, "a.txt"), "hello2\n");
            assert_eq!(read(&d, "nested/b.txt"), "world\n");
            materialized = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        materialized,
        "files-mode worktree materializes after phase 2"
    );
}

/// Option A: after upstream advances, depth=0 keeps serving the PREVIOUS commit
/// during the gap (never fails), then upgrades to the new commit. We assert the
/// end state — depth=0 reaches the new commit and is complete — across a second
/// two-phase sync.
#[tokio::test]
async fn two_phase_resync_full_upgrades() {
    enable_two_phase();
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tp2");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();
    server
        .client()
        .sync_repo("acme", "tp2", None, None)
        .await
        .unwrap();

    // Wait for the first full to land.
    let mut ready = false;
    for _ in 0..120 {
        if let Ok((_g, d)) = clone_only(&server, "acme", "tp2", 0, CloneMode::Editable).await
            && git(&d, &["rev-list", "--count", "HEAD"]) == "1"
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(ready, "first full clonepack");

    // Advance upstream and re-sync.
    origin.commit(&[("a", "2\n"), ("c", "new\n")], "c2");
    origin.publish();
    server
        .client()
        .sync_repo("acme", "tp2", None, None)
        .await
        .unwrap();

    // depth=1 immediately reflects the new commit.
    let (_g1, c1) = clone_only(&server, "acme", "tp2", 1, CloneMode::Editable)
        .await
        .expect("depth=1 new commit");
    assert_eq!(read(&c1, "a"), "2\n");
    assert_eq!(read(&c1, "c"), "new\n");

    // depth=0 never fails during the gap, and upgrades to the new commit.
    let mut upgraded = false;
    for _ in 0..120 {
        let (_g, d) = clone_only(&server, "acme", "tp2", 0, CloneMode::Editable)
            .await
            .expect("depth=0 must not fail during the gap (option A)");
        assert!(git_ok(&d, &["fsck", "--connectivity-only", "HEAD"]));
        if read(&d, "a") == "2\n" && git(&d, &["rev-list", "--count", "HEAD"]) == "2" {
            upgraded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(upgraded, "depth=0 upgrades to the new full commit");
}
