//! End-to-end tests for two-phase publish (always on): a sync publishes the
//! depth=1 clonepack in the foreground and builds full history in the
//! background, so depth=1 is clonable immediately and depth=0 shortly after.

use crate::common::*;
use ripclone::mode::CloneMode;
use std::path::Path;
use std::time::Duration;

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap()
}

async fn repo_status(server: &Server, owner: &str, repo: &str) -> serde_json::Value {
    let url = format!("{}/v1/repos/github/{owner}/{repo}/status", server.url);
    reqwest::Client::new()
        .get(url)
        .header("Authorization", format!("Ripclone {}", token_hash()))
        .send()
        .await
        .expect("status request")
        .error_for_status()
        .expect("status 2xx")
        .json()
        .await
        .expect("status json")
}

/// After a single two-phase sync: depth=1 is immediately clonable + correct, and
/// depth=0 becomes a complete, fsck-clean full clone once phase 2 finishes.
#[tokio::test]
async fn two_phase_depth1_immediate_then_full() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tp");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("b.txt", "x\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();

    // Sync returns after phase 1 (depth=1 published; full builds in background).
    register_added_without_build(&server, "acme/tp")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/tp", None)
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
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tpf");
    origin.commit(&[("a.txt", "hello\n"), ("nested/b.txt", "world\n")], "c1");
    origin.commit(&[("a.txt", "hello2\n")], "c2");
    origin.publish();
    register_added_without_build(&server, "acme/tpf")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/tpf", None)
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
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "tp2");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();
    register_added_without_build(&server, "acme/tp2")
        .await
        .expect("add repo");
    server.client().sync_repo("acme/tp2", None).await.unwrap();

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
    server.client().sync_repo("acme/tp2", None).await.unwrap();

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

#[tokio::test]
async fn delayed_older_editable_publish_does_not_clear_newer_archive() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "phase2guard");
    let old = origin.commit(&[("f", "old\n")], "old");
    origin.publish();

    // SAFETY: this test hook targets only this exact commit.
    unsafe {
        std::env::set_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_COMMIT", &old);
        std::env::set_var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_MS", "3000");
    }

    register_added_without_build(&server, "acme/phase2guard")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/phase2guard", None)
        .await
        .expect("sync old");

    let new = origin.commit(&[("f", "new\n"), ("g", "new file\n")], "new");
    origin.publish();
    server
        .client()
        .sync_repo("acme/phase2guard", None)
        .await
        .expect("sync new");

    let (_ready_guard, ready) =
        clone_files_when(&server, "acme", "phase2guard", "f", "new\n").await;
    assert_eq!(read(&ready, "g"), "new file\n");

    tokio::time::sleep(Duration::from_millis(3600)).await;

    let info = server
        .client()
        .resolve_ref_with_clonepack("acme/phase2guard", "HEAD", Some("full"), None)
        .await
        .expect("resolve full ref");
    assert_eq!(info.commit, new);
    assert!(
        info.archive_ready,
        "older phase-2 publish must not clear the newer archive"
    );

    let (_guard, after) = clone_files_when(&server, "acme", "phase2guard", "f", "new\n").await;
    assert_eq!(read(&after, "g"), "new file\n");
}

#[tokio::test]
async fn failed_phase2_status_recovers_on_resync() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "phase2fail");
    let commit = origin.commit(&[("f", "v1\n")], "c1");
    origin.publish();

    unsafe {
        std::env::set_var("RIPCLONE_TEST_PHASE2_FAIL_COMMIT", &commit);
    }

    register_added_without_build(&server, "acme/phase2fail")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/phase2fail", None)
        .await
        .expect("sync with forced phase-2 failure");

    let mut failed_status = None;
    for _ in 0..120 {
        let status = repo_status(&server, "acme", "phase2fail").await;
        failed_status = status["refs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["branch"] == "main")
            .and_then(|entry| entry["build_status"].as_str())
            .map(str::to_string);
        if failed_status
            .as_deref()
            .is_some_and(|s| s.starts_with("failed: "))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let failed_status = failed_status.expect("phase-2 failure status visible");
    assert!(
        failed_status.starts_with("failed: "),
        "phase-2 status should fail, got {failed_status}"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_TEST_PHASE2_FAIL_COMMIT");
    }

    server
        .client()
        .sync_repo("acme/phase2fail", None)
        .await
        .expect("resync after clearing phase-2 failure");

    let mut recovered = false;
    for _ in 0..120 {
        if let Ok((_g, d)) = clone_only(&server, "acme", "phase2fail", 0, CloneMode::Editable).await
            && git(&d, &["rev-parse", "HEAD"]) == commit
            && git_ok(&d, &["fsck", "--connectivity-only", "HEAD"])
        {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(recovered, "subsequent sync should recover the full clone");
}

/// A *panic* in the detached phase-2 task (not a returned error) must not
/// silently strand the ref at "full history building" forever — the giant-repo
/// stall. The panic must be caught, surfaced, and the build marked `failed:` so
/// a following sync rebuilds and recovers the full clone.
#[tokio::test]
async fn panicking_phase2_status_recovers_on_resync() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "phase2panic");
    let commit = origin.commit(&[("f", "v1\n")], "c1");
    origin.publish();

    unsafe {
        std::env::set_var("RIPCLONE_TEST_PHASE2_PANIC_COMMIT", &commit);
    }

    register_added_without_build(&server, "acme/phase2panic")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/phase2panic", None)
        .await
        .expect("sync with forced phase-2 panic");

    // The detached phase-2 task panics; the outer guard must catch it and mark
    // the build failed instead of leaving it stuck at "full history building".
    let mut failed_status = None;
    for _ in 0..120 {
        let status = repo_status(&server, "acme", "phase2panic").await;
        failed_status = status["refs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["branch"] == "main")
            .and_then(|entry| entry["build_status"].as_str())
            .map(str::to_string);
        if failed_status
            .as_deref()
            .is_some_and(|s| s.starts_with("failed: "))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let failed_status = failed_status.expect("phase-2 panic status visible");
    assert!(
        failed_status.starts_with("failed: "),
        "a panicking phase-2 should be marked failed, got {failed_status}"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_TEST_PHASE2_PANIC_COMMIT");
    }

    server
        .client()
        .sync_repo("acme/phase2panic", None)
        .await
        .expect("resync after clearing phase-2 panic");

    let mut recovered = false;
    for _ in 0..120 {
        if let Ok((_g, d)) =
            clone_only(&server, "acme", "phase2panic", 0, CloneMode::Editable).await
            && git(&d, &["rev-parse", "HEAD"]) == commit
            && git_ok(&d, &["fsck", "--connectivity-only", "HEAD"])
        {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        recovered,
        "subsequent sync should recover the full clone after a phase-2 panic"
    );
}
