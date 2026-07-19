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
    setup(true); // two-phase + LSM + async (production defaults)
    let server = start_server().await;
    let origin = make_origin("acme", "atrev");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("b.txt", "B\n")], "c2");
    origin.commit(&[("a.txt", "3\n"), ("c.txt", "C\n")], "c3");
    origin.publish();

    let client = server.client();
    register_added_without_build(&server, "acme/atrev")
        .await
        .expect("add repo");

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
    let (_g3, c3dir) = clone_full_at(&server, "acme", "atrev", "3").await;
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
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "noclob");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();
    let client = server.client();
    register_added_without_build(&server, "acme/noclob")
        .await
        .expect("add repo");

    // Normal tip sync (builds the real branch entry at c3).
    client.sync_repo("acme/noclob", None).await.unwrap();
    let (_g0, tip0) = clone_full_at(&server, "acme", "noclob", "3").await;
    assert_eq!(read(&tip0, "a.txt"), "3\n");

    // Now sync at an OLDER rev. Under the buggy (clobbering) behavior this would
    // overwrite the branch entry with c1 and break the next tip clone.
    client
        .sync_repo_at("acme/noclob", Some("HEAD~2"), None)
        .await
        .unwrap();

    // A plain tip clone must STILL serve c3 correctly.
    let (_g1, tip1) = clone_full_at(&server, "acme", "noclob", "3").await;
    assert_eq!(
        read(&tip1, "a.txt"),
        "3\n",
        "tip clone unaffected by at-rev sync"
    );
    assert_repo_usable(&tip1, "3");
}

/// Regression: the documented pairing `ripclone sync <repo> --at REV` then
/// `ripclone clone <repo> --at REV` must work on the FIRST try.
///
/// A two-phase sync publishes the depth-1 clonepack immediately and the full
/// history in the background. The ref endpoint used to answer `202 building`
/// only for branch-tip requests, so a rev-targeted clone raced that background
/// phase and failed outright with "ref is missing clonepack manifest; run sync
/// first" — right after the user had run sync. The clone must poll like the tip
/// path does, so no retry loop here on purpose: a single call has to succeed.
#[tokio::test]
async fn clone_at_rev_waits_for_the_background_full_build() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "atwait");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.publish();

    // Register + build the tip first, so the rev build below is the only thing
    // still in flight when the clone lands.
    ensure_added(&server, "acme/atwait")
        .await
        .expect("add repo");

    let client = server.client();
    client
        .sync_repo_at("acme/atwait", Some("HEAD~2"), None)
        .await
        .expect("sync at HEAD~2");

    // Immediately clone the full (depth=0) artifacts for that rev. No retries.
    let (_g, dir) = clone_only_at(
        &server,
        "acme",
        "atwait",
        Some("HEAD~2"),
        0,
        CloneMode::Editable,
    )
    .await
    .expect("clone --at HEAD~2 straight after sync --at HEAD~2");
    assert_eq!(git(&dir, &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(read(&dir, "a.txt"), "1\n");
    assert_repo_usable(&dir, "1");
}

#[tokio::test]
async fn public_cli_clones_at_a_full_sha() {
    setup(true);
    let server = start_server().await;
    let origin = make_origin("acme", "at-full-sha");
    let pinned = origin.commit(&[("a.txt", "pinned\n")], "pinned");
    origin.commit(&[("a.txt", "tip\n")], "tip");
    origin.publish();
    ensure_added(&server, "acme/at-full-sha")
        .await
        .expect("add and build full-SHA fixture through the public workflow");
    server
        .client()
        .sync_repo_at("acme/at-full-sha", Some(&pinned), None)
        .await
        .expect("sync at full SHA");
    server
        .client()
        .resolve_ref_with_clonepack("acme/at-full-sha", "HEAD", Some("full"), Some(&pinned))
        .await
        .expect("full-SHA build ready");

    let out = tempfile::tempdir().expect("CLI output");
    let target = out.path().join("clone");
    let binary = cargo_bin("ripclone");
    if let Some(dir) = std::env::var_os("RIPCLONE_BIN_DIR") {
        assert_eq!(
            binary.canonicalize().expect("canonical selected CLI"),
            std::path::PathBuf::from(dir)
                .join("ripclone")
                .canonicalize()
                .expect("canonical requested CLI"),
            "full-SHA proof must spawn the requested release binary"
        );
    }
    let child = std::process::Command::new(binary)
        .arg("--server")
        .arg(&server.url)
        .arg("clone")
        .arg("acme/at-full-sha")
        .arg(&target)
        .arg("--at")
        .arg(&pinned)
        .arg("--depth")
        .arg("0")
        .arg("--verify-upstream=never")
        .arg("--no-metrics")
        .env("RIPCLONE_SERVER_TOKEN", TOKEN)
        .env("RIPCLONE_NO_METRICS", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn full-SHA CLI");
    let output = wait_child_output_bounded(child, Duration::from_secs(60))
        .await
        .expect("full-SHA CLI bounded, killed, and reaped on timeout");
    assert!(
        output.status.success(),
        "full-SHA clone failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), pinned);
    assert_eq!(read(&target, "a.txt"), "pinned\n");
    assert_repo_usable(&target, "1");
}
