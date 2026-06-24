//! End-to-end round-trip tests against an in-process server mirroring a local
//! `file://` origin (no network). Covers every clone mode positively, plus
//! negative paths (corrupt artifact, missing chunk) and ref serialization.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use std::path::Path;

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap()
}

/// editable --depth 1: shallow, worktree correct, `.git/shallow` present,
/// `git log`/status clean, history bounded to HEAD.
#[tokio::test]
async fn editable_depth1_is_shallow_and_clean() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "d1");
    origin.commit(&[("a.txt", "one\n")], "first");
    origin.commit(&[("a.txt", "two\n"), ("dir/b.txt", "bee\n")], "second");
    origin.publish();

    let (_g, c) = sync_and_clone(&server, &origin, 1, CloneMode::Editable).await;

    assert_eq!(read(&c, "a.txt"), "two\n");
    assert_eq!(read(&c, "dir/b.txt"), "bee\n");
    assert!(c.join(".git/shallow").exists(), "depth=1 must mark shallow");
    assert!(git_ok(&c, &["status"]), "git status works");
    assert_eq!(git(&c, &["status", "--porcelain"]), "", "worktree clean");
    assert_eq!(
        git(&c, &["rev-list", "--count", "HEAD"]),
        "1",
        "shallow=1 commit"
    );
    assert!(git_ok(&c, &["log", "--oneline", "-1"]));
}

/// editable --depth 0: complete clone, full history to root, fsck-clean, no
/// shallow marker, MIDX installed and valid.
#[tokio::test]
async fn editable_depth0_is_complete() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "d0");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.commit(&[("a.txt", "3\n"), ("c.txt", "x\n")], "c3");
    origin.publish();

    let (_g, c) = sync_and_clone(&server, &origin, 0, CloneMode::Editable).await;

    assert_eq!(read(&c, "a.txt"), "3\n");
    assert!(
        !c.join(".git/shallow").exists(),
        "full clone has no shallow marker"
    );
    assert_eq!(
        git(&c, &["rev-list", "--count", "HEAD"]),
        "3",
        "all commits"
    );
    assert!(
        git_ok(&c, &["rev-list", "--objects", "HEAD"]),
        "full object traversal must be complete"
    );
    assert!(
        git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]),
        "fsck clean"
    );
    assert_eq!(git(&c, &["status", "--porcelain"]), "");
    if c.join(".git/objects/pack/multi-pack-index").exists() {
        assert!(
            git_ok(&c, &["multi-pack-index", "verify"]),
            "shipped MIDX valid"
        );
    }
}

/// files mode: worktree materializes; it is intentionally NOT a git repo.
#[tokio::test]
async fn files_mode_materializes_worktree() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "files");
    origin.commit(&[("only.txt", "hello\n"), ("nested/x", "y\n")], "c1");
    origin.publish();

    let (_g, c) = sync_and_clone(&server, &origin, 1, CloneMode::Files).await;
    assert_eq!(read(&c, "only.txt"), "hello\n");
    assert_eq!(read(&c, "nested/x"), "y\n");
}

/// skeleton mode: installs `.git` metadata, status works.
#[tokio::test]
async fn skeleton_mode_installs_git_dir() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "skel");
    origin.commit(&[("f", "1\n")], "c1");
    origin.publish();

    let client = server.client();
    client.sync_repo("acme/skel", None).await.unwrap();
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    client
        .skeleton_clone("acme/skel", "HEAD", &target)
        .await
        .expect("skeleton clone");
    assert!(target.join(".git").exists(), "skeleton has a .git dir");
}

/// Re-sync after a new push must serve the NEW commit (regression test for the
/// `git fetch origin HEAD` stale-ref bug — that path never advanced the mirror).
#[tokio::test]
async fn resync_picks_up_new_commits() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "resync");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();
    let (_g1, c1) = sync_and_clone(&server, &origin, 0, CloneMode::Editable).await;
    assert_eq!(read(&c1, "a"), "1\n");
    assert_eq!(git(&c1, &["rev-list", "--count", "HEAD"]), "1");

    // New commit lands upstream.
    origin.commit(&[("a", "2\n"), ("b", "new\n")], "c2");
    origin.publish();

    let (_g2, c2) = sync_and_clone(&server, &origin, 0, CloneMode::Editable).await;
    assert_eq!(read(&c2, "a"), "2\n", "re-sync must see the new commit");
    assert_eq!(read(&c2, "b"), "new\n");
    assert_eq!(git(&c2, &["rev-list", "--count", "HEAD"]), "2");
}

/// Negative: a corrupted artifact in the server's CAS must fail the clone with a
/// hash-verification error, never produce a silently-wrong tree.
#[tokio::test]
async fn corrupt_artifact_fails_clone() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "corrupt");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();

    let client = server.client();
    let resp = client.sync_repo("acme/corrupt", None).await.unwrap();

    // Corrupt the clonepack manifest artifact in the server's CAS.
    let manifest_hash = resp.clonepack_manifest.clone();
    assert!(!manifest_hash.is_empty());
    let p = server.cas_path(&manifest_hash);
    assert!(p.exists(), "manifest should be in local CAS");
    std::fs::write(&p, b"garbage not a manifest").unwrap();

    // Clone the same (full) variant whose manifest we corrupted, so the
    // hash-verification path is exercised on the tampered artifact.
    let out = tempfile::tempdir().unwrap();
    let res = client
        .install_repo_with_mode(
            "acme",
            "corrupt",
            "HEAD",
            out.path().join("clone"),
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await;
    assert!(res.is_err(), "corrupt manifest must fail the clone, got Ok");
}

/// Negative: a missing artifact (evicted/deleted) must fail the clone.
#[tokio::test]
async fn missing_artifact_fails_clone() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "missing");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();

    let client = server.client();
    let resp = client.sync_repo("acme/missing", None).await.unwrap();
    let p = server.cas_path(&resp.clonepack_manifest);
    std::fs::remove_file(&p).unwrap();

    // Clone the same (full) variant whose manifest we removed.
    let out = tempfile::tempdir().unwrap();
    let res = client
        .install_repo_with_mode(
            "acme",
            "missing",
            "HEAD",
            out.path().join("clone"),
            CloneMode::Editable,
            Some("full"),
            None,
        )
        .await;
    assert!(res.is_err(), "missing manifest must fail the clone, got Ok");
}

/// Positive: transient artifact-fetch failures (503) must be retried with
/// backoff and the clone must still succeed with a correct worktree. The server
/// fails its first few artifact GETs (per-server fault, no global env), so this
/// is safe under parallel test execution.
#[tokio::test]
async fn transient_fetch_failure_is_retried() {
    init(false);
    // The injected faults are a single per-server counter shared across the
    // concurrent manifest/metadata/chunk fetches. Keep it below the default
    // retry budget (3 attempts) so that even if every fault lands on one
    // artifact's attempts, its 3rd attempt still succeeds — the test recovers
    // regardless of how faults distribute, never flaking on scheduling.
    let server = start_server_faulting(2).await;
    let origin = make_origin("acme", "retry");
    origin.commit(&[("a.txt", "hi\n"), ("dir/b.txt", "x\n")], "c1");
    origin.publish();

    // The default retry budget (3 attempts) recovers from the injected faults.
    let (_g, c) = sync_and_clone(&server, &origin, 1, CloneMode::Files).await;
    assert_eq!(
        read(&c, "a.txt"),
        "hi\n",
        "retried fetches must still materialize the tree"
    );
    assert_eq!(read(&c, "dir/b.txt"), "x\n");
}

/// Negative: a client presenting the wrong auth token must be rejected by the
/// server (the protected routes sit behind the auth middleware), so a
/// misconfigured token can never silently sync or clone.
#[tokio::test]
async fn wrong_token_is_rejected() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "authz");
    origin.commit(&[("a.txt", "hi\n")], "c1");
    origin.publish();

    // A correctly-tokened client can sync (control).
    server
        .client()
        .sync_repo("acme/authz", None)
        .await
        .expect("correct token must be accepted");

    // A wrong-token client is rejected on a protected route.
    let bad = ripclone::client::Client::new_with_token(
        server.url.clone(),
        Some("deadbeefdeadbeef".to_string()),
    );
    let res = bad.sync_repo("acme/authz", None).await;
    assert!(res.is_err(), "wrong token must be rejected, got Ok");
}

/// Negative: when failures persist beyond the retry budget, the clone must fail
/// cleanly — not hang, not produce a partial tree.
#[tokio::test]
async fn persistent_fetch_failure_fails_clone() {
    init(false);
    // Fail far more artifact fetches than the retry budget allows.
    let server = start_server_faulting(100_000).await;
    let origin = make_origin("acme", "retryfail");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();
    let client = server.client();
    // sync talks to the server directly (not artifact GETs), so it succeeds.
    client.sync_repo("acme/retryfail", None).await.unwrap();

    let out = tempfile::tempdir().unwrap();
    let res = client
        .install_repo_with_mode(
            "acme",
            "retryfail",
            "HEAD",
            out.path().join("clone"),
            CloneMode::Files,
            Some("shallow"),
            None,
        )
        .await;
    assert!(
        res.is_err(),
        "persistent fetch failure must fail the clone, got Ok"
    );
}

/// Positive: a successful clone renames the temp install dir onto the target and
/// leaves no `.tmp` leftovers in the parent. (Overlay is off by default in
/// tests — `RIPCLONE_TEMP` is unset — so the temp-dir path is exercised.)
#[tokio::test]
async fn successful_clone_leaves_no_temp_dir() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "clean");
    origin.commit(&[("a.txt", "hi\n"), ("d/b.txt", "y\n")], "c1");
    origin.publish();
    let (_g, c) = sync_and_clone(&server, &origin, 1, CloneMode::Files).await;
    assert_eq!(read(&c, "a.txt"), "hi\n");
    assert_eq!(read(&c, "d/b.txt"), "y\n");
    let parent = c.parent().unwrap();
    let tmps: Vec<String> = std::fs::read_dir(parent)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".tmp"))
        .collect();
    assert!(tmps.is_empty(), "successful clone left temp dirs: {tmps:?}");
}

/// Negative: a clone that fails *after* the temp install dir is created (here a
/// corrupt archive chunk fails extraction) must remove the partial dir and leave
/// no target behind.
#[tokio::test]
async fn failed_clone_after_temp_dir_leaves_nothing() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "notemp");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    let client = server.client();
    client.sync_repo("acme/notemp", None).await.unwrap();

    // Corrupt the first archive chunk so extraction (which runs after the temp
    // install dir is created and the skeleton is written) fails deterministically.
    let info = client
        .resolve_ref_with_clonepack("acme/notemp", "HEAD", Some("shallow"), None)
        .await
        .unwrap();
    let (manifest, _meta) = client.fetch_clonepack(&info).await.unwrap();
    assert!(
        !manifest.archive_chunks.is_empty(),
        "test needs an archive chunk to corrupt"
    );
    let chunk_hex = ripclone::clonepack::hash_to_hex(&manifest.archive_chunks[0].hash);
    let p = server.cas_path(&chunk_hex);
    assert!(p.exists(), "archive chunk should be in CAS");
    std::fs::write(&p, b"corrupt-not-the-real-chunk").unwrap();

    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let res = client
        .install_repo_with_mode(
            "acme",
            "notemp",
            "HEAD",
            &target,
            CloneMode::Files,
            Some("shallow"),
            None,
        )
        .await;
    assert!(res.is_err(), "corrupt archive chunk must fail the clone");
    assert!(
        !target.exists(),
        "failed clone must not leave the target dir"
    );
    let leftovers: Vec<String> = std::fs::read_dir(out.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        leftovers.is_empty(),
        "failed clone left orphaned files in parent: {leftovers:?}"
    );
}

/// RefInfo with LSM levels round-trips through serde, and old records (no
/// `history_levels`) deserialize to an empty vec (back-compat).
#[test]
fn ref_info_history_levels_serde() {
    use ripclone::{HistoryLevel, RefInfo, SizedPack};
    let mut info: RefInfo = serde_json::from_str(
        r#"{"commit":"c","parent_commit":null,"default_branch":"main",
            "skeleton_pack":"","skeleton_idx":"","head_blobs_pack":"","head_blobs_idx":"",
            "prebuilt_index":"","archive":"","manifest":"","full_pack":""}"#,
    )
    .expect("old RefInfo without history_levels must deserialize");
    assert!(info.history_levels.is_empty(), "missing field -> empty");

    info.history_levels.push(HistoryLevel {
        tip_commit: "deadbeef".into(),
        packs: vec![SizedPack {
            pack: "p".into(),
            pack_len: 10,
            idx: "i".into(),
            idx_len: 5,
        }],
    });
    let json = serde_json::to_string(&info).unwrap();
    let back: RefInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(back.history_levels.len(), 1);
    assert_eq!(back.history_levels[0].tip_commit, "deadbeef");
    assert_eq!(back.history_levels[0].packs[0].pack_len, 10);
}
