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
    client.sync_repo("acme", "skel", None, None).await.unwrap();
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    client
        .skeleton_clone("acme", "skel", "HEAD", &target)
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
    let resp = client
        .sync_repo("acme", "corrupt", None, None)
        .await
        .unwrap();

    // Corrupt the clonepack manifest artifact in the server's CAS.
    let manifest_hash = resp.clonepack_manifest.clone();
    assert!(!manifest_hash.is_empty());
    let p = server.cas_path(&manifest_hash);
    assert!(p.exists(), "manifest should be in local CAS");
    std::fs::write(&p, b"garbage not a manifest").unwrap();

    let out = tempfile::tempdir().unwrap();
    let res = client
        .install_repo_with_mode(
            "acme",
            "corrupt",
            "HEAD",
            out.path().join("clone"),
            CloneMode::Editable,
            Some("shallow"),
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
    let resp = client
        .sync_repo("acme", "missing", None, None)
        .await
        .unwrap();
    let p = server.cas_path(&resp.clonepack_manifest);
    std::fs::remove_file(&p).unwrap();

    let out = tempfile::tempdir().unwrap();
    let res = client
        .install_repo_with_mode(
            "acme",
            "missing",
            "HEAD",
            out.path().join("clone"),
            CloneMode::Editable,
            Some("shallow"),
            None,
        )
        .await;
    assert!(res.is_err(), "missing manifest must fail the clone, got Ok");
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
