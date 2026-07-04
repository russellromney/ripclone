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

async fn wait_archive_ref(server: &Server, owner: &str, repo: &str) -> ripclone::RefInfo {
    let root = server.repo_root.join(".ripclone-refs");
    let mut last = String::from("<not read>");
    for _ in 0..160 {
        let mut files = Vec::new();
        collect_json_files(&root, &mut files);
        for path in files {
            match std::fs::read(&path) {
                Ok(data) => match serde_json::from_slice::<ripclone::RefInfo>(&data) {
                    Ok(info)
                        if !info.archive_chunks.is_empty() && !info.archive_frames.is_empty() =>
                    {
                        return info;
                    }
                    Ok(info) => {
                        last = format!(
                            "{}: archive_chunks={} archive_frames={} commit={}",
                            path.display(),
                            info.archive_chunks.len(),
                            info.archive_frames.len(),
                            info.commit
                        );
                    }
                    Err(e) => last = format!("{} parse: {e}", path.display()),
                },
                Err(e) => last = format!("{} read: {e}", path.display()),
            }
        }
        if last == "<not read>" {
            last = format!("no ref json under {}", root.display());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("archive ref never became ready for {owner}/{repo} (last: {last})");
}

fn collect_json_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_json_files(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("json") {
            out.push(path);
        }
    }
}

/// editable --depth 1: shallow, worktree correct, `.git/shallow` present,
/// `git log`/status clean, history bounded to HEAD.
#[ignore = "slow: polls for background phase-2 builds"]
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
#[ignore = "slow: polls for background phase-2 builds"]
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
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn files_mode_materializes_worktree() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "files");
    origin.commit(&[("only.txt", "hello\n"), ("nested/x", "y\n")], "c1");
    origin.publish();

    // The archive (and thus files mode) is carried by the full clonepack under
    // two-phase publish; the shallow snapshot has no archive.
    let (_g, c) = sync_and_clone(&server, &origin, 0, CloneMode::Files).await;
    assert_eq!(read(&c, "only.txt"), "hello\n");
    assert_eq!(read(&c, "nested/x"), "y\n");
    assert!(
        !c.join(".git").exists(),
        "files mode should materialize only files, not a git repository"
    );
}

#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn files_mode_resync_works_after_remote_storage_evicted_local_archive_artifacts() {
    init(false);
    let server = start_server_split_storage().await;
    let origin = make_origin("acme", "remotecontract");
    let big = vec![b'a'; 17 * 1024 * 1024];

    origin.commit_bytes(&[("big.bin", &big), ("tail.txt", b"one\n")], "c1");
    origin.publish();
    server
        .client()
        .sync_repo("acme/remotecontract", None)
        .await
        .expect("initial sync");
    let (_g1, c1) = clone_files_when(&server, "acme", "remotecontract", "tail.txt", "one\n").await;
    assert_eq!(
        std::fs::metadata(c1.join("big.bin")).unwrap().len(),
        big.len() as u64
    );
    assert!(
        !c1.join(".git").exists(),
        "files mode should not create .git before remote artifact eviction"
    );

    let info1 = wait_archive_ref(&server, "acme", "remotecontract").await;
    assert!(
        !info1.archive_chunks.is_empty(),
        "files archive bundles should be published"
    );
    assert!(
        !info1.archive_frames.is_empty(),
        "per-frame reuse metadata should be persisted"
    );
    for hash in info1
        .archive_chunks
        .iter()
        .chain(info1.archive_frames.iter().map(|f| &f.chunk_hash))
    {
        assert!(
            !server.cas_path(hash).exists(),
            "remote storage settlement should evict local CAS artifact {hash}"
        );
        assert!(
            server.storage_path(hash).exists(),
            "durable storage should retain artifact {hash}"
        );
    }

    origin.commit_bytes(&[("big.bin", &big), ("tail.txt", b"two\n")], "c2");
    origin.publish();
    server
        .client()
        .sync_repo("acme/remotecontract", None)
        .await
        .expect("resync");
    let (_g2, c2) = clone_files_when(&server, "acme", "remotecontract", "tail.txt", "two\n").await;
    assert_eq!(
        std::fs::metadata(c2.join("big.bin")).unwrap().len(),
        big.len() as u64
    );
    assert!(
        !c2.join(".git").exists(),
        "files mode should not create .git after rebuilding from durable storage"
    );

    let info2 = wait_archive_ref(&server, "acme", "remotecontract").await;
    assert_eq!(info2.commit, git(&origin.bare, &["rev-parse", "HEAD"]));
    assert!(
        info2.archive_frames.iter().any(|f| info1
            .archive_frames
            .iter()
            .any(|p| p.chunk_hash == f.chunk_hash)),
        "resync should reuse at least one prior archive frame from durable storage"
    );
}

/// Re-sync after a new push must serve the NEW commit (regression test for the
/// `git fetch origin HEAD` stale-ref bug — that path never advanced the mirror).
#[ignore = "slow: polls for background phase-2 builds"]
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
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn corrupt_artifact_fails_clone() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "corrupt");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();

    let client = server.client();
    // The full clonepack (and its manifest) publishes in phase 2, so wait for it.
    let resp = sync_until_manifest(&server, "acme", "corrupt").await;

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
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn missing_artifact_fails_clone() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "missing");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();

    let client = server.client();
    // The full clonepack (and its manifest) publishes in phase 2, so wait for it.
    let resp = sync_until_manifest(&server, "acme", "missing").await;
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
#[ignore = "slow: polls for background phase-2 builds"]
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

    // Wait for the full clonepack (with archive) to publish without consuming the
    // fault budget on not-yet-ready poll attempts, then do a single files clone:
    // the default retry budget (3 attempts) recovers from the injected faults.
    sync_until_manifest(&server, "acme", "retry").await;
    let (_g, c) = clone_only(&server, "acme", "retry", 0, CloneMode::Files)
        .await
        .expect("retried fetches must still materialize the tree");
    assert_eq!(read(&c, "a.txt"), "hi\n");
    assert_eq!(read(&c, "dir/b.txt"), "x\n");
}

/// Negative: a client presenting the wrong auth token must be rejected by the
/// server (the protected routes sit behind the auth middleware), so a
/// misconfigured token can never silently sync or clone.
#[ignore = "slow: polls for background phase-2 builds"]
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
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn persistent_fetch_failure_fails_clone() {
    init(false);
    // Fail far more artifact fetches than the retry budget allows.
    let server = start_server_faulting(100_000).await;
    let origin = make_origin("acme", "retryfail");
    origin.commit(&[("a", "1\n")], "c1");
    origin.publish();
    // sync talks to the server directly (not artifact GETs), so it succeeds; wait
    // for the full clonepack (with archive) to publish in phase 2.
    sync_until_manifest(&server, "acme", "retryfail").await;
    let client = server.client();

    let out = tempfile::tempdir().unwrap();
    let res = client
        .install_repo_with_mode(
            "acme",
            "retryfail",
            "HEAD",
            out.path().join("clone"),
            CloneMode::Files,
            Some("full"),
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
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn successful_clone_leaves_no_temp_dir() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "clean");
    origin.commit(&[("a.txt", "hi\n"), ("d/b.txt", "y\n")], "c1");
    origin.publish();
    let (_g, c) = sync_and_clone(&server, &origin, 0, CloneMode::Files).await;
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
#[ignore = "slow: polls for background phase-2 builds"]
#[tokio::test]
async fn failed_clone_after_temp_dir_leaves_nothing() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "notemp");
    origin.commit(&[("a.txt", "hello\n")], "c1");
    origin.publish();
    let client = server.client();
    // The archive builds in phase 2; wait until the clonepack manifest is
    // published, then resolve the shallow clonepack and poll until its archive
    // chunk is available to corrupt.
    sync_until_manifest(&server, "acme", "notemp").await;
    let mut manifest = None;
    for _ in 0..160 {
        let info = client
            .resolve_ref_with_clonepack("acme/notemp", "HEAD", Some("full"), None)
            .await
            .unwrap();
        let (man, _meta) = client.fetch_clonepack(&info).await.unwrap();
        if !man.archive_chunks.is_empty() {
            manifest = Some(man);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    let manifest = manifest.expect("archive chunk never published for acme/notemp");
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
            Some("full"),
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
#[ignore = "slow: polls for background phase-2 builds"]
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
