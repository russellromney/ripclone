//! End-to-end tests for the HEAD delta-pack build (two-phase publish).
//!
//! A re-sync packs only the depth-1 objects new since the prior commit into a
//! small delta pack, keeping the prior sync's HEAD packs as an immutable base.
//! These tests pin the correctness-critical edges that distinguish the chosen
//! design (set difference of the two depth-1 closures) from a naive
//! reachability-exclude, plus the background compaction that bounds the chain.
//!
//! Broad end-to-end coverage of the delta path (cold build, linear re-syncs,
//! multi-commit growth, files mode) lives in the e2e_matrix_twophase_lsm /
//! e2e_matrix_async_twophase_lsm batteries; this file adds the adversarial cases.

mod common;

use common::*;
use ripclone::mode::CloneMode;
use std::sync::Once;
use std::time::Duration;

/// Two-phase + LSM, with a 1-byte HEAD rebase threshold so the background
/// base-rebuild path runs on every non-empty delta — exercising rebase
/// continuously rather than only after a large cumulative delta.
fn setup() {
    enable_two_phase();
    static O: Once = Once::new();
    // SAFETY: set once via Once, before any server/sync reads it. Correctness
    // holds for any threshold; a 1-byte one just rebases every sync.
    O.call_once(|| unsafe { std::env::set_var("RIPCLONE_HEAD_REBASE_BYTES", "1") });
    init(true);
}

/// Sync, then wait until depth=0 reaches `want_count` so phase 2 (full history,
/// archive, and any HEAD compaction) has fully landed before the next sync.
async fn sync_and_settle(server: &Server, origin: &Origin, want_count: &str) {
    server
        .client()
        .sync_repo(&origin.owner, &origin.repo, None, None)
        .await
        .expect("sync");
    let _ = clone_full_at(server, &origin.owner, &origin.repo, want_count, true).await;
}

/// The re-add trap: a blob present at c1, deleted at c2, then re-added *identical*
/// at c3. At c3 the blob is reachable from the prior commit's *history* (via c1)
/// but absent from the prior commit's *tip tree* — so a `rev-list HEAD ^prev`
/// delta would wrongly exclude it, corrupting the depth=1 worktree. The set
/// difference of the two depth-1 closures keeps it. depth=1 at c3 must contain it.
#[tokio::test]
async fn re_add_identical_blob_survives_in_depth1() {
    setup();
    let server = start_server().await;
    let origin = make_origin("acme", "readd");

    origin.commit(&[("a.txt", "1\n"), ("secret.txt", "TOPSECRET\n")], "c1");
    origin.publish();
    sync_and_settle(&server, &origin, "1").await;

    // c2 deletes secret.txt (stage the removal via add -A).
    std::fs::remove_file(origin.work.join("secret.txt")).unwrap();
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();
    sync_and_settle(&server, &origin, "2").await;

    // c3 re-adds secret.txt with byte-identical content (same blob oid as c1).
    origin.commit(&[("a.txt", "3\n"), ("secret.txt", "TOPSECRET\n")], "c3");
    origin.publish();
    server
        .client()
        .sync_repo("acme", "readd", None, None)
        .await
        .expect("sync c3");

    // depth=1 (served straight from the HEAD base + deltas) must materialize the
    // re-added blob — the whole point of the set-difference delta.
    let (_g, d) = clone_only(&server, "acme", "readd", 1, CloneMode::Editable)
        .await
        .expect("depth=1 at c3");
    assert_eq!(read(&d, "a.txt"), "3\n");
    assert_eq!(
        read(&d, "secret.txt"),
        "TOPSECRET\n",
        "re-added identical blob must be present in the depth=1 worktree"
    );
    assert_eq!(git(&d, &["status", "--porcelain"]), "", "status clean");

    // Full clone after phase 2 is complete + fsck-clean.
    let (_g0, d0) = clone_full_at(&server, "acme", "readd", "3", true).await;
    assert_eq!(read(&d0, "secret.txt"), "TOPSECRET\n");
    assert_repo_usable(&d0, "3");
}

/// Force-push to a non-ancestor: after c1,c2 are synced, reset to c1 and commit a
/// fresh tip c3' whose parent is c1 (so the prior synced commit c2 is NOT an
/// ancestor) and which restores a blob that c2 had deleted. The delta vs c2 must
/// still cover c3's whole tip closure. depth=1 and the full clone must be correct.
#[tokio::test]
async fn force_push_to_non_ancestor_stays_correct() {
    setup();
    let server = start_server().await;
    let origin = make_origin("acme", "fpush");

    let c1 = origin.commit(&[("a.txt", "1\n"), ("keep.txt", "KEEP\n")], "c1");
    origin.publish();
    sync_and_settle(&server, &origin, "1").await;

    // c2 deletes keep.txt and changes a.txt.
    std::fs::remove_file(origin.work.join("keep.txt")).unwrap();
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();
    sync_and_settle(&server, &origin, "2").await;

    // Rewind to c1 (restores keep.txt) and commit a divergent tip c3' (parent c1).
    git(&origin.work, &["reset", "--hard", &c1]);
    origin.commit(&[("a.txt", "3\n")], "c3prime");
    origin.publish(); // force push (publish pushes with --force)
    server
        .client()
        .sync_repo("acme", "fpush", None, None)
        .await
        .expect("sync c3prime");

    // depth=1 reflects the divergent tip: a.txt=3 and keep.txt restored. keep.txt
    // is reachable from c2 only through c1's history, not c2's tip tree — a
    // reachability-exclude delta would drop it; the closure difference keeps it.
    let (_g, d) = clone_only(&server, "acme", "fpush", 1, CloneMode::Editable)
        .await
        .expect("depth=1 at c3prime");
    assert_eq!(read(&d, "a.txt"), "3\n");
    assert_eq!(
        read(&d, "keep.txt"),
        "KEEP\n",
        "blob restored by force-push must be present in depth=1"
    );
    assert_eq!(git(&d, &["status", "--porcelain"]), "", "status clean");

    // depth=0 never fails during the Option-A gap and upgrades to the divergent
    // tip. The commit count stays 2 across the force-push (c1→c2 vs c1→c3'), so we
    // poll on the NEW content (a.txt=3) rather than the count. Every successful
    // clone in the window must be a complete, fsck-clean repo — never a broken one.
    let mut upgraded = false;
    for _ in 0..160 {
        let (_g, d) = clone_only(&server, "acme", "fpush", 0, CloneMode::Editable)
            .await
            .expect("depth=0 must not fail during the gap (option A)");
        assert!(
            git_ok(&d, &["fsck", "--connectivity-only", "HEAD"]),
            "every depth=0 clone in the window is complete"
        );
        if read(&d, "a.txt") == "3\n" {
            assert_eq!(
                read(&d, "keep.txt"),
                "KEEP\n",
                "force-push restored keep.txt"
            );
            assert_eq!(git(&d, &["rev-list", "--count", "HEAD"]), "2");
            upgraded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        upgraded,
        "depth=0 upgrades to the divergent force-pushed tip"
    );
}

/// Many single-commit re-syncs with a 1-byte rebase threshold: the background
/// phase rebuilds a fresh HEAD base on every sync. Every increment's depth=1 and
/// full clone must stay complete across the repeated rebases.
#[tokio::test]
async fn head_compaction_keeps_clones_complete() {
    setup(); // RIPCLONE_HEAD_REBASE_BYTES=1 → rebase every sync
    let server = start_server().await;
    let origin = make_origin("acme", "compact");

    origin.commit(&[("base.txt", "0\n")], "c1");
    origin.publish();
    sync_and_settle(&server, &origin, "1").await;

    // Drive enough increments to cross the compaction threshold several times.
    for i in 2..=7u32 {
        let f = format!("f{i}.txt");
        let body = format!("{i}\n");
        origin.commit(
            &[(f.as_str(), body.as_str()), ("base.txt", &body)],
            &format!("c{i}"),
        );
        origin.publish();

        // depth=1 immediately reflects the new commit, served from base + deltas
        // (or a freshly compacted base).
        server
            .client()
            .sync_repo("acme", "compact", None, None)
            .await
            .expect("sync");
        let (_g, d) = clone_only(&server, "acme", "compact", 1, CloneMode::Editable)
            .await
            .expect("depth=1 during compaction churn");
        assert_eq!(read(&d, "base.txt"), body, "depth=1 base.txt at c{i}");
        assert_eq!(read(&d, &f), body, "depth=1 {f} present");
        assert_eq!(
            git(&d, &["status", "--porcelain"]),
            "",
            "status clean at c{i}"
        );

        // Wait for phase 2 (incl. any compaction) to land before the next sync.
        let (_g0, d0) = clone_full_at(&server, "acme", "compact", &i.to_string(), true).await;
        for j in 2..=i {
            assert!(
                d0.join(format!("f{j}.txt")).exists(),
                "full clone has f{j}.txt at c{i}"
            );
        }
        assert!(git_ok(&d0, &["fsck", "--connectivity-only", "HEAD"]));
    }

    // Final full clone is a complete, usable repo with every file.
    let (_g, d) = clone_full_at(&server, "acme", "compact", "7", true).await;
    assert_repo_usable(&d, "7");
    tokio::time::sleep(Duration::from_millis(10)).await; // let any stray task settle
}
