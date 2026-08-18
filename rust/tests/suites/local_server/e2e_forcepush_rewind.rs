//! Force-push rewind to a shallower, never-before-built commit.
//!
//! A branch is warmed at a deep tip, then upstream runs the moral equivalent of
//! `git reset --hard <older> && git commit && git push --force`, landing a NEW
//! tip whose history is *shallower* than the abandoned one. That commit was never
//! built as a tip, so the sync builds it fresh. The freshly built commit is the
//! confirmed upstream tip and must be published authoritatively — the served ref
//! has to follow upstream to the shallow tip, not stay stranded on the deeper,
//! abandoned commit. Ordering by history depth alone would keep serving the old
//! tree; this pins that the confirmed tip wins regardless of depth.

use crate::common::*;

/// Warm a branch at a deep tip, force-push-rewind to a shallower never-built
/// tip, sync, and assert the served ref is the shallow tip (byte-correct).
#[tokio::test]
async fn forcepush_rewind_to_shallower_tip_serves_new_tip() {
    setup(true); // two-phase + LSM + async (production defaults)
    let server = start_server().await;
    let origin = make_origin("acme", "fprewind");

    // Deep chain: c1..c5 (5 commits). Warm the branch at c5.
    origin.commit(&[("a.txt", "1\n")], "c1");
    let c2 = origin.commit(&[("a.txt", "2\n")], "c2");
    origin.commit(&[("a.txt", "3\n")], "c3");
    origin.commit(&[("a.txt", "4\n")], "c4");
    origin.commit(&[("a.txt", "5\n"), ("marker.txt", "DEEP\n")], "c5");
    origin.publish();

    register_added_without_build(&server, "acme/fprewind")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/fprewind", None)
        .await
        .expect("sync c5");
    // Let phase 2 land so the branch is fully warm at the deep tip (count 5).
    let _ = clone_full_at(&server, "acme", "fprewind", "5").await;

    // Force-push rewind: reset to c2 and land a fresh tip on top. The new tip has
    // depth 3 (c1, c2, c_new) — SHALLOWER than the abandoned c5 (depth 5) — and
    // was never built as a tip. marker.txt (from c5) is gone; tip.txt is new.
    git(&origin.work, &["reset", "--hard", &c2]);
    let c_new = origin.commit(&[("a.txt", "rewound\n"), ("tip.txt", "SHALLOW\n")], "c_new");
    origin.publish(); // publish force-pushes

    server
        .client()
        .sync_repo("acme/fprewind", None)
        .await
        .expect("sync rewound tip");

    // The published (depth=1) ref must follow upstream to the shallow tip. Before
    // the fix, `should_replace_ref` ordered by history depth first, so the fresh
    // depth-3 build (gen 3) lost to the stranded depth-5 ref (gen 5) and this
    // clone kept serving the abandoned c5 tree (a.txt=5, marker DEEP).
    let (_g1, d1) = clone_only(
        &server,
        "acme",
        "fprewind",
        1,
        ripclone::mode::CloneMode::Editable,
    )
    .await
    .expect("depth=1 clone after rewind sync");
    assert_eq!(
        git(&d1, &["rev-parse", "HEAD"]),
        c_new,
        "served ref must be the confirmed upstream tip, not the abandoned deep commit"
    );
    assert_eq!(
        read(&d1, "a.txt"),
        "rewound\n",
        "depth=1 serves the new tip"
    );
    assert_eq!(read(&d1, "tip.txt"), "SHALLOW\n", "new tip file present");
    assert!(
        !d1.join("marker.txt").exists(),
        "abandoned deep-tip file must be gone"
    );
    assert_eq!(
        git(&d1, &["status", "--porcelain"]),
        "",
        "depth=1 status clean"
    );

    // Full clone must also converge on the shallow tip, byte-correct (count 3).
    let (_g0, d0) = clone_full_at(&server, "acme", "fprewind", "3").await;
    assert_eq!(
        git(&d0, &["rev-parse", "HEAD"]),
        c_new,
        "full clone at new tip"
    );
    assert_eq!(read(&d0, "a.txt"), "rewound\n");
    assert_eq!(read(&d0, "tip.txt"), "SHALLOW\n");
    assert!(
        !d0.join("marker.txt").exists(),
        "no abandoned deep-tip file"
    );
    assert_repo_usable(&d0, "3");
}
