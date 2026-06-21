//! End-to-end test for LSM compaction. With seal-every-sync and a low
//! `RIPCLONE_LSM_MAX_LEVELS`, many syncs accumulate levels that are repeatedly
//! compacted (merged + re-packed). The full clone must stay complete and
//! fsck-clean throughout — i.e. compaction never drops or corrupts history.

mod common;

use common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn compaction_keeps_full_clone_correct() {
    // Bound the level count low so compaction fires after a few syncs.
    // SAFETY: set before the server/sync read it; this binary has one test.
    unsafe { std::env::set_var("RIPCLONE_LSM_MAX_LEVELS", "2") };
    init(true); // LSM on, seal every advancing non-empty tail (SEAL_BYTES=1)

    let server = start_server().await;
    let origin = make_origin("acme", "cmp");
    origin.commit(&[("f0", "0\n")], "c0");
    origin.publish();

    let client = server.client();
    client.sync_repo("acme", "cmp", None, None).await.unwrap();

    // Each sync adds a commit -> seals a new level -> eventually triggers
    // compaction back down to MAX_LEVELS. Re-clone full each time and verify it
    // stays complete (catches any range dropped/corrupted by a merge).
    for i in 1..=6u32 {
        let name = format!("f{i}");
        let content = format!("{i}\n");
        let msg = format!("c{i}");
        origin.commit(&[(name.as_str(), content.as_str())], &msg);
        origin.publish();
        client.sync_repo("acme", "cmp", None, None).await.unwrap();

        let (_g, c) = clone_only(&server, "acme", "cmp", 0, CloneMode::Editable)
            .await
            .expect("full clone after compaction");
        let want = (i + 1).to_string();
        assert_eq!(
            git(&c, &["rev-list", "--count", "HEAD"]),
            want,
            "all {want} commits present after sync {i}"
        );
        assert!(
            git_ok(&c, &["rev-list", "--objects", "HEAD"]),
            "full object traversal complete after sync {i}"
        );
        assert!(
            git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]),
            "fsck clean after sync {i}"
        );
        for j in 0..=i {
            assert!(
                c.join(format!("f{j}")).exists(),
                "f{j} present after sync {i}"
            );
        }
    }
}
