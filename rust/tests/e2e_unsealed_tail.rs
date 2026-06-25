//! Regression test for the unsealed-tail completeness gap (found in adversarial
//! review). With a seal threshold so high no tail ever seals into a level, the
//! flattened sealed levels are empty — yet the full clone must still ship the
//! unsealed `(sealed_tip, HEAD]` objects. Before the fix, dropping the full
//! skeleton left those commits/trees unshipped and the full clone was
//! incomplete; `seal_and_compact` now appends the unsealed tail to the manifest.

mod common;

use common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn unsealed_tail_full_clone_is_complete() {
    init(true); // LSM on
    // Override the seal threshold to effectively infinite so nothing ever seals.
    // SAFETY: set before any sync reads it; this binary has one test.
    unsafe { std::env::set_var("RIPCLONE_LSM_SEAL_BYTES", "1000000000") };

    let server = start_server().await;
    let origin = make_origin("acme", "unsealed");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.publish();
    let client = server.client();
    client.sync_repo("acme/unsealed", None).await.unwrap();

    // Several syncs; none seal (tail stays under threshold), so the full clone is
    // served entirely from the unsealed tail.
    for i in 2..=5u32 {
        let f = format!("f{i}.txt");
        let c = format!("{i}\n");
        let m = format!("c{i}");
        origin.commit(&[(f.as_str(), c.as_str())], &m);
        origin.publish();
        client.sync_repo("acme/unsealed", None).await.unwrap();

        let (_g, d) = clone_only(&server, "acme", "unsealed", 0, CloneMode::Editable)
            .await
            .expect("full clone");
        let want = i.to_string();
        assert_eq!(
            git(&d, &["rev-list", "--count", "HEAD"]),
            want,
            "all {want} commits present (unsealed tail must ship)"
        );
        assert!(
            git_ok(&d, &["rev-list", "--objects", "HEAD"]),
            "full object traversal complete after sync {i}"
        );
        assert!(
            git_ok(&d, &["fsck", "--connectivity-only", "HEAD"]),
            "fsck clean after sync {i}"
        );
        assert!(d.join("a.txt").exists(), "a.txt present after sync {i}");
        for j in 2..=i {
            assert!(d.join(format!("f{j}.txt")).exists(), "f{j} after sync {i}");
        }
    }
}
