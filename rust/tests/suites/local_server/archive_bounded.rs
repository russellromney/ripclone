//! A files-mode clone is byte-correct when the server builds the archive with the
//! bounded (read-only-the-changed-region) path. Uses multi-MB files so the archive
//! has several CDC frames and a re-sync exercises real prefix/suffix reuse + the
//! server wiring (prior files table + commit). The bounded build's byte-identity to
//! the full build is covered by the archive unit tests; this checks it end to end.

use crate::common::*;
use std::sync::Once;

fn setup() {
    static O: Once = Once::new();
    // SAFETY: set once, before any server/sync reads it.
    O.call_once(|| unsafe { std::env::set_var("RIPCLONE_ARCHIVE_BOUNDED", "1") });
    init(true);
}

/// ~3 MB of varied, CDC-cuttable printable text.
fn varied(seed: u64, n: usize) -> String {
    (0..n as u64)
        .map(|i| {
            let mut z = (seed << 40)
                .wrapping_add(i)
                .wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            char::from(32 + ((z ^ (z >> 31)) % 90) as u8)
        })
        .collect()
}

#[tokio::test]
async fn files_mode_correct_with_bounded_archive() {
    setup();
    let server = start_server().await;
    let origin = make_origin("acme", "arch");
    let f = |s: u64| varied(s, 3_000_000);
    let (a, b, c, d, e) = (f(1), f(2), f(3), f(4), f(5));

    origin.commit(
        &[
            ("a.bin", &a),
            ("b.bin", &b),
            ("c.bin", &c),
            ("d.bin", &d),
            ("e.bin", &e),
            ("marker", "1"),
        ],
        "c1",
    );
    origin.publish();
    server
        .client()
        .add_repo("acme/arch")
        .await
        .expect("add arch");
    server
        .client()
        .sync_repo("acme/arch", None)
        .await
        .expect("sync c1");

    {
        let (_g, dir) = clone_files_when(&server, "acme", "arch", "marker", "1").await;
        assert_eq!(read(&dir, "a.bin"), a);
        assert_eq!(read(&dir, "c.bin"), c);
        assert_eq!(read(&dir, "e.bin"), e);
    }

    // Change a middle file; re-sync goes through the bounded archive (prefix a/b
    // reused, suffix d/e reused, only c re-read).
    let c2 = f(99);
    origin.commit(&[("c.bin", &c2), ("marker", "2")], "c2");
    origin.publish();
    server
        .client()
        .sync_repo("acme/arch", None)
        .await
        .expect("sync c2");

    {
        let (_g, dir) = clone_files_when(&server, "acme", "arch", "marker", "2").await;
        assert_eq!(read(&dir, "a.bin"), a, "unchanged prefix file");
        assert_eq!(read(&dir, "c.bin"), c2, "changed file");
        assert_eq!(read(&dir, "e.bin"), e, "unchanged suffix file");
    }

    // Delete a file and grow another; re-sync shifts the suffix by a delta.
    std::fs::remove_file(origin.work.join("d.bin")).unwrap();
    let b2 = format!("{b}{}", f(7));
    origin.commit(&[("b.bin", &b2), ("marker", "3")], "c3");
    origin.publish();
    server
        .client()
        .sync_repo("acme/arch", None)
        .await
        .expect("sync c3");

    {
        let (_g, dir) = clone_files_when(&server, "acme", "arch", "marker", "3").await;
        assert_eq!(read(&dir, "a.bin"), a);
        assert_eq!(read(&dir, "b.bin"), b2, "grown file");
        assert_eq!(read(&dir, "c.bin"), c2);
        assert_eq!(read(&dir, "e.bin"), e);
        assert!(!dir.join("d.bin").exists(), "deleted file gone");
    }
}
