//! An editable full clone is ready as soon as history is built; it does not wait
//! for the zstd archive (which only files mode needs). This test holds the archive
//! back with RIPCLONE_TEST_ARCHIVE_DELAY_MS so the gap is observable.

use crate::common::*;
use ripclone::mode::CloneMode;
use std::sync::Once;
use std::time::{Duration, Instant};

/// How long the server holds the archive back after publishing the editable clone.
const ARCHIVE_DELAY_MS: u64 = 3000;

fn setup() {
    static O: Once = Once::new();
    // SAFETY: set once, before any server/sync reads it.
    O.call_once(|| unsafe {
        std::env::set_var(
            "RIPCLONE_TEST_ARCHIVE_DELAY_MS",
            ARCHIVE_DELAY_MS.to_string(),
        );
    });
    init(true);
}

#[tokio::test]
async fn editable_full_ready_before_files() {
    setup();
    let server = start_server().await;
    let origin = make_origin("acme", "decouple");
    origin.commit(&[("a.txt", "1\n"), ("dir/b.txt", "B\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();
    register_added_without_build(&server, "acme/decouple")
        .await
        .expect("add repo");
    server
        .client()
        .sync_repo("acme/decouple", None)
        .await
        .expect("sync");

    // The editable full clone lands well before the archive delay elapses — proof
    // it never waited for the archive.
    let t = Instant::now();
    let mut editable = None;
    for _ in 0..40 {
        if let Ok((g, d)) = clone_only(&server, "acme", "decouple", 0, CloneMode::Editable).await
            && git(&d, &["rev-list", "--count", "HEAD"]) == "2"
        {
            editable = Some((g, d));
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (_g0, d0) = editable.expect("editable full clone ready");
    let editable_ms = t.elapsed().as_millis() as u64;
    assert_eq!(read(&d0, "a.txt"), "2\n");
    assert_eq!(read(&d0, "dir/b.txt"), "B\n");
    assert_repo_usable(&d0, "2");
    assert!(
        editable_ms < ARCHIVE_DELAY_MS - 500,
        "editable full clone waited on the archive ({editable_ms} ms)"
    );

    // Files mode is published a moment later; it waits for the archive, then works.
    let (_g1, d1) = clone_files_when(&server, "acme", "decouple", "a.txt", "2\n").await;
    assert_eq!(read(&d1, "dir/b.txt"), "B\n");
}
