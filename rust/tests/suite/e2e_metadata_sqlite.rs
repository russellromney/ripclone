//! End-to-end test that the server keeps its ref metadata in a SQL database
//! (`RIPCLONE_METADATA=sqlite`) rather than file/S3: a real `sync` writes the
//! ref into the `refs` table and a subsequent `clone` reads it back from there.
//! Uses the default local queue + local storage, so only the metadata path is
//! exercised against SQL.

use crate::common::*;
use std::path::Path;

#[tokio::test]
async fn metadata_sqlite_sync_then_clone() {
    let mdir = tempfile::tempdir().expect("metadata dir");
    let db_path = mdir.path().join("meta.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_METADATA", "sqlite");
        std::env::set_var("RIPCLONE_METADATA_DB_URL", &db_path);
    }
    init(false);

    let server = start_server().await;

    let origin = make_origin("acme", "meta");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();

    register_added_without_build(&server, "acme/meta")
        .await
        .expect("add repo");
    let resp = server
        .client()
        .sync_repo("acme/meta", None)
        .await
        .expect("sync with sqlite metadata store");
    assert!(!resp.commit.is_empty());

    // The metadata db file was created (the ref lives in SQL, not a file/S3).
    assert!(
        Path::new(&db_path).exists(),
        "sqlite metadata db should exist"
    );

    // The full clone builds in the background under two-phase, so poll for it
    // (this also exercises reading the ref back from the sqlite metadata store).
    let (_g, c) = clone_full_at(&server, "acme", "meta", "2").await;
    assert_eq!(std::fs::read_to_string(c.join("a.txt")).unwrap(), "2\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));
}
