//! Full-server e2e: the metadata store on **Postgres** (`RIPCLONE_METADATA=postgres`)
//! with default local storage + local queue. A real `sync` writes the ref into
//! the pg `refs` table and `clone` reads it back. Runs only when
//! `RIPCLONE_TEST_PG_URL` is set (see scripts/test-queue-sql.sh); skips otherwise.
//! Uses a unique repo name so it never collides with rows from prior runs.

mod common;

use common::*;
use ripclone::mode::CloneMode;

#[tokio::test]
async fn metadata_postgres_sync_then_clone() {
    let Ok(url) = std::env::var("RIPCLONE_TEST_PG_URL") else {
        eprintln!("SKIP metadata_postgres_sync_then_clone: RIPCLONE_TEST_PG_URL unset");
        return;
    };
    unsafe {
        std::env::set_var("RIPCLONE_METADATA", "postgres");
        std::env::set_var("RIPCLONE_METADATA_DB_URL", &url);
    }
    init(false);

    let server = start_server().await;
    let repo = format!("pgmeta{}", std::process::id());

    let origin = make_origin("acme", &repo);
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n")], "c2");
    origin.publish();

    let resp = server
        .client()
        .sync_repo(&format!("acme/{repo}"), None)
        .await
        .expect("sync with postgres metadata store");
    assert!(!resp.commit.is_empty());

    let (_g, c) = clone_only(&server, "acme", &repo, 0, CloneMode::Editable)
        .await
        .expect("clone reads ref back from postgres metadata store");
    assert_eq!(std::fs::read_to_string(c.join("a.txt")).unwrap(), "2\n");
    assert_eq!(git(&c, &["rev-list", "--count", "HEAD"]), "2");
}
