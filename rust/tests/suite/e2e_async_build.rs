//! End-to-end tests for the async build queue (always on): `/sync` enqueues onto
//! the bounded background worker (survives disconnect, rate-bounded) and waits
//! for completion; concurrent syncs for the same repo coalesce onto one build.

use crate::common::*;
use ripclone::mode::CloneMode;
use std::path::Path;

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap()
}

/// A sync via the queue returns the ref, and both depth=1 and depth=0 clone
/// correctly.
#[tokio::test]
async fn async_sync_then_clone() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "aq");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("b.txt", "x\n")], "c2");
    origin.publish();

    server.client().add_repo("acme/aq").await.expect("add aq");
    // Sync goes through the background worker; the client waits for completion.
    server
        .client()
        .sync_repo("acme/aq", None)
        .await
        .expect("async sync");

    let (_g1, c1) = clone_only(&server, "acme", "aq", 1, CloneMode::Editable)
        .await
        .expect("depth=1");
    assert_eq!(read(&c1, "a.txt"), "2\n");
    assert_eq!(git(&c1, &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(git(&c1, &["status", "--porcelain"]), "");

    // depth=0 (full) builds in the background under two-phase, so poll for it.
    let (_g0, c0) = clone_full_at(&server, "acme", "aq", "2").await;
    assert_eq!(read(&c0, "a.txt"), "2\n");
    assert!(git_ok(&c0, &["fsck", "--connectivity-only", "HEAD"]));
}

/// Concurrent syncs for the same repo coalesce onto one build and all succeed
/// with the same resolved commit.
#[tokio::test]
async fn async_concurrent_syncs_coalesce() {
    init(false);
    let server = start_server().await;
    let origin = make_origin("acme", "aqc");
    origin.commit(&[("f", "1\n")], "c1");
    origin.commit(&[("f", "2\n")], "c2");
    origin.publish();

    server.client().add_repo("acme/aqc").await.expect("add aqc");
    // Fire several concurrent syncs for the same repo.
    let mut handles = Vec::new();
    for _ in 0..6 {
        let client = server.client();
        handles.push(tokio::spawn(async move {
            client.sync_repo("acme/aqc", None).await
        }));
    }
    let mut commits = Vec::new();
    for h in handles {
        let resp = h.await.expect("join").expect("sync ok");
        commits.push(resp.commit);
    }
    // All resolved to the same commit (one coalesced build served everyone).
    assert!(
        commits.windows(2).all(|w| w[0] == w[1]),
        "all coalesced syncs return the same commit: {commits:?}"
    );

    // depth=0 (full) builds in the background under two-phase, so poll for it.
    let (_g, c) = clone_full_at(&server, "acme", "aqc", "2").await;
    assert_eq!(read(&c, "f"), "2\n");
    assert!(git_ok(&c, &["fsck", "--connectivity-only", "HEAD"]));
}
