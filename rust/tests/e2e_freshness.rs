//! End-to-end tests for the post-build freshness re-check: when the upstream tip
//! moves *during* a build, the build that just finished re-checks the tip and
//! builds the latest commit with no external poke (webhook/poll/Actions).
//!
//! These rely on a test-only hook (`RIPCLONE_TEST_RECHECK_DELAY_MS`) that holds
//! the re-check briefly so a push can land after the build resolved its commit
//! but before the re-check reads the tip. Because the hook env vars are read at
//! build time and the server runs in-process, the tests serialize on `SERIAL`.

mod common;

use common::*;
use std::time::Duration;

/// Serializes these tests: they set process-global, build-time env vars
/// (`RIPCLONE_TEST_RECHECK_DELAY_MS`, `RIPCLONE_RECHECK_MAX`) that the in-process
/// server reads, so only one may run at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn set_env(key: &str, val: &str) {
    // SAFETY: tests holding SERIAL are the only mutators of these vars.
    unsafe { std::env::set_var(key, val) };
}

/// Scrape the completed-build counter from `/metrics` (unauthenticated).
async fn builds_completed(server: &Server) -> u64 {
    let body = reqwest::get(format!("{}/metrics", server.url))
        .await
        .expect("metrics request")
        .text()
        .await
        .expect("metrics body");
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("ripclone_builds_completed_total ") {
            return rest.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// The branch's currently-served commit, or `None` if not resolvable yet.
async fn served_commit(server: &Server, repo: &str) -> Option<String> {
    server
        .client()
        .resolve_ref(repo, "HEAD")
        .await
        .ok()
        .map(|r| r.commit)
}

/// Poll until at least `min_builds` builds have completed and the served commit
/// equals `want`, or panic after ~20s. Gating on the completed-build counter (not
/// just the served commit) avoids the "building" placeholder, which exposes the
/// in-progress commit before its build records completion.
async fn wait_until(server: &Server, repo: &str, want: &str, min_builds: u64) {
    for _ in 0..160 {
        if builds_completed(server).await >= min_builds
            && served_commit(server, repo).await.as_deref() == Some(want)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
    let got = served_commit(server, repo).await;
    let n = builds_completed(server).await;
    panic!("never reached {want} with >= {min_builds} builds; last seen {got:?}, builds {n}");
}

/// A push that lands during a build is built by the post-build re-check, with no
/// external poke.
#[tokio::test]
async fn recheck_builds_tip_that_moved_during_build() {
    let _guard = SERIAL.lock().await;
    enable_async_build();
    set_env("RIPCLONE_RECHECK_MAX", "3");
    set_env("RIPCLONE_TEST_RECHECK_DELAY_MS", "2500");
    init(false);
    let server = start_server().await;

    let origin = make_origin("acme", "fresh1");
    let a = origin.commit(&[("f", "a\n")], "A");
    origin.publish();

    // Build A. The re-check after it will hold (2.5s) before reading the tip.
    let client = server.client();
    let sync = tokio::spawn(async move { client.sync_repo("acme/fresh1", None).await });

    // While the re-check is held, advance the origin to B.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let b = origin.commit(&[("f", "b\n")], "B");
    origin.publish();

    let resp = sync.await.expect("join").expect("sync A");
    assert_eq!(resp.commit, a, "the synced build is A");

    // No webhook, no poll: the post-build re-check alone catches up to B.
    wait_until(&server, "acme/fresh1", &b, 2).await;
    // Let any (incorrect) further re-trigger settle, then confirm exactly two.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(
        builds_completed(&server).await,
        2,
        "only A and B were built"
    );
}

/// A burst of pushes during a build collapses to one catch-up build of the latest
/// commit; the intermediate commits are skipped.
#[tokio::test]
async fn recheck_burst_collapses_to_latest() {
    let _guard = SERIAL.lock().await;
    enable_async_build();
    set_env("RIPCLONE_RECHECK_MAX", "5");
    set_env("RIPCLONE_TEST_RECHECK_DELAY_MS", "2500");
    init(false);
    let server = start_server().await;

    let origin = make_origin("acme", "fresh2");
    let a = origin.commit(&[("f", "a\n")], "A");
    origin.publish();

    let client = server.client();
    let sync = tokio::spawn(async move { client.sync_repo("acme/fresh2", None).await });

    // While A's re-check is held, push B, C, D.
    tokio::time::sleep(Duration::from_millis(800)).await;
    origin.commit(&[("f", "b\n")], "B");
    origin.commit(&[("f", "c\n")], "C");
    let d = origin.commit(&[("f", "d\n")], "D");
    origin.publish();

    let resp = sync.await.expect("join").expect("sync A");
    assert_eq!(resp.commit, a);

    // The re-check builds the latest tip (D) directly, skipping B and C.
    wait_until(&server, "acme/fresh2", &d, 2).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(
        builds_completed(&server).await,
        2,
        "only A and the latest (D) were built; B and C were skipped"
    );
}

/// A repo whose tip keeps moving stops re-triggering after the cap; the chain
/// does not livelock. The key: a moved tip is present during the *capped*
/// re-check's window, so only the cap (not "the tip didn't move") prevents the
/// extra build — the catch-up test shows an uncapped re-check would build it.
#[tokio::test]
async fn recheck_stops_at_cap() {
    let _guard = SERIAL.lock().await;
    enable_async_build();
    set_env("RIPCLONE_RECHECK_MAX", "1");
    set_env("RIPCLONE_TEST_RECHECK_DELAY_MS", "2000");
    init(false);
    let server = start_server().await;

    let origin = make_origin("acme", "fresh3");
    let a = origin.commit(&[("f", "a\n")], "A");
    origin.publish();

    // Build A (recheck=0). Its re-check (held by the delay) builds B (recheck=1).
    let client = server.client();
    let sync = tokio::spawn(async move { client.sync_repo("acme/fresh3", None).await });
    tokio::time::sleep(Duration::from_millis(700)).await;
    let b = origin.commit(&[("f", "b\n")], "B");
    origin.publish();
    let resp = sync.await.expect("join").expect("sync A");
    assert_eq!(resp.commit, a);

    // B builds and catches up. B's own re-check (recheck=1) is now in its held
    // window, about to hit the cap.
    wait_until(&server, "acme/fresh3", &b, 2).await;

    // Move the tip to C while B's capped re-check is held. An uncapped re-check
    // would wake, see C, and build it; the cap must stop it instead.
    let c = origin.commit(&[("f", "c\n")], "C");
    origin.publish();

    // Wait out B's re-check window plus margin, then assert C was never built.
    tokio::time::sleep(Duration::from_millis(3000)).await;
    assert_ne!(
        served_commit(&server, "acme/fresh3").await.as_deref(),
        Some(c.as_str()),
        "C must not be served; the cap stopped the chain"
    );
    assert_eq!(
        served_commit(&server, "acme/fresh3").await.as_deref(),
        Some(b.as_str()),
        "served tip stays at B"
    );
    assert_eq!(
        builds_completed(&server).await,
        2,
        "only A and B were built; the cap prevented building C"
    );
}

/// A re-check that finds the tip unchanged does nothing — no extra build.
#[tokio::test]
async fn recheck_noop_when_tip_unchanged() {
    let _guard = SERIAL.lock().await;
    enable_async_build();
    set_env("RIPCLONE_RECHECK_MAX", "3");
    set_env("RIPCLONE_TEST_RECHECK_DELAY_MS", "0");
    init(false);
    let server = start_server().await;

    let origin = make_origin("acme", "fresh4");
    let a = origin.commit(&[("f", "a\n")], "A");
    origin.publish();

    server
        .client()
        .sync_repo("acme/fresh4", None)
        .await
        .expect("sync A");

    wait_until(&server, "acme/fresh4", &a, 1).await;
    // Let any spurious re-trigger settle, then confirm exactly one build ran.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(
        served_commit(&server, "acme/fresh4").await.as_deref(),
        Some(a.as_str())
    );
    assert_eq!(
        builds_completed(&server).await,
        1,
        "no extra build on a no-op re-check"
    );
}
