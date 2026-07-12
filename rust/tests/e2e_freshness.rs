//! End-to-end tests for the post-build freshness re-check: when the upstream tip
//! moves *during* a build, the build that just finished re-checks the tip and
//! builds the latest commit with no external poke (webhook/poll/Actions).
//!
//! These rely on a test-only barrier (`ripclone::server::set_recheck_barrier`)
//! that holds each re-check until the test explicitly releases it. This replaces
//! the old wall-clock sleep hook and makes the tests deterministic. The tests
//! serialize on `SERIAL` so only one barrier is active at a time.

mod common;

use common::*;
use ripclone::provider::RepoId;
use ripclone::ref_store::{AddedRepo, AddedRepoSource, FileRefStore, RefStore};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Serializes these tests: they set a process-global re-check barrier that the
/// in-process server reads, so only one may run at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn set_env(key: &str, val: &str) {
    // SAFETY: tests holding SERIAL are the only mutators of these vars.
    unsafe { std::env::set_var(key, val) };
}

/// Test-controlled barrier for post-build re-checks. The server signals when it
/// enters a re-check; the test advances the proceed counter to release it. This
/// two-way handshake removes all wall-clock races. On drop it releases any
/// pending re-checks and clears the global barrier so server tasks never hang
/// across tests.
struct RecheckBarrier {
    entered_rx: tokio::sync::watch::Receiver<usize>,
    proceed_tx: tokio::sync::watch::Sender<usize>,
    last_entered: usize,
}

impl RecheckBarrier {
    fn new() -> Self {
        let (entered_tx, entered_rx) = tokio::sync::watch::channel(0);
        let (proceed_tx, proceed_rx) = tokio::sync::watch::channel(0);
        ripclone::server::set_recheck_barrier(entered_tx, proceed_rx);
        Self {
            entered_rx,
            proceed_tx,
            last_entered: 0,
        }
    }

    /// Wait until the server has entered the next re-check.
    async fn wait_entered(&mut self) {
        self.entered_rx
            .wait_for(|v| *v > self.last_entered)
            .await
            .expect("recheck barrier sender not dropped");
        self.last_entered += 1;
    }

    /// Release the re-check that most recently entered the barrier.
    fn release(&self) {
        self.proceed_tx.send_modify(|v| *v += 1);
    }
}

impl Drop for RecheckBarrier {
    fn drop(&mut self) {
        // Release any re-checks still waiting for this test so the server's
        // background tasks complete instead of hanging. Read the current value
        // before sending: watch::Sender::send needs a write lock, so we must not
        // hold a borrow across the call.
        for _ in 0..5 {
            let next = *self.proceed_tx.borrow() + 1;
            let _ = self.proceed_tx.send(next);
        }
        ripclone::server::clear_recheck_barrier();
    }
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
        let n = builds_completed(server).await;
        let got = served_commit(server, repo).await;
        if n >= min_builds && got.as_deref() == Some(want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
    let got = served_commit(server, repo).await;
    let n = builds_completed(server).await;
    panic!("never reached {want} with >= {min_builds} builds; last seen {got:?}, builds {n}");
}

async fn register_added_repo(server: &Server, repo: &str) -> u64 {
    let repo_id = RepoId::github(repo);
    let store = FileRefStore::new(&server.repo_root);
    store
        .add_repo(&AddedRepo {
            repo_id,
            added_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_secs(),
            history_enabled: true,
            source: AddedRepoSource::Api,
            repo_size_bytes: None,
            state: ripclone::ref_store::RepoLifecycleState::Active,
            initialization_branch: None,
            initialization_target: None,
            activated_at: None,
            failure: None,
            initialization_attempt_id: None,
        })
        .await
        .expect("mark repo added");
    builds_completed(server).await
}

/// A push that lands during a build is built by the post-build re-check, with no
/// external poke.
#[tokio::test]
async fn recheck_builds_tip_that_moved_during_build() {
    let _guard = SERIAL.lock().await;
    set_env("RIPCLONE_RECHECK_MAX", "3");
    init(false);
    let server = start_server().await;

    let origin = make_origin("acme", "fresh1");
    let baseline = register_added_repo(&server, "acme/fresh1").await;
    let mut barrier = RecheckBarrier::new();
    let a = origin.commit(&[("f", "a\n")], "A");
    origin.publish();

    // Build A. The re-check after it will wait on the barrier.
    let client = server.client();
    let sync = tokio::spawn(async move { client.sync_repo("acme/fresh1", None).await });

    // Wait until the first post-build re-check enters the barrier, then advance
    // the upstream tip to B before letting it proceed.
    barrier.wait_entered().await;
    let b = origin.commit(&[("f", "b\n")], "B");
    origin.publish();
    barrier.release();

    let resp = sync.await.expect("join").expect("sync A");
    assert_eq!(resp.commit, a, "the synced build is A");

    // No webhook, no poll: the post-build re-check alone catches up to B.
    wait_until(&server, "acme/fresh1", &b, baseline + 2).await;

    // B's own re-check enters the barrier next. Let it proceed (it will see no
    // tip change and do nothing) and assert exactly two builds ran.
    barrier.wait_entered().await;
    barrier.release();
    assert_eq!(
        builds_completed(&server).await,
        baseline + 2,
        "only A and B were built"
    );
}

/// A burst of pushes during a build collapses to one catch-up build of the latest
/// commit; the intermediate commits are skipped.
#[tokio::test]
async fn recheck_burst_collapses_to_latest() {
    let _guard = SERIAL.lock().await;
    set_env("RIPCLONE_RECHECK_MAX", "5");
    init(false);
    let server = start_server().await;

    let origin = make_origin("acme", "fresh2");
    let baseline = register_added_repo(&server, "acme/fresh2").await;
    let mut barrier = RecheckBarrier::new();
    let a = origin.commit(&[("f", "a\n")], "A");
    origin.publish();

    let client = server.client();
    let sync = tokio::spawn(async move { client.sync_repo("acme/fresh2", None).await });

    barrier.wait_entered().await;
    origin.commit(&[("f", "b\n")], "B");
    origin.commit(&[("f", "c\n")], "C");
    let d = origin.commit(&[("f", "d\n")], "D");
    origin.publish();
    barrier.release();

    let resp = sync.await.expect("join").expect("sync A");
    assert_eq!(resp.commit, a);

    // The re-check builds the latest tip (D) directly, skipping B and C.
    wait_until(&server, "acme/fresh2", &d, baseline + 2).await;
    barrier.wait_entered().await;
    barrier.release();
    assert_eq!(
        builds_completed(&server).await,
        baseline + 2,
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
    set_env("RIPCLONE_RECHECK_MAX", "1");
    init(false);
    let server = start_server().await;

    let origin = make_origin("acme", "fresh3");
    let baseline = register_added_repo(&server, "acme/fresh3").await;
    let mut barrier = RecheckBarrier::new();
    let a = origin.commit(&[("f", "a\n")], "A");
    origin.publish();

    // Build A (recheck=0). Its re-check (held by the barrier) builds B (recheck=1).
    let client = server.client();
    let sync = tokio::spawn(async move { client.sync_repo("acme/fresh3", None).await });
    barrier.wait_entered().await;
    let b = origin.commit(&[("f", "b\n")], "B");
    origin.publish();
    barrier.release();

    let resp = sync.await.expect("join").expect("sync A");
    assert_eq!(resp.commit, a);

    // B builds and catches up. B's own re-check (recheck=1) is now blocked on
    // the barrier, about to hit the cap.
    wait_until(&server, "acme/fresh3", &b, baseline + 2).await;

    // Move the tip to C while B's capped re-check is held. An uncapped re-check
    // would wake, see C, and build it; the cap must stop it instead.
    barrier.wait_entered().await;
    let c = origin.commit(&[("f", "c\n")], "C");
    origin.publish();

    // Release B's capped re-check. It sees C but is at the cap, so it does not
    // enqueue a build.
    barrier.release();

    // Wait briefly for the re-check to complete, then assert C was never built.
    for _ in 0..20 {
        if served_commit(&server, "acme/fresh3").await.as_deref() == Some(b.as_str()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
        baseline + 2,
        "only A and B were built; the cap prevented building C"
    );
}

/// A re-check that finds the tip unchanged does nothing — no extra build.
#[tokio::test]
async fn recheck_noop_when_tip_unchanged() {
    let _guard = SERIAL.lock().await;
    set_env("RIPCLONE_RECHECK_MAX", "3");
    init(false);
    let server = start_server().await;

    let origin = make_origin("acme", "fresh4");
    let baseline = register_added_repo(&server, "acme/fresh4").await;
    let mut barrier = RecheckBarrier::new();
    let a = origin.commit(&[("f", "a\n")], "A");
    origin.publish();

    let sync = tokio::spawn({
        let client = server.client();
        async move { client.sync_repo("acme/fresh4", None).await }
    });

    // Wait until the first re-check enters the barrier, then release it with
    // the tip still at A.
    barrier.wait_entered().await;
    barrier.release();

    sync.await.expect("join").expect("sync A");

    wait_until(&server, "acme/fresh4", &a, baseline + 1).await;
    assert_eq!(
        served_commit(&server, "acme/fresh4").await.as_deref(),
        Some(a.as_str())
    );
    assert_eq!(
        builds_completed(&server).await,
        baseline + 1,
        "no extra build on a no-op re-check"
    );
}
