//! Dead-man's switch, real seam: drives the actual `reconcile_once` ->
//! `DeadMansSwitch` wiring (the same two calls `run_loop` makes each pass)
//! against a real sqlite job queue and a `MockProvider`, with a real local
//! HTTP sink recording hits. No real worker processes / SIGKILL needed here —
//! the switch only cares about `ReconcileOutcome` shape, so this is
//! deterministic (asserted on the sink's hit count, never wall-clock).
//!
//! UNIT coverage of the switch's own state machine lives in
//! `dispatch::heartbeat`. This is the "wire it into the real loop" proof:
//! a `MockProvider` that always fails `ensure_worker` + a queue with pending
//! depth reproduces the exact "capacity/provider outage while work piles up"
//! shape the switch is meant to catch.

use axum::Router;
use axum::routing::get;
use ripclone::dispatch::autoscale::ReconcileInputs;
use ripclone::dispatch::{BackoffState, DeadMansSwitch, MockProvider, WEDGED_CYCLES_TO_STOP};
use ripclone::provider::RepoId;
use ripclone::queue::{BuildJob, JobQueue, SqlJobQueue, SqliteDb};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

async fn spawn_sink() -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/heartbeat",
        get(move || {
            let hits = hits2.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                "ok"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    tokio::task::yield_now().await;
    (format!("http://{addr}/heartbeat"), hits)
}

async fn test_queue() -> (SqlJobQueue, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("q.db").to_string_lossy().to_string();
    let q = SqlJobQueue::new(Box::new(SqliteDb::connect(&path).await.unwrap()))
        .await
        .unwrap();
    (q, dir)
}

fn job(path: &str) -> BuildJob {
    BuildJob {
        repo_id: RepoId::github(path),
        branch: "main".into(),
        rev: None,
        credential: None,
        recheck: 0,
        size_bytes: Some(100),
    }
}

/// Real seam: a healthy (empty-queue) reconcile pings the sink; a sustained
/// provider outage while work piles up (depth > 0, started == 0, failed > 0
/// each pass) stops pinging after `WEDGED_CYCLES_TO_STOP` cycles; recovery
/// (started > 0) resumes pinging on the same pass.
#[tokio::test]
async fn heartbeat_pings_healthy_stops_on_wedged_streak_resumes_on_recovery() {
    let (url, hits) = spawn_sink().await;
    let (q, _dir) = test_queue().await;
    let mock = MockProvider::new();
    let env = std::collections::BTreeMap::new();
    let mut heartbeat = DeadMansSwitch::new(Some(url));

    // ---- Healthy: empty queue.
    let mut backoff = BackoffState::new();
    let out = ripclone::dispatch::reconcile_once(ReconcileInputs {
        queue: &q,
        provider: &mock,
        max_workers: 5,
        worker_env: &env,
        backoff: &mut backoff,
        now: Instant::now(),
    })
    .await
    .unwrap();
    assert_eq!(out.plan.total_pending, 0);
    heartbeat
        .on_reconcile(out.plan.total_pending, out.started, out.failed)
        .await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "healthy (empty-queue) reconcile must ping"
    );

    // ---- Wedged: one job pending, provider fails ensure_worker every pass.
    // A fresh BackoffState per pass keeps each one an actual attempt-and-fail
    // (not a backoff-skipped no-op), matching the "dispatch actively failing"
    // shape the switch defines as wedged.
    q.enqueue(job("o/wedge")).await.unwrap();
    for cycle in 1..=WEDGED_CYCLES_TO_STOP {
        mock.fail_next(1);
        let mut backoff = BackoffState::new();
        let out = ripclone::dispatch::reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        assert_eq!(out.plan.total_pending, 1, "cycle {cycle}: {out:?}");
        assert_eq!(out.started, 0, "cycle {cycle}: {out:?}");
        assert_eq!(out.failed, 1, "cycle {cycle}: {out:?}");
        heartbeat
            .on_reconcile(out.plan.total_pending, out.started, out.failed)
            .await;
    }
    let stopped_hits = hits.load(Ordering::SeqCst);
    assert_eq!(
        stopped_hits,
        1 + (WEDGED_CYCLES_TO_STOP as usize - 1),
        "pings continue through cycles under the threshold, stop AT the Nth"
    );
    assert!(heartbeat.is_stopped());

    // One more wedged cycle beyond the threshold: still no new ping.
    mock.fail_next(1);
    let mut backoff = BackoffState::new();
    let out = ripclone::dispatch::reconcile_once(ReconcileInputs {
        queue: &q,
        provider: &mock,
        max_workers: 5,
        worker_env: &env,
        backoff: &mut backoff,
        now: Instant::now(),
    })
    .await
    .unwrap();
    heartbeat
        .on_reconcile(out.plan.total_pending, out.started, out.failed)
        .await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        stopped_hits,
        "no new ping while the outage continues past the stop threshold"
    );

    // ---- Recovery: provider works again, this pass actually starts a
    // worker for the pending job (started > 0) — resets the streak AND
    // resumes pinging on this same pass.
    mock.reset();
    let mut backoff = BackoffState::new();
    let out = ripclone::dispatch::reconcile_once(ReconcileInputs {
        queue: &q,
        provider: &mock,
        max_workers: 5,
        worker_env: &env,
        backoff: &mut backoff,
        now: Instant::now(),
    })
    .await
    .unwrap();
    assert!(
        out.started > 0,
        "recovery pass must start a worker: {out:?}"
    );
    heartbeat
        .on_reconcile(out.plan.total_pending, out.started, out.failed)
        .await;
    assert!(!heartbeat.is_stopped());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        stopped_hits + 1,
        "a healthy (started > 0) reconcile resumes pinging immediately"
    );
}

/// `RIPCLONE_HEARTBEAT_URL` unset -> `DeadMansSwitch::from_env()` is fully
/// inert: no pinging even across a sustained wedged streak. Proves the "no
/// behavior change when unset" contract using the real env-reading
/// constructor (not just `DeadMansSwitch::new(None)`).
#[tokio::test]
async fn unset_heartbeat_url_env_is_fully_inert() {
    // SAFETY: this test does not run concurrently with anything else that
    // reads RIPCLONE_HEARTBEAT_URL; ensure it is absent for this process.
    unsafe {
        std::env::remove_var("RIPCLONE_HEARTBEAT_URL");
    }
    let (q, _dir) = test_queue().await;
    let mock = MockProvider::new();
    let env = std::collections::BTreeMap::new();
    let mut heartbeat = DeadMansSwitch::from_env();
    assert!(!heartbeat.is_configured());

    q.enqueue(job("o/wedge")).await.unwrap();
    for _ in 0..(WEDGED_CYCLES_TO_STOP * 2) {
        mock.fail_next(1);
        let mut backoff = BackoffState::new();
        let out = ripclone::dispatch::reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        heartbeat
            .on_reconcile(out.plan.total_pending, out.started, out.failed)
            .await;
    }
    assert_eq!(
        heartbeat.consecutive_wedged(),
        0,
        "an inert switch (no URL) must not even track wedged state"
    );
    assert!(!heartbeat.is_stopped());
}
