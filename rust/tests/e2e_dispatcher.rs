//! End-to-end: depth-based dispatcher autoscale drives real `ripclone-worker`
//! processes through `ExecProvider` against a real sqlite queue.
//!
//! UNIT coverage in `dispatch::autoscale` uses a mock provider. These tests are
//! the user-facing seam: enqueue → `reconcile_once` → real workers claim/build/ack
//! → queue drains. Positive and negative paths both go through that seam.
//!
//! ## exec → worker launch script
//!
//! `ExecProvider` appends `size_class` as a trailing argv element. `ripclone-worker`
//! is clap-flag-only and rejects that positional — production therefore uses a
//! launch script that ignores it and `exec`s the worker with env + flags. These
//! tests write the same shape of wrapper (faithful, not a workaround).
//!
//! Queue URL + worker env is process-global; serialize like `e2e_worker_idle_exit`.

mod common;

use common::*;
use ripclone::backends;
use ripclone::dispatch::autoscale::ReconcileInputs;
use ripclone::dispatch::{
    BackoffState, ComputeProvider, ExecProvider, ExecProviderConfig, WorkerSpec,
    collect_worker_env, reconcile_once,
};
use ripclone::provider::RepoId;
use ripclone::queue::{BuildJob, JobQueue, JobState, SqlJobQueue};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Queue URL + worker env is process-global; cargo runs tests in parallel.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// > 1 GiB → large under launch defaults (`default_size_classes`).
const LARGE_BYTES: u64 = (1 << 30) + 1;
/// Well under 1 GiB → small.
const SMALL_BYTES: u64 = 100;
/// Rank of `large` under launch defaults (small=0, large=1).
const LARGE_RANK: i64 = 1;

fn setup_sqlite_queue() -> tempfile::TempDir {
    let qdir = tempfile::tempdir().expect("queue dir");
    let db_path = qdir.path().join("queue.db").to_string_lossy().to_string();
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "sqlite");
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", &db_path);
    }
    init(false);
    qdir
}

fn publish_origin(owner: &str, repo: &str, file: &str, body: &str) -> Origin {
    let origin = make_origin(owner, repo);
    origin.commit(&[(file, body)], "c1");
    origin.publish();
    origin
}

async fn enqueue_sized(queue: &SqlJobQueue, path: &str, size_bytes: Option<u64>) -> i64 {
    let enq = queue
        .enqueue(BuildJob {
            repo_id: RepoId::github(path),
            branch: "main".into(),
            rev: None,
            credential: None,
            recheck: 0,
            size_bytes,
        })
        .await
        .expect("enqueue");
    enq.job_id.expect("job id")
}

async fn assert_still_pending(queue: &SqlJobQueue, id: i64) {
    match queue.job_status(id).await.expect("status") {
        JobState::Pending => {}
        other => panic!("expected job {id} still pending, got {other:?}"),
    }
}

/// Wait until the workers registry shows a live small-only worker (heartbeat).
///
/// Without this, a size-class negative can pass for the wrong reason: no worker
/// ever started, so the large job stays queued, then a later large reconcile
/// drains it. The claim-filter proof requires a live small-capped process.
async fn wait_small_only_live(queue: &SqlJobQueue, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let live = queue.live_worker_count().await.expect("live count");
        let capable_large = queue
            .live_worker_count_capable(LARGE_RANK)
            .await
            .expect("capable count");
        if live >= 1 && capable_large == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a small-only live worker \
             (live={live}, large_capable={capable_large}); \
             worker may have failed to start or heartbeat"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn chmod_x(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms).unwrap();
    }
    let _ = path;
}

/// Production-shaped launch script: ignore trailing `size_class`, exec the real
/// worker with cas/repo flags. Env bag (queue, storage, lifecycle, max-size-class)
/// is inherited + overlaid by `ExecProvider`.
fn write_worker_wrapper(
    dir: &Path,
    worker_bin: &Path,
    cas_dir: &Path,
    repo_root: &Path,
) -> PathBuf {
    let path = dir.join("launch-worker.sh");
    // Paths are absolute tempdir paths from this process — no shell metachar risk.
    let script = format!(
        r#"#!/bin/sh
# Exec dispatch launch helper: size_class is the trailing argv (from
# ExecProvider). The real worker is flag/env only and rejects that positional.
# All config (queue URL, max-size-class, max-jobs, idle-exit, …) is in env.
exec "{worker}" --cas-dir "{cas}" --repo-root "{repos}" --idle-poll-ms 100
"#,
        worker = worker_bin.display(),
        cas = cas_dir.display(),
        repos = repo_root.display(),
    );
    std::fs::write(&path, &script).expect("write wrapper");
    chmod_x(&path);
    path
}

/// Full local worker bag the dispatcher must pass through (mirrors what
/// `collect_worker_env` + lifecycle flags look like for a self-host exec deploy).
fn local_worker_env() -> BTreeMap<String, String> {
    let mut env = collect_worker_env();
    assert!(
        env.contains_key("RIPCLONE_QUEUE"),
        "worker_env must forward RIPCLONE_QUEUE from dispatcher process"
    );
    assert!(
        env.contains_key("RIPCLONE_QUEUE_DB_URL"),
        "worker_env must forward RIPCLONE_QUEUE_DB_URL from dispatcher process"
    );
    // Drain cleanly so the test does not leave forever-workers around.
    env.insert("RIPCLONE_IDLE_EXIT_SECS".into(), "2".into());
    // Live registry so multi-pass reconcile (and size-class proofs) see started workers.
    env.insert("RIPCLONE_WORKER_HEARTBEAT".into(), "queue".into());
    // First heartbeat is immediate; short interval keeps the registry fresh in tests.
    env.insert("RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS".into(), "1".into());
    env.insert("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS".into(), "30".into());
    env
}

fn exec_provider(wrapper: PathBuf) -> ExecProvider {
    ExecProvider::new(ExecProviderConfig {
        program: wrapper,
        fixed_args: Vec::<OsString>::new(),
    })
    .expect("ExecProvider")
}

/// Drive `reconcile_once` until every job is Done (or timeout).
async fn reconcile_until_done(
    queue: &SqlJobQueue,
    provider: &dyn ComputeProvider,
    worker_env: &BTreeMap<String, String>,
    max_workers: usize,
    job_ids: &[i64],
    timeout: Duration,
    backoff: &mut BackoffState,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_done = true;
        for &id in job_ids {
            match queue.job_status(id).await.expect("status") {
                JobState::Done => {}
                JobState::Failed(e) => panic!("job {id} failed during reconcile drain: {e}"),
                JobState::Pending | JobState::Unknown => all_done = false,
            }
        }
        if all_done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "jobs {job_ids:?} did not drain via dispatcher within {timeout:?}"
        );

        let now = Instant::now();
        let out = reconcile_once(ReconcileInputs {
            queue,
            provider,
            max_workers,
            worker_env,
            backoff,
            now,
        })
        .await
        .expect("reconcile_once");

        // Respect real wall-clock backoff (do not clear it in the test harness).
        if out.skipped_backoff || out.failed > 0 {
            let wait = backoff.remaining(Instant::now()).max(Duration::from_millis(200));
            tokio::time::sleep(wait).await;
        } else {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// POSITIVE
// ---------------------------------------------------------------------------

/// Enqueue → reconcile starts REAL workers via ExecProvider → claim/build/ack
/// → queue depth 0 and every job Done.
///
/// A no-op or broken dispatcher never drains; the drain assertion is the proof.
#[tokio::test]
async fn dispatcher_reconcile_drains_real_workers() {
    let _guard = SERIAL.lock().await;
    let _qdir = setup_sqlite_queue();
    let server = start_server().await;
    let wrapper_dir = tempfile::tempdir().expect("wrapper dir");
    let wrapper = write_worker_wrapper(
        wrapper_dir.path(),
        &cargo_bin("ripclone-worker"),
        &server.cas_dir,
        &server.repo_root,
    );
    let provider = exec_provider(wrapper);
    let worker_env = local_worker_env();

    let _o1 = publish_origin("acme", "disp-a", "a.txt", "a\n");
    let _o2 = publish_origin("acme", "disp-b", "b.txt", "b\n");
    let _o3 = publish_origin("acme", "disp-c", "c.txt", "c\n");

    let queue = backends::connect_sql_queue().await.expect("queue");
    let id_a = enqueue_sized(&queue, "acme/disp-a", Some(SMALL_BYTES)).await;
    let id_b = enqueue_sized(&queue, "acme/disp-b", Some(SMALL_BYTES)).await;
    let id_c = enqueue_sized(&queue, "acme/disp-c", Some(SMALL_BYTES)).await;
    assert_eq!(queue.depth().await, 3, "three jobs queued before reconcile");

    // One reconcile pass should plan desired=3 and spawn that many workers.
    let mut backoff = BackoffState::new();
    let first = reconcile_once(ReconcileInputs {
        queue: &queue,
        provider: &provider,
        max_workers: 10,
        worker_env: &worker_env,
        backoff: &mut backoff,
        now: Instant::now(),
    })
    .await
    .expect("first reconcile");
    assert_eq!(first.plan.total_pending, 3, "plan sees all pending: {first:?}");
    assert_eq!(first.plan.desired, 3, "desired tracks pending: {first:?}");
    assert_eq!(first.plan.to_start, 3, "no live workers yet → start 3: {first:?}");
    assert_eq!(
        first.started, 3,
        "ExecProvider must spawn all planned workers: {first:?}"
    );
    assert_eq!(first.failed, 0, "first ensure_worker batch must not fail: {first:?}");
    assert!(
        first.plan.size_classes.iter().all(|s| s == "small"),
        "small pending must start small-class slots: {:?}",
        first.plan.size_classes
    );

    reconcile_until_done(
        &queue,
        &provider,
        &worker_env,
        10,
        &[id_a, id_b, id_c],
        Duration::from_secs(120),
        &mut backoff,
    )
    .await;

    assert_eq!(queue.depth().await, 0, "queue empty after dispatcher drain");
    for id in [id_a, id_b, id_c] {
        match queue.job_status(id).await.expect("status") {
            JobState::Done => {}
            other => panic!("job {id} expected Done, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// NEGATIVE: provider down → no data loss → recovery
// ---------------------------------------------------------------------------

/// Provider outage: `ensure_worker` fails, jobs stay queued (not Failed), backoff
/// blocks starts. Wait out real backoff, swap to a working provider, drain.
#[tokio::test]
async fn dispatcher_provider_down_no_job_loss_then_recovers() {
    let _guard = SERIAL.lock().await;
    let _qdir = setup_sqlite_queue();
    let server = start_server().await;
    let wrapper_dir = tempfile::tempdir().expect("wrapper dir");
    let good_wrapper = write_worker_wrapper(
        wrapper_dir.path(),
        &cargo_bin("ripclone-worker"),
        &server.cas_dir,
        &server.repo_root,
    );
    let worker_env = local_worker_env();

    let _o1 = publish_origin("acme", "disp-down-a", "a.txt", "a\n");
    let _o2 = publish_origin("acme", "disp-down-b", "b.txt", "b\n");

    let queue = backends::connect_sql_queue().await.expect("queue");
    let id_a = enqueue_sized(&queue, "acme/disp-down-a", Some(SMALL_BYTES)).await;
    let id_b = enqueue_sized(&queue, "acme/disp-down-b", Some(SMALL_BYTES)).await;
    let depth_before = queue.depth().await;
    assert_eq!(depth_before, 2);

    // Path that fails to spawn — ensure_worker surfaces Err (unit test pattern).
    // A wrapper that exits 1 is NOT enough: ExecProvider is fire-and-forget and
    // returns Ok as soon as spawn succeeds.
    let bad = ExecProvider::new(ExecProviderConfig {
        program: PathBuf::from("/nonexistent/ripclone-dispatch-helper-e2e-xyz"),
        fixed_args: vec![],
    })
    .expect("bad provider config");

    let mut backoff = BackoffState::new();
    let now = Instant::now();
    let out = reconcile_once(ReconcileInputs {
        queue: &queue,
        provider: &bad,
        max_workers: 10,
        worker_env: &worker_env,
        backoff: &mut backoff,
        now,
    })
    .await
    .expect("reconcile against down provider must not abort the loop");

    assert_eq!(out.failed, 1, "ensure_worker must report failure: {out:?}");
    assert_eq!(out.started, 0, "no workers started while provider is down");
    assert_eq!(
        backoff.consecutive_failures(),
        1,
        "backoff must record the failure"
    );
    assert!(
        backoff.is_blocked(now),
        "backoff must block further starts immediately after failure"
    );

    // While blocked, a second reconcile must skip starts (not hammer the provider).
    let out_blocked = reconcile_once(ReconcileInputs {
        queue: &queue,
        provider: &bad,
        max_workers: 10,
        worker_env: &worker_env,
        backoff: &mut backoff,
        now: Instant::now(),
    })
    .await
    .expect("blocked reconcile");
    assert!(
        out_blocked.skipped_backoff,
        "second pass during backoff must skip starts: {out_blocked:?}"
    );
    assert_eq!(out_blocked.started, 0);
    assert_eq!(out_blocked.failed, 0);

    // Jobs are NOT lost and NOT marked failed.
    assert_eq!(
        queue.depth().await,
        depth_before,
        "provider outage must not dequeue work"
    );
    for id in [id_a, id_b] {
        match queue.job_status(id).await.expect("status") {
            JobState::Pending => {}
            JobState::Failed(e) => panic!("job {id} must not fail on provider outage: {e}"),
            JobState::Done => panic!("job {id} must not complete without a worker"),
            JobState::Unknown => panic!("job {id} status unknown"),
        }
    }

    // Recovery: wait out the real backoff window (no artificial on_success clear),
    // then swap to a working ExecProvider and drain.
    let wait = backoff.remaining(Instant::now()) + Duration::from_millis(50);
    tokio::time::sleep(wait).await;
    assert!(
        !backoff.is_blocked(Instant::now()),
        "backoff window must have expired before recovery reconcile"
    );

    let good = exec_provider(good_wrapper);
    reconcile_until_done(
        &queue,
        &good,
        &worker_env,
        10,
        &[id_a, id_b],
        Duration::from_secs(120),
        &mut backoff,
    )
    .await;
    assert_eq!(queue.depth().await, 0, "queue drains after provider recovery");
}

// ---------------------------------------------------------------------------
// NEGATIVE: size-class filter end-to-end
// ---------------------------------------------------------------------------

/// A large job must not be claimed by a small-capped worker; it stays queued
/// until reconcile starts a large-capable worker *while the small worker is still
/// live* (livelock would set to_start=0 until the small idle-exits — that is not
/// an acceptable proof).
#[tokio::test]
async fn dispatcher_size_class_large_not_claimed_by_small() {
    let _guard = SERIAL.lock().await;
    let _qdir = setup_sqlite_queue();
    let server = start_server().await;
    let wrapper_dir = tempfile::tempdir().expect("wrapper dir");
    let wrapper = write_worker_wrapper(
        wrapper_dir.path(),
        &cargo_bin("ripclone-worker"),
        &server.cas_dir,
        &server.repo_root,
    );
    let provider = exec_provider(wrapper);
    let base_env = local_worker_env();

    let _origin = publish_origin("acme", "disp-huge", "huge.txt", "x\n");
    let queue = backends::connect_sql_queue().await.expect("queue");
    let id = enqueue_sized(&queue, "acme/disp-huge", Some(LARGE_BYTES)).await;
    assert_eq!(queue.depth().await, 1);

    // Confirm classification: one large pending rank.
    let pending = queue.pending_by_class().await.expect("pending_by_class");
    assert_eq!(
        pending,
        vec![(LARGE_RANK, 1)],
        "large size_bytes must classify as rank {LARGE_RANK}: {pending:?}"
    );

    // Start only a SMALL-capped worker (env ceiling). reconcile would correctly
    // plan large for this queue — we force small here on purpose (leftover pool /
    // mis-sized machine still live while large work waits).
    let mut small_env = base_env.clone();
    small_env.insert("RIPCLONE_MAX_SIZE_CLASS".into(), "small".into());
    // Stay up through the refusal window + large-start assertion (must outlive
    // that path so we cannot "prove" drain only after small idle-exits).
    small_env.insert("RIPCLONE_IDLE_EXIT_SECS".into(), "60".into());

    provider
        .ensure_worker(&WorkerSpec::new("small", small_env))
        .await
        .expect("spawn small-capped worker");

    // Proof the small worker is actually up and registered as not large-capable.
    // Without this, "still pending" is also true when no worker started at all.
    wait_small_only_live(&queue, Duration::from_secs(30)).await;
    assert_eq!(
        queue.live_worker_count().await.expect("live"),
        1,
        "exactly one small worker should be live"
    );
    assert_eq!(
        queue
            .live_worker_count_capable(LARGE_RANK)
            .await
            .expect("capable"),
        0,
        "small-only worker must not count as large-capable"
    );

    // Window where a wrongly-claiming small worker would flip the job (depth
    // drops on claim even while status is still Pending).
    let window = Duration::from_secs(5);
    let start = Instant::now();
    while start.elapsed() < window {
        assert_still_pending(&queue, id).await;
        assert_eq!(
            queue.depth().await,
            1,
            "large job must remain queued (not claimed) under a small-only worker"
        );
        // Small must still be live for the whole refusal window.
        assert!(
            queue.live_worker_count().await.expect("live") >= 1,
            "small worker died during refusal window — proof is void"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_still_pending(&queue, id).await;

    // Large-capable path via reconcile *while small is still live*:
    // capability-filtered live must be 0 → to_start=1 with size_class=large.
    // (Raw live_count=1 would livelock: desired=1, live=1, to_start=0.)
    let mut backoff = BackoffState::new();
    let large_start = reconcile_once(ReconcileInputs {
        queue: &queue,
        provider: &provider,
        max_workers: 10,
        worker_env: &base_env,
        backoff: &mut backoff,
        now: Instant::now(),
    })
    .await
    .expect("large reconcile");
    assert_eq!(
        large_start.plan.live_workers, 0,
        "plan must use capable live (0), not raw fleet size: {large_start:?}"
    );
    assert_eq!(
        large_start.plan.to_start, 1,
        "must start a large worker while small is still live (livelock if 0): {large_start:?}"
    );
    assert_eq!(
        large_start.started, 1,
        "large ensure_worker must succeed: {large_start:?}"
    );
    assert_eq!(
        large_start.plan.size_classes,
        vec!["large".to_string()],
        "max pending rank must select large: {large_start:?}"
    );
    // Small still live at the moment we started large — otherwise livelock
    // "fixed itself" by idle-exit and the assertion above is weak.
    assert!(
        queue.live_worker_count().await.expect("live") >= 1,
        "small worker must still be live when large was started"
    );

    reconcile_until_done(
        &queue,
        &provider,
        &base_env,
        10,
        &[id],
        Duration::from_secs(120),
        &mut backoff,
    )
    .await;
    assert_eq!(queue.depth().await, 0, "large job drains on large-capable worker");
    match queue.job_status(id).await.expect("status") {
        JobState::Done => {}
        other => panic!("large job expected Done, got {other:?}"),
    }
}
