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
use ripclone::dispatch::autoscale::{
    BackoffState, ReconcileInputs, collect_worker_env, reconcile_once,
};
use ripclone::dispatch::{ComputeProvider, ExecProvider, ExecProviderConfig, WorkerSpec};
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

async fn wait_done(queue: &SqlJobQueue, id: i64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match queue.job_status(id).await.expect("status") {
            JobState::Done => return,
            JobState::Failed(e) => panic!("job {id} failed: {e}"),
            JobState::Pending | JobState::Unknown => {
                assert!(
                    Instant::now() < deadline,
                    "job {id} did not finish within {timeout:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn assert_still_pending(queue: &SqlJobQueue, id: i64) {
    match queue.job_status(id).await.expect("status") {
        JobState::Pending => {}
        other => panic!("expected job {id} still pending, got {other:?}"),
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
    // Drain cleanly so the test does not leave forever-workers around.
    env.insert("RIPCLONE_IDLE_EXIT_SECS".into(), "2".into());
    // Live registry so multi-pass reconcile sees started workers (optional for
    // drain correctness, but matches production autoscale).
    env.insert("RIPCLONE_WORKER_HEARTBEAT".into(), "queue".into());
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
) {
    let mut backoff = BackoffState::new();
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
            backoff: &mut backoff,
            now,
        })
        .await
        .expect("reconcile_once");

        // Respect backoff so a transient provider blip is not hammered.
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

    // One reconcile pass should plan to_start > 0 against a real provider.
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
    assert!(
        first.plan.to_start >= 1,
        "pending work must plan starts: {first:?}"
    );
    assert!(
        first.started >= 1,
        "ExecProvider must successfully spawn at least one worker: {first:?}"
    );
    assert_eq!(first.failed, 0, "first ensure_worker batch must not fail: {first:?}");

    reconcile_until_done(
        &queue,
        &provider,
        &worker_env,
        10,
        &[id_a, id_b, id_c],
        Duration::from_secs(120),
    )
    .await;

    assert_eq!(queue.depth().await, 0, "queue empty after dispatcher drain");
    for id in [id_a, id_b, id_c] {
        wait_done(&queue, id, Duration::from_secs(1)).await;
    }
}

// ---------------------------------------------------------------------------
// NEGATIVE: provider down → no data loss → recovery
// ---------------------------------------------------------------------------

/// Provider outage: `ensure_worker` fails, jobs stay queued (not Failed), backoff
/// blocks starts. Swap to a working provider and the queue drains.
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
    assert!(
        backoff.consecutive_failures() >= 1,
        "backoff must record the failure"
    );
    assert!(
        backoff.is_blocked(now),
        "backoff must block further starts immediately after failure"
    );

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

    // Recovery: working ExecProvider + backoff cleared (or wall-clock past block).
    let good = exec_provider(good_wrapper);
    backoff.on_success(); // operator recovery / next healthy window
    reconcile_until_done(
        &queue,
        &good,
        &worker_env,
        10,
        &[id_a, id_b],
        Duration::from_secs(120),
    )
    .await;
    assert_eq!(queue.depth().await, 0, "queue drains after provider recovery");
}

// ---------------------------------------------------------------------------
// NEGATIVE: size-class filter end-to-end
// ---------------------------------------------------------------------------

/// A large job must not be claimed by a small-capped worker; it stays queued
/// until a large-capable worker is started (via reconcile / ensure_worker).
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

    // Start only a SMALL-capped worker (env ceiling). This is the mis-sized /
    // leftover-pool case: a small machine is live while large work waits.
    // reconcile would correctly plan large — we force small here on purpose.
    let mut small_env = base_env.clone();
    small_env.insert("RIPCLONE_MAX_SIZE_CLASS".into(), "small".into());
    // Stay up long enough for the "does not claim" window; still idle-exit later.
    small_env.insert("RIPCLONE_IDLE_EXIT_SECS".into(), "15".into());

    provider
        .ensure_worker(&WorkerSpec::new("small", small_env))
        .await
        .expect("spawn small-capped worker");

    // Window where a wrongly-claiming small worker would flip the job to Done.
    let window = Duration::from_secs(5);
    let start = Instant::now();
    while start.elapsed() < window {
        assert_still_pending(&queue, id).await;
        assert_eq!(
            queue.depth().await,
            1,
            "large job must remain queued under a small-only worker"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_still_pending(&queue, id).await;

    // Large-capable path: reconcile plans large (max pending rank) and starts
    // workers with RIPCLONE_MAX_SIZE_CLASS=large → drain.
    reconcile_until_done(
        &queue,
        &provider,
        &base_env,
        10,
        &[id],
        Duration::from_secs(120),
    )
    .await;
    assert_eq!(queue.depth().await, 0, "large job drains on large-capable worker");
}
