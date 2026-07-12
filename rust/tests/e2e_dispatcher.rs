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
use std::process::{Command, Stdio};
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
            initialization_attempt_id: None,
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
    // The dispatcher no longer auto-forwards DB creds — farm-out workers are
    // token-only (see WORKER_ENV_KEYS). These tests exercise the dispatcher's
    // reconcile/size-class mechanics with real DIRECT-SQL workers, so they inject
    // the queue DB URL explicitly. Production farm-out uses the api queue instead
    // (covered by e2e_token_only_worker).
    let db_url =
        std::env::var("RIPCLONE_QUEUE_DB_URL").expect("test sets RIPCLONE_QUEUE_DB_URL in setup");
    env.insert("RIPCLONE_QUEUE_DB_URL".into(), db_url);
    // Drain cleanly so the test does not leave forever-workers around.
    env.insert("RIPCLONE_IDLE_EXIT_SECS".into(), "2".into());
    // Live registry so multi-pass reconcile (and size-class proofs) see started workers.
    env.insert("RIPCLONE_WORKER_HEARTBEAT".into(), "queue".into());
    // First heartbeat is immediate; short interval keeps the registry fresh in tests.
    env.insert("RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS".into(), "1".into());
    env.insert("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS".into(), "30".into());
    env
}

/// Like [`write_worker_wrapper`], but first records its own pid to `pidfile`
/// before exec'ing the real worker. `exec` replaces the process image without
/// forking, so the pid recorded here IS the real worker's pid for its whole
/// life — the reaper test uses it to SIGKILL the exact worker that claimed
/// the job (a hard crash mid-build, no ack, ever).
fn write_worker_wrapper_with_pidfile(
    dir: &Path,
    worker_bin: &Path,
    cas_dir: &Path,
    repo_root: &Path,
    pidfile: &Path,
) -> PathBuf {
    let path = dir.join("launch-worker-doomed.sh");
    let script = format!(
        r#"#!/bin/sh
# Reaper e2e helper: record this process's pid, then exec into the real
# worker (same pid, new image) so the test can hard-kill it mid-build.
echo $$ > "{pidfile}"
exec "{worker}" --cas-dir "{cas}" --repo-root "{repos}" --idle-poll-ms 100
"#,
        pidfile = pidfile.display(),
        worker = worker_bin.display(),
        cas = cas_dir.display(),
        repos = repo_root.display(),
    );
    std::fs::write(&path, &script).expect("write doomed wrapper");
    chmod_x(&path);
    path
}

fn exec_provider(wrapper: PathBuf) -> ExecProvider {
    ExecProvider::new(ExecProviderConfig {
        program: wrapper,
        fixed_args: Vec::<OsString>::new(),
    })
    .expect("ExecProvider")
}

/// Poll job statuses until every id is Done (or timeout). Used when an external
/// process (e.g. `ripclone-dispatcher`) owns the reconcile loop.
async fn wait_all_done(queue: &SqlJobQueue, job_ids: &[i64], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut all_done = true;
        for &id in job_ids {
            match queue.job_status(id).await.expect("status") {
                JobState::Done => {}
                JobState::Failed(e) => panic!("job {id} failed: {e}"),
                JobState::Pending | JobState::Unknown => all_done = false,
            }
        }
        if all_done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "jobs {job_ids:?} did not finish within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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
            let wait = backoff
                .remaining(Instant::now())
                .max(Duration::from_millis(200));
            tokio::time::sleep(wait).await;
        } else {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Real `ripclone-dispatcher` binary (infinite poll loop). Kill on drop.
struct DispatcherProc(std::process::Child);

impl Drop for DispatcherProc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn the product binary with exec dispatch → launch-script workers.
/// Worker bag keys are set on the child so `collect_worker_env` inside the
/// binary forwards them (same path as production).
fn spawn_dispatcher_binary(
    wrapper: &Path,
    max_workers: usize,
    interval_secs: u64,
) -> DispatcherProc {
    let mut cmd = Command::new(cargo_bin("ripclone-dispatcher"));
    cmd.env("RIPCLONE_DISPATCH", "exec")
        .env("RIPCLONE_DISPATCH_CMD", wrapper)
        .env("RIPCLONE_DISPATCH_INTERVAL_SECS", interval_secs.to_string())
        .env("RIPCLONE_DISPATCH_MAX_WORKERS", max_workers.to_string())
        // Forwarded into WorkerSpec.env by the binary's collect_worker_env().
        .env("RIPCLONE_IDLE_EXIT_SECS", "2")
        .env("RIPCLONE_WORKER_HEARTBEAT", "queue")
        .env("RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS", "1")
        .env("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS", "30")
        // Inherit queue URL / origin base / trust from the test process env.
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let child = cmd.spawn().expect("spawn ripclone-dispatcher binary");
    DispatcherProc(child)
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
    assert_eq!(
        first.plan.total_pending, 3,
        "plan sees all pending: {first:?}"
    );
    assert_eq!(first.plan.desired, 3, "desired tracks pending: {first:?}");
    assert_eq!(
        first.plan.to_start, 3,
        "no live workers yet → start 3: {first:?}"
    );
    assert_eq!(
        first.started, 3,
        "ExecProvider must spawn all planned workers: {first:?}"
    );
    assert_eq!(
        first.failed, 0,
        "first ensure_worker batch must not fail: {first:?}"
    );
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

/// Same drain proof under `max_workers=1`: first pass starts only one worker,
/// then the queue still fully drains (serial / cap-bound path). Complements the
/// parallel path above — does not replace it.
#[tokio::test]
async fn dispatcher_reconcile_max_workers_one_serial_drain() {
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

    let _o1 = publish_origin("acme", "disp-serial-a", "a.txt", "a\n");
    let _o2 = publish_origin("acme", "disp-serial-b", "b.txt", "b\n");
    let _o3 = publish_origin("acme", "disp-serial-c", "c.txt", "c\n");

    let queue = backends::connect_sql_queue().await.expect("queue");
    let id_a = enqueue_sized(&queue, "acme/disp-serial-a", Some(SMALL_BYTES)).await;
    let id_b = enqueue_sized(&queue, "acme/disp-serial-b", Some(SMALL_BYTES)).await;
    let id_c = enqueue_sized(&queue, "acme/disp-serial-c", Some(SMALL_BYTES)).await;
    assert_eq!(queue.depth().await, 3);

    let mut backoff = BackoffState::new();
    let first = reconcile_once(ReconcileInputs {
        queue: &queue,
        provider: &provider,
        max_workers: 1,
        worker_env: &worker_env,
        backoff: &mut backoff,
        now: Instant::now(),
    })
    .await
    .expect("first reconcile under cap");
    assert_eq!(first.plan.total_pending, 3, "{first:?}");
    assert_eq!(
        first.plan.desired, 1,
        "cap must bind desired to max_workers=1: {first:?}"
    );
    assert_eq!(
        first.plan.to_start, 1,
        "start exactly one under cap: {first:?}"
    );
    assert_eq!(first.started, 1, "must not spawn past the cap: {first:?}");
    assert_eq!(first.failed, 0, "{first:?}");

    // One live worker (or successive one-at-a-time starts) must still drain N jobs.
    reconcile_until_done(
        &queue,
        &provider,
        &worker_env,
        1,
        &[id_a, id_b, id_c],
        Duration::from_secs(180),
        &mut backoff,
    )
    .await;
    assert_eq!(queue.depth().await, 0);
    for id in [id_a, id_b, id_c] {
        match queue.job_status(id).await.expect("status") {
            JobState::Done => {}
            other => panic!("job {id} expected Done under max_workers=1, got {other:?}"),
        }
    }
}

/// Product binary wiring: real `ripclone-dispatcher` with `RIPCLONE_DISPATCH=exec`
/// polls, starts workers via the launch script, and drains the queue.
#[tokio::test]
async fn dispatcher_binary_exec_drains_queue() {
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

    let _o1 = publish_origin("acme", "disp-bin-a", "a.txt", "a\n");
    let _o2 = publish_origin("acme", "disp-bin-b", "b.txt", "b\n");

    let queue = backends::connect_sql_queue().await.expect("queue");
    let id_a = enqueue_sized(&queue, "acme/disp-bin-a", Some(SMALL_BYTES)).await;
    let id_b = enqueue_sized(&queue, "acme/disp-bin-b", Some(SMALL_BYTES)).await;
    assert_eq!(queue.depth().await, 2);

    // Binary owns the poll loop (interval=1s). Kill on drop after drain.
    let _dispatcher =
        spawn_dispatcher_binary(&wrapper, /*max_workers=*/ 5, /*interval_secs=*/ 1);

    wait_all_done(&queue, &[id_a, id_b], Duration::from_secs(120)).await;
    assert_eq!(
        queue.depth().await,
        0,
        "binary dispatcher must drain the real queue"
    );
}

/// Binary no-op path: `RIPCLONE_DISPATCH=none` exits cleanly without starting
/// workers (static-pool self-host). Jobs stay queued — proves the binary
/// honors the documented off switch.
#[tokio::test]
async fn dispatcher_binary_none_is_noop() {
    let _guard = SERIAL.lock().await;
    let _qdir = setup_sqlite_queue();
    let _server = start_server().await;

    let _origin = publish_origin("acme", "disp-none", "a.txt", "a\n");
    let queue = backends::connect_sql_queue().await.expect("queue");
    let id = enqueue_sized(&queue, "acme/disp-none", Some(SMALL_BYTES)).await;
    assert_eq!(queue.depth().await, 1);

    let out = Command::new(cargo_bin("ripclone-dispatcher"))
        .env("RIPCLONE_DISPATCH", "none")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .expect("run ripclone-dispatcher none");
    assert!(
        out.status.success(),
        "RIPCLONE_DISPATCH=none must exit 0, got {:?}",
        out.status
    );

    // Still queued — no workers started by the no-op binary.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(queue.depth().await, 1, "none must not dequeue work");
    match queue.job_status(id).await.expect("status") {
        JobState::Pending => {}
        other => panic!("job must stay pending under none, got {other:?}"),
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
    assert_eq!(
        queue.depth().await,
        0,
        "queue drains after provider recovery"
    );
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
    assert_eq!(
        queue.depth().await,
        0,
        "large job drains on large-capable worker"
    );
    match queue.job_status(id).await.expect("status") {
        JobState::Done => {}
        other => panic!("large job expected Done, got {other:?}"),
    }
}

/// INTEGRATED provider e2e (Model B): the assembled path — `reconcile_once`
/// drives a REAL `ExecProvider` that starts a REAL `ripclone-worker`, which
/// claims + builds + acks entirely over the server's HTTP API
/// (`RIPCLONE_QUEUE=api`), and the refs land via `POST /v1/refs`. This is the
/// seam the worker-level e2e can't cover: a real provider forwarding the api env
/// + provisioned token to a real worker.
///
/// The drain PROVES the server's `/v1/jobs/*` endpoints were hit — with
/// `RIPCLONE_QUEUE=api` there is no SQL fallback, so a Done job means claim+ack
/// went over HTTP. Reaching Done also proves the metadata API path: an
/// `ApiRefStore` report failure is non-swallowed, so a failed `POST /v1/refs`
/// would fail the build (job Failed), not Done.
///
/// (exec inherits the parent env, which here still holds the sqlite DB URL — that
/// is fine for the trusted local escape hatch; `RIPCLONE_QUEUE=api` ignores it.
/// The Fly no-DB guarantee is provisioning, covered by the WORKER_ENV_KEYS unit
/// test.)
#[tokio::test]
async fn dispatcher_reconcile_drains_workers_over_api_queue() {
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

    // Operator-provisioned durable worker token (Model B): mint once, forward it.
    let token = {
        let secret = ripclone::job_token::report_token_secret_from_env()
            .expect("job token secret (RIPCLONE_SERVER_TOKEN set by init)");
        ripclone::job_token::mint_job_token(&secret, Duration::from_secs(3600))
            .expect("mint worker token")
    };

    // Token-only farm-out env: claim + report over the server's HTTP API, no DB.
    let mut worker_env: BTreeMap<String, String> = BTreeMap::new();
    worker_env.insert("RIPCLONE_QUEUE".into(), "api".into());
    worker_env.insert("RIPCLONE_QUEUE_API_URL".into(), server.url.clone());
    worker_env.insert("RIPCLONE_METADATA".into(), "api".into());
    worker_env.insert(
        "RIPCLONE_METADATA_REPORT_URL".into(),
        format!("{}/v1/refs", server.url),
    );
    worker_env.insert("RIPCLONE_METADATA_JOB_TOKEN".into(), token);
    worker_env.insert("RIPCLONE_IDLE_EXIT_SECS".into(), "3".into());
    // Heartbeat over the API so the dispatcher's live count sees these workers
    // (the api worker POSTs /v1/jobs/heartbeat; the server writes its registry,
    // which the dispatcher reads) and reconcile converges instead of over-spawning.
    worker_env.insert("RIPCLONE_WORKER_HEARTBEAT".into(), "queue".into());
    worker_env.insert("RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS".into(), "1".into());
    worker_env.insert("RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS".into(), "30".into());
    // Hermetic proof of the API path: the worker inherits the test's real
    // `RIPCLONE_QUEUE_DB_URL` (process-global from `setup_sqlite_queue`). Overlay a
    // fresh, EMPTY decoy DB so that IF `RIPCLONE_QUEUE=api` ever failed to apply,
    // the worker would fall back to the empty decoy and drain NOTHING (test fails)
    // rather than silently draining the real queue via SQL (false positive). In
    // the normal case `RIPCLONE_QUEUE=api` never reads a DB URL, so the decoy is
    // inert and the drain goes over `/v1/jobs/*`. Metadata decoy for symmetry.
    let decoy_dir = tempfile::tempdir().expect("decoy dir");
    worker_env.insert(
        "RIPCLONE_QUEUE_DB_URL".into(),
        decoy_dir
            .path()
            .join("decoy-queue.db")
            .to_string_lossy()
            .into_owned(),
    );
    worker_env.insert(
        "RIPCLONE_METADATA_DB_URL".into(),
        decoy_dir
            .path()
            .join("decoy-meta.db")
            .to_string_lossy()
            .into_owned(),
    );

    let _o1 = publish_origin("acme", "api-disp-a", "a.txt", "a\n");
    let _o2 = publish_origin("acme", "api-disp-b", "b.txt", "b\n");

    let queue = backends::connect_sql_queue().await.expect("queue");
    let id_a = enqueue_sized(&queue, "acme/api-disp-a", Some(SMALL_BYTES)).await;
    let id_b = enqueue_sized(&queue, "acme/api-disp-b", Some(SMALL_BYTES)).await;
    assert_eq!(queue.depth().await, 2);

    // One worker drains both jobs serially — proves the API path without piling
    // pollers on the server's rate limiter.
    let mut backoff = BackoffState::new();
    reconcile_until_done(
        &queue,
        &provider,
        &worker_env,
        /*max_workers=*/ 1,
        &[id_a, id_b],
        Duration::from_secs(120),
        &mut backoff,
    )
    .await;
    assert_eq!(
        queue.depth().await,
        0,
        "a real worker must drain the queue over the HTTP API (RIPCLONE_QUEUE=api)"
    );
}

// ---------------------------------------------------------------------------
// REAPER: stale-claim self-heal on reconcile
// ---------------------------------------------------------------------------

/// Reaper on reconcile: a job stuck `claimed` by a dead worker on an
/// otherwise-idle queue must be reclaimed by the dispatcher's OWN reconcile
/// loop — no new enqueue, no human action. Before this fix, `reclaim_stale`
/// only ran when a worker claimed (`queue::sql::claim_capped`); an idle queue
/// (nothing `queued`) never triggers a claim, so a SIGKILLed worker's job was
/// stranded forever: 0 queued depth -> dispatcher starts no worker -> nobody
/// claims -> nobody reclaims.
///
/// Real seam, both halves: a REAL `ripclone-worker` process (via
/// `ExecProvider`) claims the job and is then SIGKILLed mid-build (no ack,
/// ever); recovery then runs ONLY through `reconcile_once` calls against a
/// healthy provider — the same call the real dispatcher poll loop makes.
///
/// Prove-it: remove `input.queue.reclaim_stale().await?` from
/// `reconcile_once` (`src/dispatch/autoscale.rs`) and this test times out —
/// the claimed row never flips back to `queued`, `pending_by_class` stays
/// empty forever, and no replacement worker is ever started.
#[tokio::test]
async fn dispatcher_reaper_reclaims_dead_worker_on_reconcile() {
    let _guard = SERIAL.lock().await;
    let _qdir = setup_sqlite_queue();
    // Short stale window: the killed worker's claim must become
    // reclaim-eligible quickly (test timeout), while staying long enough that
    // the REAL healthy worker's own claim->build->ack cycle for a trivial
    // repo (well under a second) is never caught by it.
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE_STALE_SECS", "1");
    }
    let server = start_server().await;
    let wrapper_dir = tempfile::tempdir().expect("wrapper dir");
    let pidfile = wrapper_dir.path().join("worker.pid");
    let doomed_wrapper = write_worker_wrapper_with_pidfile(
        wrapper_dir.path(),
        &cargo_bin("ripclone-worker"),
        &server.cas_dir,
        &server.repo_root,
        &pidfile,
    );
    let good_wrapper = write_worker_wrapper(
        wrapper_dir.path(),
        &cargo_bin("ripclone-worker"),
        &server.cas_dir,
        &server.repo_root,
    );
    let worker_env = local_worker_env();

    let _origin = publish_origin("acme", "reaper-a", "a.txt", "a\n");
    let queue = backends::connect_sql_queue().await.expect("queue");
    let id = enqueue_sized(&queue, "acme/reaper-a", Some(SMALL_BYTES)).await;
    assert_eq!(queue.depth().await, 1, "one job queued before reconcile");

    // Reconcile pass #1: real seam via ExecProvider starts the doomed worker.
    let doomed_provider = exec_provider(doomed_wrapper);
    let mut backoff = BackoffState::new();
    let first = reconcile_once(ReconcileInputs {
        queue: &queue,
        provider: &doomed_provider,
        max_workers: 1,
        worker_env: &worker_env,
        backoff: &mut backoff,
        now: Instant::now(),
    })
    .await
    .expect("first reconcile starts the doomed worker");
    assert_eq!(first.started, 1, "doomed worker must start: {first:?}");

    // Wait for the REAL claim: `depth()` drops to 0 only via the atomic claim
    // UPDATE in `try_claim`, so this is not a timing guess. Then hard-kill
    // that exact worker pid — a crash mid-build, no ack, ever.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if queue.depth().await == 0 && pidfile.exists() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "doomed worker never claimed the job within 30s"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_still_pending(&queue, id).await;
    let pid: i32 = std::fs::read_to_string(&pidfile)
        .expect("read pidfile")
        .trim()
        .parse()
        .expect("pidfile must contain a pid");
    let rc = unsafe { libc::kill(pid, libc::SIGKILL) };
    assert_eq!(
        rc,
        0,
        "SIGKILL the doomed worker (pid {pid}): {}",
        std::io::Error::last_os_error()
    );

    // Confirm the crash landed: row stuck `claimed`, no ack, queue LOOKS idle
    // (nothing `queued`) — exactly the stranded-claim scenario this fix
    // closes. Give the killed process a moment to actually exit.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        queue.depth().await,
        0,
        "queue looks idle: the stuck claim is not counted as queued"
    );
    assert_still_pending(&queue, id).await;

    // NO new enqueue, no human action from here: only the dispatcher's own
    // reconcile loop (real `reconcile_once`, called repeatedly exactly like
    // the production poll loop) against a WORKING provider drives recovery.
    let good_provider = exec_provider(good_wrapper);
    reconcile_until_done(
        &queue,
        &good_provider,
        &worker_env,
        1,
        &[id],
        Duration::from_secs(60),
        &mut backoff,
    )
    .await;

    assert_eq!(queue.depth().await, 0, "queue drains after self-heal");
    match queue.job_status(id).await.expect("status") {
        JobState::Done => {}
        other => panic!("reaped job expected Done, got {other:?}"),
    }
}
