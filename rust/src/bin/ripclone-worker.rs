//! Standalone build worker.
//!
//! Pulls sync jobs through the authenticated server API and runs them through
//! the same build path as the embedded worker. It never opens the control
//! database and never accepts a database or Turso credential.
//!
//! Env:
//! - `RIPCLONE_QUEUE_API_URL`: server base URL serving `POST /v1/jobs/*`.
//! - `RIPCLONE_METADATA_REPORT_URL`: server base URL serving ref reports.
//! - `RIPCLONE_METADATA_JOB_TOKEN`: signed bearer authenticating both APIs. A
//!   rejection exits cleanly; the server later reclaims the stale durable claim.
//! - storage env (`RIPCLONE_S3_*` or local) and provider config
//!   (`RIPCLONE_PROVIDERS` or `config.toml`).
//! - `RIPCLONE_QUEUE_STALE_SECS` (default 1800) bounds how long a crashed
//!   worker's claimed job is held before another worker reclaims it — set it
//!   above your longest build.
//! - `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` (default 7d): the worker periodically
//!   prunes `failed` jobs older than this. `done` jobs are kept as build history.
//! - `RIPCLONE_MAX_SIZE_CLASS` / `--max-size-class`: largest configured size
//!   class this worker will claim. Omit to claim everything.
//! - `RIPCLONE_IDLE_EXIT_SECS` / `--idle-exit-secs`: exit after N seconds of
//!   empty claim attempts (scale-to-zero). Off by default.
//! - `RIPCLONE_MAX_JOBS` / `--max-jobs`: exit after N builds (one-shot
//!   platforms). Off by default.
//! - Active claims are always renewed through the authenticated heartbeat API.
//!   `RIPCLONE_WORKER_HEARTBEAT` (default off) additionally registers idle workers.
//! - `RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS` (default 60): soft age-out for
//!   live-count (must exceed the interval so a healthy worker never looks dead).
//! - `RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS` (default timeout/3): how often
//!   the worker renews an active claim or refreshes an idle registry row.
//!
//! ## Topology constraints
//!
//! - **One `repo_root` per worker.** The bare git mirror under `repo_root` is
//!   per-repo scratch guarded only by an in-process lock. Two worker processes
//!   sharing a `repo_root` could `git fetch` the same mirror concurrently (the
//!   queue coalesces per *branch*, but the mirror is per *repo*) and corrupt it.
//!   Give each worker its own scratch `repo_root` (the natural farm-out layout,
//!   since each machine/Machine/Lambda has its own disk). All workers DO share
//!   the durable `StorageBackend` and `RefStore` — that is where real state lives.
//! - **Metrics are per-process.** Build metrics recorded here live on this
//!   worker, not the server; scrape workers too for full visibility.
//! - **Lifecycle is opt-in.** Without the flags the loop runs forever (today's
//!   behavior). With them a compute provider can drain-and-exit without knowing
//!   which mode it is in — both flags live in the same env bag.
//! - **Active lease renewal is unconditional.** The opt-in heartbeat setting
//!   controls idle fleet visibility only. Both use the same bearer-authenticated
//!   API and write no local control state.

use anyhow::{Context, Result, bail};
use clap::Parser;
use ripclone::api_job_queue::ApiJobQueue;
use ripclone::api_ref_store::ApiRefStore;
use ripclone::api_ref_store::ApiReportError;
use ripclone::backends::Backends;
use ripclone::metrics::Metrics;
use ripclone::queue::{
    BuildError, BuildJob, JobQueueRef, WorkerQueue, WorkerQueueRef, make_worker_id,
    validate_heartbeat_timing, worker_heartbeat_enabled_from_env, worker_heartbeat_interval_secs,
};
use ripclone::server::{ServerState, process_build_job, spawn_dedicated_claim_heartbeat_thread};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// True when an error means the worker's bearer token was rejected (401/403) on
/// the API path. The worker exits cleanly without re-minting or spinning.
fn is_queue_auth_expired(err: &anyhow::Error) -> bool {
    err.chain().any(|c| {
        c.downcast_ref::<ApiReportError>()
            .is_some_and(|e| e.is_unauthorized())
    })
}

/// Optionally register an idle worker for fleet sizing. Active jobs use their
/// own mandatory lease-renewal loop; this task deliberately skips them.
fn spawn_idle_heartbeat_loop(
    queue: WorkerQueueRef,
    worker_id: String,
    current_job: Arc<AtomicI64>,
    interval: Duration,
) {
    tokio::spawn(async move {
        loop {
            if current_job.load(Ordering::Relaxed) < 0
                && let Err(e) = queue.heartbeat(&worker_id, None).await
            {
                error!("idle worker heartbeat failed: {e:#}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}

/// Test-only: hold every Tokio worker thread with blocking work for a fixed
/// duration. Models a CPU-bound build that never yields, which is the condition
/// a runtime-scheduled claim renewal cannot survive. Off unless
/// `RIPCLONE_TESTING=1` and `RIPCLONE_TEST_SATURATE_RUNTIME_MS` are both set.
fn saturate_runtime_for_test() {
    if std::env::var("RIPCLONE_TESTING").as_deref() != Ok("1") {
        return;
    }
    let Some(hold_ms) = std::env::var("RIPCLONE_TEST_SATURATE_RUNTIME_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return;
    };
    let hold = Duration::from_millis(hold_ms);
    // Two blocking tasks per runtime worker thread, so every thread is busy
    // even if the scheduler hands two to the same worker.
    let tasks = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        * 2;
    if let Some(dir) = std::env::var_os("RIPCLONE_TEST_SATURATE_RUNTIME_DIR").map(PathBuf::from) {
        std::fs::create_dir_all(&dir).expect("create runtime saturation marker directory");
        std::fs::write(dir.join("entered"), format!("{tasks}\n"))
            .expect("signal runtime saturation");
    }
    for _ in 0..tasks {
        tokio::spawn(async move { std::thread::sleep(hold) });
    }
}

#[derive(Parser)]
#[command(name = "ripclone-worker")]
#[command(about = "Standalone API-only build worker")]
struct Args {
    #[arg(long, default_value = "/data/cache")]
    cas_dir: PathBuf,

    #[arg(long, default_value = "/data/repos")]
    repo_root: PathBuf,

    /// How long to wait before polling again when the queue is empty (ms).
    #[arg(long, default_value = "1000")]
    idle_poll_ms: u64,

    /// Largest size class this worker will claim (inclusive). Jobs above this
    /// ceiling stay queued for a bigger worker. Omit to claim everything —
    /// single-worker self-host is unchanged. Names come from the configured
    /// size classes (launch default: `small` | `large`).
    #[arg(long, env = "RIPCLONE_MAX_SIZE_CLASS")]
    max_size_class: Option<String>,

    /// Exit after the queue has been empty for N seconds (scale-to-zero).
    ///
    /// Idle-exit is atomic with claiming: the worker exits only on a claim that
    /// comes back empty after N seconds of continuous empty claims. A job that
    /// lands in the exit window is not re-checked here — the cloud reconcile
    /// cron (or the next worker start) covers it. Off by default.
    #[arg(long, env = "RIPCLONE_IDLE_EXIT_SECS")]
    idle_exit_secs: Option<u64>,

    /// Exit after N builds (one-shot platforms, e.g. Lambda). Counts each
    /// claimed job that finishes the build+ack cycle. Off by default.
    #[arg(long, env = "RIPCLONE_MAX_JOBS")]
    max_jobs: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let args = Args::parse();
    ripclone::git::require_system_git()?;
    ripclone::control::validate_worker_environment()?;
    // Complete storage parsing and client construction before creating cache
    // or mirror directories, or contacting the worker APIs.
    Backends::validate_environment()?;
    // Validate the complete token-only API configuration before creating local
    // paths or initializing artifact storage. Standalone workers have no
    // control-database mode or credential.
    let api_queue = Arc::new(
        ApiJobQueue::from_env()?
            .with_max_size_class(args.max_size_class.as_deref().map(str::to_owned)),
    );
    let ref_store = Arc::new(ApiRefStore::from_env()?);
    let queue = api_queue.clone() as WorkerQueueRef;
    let build_queue = queue.clone() as JobQueueRef;

    if !queue.supports_worker_registry() {
        bail!("server API does not support active-claim renewal");
    }
    let interval_secs = worker_heartbeat_interval_secs();
    let timeout_secs = queue.heartbeat_timeout_secs();
    validate_heartbeat_timing(interval_secs, timeout_secs)?;
    let heartbeat_interval = Duration::from_secs(interval_secs);

    std::fs::create_dir_all(&args.cas_dir)?;
    std::fs::create_dir_all(&args.repo_root)?;

    let metrics = Metrics::new();
    let b = Backends::from_env_with_ref_store(&args.cas_dir, &args.repo_root, &metrics, ref_store)
        .await?;
    b.cache_retention.clone().spawn_from_env();
    let state = ServerState::for_worker(b, build_queue, metrics)?;

    // Fleet-unique id (host/machine + pid + start nanos). PID-only collides
    // across machines and under-counts the live fleet in the registry.
    let worker_id = make_worker_id();
    let heartbeat_on = worker_heartbeat_enabled_from_env()?;
    // -1 = idle; non-negative = claimed job id. The optional idle registry
    // task reads it so it cannot overwrite active-job ownership metadata.
    let current_job = Arc::new(AtomicI64::new(-1));
    if heartbeat_on {
        info!(
            "idle worker registry enabled (interval={}s, timeout={}s)",
            heartbeat_interval.as_secs(),
            timeout_secs
        );
        spawn_idle_heartbeat_loop(
            queue.clone(),
            worker_id.clone(),
            current_job.clone(),
            heartbeat_interval,
        );
    }
    match args.max_size_class.as_deref() {
        Some(ceiling) => info!(
            "ripclone-worker {worker_id} polling server API (max-size-class={ceiling}, idle_exit_secs={:?}, max_jobs={:?}, heartbeat={heartbeat_on})",
            args.idle_exit_secs, args.max_jobs,
        ),
        None => info!(
            "ripclone-worker {worker_id} polling server API (idle_exit_secs={:?}, max_jobs={:?}, heartbeat={heartbeat_on})",
            args.idle_exit_secs, args.max_jobs,
        ),
    }

    let idle = Duration::from_millis(args.idle_poll_ms);
    // Periodically prune expired `failed` jobs (done jobs are kept as history).
    // Runs on the first iteration too, so an ephemeral worker still prunes.
    let prune_interval = Duration::from_secs(3600);
    let mut pruned_at: Option<Instant> = None;
    // Wall-clock of the first empty claim in the current idle streak. Reset on
    // every successful claim so a burst drains fully before idle-exit can fire.
    let mut idle_since: Option<Instant> = None;
    let mut jobs_done: u64 = 0;
    loop {
        let prune_due = pruned_at
            .map(|t| t.elapsed() >= prune_interval)
            .unwrap_or(true);
        if prune_due {
            match queue.prune_failed().await {
                Ok(n) if n > 0 => info!("pruned {n} expired failed jobs"),
                Ok(_) => {}
                Err(e) => error!("prune failed jobs: {e}"),
            }
            pruned_at = Some(Instant::now());
        }
        match queue.claim(&worker_id).await {
            Ok(Some(claimed)) => {
                idle_since = None;
                let job_id = claimed.id;
                current_job.store(job_id, Ordering::Relaxed);
                let repo_id = claimed.repo_id();
                info!(
                    "claimed job {} for {}@{}",
                    job_id,
                    repo_id.storage_key(),
                    claimed.admitted_commit
                );
                let admitted_commit = claimed.admitted_commit.clone();
                if let Err(e) = ripclone::validation::validate_object_id(&admitted_commit) {
                    let error = BuildError::permanent(format!(
                        "queued job has invalid admitted commit: {e}"
                    ));
                    match queue.ack(job_id, &worker_id, Err(error)).await {
                        Ok(_) => {
                            current_job.store(-1, Ordering::Relaxed);
                            jobs_done += 1;
                            continue;
                        }
                        Err(e) => {
                            error!("failed to settle invalid job {job_id}: {e:#}");
                            break;
                        }
                    }
                }
                // The server is authoritative between jobs. A previous job may
                // have populated this worker's process-local exact-result cache
                // before a later new request admitted the same commit again.
                state.ref_store.invalidate(&repo_id, &admitted_commit).await;
                // Prefer the per-job upstream credential the enqueuer persisted
                // (the cloud's per-request X-Upstream-Token, for a private repo
                // the worker has no standing credential for); fall back to the
                // broker's configured token for this provider.
                let credential = state
                    .broker
                    .fetch_credential(&repo_id, claimed.credential.as_ref())
                    .with_context(|| {
                        format!("fetch credential for queued job {}", repo_id.storage_key())
                    })?;
                let job = BuildJob {
                    repo_id: repo_id.clone(),
                    admitted_commit,
                    repo_config: claimed.repo_config,
                    credential,
                    size_bytes: None,
                };
                // Isolate the build in its own task so a panic fails just this
                // job (acked as failed) instead of killing the worker and
                // leaving the row `claimed` until the stale-reclaim timeout.
                let st = state.clone();
                let build_worker_id = worker_id.clone();
                let mut build = tokio::spawn(async move {
                    process_build_job(&st, &job, job_id, &build_worker_id).await
                });
                saturate_runtime_for_test();
                // Renew the claim from a dedicated thread with its own
                // runtime. A build that saturates this runtime must not be able
                // to starve renewal and lose a claim it still owns.
                let heartbeat_auth_expired = Arc::new(AtomicBool::new(false));
                let renewal_queue = match api_queue.for_claim_renewal() {
                    Ok(renewal_queue) => Arc::new(renewal_queue),
                    Err(error) => {
                        error!("failed to build claim renewal client for job {job_id}: {error:#}");
                        build.abort();
                        let _ = build.await;
                        bail!("claim renewal client unavailable: {error:#}");
                    }
                };
                let renewal_worker_id = worker_id.clone();
                let renewal_auth_expired = heartbeat_auth_expired.clone();
                let heartbeat = spawn_dedicated_claim_heartbeat_thread(
                    format!("ripclone-claim-{job_id}"),
                    heartbeat_interval,
                    move || {
                        let queue = renewal_queue.clone();
                        let worker_id = renewal_worker_id.clone();
                        let auth_expired = renewal_auth_expired.clone();
                        async move {
                            let renewed = queue.heartbeat(&worker_id, Some(job_id)).await;
                            if let Err(error) = &renewed
                                && is_queue_auth_expired(error)
                            {
                                auth_expired.store(true, Ordering::Relaxed);
                            }
                            renewed
                        }
                    },
                );
                let result = match heartbeat {
                    Err(error) => {
                        error!("failed to start claim heartbeat for job {job_id}: {error:#}");
                        build.abort();
                        let _ = build.await;
                        Err(BuildError::retryable(format!(
                            "claim heartbeat unavailable: {error:#}"
                        )))
                    }
                    Ok((mut heartbeat, mut heartbeat_failure)) => {
                        let result = tokio::select! {
                            joined = &mut build => {
                                match joined {
                                    Ok(result) => result,
                                    Err(error) => Err(BuildError::retryable(format!(
                                        "build task panicked: {error}"
                                    ))),
                                }
                            }
                            failure = &mut heartbeat_failure => {
                                let error = failure.unwrap_or_else(|_| {
                                    "claim heartbeat exited without reporting a result".to_string()
                                });
                                error!("active claim heartbeat failed for job {job_id}: {error}");
                                build.abort();
                                let _ = build.await;
                                Err(BuildError::retryable(format!(
                                    "durable claim lost while building: {error}"
                                )))
                            }
                        };
                        heartbeat.stop_and_join();
                        result
                    }
                };
                if heartbeat_auth_expired.load(Ordering::Relaxed) {
                    current_job.store(-1, Ordering::Relaxed);
                    info!(
                        "queue token expired (401) during active build; exiting cleanly for respawn"
                    );
                    break;
                }
                match queue.ack(job_id, &worker_id, result.map(|_| ())).await {
                    Ok(true) => {}
                    Ok(false) => warn!(
                        "job {job_id} was reclaimed (or dead-lettered) before this worker \
                         finished; claim-scoped publication rejected the stale result"
                    ),
                    Err(e) if is_queue_auth_expired(&e) => {
                        // Token expired mid-job: exit cleanly for respawn. The
                        // claim was not settled, so the server reclaims it after
                        // the stale window and a fresh worker rebuilds — no
                        // result is silently dropped.
                        info!("queue token expired (401) on ack; exiting cleanly for respawn");
                        break;
                    }
                    Err(e) => error!("failed to ack job {job_id}: {e}"),
                }
                current_job.store(-1, Ordering::Relaxed);
                jobs_done += 1;
                if let Some(max) = args.max_jobs
                    && jobs_done >= max
                {
                    info!("reached max-jobs {max}, exiting");
                    break;
                }
            }
            Ok(None) => {
                // Exit only on an empty claim after N seconds of continuous
                // emptiness. Do not exit after sleeping without re-claiming —
                // that would race a job landing in the sleep window.
                if let Some(secs) = args.idle_exit_secs {
                    let since = idle_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= Duration::from_secs(secs) {
                        info!("queue empty for {secs}s, exiting");
                        break;
                    }
                }
                tokio::time::sleep(idle).await;
            }
            Err(e) if is_queue_auth_expired(&e) => {
                // A rejected bearer cannot be refreshed locally. Exit cleanly;
                // the server will reclaim the stale durable claim.
                info!("queue token expired (401) on claim; exiting cleanly for respawn");
                break;
            }
            Err(e) => {
                // Claim errors are not empty claims — don't start/advance idle
                // exit, and don't count toward max-jobs. Fail loudly, poll again.
                error!("claim failed: {e}");
                tokio::time::sleep(idle).await;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Args;
    use clap::Parser;

    /// Set `key=value` for the duration of `f`, restoring the previous value
    /// (or removing the var) afterwards. Env mutation is `unsafe` in Rust 2024.
    fn with_env<T>(key: &str, value: &str, f: impl FnOnce() -> T) -> T {
        let previous = std::env::var(key).ok();
        unsafe { std::env::set_var(key, value) };
        let result = f();
        match previous {
            Some(previous) => unsafe { std::env::set_var(key, previous) },
            None => unsafe { std::env::remove_var(key) },
        }
        result
    }

    /// Parse with no CLI args at all; every value must come from env.
    fn parse_env_only() -> Args {
        Args::try_parse_from(["ripclone-worker"]).expect("parse from env only")
    }

    #[test]
    fn max_size_class_from_env() {
        let args = with_env("RIPCLONE_MAX_SIZE_CLASS", "large", parse_env_only);
        assert_eq!(args.max_size_class.as_deref(), Some("large"));
    }

    #[test]
    fn idle_exit_secs_from_env() {
        let args = with_env("RIPCLONE_IDLE_EXIT_SECS", "42", parse_env_only);
        assert_eq!(args.idle_exit_secs, Some(42));
    }

    #[test]
    fn max_jobs_from_env() {
        let args = with_env("RIPCLONE_MAX_JOBS", "7", parse_env_only);
        assert_eq!(args.max_jobs, Some(7));
    }
}
