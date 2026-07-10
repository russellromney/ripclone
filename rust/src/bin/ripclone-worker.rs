//! Standalone build worker.
//!
//! Pulls sync jobs from the queue and runs them through the same
//! `process_build_job` the in-process worker uses. Because all durable state
//! lives in shared storage + the metadata store, this can run anywhere — another
//! machine, a Fly Machine, a container, etc. Two claim paths: a direct SQL
//! connection (trusted single-box) or, for farm-out on untrusted infra, the
//! token-only `api` queue (claim/ack/heartbeat over HTTP, **no** DB credentials).
//!
//! Env:
//! - `RIPCLONE_QUEUE` = `api` (token-only farm-out, no DB creds) | `sqlite`
//!   (local file) | `postgres` | `mysql` (network db) | `libsql` (remote Turso
//!   Cloud). The SQL kinds hold a direct DB connection (trusted single-box); the
//!   SQL kinds' url comes from `RIPCLONE_QUEUE_DB_URL` (+ `RIPCLONE_QUEUE_DB_TOKEN`
//!   for libsql). `api` holds **no** database credentials.
//! - For `RIPCLONE_QUEUE=api`: `RIPCLONE_QUEUE_API_URL` (the server base URL that
//!   serves `POST /v1/jobs/*`) + `RIPCLONE_METADATA_JOB_TOKEN` (the one signed,
//!   expiring bearer token that authenticates claim/ack/heartbeat **and** the
//!   `api` metadata reports). Pair with `RIPCLONE_METADATA=api`. A 401 → the
//!   worker exits cleanly so the dispatcher respawns it with a fresh token.
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
//! - `RIPCLONE_WORKER_HEARTBEAT` (default off): when set to `queue` (or the
//!   queue DSN / a truthy `1`/`true`), the worker writes a row into the queue
//!   DB's `workers` registry so a dispatcher autoscaler can count live
//!   workers. Self-hosters without a dispatcher leave this unset — the worker
//!   is byte-for-byte unchanged.
//! - `RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS` (default 60): soft age-out for
//!   live-count (must exceed the interval so a healthy worker never looks dead).
//! - `RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS` (default timeout/3): how often
//!   the worker refreshes its registry row (including mid-build).
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
//! - **Heartbeat is opt-in.** Off by default so single-worker self-host never
//!   touches the registry. Enable only when a dispatcher (or anything else)
//!   needs live-worker visibility.

use anyhow::{Context, Result, bail};
use clap::Parser;
use ripclone::api_ref_store::ApiReportError;
use ripclone::backends::{self, Backends, WorkerQueueBackend};
use ripclone::metrics::Metrics;
use ripclone::queue::{
    BuildError, BuildJob, JobQueueRef, JobState, WorkerQueueRef, make_worker_id,
    validate_heartbeat_timing, worker_heartbeat_enabled_from_env, worker_heartbeat_interval_secs,
};
use ripclone::server::{ServerState, mark_branch_build_failed, process_build_job};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// True when an error means the worker's bearer token was rejected (401/403) on
/// the `api` queue path. The worker then exits cleanly so the dispatcher can
/// respawn it with a fresh token — no worker-side re-mint, no spin.
fn is_queue_auth_expired(err: &anyhow::Error) -> bool {
    err.chain().any(|c| {
        c.downcast_ref::<ApiReportError>()
            .is_some_and(|e| e.is_unauthorized())
    })
}

/// Spawn a background task that periodically upserts this worker's registry
/// row. `current_job` is `-1` when idle, else the claimed job id.
fn spawn_heartbeat_loop(
    queue: WorkerQueueRef,
    worker_id: String,
    current_job: Arc<AtomicI64>,
    interval: Duration,
) {
    tokio::spawn(async move {
        loop {
            let job = match current_job.load(Ordering::Relaxed) {
                n if n < 0 => None,
                id => Some(id),
            };
            if let Err(e) = queue.heartbeat(&worker_id, job).await {
                // Fail loudly in logs; keep trying so a transient DB blip does
                // not permanently hide a live worker from the autoscaler.
                error!("worker heartbeat failed: {e:#}");
            }
            tokio::time::sleep(interval).await;
        }
    });
}

#[derive(Parser)]
#[command(name = "ripclone-worker")]
#[command(about = "Standalone build worker: pulls sync jobs from the SQL queue")]
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
    std::fs::create_dir_all(&args.cas_dir)?;
    std::fs::create_dir_all(&args.repo_root)?;

    // The worker claims through either a direct SQL connection (trusted
    // single-box) or the token-only HTTP `ApiJobQueue` (farm-out). Both back the
    // same worker loop via the `WorkerQueue` trait; the api queue also serves as
    // the `build_queue` (JobQueueRef) for the ServerState.
    let (queue, build_queue): (WorkerQueueRef, JobQueueRef) =
        match backends::connect_worker_queue(args.max_size_class.as_deref()).await? {
            WorkerQueueBackend::Sql(q) => (q.clone() as WorkerQueueRef, q as JobQueueRef),
            WorkerQueueBackend::Api(q) => (q.clone() as WorkerQueueRef, q as JobQueueRef),
        };

    let metrics = Metrics::new();
    let b = Backends::from_env(&args.cas_dir, &args.repo_root, &metrics).await?;
    let state = ServerState::for_worker(b, build_queue, metrics)?;

    // Fleet-unique id (host/machine + pid + start nanos). PID-only collides
    // across machines and under-counts the live fleet in the registry.
    let worker_id = make_worker_id();
    let heartbeat_on = worker_heartbeat_enabled_from_env()?;
    // -1 = idle; non-negative = claimed job id. Background heartbeat task reads it.
    // Only allocated when heartbeat is on so the disabled path stays lean.
    let current_job = heartbeat_on.then(|| Arc::new(AtomicI64::new(-1)));
    if heartbeat_on {
        if !queue.supports_worker_registry() {
            bail!(
                "RIPCLONE_WORKER_HEARTBEAT is set but RIPCLONE_QUEUE={} does not \
                 support the workers registry (need sqlite or libsql; postgres/mysql lag)",
                backends::queue_kind()
            );
        }
        let interval_secs = worker_heartbeat_interval_secs();
        let timeout_secs = queue.heartbeat_timeout_secs();
        validate_heartbeat_timing(interval_secs, timeout_secs)?;
        let interval = Duration::from_secs(interval_secs);
        info!(
            "worker heartbeat enabled (interval={}s, timeout={}s)",
            interval.as_secs(),
            timeout_secs
        );
        spawn_heartbeat_loop(
            queue.clone(),
            worker_id.clone(),
            current_job.clone().expect("heartbeat current_job"),
            interval,
        );
    }
    match args.max_size_class.as_deref() {
        Some(ceiling) => info!(
            "ripclone-worker {worker_id} polling {} queue (max-size-class={ceiling}, idle_exit_secs={:?}, max_jobs={:?}, heartbeat={heartbeat_on})",
            backends::queue_kind(),
            args.idle_exit_secs,
            args.max_jobs,
        ),
        None => info!(
            "ripclone-worker {worker_id} polling {} queue (idle_exit_secs={:?}, max_jobs={:?}, heartbeat={heartbeat_on})",
            backends::queue_kind(),
            args.idle_exit_secs,
            args.max_jobs,
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
                // Surface the active claim to the heartbeat task (if any).
                if let Some(ref cur) = current_job {
                    cur.store(job_id, Ordering::Relaxed);
                }
                let repo_id = claimed.repo_id();
                info!(
                    "claimed job {} for {}@{}",
                    job_id,
                    repo_id.storage_key(),
                    claimed.branch
                );
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
                let branch = claimed.branch.clone();
                let job = BuildJob {
                    repo_id: repo_id.clone(),
                    branch: branch.clone(),
                    rev: None,
                    credential,
                    // The SQL queue does not persist the re-check counter; a
                    // cross-process worker starts each claimed job fresh and the
                    // periodic poller is the freshness backstop here.
                    recheck: 0,
                    size_bytes: None,
                };
                // Isolate the build in its own task so a panic fails just this
                // job (acked as failed) instead of killing the worker and
                // leaving the row `claimed` until the stale-reclaim timeout.
                let st = state.clone();
                let result =
                    match tokio::spawn(async move { process_build_job(&st, &job).await }).await {
                        Ok(r) => r,
                        Err(e) => Err(BuildError::retryable(format!("build task panicked: {e}"))),
                    };
                // Retryable errors leave metadata non-terminal (so intermediate
                // retries don't look permanent). If ack dead-letters at the
                // attempts cap, surface that as a terminal failed status.
                let maybe_retryable_msg = result
                    .as_ref()
                    .err()
                    .filter(|e| e.is_retryable())
                    .map(|e| e.message().to_string());
                match queue.ack(job_id, &worker_id, result.map(|_| ())).await {
                    Ok(true) => {
                        // Only when the build error was retryable: permanent
                        // failures already wrote terminal metadata in
                        // process_build_job. Dead-letter at the attempts cap
                        // is the case that still needs a terminal write.
                        if maybe_retryable_msg.is_some()
                            && let Ok(JobState::Failed(err)) = queue.job_status(job_id).await
                            && let Err(e) =
                                mark_branch_build_failed(&state, &repo_id, &branch, &err).await
                        {
                            error!(
                                "failed to mark {}@{} terminal after dead-letter: {e:#}",
                                repo_id.storage_key(),
                                branch
                            );
                        }
                    }
                    Ok(false) => warn!(
                        "job {job_id} was reclaimed (or dead-lettered) before this worker \
                         finished; discarding its build result"
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
                if let Some(ref cur) = current_job {
                    cur.store(-1, Ordering::Relaxed);
                }
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
                // On the api queue path a 401 means the bearer token expired.
                // Exit cleanly (code 0) so the dispatcher respawns this worker
                // with a fresh token; no worker-side re-mint, no spin.
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
