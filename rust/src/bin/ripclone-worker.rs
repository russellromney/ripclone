//! Standalone build worker.
//!
//! Pulls sync jobs from the SQL queue and runs them through the same
//! `process_build_job` the in-process worker uses. Because all durable state
//! lives in shared storage + the metadata store, this can run anywhere that
//! shares the same `RIPCLONE_QUEUE_DB_URL`, storage, and repo root — another
//! machine, a Fly Machine, a container, etc.
//!
//! Env:
//! - `RIPCLONE_QUEUE` = `sqlite` (local file) | `postgres` | `mysql` (network db)
//!   | `libsql` (remote Turso Cloud). Must match the server.
//! - `RIPCLONE_QUEUE_DB_URL` (required): a local path for sqlite; a
//!   `postgres://` / `mysql://` url for those; a `libsql://` url for libsql
//!   (with `RIPCLONE_QUEUE_DB_TOKEN`).
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

use anyhow::{Context, Result};
use clap::Parser;
use ripclone::backends::{self, Backends};
use ripclone::metrics::Metrics;
use ripclone::queue::{BuildError, BuildJob, JobQueue, JobQueueRef, JobState};
use ripclone::server::{ServerState, mark_branch_build_failed, process_build_job};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

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

    let queue = backends::connect_sql_queue().await?;
    let queue = Arc::new(
        queue
            .with_max_size_class(args.max_size_class.as_deref())
            .context("resolve --max-size-class")?,
    );

    let metrics = Metrics::new();
    let b = Backends::from_env(&args.cas_dir, &args.repo_root, &metrics).await?;
    let state = ServerState::for_worker(b, queue.clone() as JobQueueRef, metrics)?;

    let worker_id = format!("worker-{}", std::process::id());
    match args.max_size_class.as_deref() {
        Some(ceiling) => info!(
            "ripclone-worker {worker_id} polling {} queue (max-size-class={ceiling}, idle_exit_secs={:?}, max_jobs={:?})",
            backends::queue_kind(),
            args.idle_exit_secs,
            args.max_jobs,
        ),
        None => info!(
            "ripclone-worker {worker_id} polling {} queue (idle_exit_secs={:?}, max_jobs={:?})",
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
                    Err(e) => error!("failed to ack job {job_id}: {e}"),
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
