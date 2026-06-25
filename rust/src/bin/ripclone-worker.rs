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
//! - storage env (`RIPCLONE_S3_*` or local) and `RIPCLONE_GITHUB_TOKEN`.
//! - `RIPCLONE_QUEUE_STALE_SECS` (default 1800) bounds how long a crashed
//!   worker's claimed job is held before another worker reclaims it — set it
//!   above your longest build.
//! - `RIPCLONE_QUEUE_FAILED_RETENTION_SECS` (default 7d): the worker periodically
//!   prunes `failed` jobs older than this. `done` jobs are kept as build history.
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

use anyhow::Result;
use clap::Parser;
use ripclone::backends::{self, Backends};
use ripclone::metrics::Metrics;
use ripclone::queue::{BuildJob, JobQueueRef};
use ripclone::server::{ServerState, process_build_job};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info};
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let args = Args::parse();
    std::fs::create_dir_all(&args.cas_dir)?;
    std::fs::create_dir_all(&args.repo_root)?;

    let queue = Arc::new(backends::connect_sql_queue().await?);

    let metrics = Metrics::new();
    let b = Backends::from_env(&args.cas_dir, &args.repo_root, &metrics).await?;
    let state = ServerState::for_worker(b, queue.clone() as JobQueueRef, metrics)?;

    let worker_id = format!("worker-{}", std::process::id());
    info!(
        "ripclone-worker {worker_id} polling {} queue",
        backends::queue_kind()
    );

    let idle = Duration::from_millis(args.idle_poll_ms);
    // Periodically prune expired `failed` jobs (done jobs are kept as history).
    // Runs on the first iteration too, so an ephemeral worker still prunes.
    let prune_interval = Duration::from_secs(3600);
    let mut pruned_at: Option<Instant> = None;
    loop {
        let prune_due = pruned_at.map(|t| t.elapsed() >= prune_interval).unwrap_or(true);
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
                let job_id = claimed.id;
                let repo_id = claimed.repo_id();
                info!(
                    "claimed job {} for {}@{}",
                    job_id,
                    repo_id.storage_key(),
                    claimed.branch
                );
                // The cross-process queue never carries credentials; the worker
                // resolves its own from the credential broker for this repo.
                let credential = state.broker.fetch_credential(&repo_id, None);
                let job = BuildJob {
                    repo_id,
                    branch: claimed.branch,
                    rev: None,
                    credential,
                };
                // Isolate the build in its own task so a panic fails just this
                // job (acked as failed) instead of killing the worker and
                // leaving the row `claimed` until the stale-reclaim timeout.
                let st = state.clone();
                let result = match tokio::spawn(async move {
                    process_build_job(&st, &job).await
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => Err(format!("build task panicked: {e}")),
                };
                if let Err(e) = queue.ack(job_id, result).await {
                    error!("failed to ack job {job_id}: {e}");
                }
            }
            Ok(None) => tokio::time::sleep(idle).await,
            Err(e) => {
                error!("claim failed: {e}");
                tokio::time::sleep(idle).await;
            }
        }
    }
}
