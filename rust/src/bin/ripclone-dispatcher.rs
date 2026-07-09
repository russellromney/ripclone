//! Standalone OSS dispatcher: depth-based autoscale loop for build workers.
//!
//! Polls the SQL job queue on an interval, computes how many workers the
//! pending depth needs (capped by `RIPCLONE_DISPATCH_MAX_WORKERS`), and wakes
//! them through the active [`ripclone::dispatch::ComputeProvider`]. The cloud
//! only enqueues and reads status; this binary is the only thing that
//! dispatches.
//!
//! Env:
//! - `RIPCLONE_DISPATCH` = `fly` | `exec` | `http` | `mock` | `none`
//!   (existing selector). `none` / unset → no-op exit (self-host with a static
//!   worker pool needs no dispatcher).
//! - `RIPCLONE_DISPATCH_INTERVAL_SECS` (default 5): reconcile poll interval.
//!   This alone is the correctness floor — converges within one interval.
//! - `RIPCLONE_DISPATCH_MAX_WORKERS` (default 10): global cap on desired
//!   workers. Never exceeded.
//! - Queue env (`RIPCLONE_QUEUE`, `RIPCLONE_QUEUE_DB_URL`, …): same as
//!   `ripclone-worker`. Must be a SQL queue with a workers registry
//!   (`sqlite` or `libsql`) so live counts work.
//! - Worker env bag keys present in **this** process are forwarded into each
//!   `WorkerSpec.env` (queue, storage, metadata, upstream-cred — see
//!   `dispatch/ENV_BAG.md`). `size_class` is set per started slot, not from a
//!   fixed env value. Today that bag still includes `RIPCLONE_METADATA_DB_URL`.
//! - Size classes: `RIPCLONE_SIZE_CLASSES` / launch defaults (`small`|`large`).
//! - Provider-specific env: `FLY_WORKER_APP` / `FLY_API_TOKEN` for `fly`,
//!   `RIPCLONE_DISPATCH_CMD` for `exec`, `RIPCLONE_DISPATCH_URL` for `http`.
//!
//! ## Topology
//!
//! - **Poll-only wake.** Cross-process HTTP poke is out of scope. In-process
//!   enqueue subscribe is optional and not wired here — poll converges alone.
//! - **Best-effort starts.** `ensure_worker` failures log, back off, and retry
//!   next reconcile; jobs stay queued.
//! - **Scale-to-zero** is idle-exit on workers + this loop starting zero when
//!   the queue is empty.

use anyhow::{Context, Result, bail};
use ripclone::backends;
use ripclone::dispatch::{
    AutoscaleConfig, SelectProviderOptions, collect_worker_env, get_compute_provider,
    parse_dispatch_backend, run_loop,
};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let backend = parse_dispatch_backend(std::env::var("RIPCLONE_DISPATCH").ok().as_deref())?;
    if matches!(backend, ripclone::dispatch::DispatchBackend::None) {
        info!(
            "RIPCLONE_DISPATCH=none (or unset): dispatcher is a no-op. \
             Run a static worker pool, or set RIPCLONE_DISPATCH=fly|exec|http|mock."
        );
        return Ok(());
    }

    let config = AutoscaleConfig::from_env().context("dispatcher config")?;
    let provider = get_compute_provider(SelectProviderOptions {
        dispatch: Some(backend.as_str().into()),
        ..Default::default()
    })?
    .ok_or_else(|| anyhow::anyhow!("internal: backend {backend:?} resolved to no provider"))?;

    let queue = backends::connect_sql_queue().await?;
    if !queue.supports_worker_registry() {
        bail!(
            "dispatcher requires RIPCLONE_QUEUE=sqlite|libsql so live_worker_count \
             works (postgres/mysql lag the workers registry)"
        );
    }
    let queue = Arc::new(queue);

    let worker_env = collect_worker_env();
    info!(
        provider = provider.name(),
        interval_secs = config.interval.as_secs(),
        max_workers = config.max_workers,
        env_keys = worker_env.len(),
        "ripclone-dispatcher starting"
    );

    run_loop(queue, provider, config, worker_env).await
}
