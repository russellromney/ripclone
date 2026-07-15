//! Depth-based dispatcher autoscale: pure reconcile plan + async loop.
//!
//! The cloud enqueues and reads status; it never dispatches. This module is the
//! OSS reconcile that keeps enough workers running to drain the job queue,
//! sized to pending work's size-class.
//!
//! ## Formula
//!
//! ```text
//! desired  = min(max_workers, total_pending)
//! to_start = max(0, desired - live_worker_count)
//! ```
//!
//! One worker per pending job up to the configured cap. Empty queue →
//! `to_start == 0` (scale-to-zero convergence with worker idle-exit). A single
//! live worker behind a growing backlog raises `desired` with depth and starts
//! more workers until the cap.
//!
//! Size class for each started slot is the **max rank among pending jobs**
//! (large-capable workers may also drain small jobs; a small worker must never
//! be started when a large job is waiting — it can OOM).
//!
//! Live count is **capability-filtered**: only workers that can claim the max
//! pending rank count toward `desired - live`. A small-only live worker does
//! not block starting a large worker for large pending work (the classic
//! size-class livelock).
//!
//! ## Invariants
//!
//! - Never start below 0; never exceed `max_workers`.
//! - `ensure_worker` is best-effort + idempotent: failures are logged, exponential
//!   backoff advances, the job stays queued, the next reconcile retries.
//! - Live count always comes from the queue registry (capability-filtered), not
//!   a local guess — two reconciles with the same state converge without
//!   double-counting.

use super::{ComputeProvider, WorkerSpec};
use crate::queue::SqlJobQueue;
use crate::queue::size_class::{SizeClass, class_name};
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// True when the worker env bag targets the token-only farm-out path: the
/// worker claims over HTTP (`RIPCLONE_QUEUE_API_URL`) and reports metadata over
/// HTTP (`RIPCLONE_METADATA_REPORT_URL`). When set, the dispatcher forwards a
/// durable, operator-provisioned bearer token to each worker (it does not mint).
pub fn api_mode_configured(env: &BTreeMap<String, String>) -> bool {
    env.contains_key("RIPCLONE_QUEUE_API_URL") && env.contains_key("RIPCLONE_METADATA_REPORT_URL")
}

/// Default poll interval when `RIPCLONE_DISPATCH_INTERVAL_SECS` is unset.
pub const DEFAULT_INTERVAL_SECS: u64 = 5;

/// Default global worker cap when `RIPCLONE_DISPATCH_MAX_WORKERS` is unset.
pub const DEFAULT_MAX_WORKERS: usize = 10;

/// Base delay for exponential backoff after `ensure_worker` failure.
const BACKOFF_BASE: Duration = Duration::from_millis(500);

/// Cap on backoff so a long outage still retries within a reasonable window.
const BACKOFF_CAP: Duration = Duration::from_secs(60);

/// Pure result of one reconcile step (no I/O).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilePlan {
    /// Target fleet size: `min(max_workers, total_pending)`.
    pub desired: usize,
    /// How many `ensure_worker` calls this step should make.
    pub to_start: usize,
    /// Total pending jobs across all size classes.
    pub total_pending: usize,
    /// Live workers **capable of the max pending rank** at plan time.
    /// (Small-only workers do not count when a large job is waiting.)
    pub live_workers: usize,
    /// Size-class name for every slot to start (length == `to_start`).
    pub size_classes: Vec<String>,
}

/// Max size-class rank among pending jobs with depth > 0.
pub fn max_pending_rank(pending_by_class: &[(i64, usize)]) -> Option<i64> {
    pending_by_class
        .iter()
        .filter(|(_, c)| *c > 0)
        .map(|(r, _)| *r)
        .max()
}

/// Pure depth-based plan.
///
/// `pending_by_class` is `(rank, count)` from [`SqlJobQueue::pending_by_class`].
/// `live_workers` must be the count of live workers **capable of covering the
/// max pending rank** (see [`SqlJobQueue::live_worker_count_capable`]) — not
/// the raw fleet size. Ranks outside `size_classes` clamp via [`class_name`].
pub fn plan_reconcile(
    pending_by_class: &[(i64, usize)],
    live_workers: usize,
    max_workers: usize,
    size_classes: &[SizeClass],
) -> ReconcilePlan {
    let total_pending: usize = pending_by_class.iter().map(|(_, c)| *c).sum();
    let desired = total_pending.min(max_workers);
    let to_start = desired.saturating_sub(live_workers);

    // Size every new worker to the largest pending rank so a large job never
    // lands on a too-small box. Large-capable workers also drain smaller jobs.
    let max_rank = max_pending_rank(pending_by_class).unwrap_or(0);
    let name = if total_pending == 0 || size_classes.is_empty() {
        String::new()
    } else {
        class_name(max_rank, size_classes).to_string()
    };

    ReconcilePlan {
        desired,
        to_start,
        total_pending,
        live_workers,
        size_classes: if to_start == 0 || name.is_empty() {
            Vec::new()
        } else {
            vec![name; to_start]
        },
    }
}

/// Exponential backoff state for non-fatal `ensure_worker` failures.
#[derive(Debug, Clone)]
pub struct BackoffState {
    consecutive_failures: u32,
    /// Wall-clock until which reconcile should skip starts (still reads queue).
    blocked_until: Option<Instant>,
    /// Optional retry-after override from the last failure (e.g. provider quota).
    last_retry_after: Option<Duration>,
}

impl Default for BackoffState {
    fn default() -> Self {
        Self::new()
    }
}

impl BackoffState {
    pub fn new() -> Self {
        Self {
            consecutive_failures: 0,
            blocked_until: None,
            last_retry_after: None,
        }
    }

    /// True when starts should wait (backoff window still open).
    pub fn is_blocked(&self, now: Instant) -> bool {
        self.blocked_until.is_some_and(|t| now < t)
    }

    /// Remaining block duration at `now` (zero if not blocked).
    pub fn remaining(&self, now: Instant) -> Duration {
        self.blocked_until
            .map(|t| t.saturating_duration_since(now))
            .unwrap_or(Duration::ZERO)
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Record a successful ensure (clears backoff).
    pub fn on_success(&mut self) {
        self.consecutive_failures = 0;
        self.blocked_until = None;
        self.last_retry_after = None;
    }

    /// Record a failure. Uses `retry_after` when the provider surfaces one;
    /// otherwise exponential backoff from the failure count.
    pub fn on_failure(&mut self, now: Instant, retry_after: Option<Duration>) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_retry_after = retry_after;
        let delay = retry_after.unwrap_or_else(|| {
            let shift = self.consecutive_failures.saturating_sub(1).min(6);
            let d = BACKOFF_BASE.saturating_mul(2u32.pow(shift));
            d.min(BACKOFF_CAP)
        });
        self.blocked_until = Some(now + delay);
    }

    /// Delay that would be applied for the next failure (tests / logs).
    pub fn next_delay(&self) -> Duration {
        if let Some(d) = self.last_retry_after {
            return d;
        }
        let shift = self.consecutive_failures.saturating_sub(1).min(6);
        let d = BACKOFF_BASE.saturating_mul(2u32.pow(shift));
        d.min(BACKOFF_CAP)
    }
}

/// Parse a retry-after hint from an error string when present.
///
/// Accepts `retry-after: N` / `retry_after=N` (seconds). Providers that surface
/// structured retry-after can also pass `Some` into [`BackoffState::on_failure`]
/// directly; this is the best-effort message scrape.
pub fn parse_retry_after_secs(err: &str) -> Option<Duration> {
    let lower = err.to_ascii_lowercase();
    for key in [
        "retry-after:",
        "retry-after=",
        "retry_after=",
        "retry_after:",
    ] {
        if let Some(idx) = lower.find(key) {
            let rest = err[idx + key.len()..].trim_start();
            let num: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(secs) = num.parse::<u64>() {
                if secs > 0 {
                    return Some(Duration::from_secs(secs));
                }
            }
        }
    }
    None
}

/// ENV_BAG.md worker keys forwarded from the dispatcher's own environment.
///
/// `size_class` is **not** an env key — it lives on [`WorkerSpec`] per slot.
/// Lifecycle / max-size-class flags are included when present so a helper
/// script does not need a second injection path.
pub const WORKER_ENV_KEYS: &[&str] = &[
    // Queue (claim). Farm-out workers are token-only: they claim over HTTP
    // (`RIPCLONE_QUEUE=api` + `RIPCLONE_QUEUE_API_URL`) and hold **no** DB
    // credentials — the four `_DB_URL`/`_DB_TOKEN` keys are deliberately absent
    // so a dispatcher-started worker cannot express a DB connection at all.
    "RIPCLONE_QUEUE",
    "RIPCLONE_QUEUE_API_URL",
    // Storage
    "RIPCLONE_S3_ENDPOINT",
    "RIPCLONE_S3_REGION",
    "RIPCLONE_S3_BUCKET",
    "RIPCLONE_S3_PREFIX",
    "RIPCLONE_S3_CACHE_DIR",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_ENDPOINT_URL_S3",
    "AWS_REGION",
    "BUCKET_NAME",
    // Metadata: `api` reports over HTTP, no DB creds. The one bearer token
    // (`RIPCLONE_METADATA_JOB_TOKEN`) is a durable, operator-provisioned value
    // the dispatcher **forwards** to each worker (mint it once with
    // `ripclone mint-worker-token`). On Fly it is a machine secret provisioned
    // out of band, not forwarded through this bag.
    "RIPCLONE_METADATA",
    "RIPCLONE_METADATA_REPORT_URL",
    "RIPCLONE_METADATA_JOB_TOKEN",
    // Upstream-credential source
    "RIPCLONE_PROVIDERS",
    "RIPCLONE_GITHUB_TOKEN",
    "RIPCLONE_GITHUB_APP_ID",
    "RIPCLONE_GITHUB_APP_INSTALLATION_ID",
    "RIPCLONE_GITHUB_APP_PRIVATE_KEY",
    "RIPCLONE_GITHUB_APP_PRIVATE_KEY_PATH",
    "RIPCLONE_GITHUB_API_BASE",
    // Reserved token
    "RIPCLONE_TOKEN",
    // Size-class + lifecycle (optional)
    "RIPCLONE_MAX_SIZE_CLASS",
    "RIPCLONE_IDLE_EXIT_SECS",
    "RIPCLONE_MAX_JOBS",
    "RIPCLONE_SIZE_CLASSES",
    // Heartbeat so live_worker_count can see spawned workers
    "RIPCLONE_WORKER_HEARTBEAT",
    "RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS",
    "RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS",
];

/// Collect ENV_BAG worker keys present in the current process environment.
pub fn collect_worker_env() -> BTreeMap<String, String> {
    collect_worker_env_from(|k| std::env::var(k).ok())
}

/// Testable form of [`collect_worker_env`].
pub fn collect_worker_env_from(
    mut get: impl FnMut(&str) -> Option<String>,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for &key in WORKER_ENV_KEYS {
        if let Some(v) = get(key) {
            if !v.is_empty() {
                env.insert(key.to_string(), v);
            }
        }
    }
    env
}

/// Inputs for one reconcile against a live queue + provider.
pub struct ReconcileInputs<'a> {
    pub queue: &'a SqlJobQueue,
    pub provider: &'a dyn ComputeProvider,
    pub max_workers: usize,
    pub worker_env: &'a BTreeMap<String, String>,
    pub backoff: &'a mut BackoffState,
    /// Clock for backoff (inject in tests).
    pub now: Instant,
}

/// Outcome of one full reconcile (plan + ensure_worker attempts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub plan: ReconcilePlan,
    /// Successful `ensure_worker` calls this step.
    pub started: usize,
    /// Failed `ensure_worker` calls this step (non-fatal).
    pub failed: usize,
    /// True when backoff skipped starts entirely.
    pub skipped_backoff: bool,
}

/// One reconcile step: reap stale claims, read depth + capable live count,
/// plan, start workers.
///
/// Failures from `ensure_worker` never panic and never lose work — they log,
/// advance backoff, and leave jobs queued for the next pass.
pub async fn reconcile_once(input: ReconcileInputs<'_>) -> Result<ReconcileOutcome> {
    // Reap claims abandoned by dead/stuck workers BEFORE reading pending depth.
    // `queue::sql::claim_capped` already reclaims on every claim, so a queue
    // with live claim traffic self-heals on its own — but a job stuck `claimed`
    // on an otherwise-idle queue has no claimer to trigger that path: nothing
    // is `queued`, this reconcile would see 0 pending, start no worker, and
    // nobody would ever claim (and thus reclaim) it again. Calling it here,
    // every pass, closes that gap: a dead worker's job flips back to `queued`
    // and is counted in *this* pass, pulling a fresh worker immediately. Reuses
    // the queue's configured stale window / max-attempts / dead-letter — same
    // semantics as the claim-time reclaim, just called on a different trigger.
    input.queue.reclaim_stale().await?;

    let pending = input.queue.pending_by_class().await?;
    // Capability-filtered live count: for large pending, only large-capable
    // (or uncapped) workers count. Raw live_worker_count would livelock when a
    // small-only worker is already up and a large job waits.
    let live = match max_pending_rank(&pending) {
        Some(rank) => input.queue.live_worker_count_capable(rank).await?,
        None => 0,
    };
    let plan = plan_reconcile(
        &pending,
        live,
        input.max_workers,
        input.queue.size_classes(),
    );

    if plan.to_start == 0 {
        // Healthy idle / at-cap: clear backoff so a later burst starts immediately.
        if plan.total_pending == 0 {
            input.backoff.on_success();
        }
        return Ok(ReconcileOutcome {
            plan,
            started: 0,
            failed: 0,
            skipped_backoff: false,
        });
    }

    if input.backoff.is_blocked(input.now) {
        warn!(
            remaining_ms = input.backoff.remaining(input.now).as_millis() as u64,
            to_start = plan.to_start,
            "autoscale: backoff active, deferring ensure_worker"
        );
        return Ok(ReconcileOutcome {
            plan,
            started: 0,
            failed: 0,
            skipped_backoff: true,
        });
    }

    let mut started = 0usize;
    let mut failed = 0usize;
    for size_class in &plan.size_classes {
        // Per-slot ceiling: worker claims jobs at or below this class. The
        // token-only farm-out env (RIPCLONE_QUEUE=api + the bearer token) is
        // already in `worker_env`, forwarded whole — the dispatcher does not mint.
        let mut env = input.worker_env.clone();
        env.insert("RIPCLONE_MAX_SIZE_CLASS".into(), size_class.clone());
        let spec = WorkerSpec::new(size_class.clone(), env);
        match input.provider.ensure_worker(&spec).await {
            Ok(()) => {
                started += 1;
                input.backoff.on_success();
            }
            Err(e) => {
                failed += 1;
                error!(
                    size_class = %size_class,
                    err = %e,
                    "autoscale: ensure_worker failed (non-fatal; job stays queued)"
                );
                let retry_after = parse_retry_after_secs(&e.to_string());
                input.backoff.on_failure(input.now, retry_after);
                // Stop starting more this pass — provider is unhealthy.
                break;
            }
        }
    }

    if started > 0 {
        info!(
            started,
            failed,
            desired = plan.desired,
            live = plan.live_workers,
            pending = plan.total_pending,
            "autoscale: ensure_worker batch done"
        );
    }

    Ok(ReconcileOutcome {
        plan,
        started,
        failed,
        skipped_backoff: false,
    })
}

/// Config for the long-running poll loop.
#[derive(Debug, Clone)]
pub struct AutoscaleConfig {
    pub interval: Duration,
    pub max_workers: usize,
}

impl AutoscaleConfig {
    /// Load from env. Missing/invalid values use defaults (fail loudly only on
    /// non-numeric garbage for the knobs we own).
    pub fn from_env() -> Result<Self> {
        let interval_secs = match std::env::var("RIPCLONE_DISPATCH_INTERVAL_SECS") {
            Ok(s) if !s.trim().is_empty() => s
                .trim()
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("RIPCLONE_DISPATCH_INTERVAL_SECS: {e}"))?,
            _ => DEFAULT_INTERVAL_SECS,
        };
        if interval_secs == 0 {
            anyhow::bail!("RIPCLONE_DISPATCH_INTERVAL_SECS must be >= 1");
        }
        let max_workers = match std::env::var("RIPCLONE_DISPATCH_MAX_WORKERS") {
            Ok(s) if !s.trim().is_empty() => s
                .trim()
                .parse::<usize>()
                .map_err(|e| anyhow::anyhow!("RIPCLONE_DISPATCH_MAX_WORKERS: {e}"))?,
            _ => DEFAULT_MAX_WORKERS,
        };
        if max_workers == 0 {
            anyhow::bail!("RIPCLONE_DISPATCH_MAX_WORKERS must be >= 1");
        }
        Ok(Self {
            interval: Duration::from_secs(interval_secs),
            max_workers,
        })
    }
}

/// Poll-only autoscale loop. Converges within one `interval` of enqueue.
///
/// Cross-process HTTP poke and in-process `QueueEvent` subscribe are out of
/// scope here (poll alone is the correctness floor).
pub async fn run_loop(
    queue: Arc<SqlJobQueue>,
    provider: Arc<dyn ComputeProvider>,
    config: AutoscaleConfig,
    worker_env: BTreeMap<String, String>,
) -> Result<()> {
    let mut heartbeat = super::heartbeat::DeadMansSwitch::from_env();
    info!(
        interval_secs = config.interval.as_secs(),
        max_workers = config.max_workers,
        provider = provider.name(),
        api_mode = api_mode_configured(&worker_env),
        heartbeat_configured = heartbeat.is_configured(),
        "dispatcher autoscale loop starting (poll-only)"
    );
    let mut backoff = BackoffState::new();
    loop {
        let now = Instant::now();
        match reconcile_once(ReconcileInputs {
            queue: &queue,
            provider: provider.as_ref(),
            max_workers: config.max_workers,
            worker_env: &worker_env,
            backoff: &mut backoff,
            now,
        })
        .await
        {
            Ok(out) => {
                if out.plan.total_pending > 0 || out.started > 0 || out.failed > 0 {
                    info!(
                        desired = out.plan.desired,
                        live = out.plan.live_workers,
                        pending = out.plan.total_pending,
                        to_start = out.plan.to_start,
                        started = out.started,
                        failed = out.failed,
                        skipped_backoff = out.skipped_backoff,
                        "autoscale reconcile"
                    );
                }
                // Dead-man's switch: unset RIPCLONE_HEARTBEAT_URL makes this a
                // no-op. Best-effort — never breaks this loop.
                heartbeat
                    .on_reconcile(out.plan.total_pending, out.started, out.failed)
                    .await;
            }
            Err(e) => {
                // Queue read failure: log and keep looping — do not exit and
                // abandon the fleet. Back off briefly so a down DB is not hammered.
                error!("autoscale reconcile error: {e:#}");
                backoff.on_failure(Instant::now(), None);
            }
        }
        tokio::time::sleep(config.interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::MockProvider;
    use crate::provider::{ProviderInstanceId, RepoId};
    use crate::queue::size_class::default_size_classes;
    use crate::queue::{BuildJob, JobQueue, SqlJobQueue, SqliteDb};
    use std::sync::Arc;

    fn classes() -> Vec<SizeClass> {
        default_size_classes()
    }

    fn plan(pending: &[(i64, usize)], live: usize, max: usize) -> ReconcilePlan {
        plan_reconcile(pending, live, max, &classes())
    }

    #[test]
    fn empty_queue_starts_zero() {
        let p = plan(&[], 0, 10);
        assert_eq!(p.desired, 0);
        assert_eq!(p.to_start, 0);
        assert!(p.size_classes.is_empty());
    }

    #[test]
    fn empty_with_live_still_starts_zero() {
        // Scale-to-zero: idle-exit drains live; we do not start more.
        let p = plan(&[], 3, 10);
        assert_eq!(p.desired, 0);
        assert_eq!(p.to_start, 0);
    }

    #[test]
    fn depth_grows_past_live_starts_up_to_cap() {
        // 1 live, 5 pending, cap 10 → start 4 (desired=5).
        let p = plan(&[(0, 5)], 1, 10);
        assert_eq!(p.desired, 5);
        assert_eq!(p.to_start, 4);
        assert_eq!(p.size_classes.len(), 4);
        assert!(p.size_classes.iter().all(|s| s == "small"));

        // Cap binds: 100 pending, 2 live, cap 10 → start 8.
        let p = plan(&[(0, 100)], 2, 10);
        assert_eq!(p.desired, 10);
        assert_eq!(p.to_start, 8);

        // Already at cap: start 0.
        let p = plan(&[(0, 50)], 10, 10);
        assert_eq!(p.to_start, 0);

        // Never negative when live > desired (over-provisioned fleet).
        let p = plan(&[(0, 1)], 5, 10);
        assert_eq!(p.desired, 1);
        assert_eq!(p.to_start, 0);
    }

    #[test]
    fn one_worker_behind_growing_backlog_triggers_more() {
        // The acceptance case: one worker, backlog grows past it, up to cap.
        let mut live = 1usize;
        let mut started_total = 0usize;
        let max = 5usize;
        for pending in [1usize, 2, 3, 5, 8, 20] {
            let p = plan(&[(0, pending)], live, max);
            // After starts, pretend workers become live immediately (registry).
            started_total += p.to_start;
            live = live.saturating_add(p.to_start);
            assert!(live <= max, "never exceed cap: live={live} max={max}");
            assert_eq!(
                p.desired,
                pending.min(max),
                "desired tracks pending up to cap"
            );
        }
        assert!(
            started_total >= 4,
            "growing backlog must have started more workers; started={started_total}"
        );
        assert_eq!(live, max, "converges at cap");
    }

    #[test]
    fn large_pending_selects_large_size_class() {
        // Rank 1 = large under launch defaults.
        let p = plan(&[(1, 1)], 0, 10);
        assert_eq!(p.to_start, 1);
        assert_eq!(p.size_classes, vec!["large".to_string()]);

        // Mixed: max rank wins so a large job never gets a small worker.
        let p = plan(&[(0, 3), (1, 1)], 0, 10);
        assert_eq!(p.to_start, 4);
        assert!(
            p.size_classes.iter().all(|s| s == "large"),
            "mixed pending must start large-capable workers: {:?}",
            p.size_classes
        );
    }

    #[test]
    fn backoff_advances_and_clears() {
        let mut b = BackoffState::new();
        let t0 = Instant::now();
        assert!(!b.is_blocked(t0));
        b.on_failure(t0, None);
        assert_eq!(b.consecutive_failures(), 1);
        assert!(b.is_blocked(t0));
        assert!(!b.is_blocked(t0 + BACKOFF_CAP + Duration::from_secs(1)));

        b.on_failure(t0, Some(Duration::from_secs(12)));
        assert_eq!(b.consecutive_failures(), 2);
        assert!(b.is_blocked(t0 + Duration::from_secs(11)));
        assert!(!b.is_blocked(t0 + Duration::from_secs(13)));

        b.on_success();
        assert_eq!(b.consecutive_failures(), 0);
        assert!(!b.is_blocked(t0));
    }

    #[test]
    fn parse_retry_after_from_error_message() {
        assert_eq!(
            parse_retry_after_secs("quota exceeded retry-after: 30"),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after_secs("Retry_After=5 more text"),
            Some(Duration::from_secs(5))
        );
        assert_eq!(parse_retry_after_secs("no hint here"), None);
    }

    #[test]
    fn collect_worker_env_forwards_bag_keys_only() {
        let env = collect_worker_env_from(|k| match k {
            "RIPCLONE_QUEUE" => Some("api".into()),
            "RIPCLONE_QUEUE_API_URL" => Some("https://srv".into()),
            "RIPCLONE_METADATA_REPORT_URL" => Some("https://srv/v1/refs".into()),
            // The durable worker token IS forwarded (operator-provisioned).
            "RIPCLONE_METADATA_JOB_TOKEN" => Some("rcjt1.tok".into()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret".into()),
            "UNRELATED" => Some("nope".into()),
            // DB creds are no longer in the bag: a farm-out worker is token-only.
            "RIPCLONE_QUEUE_DB_URL" => Some("/decoy/queue.db".into()),
            "RIPCLONE_METADATA_DB_URL" => Some("/decoy/metadata.db".into()),
            "RIPCLONE_QUEUE_DB_TOKEN" => Some("qtok".into()),
            "RIPCLONE_METADATA_DB_TOKEN" => Some("mtok".into()),
            _ => None,
        });
        assert_eq!(env.get("RIPCLONE_QUEUE").map(String::as_str), Some("api"));
        assert_eq!(
            env.get("RIPCLONE_QUEUE_API_URL").map(String::as_str),
            Some("https://srv")
        );
        assert_eq!(
            env.get("RIPCLONE_METADATA_REPORT_URL").map(String::as_str),
            Some("https://srv/v1/refs")
        );
        assert_eq!(
            env.get("RIPCLONE_METADATA_JOB_TOKEN").map(String::as_str),
            Some("rcjt1.tok")
        );
        assert!(!env.contains_key("UNRELATED"));
        // TRAP — no DB fallback in a farm-out worker: none of the four DB-cred
        // keys may ever be forwarded.
        for k in [
            "RIPCLONE_QUEUE_DB_URL",
            "RIPCLONE_QUEUE_DB_TOKEN",
            "RIPCLONE_METADATA_DB_URL",
            "RIPCLONE_METADATA_DB_TOKEN",
        ] {
            assert!(!env.contains_key(k), "DB cred {k} must not be in the bag");
        }
    }

    #[test]
    fn worker_env_keys_have_no_db_creds() {
        for k in [
            "RIPCLONE_QUEUE_DB_URL",
            "RIPCLONE_QUEUE_DB_TOKEN",
            "RIPCLONE_METADATA_DB_URL",
            "RIPCLONE_METADATA_DB_TOKEN",
        ] {
            assert!(
                !WORKER_ENV_KEYS.contains(&k),
                "WORKER_ENV_KEYS must not carry DB cred {k}"
            );
        }
    }

    #[test]
    fn api_mode_detected_from_urls() {
        let mut env = BTreeMap::new();
        assert!(!api_mode_configured(&env));
        env.insert("RIPCLONE_QUEUE_API_URL".into(), "https://s".into());
        assert!(!api_mode_configured(&env), "needs both URLs");
        env.insert(
            "RIPCLONE_METADATA_REPORT_URL".into(),
            "https://s/v1/refs".into(),
        );
        assert!(api_mode_configured(&env));
    }

    #[tokio::test]
    async fn reconcile_forwards_provisioned_token_per_worker() {
        // Model B: the dispatcher does NOT mint. A durable, operator-provisioned
        // `RIPCLONE_METADATA_JOB_TOKEN` lives in the worker_env bag and is
        // forwarded whole into every started worker's spec.
        let (q, _dir) = test_queue().await;
        q.enqueue(job("o/r", Some(100))).await.unwrap();
        let mock = MockProvider::new();
        let env = BTreeMap::from([
            ("RIPCLONE_QUEUE".to_string(), "api".to_string()),
            (
                "RIPCLONE_QUEUE_API_URL".to_string(),
                "https://srv".to_string(),
            ),
            (
                "RIPCLONE_METADATA_JOB_TOKEN".to_string(),
                "rcjt1.provisioned.token".to_string(),
            ),
        ]);
        let mut backoff = BackoffState::new();
        let out = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        assert_eq!(out.started, 1);
        assert_eq!(
            mock.calls()[0]
                .env
                .get("RIPCLONE_METADATA_JOB_TOKEN")
                .map(String::as_str),
            Some("rcjt1.provisioned.token"),
            "the provisioned worker token must be forwarded verbatim"
        );
    }

    fn job(path: &str, size_bytes: Option<u64>) -> BuildJob {
        BuildJob {
            repo_id: RepoId {
                workspace: ProviderInstanceId::new("github"),
                path: path.into(),
            },
            branch: "main".into(),
            initialization_attempt_id: None,
            rev: None,
            credential: None,
            recheck: 0,
            size_bytes,
        }
    }

    async fn test_queue() -> (SqlJobQueue, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let q = SqlJobQueue::new(Box::new(SqliteDb::connect(&path).await.unwrap()))
            .await
            .unwrap()
            .with_heartbeat_timeout_secs(60);
        (q, dir)
    }

    /// Zero-tolerance stale window: any currently `claimed` row is immediately
    /// reclaim-eligible. Used to make the reaper deterministic in tests.
    async fn test_queue_zero_stale() -> (SqlJobQueue, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let q = SqlJobQueue::new(Box::new(SqliteDb::connect(&path).await.unwrap()))
            .await
            .unwrap()
            .with_heartbeat_timeout_secs(60)
            .with_stale_claim_secs(0);
        (q, dir)
    }

    /// Reaper-on-reconcile (O11a): a job stuck `claimed` by a dead worker on an
    /// otherwise-idle queue (nothing `queued`) must still be reclaimed and
    /// counted THIS pass, not left stranded because `pending_by_class` alone
    /// sees zero depth.
    ///
    /// Prove-it: remove the `input.queue.reclaim_stale().await?` call from
    /// `reconcile_once` and this test fails — `total_pending` and `started`
    /// both go to 0 because the claimed-but-abandoned row is never reclaimed.
    #[tokio::test]
    async fn reconcile_reaps_stale_claim_before_reading_pending_depth() {
        let (q, _dir) = test_queue_zero_stale().await;
        q.enqueue(job("o/r", Some(100))).await.unwrap();

        // Simulate a dead worker: claim the job directly (a real SIGKILLed
        // worker leaves exactly this state — row `claimed`, no ack, ever), then
        // never ack. No new job is enqueued after this.
        let claimed = q.claim("dead-worker").await.unwrap();
        assert!(claimed.is_some(), "setup: job must be claimed");

        // Otherwise-idle queue: nothing `queued`, so a plain pending_by_class
        // read reports zero pending — the stranded-claim bug this fix closes.
        assert_eq!(q.depth().await, 0, "claimed job is not counted as queued");
        assert!(
            q.pending_by_class().await.unwrap().is_empty(),
            "pending_by_class alone sees no depth for a claimed-but-abandoned job"
        );

        let mock = MockProvider::new();
        let env = BTreeMap::new();
        let mut backoff = BackoffState::new();
        let out = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();

        assert_eq!(
            out.plan.total_pending, 1,
            "reclaim must run before pending is read, so this pass counts the \
             reclaimed job: {out:?}"
        );
        assert_eq!(
            out.started, 1,
            "reconcile must start a fresh worker for the reclaimed job: {out:?}"
        );
        assert_eq!(mock.calls().len(), 1);
    }

    #[tokio::test]
    async fn reconcile_empty_queue_zero_starts() {
        let (q, _dir) = test_queue().await;
        let mock = MockProvider::new();
        let env = BTreeMap::new();
        let mut backoff = BackoffState::new();
        let out = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 10,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        assert_eq!(out.plan.to_start, 0);
        assert_eq!(out.started, 0);
        assert!(mock.calls().is_empty());
    }

    #[tokio::test]
    async fn reconcile_depth_starts_workers_up_to_cap() {
        let (q, _dir) = test_queue().await;
        // 3 small pending, 0 live, cap 2 → start 2 small.
        for i in 0..3 {
            q.enqueue(job(&format!("o/r{i}"), Some(100))).await.unwrap();
        }
        let mock = MockProvider::new();
        let env = BTreeMap::from([("RIPCLONE_QUEUE".into(), "sqlite".into())]);
        let mut backoff = BackoffState::new();
        let out = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 2,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        assert_eq!(out.plan.desired, 2);
        assert_eq!(out.plan.to_start, 2);
        assert_eq!(out.started, 2);
        assert_eq!(mock.calls().len(), 2);
        for c in mock.calls() {
            assert_eq!(c.size_class, "small");
            assert_eq!(
                c.env.get("RIPCLONE_QUEUE").map(String::as_str),
                Some("sqlite")
            );
            assert_eq!(
                c.env.get("RIPCLONE_MAX_SIZE_CLASS").map(String::as_str),
                Some("small")
            );
        }
        // Jobs still queued (ensure_worker does not claim).
        assert_eq!(q.depth().await, 3);
    }

    #[tokio::test]
    async fn reconcile_large_job_starts_large_capable_worker() {
        let (q, _dir) = test_queue().await;
        // > 1 GiB → large under launch defaults.
        q.enqueue(job("o/huge", Some((1 << 30) + 1))).await.unwrap();
        let mock = MockProvider::new();
        let env = BTreeMap::new();
        let mut backoff = BackoffState::new();
        let out = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        assert_eq!(out.started, 1);
        assert_eq!(mock.calls()[0].size_class, "large");
        assert_eq!(
            mock.calls()[0]
                .env
                .get("RIPCLONE_MAX_SIZE_CLASS")
                .map(String::as_str),
            Some("large")
        );
    }

    #[tokio::test]
    async fn small_live_worker_does_not_block_large_pending() {
        // Livelock regression: one small-only worker is live, one large job
        // waits. Raw live_count would give desired=1, live=1, to_start=0 and
        // the large job never gets a capable worker.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db").to_string_lossy().to_string();
        let q = SqlJobQueue::new(Box::new(SqliteDb::connect(&path).await.unwrap()))
            .await
            .unwrap()
            .with_heartbeat_timeout_secs(60);
        q.enqueue(job("o/huge", Some((1 << 30) + 1))).await.unwrap();

        // Separate handle with small ceiling — what a small worker heartbeats.
        let small_worker = SqlJobQueue::new(Box::new(SqliteDb::connect(&path).await.unwrap()))
            .await
            .unwrap()
            .with_max_size_class(Some("small"))
            .unwrap()
            .with_heartbeat_timeout_secs(60);
        let now = {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        };
        small_worker
            .heartbeat_at("small-only", None, now)
            .await
            .unwrap();
        assert_eq!(
            q.live_worker_count_at(now).await.unwrap(),
            1,
            "raw live count sees the small worker"
        );
        assert_eq!(
            q.live_worker_count_capable_at(1, now).await.unwrap(),
            0,
            "small-only worker is not large-capable"
        );

        let mock = MockProvider::new();
        let env = BTreeMap::new();
        let mut backoff = BackoffState::new();
        let out = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        assert_eq!(out.plan.live_workers, 0, "plan uses capable live, not raw");
        assert_eq!(out.plan.to_start, 1);
        assert_eq!(out.started, 1);
        assert_eq!(mock.calls()[0].size_class, "large");
    }

    #[tokio::test]
    async fn ensure_worker_err_is_non_fatal_backoff_advances() {
        let (q, _dir) = test_queue().await;
        q.enqueue(job("o/r", Some(100))).await.unwrap();
        let mock = MockProvider::new();
        mock.fail_next(1);
        let env = BTreeMap::new();
        let mut backoff = BackoffState::new();
        let now = Instant::now();
        let out = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now,
        })
        .await
        .unwrap();
        assert_eq!(out.failed, 1);
        assert_eq!(out.started, 0);
        assert_eq!(backoff.consecutive_failures(), 1);
        assert!(backoff.is_blocked(now));
        // Job still queued — failed wake never loses work.
        assert_eq!(q.depth().await, 1);

        // Next reconcile while blocked: no more starts.
        mock.reset();
        mock.fail_next(0);
        let out2 = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now, // still inside backoff window
        })
        .await
        .unwrap();
        assert!(out2.skipped_backoff);
        assert_eq!(out2.started, 0);
        assert!(mock.calls().is_empty());

        // After backoff expires, retry succeeds.
        let later = now + BACKOFF_CAP + Duration::from_secs(1);
        let out3 = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: &mock,
            max_workers: 5,
            worker_env: &env,
            backoff: &mut backoff,
            now: later,
        })
        .await
        .unwrap();
        assert_eq!(out3.started, 1);
        assert_eq!(backoff.consecutive_failures(), 0);
        assert_eq!(q.depth().await, 1, "still queued until a worker claims");
    }

    #[tokio::test]
    async fn idempotent_reconcile_uses_live_count_not_local_guess() {
        let (q, _dir) = test_queue().await;
        for i in 0..3 {
            q.enqueue(job(&format!("o/r{i}"), Some(100))).await.unwrap();
        }
        // Simulate 2 live workers already registered.
        q.heartbeat_at("w1", None, {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        })
        .await
        .unwrap();
        q.heartbeat_at("w2", None, {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        })
        .await
        .unwrap();

        let mock = Arc::new(MockProvider::new());
        let env = BTreeMap::new();
        let mut backoff = BackoffState::new();
        let out1 = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: mock.as_ref(),
            max_workers: 10,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        // desired=3, live=2 → start 1
        assert_eq!(out1.plan.live_workers, 2);
        assert_eq!(out1.plan.to_start, 1);
        assert_eq!(out1.started, 1);
        assert_eq!(mock.calls().len(), 1);

        // Second reconcile: live count still 2 (ensure_worker does not register
        // a heartbeat). Same plan → one more ensure (idempotent wake is fine).
        let out2 = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: mock.as_ref(),
            max_workers: 10,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        assert_eq!(out2.plan.live_workers, 2);
        assert_eq!(out2.plan.to_start, 1);
        assert_eq!(mock.calls().len(), 2);

        // Register the third worker: converge to to_start=0.
        q.heartbeat_at("w3", None, {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        })
        .await
        .unwrap();
        mock.reset();
        let out3 = reconcile_once(ReconcileInputs {
            queue: &q,
            provider: mock.as_ref(),
            max_workers: 10,
            worker_env: &env,
            backoff: &mut backoff,
            now: Instant::now(),
        })
        .await
        .unwrap();
        assert_eq!(out3.plan.live_workers, 3);
        assert_eq!(out3.plan.to_start, 0);
        assert!(mock.calls().is_empty());
    }

    #[test]
    fn autoscale_config_defaults() {
        // Pure parse via explicit strings would need env; just check constants.
        assert_eq!(DEFAULT_INTERVAL_SECS, 5);
        assert_eq!(DEFAULT_MAX_WORKERS, 10);
    }
}
