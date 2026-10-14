//! Build workers: two capped pools of slots that lease rows from the control
//! store.
//!
//! A Head slot fetches the admitted commit, builds Head and publishes it. A
//! Background slot builds the missing Full and Files of a row whose Head is
//! ready; each publishes or fails on its own. A slot holds a 60 s lease and
//! renews it every 20 s; a lost lease aborts the build. Failures record a
//! retry time instead of sleeping, so no slot waits while holding a lease.

use super::api::row_info;
use super::builder::{
    SyncPhases, build_files_result, build_full_result, build_head_result, report_sync_bench,
    report_sync_phases, sweep_stale_tempdirs,
};
use super::{ServerState, duration_ms, fetch_semaphore};
use crate::control::{BuildRow, Claim, Out, Output, Pool, Published};
use crate::provider::RepoId;
use crate::{ExactResultKind, git, validation};
use anyhow::{Context, Result};
use axum::http::StatusCode;
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tracing::{error, info, warn};

const LEASE: Duration = Duration::from_secs(60);
const RENEW_EVERY: Duration = Duration::from_secs(20);
const IDLE_POLL: Duration = Duration::from_secs(1);
const RETRY_CAP: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WorkerSettings {
    /// Concurrent Head builds. At least 1.
    pub head_slots: usize,
    /// Concurrent Full and Files builds. 0 pauses them.
    pub background_slots: usize,
    /// First retry delay; doubles per attempt up to `RETRY_CAP`.
    pub retry_base: Duration,
    /// Claims per phase before an output fails.
    pub max_attempts: i64,
}

impl WorkerSettings {
    pub(super) fn from_env() -> Result<Self> {
        Self::from_lookup(|key| env::var(key).ok())
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let number = |key: &str, default: u64| -> Result<u64> {
            match get(key) {
                None => Ok(default),
                Some(value) => value.trim().parse().with_context(|| {
                    format!("{key} must be a non-negative integer, got {value:?}")
                }),
            }
        };
        let head_slots = number("RIPCLONE_HEAD_SLOTS", 2)?;
        anyhow::ensure!(head_slots >= 1, "RIPCLONE_HEAD_SLOTS must be at least 1");
        let background_slots = number("RIPCLONE_BACKGROUND_SLOTS", 1)?;
        let retry_base = number("RIPCLONE_BUILD_RETRY_BASE_MS", 2000)?;
        let max_attempts = number("RIPCLONE_BUILD_MAX_ATTEMPTS", 5)?;
        anyhow::ensure!(
            max_attempts >= 1,
            "RIPCLONE_BUILD_MAX_ATTEMPTS must be at least 1"
        );
        Ok(Self {
            head_slots: usize::try_from(head_slots).context("RIPCLONE_HEAD_SLOTS is too large")?,
            background_slots: usize::try_from(background_slots)
                .context("RIPCLONE_BACKGROUND_SLOTS is too large")?,
            retry_base: Duration::from_millis(retry_base),
            max_attempts: i64::try_from(max_attempts)
                .context("RIPCLONE_BUILD_MAX_ATTEMPTS is too large")?,
        })
    }
}

/// Delay before the next claim after the `attempts`-th claim failed.
fn retry_delay(base: Duration, attempts: i64) -> Duration {
    let doublings = u32::try_from(attempts.saturating_sub(1).clamp(0, 30)).unwrap_or(30);
    base.saturating_mul(2u32.saturating_pow(doublings))
        .min(RETRY_CAP)
}

/// Error returned by a build, classified for retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BuildError {
    message: String,
    retryable: bool,
}

impl BuildError {
    fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }

    fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    fn is_retryable(&self) -> bool {
        self.retryable
    }

    fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

/// Transient upstream, storage, network and control-database errors retry;
/// everything else fails the output.
pub(super) fn classify_build_error(error: &anyhow::Error) -> BuildError {
    for cause in error.chain() {
        if let Some(s3_error) = cause.downcast_ref::<s3::Error>() {
            let message = format!("{error:#}");
            return if s3_error.is_retryable() {
                BuildError::retryable(message)
            } else {
                BuildError::permanent(message)
            };
        }
        if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>() {
            let message = format!("{error:#}");
            return if reqwest_error.is_timeout()
                || reqwest_error.is_connect()
                || reqwest_error
                    .status()
                    .is_some_and(|s| s == StatusCode::TOO_MANY_REQUESTS || s.is_server_error())
            {
                BuildError::retryable(message)
            } else {
                BuildError::permanent(message)
            };
        }
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>()
            && is_retryable_io_error(io_error)
        {
            return BuildError::retryable(format!("{error:#}"));
        }
        if let Some(git_error) = cause.downcast_ref::<git::UpstreamGitError>() {
            let message = format!("{error:#}");
            return if git_error.is_retryable() {
                BuildError::retryable(message)
            } else {
                BuildError::permanent(message)
            };
        }
        if cause.is::<tokio::time::error::Elapsed>() || cause.is::<sqlx::Error>() {
            return BuildError::retryable(format!("{error:#}"));
        }
    }
    BuildError::permanent(format!("{error:#}"))
}

fn is_retryable_io_error(error: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        error.kind(),
        ErrorKind::TimedOut
            | ErrorKind::Interrupted
            | ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::NotConnected
            | ErrorKind::BrokenPipe
            | ErrorKind::UnexpectedEof
    )
}

/// Start every slot. Lease owners are `{pid}-{boot nanos}-{pool}-{slot}-{seq}`,
/// unique across restarts.
pub(super) fn spawn_workers(state: &ServerState, settings: &WorkerSettings) {
    let boot_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let boot = format!("{}-{boot_nanos:x}", std::process::id());
    for slot in 0..settings.head_slots {
        tokio::spawn(run_slot(
            state.clone(),
            *settings,
            Pool::Head,
            format!("{boot}-head-{slot}"),
        ));
    }
    for slot in 0..settings.background_slots {
        tokio::spawn(run_slot(
            state.clone(),
            *settings,
            Pool::Background,
            format!("{boot}-background-{slot}"),
        ));
    }
    info!(
        head_slots = settings.head_slots,
        background_slots = settings.background_slots,
        "build workers started"
    );
}

async fn run_slot(state: ServerState, settings: WorkerSettings, pool: Pool, slot: String) {
    let wake = match pool {
        Pool::Head => Arc::clone(&state.head_wake),
        Pool::Background => Arc::clone(&state.background_wake),
    };
    let mut sequence = 0u64;
    loop {
        // Register for a wake-up before reading SQLite, so an admission that
        // commits during the claim is not missed.
        let notified = wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        sequence = sequence.wrapping_add(1);
        let owner = format!("{slot}-{sequence}");
        match state.control.claim(&owner, pool, LEASE).await {
            Ok(Some(claim)) => {
                run_claim(&state, settings, pool, claim).await;
                continue;
            }
            Ok(None) => {}
            Err(error) => error!("{pool:?} build claim failed: {error:#}"),
        }
        tokio::select! {
            () = &mut notified => {}
            () = tokio::time::sleep(IDLE_POLL) => {}
        }
    }
}

/// Run one leased build, renewing the lease until it finishes. A lost lease
/// aborts the build; another worker owns the row now.
async fn run_claim(state: &ServerState, settings: WorkerSettings, pool: Pool, claim: Claim) {
    let claim = Arc::new(claim);
    let task_state = state.clone();
    let task_claim = Arc::clone(&claim);
    let mut task = tokio::spawn(async move {
        match pool {
            Pool::Head => build_head(&task_state, settings, &task_claim).await,
            Pool::Background => build_background(&task_state, settings, &task_claim).await,
        }
    });
    let mut renew =
        tokio::time::interval_at(tokio::time::Instant::now() + RENEW_EVERY, RENEW_EVERY);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            joined = &mut task => {
                if let Err(error) = joined {
                    let failure = BuildError::retryable(format!("build task panicked: {error}"));
                    for kind in pool_kinds(pool) {
                        record_failure(state, settings, &claim, *kind, &failure).await;
                    }
                }
                break;
            }
            _ = renew.tick() => match state.control.renew(&claim, LEASE).await {
                Ok(true) => {}
                Ok(false) => {
                    warn!(
                        "lost {pool:?} lease for {}@{}; aborting build",
                        claim.repo.storage_key(),
                        claim.build.commit
                    );
                    task.abort();
                    let _ = task.await;
                    return;
                }
                Err(error) => warn!(
                    "renew {pool:?} lease for {}@{} failed: {error:#}",
                    claim.repo.storage_key(),
                    claim.build.commit
                ),
            },
        }
    }
    if let Err(error) = state.control.release(&claim).await {
        error!(
            "release lease for {}@{} failed: {error:#}",
            claim.repo.storage_key(),
            claim.build.commit
        );
    }
    // The row is claimable by the Background pool only once released.
    if pool == Pool::Head {
        state.background_wake.notify_one();
    }
}

fn pool_kinds(pool: Pool) -> &'static [ExactResultKind] {
    match pool {
        Pool::Head => &[ExactResultKind::Head],
        Pool::Background => &[ExactResultKind::Full, ExactResultKind::Files],
    }
}

/// Record a failed output: pending with a retry time when the error is
/// retryable, failed otherwise. The store fails it once attempts run out.
async fn record_failure(
    state: &ServerState,
    settings: WorkerSettings,
    claim: &Claim,
    kind: ExactResultKind,
    error: &BuildError,
) {
    let retry_after = error
        .is_retryable()
        .then(|| retry_delay(settings.retry_base, claim.build.attempts));
    let gives_up = retry_after.is_none() || claim.build.attempts >= settings.max_attempts;
    let target = format!("{}@{}", claim.repo.storage_key(), claim.build.commit);
    if let Err(write_error) = state
        .control
        .fail(claim, kind, error.message(), retry_after)
        .await
    {
        error!("record {kind} failure for {target} failed: {write_error:#}");
        return;
    }
    if gives_up {
        state.metrics.record_build_failed();
        warn!("{kind} build failed for {target}: {error}");
    } else {
        warn!(
            "{kind} build attempt {} failed for {target}, retrying in {:?}: {error}",
            claim.build.attempts,
            retry_after.unwrap_or_default()
        );
    }
}

/// Publish a built output, or record why it could not be built.
async fn settle(
    state: &ServerState,
    settings: WorkerSettings,
    claim: &Claim,
    kind: ExactResultKind,
    built: Result<Output>,
) -> Option<Published> {
    let published = match built {
        Ok(output) => state
            .control
            .publish(claim, output)
            .await
            .with_context(|| format!("publish {kind} result")),
        Err(error) => Err(error),
    };
    match published {
        Ok(published) => {
            record_published(state, claim, kind, published).await;
            Some(published)
        }
        Err(error) => {
            record_failure(state, settings, claim, kind, &classify_build_error(&error)).await;
            None
        }
    }
}

async fn record_published(
    state: &ServerState,
    claim: &Claim,
    kind: ExactResultKind,
    published: Published,
) {
    let repo_id = &claim.repo;
    let commit = &claim.build.commit;
    if !published.applied {
        warn!(
            "{kind} result for {}@{commit} was not published: the lease was lost",
            repo_id.storage_key()
        );
        return;
    }
    state.metrics.record_output_published(kind);
    info!(
        "published {kind} result for {}@{}",
        repo_id.storage_key(),
        &commit[..7.min(commit.len())]
    );
    if !published.row_complete {
        return;
    }
    let now = crate::control::Clock::now(&crate::control::SystemClock);
    let age = u64::try_from(now.saturating_sub(claim.build.created_at)).unwrap_or(0);
    state
        .metrics
        .record_build_completed(Duration::from_secs(age));
    if std::env::var_os("RIPCLONE_BENCH").is_some()
        && let Ok(Some(row)) = state.control.status(repo_id, commit).await
    {
        let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
        report_sync_bench(
            repo_id,
            commit,
            &state.storage,
            &row_info(&row),
            &mirror_dir,
        );
    }
}

async fn repo_lock(
    locks: &Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    repo_id: &RepoId,
) -> Arc<tokio::sync::Mutex<()>> {
    let key = repo_id.storage_key();
    let mut map = locks.lock().await;
    map.entry(key)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// Fetch `commit` into the mirror under the process-wide fetch cap. The
/// caller holds the repo lock.
async fn fetch_commit(
    state: &ServerState,
    repo_id: &RepoId,
    commit: &str,
    mirror_dir: &std::path::Path,
) -> Result<()> {
    let provider = state
        .provider_registry
        .get(repo_id.provider.as_str())
        .cloned()
        .with_context(|| format!("unknown provider {}", repo_id.provider.as_str()))?;
    let credential = state
        .broker
        .fetch_credential(repo_id)
        .with_context(|| format!("fetch credential for {}", repo_id.storage_key()))?;
    let _permit = fetch_semaphore()
        .acquire()
        .await
        .expect("fetch semaphore never closed");
    let (mirror_dir, repo_id, commit) = (
        mirror_dir.to_path_buf(),
        repo_id.clone(),
        commit.to_string(),
    );
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror_admitted(
            &mirror_dir,
            &provider,
            &repo_id,
            &commit,
            credential.as_ref(),
        )
    })
    .await
    .context("mirror fetch task")?
}

async fn build_head(state: &ServerState, settings: WorkerSettings, claim: &Claim) {
    let started = Instant::now();
    let mut phases = SyncPhases::default();
    let built = head_output(state, claim, &mut phases).await;
    let publish_start = Instant::now();
    let Some(published) = settle(state, settings, claim, ExactResultKind::Head, built).await else {
        return;
    };
    if !published.applied {
        return;
    }
    phases.ref_publish_ms = Some(duration_ms(publish_start.elapsed()));
    phases.publish_head_ms = Some(duration_ms(started.elapsed()));
    state.metrics.record_sync_phases((&phases).into());
    report_sync_phases(&claim.repo, &claim.build.commit, &phases);
}

async fn head_output(
    state: &ServerState,
    claim: &Claim,
    phases: &mut SyncPhases,
) -> Result<Output> {
    let repo_id = &claim.repo;
    let commit = claim.build.commit.as_str();
    validation::validate_object_id(commit).context("validate admitted commit")?;
    info!("building Head for {}@{commit}", repo_id.storage_key());
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    // A killed build leaks its `.tmp*` dir in TMPDIR (= repo_root). Only old
    // ones are removed, so a running build's temp dir is never touched.
    sweep_stale_tempdirs(&state.repo_root, Duration::from_secs(2 * 3600));

    // The mirror is written only under the repo lock: fetch and commit-graph.
    // The build below only reads it (auto-gc is off).
    let parent = {
        let lock = repo_lock(&state.sync_locks, repo_id).await;
        let _guard = lock.lock().await;
        let t = Instant::now();
        fetch_commit(state, repo_id, commit, &mirror_dir).await?;
        phases.mirror_fetch_ms = Some(duration_ms(t.elapsed()));
        let t = Instant::now();
        let (prep_dir, prep_commit) = (mirror_dir.clone(), commit.to_string());
        let parent = tokio::task::spawn_blocking(move || -> Result<Option<String>> {
            // An empty upstream mirrors fine but has nothing to build.
            if git::is_empty_repo(&prep_dir).unwrap_or(false) {
                anyhow::bail!("repository has no commits (nothing to clone)");
            }
            let resolved = git::resolve_commit(&prep_dir, &prep_commit)
                .with_context(|| format!("resolve admitted commit {prep_commit}"))?;
            anyhow::ensure!(
                resolved == prep_commit,
                "admitted commit resolved unexpectedly: requested {prep_commit}, got {resolved}"
            );
            let parent = git::parent_commit(&prep_dir, &prep_commit).ok().flatten();
            // Best-effort: speeds up the rev-list walks of every build.
            let _ = git::write_commit_graph(&prep_dir);
            Ok(parent)
        })
        .await
        .context("mirror prep task")??;
        phases.commit_graph_ms = Some(duration_ms(t.elapsed()));
        parent
    };

    // Head packs are a delta against an immutable base: the compacted base
    // recorded by Full(parent), else the base Head(parent) used, else none.
    let parent_row = match parent.as_deref() {
        Some(parent) => state.control.status(repo_id, parent).await?,
        None => None,
    };
    let parent_head = parent_row.as_ref().and_then(|row| row.head.ready());
    let base = parent_row
        .as_ref()
        .and_then(|row| row.full.ready())
        .and_then(|full| full.head_base.clone())
        .or_else(|| {
            parent_head.map(|head| crate::HeadBase {
                commit: head.base_commit.clone(),
                packs: head.base_packs.clone(),
            })
        });
    let result = build_head_result(
        &state.cas,
        &mirror_dir,
        commit,
        parent.clone(),
        parent_head,
        base,
        &state.storage,
        phases,
    )
    .await?;
    Ok(Output::Head { result, parent })
}

async fn build_background(state: &ServerState, settings: WorkerSettings, claim: &Claim) {
    let need_full = matches!(claim.build.full, Out::Pending { .. });
    let need_files = matches!(claim.build.files, Out::Pending { .. });
    let (head, parent_row, config, mirror_dir) =
        match background_prep(state, claim, need_full).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let failure = classify_build_error(&error);
                for (kind, needed) in [
                    (ExactResultKind::Full, need_full),
                    (ExactResultKind::Files, need_files),
                ] {
                    if needed {
                        record_failure(state, settings, claim, kind, &failure).await;
                    }
                }
                return;
            }
        };
    let repo_id = &claim.repo;
    let commit = claim.build.commit.as_str();
    let full = async {
        if !need_full {
            return;
        }
        let levels = parent_row
            .as_ref()
            .and_then(|row| row.full.ready())
            .map(|full| full.history_levels.clone())
            .unwrap_or_default();
        let built = build_full_result(
            &state.cas,
            &mirror_dir,
            repo_id,
            commit,
            &head,
            levels,
            &state.storage,
        )
        .await
        .map(Output::Full);
        settle(state, settings, claim, ExactResultKind::Full, built).await;
    };
    let files = async {
        if !need_files {
            return;
        }
        let parent_files = parent_row
            .as_ref()
            .and_then(|row| row.files.ready().map(|files| (row.commit.as_str(), files)));
        let built = build_files_result(
            &state.cas,
            &mirror_dir,
            commit,
            &head,
            parent_files,
            &state.storage,
            config.compression_level(),
        )
        .await
        .map(Output::Files);
        settle(state, settings, claim, ExactResultKind::Files, built).await;
    };
    tokio::join!(full, files);
}

/// Everything Full and Files share: the ready Head, the parent's row, the
/// repository config as stored now, and a mirror holding the commit.
async fn background_prep(
    state: &ServerState,
    claim: &Claim,
    need_full: bool,
) -> Result<(
    crate::HeadResult,
    Option<BuildRow>,
    crate::repo_config::RepoConfig,
    std::path::PathBuf,
)> {
    let repo_id = &claim.repo;
    let commit = claim.build.commit.as_str();
    let head = claim
        .build
        .head
        .ready()
        .cloned()
        .context("Head result is not ready")?;
    let config = state.control.get_config(repo_id).await?.unwrap_or_default();
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    {
        let lock = repo_lock(&state.sync_locks, repo_id).await;
        let _guard = lock.lock().await;
        // Usually a no-op: the Head build fetched the commit.
        fetch_commit(state, repo_id, commit, &mirror_dir).await?;
        if need_full {
            // Best-effort: a reachability bitmap makes the history walk much
            // faster on large repositories.
            let bitmap_dir = mirror_dir.clone();
            let _ = tokio::task::spawn_blocking(move || git::write_bitmap(&bitmap_dir)).await;
        }
    }
    let parent_row = match head.parent_commit.as_deref() {
        Some(parent) => state.control.status(repo_id, parent).await?,
        None => None,
    };
    Ok((head, parent_row, config, mirror_dir))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_default_and_parse() {
        let defaults = WorkerSettings::from_lookup(|_| None).unwrap();
        assert_eq!(
            defaults,
            WorkerSettings {
                head_slots: 2,
                background_slots: 1,
                retry_base: Duration::from_millis(2000),
                max_attempts: 5,
            }
        );
        let set = WorkerSettings::from_lookup(|key| {
            Some(
                match key {
                    "RIPCLONE_HEAD_SLOTS" => "3",
                    "RIPCLONE_BACKGROUND_SLOTS" => "0",
                    "RIPCLONE_BUILD_RETRY_BASE_MS" => "50",
                    "RIPCLONE_BUILD_MAX_ATTEMPTS" => "2",
                    _ => return None,
                }
                .to_string(),
            )
        })
        .unwrap();
        assert_eq!(set.head_slots, 3);
        assert_eq!(set.background_slots, 0);
        assert_eq!(set.retry_base, Duration::from_millis(50));
        assert_eq!(set.max_attempts, 2);
        for (key, value) in [
            ("RIPCLONE_HEAD_SLOTS", "0"),
            ("RIPCLONE_HEAD_SLOTS", "two"),
            ("RIPCLONE_BACKGROUND_SLOTS", "-1"),
            ("RIPCLONE_BUILD_RETRY_BASE_MS", "soon"),
            ("RIPCLONE_BUILD_MAX_ATTEMPTS", "0"),
        ] {
            let error =
                WorkerSettings::from_lookup(|k| (k == key).then(|| value.to_string())).unwrap_err();
            assert!(error.to_string().contains(key), "{key}={value}: {error:#}");
        }
    }

    #[test]
    fn retry_delay_doubles_to_the_cap() {
        let base = Duration::from_millis(2000);
        assert_eq!(retry_delay(base, 1), Duration::from_secs(2));
        assert_eq!(retry_delay(base, 2), Duration::from_secs(4));
        assert_eq!(retry_delay(base, 4), Duration::from_secs(16));
        assert_eq!(retry_delay(base, 9), RETRY_CAP);
        assert_eq!(retry_delay(base, i64::MAX), RETRY_CAP);
        assert_eq!(retry_delay(Duration::ZERO, 3), Duration::ZERO);
    }

    #[test]
    fn build_error_classification_by_source_type() {
        let api = |status| s3::Error::Api {
            status,
            code: None,
            message: None,
            request_id: None,
            host_id: None,
            body_snippet: None,
        };
        let io = |kind| anyhow::Error::new(std::io::Error::from(kind));
        for (case, error, retryable) in [
            (
                "s3 transport",
                anyhow::Error::new(s3::Error::transport("connection reset", None))
                    .context("S3 get_object"),
                true,
            ),
            (
                "s3 503",
                anyhow::Error::new(api(StatusCode::SERVICE_UNAVAILABLE)),
                true,
            ),
            (
                "s3 404",
                anyhow::Error::new(api(StatusCode::NOT_FOUND)),
                false,
            ),
            (
                "s3 config",
                anyhow::Error::new(s3::Error::InvalidConfig {
                    message: "bad bucket".into(),
                }),
                false,
            ),
            (
                "io reset",
                io(std::io::ErrorKind::ConnectionReset).context("upload chunk"),
                true,
            ),
            ("io timeout", io(std::io::ErrorKind::TimedOut), true),
            ("io not found", io(std::io::ErrorKind::NotFound), false),
            (
                "io invalid input",
                io(std::io::ErrorKind::InvalidInput),
                false,
            ),
            (
                "upstream retryable",
                anyhow::Error::new(git::UpstreamGitError::new("fetch", true)),
                true,
            ),
            (
                "upstream terminal",
                anyhow::Error::new(git::UpstreamGitError::new("fetch", false)),
                false,
            ),
            (
                "control database",
                anyhow::Error::new(sqlx::Error::PoolTimedOut).context("read parent row"),
                true,
            ),
            ("unknown", anyhow::anyhow!("malformed pack index"), false),
        ] {
            assert_eq!(
                classify_build_error(&error).is_retryable(),
                retryable,
                "{case}"
            );
        }
    }
}
