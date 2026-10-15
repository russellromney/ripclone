//! Build workers: three capped pools of slots that lease rows from the
//! control store.
//!
//! A Head slot fetches the admitted commit, builds Head and publishes it. A
//! Files slot builds Files of a row whose Head is ready. A Full slot builds
//! Full of a row whose Files is ready or failed, so a new push's Files never
//! waits behind another commit's Full. A slot holds a 60 s lease and renews it
//! every 20 s; a lost lease aborts the build. Failures record a retry time
//! instead of sleeping, so no slot waits while holding a lease.
//!
//! Each output builds incrementally from the same output of its base: the
//! newest commit of the repo with that output ready that is an ancestor of
//! the commit being built (see [`ancestor_base`]). Without one it builds cold.

use super::api::row_info;
use super::builder::{
    SyncPhases, build_files_result, build_full_result, build_head_result, report_sync_bench,
    report_sync_phases, sweep_stale_tempdirs,
};
use super::{ServerState, duration_ms, fetch_semaphore};
use crate::control::{BuildRow, Claim, Output, Pool, Published};
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
    /// Concurrent Files builds. 0 pauses Files, and so Full.
    pub files_slots: usize,
    /// Concurrent Full builds. 0 pauses them.
    pub full_slots: usize,
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
            crate::env::parse_number(key, get(key).as_deref(), default)
        };
        let head_slots = number("RIPCLONE_HEAD_SLOTS", 2)?;
        anyhow::ensure!(head_slots >= 1, "RIPCLONE_HEAD_SLOTS must be at least 1");
        let files_slots = number("RIPCLONE_FILES_SLOTS", 1)?;
        let full_slots = number("RIPCLONE_FULL_SLOTS", 1)?;
        let retry_base = number("RIPCLONE_BUILD_RETRY_BASE_MS", 2000)?;
        let max_attempts = number("RIPCLONE_BUILD_MAX_ATTEMPTS", 5)?;
        anyhow::ensure!(
            max_attempts >= 1,
            "RIPCLONE_BUILD_MAX_ATTEMPTS must be at least 1"
        );
        Ok(Self {
            head_slots: usize::try_from(head_slots).context("RIPCLONE_HEAD_SLOTS is too large")?,
            files_slots: usize::try_from(files_slots)
                .context("RIPCLONE_FILES_SLOTS is too large")?,
            full_slots: usize::try_from(full_slots).context("RIPCLONE_FULL_SLOTS is too large")?,
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
    for (pool, slots, name) in [
        (Pool::Head, settings.head_slots, "head"),
        (Pool::Files, settings.files_slots, "files"),
        (Pool::Full, settings.full_slots, "full"),
    ] {
        for slot in 0..slots {
            tokio::spawn(run_slot(
                state.clone(),
                *settings,
                pool,
                format!("{boot}-{name}-{slot}"),
            ));
        }
    }
    info!(
        head_slots = settings.head_slots,
        files_slots = settings.files_slots,
        full_slots = settings.full_slots,
        "build workers started"
    );
}

async fn run_slot(state: ServerState, settings: WorkerSettings, pool: Pool, slot: String) {
    let wake = Arc::clone(state.wake(pool));
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
            Pool::Files => build_files(&task_state, settings, &task_claim).await,
            Pool::Full => build_full(&task_state, settings, &task_claim).await,
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
                    record_failure(state, settings, &claim, pool_kind(pool), &failure).await;
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
    // The row is claimable by the next pool only once released.
    match pool {
        Pool::Head => state.wake(Pool::Files).notify_one(),
        Pool::Files => state.wake(Pool::Full).notify_one(),
        Pool::Full => {}
    }
}

/// The output a pool builds.
fn pool_kind(pool: Pool) -> ExactResultKind {
    match pool {
        Pool::Head => ExactResultKind::Head,
        Pool::Files => ExactResultKind::Files,
        Pool::Full => ExactResultKind::Full,
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
    // recorded by Full of the base commit, else the base its Head used, else
    // none. The base commit's files table also seeds this one.
    let base_row = ancestor_base(state, claim, ExactResultKind::Head, &mirror_dir).await?;
    let base_head = base_row.as_ref().and_then(|row| row.head.ready());
    let base = base_row
        .as_ref()
        .and_then(|row| row.full.ready())
        .and_then(|full| full.head_base.clone())
        .or_else(|| {
            base_head.map(|head| crate::HeadBase {
                commit: head.base_commit.clone(),
                packs: head.base_packs.clone(),
            })
        });
    let result = build_head_result(
        &state.cas,
        &mirror_dir,
        commit,
        parent.clone(),
        base_head,
        base,
        &state.storage,
        phases,
    )
    .await?;
    Ok(Output::Head { result, parent })
}

async fn build_files(state: &ServerState, settings: WorkerSettings, claim: &Claim) {
    let built = async {
        let (head, mirror_dir) = background_prep(state, claim).await?;
        let config = state
            .control
            .get_config(&claim.repo)
            .await?
            .unwrap_or_default();
        let base_row = ancestor_base(state, claim, ExactResultKind::Files, &mirror_dir).await?;
        let base_files = base_row
            .as_ref()
            .and_then(|row| row.files.ready().map(|files| (row.commit.as_str(), files)));
        build_files_result(
            &state.cas,
            &mirror_dir,
            &claim.build.commit,
            &head,
            base_files,
            &state.storage,
            config.compression_level(),
        )
        .await
        .map(Output::Files)
    }
    .await;
    settle(state, settings, claim, ExactResultKind::Files, built).await;
}

async fn build_full(state: &ServerState, settings: WorkerSettings, claim: &Claim) {
    let built = async {
        let (head, mirror_dir) = background_prep(state, claim).await?;
        // Full builds on the history levels of its base. A repo's Fulls are
        // claimed in admission order, so an earlier push's Full has settled.
        let levels = ancestor_base(state, claim, ExactResultKind::Full, &mirror_dir)
            .await?
            .as_ref()
            .and_then(|row| row.full.ready())
            .map(|full| full.history_levels.clone())
            .unwrap_or_default();
        // Best-effort: a reachability bitmap makes the history walk much
        // faster on large repositories. It takes minutes on Linux-sized
        // mirrors, so it runs here, outside the repo lock and off the Files
        // path.
        let bitmap_dir = mirror_dir.clone();
        let _ = tokio::task::spawn_blocking(move || git::write_bitmap(&bitmap_dir)).await;
        build_full_result(
            &state.cas,
            &mirror_dir,
            &claim.repo,
            &claim.build.commit,
            &head,
            levels,
            &state.storage,
        )
        .await
        .map(Output::Full)
    }
    .await;
    settle(state, settings, claim, ExactResultKind::Full, built).await;
}

/// What Files and Full share: the ready Head and a mirror holding the
/// commit.
async fn background_prep(
    state: &ServerState,
    claim: &Claim,
) -> Result<(crate::HeadResult, std::path::PathBuf)> {
    let repo_id = &claim.repo;
    let commit = claim.build.commit.as_str();
    let head = claim
        .build
        .head
        .ready()
        .cloned()
        .context("Head result is not ready")?;
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    {
        let lock = repo_lock(&state.sync_locks, repo_id).await;
        let _guard = lock.lock().await;
        // Usually a no-op: the Head build fetched the commit.
        fetch_commit(state, repo_id, commit, &mirror_dir).await?;
    }
    Ok((head, mirror_dir))
}

/// Ready commits checked, newest first, for a build's base.
const BASE_CANDIDATES: usize = 32;

/// The row whose `kind` output a build of the claimed commit reuses: the
/// newest row of the repo with `kind` ready whose commit is an ancestor of
/// the claimed commit in the mirror. "Newest" is by history: among the
/// ancestors found, one no other of them descends from, rows of the claimed
/// ref and later admissions first. A commit that is not an ancestor (after
/// a force push) or not in the mirror is never a base, so a build never
/// carries objects the commit cannot reach. `None` builds `kind` cold.
async fn ancestor_base(
    state: &ServerState,
    claim: &Claim,
    kind: ExactResultKind,
    mirror_dir: &std::path::Path,
) -> Result<Option<BuildRow>> {
    let repo_id = &claim.repo;
    let commit = claim.build.commit.as_str();
    let candidates = state
        .control
        .ready_commits(
            repo_id,
            kind,
            claim.build.ref_name.as_deref(),
            commit,
            BASE_CANDIDATES,
        )
        .await?;
    let target = format!("{}@{}", repo_id.storage_key(), short(commit));
    let (dir, tip, log_target) = (mirror_dir.to_path_buf(), commit.to_string(), target.clone());
    let found = tokio::task::spawn_blocking(move || {
        let ancestors: Vec<String> = candidates
            .into_iter()
            .filter(|candidate| match git::is_ancestor(&dir, candidate, &tip) {
                Ok(is_ancestor) => is_ancestor,
                Err(error) => {
                    info!(
                        "{kind} base candidate {} for {log_target} is not usable: {error:#}",
                        short(candidate)
                    );
                    false
                }
            })
            .collect();
        // Admission times are in seconds, so admission order alone can rank
        // an older ancestor first.
        let newest = match ancestors.len() {
            0 | 1 => None,
            _ => git::independent_commits(&dir, &ancestors).ok(),
        };
        let base = ancestors
            .iter()
            .find(|ancestor| {
                newest
                    .as_ref()
                    .is_none_or(|newest| newest.contains(*ancestor))
            })
            .or(ancestors.first())?
            .clone();
        let distance = git::count_commits_since(&dir, &base, &tip).ok();
        Some((base, distance))
    })
    .await
    .context("base lookup task")?;
    let Some((base, distance)) = found else {
        info!("{kind} base for {target}: none, building cold");
        return Ok(None);
    };
    let distance = distance.map_or_else(|| "?".to_string(), |n| n.to_string());
    info!(
        "{kind} base for {target}: {} ({distance} commits back)",
        short(&base)
    );
    // A ready output never changes, so the row still has it.
    state.control.status(repo_id, &base).await
}

fn short(commit: &str) -> &str {
    &commit[..7.min(commit.len())]
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
                files_slots: 1,
                full_slots: 1,
                retry_base: Duration::from_millis(2000),
                max_attempts: 5,
            }
        );
        let set = WorkerSettings::from_lookup(|key| {
            Some(
                match key {
                    "RIPCLONE_HEAD_SLOTS" => "3",
                    "RIPCLONE_FILES_SLOTS" => "0",
                    "RIPCLONE_FULL_SLOTS" => "4",
                    "RIPCLONE_BUILD_RETRY_BASE_MS" => "50",
                    "RIPCLONE_BUILD_MAX_ATTEMPTS" => "2",
                    _ => return None,
                }
                .to_string(),
            )
        })
        .unwrap();
        assert_eq!(set.head_slots, 3);
        assert_eq!(set.files_slots, 0);
        assert_eq!(set.full_slots, 4);
        assert_eq!(set.retry_base, Duration::from_millis(50));
        assert_eq!(set.max_attempts, 2);
        for (key, value) in [
            ("RIPCLONE_HEAD_SLOTS", "0"),
            ("RIPCLONE_HEAD_SLOTS", "two"),
            ("RIPCLONE_FILES_SLOTS", "-1"),
            ("RIPCLONE_FULL_SLOTS", "many"),
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
