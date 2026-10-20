//! Build workers: three capped pools of slots that lease rows from the
//! control store.
//!
//! A Head slot and a Files slot build an admitted commit's Head and Files at
//! the same time; both need only the commit. Once Head is ready, a Full slot
//! builds the row's Full, which reuses Head's packs. Each output kind has its
//! own lease, so a Full never waits for its commit's Files, and a new push's
//! Files never waits behind another commit's Full or Head. A slot holds a
//! lease (`RIPCLONE_BUILD_LEASE_SECS`, 60 s) and renews it every third of
//! that. A process never claims its own leases again, even expired ones, so a
//! process that stalls (a sleeping laptop, a long pause) carries on with its
//! builds when it resumes. A lease is lost only when it expired and another
//! process took it; that cancels the build: its git children are stopped,
//! it stops at its next step, and it never publishes. Failures record a retry
//! time instead of sleeping, so no slot waits while holding a lease. A repo
//! holds at most one lease per pool, so one repo's builds never fill a pool,
//! and each pool serves repo owners in turn, so one owner's many repos never
//! hold up another owner's.
//!
//! The mirror is shared by every build of a repo (see [`super::RepoLock`]).
//! Builds read it under a shared lock. A build fetches its commit only when
//! the mirror lacks it, under the repo's write lock, so a Files or Full whose
//! commit Head already fetched never waits on another commit's fetch. Full
//! writes the commit-graph before it builds. After a Files or Full has
//! published, it compacts the mirror if no other build reads it, so the
//! later of a commit's Files and Full usually finds the mirror free.
//!
//! Each output builds incrementally from the same output of its base: the
//! newest commit of the repo with that output ready that is an ancestor of
//! the commit being built (see [`ancestor_base`]). Without one it builds cold.

use super::builder::{
    BuildPhases, SyncPhases, build_files_result, build_full_result, build_head_result,
    report_sync_phases,
};
use super::{RepoLock, ServerState, duration_ms};
use crate::cancel::{self, Cancel};
use crate::cas::Cas;
use crate::control::{BuildRow, Claim, Output, Pool, Published};
use crate::pins::PinSet;
use crate::provider::RepoId;
use crate::{ExactResultKind, git, validation};
use anyhow::{Context, Result};
use axum::http::StatusCode;
use std::collections::BTreeSet;
use std::env;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

const IDLE_POLL: Duration = Duration::from_secs(1);
const RETRY_CAP: Duration = Duration::from_secs(300);
/// Cap on each mirror upkeep step: the commit-graph before a Full, and
/// compaction after a Files or Full. They hold the repo's write lock, which
/// a build waits on only when it must fetch.
const UPKEEP_TIMEOUT: Duration = Duration::from_secs(120);
/// After a failed compaction, a repo's Fulls skip compaction this long.
const COMPACT_BACKOFF: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WorkerSettings {
    /// Concurrent Head builds. At least 1.
    pub head_slots: usize,
    /// Concurrent Files builds. 0 pauses them.
    pub files_slots: usize,
    /// Concurrent Full builds. 0 pauses them.
    pub full_slots: usize,
    /// First retry delay; doubles per attempt up to `RETRY_CAP`.
    pub retry_base: Duration,
    /// Claims of one output kind before it fails.
    pub max_attempts: i64,
    /// How long a claim holds its output without renewing. Renewed every
    /// third of it.
    pub lease: Duration,
}

impl WorkerSettings {
    pub(super) fn from_env() -> Result<Self> {
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        Self::from_lookup(|key| env::var(key).ok(), cores)
    }

    /// Slot defaults scale with `cores`. A Head build is mostly
    /// single-threaded git and gix work plus uploads, so a quarter of the
    /// cores, at least 2. A Files build after the first reads and compresses
    /// only changed bytes and waits on uploads, so it gets as many slots as
    /// Head; one slow repo's Files then does not hold back every other
    /// repo's. An incremental Full packs only the commits since its base and
    /// spends most of its time on uploads, so it gets as many slots as Head
    /// too. A repo holds at most one slot of each pool, so more slots only
    /// help when more repos build at once.
    fn from_lookup(get: impl Fn(&str) -> Option<String>, cores: usize) -> Result<Self> {
        let number = |key: &str, default: usize| -> Result<u64> {
            crate::env::parse_number(key, get(key).as_deref(), default as u64)
        };
        let head_slots = number("RIPCLONE_HEAD_SLOTS", (cores / 4).max(2))?;
        anyhow::ensure!(head_slots >= 1, "RIPCLONE_HEAD_SLOTS must be at least 1");
        let files_slots = number("RIPCLONE_FILES_SLOTS", (cores / 4).max(2))?;
        let full_slots = number("RIPCLONE_FULL_SLOTS", (cores / 4).max(2))?;
        let retry_base = number("RIPCLONE_BUILD_RETRY_BASE_MS", 2000)?;
        let max_attempts = number("RIPCLONE_BUILD_MAX_ATTEMPTS", 5)?;
        anyhow::ensure!(
            max_attempts >= 1,
            "RIPCLONE_BUILD_MAX_ATTEMPTS must be at least 1"
        );
        let lease = number("RIPCLONE_BUILD_LEASE_SECS", 60)?;
        anyhow::ensure!(lease >= 3, "RIPCLONE_BUILD_LEASE_SECS must be at least 3");
        Ok(Self {
            head_slots: usize::try_from(head_slots).context("RIPCLONE_HEAD_SLOTS is too large")?,
            files_slots: usize::try_from(files_slots)
                .context("RIPCLONE_FILES_SLOTS is too large")?,
            full_slots: usize::try_from(full_slots).context("RIPCLONE_FULL_SLOTS is too large")?,
            retry_base: Duration::from_millis(retry_base),
            max_attempts: i64::try_from(max_attempts)
                .context("RIPCLONE_BUILD_MAX_ATTEMPTS is too large")?,
            lease: Duration::from_secs(lease),
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

/// Start every slot. Lease owners are `{pool}-{slot}-{seq}`; the store adds
/// its instance, so they are unique across restarts.
pub(super) fn spawn_workers(state: &ServerState, settings: &WorkerSettings) {
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
                format!("{name}-{slot}"),
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
        match state.control.claim(&owner, pool, settings.lease).await {
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
/// cancels the build, and the slot waits for it to stop before it claims
/// again; another process owns the output now.
async fn run_claim(state: &ServerState, settings: WorkerSettings, pool: Pool, claim: Claim) {
    let claim = Arc::new(claim);
    let cancel = Cancel::new();
    let task_state = state.clone();
    let task_claim = Arc::clone(&claim);
    let task_cancel = cancel.clone();
    // Held until the build ends, so nothing it relies on is deleted before
    // it publishes.
    let pins = PinSet::new(&state.pins);
    let task_pins = Arc::clone(&pins);
    let mut task = tokio::spawn(async move {
        let pins = &task_pins;
        task_cancel
            .scope(async {
                match pool {
                    Pool::Head => build_head(&task_state, settings, &task_claim, pins).await,
                    Pool::Files => build_files(&task_state, settings, &task_claim, pins).await,
                    Pool::Full => build_full(&task_state, settings, &task_claim, pins).await,
                }
            })
            .await
    });
    let every = settings.lease / 3;
    let mut renew = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            joined = &mut task => {
                if let Err(error) = joined {
                    let failure = BuildError::retryable(format!("build task panicked: {error}"));
                    record_failure(state, settings, &claim, &failure).await;
                    discard(state, &claim, &pins, &BTreeSet::new()).await;
                }
                break;
            }
            _ = renew.tick() => match state.control.renew(&claim, settings.lease).await {
                Ok(true) => {}
                Ok(false) => {
                    warn!(
                        "lost {pool:?} lease for {}@{}; cancelling build",
                        claim.repo.storage_key(),
                        claim.build.commit
                    );
                    cancel.cancel();
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
    release(state, &claim).await;
    // A ready Head makes the row's Full claimable.
    if pool == Pool::Head {
        state.wake(Pool::Full).notify_one();
    }
}

/// Give up the claim's lease, retrying until the store takes it. This process
/// never claims its own leases again, so one left behind would keep the
/// output from building until a restart.
async fn release(state: &ServerState, claim: &Claim) {
    let mut delay = Duration::from_millis(100);
    while let Err(error) = state.control.release(claim).await {
        error!(
            "release lease for {}@{} failed, retrying in {delay:?}: {error:#}",
            claim.repo.storage_key(),
            claim.build.commit
        );
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

/// Record a failed output: pending with a retry time when the error is
/// retryable, failed otherwise. The store fails it once attempts run out.
async fn record_failure(
    state: &ServerState,
    settings: WorkerSettings,
    claim: &Claim,
    error: &BuildError,
) {
    let kind = claim.kind();
    let attempts = claim.attempts();
    let retry_after = error
        .is_retryable()
        .then(|| retry_delay(settings.retry_base, attempts));
    let gives_up = retry_after.is_none() || attempts >= settings.max_attempts;
    let target = format!("{}@{}", claim.repo.storage_key(), claim.build.commit);
    if let Err(write_error) = state
        .control
        .fail(claim, error.message(), retry_after)
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
            "{kind} build attempt {attempts} failed for {target}, retrying in {:?}: {error}",
            retry_after.unwrap_or_default()
        );
    }
}

/// Publish a built output, or record why it could not be built. A cancelled
/// build records nothing: its lease is gone and another worker owns the row.
/// Every artifact the build stored or relied on that its published output
/// does not name, such as a level merged away within the build, or all of
/// them when nothing was published, is queued for deletion.
async fn settle(
    state: &ServerState,
    settings: WorkerSettings,
    claim: &Claim,
    built: Result<Output>,
    pins: &PinSet,
) -> Option<Published> {
    let mut named: BTreeSet<String> = built.as_ref().map_or_else(
        |_| BTreeSet::new(),
        |output| output.artifacts().into_iter().map(str::to_string).collect(),
    );
    let published = publish_or_fail(state, settings, claim, built).await;
    if !published.is_some_and(|published| published.applied) {
        named.clear();
    }
    discard(state, claim, pins, &named).await;
    published
}

async fn publish_or_fail(
    state: &ServerState,
    settings: WorkerSettings,
    claim: &Claim,
    built: Result<Output>,
) -> Option<Published> {
    let kind = claim.kind();
    if Cancel::current().is_some_and(|cancel| cancel.is_cancelled()) {
        warn!(
            "{kind} build for {}@{} stopped: the lease was lost",
            claim.repo.storage_key(),
            short(&claim.build.commit)
        );
        return None;
    }
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
            record_failure(state, settings, claim, &classify_build_error(&error)).await;
            None
        }
    }
}

/// Queue every artifact the build pinned but `keep` does not name for
/// deletion now. Deletion keeps those that any published output names.
async fn discard(state: &ServerState, claim: &Claim, pins: &PinSet, keep: &BTreeSet<String>) {
    if let Err(error) = super::purge::queue_unnamed(state, pins, keep).await {
        warn!(
            "queue the unnamed {} artifacts of {}@{} for deletion: {error:#}",
            claim.kind(),
            claim.repo.storage_key(),
            short(&claim.build.commit)
        );
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
}

pub(super) async fn repo_lock(state: &ServerState, repo_id: &RepoId) -> Arc<RepoLock> {
    let mut map = state.sync_locks.lock().await;
    map.entry(repo_id.storage_key()).or_default().clone()
}

/// Make the mirror hold `commit`. When it already does (Full after
/// Head, a retry) the write lock is not taken, so a slow fetch of another
/// commit of the repo does not hold this build. Head and Files of a new
/// commit start together: the one that waited for the write lock finds the
/// commit fetched and does not fetch again. The caller holds `lock.read`.
///
/// Without the write lock, a fetch may still be running, so the check is the
/// fetch's ref, which git writes only once the commit's history is in place
/// and connected. The commit object alone is not enough: it may be in a pack
/// whose fetch has not finished or failed. Such a commit is fetched again
/// under the write lock; git skips the transfer when the history is there.
async fn ensure_commit(
    state: &ServerState,
    repo_id: &RepoId,
    commit: &str,
    mirror_dir: &std::path::Path,
    lock: &RepoLock,
) -> Result<()> {
    let has_commit = || {
        let (dir, wanted) = (mirror_dir.to_path_buf(), commit.to_string());
        async move {
            cancel::spawn_blocking(move || git::mirror_fetched_commit(&dir, &wanted))
                .await
                .context("mirror check task")
        }
    };
    if has_commit().await? {
        return Ok(());
    }
    let _writing = lock.write.lock().await;
    Cancel::check_current()?;
    if has_commit().await? {
        return Ok(());
    }
    fetch_commit(state, repo_id, commit, mirror_dir).await
}

/// Fetch `commit` into the mirror. The caller holds the repo's write lock.
/// The fetch is bounded by `RIPCLONE_FETCH_TIMEOUT_SECS` and git's low-speed
/// limit, and stopped if the build is cancelled.
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
        .credentials
        .get(repo_id)
        .await
        .with_context(|| format!("fetch credential for {}", repo_id.storage_key()))?;
    let (mirror_dir, repo_id, commit) = (
        mirror_dir.to_path_buf(),
        repo_id.clone(),
        commit.to_string(),
    );
    cancel::spawn_blocking(move || {
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

async fn build_head(
    state: &ServerState,
    settings: WorkerSettings,
    claim: &Claim,
    pins: &Arc<PinSet>,
) {
    let started = Instant::now();
    let mut phases = SyncPhases::default();
    let built = head_output(state, claim, &state.cas.with_pins(pins), &mut phases).await;
    let publish_start = Instant::now();
    let Some(published) = settle(state, settings, claim, built, pins).await else {
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
    cas: &Cas,
    phases: &mut SyncPhases,
) -> Result<Output> {
    let repo_id = &claim.repo;
    let commit = claim.build.commit.as_str();
    validation::validate_object_id(commit).context("validate admitted commit")?;
    info!("building Head for {}@{commit}", repo_id.storage_key());
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let lock = repo_lock(state, repo_id).await;
    let _reading = lock.read.read().await;
    let t = Instant::now();
    ensure_commit(state, repo_id, commit, &mirror_dir, &lock).await?;
    phases.mirror_fetch_ms = Some(duration_ms(t.elapsed()));
    // The tags this fetch brought are in the mirror now (see `tags`); they
    // publish while Head builds, and before Head is ready.
    let build = async {
        let (prep_dir, prep_commit) = (mirror_dir.clone(), commit.to_string());
        let parent = cancel::spawn_blocking(move || -> Result<Option<String>> {
            let resolved = git::resolve_commit(&prep_dir, &prep_commit)
                .with_context(|| format!("resolve admitted commit {prep_commit}"))?;
            anyhow::ensure!(
                resolved == prep_commit,
                "admitted commit resolved unexpectedly: requested {prep_commit}, got {resolved}"
            );
            Ok(git::parent_commit(&prep_dir, &prep_commit).ok().flatten())
        })
        .await
        .context("mirror prep task")??;

        // Head packs are a base plus levels: the new base Full of the base
        // commit packed, else the base commit's Head packs, else none.
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
                    levels: head.levels.clone(),
                })
            });
        Cancel::check_current()?;
        let result = build_head_result(
            cas,
            &mirror_dir,
            commit,
            parent.clone(),
            base,
            // Its files table is a delta on the base Head's.
            base_head.map(|head| head.clonepack.clone()),
            &state.storage,
            phases,
        )
        .await?;
        Ok(Output::Head { result, parent })
    };
    // The tags store through the build's pins, so what they stored is
    // queued with the build's artifacts if the build fails and drops them.
    let (output, _) = tokio::try_join!(
        build,
        super::refs::publish_pinned(state, repo_id, super::refs::Scope::Tags, cas)
    )?;
    Ok(output)
}

async fn build_files(
    state: &ServerState,
    settings: WorkerSettings,
    claim: &Claim,
    pins: &Arc<PinSet>,
) {
    let started = Instant::now();
    let mut phases = BuildPhases::default();
    let cas = state.cas.with_pins(pins);
    let mirror_dir = state.repo_root.join(claim.repo.mirror_dir_name());
    let lock = repo_lock(state, &claim.repo).await;
    let built = async {
        let start = Instant::now();
        let _reading = lock.read.read().await;
        phases.record("lock_wait", start);
        let start = Instant::now();
        ensure_commit(state, &claim.repo, &claim.build.commit, &mirror_dir, &lock).await?;
        phases.record("fetch", start);
        let (dir, commit) = (mirror_dir.clone(), claim.build.commit.clone());
        let parent =
            cancel::spawn_blocking(move || git::parent_commit(&dir, &commit).ok().flatten())
                .await
                .context("parent lookup task")?;
        let start = Instant::now();
        let base_row = ancestor_base(state, claim, ExactResultKind::Files, &mirror_dir).await?;
        let base_files = base_row
            .as_ref()
            .and_then(|row| row.files.ready().map(|files| (row.commit.as_str(), files)));
        phases.record("base", start);
        Cancel::check_current()?;
        build_files_result(
            &cas,
            &mirror_dir,
            &claim.build.commit,
            parent,
            base_files,
            &state.storage,
            &mut phases,
        )
        .await
        .map(Output::Files)
    }
    .await;
    let ready = built.is_ok();
    phases.report("files", &claim.repo, &claim.build.commit, ready, started);
    settle(state, settings, claim, built, pins).await;
    compact_mirror(&claim.repo, &mirror_dir, &lock).await;
}

async fn build_full(
    state: &ServerState,
    settings: WorkerSettings,
    claim: &Claim,
    pins: &Arc<PinSet>,
) {
    let started = Instant::now();
    let mut phases = BuildPhases::default();
    let cas = state.cas.with_pins(pins);
    let mirror_dir = state.repo_root.join(claim.repo.mirror_dir_name());
    let lock = repo_lock(state, &claim.repo).await;
    let built = async {
        let head = ready_head(claim)?;
        write_commit_graph(&claim.repo, &mirror_dir, &lock, &mut phases).await;
        let start = Instant::now();
        let _reading = lock.read.read().await;
        phases.record("lock_wait", start);
        let start = Instant::now();
        ensure_commit(state, &claim.repo, &claim.build.commit, &mirror_dir, &lock).await?;
        phases.record("fetch", start);
        // Full builds on the history levels of its base. A repo's Fulls are
        // claimed in admission order, so an earlier push's Full has settled.
        let start = Instant::now();
        let base = ancestor_base(state, claim, ExactResultKind::Full, &mirror_dir).await?;
        let base = base.and_then(|row| Some((row.full.ready()?.clone(), row.commit)));
        let levels = (base.as_ref())
            .map(|(full, _)| full.history_levels.clone())
            .unwrap_or_default();
        phases.record("base", start);
        let own_base = own_history_base(state, claim, base.map(|(_, commit)| commit)).await;
        Cancel::check_current()?;
        build_full_result(
            &cas,
            &mirror_dir,
            &claim.repo,
            &claim.build.commit,
            &head,
            levels,
            own_base,
            &state.storage,
            &mut phases,
        )
        .await
        .map(Output::Full)
    }
    .await;
    // The first build's Full waits for the refresh its Head started, so
    // a full clone of a new repo gets every branch and tag from the server.
    // A failed refresh does not throw the Full away: the poller refreshes
    // a repo with no branches, and until then clones fetch them upstream.
    if built.is_ok() {
        let start = Instant::now();
        if let Err(error) = super::refs::first_branches(state, &claim.repo).await {
            warn!(
                "Full of {}@{} is ready without the repo's branches: {error:#}",
                claim.repo.storage_key(),
                short(&claim.build.commit)
            );
        }
        phases.record("refs", start);
    }
    let ready = built.is_ok();
    phases.report("full", &claim.repo, &claim.build.commit, ready, started);
    settle(state, settings, claim, built, pins).await;
    compact_mirror(&claim.repo, &mirror_dir, &lock).await;
}

/// The commit a Full's own history leaves out. It is the older published
/// refs base, which the newer one holds, so a clone given either can use
/// it. Before the first publish it is `built_on`, the commit whose Full it
/// builds on, which a later base holds unless its branch was force-pushed.
/// None for a commit first admitted on the default branch: a clone of it
/// gets its history from the base's.
async fn own_history_base(
    state: &ServerState,
    claim: &Claim,
    built_on: Option<String>,
) -> Option<String> {
    let refs = match state.control.get_refs(&claim.repo).await {
        Ok(refs) => refs.unwrap_or_default(),
        Err(error) => {
            warn!(
                "read refs of {} for its Full: {error:#}",
                claim.repo.storage_key()
            );
            return None;
        }
    };
    let default = (refs.branches.iter().chain(&refs.ready))
        .find_map(|branches| branches.default_branch.as_deref());
    if let Some(default) = default
        && claim.build.ref_name.as_deref() == Some(&format!("refs/heads/{default}"))
    {
        return None;
    }
    let base = |branches: Option<crate::control::RepoBranches>| branches.and_then(|b| b.base);
    base(refs.ready).or(base(refs.branches)).or(built_on)
}

/// The Head result Files and Full build on.
fn ready_head(claim: &Claim) -> Result<crate::HeadResult> {
    claim
        .build
        .head
        .ready()
        .cloned()
        .context("Head result is not ready")
}

/// Before a Full, write the commit-graph, best-effort and bounded by
/// [`UPKEEP_TIMEOUT`]. It needs the write lock and is skipped while another
/// build holds it.
async fn write_commit_graph(
    repo_id: &RepoId,
    mirror_dir: &std::path::Path,
    lock: &RepoLock,
    phases: &mut BuildPhases,
) {
    let Ok(_writing) = lock.write.try_lock() else {
        return;
    };
    if !mirror_dir.exists() {
        return;
    }
    let target = repo_id.storage_key();
    let dir = mirror_dir.to_path_buf();
    let start = Instant::now();
    match cancel::spawn_blocking(move || git::write_commit_graph(&dir, UPKEEP_TIMEOUT)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!("commit-graph for {target} failed: {error:#}"),
        Err(error) => warn!("commit-graph task for {target} failed: {error}"),
    }
    phases.record("commit_graph", start);
}

/// After a Files or Full, compact the mirror (see [`git::compact_mirror`]),
/// best-effort and bounded by [`UPKEEP_TIMEOUT`]. Compaction deletes packs,
/// so it needs the mirror to itself: it runs only after the build has
/// stopped reading the mirror and is skipped while any other build reads it
/// or holds the write lock. A commit's Files and Full run together, so the
/// later of the two usually finds the mirror free; a later build tries again
/// otherwise.
async fn compact_mirror(repo_id: &RepoId, mirror_dir: &std::path::Path, lock: &RepoLock) {
    let backing_off = lock
        .compact_failed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_some_and(|failed| failed.elapsed() < COMPACT_BACKOFF);
    if backing_off || !mirror_dir.exists() {
        return;
    }
    let Ok(_alone) = lock.read.try_write() else {
        return;
    };
    let Ok(_writing) = lock.write.try_lock() else {
        return;
    };
    let target = repo_id.storage_key();
    let dir = mirror_dir.to_path_buf();
    let start = Instant::now();
    let compacted = cancel::spawn_blocking(move || git::compact_mirror(&dir, UPKEEP_TIMEOUT))
        .await
        .map_err(anyhow::Error::from)
        .and_then(|result| result);
    match compacted {
        Ok(done) if done != git::MirrorCompaction::default() => {
            info!(
                "compacted mirror of {target} in {} ms: {done:?}",
                duration_ms(start.elapsed())
            );
        }
        Ok(_) => {}
        Err(error) => {
            warn!("mirror compaction for {target} failed: {error:#}");
            *lock
                .compact_failed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Instant::now());
        }
    }
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
    let found = cancel::spawn_blocking(move || {
        crate::gix_util::with_ancestry(&dir, |is_ancestor| {
            let ancestors: Vec<String> = candidates
                .into_iter()
                .filter(|candidate| match is_ancestor(candidate, &tip) {
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
            // Admission order is not history order (a commit admitted late, a
            // ref moved back), so the base is the first ancestor no other
            // descends from.
            let newest = ancestors.iter().find(|ancestor| {
                !ancestors.iter().any(|other| {
                    other != *ancestor && is_ancestor(ancestor, other).unwrap_or(false)
                })
            });
            newest.or(ancestors.first()).cloned()
        })
    })
    .await
    .context("base lookup task")?
    .with_context(|| format!("open mirror for {target}"))?;
    let Some(base) = found else {
        info!("{kind} base for {target}: none, building cold");
        return Ok(None);
    };
    info!("{kind} base for {target}: {}", short(&base));
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
        let defaults = WorkerSettings::from_lookup(|_| None, 8).unwrap();
        assert_eq!(
            defaults,
            WorkerSettings {
                head_slots: 2,
                files_slots: 2,
                full_slots: 2,
                retry_base: Duration::from_millis(2000),
                max_attempts: 5,
                lease: Duration::from_secs(60),
            }
        );
        for (cores, slots) in [(1, 2), (16, 4), (64, 16)] {
            let scaled = WorkerSettings::from_lookup(|_| None, cores).unwrap();
            assert_eq!(
                (scaled.head_slots, scaled.files_slots, scaled.full_slots),
                (slots, slots, slots),
                "{cores} cores"
            );
        }
        let set = WorkerSettings::from_lookup(
            |key| {
                Some(
                    match key {
                        "RIPCLONE_HEAD_SLOTS" => "3",
                        "RIPCLONE_FILES_SLOTS" => "0",
                        "RIPCLONE_FULL_SLOTS" => "4",
                        "RIPCLONE_BUILD_RETRY_BASE_MS" => "50",
                        "RIPCLONE_BUILD_MAX_ATTEMPTS" => "2",
                        "RIPCLONE_BUILD_LEASE_SECS" => "6",
                        _ => return None,
                    }
                    .to_string(),
                )
            },
            8,
        )
        .unwrap();
        assert_eq!(set.head_slots, 3);
        assert_eq!(set.files_slots, 0);
        assert_eq!(set.full_slots, 4);
        assert_eq!(set.retry_base, Duration::from_millis(50));
        assert_eq!(set.max_attempts, 2);
        assert_eq!(set.lease, Duration::from_secs(6));
        for (key, value) in [
            ("RIPCLONE_HEAD_SLOTS", "0"),
            ("RIPCLONE_HEAD_SLOTS", "two"),
            ("RIPCLONE_FILES_SLOTS", "-1"),
            ("RIPCLONE_FULL_SLOTS", "many"),
            ("RIPCLONE_BUILD_RETRY_BASE_MS", "soon"),
            ("RIPCLONE_BUILD_MAX_ATTEMPTS", "0"),
            ("RIPCLONE_BUILD_LEASE_SECS", "2"),
        ] {
            let error = WorkerSettings::from_lookup(|k| (k == key).then(|| value.to_string()), 8)
                .unwrap_err();
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

    /// A control store whose first `failures` releases fail; everything else,
    /// and every later release, goes to `inner`.
    struct FailingReleases {
        inner: std::sync::Arc<dyn crate::control::ControlStore>,
        failures: std::sync::atomic::AtomicUsize,
        attempts: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::control::ControlStore for FailingReleases {
        async fn admit(
            &self,
            repo: &RepoId,
            commit: &str,
            ref_name: Option<&str>,
        ) -> Result<(BuildRow, bool)> {
            self.inner.admit(repo, commit, ref_name).await
        }
        async fn retry(&self, repo: &RepoId, commit: &str) -> Result<Option<BuildRow>> {
            self.inner.retry(repo, commit).await
        }
        async fn claim(&self, owner: &str, pool: Pool, lease: Duration) -> Result<Option<Claim>> {
            self.inner.claim(owner, pool, lease).await
        }
        async fn renew(&self, claim: &Claim, lease: Duration) -> Result<bool> {
            self.inner.renew(claim, lease).await
        }
        async fn publish(&self, claim: &Claim, output: Output) -> Result<Published> {
            self.inner.publish(claim, output).await
        }
        async fn fail(
            &self,
            claim: &Claim,
            error: &str,
            retry_after: Option<Duration>,
        ) -> Result<()> {
            self.inner.fail(claim, error, retry_after).await
        }
        async fn release(&self, claim: &Claim) -> Result<()> {
            use std::sync::atomic::Ordering::SeqCst;
            self.attempts.fetch_add(1, SeqCst);
            let failing = self
                .failures
                .fetch_update(SeqCst, SeqCst, |left| left.checked_sub(1))
                .is_ok();
            if failing {
                anyhow::bail!("control database is locked");
            }
            self.inner.release(claim).await
        }
        async fn reset_leases(&self) -> Result<u64> {
            self.inner.reset_leases().await
        }
        async fn status(&self, repo: &RepoId, commit: &str) -> Result<Option<BuildRow>> {
            self.inner.status(repo, commit).await
        }
        async fn list_builds(&self, repo: &RepoId) -> Result<Vec<BuildRow>> {
            self.inner.list_builds(repo).await
        }
        async fn latest_ready_full(
            &self,
            repo: &RepoId,
            ref_name: &str,
        ) -> Result<Option<(String, crate::FullResult)>> {
            self.inner.latest_ready_full(repo, ref_name).await
        }
        async fn ready_commits(
            &self,
            repo: &RepoId,
            kind: ExactResultKind,
            prefer_ref: Option<&str>,
            except: &str,
            limit: usize,
        ) -> Result<Vec<String>> {
            self.inner
                .ready_commits(repo, kind, prefer_ref, except, limit)
                .await
        }
        async fn add_repo(&self, repo: &RepoId, owner: Option<&str>) -> Result<bool> {
            self.inner.add_repo(repo, owner).await
        }
        async fn remove_repo(&self, repo: &RepoId) -> Result<bool> {
            self.inner.remove_repo(repo).await
        }
        async fn get_repo(&self, repo: &RepoId) -> Result<Option<crate::control::AddedRepo>> {
            self.inner.get_repo(repo).await
        }
        async fn list_repos(&self) -> Result<Vec<crate::control::AddedRepo>> {
            self.inner.list_repos().await
        }
        async fn purge_repo(&self, repo: &RepoId, due_at: i64) -> Result<crate::control::Purged> {
            self.inner.purge_repo(repo, due_at).await
        }
        async fn queue_deletions(&self, hashes: &[String], due_at: i64) -> Result<()> {
            self.inner.queue_deletions(hashes, due_at).await
        }
        async fn due_deletions(&self, now: i64, limit: usize) -> Result<Vec<String>> {
            self.inner.due_deletions(now, limit).await
        }
        async fn deletable(&self, hash: &str, now: i64) -> Result<bool> {
            self.inner.deletable(hash, now).await
        }
        async fn dequeue_deletions(&self, hashes: &[String], now: i64) -> Result<()> {
            self.inner.dequeue_deletions(hashes, now).await
        }
        async fn knows_repo(&self, repo: &RepoId) -> Result<bool> {
            self.inner.knows_repo(repo).await
        }
        async fn known_repos(&self) -> Result<Vec<RepoId>> {
            self.inner.known_repos().await
        }
        async fn get_refs(&self, repo: &RepoId) -> Result<Option<crate::control::RepoRefs>> {
            self.inner.get_refs(repo).await
        }
        async fn set_refs(&self, repo: &RepoId, refs: &crate::control::RepoRefs) -> Result<bool> {
            self.inner.set_refs(repo, refs).await
        }
        async fn health(&self) -> Result<()> {
            self.inner.health().await
        }
    }

    /// A release the store refuses is retried until it lands: the lease
    /// stays held while the store refuses, and the output can be claimed
    /// again once the release goes through.
    #[tokio::test]
    async fn a_refused_release_is_retried_until_the_lease_is_free() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let tmp = tempfile::tempdir().unwrap();
        let mut state = crate::server::tests::test_state(&tmp).await;
        let store = std::sync::Arc::new(FailingReleases {
            inner: state.control.clone(),
            failures: AtomicUsize::new(2),
            attempts: AtomicUsize::new(0),
        });
        state.control = store.clone();
        let repo = RepoId::github("acme/api");
        let commit = "a".repeat(40);
        let lease = Duration::from_secs(60);
        state.control.admit(&repo, &commit, None).await.unwrap();
        let claim = state
            .control
            .claim("test-head", Pool::Head, lease)
            .await
            .unwrap()
            .expect("the new row's Head is claimable");

        let releasing = tokio::spawn({
            let state = state.clone();
            async move { release(&state, &claim).await }
        });
        // The first refusal comes at once; the retry waits 100 ms.
        while store.attempts.load(SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert!(
            state
                .control
                .claim("test-head", Pool::Head, lease)
                .await
                .unwrap()
                .is_none(),
            "a refused release leaves the lease held"
        );
        tokio::time::timeout(Duration::from_secs(5), releasing)
            .await
            .expect("the release retried until it landed")
            .unwrap();
        assert_eq!(
            store.attempts.load(SeqCst),
            3,
            "two refusals, then the release"
        );
        let again = state
            .control
            .claim("test-head", Pool::Head, lease)
            .await
            .unwrap()
            .expect("a released Head is claimable again");
        assert_eq!(again.build.commit, commit);
    }
}
