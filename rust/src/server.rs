use crate::archive::ArchiveBuilder;
use crate::auth::access::{AccessDecision, AccessVerifier, HttpAccessVerifier};
use crate::auth::broker::{CredentialBroker, broker_from_env};
use crate::backends;
use crate::cas::Cas;
use crate::clonepack::{
    ChunkRef, ClonepackManifest, collect_manifest_hashes, hash_from_hex, hash_to_hex,
    manifest_chunk_refs,
};
use crate::git;
use crate::metrics::{Metrics, SyncPhaseMetrics};
use crate::oidc::OidcVerifier;
use crate::pack::PackBuilder;
use crate::provider::{ProviderInstance, ProviderRegistry, RepoId};
use crate::queue::{BuildError, BuildJob, EnqueueOutcome, JobQueueRef, JobState};
use crate::ref_store::{AddedRepo, AddedRepoSource, RefStore};
use crate::storage::StorageRef;
use crate::validation;
use crate::webhook::{EventKind, WebhookConfig};
use crate::{ExactResultKind, RefInfo};
use anyhow::{Context, Result};
use axum::{
    Form, Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, DefaultBodyLimit, OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{delete, get, post},
};
use futures::{SinkExt, StreamExt, TryStreamExt};
use prost::Message;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tracing::{error, info, warn};

/// Test-only deterministic barrier for artifact downloads. The first matching
/// artifact response splits its body into a stream: it sends the prefix,
/// signals `entered`, waits on `proceed`, and then either sends the remainder
/// or closes the connection (when `close_on_proceed`). This lets tests pause a
/// download mid-body without relying on wall-clock timing or first-request
/// fault injection.
///
/// A response matches when its body is larger than `after_bytes` and it
/// satisfies [`BarrierTarget`].
#[derive(Clone)]
pub struct ArtifactBarrier {
    pub after_bytes: usize,
    pub target: BarrierTarget,
    pub entered: Arc<StdMutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    pub proceed: Arc<StdMutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
    pub close_on_proceed: bool,
    pub range_behavior: ArtifactRangeBehavior,
    pub range_requests: Arc<StdMutex<Vec<String>>>,
    /// Every artifact request observed while the fixture is installed. The
    /// optional string is the request's Range header.
    pub artifact_requests: Arc<StdMutex<Vec<(String, Option<String>)>>>,
    pub max_chunk_sent: Arc<AtomicUsize>,
    pub consumed: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Default)]
pub enum ArtifactRangeBehavior {
    #[default]
    Normal,
    Ignore,
    InvalidContentRange,
    CorruptBody,
}

/// Which artifact the barrier holds.
#[derive(Clone)]
pub enum BarrierTarget {
    /// The first response whose body exceeds `after_bytes`.
    FirstLargeBody,
    /// Only the artifact whose content hash the test has written into the slot,
    /// and only once it has been named — an empty slot matches nothing. This
    /// lets a test start the server, sync, discover the exact chunk or pack it
    /// wants to hold, and then arm the barrier.
    Hash(Arc<StdMutex<Option<String>>>),
}

impl BarrierTarget {
    fn matches(&self, hash: &str) -> bool {
        match self {
            BarrierTarget::FirstLargeBody => true,
            BarrierTarget::Hash(slot) => {
                slot.lock().unwrap_or_else(|e| e.into_inner()).as_deref() == Some(hash)
            }
        }
    }
}

static TEST_ARTIFACT_BARRIER: StdMutex<Option<ArtifactBarrier>> = StdMutex::new(None);

/// Install a barrier for the next server constructed in this process. Returns a
/// guard that clears the slot when dropped, so a panicked test cannot leak the
/// barrier into the next test in the same binary.
pub fn set_test_artifact_barrier(barrier: ArtifactBarrier) -> TestArtifactBarrierGuard {
    *TEST_ARTIFACT_BARRIER
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(barrier);
    TestArtifactBarrierGuard
}

/// RAII guard for [`set_test_artifact_barrier`].
pub struct TestArtifactBarrierGuard;

impl Drop for TestArtifactBarrierGuard {
    fn drop(&mut self) {
        *TEST_ARTIFACT_BARRIER
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
    }
}

fn take_test_artifact_barrier() -> Option<ArtifactBarrier> {
    TEST_ARTIFACT_BARRIER
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
}

/// Narrow test-only synchronization for the immutable-admission proof. The
/// production path has no counters or barriers unless `RIPCLONE_TESTING=1`
/// and an integration test installs this probe. Each gate is notification-
/// based so tests never infer concurrency from a sleep duration.
pub struct AdmissionTestBarrier {
    armed: AtomicBool,
    released: AtomicBool,
    entered: AtomicUsize,
    signal: tokio::sync::watch::Sender<u64>,
}

impl Default for AdmissionTestBarrier {
    fn default() -> Self {
        let (signal, _) = tokio::sync::watch::channel(0);
        Self {
            armed: AtomicBool::new(false),
            released: AtomicBool::new(false),
            entered: AtomicUsize::new(0),
            signal,
        }
    }
}

impl AdmissionTestBarrier {
    fn signal(&self) {
        self.signal.send_modify(|epoch| {
            *epoch = epoch.wrapping_add(1);
        });
    }

    fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.signal.subscribe()
    }

    pub fn arm(&self) {
        self.entered.store(0, Ordering::SeqCst);
        self.released.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
        self.signal();
    }

    pub fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.released.store(true, Ordering::SeqCst);
        self.signal();
    }

    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.signal();
    }

    pub fn entered(&self) -> usize {
        self.entered.load(Ordering::SeqCst)
    }

    pub async fn wait_until_entered(&self, count: usize) {
        while self.entered() < count {
            let mut signal = self.subscribe();
            if self.entered() >= count {
                break;
            }
            let _ = signal.changed().await;
        }
    }

    async fn wait(&self) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.signal();
        loop {
            let mut signal = self.subscribe();
            if self.released.load(Ordering::SeqCst) || !self.armed.load(Ordering::SeqCst) {
                return;
            }
            let _ = signal.changed().await;
        }
    }
}

/// Operation counters and barriers used by `e2e_sync_admission`. These are
/// intentionally test-only hooks; they are not metrics, API state, or a
/// production scheduler mechanism.
pub struct AdmissionTestProbe {
    pub before_claim: AdmissionTestBarrier,
    pub after_claim: AdmissionTestBarrier,
    pub fetch_entry: AdmissionTestBarrier,
    pub builder_entry: AdmissionTestBarrier,
    pub after_head_entry: AdmissionTestBarrier,
    pub before_full_publish: AdmissionTestBarrier,
    pub before_files_publish: AdmissionTestBarrier,
    pub after_full_publish: AdmissionTestBarrier,
    pub after_files_publish: AdmissionTestBarrier,
    pub embedded_idle_wait: AdmissionTestBarrier,
    pub before_admission_tx: AdmissionTestBarrier,
    pub inside_admission_tx: AdmissionTestBarrier,
    pub enqueue_attempts: AtomicUsize,
    pub queue_inserts: AtomicUsize,
    pub coalesces: AtomicUsize,
    pub pending_responses: AtomicUsize,
    pub tip_probes: AtomicUsize,
    pub exact_fetches: AtomicUsize,
    pub builder_entries: AtomicUsize,
    pub head_builds: AtomicUsize,
    pub full_builds: AtomicUsize,
    pub files_builds: AtomicUsize,
    pub bitmap_writes: AtomicUsize,
    pub head_publishes: AtomicUsize,
    pub full_publishes: AtomicUsize,
    pub files_publishes: AtomicUsize,
    pub ref_store_writes: AtomicUsize,
    pub artifact_uploads: AtomicUsize,
    pub embedded_notification_wakes: AtomicUsize,
    pub embedded_fallback_polls: AtomicUsize,
    pub claim_losses: AtomicUsize,
    repo_reads_denied: AtomicBool,
    pub fetch_targets: StdMutex<Vec<String>>,
    pub builder_targets: StdMutex<Vec<String>>,
    pub failure_targets: StdMutex<Vec<(String, String)>>,
    full_failure_targets: StdMutex<std::collections::HashSet<String>>,
    files_failure_targets: StdMutex<std::collections::HashSet<String>>,
    pub http_trace: StdMutex<Vec<String>>,
    admission_tx_target: StdMutex<Option<String>>,
    inside_admission_tx_target: StdMutex<Option<String>>,
    full_notify: Arc<tokio::sync::Notify>,
    failure_notify: Arc<tokio::sync::Notify>,
    http_notify: Arc<tokio::sync::Notify>,
    claim_loss_notify: Arc<tokio::sync::Notify>,
}

impl Default for AdmissionTestProbe {
    fn default() -> Self {
        Self {
            before_claim: AdmissionTestBarrier::default(),
            after_claim: AdmissionTestBarrier::default(),
            fetch_entry: AdmissionTestBarrier::default(),
            builder_entry: AdmissionTestBarrier::default(),
            after_head_entry: AdmissionTestBarrier::default(),
            before_full_publish: AdmissionTestBarrier::default(),
            before_files_publish: AdmissionTestBarrier::default(),
            after_full_publish: AdmissionTestBarrier::default(),
            after_files_publish: AdmissionTestBarrier::default(),
            embedded_idle_wait: AdmissionTestBarrier::default(),
            before_admission_tx: AdmissionTestBarrier::default(),
            inside_admission_tx: AdmissionTestBarrier::default(),
            enqueue_attempts: AtomicUsize::new(0),
            queue_inserts: AtomicUsize::new(0),
            coalesces: AtomicUsize::new(0),
            pending_responses: AtomicUsize::new(0),
            tip_probes: AtomicUsize::new(0),
            exact_fetches: AtomicUsize::new(0),
            builder_entries: AtomicUsize::new(0),
            head_builds: AtomicUsize::new(0),
            full_builds: AtomicUsize::new(0),
            files_builds: AtomicUsize::new(0),
            bitmap_writes: AtomicUsize::new(0),
            head_publishes: AtomicUsize::new(0),
            full_publishes: AtomicUsize::new(0),
            files_publishes: AtomicUsize::new(0),
            ref_store_writes: AtomicUsize::new(0),
            artifact_uploads: AtomicUsize::new(0),
            embedded_notification_wakes: AtomicUsize::new(0),
            embedded_fallback_polls: AtomicUsize::new(0),
            claim_losses: AtomicUsize::new(0),
            repo_reads_denied: AtomicBool::new(false),
            fetch_targets: StdMutex::new(Vec::new()),
            builder_targets: StdMutex::new(Vec::new()),
            failure_targets: StdMutex::new(Vec::new()),
            full_failure_targets: StdMutex::new(std::collections::HashSet::new()),
            files_failure_targets: StdMutex::new(std::collections::HashSet::new()),
            http_trace: StdMutex::new(Vec::new()),
            admission_tx_target: StdMutex::new(None),
            inside_admission_tx_target: StdMutex::new(None),
            full_notify: Arc::new(tokio::sync::Notify::new()),
            failure_notify: Arc::new(tokio::sync::Notify::new()),
            http_notify: Arc::new(tokio::sync::Notify::new()),
            claim_loss_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl AdmissionTestProbe {
    /// Make subsequent repository authorization checks fail. This models an
    /// active access revocation after a clone has already received a signed
    /// artifact URL.
    pub fn deny_repo_reads(&self) {
        self.repo_reads_denied.store(true, Ordering::SeqCst);
    }

    pub fn allow_repo_reads(&self) {
        self.repo_reads_denied.store(false, Ordering::SeqCst);
    }

    pub fn fail_full_for(&self, commit: &str) {
        self.full_failure_targets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(commit.to_string());
    }

    pub fn allow_full_for(&self, commit: &str) {
        self.full_failure_targets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(commit);
    }

    pub fn fail_files_for(&self, commit: &str) {
        self.files_failure_targets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(commit.to_string());
    }

    pub fn allow_files_for(&self, commit: &str) {
        self.files_failure_targets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(commit);
    }

    pub fn hold_admission_transaction(&self, commit: &str) {
        *self
            .admission_tx_target
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(commit.to_string());
        self.before_admission_tx.arm();
    }

    pub fn hold_inside_admission_transaction(&self, commit: &str) {
        *self
            .inside_admission_tx_target
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(commit.to_string());
        self.inside_admission_tx.arm();
    }

    pub async fn wait_until_full_published(&self, count: usize) {
        while self.full_publishes.load(Ordering::SeqCst) < count {
            let notified = self.full_notify.notified();
            if self.full_publishes.load(Ordering::SeqCst) >= count {
                break;
            }
            notified.await;
        }
    }

    pub async fn wait_until_failure(&self, count: usize) {
        loop {
            let notified = self.failure_notify.notified();
            if self
                .failure_targets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len()
                >= count
            {
                return;
            }
            notified.await;
        }
    }

    pub async fn wait_until_http_trace_len(&self, count: usize) {
        loop {
            let notified = self.http_notify.notified();
            if self
                .http_trace
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len()
                >= count
            {
                return;
            }
            notified.await;
        }
    }

    pub async fn wait_until_claim_lost(&self, count: usize) {
        while self.claim_losses.load(Ordering::SeqCst) < count {
            let notified = self.claim_loss_notify.notified();
            if self.claim_losses.load(Ordering::SeqCst) >= count {
                break;
            }
            notified.await;
        }
    }
}

static ADMISSION_TEST_PROBE: StdMutex<Option<Arc<AdmissionTestProbe>>> = StdMutex::new(None);

pub struct AdmissionTestProbeGuard {
    probe: Arc<AdmissionTestProbe>,
}

pub fn install_admission_test_probe(probe: Arc<AdmissionTestProbe>) -> AdmissionTestProbeGuard {
    *ADMISSION_TEST_PROBE
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&probe));
    AdmissionTestProbeGuard { probe }
}

impl Drop for AdmissionTestProbeGuard {
    fn drop(&mut self) {
        self.probe.before_claim.disarm();
        self.probe.after_claim.disarm();
        self.probe.fetch_entry.disarm();
        self.probe.builder_entry.disarm();
        self.probe.after_head_entry.disarm();
        self.probe.before_full_publish.disarm();
        self.probe.before_files_publish.disarm();
        self.probe.after_full_publish.disarm();
        self.probe.after_files_publish.disarm();
        self.probe.embedded_idle_wait.disarm();
        self.probe.before_admission_tx.disarm();
        self.probe.inside_admission_tx.disarm();
        let mut slot = ADMISSION_TEST_PROBE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot
            .as_ref()
            .is_some_and(|installed| Arc::ptr_eq(installed, &self.probe))
        {
            slot.take();
        }
    }
}

fn admission_test_probe() -> Option<Arc<AdmissionTestProbe>> {
    std::env::var_os("RIPCLONE_TESTING")?;
    ADMISSION_TEST_PROBE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// A single interception point in production control flow where a test may
/// want to observe or pause execution. One variant per point; adding a new
/// hook means adding a variant here, not a new function.
pub enum TestStage<'a> {
    BeforeClaim,
    AfterClaim,
    BeforeAdmissionTx(&'a str),
    InsideAdmissionTx(&'a str),
    TipProbe,
    RefStoreWrite,
    ArtifactUpload,
    FetchEntry(Option<&'a str>),
    BuilderEntry(&'a str),
    HeadBuild,
    /// Merges the in-process `after_head_entry` probe wait with the
    /// cross-process, file-signaled after-Head barrier (used when the test
    /// drives a separately spawned server binary).
    AfterHeadBarrier(&'a str),
    HeadPublished,
    FullBuild(&'a str),
    FilesBuild(&'a str),
    BitmapWrite,
    BeforeFullPublish,
    AfterFullPublish,
    FullPublished(&'a str),
    BeforeFilesPublish,
    AfterFilesPublish,
    FilesPublished,
    EmbeddedIdleWait,
    EmbeddedWake {
        fallback: bool,
    },
    ClaimLost,
    BuildFailure {
        commit: Option<&'a str>,
        message: &'a str,
    },
    Http(String),
    Enqueue(EnqueueOutcome),
    PendingResponse,
    RepoReadsDenied,
    /// Cross-process, file-signaled barrier used only by the direct
    /// worker-crash tests, keyed by an upload-pipeline stage name
    /// (`before_upload`, `during_upload`, `after_upload`,
    /// `before_ready_publication`).
    BuildCrash {
        stage: &'static str,
        commit: &'a str,
    },
}

/// The one call form production code uses to reach every test hook:
/// `test_hook(TestStage::Whatever).await?`. A no-op unless `RIPCLONE_TESTING`
/// is set and the relevant fixture (probe or barrier directory) is installed.
/// The bool result is meaningful only for stages that report one
/// (`FullBuild`, `FilesBuild`, `RepoReadsDenied`); everywhere else it is
/// `false` and ignored.
pub async fn test_hook(stage: TestStage<'_>) -> Result<bool> {
    match stage {
        TestStage::BeforeClaim => {
            if let Some(probe) = admission_test_probe() {
                probe.before_claim.wait().await;
            }
        }
        TestStage::AfterClaim => {
            if let Some(probe) = admission_test_probe() {
                probe.after_claim.wait().await;
            }
        }
        TestStage::BeforeAdmissionTx(commit) => {
            if let Some(probe) = admission_test_probe() {
                let held = probe
                    .admission_tx_target
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .as_deref()
                    == Some(commit);
                if held {
                    probe.before_admission_tx.wait().await;
                }
            }
        }
        TestStage::InsideAdmissionTx(commit) => {
            if let Some(probe) = admission_test_probe() {
                let held = probe
                    .inside_admission_tx_target
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .as_deref()
                    == Some(commit);
                if held {
                    probe.inside_admission_tx.wait().await;
                }
            }
        }
        TestStage::TipProbe => {
            if let Some(probe) = admission_test_probe() {
                probe.tip_probes.fetch_add(1, Ordering::SeqCst);
            }
        }
        TestStage::RefStoreWrite => {
            if let Some(probe) = admission_test_probe() {
                probe.ref_store_writes.fetch_add(1, Ordering::SeqCst);
            }
        }
        TestStage::ArtifactUpload => {
            if let Some(probe) = admission_test_probe() {
                probe.artifact_uploads.fetch_add(1, Ordering::SeqCst);
            }
        }
        TestStage::FetchEntry(target) => {
            if let Some(probe) = admission_test_probe() {
                probe.exact_fetches.fetch_add(1, Ordering::SeqCst);
                if let Some(target) = target {
                    probe
                        .fetch_targets
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(target.to_string());
                }
                probe.fetch_entry.wait().await;
            }
        }
        TestStage::BuilderEntry(commit) => {
            if let Some(probe) = admission_test_probe() {
                probe.builder_entries.fetch_add(1, Ordering::SeqCst);
                probe
                    .builder_targets
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(commit.to_string());
                probe.builder_entry.wait().await;
            }
        }
        TestStage::HeadBuild => {
            if let Some(probe) = admission_test_probe() {
                probe.head_builds.fetch_add(1, Ordering::SeqCst);
            }
        }
        TestStage::HeadPublished => {
            if let Some(probe) = admission_test_probe() {
                probe.head_publishes.fetch_add(1, Ordering::SeqCst);
            }
        }
        TestStage::AfterHeadBarrier(commit) => {
            if !explicit_test_mode(std::env::var_os("RIPCLONE_TESTING").as_deref()) {
                return Ok(false);
            }
            if let Some(probe) = admission_test_probe() {
                probe.after_head_entry.wait().await;
            }
            let Some(dir) =
                std::env::var_os("RIPCLONE_TEST_AFTER_HEAD_BARRIER_DIR").map(PathBuf::from)
            else {
                return Ok(false);
            };
            if let Some(target) = std::env::var_os("RIPCLONE_TEST_AFTER_HEAD_BARRIER_COMMIT")
                && target.to_str() != Some(commit)
            {
                return Ok(false);
            }
            std::fs::create_dir_all(&dir).context("create test after-Head barrier directory")?;
            std::fs::write(dir.join("entered"), format!("{commit}\n"))
                .context("signal test after-Head barrier")?;
            let deadline = Instant::now() + Duration::from_secs(60);
            while !dir.join("proceed").exists() {
                if Instant::now() >= deadline {
                    anyhow::bail!("test after-Head barrier was not released within 60 seconds");
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        TestStage::FullBuild(commit) => {
            if let Some(probe) = admission_test_probe() {
                probe.full_builds.fetch_add(1, Ordering::SeqCst);
                return Ok(probe
                    .full_failure_targets
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .contains(commit));
            }
        }
        TestStage::FilesBuild(commit) => {
            if let Some(probe) = admission_test_probe() {
                probe.files_builds.fetch_add(1, Ordering::SeqCst);
                return Ok(probe
                    .files_failure_targets
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .contains(commit));
            }
        }
        TestStage::BitmapWrite => {
            if let Some(probe) = admission_test_probe() {
                probe.bitmap_writes.fetch_add(1, Ordering::SeqCst);
            }
        }
        TestStage::BeforeFullPublish => {
            if let Some(probe) = admission_test_probe() {
                probe.before_full_publish.wait().await;
            }
        }
        TestStage::AfterFullPublish => {
            if let Some(probe) = admission_test_probe() {
                probe.after_full_publish.wait().await;
            }
        }
        TestStage::FullPublished(commit) => {
            if let Some(probe) = admission_test_probe() {
                probe.full_publishes.fetch_add(1, Ordering::SeqCst);
                probe.full_notify.notify_waiters();
                tracing::debug!("admission test observed full publication for {commit}");
            }
        }
        TestStage::BeforeFilesPublish => {
            if let Some(probe) = admission_test_probe() {
                probe.before_files_publish.wait().await;
            }
        }
        TestStage::AfterFilesPublish => {
            if let Some(probe) = admission_test_probe() {
                probe.after_files_publish.wait().await;
            }
        }
        TestStage::FilesPublished => {
            if let Some(probe) = admission_test_probe() {
                probe.files_publishes.fetch_add(1, Ordering::SeqCst);
            }
        }
        TestStage::EmbeddedIdleWait => {
            if let Some(probe) = admission_test_probe() {
                probe.embedded_idle_wait.wait().await;
            }
        }
        TestStage::EmbeddedWake { fallback } => {
            if let Some(probe) = admission_test_probe() {
                if fallback {
                    probe.embedded_fallback_polls.fetch_add(1, Ordering::SeqCst);
                } else {
                    probe
                        .embedded_notification_wakes
                        .fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        TestStage::ClaimLost => {
            if let Some(probe) = admission_test_probe() {
                probe.claim_losses.fetch_add(1, Ordering::SeqCst);
                probe.claim_loss_notify.notify_waiters();
            }
        }
        TestStage::BuildFailure { commit, message } => {
            if let Some(probe) = admission_test_probe() {
                probe
                    .failure_targets
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((commit.unwrap_or_default().to_string(), message.to_string()));
                probe.failure_notify.notify_waiters();
            }
        }
        TestStage::Http(event) => {
            if let Some(probe) = admission_test_probe() {
                probe
                    .http_trace
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(event);
                probe.http_notify.notify_waiters();
            }
        }
        TestStage::Enqueue(outcome) => {
            if let Some(probe) = admission_test_probe() {
                probe.enqueue_attempts.fetch_add(1, Ordering::SeqCst);
                match outcome {
                    EnqueueOutcome::Enqueued => {
                        probe.queue_inserts.fetch_add(1, Ordering::SeqCst);
                    }
                    EnqueueOutcome::Coalesced => {
                        probe.coalesces.fetch_add(1, Ordering::SeqCst);
                    }
                    EnqueueOutcome::Full => {}
                }
            }
        }
        TestStage::PendingResponse => {
            if let Some(probe) = admission_test_probe() {
                probe.pending_responses.fetch_add(1, Ordering::SeqCst);
            }
        }
        TestStage::RepoReadsDenied => {
            return Ok(admission_test_probe()
                .is_some_and(|probe| probe.repo_reads_denied.load(Ordering::SeqCst)));
        }
        TestStage::BuildCrash { stage, commit } => {
            if !test_build_crash_barrier_matches(stage, commit) {
                return Ok(false);
            }
            let Some(dir) =
                std::env::var_os("RIPCLONE_TEST_BUILD_CRASH_BARRIER_DIR").map(PathBuf::from)
            else {
                return Ok(false);
            };
            std::fs::create_dir_all(&dir).context("create test build-crash barrier directory")?;
            let entered = dir.join("entered");
            if entered.exists() {
                return Ok(false);
            }
            std::fs::write(&entered, format!("{stage} {commit}\n"))
                .context("signal test build-crash barrier")?;
            let deadline = Instant::now() + Duration::from_secs(60);
            while !dir.join("proceed").exists() {
                if Instant::now() >= deadline {
                    anyhow::bail!("test build-crash barrier was not released within 60 seconds");
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
    Ok(false)
}

fn explicit_test_mode(value: Option<&std::ffi::OsStr>) -> bool {
    value == Some(std::ffi::OsStr::new("1"))
}

fn test_build_crash_barrier_matches(stage: &str, commit: &str) -> bool {
    explicit_test_mode(std::env::var_os("RIPCLONE_TESTING").as_deref())
        && std::env::var("RIPCLONE_TEST_BUILD_CRASH_STAGE").as_deref() == Ok(stage)
        && std::env::var("RIPCLONE_TEST_BUILD_CRASH_COMMIT")
            .ok()
            .is_none_or(|target| target == commit)
}
#[derive(Clone)]
pub struct ServerState {
    pub cas: Cas,
    pub storage: StorageRef,
    pub repo_root: PathBuf,
    pub ref_store: Arc<dyn RefStore>,
    pub provider_registry: ProviderRegistry,
    pub broker: Arc<dyn CredentialBroker>,
    pub token_hash: Option<String>,
    /// Signing material for short-lived session tokens (`ripclone auth login`).
    /// `None` when no signing secret is available (only the token *hash* is
    /// configured); session-token issuance is then disabled.
    pub jwt: Option<Arc<crate::auth::jwt::JwtKeys>>,
    pub metrics: Arc<Metrics>,
    pub rate_limiter: RateLimiter,
    pub build_queue: JobQueueRef,
    /// Holds the server's process-ownership lock and concrete control driver.
    /// Standalone API workers never construct this value.
    pub control_db: Option<Arc<crate::control::ControlDb>>,
    /// Backs worker-facing `/v1/jobs/*` endpoints so a token-only standalone
    /// worker never touches the database. `None` only in a worker's non-serving
    /// `ServerState`.
    pub worker_queue: Option<Arc<crate::queue::SqlJobQueue>>,
    pub build_queue_depth: Arc<AtomicUsize>,
    pub oidc_verifier: Option<Arc<OidcVerifier>>,
    /// Webhook receiver config: per-provider HMAC secret + optional repo
    /// allowlist. A provider with no configured secret returns 503. Reads
    /// `RIPCLONE_WEBHOOK_SECRET_<provider>`.
    pub webhook_config: Arc<WebhookConfig>,
    /// Per-repo mutexes so concurrent syncs for the same repo cannot corrupt
    /// the bare mirror directory.
    pub sync_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Count of artifact GETs served, used only by the test-only fault injector.
    /// Per-server so tests don't leak state into each other.
    pub artifact_fetch_count: Arc<AtomicUsize>,
    /// Test-only fault injection: make the first N artifact GETs fail with 503.
    /// Read once from `RIPCLONE_TEST_FAIL_FIRST_FETCHES` at construction (0 =
    /// off), so the hot path never touches the environment in production.
    pub fail_first_fetches: usize,
    /// Test-only deterministic barrier for the first artifact download that is
    /// larger than `after_bytes`. See [`ArtifactBarrier`].
    pub artifact_barrier: Option<ArtifactBarrier>,
    /// Cached `/readyz` result `(checked_at, ready)`. Bounds backend probe cost
    /// (S3 round-trips) and damps load-balancer flapping on a transient blip.
    pub readyz_cache: Arc<std::sync::Mutex<Option<(Instant, bool)>>>,
    /// Per-repo read authorization (AU1): proves the caller may read a private
    /// repo (public repos are anonymous). Used by every repo-read entry point
    /// before serving content or signing URLs, unless `require_repo_auth` is off.
    pub access_verifier: Arc<dyn AccessVerifier>,
    /// When true (default), private repos are gated by `access_verifier` on every
    /// read. Set false by `RIPCLONE_TRUST_GATEWAY=1` for a single-tenant
    /// self-host that fully trusts whoever holds the shared server token (the old
    /// behavior); then visibility falls back to the client-supplied header.
    pub require_repo_auth: bool,
}

impl ServerState {
    /// Assemble state for a standalone `ripclone-worker`. It uses the real
    /// durable backends but none of the HTTP-only features (auth, rate limiting,
    /// OIDC, fault injection) since it never serves requests — it only runs
    /// [`process_build_job`]. It builds its own provider registry + credential
    /// broker from the environment, exactly as the server does, so it can resolve
    /// upstream credentials for the repos it builds.
    pub fn for_worker(
        b: backends::Backends,
        queue: JobQueueRef,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let provider_registry = ProviderRegistry::load().context("load provider registry")?;
        let broker = broker_from_env(provider_registry.clone())?;
        Ok(ServerState {
            cas: b.cas,
            storage: b.storage,
            repo_root: b.repo_root,
            ref_store: b.ref_store,
            provider_registry,
            broker,
            token_hash: None,
            jwt: None,
            metrics,
            rate_limiter: RateLimiter::new(60, 10.0),
            build_queue: queue,
            control_db: None,
            // A worker never serves the farm-out endpoints itself.
            worker_queue: None,
            build_queue_depth: Arc::new(AtomicUsize::new(0)),
            oidc_verifier: None,
            // No webhook secret here (worker has no HTTP; tests install their own).
            webhook_config: Arc::new(WebhookConfig::empty()),
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            artifact_fetch_count: Arc::new(AtomicUsize::new(0)),
            fail_first_fetches: 0,
            artifact_barrier: None,
            readyz_cache: Arc::new(std::sync::Mutex::new(None)),
            // The worker never serves reads; a verifier is required by the type
            // but unused, and auth enforcement is irrelevant here.
            access_verifier: Arc::new(HttpAccessVerifier::new()),
            require_repo_auth: false,
        })
    }
}

/// Whether per-repo read authz is enforced. On by default (multi-tenant safe);
/// `RIPCLONE_TRUST_GATEWAY=1` turns it off for a single-tenant self-host that
/// trusts the shared server token as the only authz layer.
fn require_repo_auth_from_env() -> bool {
    !std::env::var("RIPCLONE_TRUST_GATEWAY")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Read the test-only fault-injection threshold once at startup. Logs loudly if
/// it is active so it can never silently degrade a production server.
fn fail_first_fetches_from_env() -> usize {
    let n = std::env::var("RIPCLONE_TEST_FAIL_FIRST_FETCHES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    if n > 0 {
        tracing::warn!(
            "TEST FAULT INJECTION ACTIVE: failing the first {n} artifact fetches \
             (RIPCLONE_TEST_FAIL_FIRST_FETCHES); this must NOT be set in production"
        );
    }
    n
}

/// Simple in-memory token-bucket rate limiter keyed by real client IP.
/// The map is bounded and pruned periodically to avoid unbounded memory growth.
#[derive(Clone)]
pub struct RateLimiter {
    buckets: Arc<StdMutex<HashMap<String, (Instant, f64)>>>,
    max_burst: u32,
    restore_rate_per_sec: f64,
    max_entries: usize,
}

impl RateLimiter {
    pub fn new(max_burst: u32, restore_rate_per_sec: f64) -> Self {
        let restore_rate_per_sec = if restore_rate_per_sec.is_finite() {
            restore_rate_per_sec.max(0.0)
        } else {
            0.0
        };
        Self {
            buckets: Arc::new(StdMutex::new(HashMap::new())),
            max_burst,
            restore_rate_per_sec,
            max_entries: 10_000,
        }
    }

    pub fn check(&self, key: &str) -> bool {
        let now = Instant::now();
        // Recover from a poisoned mutex rather than wedging the server.
        let mut buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());

        // Prune stale entries before adding a new one.
        let stale_threshold = Duration::from_secs(3600);
        buckets.retain(|_, (last, _)| now.duration_since(*last) < stale_threshold);
        if buckets.len() >= self.max_entries && !buckets.contains_key(key) {
            // Map is full of live entries and this IP is new: evict the oldest.
            if let Some(oldest) = buckets
                .iter()
                .min_by_key(|(_, (last, _))| *last)
                .map(|(k, _)| k.clone())
            {
                buckets.remove(&oldest);
            }
        }

        let entry = buckets
            .entry(key.to_string())
            .or_insert_with(|| (now, f64::from(self.max_burst)));
        let elapsed = now.duration_since(entry.0).as_secs_f64();
        entry.1 =
            (entry.1 + elapsed * self.restore_rate_per_sec).clamp(0.0, f64::from(self.max_burst));
        entry.0 = now;
        let allowed = if entry.1 < 1.0 {
            false
        } else {
            entry.1 -= 1.0;
            true
        };
        drop(buckets);
        allowed
    }
}

fn parse_rate_limit_settings(
    burst: Option<&str>,
    restore_rate_per_sec: Option<&str>,
) -> Result<(u32, f64)> {
    let burst = burst
        .map(|value| {
            value
                .parse()
                .context("RIPCLONE_RATE_LIMIT_BURST must be an unsigned integer")
        })
        .transpose()?
        .unwrap_or(60);
    let restore_rate_per_sec: f64 = restore_rate_per_sec
        .map(|value| {
            value
                .parse()
                .context("RIPCLONE_RATE_LIMIT_PER_SEC must be a number")
        })
        .transpose()?
        .unwrap_or(10.0);
    anyhow::ensure!(
        restore_rate_per_sec.is_finite() && restore_rate_per_sec >= 0.0,
        "RIPCLONE_RATE_LIMIT_PER_SEC must be finite and non-negative"
    );
    Ok((burst, restore_rate_per_sec))
}

#[derive(Deserialize)]
pub struct SyncRequest {
    #[serde(default = "default_branch_value")]
    pub branch: String,
    /// Optional git rev to resolve instead of the branch tip (e.g. `HEAD~5` or
    /// a SHA). The resolved commit is the build and result identity.
    #[serde(default)]
    pub rev: Option<String>,
}

#[derive(Deserialize)]
pub struct AddRequest {
    #[serde(default = "default_added_repo_source")]
    pub source: AddedRepoSource,
}

#[derive(Deserialize)]
pub struct RefQuery {
    pub result: ExactResultKind,
    /// Optional git rev to resolve instead of the branch tip (e.g. "HEAD~5").
    /// Pairs with `sync?rev=...`: clone the artifacts built for that commit.
    #[serde(default)]
    pub rev: Option<String>,
    /// Exact commit learned from an earlier response in this clone operation.
    /// Unlike `rev`, this is metadata-only and never contacts the upstream or
    /// schedules work.
    #[serde(default)]
    pub pinned: Option<String>,
    /// Opt in to a one-commit Full-clone top-up plan on a pending pinned lookup.
    /// Ignored on initial selector, rev-targeted, and non-Full requests.
    #[serde(default)]
    pub top_up: bool,
}

fn default_added_repo_source() -> AddedRepoSource {
    AddedRepoSource::Api
}

#[derive(Deserialize)]
pub struct BuildRequest {
    pub owner: String,
    pub repo: String,
    pub commit: String,
    #[serde(rename = "ref")]
    pub ref_name: String,
}

#[derive(Serialize)]
pub struct BuildResponse {
    pub status: String,
    pub queue_depth: usize,
    pub commit: String,
    pub branch: String,
}

#[derive(Serialize)]
pub struct ArtifactPendingResponse {
    pub code: &'static str,
    pub commit: String,
    pub branch: String,
    pub status: &'static str,
    pub queue_depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_up_supported: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_up_base: Option<RefResponse>,
}

#[derive(Serialize)]
pub struct ExactRevisionUnavailableResponse {
    pub error: String,
    pub commit: String,
    pub branch: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncPhases {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mirror_fetch_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_graph_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_packs_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skeleton_build_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_table_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prebuilt_index_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_head_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_publish_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_head_ms: Option<u64>,
}

impl From<&SyncPhases> for SyncPhaseMetrics {
    fn from(phases: &SyncPhases) -> Self {
        Self {
            mirror_fetch_ms: phases.mirror_fetch_ms,
            commit_graph_ms: phases.commit_graph_ms,
            head_packs_ms: phases.head_packs_ms,
            skeleton_build_ms: phases.skeleton_build_ms,
            files_table_ms: phases.files_table_ms,
            prebuilt_index_ms: phases.prebuilt_index_ms,
            upload_head_ms: phases.upload_head_ms,
            ref_publish_ms: phases.ref_publish_ms,
            publish_head_ms: phases.publish_head_ms,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    #[serde(flatten)]
    pub ref_info: RefResponse,
    pub status: String,
    pub phases: SyncPhases,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SyncBuildResult {
    pub info: RefInfo,
    pub status: String,
    pub phases: SyncPhases,
}

fn default_branch_value() -> String {
    "HEAD".to_string()
}

/// Resolve a `{*rest}` path segment from `/v1/repos/{*rest}/...` into a
/// `(RepoId, ProviderInstance)` pair.
///
/// The first path segment MUST be a registered provider instance id; the
/// remainder is the opaque repo path. Callers
/// must address repos as `/v1/repos/{provider}/{path}/...`, even for the
/// built-in `github` default instance.
fn resolve_repo_id<'a>(
    registry: &'a ProviderRegistry,
    rest: &str,
) -> Option<(RepoId, &'a ProviderInstance)> {
    let segments: Vec<&str> = rest.split('/').collect();
    if segments.len() < 2 {
        return None;
    }
    let provider_id = segments[0];
    let path = segments[1..].join("/");
    let provider = registry.get(provider_id)?;
    Some((
        RepoId {
            provider: provider.id.clone(),
            path,
        },
        provider,
    ))
}

fn unknown_provider_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "unknown provider".to_string(),
        }),
    )
        .into_response()
}

fn repo_not_added_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": "repo not added; run `ripclone add <repo>`",
            "code": "repo_not_added",
        })),
    )
        .into_response()
}

async fn repo_is_added(state: &ServerState, repo_id: &RepoId) -> Result<bool, Response> {
    state
        .ref_store
        .load_added_repo(repo_id)
        .await
        .map(|repo| repo.is_some())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("added repo lookup failed: {e}"),
                }),
            )
                .into_response()
        })
}

fn upstream_token_from_headers(headers: &HeaderMap) -> Option<secrecy::SecretString> {
    headers
        .get("X-Upstream-Token")
        .and_then(|v| v.to_str().ok())
        .map(|s| secrecy::SecretString::new(s.to_string().into()))
}

/// Validate an `owner` or `repo` path segment. GitHub identifiers are limited
/// to ASCII alphanumeric plus `.`, `-`, and `_`, must not be empty, and must
/// not contain path separators.
fn validate_repo_id(id: &str) -> Result<()> {
    if id.is_empty() {
        anyhow::bail!("repo identifier must not be empty");
    }
    if id.len() > 128 {
        anyhow::bail!("repo identifier too long: {}", id.len());
    }
    if id.contains('/') || id.contains('\\') || id.contains('\0') {
        anyhow::bail!("repo identifier contains path separator: {}", id);
    }
    if id == "." || id == ".." {
        anyhow::bail!("repo identifier cannot be '.' or '..'");
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
    {
        anyhow::bail!("repo identifier contains invalid characters: {}", id);
    }
    Ok(())
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

fn reject_invalid_repo_ids(owner: &str, repo: &str) -> Option<Response> {
    if let Err(e) = validate_repo_id(owner).and_then(|_| validate_repo_id(repo)) {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response(),
        );
    }
    None
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RefResponse {
    pub owner: String,
    pub repo: String,
    /// Provider instance id (e.g. "github", "gitlab", "my-gitea").
    pub provider: String,
    /// Hostname of the upstream git provider.
    pub host: String,
    /// Canonical HTTPS origin URL for the repo.
    pub origin_url: String,
    pub branch: String,
    pub commit: String,
    pub parent_commit: Option<String>,
    pub clonepack_manifest: String,
    /// Signed URL for the clonepack manifest itself, if the backend supports it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clonepack_manifest_url: Option<String>,
    /// Metadata chunk hash (protobuf). The client uses this to verify the
    /// metadata bytes it downloads concurrently with the manifest.
    pub metadata_chunk: String,
    /// Signed URL for the metadata chunk, if the backend supports it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_chunk_url: Option<String>,
    /// Signed URL for each archive chunk. `None` entries fall back to the
    /// gateway's `/v1/artifacts/{hash}` endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for each chunk of the head-blobs pack, in order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_blobs_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for the optional head-blobs idx.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_blobs_idx_url: Option<String>,
    /// Signed URL for each editable pack, ordered to match `manifest.packs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pack_chunk_urls: Option<Vec<Option<String>>>,
    /// Signed URL for the pre-built multi-pack-index (`manifest.midx`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub midx_url: Option<String>,
    /// Signed URL for the concatenated idx bundle (`manifest.idx_bundle`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idx_bundle_url: Option<String>,
    pub result: ExactResultKind,
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Serialize, Deserialize)]
pub struct RepoStatusResponse {
    pub owner: String,
    pub repo: String,
    pub added: bool,
    pub refs: Vec<ExactStatusEntry>,
    pub total_bytes: u64,
    pub total_unique_bytes: u64,
    pub regions: Vec<RegionStorageEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct ExactStatusEntry {
    pub commit: String,
    pub bytes: u64,
    pub unique_bytes: u64,
    pub head: bool,
    pub full: bool,
    pub files: bool,
    pub job: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct RegionStorageEntry {
    pub region: String,
    pub unique_bytes: u64,
}

pub fn build_app(state: ServerState) -> Router {
    let protected = Router::new()
        .route("/v1/repos", get(list_added_repos))
        // Single catch-all route for all provider-qualified repo endpoints.
        .route("/v1/repos/{*path}", get(dispatch_repos_get))
        .route("/v1/repos/{*path}", post(dispatch_repos_post))
        .route("/v1/repos/{*path}", delete(dispatch_repos_delete))
        // Refresh requires a still-valid session token (the auth layer verifies
        // the Bearer); it mints a fresh one before the current expires.
        .route("/v1/auth/refresh", post(auth_refresh_handler))
        .route("/v1/artifacts/{hash}", get(get_artifact))
        // Repository build settings. Legacy `?branch=` queries are parsed only
        // to return a clear fail-closed error.
        .route(
            "/v1/admin/config/{owner}/{repo}",
            get(admin_get_config).post(admin_put_config),
        )
        // Single catch-all route for git smart-http endpoints.
        .route("/v1/git/{*path}", get(dispatch_git_get))
        .route("/v1/git/{*path}", post(dispatch_git_post))
        // Clone metrics sink. The cloud consumes these for analytics; the OSS
        // server accepts and drops them so a self-hosted CLI never spams 404s.
        .route(
            "/v1/clones/{clone_id}/metrics",
            post(clone_metrics_drop_handler),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .route_layer(middleware::from_fn(protocol_guard))
        .with_state(state.clone());

    let rate_limited = Router::new()
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        // Session-token login: the page and the exchange are unauthenticated (they
        // prove the secret in the body) but rate-limited against brute force.
        .route("/login", get(login_page_handler))
        .route("/v1/auth/login", post(auth_login_handler))
        .route("/v1/build", post(build_handler))
        // Worker metadata report: authenticated by a signed, expiring HMAC
        // bearer token (not the shared server token). Standalone workers POST
        // ref writes here; the server holds the DB
        // creds and performs the durable write. Lives outside `protected`.
        .route("/v1/refs", get(ref_read_handler).post(ref_report_handler))
        // Worker queue endpoints: a token-only worker claims, acks, and
        // heartbeats here instead of touching the DB. Same
        // signed-bearer gate as /v1/refs; the server holds the one queue DB.
        .route("/v1/jobs/claim", post(job_claim_handler))
        .route("/v1/jobs/{id}/ack", post(job_ack_handler))
        .route("/v1/jobs/heartbeat", post(job_heartbeat_handler))
        // Provider-agnostic push-webhook receiver: authenticated by the provider
        // HMAC over the raw body (not the ripclone bearer token), so it lives
        // outside the `protected` layer.
        .route("/webhooks/{provider}", post(webhook_handler))
        .merge(protected)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ))
        .with_state(state.clone());

    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/version", get(version_handler))
        .merge(rate_limited)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

/// Maximum request body size accepted by the server. This bounds the
/// `git-upload-pack` body and any other large POST payload.
const MAX_REQUEST_BODY_BYTES: usize = 256 * 1024 * 1024;
const MAX_UPLOAD_PACK_BODY_BYTES: usize = 256 * 1024 * 1024;
/// Cap for a webhook request body. The HMAC can only be verified after the whole
/// body is buffered, so this bounds what an unauthenticated caller can make the
/// server hold before the signature is checked. GitHub caps webhook payloads at
/// ~25 MiB; this is comfortably above that and far below the global limit.
const MAX_WEBHOOK_BODY_BYTES: usize = 25 * 1024 * 1024;

/// Reject explicit declarations that do not match the sole current wire
/// protocol. Callers such as vanilla Git and ordinary HTTP integrations do not
/// declare this private header and use the same implementation.
async fn protocol_guard(
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(header) = headers.get("x-ripclone-protocol") else {
        return next.run(request).await;
    };
    let client_proto = match header
        .to_str()
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
    {
        Some(protocol) => protocol,
        None => {
            return (
                StatusCode::UPGRADE_REQUIRED,
                Json(ErrorResponse {
                    error: format!(
                        "invalid client protocol; this server requires {}",
                        crate::PROTOCOL_VERSION
                    ),
                }),
            )
                .into_response();
        }
    };
    if client_proto != crate::PROTOCOL_VERSION {
        return (
            StatusCode::UPGRADE_REQUIRED,
            Json(ErrorResponse {
                error: format!(
                    "client protocol {client_proto} does not match this server's {}; use matching ripclone client and server versions",
                    crate::PROTOCOL_VERSION
                ),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

async fn auth_middleware(
    State(state): State<ServerState>,
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    if let Some(expected) = &state.token_hash {
        let authorized = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|v| check_auth_header(v, expected) || check_bearer_token(v, state.jwt.as_deref()))
            .unwrap_or(false);
        if !authorized {
            // Smart-HTTP clients (vanilla git) expect a Basic challenge so they
            // can retry with the credentials embedded in the URL.
            if path.starts_with("/v1/git/") {
                return (
                    StatusCode::UNAUTHORIZED,
                    [("WWW-Authenticate", r#"Basic realm="ripclone""#)],
                    Json(ErrorResponse {
                        error: "unauthorized".to_string(),
                    }),
                )
                    .into_response();
            }
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "unauthorized".to_string(),
                }),
            )
                .into_response();
        }
    }
    next.run(request).await
}

fn constant_time_eq_str(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

/// Accept a `Bearer <jwt>` session token issued by `/v1/auth/login`. Returns
/// false when the header isn't a bearer, session tokens are disabled, or the
/// token fails verification (bad signature, wrong issuer, expired).
fn check_bearer_token(header: &str, jwt: Option<&crate::auth::jwt::JwtKeys>) -> bool {
    let Some(token) = header.strip_prefix("Bearer ") else {
        return false;
    };
    jwt.map(|keys| keys.verify(token).is_ok()).unwrap_or(false)
}

#[derive(Deserialize)]
struct LoginQuery {
    /// Loopback URL the browser is redirected to with the minted token, for
    /// `ripclone auth login`'s auto-capture. Absent → the page shows the token
    /// for copy-paste.
    callback: Option<String>,
    /// Opaque value echoed back to the callback so the CLI can match its request.
    state: Option<String>,
}

#[derive(Deserialize)]
struct LoginForm {
    secret: String,
    callback: Option<String>,
    state: Option<String>,
}

#[derive(Serialize)]
struct TokenResponse {
    token: String,
    /// Seconds until expiry.
    expires_in: u64,
    /// Absolute expiry (epoch seconds).
    expires_at: u64,
}

/// Only ever redirect the minted token to a loopback address — never an external
/// host. This is the open-redirect / token-exfiltration guard for the callback.
fn is_loopback_callback(raw: &str) -> bool {
    // Reject control characters (CR/LF would split the redirect header) and
    // fragments (a `#` would swallow the appended `?token=…` so the CLI never
    // sees it — and isn't a valid callback anyway).
    if raw.bytes().any(|b| b.is_ascii_control()) || raw.contains('#') {
        return false;
    }
    let Some(rest) = raw.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?']).next().unwrap_or("");
    // No userinfo: `http://127.0.0.1:80@evil.com/` parses as loopback to a naive
    // host:port split but a browser connects to `evil.com`. Reject any `@`.
    if authority.contains('@') {
        return false;
    }
    // Strip an optional `:port`. For a bracketed IPv6 literal the only port colon
    // is the one after `]`, so keep the bracketed host intact.
    let host = if authority.starts_with('[') {
        match authority.find(']') {
            Some(end) => &authority[..=end],
            None => return false,
        }
    } else {
        authority
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or(authority)
    };
    host == "127.0.0.1" || host == "localhost" || host == "[::1]"
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn login_page_html(callback: Option<&str>, state: Option<&str>, error: Option<&str>) -> String {
    let callback_field = callback
        .map(|c| {
            format!(
                r#"<input type="hidden" name="callback" value="{}">"#,
                html_escape(c)
            )
        })
        .unwrap_or_default();
    let state_field = state
        .map(|s| {
            format!(
                r#"<input type="hidden" name="state" value="{}">"#,
                html_escape(s)
            )
        })
        .unwrap_or_default();
    let error_block = error
        .map(|e| format!(r#"<p class="err">{}</p>"#, html_escape(e)))
        .unwrap_or_default();
    format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ripclone — sign in</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font: 15px/1.5 system-ui, sans-serif; max-width: 26rem; margin: 12vh auto; padding: 0 1.25rem; }}
  h1 {{ font-size: 1.25rem; margin: 0 0 .25rem; }}
  p.sub {{ color: #888; margin: 0 0 1.5rem; }}
  label {{ display: block; font-weight: 600; margin-bottom: .4rem; }}
  input[type=password] {{ width: 100%; padding: .6rem .7rem; font-size: 1rem; border: 1px solid #8884; border-radius: .5rem; box-sizing: border-box; }}
  button {{ margin-top: 1rem; width: 100%; padding: .65rem; font-size: 1rem; font-weight: 600; border: 0; border-radius: .5rem; background: #2563eb; color: #fff; cursor: pointer; }}
  button:hover {{ background: #1d4ed8; }}
  p.err {{ color: #dc2626; font-weight: 600; }}
</style></head>
<body>
  <h1>ripclone</h1>
  <p class="sub">Sign in to mint a short-lived session token.</p>
  {error_block}
  <form method="post" action="/v1/auth/login">
    <label for="secret">Server token</label>
    <input id="secret" name="secret" type="password" autocomplete="current-password" autofocus required>
    {callback_field}{state_field}
    <button type="submit">Sign in</button>
  </form>
</body></html>"#
    )
}

fn token_page_html(token: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ripclone — session token</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font: 15px/1.5 system-ui, sans-serif; max-width: 32rem; margin: 12vh auto; padding: 0 1.25rem; }}
  h1 {{ font-size: 1.25rem; }}
  p.sub {{ color: #888; }}
  textarea {{ width: 100%; height: 7rem; font: 13px/1.4 ui-monospace, monospace; padding: .6rem; border: 1px solid #8884; border-radius: .5rem; box-sizing: border-box; }}
</style></head>
<body>
  <h1>Signed in ✓</h1>
  <p class="sub">Copy this token and paste it into <code>ripclone auth login</code>:</p>
  <textarea readonly onclick="this.select()">{token}</textarea>
</body></html>"#,
        token = html_escape(token)
    )
}

async fn login_page_handler(Query(q): Query<LoginQuery>) -> Html<String> {
    Html(login_page_html(
        q.callback.as_deref(),
        q.state.as_deref(),
        None,
    ))
}

async fn auth_login_handler(
    State(state): State<ServerState>,
    Form(form): Form<LoginForm>,
) -> Response {
    let (Some(expected), Some(keys)) = (state.token_hash.as_deref(), state.jwt.as_deref()) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Html(login_page_html(
                form.callback.as_deref(),
                form.state.as_deref(),
                Some("Session tokens are not enabled on this server."),
            )),
        )
            .into_response();
    };

    let presented = hex::encode(Sha256::digest(form.secret.as_bytes()));
    if !constant_time_eq_str(&presented, expected) {
        return (
            StatusCode::UNAUTHORIZED,
            Html(login_page_html(
                form.callback.as_deref(),
                form.state.as_deref(),
                Some("Invalid server token."),
            )),
        )
            .into_response();
    }

    let (token, _exp) = match keys.issue(crate::auth::jwt::ttl(), crate::auth::jwt::session_max()) {
        Ok(t) => t,
        Err(e) => {
            warn!("failed to mint session token: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "failed to mint token".to_string(),
                }),
            )
                .into_response();
        }
    };

    match form.callback.as_deref() {
        Some(cb) if is_loopback_callback(cb) => {
            let st = form.state.as_deref().unwrap_or("");
            let sep = if cb.contains('?') { '&' } else { '?' };
            // Percent-encode the query values so an attacker-supplied `state`
            // can't inject extra parameters or split the Location header.
            (
                [("cache-control", "no-store")],
                Redirect::to(&format!(
                    "{cb}{sep}token={}&state={}",
                    urlencoding::encode(&token),
                    urlencoding::encode(st)
                )),
            )
                .into_response()
        }
        Some(_) => (
            StatusCode::BAD_REQUEST,
            Html(login_page_html(
                None,
                form.state.as_deref(),
                Some("Refusing to deliver the token to a non-loopback address."),
            )),
        )
            .into_response(),
        None => (
            [("cache-control", "no-store")],
            Html(token_page_html(&token)),
        )
            .into_response(),
    }
}

async fn auth_refresh_handler(State(state): State<ServerState>, headers: HeaderMap) -> Response {
    let Some(keys) = state.jwt.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "session tokens disabled".to_string(),
            }),
        )
            .into_response();
    };
    let ttl = crate::auth::jwt::ttl();
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    // From a session token: re-issue keeping the same absolute session deadline,
    // so a refresh chain can't outlive the original session. Authed by the shared
    // token instead (no Bearer): start a fresh session.
    let minted = match bearer {
        Some(token) => keys.refresh(token, ttl),
        None => keys.issue(ttl, crate::auth::jwt::session_max()),
    };
    match minted {
        Ok((token, expires_at)) => Json(TokenResponse {
            token,
            expires_in: expires_at.saturating_sub(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            ),
            expires_at,
        })
        .into_response(),
        Err(e) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: format!("{e}"),
            }),
        )
            .into_response(),
    }
}

fn check_auth_header(header: &str, expected: &str) -> bool {
    if let Some(token) = header.strip_prefix("Ripclone ") {
        return constant_time_eq_str(token, expected);
    }
    if let Some(credentials) = header.strip_prefix("Basic ")
        && let Ok(decoded) =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, credentials)
        && let Ok(decoded) = String::from_utf8(decoded)
    {
        // Accept "<username>:<password>"; compare the password to the
        // expected hash so vanilla git can use
        // http://user:<hash>@host/... URLs.
        if let Some((_, password)) = decoded.split_once(':') {
            return constant_time_eq_str(password, expected);
        }
    }
    false
}

/// Trust a forwarded-for header for the rate-limit key. Off by default: the
/// header is client-spoofable, so only honor it when the operator has put a
/// reverse proxy directly in front (`RIPCLONE_TRUST_FORWARDED_FOR=1`). Read once.
fn trust_forwarded_for() -> bool {
    static TRUST: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRUST.get_or_init(|| {
        std::env::var("RIPCLONE_TRUST_FORWARDED_FOR")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Rate-limit bucket key for a request. Keying on the raw socket IP is useless
/// behind a reverse proxy (every request shares the proxy's IP → one global
/// bucket) and bypassable over IPv6 (a /64 gives 2^64 addresses, each a fresh
/// bucket). So: derive the client IP from the trusted forwarded-for header when
/// enabled, and collapse IPv6 to its /64 network so an attacker can't rotate
/// addresses within their allocation (AU2).
fn rate_limit_key(headers: &HeaderMap, socket: SocketAddr, trust_forwarded: bool) -> String {
    let ip = if trust_forwarded {
        headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            // Rightmost entry = the address our immediately-trusted proxy saw;
            // entries a client prepends are ignored. Assumes a single trusted
            // proxy directly in front.
            .and_then(|v| v.rsplit(',').next())
            .map(str::trim)
            .and_then(|s| s.parse::<IpAddr>().ok())
            .unwrap_or_else(|| socket.ip())
    } else {
        socket.ip()
    };
    normalize_ip_for_rate_limit(ip)
}

fn normalize_ip_for_rate_limit(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            // Collapse to the /64 network (the first four 16-bit groups).
            let s = v6.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
        }
    }
}

async fn rate_limit_middleware(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    // One logical clone fans out across content-addressed manifests, indexes,
    // and chunks. Charging those authenticated GETs to the control-plane
    // request bucket makes a sufficiently large repository rate-limit its own
    // clone. Authentication still runs in the protected router, and anonymous
    // artifact traffic remains subject to the normal per-IP limiter.
    if is_authenticated_artifact_get(&state, &request) {
        return next.run(request).await;
    }
    let key = rate_limit_key(request.headers(), addr, trust_forwarded_for());
    if !state.rate_limiter.check(&key) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse {
                error: "rate limit exceeded".to_string(),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

fn is_authenticated_artifact_get(
    state: &ServerState,
    request: &axum::http::Request<axum::body::Body>,
) -> bool {
    if request.method() != axum::http::Method::GET {
        return false;
    }
    let path = request.uri().path();
    if !path.starts_with("/v1/artifacts/") {
        return false;
    }
    let Some(expected) = state.token_hash.as_deref() else {
        return false;
    };
    request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            check_auth_header(value, expected) || check_bearer_token(value, state.jwt.as_deref())
        })
        .unwrap_or(false)
}

async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// Public version endpoint. Reports the server's build version and the wire
/// protocol version it speaks, so a client can check compatibility without
/// authenticating. Compatibility is keyed on `protocol`, not the build version.
async fn version_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": crate::PROTOCOL_VERSION,
    }))
}

/// Accept-and-drop sink for the CLI's post-clone metrics report. The payload is
/// advertising-grade telemetry, not operator metrics, so the OSS server has no
/// use for it; rejecting it would only make self-hosted clients see 404s.
async fn clone_metrics_drop_handler() -> impl IntoResponse {
    StatusCode::ACCEPTED
}

/// Shared bearer-token gate for every farmed-out-worker endpoint (`/v1/refs`,
/// `/v1/jobs/*`). Fails **closed**: 503 when no signing secret is configured,
/// 401 before any state change when the token is missing / malformed / expired /
/// signed with the wrong secret. Auth is signature + expiry only — no repo/job
/// scope, because one token is injected into a pooled worker that may claim any
/// repo's job. `Err(Response)` short-circuits the handler; `Ok(())` proceeds.
// The `Err` is an axum `Response` (large by clippy's measure) but each handler
// returns it at most once per request — not a hot path.
#[allow(clippy::result_large_err)]
fn authorize_worker_token(route: &str, headers: &HeaderMap) -> Result<(), Response> {
    use crate::job_token::{report_token_secret_from_env, verify_job_token};

    let Some(secret) = report_token_secret_from_env() else {
        error!(
            "{route}: no job-token secret configured \
             (set RIPCLONE_JOB_TOKEN_SECRET or RIPCLONE_SERVER_TOKEN)"
        );
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "job tokens not configured on this server".to_string(),
            }),
        )
            .into_response());
    };

    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(token) = presented else {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "missing Authorization: Bearer <job token>".to_string(),
            }),
        )
            .into_response());
    };

    if let Err(e) = verify_job_token(&secret, token) {
        warn!("{route}: auth rejected: {e:#}");
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid or expired job token".to_string(),
            }),
        )
            .into_response());
    }
    Ok(())
}

/// Resolve the server's concrete SQL queue for a `/v1/jobs/*` handler, or a 503
/// response when this state's control queue is unavailable. Called only after
/// [`authorize_worker_token`].
#[allow(clippy::result_large_err)]
fn worker_queue_or_503(
    route: &str,
    state: &ServerState,
) -> Result<Arc<crate::queue::SqlJobQueue>, Response> {
    match &state.worker_queue {
        Some(q) => Ok(q.clone()),
        None => {
            error!("{route}: server control queue is unavailable");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "server control queue is unavailable".to_string(),
                }),
            )
                .into_response())
        }
    }
}

/// `POST /v1/jobs/claim` — a farm-out worker claims exactly one queued job.
///
/// Same bearer gate as `/v1/refs`. Returns the one claimed job (or `null`),
/// including its per-job upstream `credential` so the worker can fetch a private
/// repo — never a list, never another job's data. Delegates to the server's SQL
/// queue, applying the worker's `max_size_class` ceiling per claim.
async fn job_claim_handler(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(req): Json<crate::api_job_queue::ClaimRequest>,
) -> Response {
    use crate::api_job_queue::{ClaimResponse, ClaimedJobWire};
    use secrecy::ExposeSecret;

    if let Err(resp) = authorize_worker_token("POST /v1/jobs/claim", &headers) {
        return resp;
    }
    let queue = match worker_queue_or_503("POST /v1/jobs/claim", &state) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    if req.worker_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "worker_id must not be empty".to_string(),
            }),
        )
            .into_response();
    }
    let ceiling = match queue.resolve_ceiling(req.max_size_class.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("invalid max_size_class: {e}"),
                }),
            )
                .into_response();
        }
    };
    match queue.claim_capped(&req.worker_id, ceiling).await {
        Ok(claimed) => {
            let job = claimed.map(|c| ClaimedJobWire {
                id: c.id,
                provider: c.provider,
                path: c.path,
                admitted_commit: c.admitted_commit,
                repo_config: c.repo_config,
                credential: c.credential.map(|s| s.expose_secret().to_string()),
            });
            (StatusCode::OK, Json(ClaimResponse { job })).into_response()
        }
        Err(e) => {
            error!("POST /v1/jobs/claim failed: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("claim failed: {e:#}"),
                }),
            )
                .into_response()
        }
    }
}

/// `POST /v1/jobs/{id}/ack` — a worker settles its claimed job. Same bearer gate
/// as `/v1/refs`. Delegates to the SQL queue and returns the post-ack lifecycle.
async fn job_ack_handler(
    State(state): State<ServerState>,
    Path(id): Path<i64>,
    headers: HeaderMap,
    Json(req): Json<crate::api_job_queue::AckRequest>,
) -> Response {
    use crate::api_job_queue::{AckResponse, job_state_tag};
    use crate::queue::{BuildError, JobQueue, JobState};

    if let Err(resp) = authorize_worker_token("POST /v1/jobs/ack", &headers) {
        return resp;
    }
    let queue = match worker_queue_or_503("POST /v1/jobs/ack", &state) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    if req.worker_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "worker_id must not be empty".to_string(),
            }),
        )
            .into_response();
    }
    let result: Result<(), BuildError> = if req.result.ok {
        Ok(())
    } else {
        let msg = req
            .result
            .error
            .unwrap_or_else(|| "build failed".to_string());
        Err(if req.result.retryable {
            BuildError::retryable(msg)
        } else {
            BuildError::permanent(msg)
        })
    };
    match queue.ack(id, &req.worker_id, result).await {
        Ok(settled) => {
            // Active API heartbeats create a registry row even when optional
            // idle fleet registration is disabled. Once this sequential worker
            // settles (or discovers it no longer owns) the job, remove that row;
            // an opt-in idle heartbeat may recreate it on its next interval.
            if let Err(error) = queue.remove_worker(&req.worker_id).await {
                error!("remove settled API worker {}: {error:#}", req.worker_id);
            }
            // Report the resulting lifecycle so the worker can detect a
            // dead-letter without a second round-trip.
            let (state_tag, error) =
                match <crate::queue::SqlJobQueue as JobQueue>::job_status(queue.as_ref(), id).await
                {
                    Ok(JobState::Failed(err)) => ("failed", Some(err)),
                    Ok(s) => (job_state_tag(&s), None),
                    Err(_) => ("unknown", None),
                };
            (
                StatusCode::OK,
                Json(AckResponse {
                    settled,
                    state: state_tag.to_string(),
                    error,
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!("POST /v1/jobs/{id}/ack failed: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("ack failed: {e:#}"),
                }),
            )
                .into_response()
        }
    }
}

/// `POST /v1/jobs/heartbeat` — a worker refreshes its registry row so the
/// autoscaler can count it. Worker-scoped (fires while idle, `current_job` may
/// be `None`), so no job id in the path. Same bearer gate as `/v1/refs`.
async fn job_heartbeat_handler(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(req): Json<crate::api_job_queue::HeartbeatRequest>,
) -> Response {
    if let Err(resp) = authorize_worker_token("POST /v1/jobs/heartbeat", &headers) {
        return resp;
    }
    let queue = match worker_queue_or_503("POST /v1/jobs/heartbeat", &state) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    if req.worker_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "worker_id must not be empty".to_string(),
            }),
        )
            .into_response();
    }
    match queue.heartbeat(&req.worker_id, req.current_job).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            error!("POST /v1/jobs/heartbeat failed: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("heartbeat failed: {e:#}"),
                }),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
struct WorkerRefReadQuery {
    repo_key: String,
    commit: String,
}

enum ExactResultRef<'a> {
    Head(&'a crate::HeadResult),
    Full(&'a crate::FullResult),
    Files(&'a crate::FilesResult),
}

impl ExactResultRef<'_> {
    fn kind(&self) -> ExactResultKind {
        match self {
            Self::Head(_) => ExactResultKind::Head,
            Self::Full(_) => ExactResultKind::Full,
            Self::Files(_) => ExactResultKind::Files,
        }
    }

    fn artifacts(&self) -> &crate::ClonepackArtifacts {
        match self {
            Self::Head(result) => &result.clonepack,
            Self::Full(result) => &result.clonepack,
            Self::Files(result) => &result.clonepack,
        }
    }
}

fn manifest_ref_hash(reference: Option<&ChunkRef>) -> String {
    reference
        .map(|reference| hash_to_hex(&reference.hash))
        .unwrap_or_default()
}

fn validate_manifest_packs(
    manifest: &ClonepackManifest,
    reported: &[crate::PackArtifact],
) -> Result<()> {
    anyhow::ensure!(
        manifest.packs.len() == reported.len(),
        "manifest pack count does not match reported result"
    );
    for (index, (manifest_pack, reported_pack)) in
        manifest.packs.iter().zip(reported.iter()).enumerate()
    {
        anyhow::ensure!(
            manifest_ref_hash(manifest_pack.pack.as_ref()) == reported_pack.pack
                && manifest_ref_hash(manifest_pack.idx.as_ref()) == reported_pack.idx,
            "manifest pack {index} does not match reported result"
        );
    }
    Ok(())
}

/// Validate one authenticated job result before its claim-protected mutation.
/// The manifest is the only object fetched here: normal readiness remains a
/// metadata-only check, and content-addressed child uploads remain repeatable.
async fn validate_claimed_result_manifest(
    storage: &StorageRef,
    commit: &str,
    result: ExactResultRef<'_>,
) -> Result<()> {
    let kind = result.kind();
    let artifacts = result.artifacts();
    anyhow::ensure!(
        crate::exact_output_artifacts_ready(commit, kind, artifacts),
        "invalid {kind} result for exact commit {commit}"
    );

    let storage = storage.clone();
    let manifest_hash = artifacts.manifest.clone();
    let fetch_hash = manifest_hash.clone();
    let bytes = tokio::task::spawn_blocking(move || storage.get(&fetch_hash))
        .await
        .context("fetch claimed result manifest task")?
        .context("fetch claimed result manifest")?;
    anyhow::ensure!(
        crate::cas::hash(&bytes) == manifest_hash,
        "claimed result manifest digest does not match its artifact ID"
    );
    let manifest =
        ClonepackManifest::decode(bytes.as_slice()).context("decode claimed result manifest")?;
    anyhow::ensure!(
        manifest.commit == commit,
        "claimed result manifest commit does not match exact commit {commit}"
    );
    anyhow::ensure!(
        manifest_ref_hash(manifest.metadata_chunk.as_ref()) == artifacts.metadata_chunk,
        "claimed result manifest metadata does not match reported result"
    );
    anyhow::ensure!(
        manifest_ref_hash(manifest.midx.as_ref()) == artifacts.midx,
        "claimed result manifest MIDX does not match reported result"
    );
    anyhow::ensure!(
        manifest_ref_hash(manifest.idx_bundle.as_ref()) == artifacts.idx_bundle,
        "claimed result manifest idx bundle does not match reported result"
    );

    match result {
        ExactResultRef::Head(result) => {
            validate_manifest_packs(&manifest, &result.packs)?;
            anyhow::ensure!(
                manifest.archive_chunks.is_empty(),
                "Head manifest contains an unreported archive list"
            );
        }
        ExactResultRef::Full(result) => {
            validate_manifest_packs(&manifest, &result.packs)?;
            anyhow::ensure!(
                manifest.archive_chunks.is_empty(),
                "Full manifest contains an unreported archive list"
            );
        }
        ExactResultRef::Files(result) => {
            anyhow::ensure!(
                manifest.packs.is_empty(),
                "Files manifest contains an unreported pack list"
            );
            let manifest_archives: Vec<String> = manifest
                .archive_chunks
                .iter()
                .map(|chunk| hash_to_hex(&chunk.hash))
                .collect();
            anyhow::ensure!(
                manifest_archives == result.archive_chunks,
                "Files manifest archive list does not match reported result"
            );
        }
    }
    Ok(())
}

/// `GET /v1/refs` — a worker reads the explicit results already published for
/// the exact commit it owns. This lets a replacement job skip ready work.
async fn ref_read_handler(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(query): Query<WorkerRefReadQuery>,
) -> Response {
    use crate::provider::parse_storage_key;

    if let Err(resp) = authorize_worker_token("GET /v1/refs", &headers) {
        return resp;
    }
    let Some(repo_id) = parse_storage_key(&query.repo_key) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("invalid repo_key: {}", query.repo_key),
            }),
        )
            .into_response();
    };
    if let Err(error) = crate::validation::validate_object_id(&query.commit) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("invalid exact commit: {error}"),
            }),
        )
            .into_response();
    }
    match state.ref_store.load_result(&repo_id, &query.commit).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("exact result read failed: {error:#}"),
            }),
        )
            .into_response(),
    }
}

/// `POST /v1/refs` — farmed-out worker reports an exact result publication.
///
/// Auth is a signed, expiring HMAC bearer token (`Authorization: Bearer …`), not
/// the shared server token. Every mutation additionally carries the existing
/// job/worker claim identity; the control database validates ownership in the
/// same transaction as the result write.
async fn ref_report_handler(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(body): Json<crate::api_ref_store::RefReport>,
) -> Response {
    use crate::api_ref_store::{RefReport, RefReportResponse};
    use crate::provider::parse_storage_key;

    // Same fail-closed (503 no secret), 401-before-any-effect gate as the
    // worker queue endpoints. Auth is signature + expiry only (no repo scope):
    // the token is injected into a pooled worker that may claim any repo's job.
    if let Err(resp) = authorize_worker_token("POST /v1/refs", &headers) {
        return resp;
    }

    // The write target comes from the request body, not the token.
    let repo_key = body.repo_key().to_string();
    let Some(repo_id) = parse_storage_key(&repo_key) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("invalid repo_key: {repo_key}"),
            }),
        )
            .into_response();
    };

    let result: Result<RefReportResponse, anyhow::Error> = match body {
        RefReport::PublishHead {
            job_id,
            worker_id,
            commit,
            head,
            ..
        } => {
            if let Err(error) = validate_claimed_result_manifest(
                &state.storage,
                &commit,
                ExactResultRef::Head(&head),
            )
            .await
            {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid Head result for exact commit {commit}: {error:#}"),
                    }),
                )
                    .into_response();
            }
            let _ = test_hook(TestStage::RefStoreWrite).await;
            match state.control_db.as_ref() {
                Some(control) => {
                    control
                        .publish_head_for_claim(job_id, &worker_id, &repo_id, &commit, *head)
                        .await
                }
                #[cfg(test)]
                None => {
                    state
                        .ref_store
                        .publish_claimed_head(&repo_id, &commit, *head, job_id, &worker_id)
                        .await
                }
                #[cfg(not(test))]
                None => Err(anyhow::anyhow!("control database unavailable")),
            }
            .map(|updated| RefReportResponse { updated })
        }
        RefReport::PublishFull {
            job_id,
            worker_id,
            commit,
            full,
            ..
        } => {
            if let Err(error) = validate_claimed_result_manifest(
                &state.storage,
                &commit,
                ExactResultRef::Full(&full),
            )
            .await
            {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid Full result for exact commit {commit}: {error:#}"),
                    }),
                )
                    .into_response();
            }
            match state.control_db.as_ref() {
                Some(control) => {
                    control
                        .publish_full_for_claim(job_id, &worker_id, &repo_id, &commit, *full)
                        .await
                }
                #[cfg(test)]
                None => {
                    state
                        .ref_store
                        .publish_claimed_full(&repo_id, &commit, *full, job_id, &worker_id)
                        .await
                }
                #[cfg(not(test))]
                None => Err(anyhow::anyhow!("control database unavailable")),
            }
            .map(|updated| RefReportResponse { updated })
        }
        RefReport::PublishFiles {
            job_id,
            worker_id,
            commit,
            files,
            ..
        } => {
            if let Err(error) = validate_claimed_result_manifest(
                &state.storage,
                &commit,
                ExactResultRef::Files(&files),
            )
            .await
            {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid Files result for exact commit {commit}: {error:#}"),
                    }),
                )
                    .into_response();
            }
            match state.control_db.as_ref() {
                Some(control) => {
                    control
                        .publish_files_for_claim(job_id, &worker_id, &repo_id, &commit, *files)
                        .await
                }
                #[cfg(test)]
                None => {
                    state
                        .ref_store
                        .publish_claimed_files(&repo_id, &commit, *files, job_id, &worker_id)
                        .await
                }
                #[cfg(not(test))]
                None => Err(anyhow::anyhow!("control database unavailable")),
            }
            .map(|updated| RefReportResponse { updated })
        }
    };

    match result {
        Ok(resp) if resp.updated => (StatusCode::OK, Json(resp)).into_response(),
        Ok(_resp) => (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "worker no longer owns the claimed job".to_string(),
            }),
        )
            .into_response(),
        Err(e) => {
            error!("POST /v1/refs write failed for {repo_key}: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("ref write failed: {e:#}"),
                }),
            )
                .into_response()
        }
    }
}

/// Readiness probe: 200 only when storage and the ref store are both reachable,
/// 503 otherwise (with the specific problems). Unlike `/healthz` (liveness),
/// this fails when a dependency is broken (e.g. the data volume is unmounted) so
/// a load balancer stops routing to a server that can't serve clones.
const READYZ_CACHE_TTL: Duration = Duration::from_secs(3);

async fn readyz(State(state): State<ServerState>) -> impl IntoResponse {
    // Serve a cached result within the TTL: bounds backend probe cost (e.g. S3
    // round-trips on this unauthenticated endpoint) and damps load-balancer
    // flapping on a single transient blip.
    if let Some((at, ready)) = *state.readyz_cache.lock().unwrap_or_else(|e| e.into_inner())
        && at.elapsed() < READYZ_CACHE_TTL
    {
        return readyz_response(ready);
    }

    let mut problems: Vec<String> = Vec::new();

    // The storage probe is synchronous (filesystem / S3); keep it off the async
    // worker.
    let storage = state.storage.clone();
    match tokio::task::spawn_blocking(move || storage.health()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => problems.push(format!("storage: {e:#}")),
        Err(e) => problems.push(format!("storage probe failed to run: {e}")),
    }

    if let Err(e) = state.ref_store.health().await {
        problems.push(format!("ref_store: {e:#}"));
    }

    let ready = problems.is_empty();
    if !ready {
        // Log details server-side; the public (unauthenticated) body stays
        // generic so internal paths aren't leaked.
        warn!("readiness check failed: {}", problems.join("; "));
    }
    *state.readyz_cache.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), ready));
    readyz_response(ready)
}

fn readyz_response(ready: bool) -> Response {
    if ready {
        (StatusCode::OK, Json(serde_json::json!({"status": "ready"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status": "not_ready"})),
        )
            .into_response()
    }
}

async fn metrics_handler(State(state): State<ServerState>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.prometheus(),
    )
}

#[derive(Deserialize)]
struct GitServiceQuery {
    service: String,
}

/// Smart-HTTP `info/refs` fallback. Advertises refs so a vanilla git client can
/// talk to ripclone when the archive-first path is not available.
async fn git_info_refs_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    query: GitServiceQuery,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if query.service != "git-upload-pack" {
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "only git-upload-pack is supported".to_string(),
            }),
        )
            .into_response();
    }

    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let request_token = upstream_token_from_headers(&headers);
    let credential = match state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref())
    {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };
    // AU1: gate the vanilla-git read surface too (it serves the private repo's
    // refs/objects directly from the mirror).
    if let Err(resp) =
        authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
    {
        return resp;
    }
    let lock = repo_lock(&state.sync_locks, &repo_id).await;
    let _guard = lock.lock().await;
    if let Err(e) = ensure_mirror(
        &mirror_dir,
        &provider,
        &repo_id,
        "HEAD",
        None,
        credential.as_ref(),
    )
    .await
    {
        state.metrics.record_error();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("mirror sync failed: {}", e),
            }),
        )
            .into_response();
    }
    drop(_guard);

    match advertise_refs(&mirror_dir).await {
        Ok(body) => (
            StatusCode::OK,
            [(
                "content-type",
                "application/x-git-upload-pack-advertisement",
            )],
            body,
        )
            .into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("advertise-refs failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

async fn advertise_refs(mirror_dir: &std::path::Path) -> Result<Vec<u8>> {
    let mirror_dir = mirror_dir.to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("git")
            .arg("upload-pack")
            .arg("--advertise-refs")
            .arg(&mirror_dir)
            .output()
    })
    .await
    .context("advertise-refs task")?;

    let output = output.context("git upload-pack --advertise-refs")?;
    if !output.status.success() {
        anyhow::bail!(
            "git upload-pack --advertise-refs failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Smart-HTTP advertisement prefix.
    let mut body = Vec::new();
    body.extend_from_slice(b"001e# service=git-upload-pack\n0000");
    body.extend_from_slice(&output.stdout);
    Ok(body)
}

/// Smart-HTTP `git-upload-pack` RPC fallback. Pipes the client's request body
/// through `git upload-pack --stateless-rpc` on the local bare mirror.
async fn git_upload_pack_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    body: Body,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let request_token = upstream_token_from_headers(&headers);
    let credential = match state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref())
    {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };
    // AU1: gate the vanilla-git upload-pack read surface.
    if let Err(resp) =
        authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
    {
        return resp;
    }
    let lock = repo_lock(&state.sync_locks, &repo_id).await;
    let _guard = lock.lock().await;
    if let Err(e) = ensure_mirror(
        &mirror_dir,
        &provider,
        &repo_id,
        "HEAD",
        None,
        credential.as_ref(),
    )
    .await
    {
        state.metrics.record_error();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("mirror sync failed: {}", e),
            }),
        )
            .into_response();
    }
    drop(_guard);

    let bytes = match axum::body::to_bytes(body, MAX_UPLOAD_PACK_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            state.metrics.record_error();
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("read body failed: {}", e),
                }),
            )
                .into_response();
        }
    };

    match upload_pack_rpc(&mirror_dir, bytes).await {
        Ok(output) => (
            StatusCode::OK,
            [("content-type", "application/x-git-upload-pack-result")],
            output,
        )
            .into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("upload-pack rpc failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

async fn dispatch_repos_get(
    Path(path): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    if let Some((repo_path, branch)) = path.rsplit_once("/refs/") {
        let params = match Query::<RefQuery>::try_from_uri(&uri) {
            Ok(query) => query.0,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: error.to_string(),
                    }),
                )
                    .into_response();
            }
        };
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return get_ref_inner(
            repo_id,
            provider.clone(),
            branch.to_string(),
            params,
            headers,
            state,
        )
        .await;
    }

    if let Some(repo_path) = path.strip_suffix("/status") {
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<RepoStatusQuery>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
                    .into_response();
            }
        };
        return repo_status_inner(repo_id, provider.clone(), query, headers, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn dispatch_repos_post(
    Path(path): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    if let Some(repo_path) = path.strip_suffix("/add") {
        let _ = test_hook(TestStage::Http(format!("POST /v1/repos/{repo_path}/add"))).await;
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<AddRequest>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid add request: {}", e),
                    }),
                )
                    .into_response();
            }
        };
        return add_repo_inner(repo_id, provider.clone(), query, headers, state).await;
    }

    if let Some(repo_path) = path.strip_suffix("/sync") {
        let _ = test_hook(TestStage::Http(format!("POST /v1/repos/{repo_path}/sync"))).await;
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        let query = match Query::<SyncRequest>::try_from_uri(&uri) {
            Ok(q) => q.0,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid sync request: {}", e),
                    }),
                )
                    .into_response();
            }
        };
        return sync_repo_inner(repo_id, provider.clone(), query, headers, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn dispatch_repos_delete(
    Path(path): Path<String>,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(repo_path) = path.strip_suffix("/add") {
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return remove_added_repo_inner(repo_id, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn list_added_repos(State(state): State<ServerState>) -> Response {
    match state.ref_store.list_added_repos().await {
        Ok(repos) => Json(
            repos
                .into_iter()
                .map(|added| added.repo_id)
                .collect::<Vec<RepoId>>(),
        )
        .into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("list added repos failed: {e}"),
                }),
            )
                .into_response()
        }
    }
}

async fn dispatch_git_get(
    Path(path): Path<String>,
    Query(query): Query<GitServiceQuery>,
    headers: HeaderMap,
    State(state): State<ServerState>,
) -> Response {
    if let Some(repo_path) = path.strip_suffix("/info/refs") {
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return git_info_refs_inner(repo_id, provider.clone(), query, headers, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn dispatch_git_post(
    Path(path): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    body: Body,
) -> Response {
    if let Some(repo_path) = path.strip_suffix("/git-upload-pack") {
        let Some((repo_id, provider)) = resolve_repo_id(&state.provider_registry, repo_path) else {
            return unknown_provider_response();
        };
        if let Some(resp) =
            validation::reject_if_invalid(|| validation::validate_repo_path(provider, &repo_id))
        {
            return resp;
        }
        return git_upload_pack_inner(repo_id, provider.clone(), body, headers, state).await;
    }

    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "not found".to_string(),
        }),
    )
        .into_response()
}

async fn upload_pack_rpc(mirror_dir: &std::path::Path, input: Bytes) -> Result<Vec<u8>> {
    let mirror_dir = mirror_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut child = std::process::Command::new("git")
            .arg("upload-pack")
            .arg("--stateless-rpc")
            .arg(&mirror_dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("spawn git upload-pack --stateless-rpc")?;

        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin.write_all(&input)?;
        }

        let output = child.wait_with_output().context("wait for upload-pack")?;
        if !output.status.success() {
            anyhow::bail!(
                "git upload-pack --stateless-rpc failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(output.stdout)
    })
    .await
    .context("upload-pack rpc task")?
}

async fn ensure_mirror(
    mirror_dir: &std::path::Path,
    provider: &ProviderInstance,
    repo_id: &RepoId,
    branch: &str,
    rev: Option<&str>,
    credential: Option<&secrecy::SecretString>,
) -> Result<()> {
    let mirror_dir = mirror_dir.to_path_buf();
    let provider = provider.clone();
    let repo_id = repo_id.clone();
    let branch = branch.to_string();
    let rev = rev.map(str::to_string);
    let credential = credential.cloned();
    // Same process-global fetch cap as the build path.
    let _fetch_permit = fetch_semaphore()
        .acquire()
        .await
        .expect("fetch semaphore never closed");
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror(
            &mirror_dir,
            &provider,
            &repo_id,
            &branch,
            rev.as_deref(),
            credential.as_ref(),
        )
    })
    .await
    .context("ensure mirror task")?
}

fn exact_result_clonepack(
    info: &RefInfo,
    result: ExactResultKind,
) -> Option<&crate::ClonepackArtifacts> {
    match result {
        ExactResultKind::Head => info.head.as_ref().map(|head| &head.clonepack),
        ExactResultKind::Full => info.full.as_ref().map(|full| &full.clonepack),
        ExactResultKind::Files => info.files.as_ref().map(|files| &files.clonepack),
    }
}

fn exact_result_ready(info: &RefInfo, result: ExactResultKind, commit: &str) -> bool {
    crate::exact_output_ready(info, result, commit)
}

fn exact_result_complete(info: &RefInfo, commit: &str) -> bool {
    crate::exact_result_complete(info, commit)
}

fn exact_parent_head_ready(info: &RefInfo, commit: &str) -> bool {
    crate::exact_output_ready(info, ExactResultKind::Head, commit)
}

async fn artifact_pending_response(commit: &str, branch: &str, queue_depth: usize) -> Response {
    artifact_pending_response_with_top_up(commit, branch, queue_depth, None, None).await
}

async fn artifact_pending_response_with_top_up(
    commit: &str,
    branch: &str,
    queue_depth: usize,
    top_up_supported: Option<bool>,
    top_up_base: Option<RefResponse>,
) -> Response {
    let _ = test_hook(TestStage::PendingResponse).await;
    let mut response = (
        StatusCode::ACCEPTED,
        Json(ArtifactPendingResponse {
            code: "artifact_pending",
            commit: commit.to_string(),
            branch: branch.to_string(),
            status: "building",
            queue_depth,
            top_up_supported,
            top_up_base,
        }),
    )
        .into_response();
    if let Ok(value) = urlencoding::encode(branch).parse() {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_LOCATION, value);
    }
    response
}

fn checked_manifest_hash(chunk: &crate::clonepack::ChunkRef) -> Option<String> {
    if chunk.hash.len() != 32 || chunk.len == 0 {
        return None;
    }
    Some(crate::clonepack::hash_to_hex(&chunk.hash))
}

/// Build an ordinary Full response for the carried base using only hashes and
/// ordering authenticated by that base manifest.
fn ref_response_from_manifest(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    manifest_hash: &str,
    manifest: &ClonepackManifest,
    storage: &crate::storage::StorageRef,
    private: bool,
) -> Option<RefResponse> {
    let metadata_chunk = checked_manifest_hash(manifest.metadata_chunk.as_ref()?)?;
    let archive_hashes = manifest
        .archive_chunks
        .iter()
        .map(checked_manifest_hash)
        .collect::<Option<Vec<_>>>()?;
    let head_blob_hashes = manifest
        .head_blobs_chunks
        .iter()
        .map(checked_manifest_hash)
        .collect::<Option<Vec<_>>>()?;
    let head_blobs_idx = match manifest.head_blobs_idx.as_ref() {
        Some(chunk) => Some(checked_manifest_hash(chunk)?),
        None => None,
    };
    let mut pack_hashes = Vec::with_capacity(manifest.packs.len());
    for entry in &manifest.packs {
        pack_hashes.push(checked_manifest_hash(entry.pack.as_ref()?)?);
        checked_manifest_hash(entry.idx.as_ref()?)?;
    }
    if manifest.packs.is_empty() {
        return None;
    }
    let midx = match manifest.midx.as_ref() {
        Some(chunk) => Some(checked_manifest_hash(chunk)?),
        None => None,
    };
    let bundle = manifest.idx_bundle.as_ref()?;
    let idx_bundle = checked_manifest_hash(bundle)?;
    for entry in &manifest.packs {
        let idx = entry.idx.as_ref()?;
        if entry
            .idx_bundle_offset
            .checked_add(idx.len)
            .is_none_or(|end| end > bundle.len)
        {
            return None;
        }
    }
    let artifacts = ResponseArtifacts {
        commit: manifest.commit.clone(),
        parent_commit: manifest.parent_commit.clone(),
        manifest: manifest_hash.to_string(),
        metadata_chunk,
        archive_chunks: archive_hashes,
        head_blobs_chunks: head_blob_hashes,
        head_blobs_idx,
        packs: pack_hashes,
        midx: midx.unwrap_or_default(),
        idx_bundle,
    };
    Some(build_ref_response(
        repo_id,
        provider,
        branch,
        &artifacts,
        storage,
        ExactResultKind::Full,
        private,
    ))
}

async fn carried_full_top_up_response(
    info: &RefInfo,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: &str,
    pinned: &str,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    private: bool,
) -> Option<RefResponse> {
    // The caller supplies the same exact-row snapshot that is still building B.
    // Do not read it again here: Full(B) could publish between reads, which
    // would otherwise turn an exact-ready response into a pending top-up miss.
    if info.commit != pinned {
        return None;
    }
    let parent = info.head.as_ref()?.parent_commit.as_deref()?;
    let parent_result = ref_store.load_result(repo_id, parent).await.ok()??;
    let full = parent_result.full.as_ref()?;
    if !crate::exact_output_artifacts_ready(parent, ExactResultKind::Full, &full.clonepack) {
        return None;
    }
    let artifact = full.clonepack.commit.as_str();
    let manifest_hash = full.clonepack.manifest.as_str();
    if parent != artifact
        || artifact.is_empty()
        || manifest_hash.is_empty()
        || crate::validation::validate_object_id(parent).is_err()
        || crate::cas::Cas::validate_artifact_id(manifest_hash).is_err()
    {
        return None;
    }
    let storage_for_read = Arc::clone(storage);
    let manifest_hash_for_read = manifest_hash.to_string();
    let test_read_log = (std::env::var_os("RIPCLONE_TESTING").is_some())
        .then(|| std::env::var_os("RIPCLONE_TEST_TOP_UP_MANIFEST_READ_LOG"))
        .flatten();
    let bytes = tokio::task::spawn_blocking(move || {
        let bytes = storage_for_read.get(&manifest_hash_for_read)?;
        if let Some(log) = test_read_log {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log)?;
            writeln!(file, "{manifest_hash_for_read}")?;
        }
        anyhow::Ok(bytes)
    })
    .await
    .ok()?
    .ok()?;
    if crate::cas::hash(&bytes) != manifest_hash {
        return None;
    }
    let manifest = ClonepackManifest::decode(bytes.as_slice()).ok()?;
    if manifest.commit != artifact {
        return None;
    }
    ref_response_from_manifest(
        repo_id,
        provider,
        branch.to_string(),
        manifest_hash,
        &manifest,
        storage,
        private,
    )
}

async fn get_ref_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    requested_checkout: String,
    params: RefQuery,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if let Some(response) = validation::reject_if_invalid(|| {
        if requested_checkout == "HEAD" {
            Ok(())
        } else {
            validation::validate_checkout_name(&requested_checkout)
        }
    }) {
        return response;
    }
    if let Some(rev) = params.rev.as_deref()
        && let Some(response) = validation::reject_if_invalid(|| validation::validate_git_rev(rev))
    {
        return response;
    }
    if let Some(pinned) = params.pinned.as_deref()
        && let Some(response) =
            validation::reject_if_invalid(|| validation::validate_object_id(pinned))
    {
        return response;
    }
    match repo_is_added(&state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return repo_not_added_response(),
        Err(response) => return response,
    }

    let request_token = upstream_token_from_headers(&headers);
    let credential = match state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref())
    {
        Ok(credential) => credential,
        Err(error) => return credential_error_response(error),
    };
    let private =
        match authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
        {
            Ok(private) => private,
            Err(response) => return response,
        };

    state.metrics.record_ref_lookup();
    let (commit, checkout_name, already_pinned) = if let Some(pinned) = params.pinned.as_deref() {
        let _ = test_hook(TestStage::Http(format!(
            "GET /v1/repos/{}/refs/{requested_checkout}?pinned={pinned}&result={}",
            repo_id.storage_key(),
            params.result
        )))
        .await;
        let checkout_name = if requested_checkout == "HEAD"
            && params.rev.as_deref() == Some(pinned)
            && validation::validate_object_id(pinned).is_ok()
        {
            String::new()
        } else {
            requested_checkout.clone()
        };
        (pinned.to_string(), checkout_name, true)
    } else if let Some(rev) = params.rev.as_deref() {
        if validation::validate_object_id(rev).is_ok() {
            let checkout_name = if requested_checkout == "HEAD" {
                String::new()
            } else {
                requested_checkout.clone()
            };
            (rev.to_string(), checkout_name, false)
        } else {
            let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
            let lock = repo_lock(&state.sync_locks, &repo_id).await;
            let _guard = lock.lock().await;
            if let Err(error) = ensure_mirror(
                &mirror_dir,
                &provider,
                &repo_id,
                &requested_checkout,
                Some(rev),
                credential.as_ref(),
            )
            .await
            {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(ErrorResponse {
                        error: format!("cannot resolve exact revision {rev}: {error:#}"),
                    }),
                )
                    .into_response();
            }
            let checkout_name = if requested_checkout == "HEAD" {
                match git::default_branch(&mirror_dir)
                    .ok()
                    .filter(|name| !name.is_empty() && name != "HEAD")
                {
                    Some(name) => name,
                    None => {
                        return (
                            StatusCode::UNPROCESSABLE_ENTITY,
                            Json(ErrorResponse {
                                error: "cannot determine checkout name for HEAD".to_string(),
                            }),
                        )
                            .into_response();
                    }
                }
            } else {
                requested_checkout.clone()
            };
            let commit = match git::resolve_commit(&mirror_dir, rev) {
                Ok(commit) => commit,
                Err(error) => {
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(ErrorResponse {
                            error: format!("cannot resolve exact revision {rev}: {error:#}"),
                        }),
                    )
                        .into_response();
                }
            };
            (commit, checkout_name, false)
        }
    } else {
        let _ = test_hook(TestStage::TipProbe).await;
        let tip = {
            let _permit = fetch_semaphore()
                .acquire()
                .await
                .expect("fetch semaphore never closed");
            git::ls_remote_tip_async(
                &provider,
                &repo_id,
                &requested_checkout,
                credential.as_ref(),
            )
            .await
        };
        match tip {
            Ok(Some(tip)) => {
                let checkout_name = if requested_checkout == "HEAD" {
                    match tip.default_branch.filter(|name| !name.is_empty()) {
                        Some(name) => name,
                        None => {
                            return (
                                StatusCode::UNPROCESSABLE_ENTITY,
                                Json(ErrorResponse {
                                    error: "upstream HEAD did not advertise a checkout name"
                                        .to_string(),
                                }),
                            )
                                .into_response();
                        }
                    }
                } else {
                    requested_checkout.clone()
                };
                (tip.commit, checkout_name, false)
            }
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!("upstream ref not found: {requested_checkout}"),
                    }),
                )
                    .into_response();
            }
            Err(error) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse {
                        error: format!("upstream tip probe failed: {error:#}"),
                    }),
                )
                    .into_response();
            }
        }
    };

    let existing = match state.ref_store.load_result(&repo_id, &commit).await {
        Ok(result) => result,
        Err(error) => {
            state.metrics.record_error();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("exact result lookup failed: {error:#}"),
                }),
            )
                .into_response();
        }
    };
    if let Some(info) = existing.as_ref()
        && exact_result_ready(info, params.result, &commit)
    {
        return (
            StatusCode::OK,
            Json(ref_response(
                &repo_id,
                &provider,
                checkout_name,
                info,
                &state.storage,
                params.result,
                private,
            )),
        )
            .into_response();
    }
    if already_pinned {
        let key = format!("{}\x1f{}", repo_id.storage_key(), commit);
        let job_state = match state.build_queue.job_state_for_key(&key).await {
            Ok(state) => state,
            Err(error) => {
                state.metrics.record_error();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ExactRevisionUnavailableResponse {
                        error: format!("exact job lookup failed: {error:#}"),
                        commit,
                        branch: checkout_name,
                    }),
                )
                    .into_response();
            }
        };
        if let JobState::Failed(error) = job_state {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ExactRevisionUnavailableResponse {
                    error: format!("{} result failed: {error}", params.result),
                    commit,
                    branch: checkout_name,
                }),
            )
                .into_response();
        }
        if !matches!(job_state, JobState::Pending) {
            return (
                StatusCode::CONFLICT,
                Json(ExactRevisionUnavailableResponse {
                    error: format!(
                        "{} result is missing for exact commit and no job is active",
                        params.result
                    ),
                    commit,
                    branch: checkout_name,
                }),
            )
                .into_response();
        }
        if params.top_up && params.result == ExactResultKind::Full {
            let base = match existing.as_ref() {
                Some(info) => {
                    carried_full_top_up_response(
                        info,
                        &repo_id,
                        &provider,
                        &checkout_name,
                        &commit,
                        &state.ref_store,
                        &state.storage,
                        private,
                    )
                    .await
                }
                None => None,
            };
            return artifact_pending_response_with_top_up(
                &commit,
                &checkout_name,
                state.build_queue_depth.load(Ordering::Relaxed),
                Some(true),
                base,
            )
            .await;
        }
        return artifact_pending_response(&commit, &checkout_name, 0).await;
    }

    match admit_commit(&state, &repo_id, &commit, existing, move || Ok(credential)).await {
        Admission::Complete(_) => unreachable!("caller already filtered complete results"),
        Admission::Enqueued(_) => {
            artifact_pending_response(
                &commit,
                &checkout_name,
                state.build_queue_depth.load(Ordering::Relaxed),
            )
            .await
        }
        Admission::Error(error) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ExactRevisionUnavailableResponse {
                    error,
                    commit,
                    branch: checkout_name,
                }),
            )
                .into_response()
        }
    }
}

const REF_SIGNED_URL_TTL_PUBLIC_SECS: u64 = 1200;
const REF_SIGNED_URL_TTL_PRIVATE_SECS: u64 = 300;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn ref_signed_url_ttl(private: bool) -> Duration {
    if private {
        Duration::from_secs(env_u64(
            "RIPCLONE_SIGNED_URL_TTL_PRIVATE_SECS",
            REF_SIGNED_URL_TTL_PRIVATE_SECS,
        ))
    } else {
        Duration::from_secs(env_u64(
            "RIPCLONE_SIGNED_URL_TTL_SECS",
            REF_SIGNED_URL_TTL_PUBLIC_SECS,
        ))
    }
}

/// Artifact hashes needed to build one [`RefResponse`], independent of
/// whether they came from a loaded `RefInfo` or from decoded manifest bytes
/// (the top-up path).
struct ResponseArtifacts {
    commit: String,
    parent_commit: Option<String>,
    manifest: String,
    metadata_chunk: String,
    archive_chunks: Vec<String>,
    head_blobs_chunks: Vec<String>,
    head_blobs_idx: Option<String>,
    packs: Vec<String>,
    midx: String,
    idx_bundle: String,
}

/// Sign and order the URLs for one ref response. Shared by every builder:
/// each caller extracts a [`ResponseArtifacts`] from its own source (a
/// `RefInfo`, or a decoded manifest) and hands it here.
fn build_ref_response(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    artifacts: &ResponseArtifacts,
    storage: &crate::storage::StorageRef,
    result: ExactResultKind,
    private: bool,
) -> RefResponse {
    let ttl = ref_signed_url_ttl(private);
    let signed = |hash: &str| signed_url(storage, ttl, hash);
    // `None` entries (e.g. local backend) fall back to the gateway. Ordered
    // to match the manifest's own chunk/pack lists.
    let signed_list = |hashes: &[String]| -> Option<Vec<Option<String>>> {
        if hashes.is_empty() {
            return None;
        }
        let urls: Vec<Option<String>> = hashes.iter().map(|hash| signed(hash)).collect();
        (!urls.iter().all(Option::is_none)).then_some(urls)
    };
    let (owner, repo) = repo_id
        .github_owner_repo()
        .map(|(o, r)| (o.to_string(), r.to_string()))
        .unwrap_or_else(|| (repo_id.provider.as_str().to_string(), repo_id.path.clone()));
    RefResponse {
        owner,
        repo,
        provider: provider.id.as_str().to_string(),
        host: provider.host.clone(),
        origin_url: provider.clone_url(&repo_id.path),
        branch,
        commit: artifacts.commit.clone(),
        parent_commit: artifacts.parent_commit.clone(),
        clonepack_manifest: artifacts.manifest.clone(),
        clonepack_manifest_url: signed(&artifacts.manifest),
        metadata_chunk: artifacts.metadata_chunk.clone(),
        metadata_chunk_url: signed(&artifacts.metadata_chunk),
        archive_chunk_urls: signed_list(&artifacts.archive_chunks),
        head_blobs_chunk_urls: signed_list(&artifacts.head_blobs_chunks),
        head_blobs_idx_url: artifacts.head_blobs_idx.as_deref().and_then(signed),
        pack_chunk_urls: signed_list(&artifacts.packs),
        // Sign the pre-built MIDX for the selected variant so the client
        // installs it directly instead of running `git multi-pack-index
        // write`, and the idx bundle so the client fetches all idx in one GET.
        midx_url: signed(&artifacts.midx),
        idx_bundle_url: signed(&artifacts.idx_bundle),
        result,
    }
}

fn ref_response(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    info: &RefInfo,
    storage: &crate::storage::StorageRef,
    result: ExactResultKind,
    private: bool,
) -> RefResponse {
    let artifacts = exact_result_clonepack(info, result)
        .expect("requested exact result is ready before response construction");
    let archive_chunks = info
        .files
        .as_ref()
        .filter(|_| result == ExactResultKind::Files)
        .map(|files| files.archive_chunks.clone())
        .unwrap_or_default();
    let packs = match result {
        ExactResultKind::Head => info.head.as_ref().map(|head| head.packs.as_slice()),
        ExactResultKind::Full => info.full.as_ref().map(|full| full.packs.as_slice()),
        ExactResultKind::Files => None,
    }
    .unwrap_or_default()
    .iter()
    .map(|p| p.pack.clone())
    .collect();
    let response_artifacts = ResponseArtifacts {
        commit: artifacts.commit.clone(),
        parent_commit: info
            .head
            .as_ref()
            .and_then(|head| head.parent_commit.clone()),
        manifest: artifacts.manifest.clone(),
        metadata_chunk: artifacts.metadata_chunk.clone(),
        archive_chunks,
        head_blobs_chunks: Vec::new(),
        head_blobs_idx: None,
        packs,
        midx: artifacts.midx.clone(),
        idx_bundle: artifacts.idx_bundle.clone(),
    };
    build_ref_response(
        repo_id,
        provider,
        branch,
        &response_artifacts,
        storage,
        result,
        private,
    )
}

/// Ready admission response. Signing already-known object URLs does not read
/// artifact bytes; keeping the byte totals absent preserves the no-op proof's
/// zero artifact-read boundary.
fn sync_response_without_storage_read(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    info: &RefInfo,
    storage: &crate::storage::StorageRef,
    result: ExactResultKind,
    private: bool,
    status: impl Into<String>,
) -> SyncResponse {
    SyncResponse {
        ref_info: ref_response(repo_id, provider, branch, info, storage, result, private),
        status: status.into(),
        phases: SyncPhases::default(),
        bytes: None,
        unique_bytes: None,
    }
}

fn signed_url(storage: &crate::storage::StorageRef, ttl: Duration, hash: &str) -> Option<String> {
    if hash.is_empty() {
        return None;
    }
    storage.signed_url(hash, ttl)
}

/// Single-tenant trust mode only: the client tags a request with the visibility
/// it resolved. Absent means public for direct self-host clients; malformed
/// values fail closed to private. This is advisory and trusted ONLY when
/// `require_repo_auth` is off; the enforced path derives visibility from the
/// provider via [`authorize_repo_read`] instead.
fn visibility_is_private(headers: &HeaderMap) -> bool {
    match headers.get("x-ripclone-visibility") {
        None => false,
        Some(value) => value
            .to_str()
            .map(|v| !v.eq_ignore_ascii_case("public"))
            .unwrap_or(true),
    }
}

fn credential_error_response(e: anyhow::Error) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: format!("credential fetch failed: {e}"),
        }),
    )
        .into_response()
}

/// 403 for a caller that may not read this repo.
fn forbidden_repo_response() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(ErrorResponse {
            error: "not authorized for this repository".to_string(),
        }),
    )
        .into_response()
}

/// Per-repo read authorization gate (AU1). Every repo-read entry point calls
/// this before serving content or signing URLs. On success it returns whether
/// the repo is private (for signed-URL TTL); on failure it returns a 403 the
/// caller must propagate.
///
/// Enforced path (`require_repo_auth`): public repos are served anonymously,
/// private repos require the caller's own credential to prove read access
/// against the provider (cached). This is what stops a holder of the shared
/// server token from reading an already-cached private repo it has no access to.
/// Trust mode (`RIPCLONE_TRUST_GATEWAY=1`): the gate is skipped and visibility
/// comes from the client header (single-tenant self-host behavior).
async fn authorize_repo_read(
    state: &ServerState,
    provider: &ProviderInstance,
    repo_id: &RepoId,
    credential: Option<&secrecy::SecretString>,
    headers: &HeaderMap,
) -> Result<bool, Response> {
    if test_hook(TestStage::RepoReadsDenied).await.unwrap_or(false) {
        return Err(forbidden_repo_response());
    }
    if !state.require_repo_auth {
        return Ok(visibility_is_private(headers));
    }
    match state
        .access_verifier
        .verify(provider, &repo_id.path, credential)
        .await
    {
        AccessDecision::Public => Ok(false),
        AccessDecision::PrivateAuthorized => Ok(true),
        AccessDecision::Denied => Err(forbidden_repo_response()),
    }
}

#[derive(Deserialize, Default)]
struct RepoStatusQuery {
    #[serde(default)]
    public: bool,
    #[serde(default)]
    fork_of: Option<String>,
}

async fn repo_status_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    query: RepoStatusQuery,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    // AU1: status reveals a private repo's existence, commit, and byte sizes.
    let request_token = upstream_token_from_headers(&headers);
    let credential = match state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref())
    {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };
    if let Err(resp) =
        authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
    {
        return resp;
    }
    match build_repo_status(&state, &repo_id, query.public, query.fork_of.as_deref()).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("status failed: {}", e),
                }),
            )
                .into_response()
        }
    }
}

fn record_chunk(unique_chunks: &mut HashMap<String, u64>, hash: &str, len: u64) {
    if hash.is_empty() || len == 0 {
        return;
    }
    unique_chunks.insert(hash.to_string(), len);
}

async fn build_repo_status(
    state: &ServerState,
    repo_id: &RepoId,
    public: bool,
    fork_of: Option<&str>,
) -> Result<RepoStatusResponse> {
    let commits = state.ref_store.list_commits(repo_id).await?;
    let mut refs = Vec::new();
    let mut unique_chunks: HashMap<String, u64> = HashMap::new();
    for commit in commits {
        let Some(info) = state.ref_store.load_result(repo_id, &commit).await? else {
            continue;
        };
        let manifest_hashes = collect_manifest_hashes(&info);
        let mut ref_bytes = 0u64;
        for manifest_hash in manifest_hashes {
            let manifest_bytes = state.storage.get(&manifest_hash)?;
            let manifest_len = manifest_bytes.len() as u64;
            record_chunk(&mut unique_chunks, &manifest_hash, manifest_len);
            ref_bytes += manifest_len;
            let manifest = ClonepackManifest::decode(manifest_bytes.as_slice())
                .context("decode clonepack manifest for status")?;
            for chunk in manifest_chunk_refs(&manifest) {
                ref_bytes += chunk.len;
                record_chunk(&mut unique_chunks, &hash_to_hex(&chunk.hash), chunk.len);
            }
        }
        if exact_result_ready(&info, ExactResultKind::Full, &info.commit)
            && let Some(full) = &info.full
        {
            for level in &full.history_levels {
                for pack in &level.packs {
                    if !pack.pack.is_empty() {
                        record_chunk(&mut unique_chunks, &pack.pack, pack.pack_len);
                        ref_bytes += pack.pack_len;
                    }
                    if !pack.idx.is_empty() {
                        record_chunk(&mut unique_chunks, &pack.idx, pack.idx_len);
                        ref_bytes += pack.idx_len;
                    }
                }
            }
        }

        let job_key = format!("{}\u{1f}{}", repo_id.storage_key(), info.commit);
        let (job, job_error) = match state.build_queue.job_state_for_key(&job_key).await? {
            JobState::Pending => ("pending".to_string(), None),
            JobState::Done => ("done".to_string(), None),
            JobState::Failed(error) => ("failed".to_string(), Some(error)),
            JobState::Unknown => ("none".to_string(), None),
        };
        let is_public_fork = public && fork_of.is_some();
        let branch_unique_bytes = if is_public_fork { 0 } else { ref_bytes };
        let head_ready = exact_result_ready(&info, ExactResultKind::Head, &info.commit);
        let full_ready = exact_result_ready(&info, ExactResultKind::Full, &info.commit);
        let files_ready = exact_result_ready(&info, ExactResultKind::Files, &info.commit);

        refs.push(ExactStatusEntry {
            commit: info.commit,
            bytes: ref_bytes,
            unique_bytes: branch_unique_bytes,
            head: head_ready,
            full: full_ready,
            files: files_ready,
            job,
            job_error,
        });
    }

    refs.sort_by(|a, b| a.commit.cmp(&b.commit));
    let total_bytes = unique_chunks.values().sum();
    // TODO: cross-repo fork-network dedup for private repos. For now, public
    // forks receive zero unique-byte allocation and every other repository
    // reports its deduplicated logical bytes.
    let is_public_fork = public && fork_of.is_some();
    let total_unique_bytes = if is_public_fork { 0 } else { total_bytes };
    let regions = state
        .storage
        .regions()
        .into_iter()
        .map(|region| RegionStorageEntry {
            region,
            unique_bytes: total_unique_bytes,
        })
        .collect();

    let (owner, repo) = repo_id
        .github_owner_repo()
        .map(|(o, r)| (o.to_string(), r.to_string()))
        .unwrap_or_default();
    Ok(RepoStatusResponse {
        owner,
        repo,
        added: state.ref_store.load_added_repo(repo_id).await?.is_some(),
        refs,
        total_bytes,
        total_unique_bytes,
        regions,
    })
}

/// Ordinary branch-tip sync: resolve one exact upstream commit, then perform a
/// read-only ready check or admit that immutable target. The response never
/// waits for the builder.
async fn sync_repo_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    params: SyncRequest,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if let Some(resp) = validation::reject_if_invalid(|| {
        if params.branch == "HEAD" {
            Ok(())
        } else {
            validation::validate_checkout_name(&params.branch)
        }
    }) {
        return resp;
    }
    if params.rev.is_some() {
        return sync_repo_at_revision(repo_id, provider, params, headers, state).await;
    }
    match repo_is_added(&state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return repo_not_added_response(),
        Err(resp) => return resp,
    }

    let request_token = upstream_token_from_headers(&headers);
    let credential = match state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref())
    {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };
    let private =
        match authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    let start = Instant::now();
    let requested_branch = params.branch;
    let _ = test_hook(TestStage::TipProbe).await;
    let tip = {
        let _permit = fetch_semaphore()
            .acquire()
            .await
            .expect("fetch semaphore never closed");
        git::ls_remote_tip_async(&provider, &repo_id, &requested_branch, credential.as_ref()).await
    };
    let tip = match tip {
        Ok(Some(tip)) => tip,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("upstream ref not found: {requested_branch}"),
                }),
            )
                .into_response();
        }
        Err(e) => {
            state.metrics.record_error();
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse {
                    error: format!("upstream tip probe failed: {e:#}"),
                }),
            )
                .into_response();
        }
    };
    // The single bounded advertisement asks for symbolic HEAD alongside a
    // concrete requested branch. Preserve that identity for every exact-only
    // mirror, not only requests whose selector was HEAD, so later historical
    // HEAD~N resolution never falls back to Git's platform init default.
    let commit = tip.commit;
    let effective_branch = if requested_branch == "HEAD" {
        tip.default_branch
            .filter(|branch| !branch.is_empty())
            .unwrap_or_else(|| requested_branch.clone())
    } else {
        requested_branch.clone()
    };

    // Explicit sync owns every missing result. It is a no-op only when Head,
    // Full, and Files are all already present.
    let loaded_exact = match state.ref_store.load_result(&repo_id, &commit).await {
        Ok(info) => info,
        Err(e) => {
            state.metrics.record_error();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("ready check failed: {e:#}"),
                }),
            )
                .into_response();
        }
    };
    let admitted_branch = effective_branch.clone();
    match admit_commit(&state, &repo_id, &commit, loaded_exact, move || {
        Ok(credential)
    })
    .await
    {
        Admission::Complete(info) => {
            state.metrics.record_sync(start.elapsed());
            let resp = sync_response_without_storage_read(
                &repo_id,
                &provider,
                effective_branch,
                &info,
                &state.storage,
                ExactResultKind::Full,
                private,
                "no-op",
            );
            (StatusCode::OK, Json(resp)).into_response()
        }
        Admission::Enqueued(outcome) => (
            StatusCode::ACCEPTED,
            Json(BuildResponse {
                status: match outcome {
                    EnqueueOutcome::Enqueued => "queued",
                    EnqueueOutcome::Coalesced => "coalesced",
                    EnqueueOutcome::Full => "full",
                }
                .to_string(),
                // Admission is complete once enqueue returns. This process-local
                // counter is an informational hint and never performs a second
                // database operation after durable acceptance.
                queue_depth: state.build_queue_depth.load(Ordering::Relaxed),
                commit,
                branch: admitted_branch,
            }),
        )
            .into_response(),
        Admission::Error(error) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse { error }),
            )
                .into_response()
        }
    }
}

/// First-class exact-revision sync used by `sync --at REV`.
async fn sync_repo_at_revision(
    repo_id: RepoId,
    provider: ProviderInstance,
    params: SyncRequest,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if let Some(resp) = validation::reject_if_invalid(|| {
        if params.branch == "HEAD" {
            Ok(())
        } else {
            validation::validate_checkout_name(&params.branch)
        }
    }) {
        return resp;
    }
    let Some(at_rev) = params.rev else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "exact revision is required".to_string(),
            }),
        )
            .into_response();
    };
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&at_rev)) {
        return resp;
    }
    match repo_is_added(&state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return repo_not_added_response(),
        Err(resp) => return resp,
    }
    let start = Instant::now();
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let mut branch = params.branch;

    let request_token = upstream_token_from_headers(&headers);
    let credential = match state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref())
    {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };
    // AU1: a sync both builds and returns the ref (with signed URLs), so gate it.
    let private =
        match authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    // Resolve a symbolic historical selector exactly once. Subsequent work and
    // polls use only the selected object id. A concrete object id never needs
    // upstream resolution: an explicit branch remains the checkout name, while
    // the default HEAD selector is represented by an empty detached marker.
    let selector_is_exact = crate::validation::validate_object_id(&at_rev).is_ok();
    let needs_resolution_fetch = !selector_is_exact;
    let selected_commit = if needs_resolution_fetch {
        let lock = repo_lock(&state.sync_locks, &repo_id).await;
        let _guard = lock.lock().await;
        let mirror = mirror_dir.clone();
        let provider = provider.clone();
        let repo = repo_id.clone();
        let fetch_branch = branch.clone();
        let selector = at_rev.clone();
        let fetch_credential = credential.clone();
        let resolved = tokio::task::spawn_blocking(move || {
            git::sync_bare_mirror(
                &mirror,
                &provider,
                &repo,
                &fetch_branch,
                Some(&selector),
                fetch_credential.as_ref(),
            )?;
            let commit = git::resolve_commit(&mirror, &selector)?;
            let default_branch = git::default_branch(&mirror)
                .ok()
                .filter(|candidate| !candidate.is_empty() && candidate != "HEAD");
            Ok::<_, anyhow::Error>((commit, default_branch))
        })
        .await;
        match resolved {
            Ok(Ok((commit, default_branch))) => {
                if branch == "HEAD" {
                    branch = default_branch.unwrap_or_else(|| branch.clone());
                }
                commit
            }
            Ok(Err(error)) => {
                state.metrics.record_error();
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(ErrorResponse {
                        error: format!("cannot resolve exact revision {at_rev}: {error:#}"),
                    }),
                )
                    .into_response();
            }
            Err(error) => {
                state.metrics.record_error();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("exact revision resolution task failed: {error}"),
                    }),
                )
                    .into_response();
            }
        }
    } else {
        if branch == "HEAD" {
            branch.clear();
        }
        at_rev.clone()
    };
    let at_rev = selected_commit;

    // A retry resolves entirely from the local mirror and exact result row. An
    // explicit sync is a no-op only when every stored result is present.
    let loaded = match state.ref_store.load_result(&repo_id, &at_rev).await {
        Ok(Some(info)) if exact_result_complete(&info, &at_rev) => {
            state.metrics.record_sync(start.elapsed());
            let response = sync_response_without_storage_read(
                &repo_id,
                &provider,
                branch.clone(),
                &info,
                &state.storage,
                ExactResultKind::Full,
                private,
                "no-op",
            );
            return (StatusCode::OK, Json(response)).into_response();
        }
        Ok(loaded) => loaded,
        Err(error) => {
            state.metrics.record_error();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("exact revision lookup failed: {error:#}"),
                }),
            )
                .into_response();
        }
    };

    // Exact revisions use the same immutable `(repository, admitted commit)`
    // lane as ordinary requests; checkout names are never queue identity.
    match admit_commit(&state, &repo_id, &at_rev, loaded, move || Ok(credential)).await {
        Admission::Complete(_) => unreachable!("caller already filtered complete results"),
        Admission::Enqueued(_) => {
            artifact_pending_response(&at_rev, &branch, state.build_queue.depth().await).await
        }
        Admission::Error(error) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ExactRevisionUnavailableResponse {
                    error,
                    commit: at_rev,
                    branch,
                }),
            )
                .into_response()
        }
    }
}

async fn add_repo_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    params: AddRequest,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    let request_token = upstream_token_from_headers(&headers);
    let credential = match state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref())
    {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };
    if let Err(resp) =
        authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
    {
        return resp;
    }

    // Tiered-add preflight: capture repo size now so the first build can be
    // size-classified without a new API call at enqueue.
    let repo_size_bytes = preflight_repo_size_bytes(&provider, &repo_id, credential.as_ref()).await;
    let added = AddedRepo {
        repo_id: repo_id.clone(),
        added_at: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        history_enabled: true,
        source: params.source,
        repo_size_bytes,
    };
    if let Err(e) = state.ref_store.add_repo(&added).await {
        state.metrics.record_error();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("add failed: {e}"),
            }),
        )
            .into_response();
    }

    sync_repo_inner(
        repo_id,
        provider,
        SyncRequest {
            branch: "HEAD".to_string(),
            rev: None,
        },
        headers,
        state,
    )
    .await
}

async fn remove_added_repo_inner(repo_id: RepoId, state: ServerState) -> Response {
    match state.ref_store.load_added_repo(&repo_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return repo_not_added_response(),
        Err(e) => {
            state.metrics.record_error();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("added repo lookup failed: {e}"),
                }),
            )
                .into_response();
        }
    }
    if let Err(e) = state.ref_store.remove_added_repo(&repo_id).await {
        state.metrics.record_error();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("remove added repo failed: {e}"),
            }),
        )
            .into_response();
    }
    (StatusCode::NO_CONTENT, Body::empty()).into_response()
}

/// Tiered-add preflight: best-effort GitHub `repo.size` (KB → bytes). Used to
/// classify the first build without a prior clonepack. Failures return `None`
/// (first build maps to largest class) — never fail the add.
async fn preflight_repo_size_bytes(
    provider: &ProviderInstance,
    repo_id: &RepoId,
    credential: Option<&secrecy::SecretString>,
) -> Option<u64> {
    use crate::provider::ProviderKind;
    if provider.kind != ProviderKind::GitHub {
        return None;
    }
    // GitHub paths are always `owner/repo` (including Enterprise / non-default
    // instance ids). Do not use `github_owner_repo()` — that only matches the
    // built-in `github` instance id and would skip every GHE / renamed instance.
    let (owner, repo) = repo_id.path.split_once('/')?;
    if owner.is_empty() || repo.is_empty() || repo.contains('/') {
        return None;
    }
    // github.com → api.github.com; GitHub Enterprise → https://{host}/api/v3.
    let host = provider
        .host
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    let api_base = if host == "github.com" || host.is_empty() {
        "https://api.github.com".to_string()
    } else {
        format!("https://{host}/api/v3")
    };
    let url = format!("{api_base}/repos/{owner}/{repo}");
    let client = reqwest::ClientBuilder::new()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .ok()?;
    let mut req = client
        .get(&url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "ripclone");
    // REST API wants Bearer / token, not the git-HTTPS Basic x-access-token form.
    if let Some(cred) = credential {
        req = req.header("Authorization", format!("Bearer {}", cred.expose_secret()));
    }
    let resp = req.send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    #[derive(serde::Deserialize)]
    struct GhRepo {
        /// GitHub reports size in kilobytes.
        size: u64,
    }
    let body: GhRepo = resp.json().await.ok()?;
    Some(github_repo_size_kb_to_bytes(body.size))
}

/// GitHub's `repo.size` field is kilobytes; convert to bytes for classification.
fn github_repo_size_kb_to_bytes(size_kb: u64) -> u64 {
    size_kb.saturating_mul(1024)
}

/// Outcome of [`admit_commit`]: already complete, enqueued, or an error.
enum Admission {
    Complete(RefInfo),
    Enqueued(EnqueueOutcome),
    Error(String),
}

/// Admission core shared by `sync`, `sync --at`, the exact-ref lookup's
/// enqueue tail, and the build trigger. Selector resolution stays with the
/// caller (it differs per entry point); the caller also loads `loaded`
/// itself, since it already needs that row for its own readiness check and
/// keeps its own lookup-failure message. `credential` is called lazily so a
/// caller that can skip the fetch on an already-complete commit does.
async fn admit_commit<F>(
    state: &ServerState,
    repo_id: &RepoId,
    commit: &str,
    loaded: Option<RefInfo>,
    credential: F,
) -> Admission
where
    F: FnOnce() -> Result<Option<secrecy::SecretString>, String>,
{
    if let Some(info) = &loaded
        && exact_result_complete(info, commit)
    {
        return Admission::Complete(loaded.expect("checked Some above"));
    }
    let credential = match credential() {
        Ok(credential) => credential,
        Err(error) => return Admission::Error(error),
    };
    let prior_size_bytes = loaded.as_ref().and_then(|info| {
        let bytes = crate::queue::prior_clonepack_bytes(info);
        (bytes > 0).then_some(bytes)
    });
    let preflight_size_bytes = match state.ref_store.load_added_repo(repo_id).await {
        Ok(Some(added)) => added.repo_size_bytes,
        _ => None,
    };
    let size_bytes = crate::queue::resolve_job_size_bytes(prior_size_bytes, preflight_size_bytes);
    let job = BuildJob {
        repo_id: repo_id.clone(),
        admitted_commit: commit.to_string(),
        repo_config: crate::repo_config::RepoConfig::default(),
        credential,
        size_bytes,
    };
    match enqueue_admitted_build(state, job).await {
        Ok(outcome) => Admission::Enqueued(outcome),
        Err(error) => Admission::Error(error),
    }
}

/// Admit one exact ordinary-tip job. The local marker spans the whole
/// durable job; the database active-key constraint covers queued and claimed
/// rows.
async fn enqueue_admitted_build(
    state: &ServerState,
    mut job: BuildJob,
) -> Result<EnqueueOutcome, String> {
    crate::validation::validate_object_id(&job.admitted_commit)
        .map_err(|e| format!("invalid admitted commit: {e}"))?;
    job.repo_config = match &state.control_db {
        Some(control) => control
            .repository_config(&job.repo_id)
            .await
            .map_err(|error| format!("repository config read failed: {error:#}"))?
            .unwrap_or_default(),
        #[cfg(test)]
        None => job.repo_config,
        #[cfg(not(test))]
        None => return Err("server control database is unavailable".to_string()),
    };
    job.repo_config
        .validate()
        .map_err(|error| format!("repository config validation failed: {error:#}"))?;
    if let Some(control) = &state.control_db {
        let admission = ExactAdmissionPlan {
            pending: pending_exact_result(&job),
        };
        state.metrics.record_build_queued();
        let _ = test_hook(TestStage::BeforeAdmissionTx(&job.admitted_commit)).await;
        return match control.admit_exact_and_job(&job, &admission.pending).await {
            Ok(enqueued) => {
                if enqueued.outcome == EnqueueOutcome::Enqueued {
                    state.metrics.record_build_accepted();
                } else {
                    state.metrics.rollback_build_queued();
                }
                let _ = test_hook(TestStage::Enqueue(enqueued.outcome)).await;
                Ok(enqueued.outcome)
            }
            Err(error) => {
                state.metrics.rollback_build_queued();
                Err(format!("durable admission failed: {error:#}"))
            }
        };
    }
    let Some(admission) = prepare_exact_admission(state, &job).await? else {
        state.metrics.record_build_accepted();
        let _ = test_hook(TestStage::Enqueue(EnqueueOutcome::Coalesced)).await;
        return Ok(EnqueueOutcome::Coalesced);
    };
    state
        .ref_store
        .save_result(&job.repo_id, &admission.pending)
        .await
        .map_err(|error| format!("exact admission persistence failed: {error}"))?;
    state.metrics.record_build_queued();
    match state.build_queue.enqueue(job).await {
        Ok(enq) if enq.outcome == EnqueueOutcome::Enqueued => {
            state.metrics.record_build_accepted();
            let _ = test_hook(TestStage::Enqueue(enq.outcome)).await;
            Ok(enq.outcome)
        }
        Ok(enq) if enq.outcome == EnqueueOutcome::Coalesced => {
            state.metrics.rollback_build_queued();
            let _ = test_hook(TestStage::Enqueue(enq.outcome)).await;
            Ok(enq.outcome)
        }
        Ok(_) => {
            state.metrics.rollback_build_queued();
            let _ = test_hook(TestStage::Enqueue(EnqueueOutcome::Full)).await;
            Err("build queue full".to_string())
        }
        Err(e) => {
            state.metrics.rollback_build_queued();
            Err(format!("build queue unavailable: {e}"))
        }
    }
}

struct ExactAdmissionPlan {
    pending: RefInfo,
}

async fn prepare_exact_admission(
    state: &ServerState,
    job: &BuildJob,
) -> Result<Option<ExactAdmissionPlan>, String> {
    let commit = job.admitted_commit.as_str();
    let existing = state
        .ref_store
        .load_result(&job.repo_id, commit)
        .await
        .map_err(|e| format!("exact admission lookup failed: {e}"))?;
    if existing
        .as_ref()
        .is_some_and(|result| exact_result_complete(result, commit))
    {
        return Ok(None);
    }
    let pending = existing.unwrap_or_else(|| pending_exact_result(job));
    Ok(Some(ExactAdmissionPlan { pending }))
}

fn pending_exact_result(job: &BuildJob) -> RefInfo {
    RefInfo {
        commit: job.admitted_commit.clone(),
        ..Default::default()
    }
}

/// Fire-and-forget: enqueue a build for `(repo_id, admitted_commit)` and return
/// immediately — the build runs ahead of any clone
/// (build-before-clone).
/// Used by the `/build` OIDC endpoint, the push-webhook receiver, and the poll
/// loop. The durable active-key constraint coalesces it exactly like `/sync`.
/// Credentials come from the server's standing
/// provider token (the caller carries no per-request token). Returns `Ok` if the
/// build is queued or folded into one already running; `Err(msg)` if the queue is
/// unavailable.
async fn trigger_build(
    state: &ServerState,
    repo_id: &RepoId,
    admitted_commit: String,
) -> Result<EnqueueOutcome, String> {
    match state.ref_store.load_added_repo(repo_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return Ok(EnqueueOutcome::Coalesced),
        Err(e) => return Err(format!("added repo lookup failed: {e}")),
    }
    crate::validation::validate_object_id(&admitted_commit)
        .map_err(|e| format!("invalid admitted commit: {e}"))?;
    let existing = state
        .ref_store
        .load_result(repo_id, &admitted_commit)
        .await
        .map_err(|e| format!("exact result lookup failed: {e}"))?;
    // A signed replay/poller wakeup for a branch that already serves this
    // exact full commit is a read-only no-op. Do not fetch credentials or
    // touch the queue merely because the trusted trigger was repeated:
    // `admit_commit` only calls `credential` once it knows enqueue is needed.
    match admit_commit(state, repo_id, &admitted_commit, existing, || {
        state
            .broker
            .fetch_credential(repo_id, None)
            .map_err(|e| e.to_string())
    })
    .await
    {
        Admission::Complete(_) => Ok(EnqueueOutcome::Coalesced),
        Admission::Enqueued(outcome) => Ok(outcome),
        Admission::Error(error) => Err(error),
    }
}

/// Legacy branch selectors are parsed only so they can fail closed without
/// changing repository configuration or admitting work.
#[derive(Deserialize)]
struct AdminConfigQuery {
    #[serde(default)]
    branch: Option<String>,
}

/// `GET /v1/admin/config/{owner}/{repo}` — return the stored repository config
/// (404 if none is stored).
async fn admin_get_config(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<AdminConfigQuery>,
    State(state): State<ServerState>,
) -> Response {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if query.branch.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "branch-level repository configuration is no longer supported".to_string(),
            }),
        )
            .into_response();
    }
    let repo_id = RepoId::github(format!("{owner}/{repo}"));
    let loaded = match &state.control_db {
        Some(control) => control.repository_config(&repo_id).await,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "server control database is unavailable".to_string(),
                }),
            )
                .into_response();
        }
    };
    match loaded {
        Ok(Some(config)) => (StatusCode::OK, Json(config)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "no config stored for this repository".to_string(),
            }),
        )
            .into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("load config: {e:#}"),
                }),
            )
                .into_response()
        }
    }
}

/// `POST /v1/admin/config/{owner}/{repo}` — store the repository config. The
/// body is validated before the control database is written; the next admitted
/// job snapshots it.
async fn admin_put_config(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<AdminConfigQuery>,
    State(state): State<ServerState>,
    Json(config): Json<crate::repo_config::RepoConfig>,
) -> Response {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    if query.branch.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "branch-level repository configuration is no longer supported".to_string(),
            }),
        )
            .into_response();
    }
    if let Err(e) = config.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("invalid config: {e:#}"),
            }),
        )
            .into_response();
    }
    let repo_id = RepoId::github(format!("{owner}/{repo}"));
    let stored = match &state.control_db {
        Some(control) => control.put_repository_config(&repo_id, &config).await,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "server control database is unavailable".to_string(),
                }),
            )
                .into_response();
        }
    };
    match stored {
        Ok(()) => (StatusCode::OK, Json(config)).into_response(),
        Err(e) => {
            state.metrics.record_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("store config: {e:#}"),
                }),
            )
                .into_response()
        }
    }
}

async fn build_handler(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(body): Json<BuildRequest>,
) -> impl IntoResponse {
    if let Some(resp) = reject_invalid_repo_ids(&body.owner, &body.repo) {
        return resp;
    }
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&body.commit))
    {
        return resp;
    }
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&body.ref_name))
    {
        return resp;
    }
    // The build endpoint accepts GitHub's OIDC token in the standard
    // Authorization header and the ripclone token in a dedicated header.
    let oidc_token = match headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "missing OIDC Authorization: Bearer token".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Verify the ripclone token if one is configured.
    if let Some(expected) = &state.token_hash {
        let ripclone_header = headers
            .get("X-Ripclone-Token")
            .and_then(|v| v.to_str().ok());
        let authorized = ripclone_header
            .map(|v| check_auth_header(&format!("Ripclone {v}"), expected))
            .unwrap_or(false);
        if !authorized {
            return (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "unauthorized".to_string(),
                }),
            )
                .into_response();
        }
    }

    let verifier = match &state.oidc_verifier {
        Some(v) => v,
        None => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                Json(ErrorResponse {
                    error: "OIDC verification is not configured".to_string(),
                }),
            )
                .into_response();
        }
    };

    if let Err(e) = verifier.verify(oidc_token, &body.owner, &body.repo).await {
        state.metrics.record_error();
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: format!("OIDC verification failed: {}", e),
            }),
        )
            .into_response();
    }

    let job_repo_id = RepoId::github(format!("{}/{}", body.owner, body.repo));

    // `/v1/build` is an explicit HEAD wakeup. The body commit is intentionally
    // untrusted; resolve HEAD once, then pass that exact result to admission.
    let Some(provider) = state.provider_registry.get("github").cloned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "github provider is not configured".to_string(),
            }),
        )
            .into_response();
    };
    let credential = match state.broker.fetch_credential(&job_repo_id, None) {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };
    let _ = test_hook(TestStage::TipProbe).await;
    let tip = {
        let _permit = fetch_semaphore()
            .acquire()
            .await
            .expect("fetch semaphore never closed");
        git::ls_remote_tip_async(&provider, &job_repo_id, "HEAD", credential.as_ref()).await
    };
    let tip = match tip {
        Ok(Some(tip)) => tip,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "upstream HEAD has no commit".to_string(),
                }),
            )
                .into_response();
        }
        Err(e) => {
            state.metrics.record_error();
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse {
                    error: format!("upstream HEAD probe failed: {e:#}"),
                }),
            )
                .into_response();
        }
    };
    let admitted_branch = tip
        .default_branch
        .clone()
        .unwrap_or_else(|| "HEAD".to_string());
    match trigger_build(&state, &job_repo_id, tip.commit.clone()).await {
        Ok(outcome) => (
            StatusCode::ACCEPTED,
            Json(BuildResponse {
                status: match outcome {
                    EnqueueOutcome::Enqueued => "queued",
                    EnqueueOutcome::Coalesced => "coalesced",
                    EnqueueOutcome::Full => "full",
                }
                .to_string(),
                queue_depth: state.build_queue_depth.load(Ordering::Relaxed),
                commit: tip.commit,
                branch: admitted_branch,
            }),
        )
            .into_response(),
        Err(error) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse { error }),
            )
                .into_response()
        }
    }
}

#[derive(Serialize)]
struct WebhookAccepted {
    ok: bool,
}

#[derive(Serialize)]
struct WebhookIgnored {
    ignored: &'static str,
}

/// Acknowledge an event we deliberately don't act on. Always `200` so the
/// provider doesn't retry a delivery we simply chose to ignore.
fn webhook_ignored(reason: &'static str) -> Response {
    (StatusCode::OK, Json(WebhookIgnored { ignored: reason })).into_response()
}

/// `POST /webhooks/{provider}` — provider-agnostic webhook receiver.
async fn webhook_handler(
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    body: Body,
) -> Response {
    handle_webhook(state, provider_id, headers, body).await
}

/// verify (HMAC over the RAW body) → normalize → trigger a build via the shared
/// `trigger_build` path (so the build runs ahead of any clone, coalescing with
/// `/sync`). Responds 2xx fast. Fail-closed: no configured secret ⇒ 503; bad
/// signature ⇒ 401. The payload is trusted only for routing (which repo / ref),
/// never to choose a credential or escalate.
async fn handle_webhook(
    state: ServerState,
    provider_id: String,
    headers: HeaderMap,
    body: Body,
) -> Response {
    // Resolve the configured provider instance from the path.
    let Some(provider) = state.provider_registry.get(&provider_id).cloned() else {
        return unknown_provider_response();
    };
    // Phase 1: only GitHub has a webhook adapter; other kinds are follow-ups.
    let Some(adapter) = crate::webhook::provider_for(provider.kind) else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                error: format!(
                    "webhooks not yet implemented for provider kind '{}'",
                    provider.kind.as_str()
                ),
            }),
        )
            .into_response();
    };
    // Fail closed: no configured secret for this provider ⇒ 503.
    let Some(secret) = state.webhook_config.secret(provider.id.as_str()).cloned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!(
                    "no webhook secret configured for provider '{}'",
                    provider.id
                ),
            }),
        )
            .into_response();
    };
    // Read the RAW body before any JSON parse — the HMAC covers these exact
    // bytes. Cap the buffer well below the global request limit: the signature
    // can only be checked after the whole body is buffered, so an unauthenticated
    // caller must not be able to make us hold a huge request before the 401.
    let raw = match axum::body::to_bytes(body, MAX_WEBHOOK_BODY_BYTES).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: format!("webhook body too large or unreadable: {e}"),
                }),
            )
                .into_response();
        }
    };
    // Verify the signature in constant time over the raw bytes.
    if !adapter.verify(&headers, &raw, secret.expose_secret()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "invalid webhook signature".to_string(),
            }),
        )
            .into_response();
    }
    // Normalize. Unhandled events parse to None and are acknowledged as ignored.
    let Some(event) = adapter.parse(&headers, &raw) else {
        return webhook_ignored("unhandled event");
    };
    match event.kind {
        EventKind::Ping => (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response(),
        EventKind::Push => webhook_dispatch_push(&state, &provider, event).await,
        EventKind::BranchDelete => webhook_ignored("exact results outlive branch deletion"),
    }
}

/// Whether the webhook allowlist admits this repo. Matches the operator-facing
/// natural key (`owner/repo` for github, `provider/path` otherwise); for the
/// github default it ALSO accepts the explicit `github/owner/repo` form, so an
/// operator generalizing from the `gitlab/...` examples isn't silently bitten by
/// github's bare-key asymmetry.
fn webhook_repo_allowed(state: &ServerState, repo_id: &RepoId) -> bool {
    let cfg = &state.webhook_config;
    cfg.allows(&repo_id.natural_key())
        || (repo_id.is_github_default() && cfg.allows(&format!("github/{}", repo_id.path)))
}

/// Default-branch push → exact admission. Applies the allowlist and added-repo
/// gates, then triggers the shared fire-and-forget path and returns immediately.
async fn webhook_dispatch_push(
    state: &ServerState,
    provider: &ProviderInstance,
    event: crate::webhook::CanonicalEvent,
) -> Response {
    let repo_id = RepoId {
        provider: provider.id.clone(),
        path: event.repo.clone(),
    };
    // Validate the payload-supplied path so a hostile push can't escape storage
    // keys. We trust the payload only for routing.
    if validation::validate_repo_path(provider, &repo_id).is_err() {
        return webhook_ignored("invalid repo path");
    }
    // Allowlist gate (allow-all when unconfigured).
    if !webhook_repo_allowed(state, &repo_id) {
        return webhook_ignored("repo not in webhook allowlist");
    }
    match repo_is_added(state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return webhook_ignored("repo not added"),
        Err(resp) => return resp,
    }
    // Only branch pushes can identify the admitted default-branch commit; tags
    // and other refs are ignored.
    let Some(branch) = event
        .ref_
        .strip_prefix("refs/heads/")
        .filter(|b| !b.is_empty())
    else {
        return webhook_ignored("non-branch ref");
    };
    let branch = branch.to_string();
    // Validate the payload-derived branch before it reaches the queue / git.
    if validation::validate_checkout_name(&branch).is_err() {
        return webhook_ignored("invalid branch");
    }
    let Some(after) = event.after.as_deref() else {
        return webhook_ignored("push event has no after commit");
    };
    if validation::validate_object_id(after).is_err() {
        return webhook_ignored("push event has invalid after commit");
    }
    // Only a signed event that names its default branch may admit a commit.
    // Missing identity and non-default pushes are acknowledged without work.
    if event.default_branch.as_deref() != Some(branch.as_str()) {
        return webhook_ignored("push is not identified as the default branch");
    }
    // The signed, validated `after` is the admission target. No second
    // upstream probe is permitted on this path.
    match trigger_build(state, &repo_id, after.to_string()).await {
        Ok(_) => {
            info!(
                "webhook: triggered build for {}@{branch}",
                repo_id.storage_key()
            );
            (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response()
        }
        Err(error) => {
            state.metrics.record_error();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse { error }),
            )
                .into_response()
        }
    }
}

fn validate_artifact_hash(hash: &str) -> Option<Response> {
    if let Err(e) = crate::cas::Cas::validate_artifact_id(hash) {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response(),
        );
    }
    None
}

/// Test-only fault injection. When the server was started with
/// `RIPCLONE_TEST_FAIL_FIRST_FETCHES=N`, the first N artifact GETs return 503 so
/// the client's retry/backoff can be exercised end to end. The threshold is read
/// once at construction (0 = off, the production default), so this is a single
/// atomic load on the hot path. The counter lives in `ServerState`, so each
/// test's server starts fresh.
fn maybe_inject_artifact_fault(state: &ServerState) -> Option<Response> {
    if state.fail_first_fetches == 0 {
        return None;
    }
    let seen = state.artifact_fetch_count.fetch_add(1, Ordering::Relaxed);
    if seen < state.fail_first_fetches {
        Some((StatusCode::SERVICE_UNAVAILABLE, "injected transient fault").into_response())
    } else {
        None
    }
}

async fn get_artifact(
    Path(hash): Path<String>,
    headers: axum::http::HeaderMap,
    State(state): State<ServerState>,
) -> impl IntoResponse {
    if let Some(resp) = maybe_inject_artifact_fault(&state) {
        return resp;
    }
    if let Some(resp) = validate_artifact_hash(&hash) {
        return resp;
    }
    serve_artifact(hash, state, Some(headers))
        .await
        .into_response()
}

const ARTIFACT_STREAM_CHUNK_BYTES: usize = 64 * 1024;

enum ArtifactBodySource {
    File(tokio::fs::File),
    Ranges {
        storage: crate::storage::StorageRef,
        hash: String,
        offset: u64,
    },
}

/// Stream an artifact through a two-chunk bounded channel. Local storage uses
/// one open file; uncommon non-file backends without signed URLs use fixed-size
/// range reads. Neither path allocates according to the artifact's total size.
fn artifact_body(
    mut source: ArtifactBodySource,
    len: u64,
    barrier: Option<ArtifactBarrier>,
) -> Body {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;

        let mut sent = 0u64;
        let corrupt_body = barrier.as_ref().is_some_and(|barrier| {
            matches!(barrier.range_behavior, ArtifactRangeBehavior::CorruptBody)
        });
        let mut corrupted = false;
        let barrier_after = barrier.as_ref().map(|barrier| {
            u64::try_from(barrier.after_bytes)
                .unwrap_or(u64::MAX)
                .min(len)
        });
        let mut barrier = barrier;
        while sent < len {
            if barrier_after == Some(sent) {
                let barrier = barrier.take().expect("barrier exists at its boundary");
                let entered = barrier
                    .entered
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                if let Some(entered) = entered {
                    let _ = entered.send(());
                }
                let proceed = barrier
                    .proceed
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                let should_continue = if let Some(proceed) = proceed {
                    proceed.await.is_ok() && !barrier.close_on_proceed
                } else {
                    false
                };
                if !should_continue {
                    let _ = tx
                        .send(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "injected test barrier close",
                        )))
                        .await;
                    return;
                }
            }

            let until_barrier = barrier_after
                .filter(|after| *after > sent)
                .map_or(u64::MAX, |after| after - sent);
            let wanted = usize::try_from(
                (len - sent)
                    .min(until_barrier)
                    .min(u64::try_from(ARTIFACT_STREAM_CHUNK_BYTES).unwrap_or(u64::MAX)),
            )
            .unwrap_or(ARTIFACT_STREAM_CHUNK_BYTES);
            let read = match &mut source {
                ArtifactBodySource::File(file) => {
                    let mut buffer = vec![0u8; wanted];
                    match file.read(&mut buffer).await {
                        Ok(0) => Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "artifact file ended before its recorded length",
                        )),
                        Ok(read) => {
                            buffer.truncate(read);
                            Ok(buffer)
                        }
                        Err(error) => Err(error),
                    }
                }
                ArtifactBodySource::Ranges {
                    storage,
                    hash,
                    offset,
                } => {
                    let storage = Arc::clone(storage);
                    let hash = hash.clone();
                    let start = *offset;
                    match tokio::task::spawn_blocking(move || {
                        storage.get_range(&hash, start, wanted as u64)
                    })
                    .await
                    {
                        Ok(Ok(bytes)) if !bytes.is_empty() => {
                            *offset += bytes.len() as u64;
                            Ok(bytes)
                        }
                        Ok(Ok(_)) => Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "artifact range ended before its recorded length",
                        )),
                        Ok(Err(error)) => Err(std::io::Error::other(error)),
                        Err(error) => Err(std::io::Error::other(error)),
                    }
                }
            };
            match read {
                Ok(mut bytes) => {
                    if let Some(barrier) = barrier.as_ref() {
                        barrier
                            .max_chunk_sent
                            .fetch_max(bytes.len(), Ordering::SeqCst);
                    }
                    if corrupt_body
                        && !corrupted
                        && let Some(byte) = bytes.first_mut()
                    {
                        *byte ^= 0x01;
                        corrupted = true;
                    }
                    sent += bytes.len() as u64;
                    if tx.send(Ok(Bytes::from(bytes))).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
        }
    });
    Body::from_stream(rx)
}

async fn serve_artifact(
    hash: String,
    state: ServerState,
    headers: Option<axum::http::HeaderMap>,
) -> impl IntoResponse {
    if let Some(barrier) = state.artifact_barrier.as_ref() {
        let range = headers.as_ref().and_then(|headers| {
            headers
                .get(axum::http::header::RANGE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        });
        barrier
            .artifact_requests
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push((hash.clone(), range));
    }
    // If the backend can hand out a signed URL, redirect the client there.
    // The client can then use its own Range requests against the CDN/object store.
    // Use the same visibility-aware TTL as the ref path (a private repo gets a
    // shorter-lived URL) rather than a flat window.
    let private = headers.as_ref().map(visibility_is_private).unwrap_or(false);
    if let Some(url) = state.storage.signed_url(&hash, ref_signed_url_ttl(private)) {
        state.metrics.record_artifact_request(0);
        return (
            StatusCode::TEMPORARY_REDIRECT,
            [("location", url.as_str())],
            Vec::new(),
        )
            .into_response();
    }

    let total_size = match tokio::task::spawn_blocking({
        let storage = state.storage.clone();
        let hash = hash.clone();
        move || -> anyhow::Result<u64> { storage.size(&hash) }
    })
    .await
    {
        Ok(Ok(size)) => size,
        _ => {
            state.metrics.record_error();
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("artifact not found: {}", hash),
                }),
            )
                .into_response();
        }
    };

    // Parse Range header if present.
    let requested_range = headers.as_ref().and_then(|h| {
        let value = h
            .get(axum::http::header::RANGE)
            .and_then(|v| v.to_str().ok())?;
        if let Some(barrier) = state
            .artifact_barrier
            .as_ref()
            .filter(|barrier| barrier.target.matches(&hash))
        {
            barrier
                .range_requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(value.to_string());
        }
        parse_byte_range(value, total_size)
    });
    let range_behavior = state
        .artifact_barrier
        .as_ref()
        .filter(|barrier| barrier.consumed.load(Ordering::SeqCst) && barrier.target.matches(&hash))
        .map_or(ArtifactRangeBehavior::Normal, |barrier| {
            barrier.range_behavior
        });
    let range = if matches!(range_behavior, ArtifactRangeBehavior::Ignore) {
        None
    } else {
        requested_range
    };

    if total_size == 0 {
        state.metrics.record_artifact_request(0);
        return (StatusCode::OK, [("content-length", "0")], Body::empty()).into_response();
    }

    let (start, end, status) = range
        .map(|(start, end)| (start, end, StatusCode::PARTIAL_CONTENT))
        .unwrap_or((0, total_size.saturating_sub(1), StatusCode::OK));
    let len = end - start + 1;
    let source = match tokio::task::spawn_blocking({
        let storage = Arc::clone(&state.storage);
        let hash = hash.clone();
        move || -> anyhow::Result<ArtifactBodySource> {
            if let Some(mut file) = storage.open_file(&hash)? {
                use std::io::{Seek, SeekFrom};
                file.seek(SeekFrom::Start(start))?;
                Ok(ArtifactBodySource::File(tokio::fs::File::from_std(file)))
            } else {
                Ok(ArtifactBodySource::Ranges {
                    storage,
                    hash,
                    offset: start,
                })
            }
        }
    })
    .await
    {
        Ok(Ok(source)) => source,
        _ => {
            state.metrics.record_error();
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("artifact not found: {}", hash),
                }),
            )
                .into_response();
        }
    };
    state.metrics.record_artifact_request(len);
    let barrier = state.artifact_barrier.clone().and_then(|barrier| {
        let matches = status == StatusCode::OK
            && !barrier.consumed.load(Ordering::SeqCst)
            && len > barrier.after_bytes as u64
            && barrier.target.matches(&hash);
        matches.then(|| {
            barrier.consumed.store(true, Ordering::SeqCst);
            barrier
        })
    });
    let body = artifact_body(source, len, barrier);
    if status == StatusCode::PARTIAL_CONTENT {
        let content_range = if matches!(range_behavior, ArtifactRangeBehavior::InvalidContentRange)
        {
            format!("bytes 0-{end}/{total_size}")
        } else {
            format!("bytes {start}-{end}/{total_size}")
        };
        (
            status,
            [
                ("content-range", content_range),
                ("content-length", len.to_string()),
            ],
            body,
        )
            .into_response()
    } else {
        (status, [("content-length", len.to_string())], body).into_response()
    }
}

/// Parse a single `bytes=start-end` range. Returns inclusive (start, end).
///
/// Clients with off-by-one range math may ask for an end past the object end;
/// clamp to the last byte rather than rejecting so the partial response still
/// satisfies the request.
fn parse_byte_range(range: &str, size: u64) -> Option<(u64, u64)> {
    let range = range.strip_prefix("bytes=")?;
    let (start_str, end_str) = range.split_once('-')?;
    let start: u64 = start_str.parse().ok()?;
    if start >= size {
        return None;
    }
    let end = if end_str.is_empty() {
        size.saturating_sub(1)
    } else {
        end_str.parse::<u64>().ok()?.min(size.saturating_sub(1))
    };
    if start > end {
        return None;
    }
    Some((start, end))
}

/// Remove `.tmp*` entries under `dir` whose mtime is older than `max_age`.
/// Best-effort cleanup of build temp dirs leaked by a killed sync.
fn sweep_stale_tempdirs(dir: &std::path::Path, max_age: Duration) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(".tmp") {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .map(|age| age > max_age)
            .unwrap_or(false);
        if !stale {
            continue;
        }
        let path = entry.path();
        let _ = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
    }
}

fn tuple_to_sized(p: &(String, u64, String, u64)) -> crate::SizedPack {
    crate::SizedPack {
        pack: p.0.clone(),
        pack_len: p.1,
        idx: p.2.clone(),
        idx_len: p.3,
    }
}

fn sized_to_tuple(p: &crate::SizedPack) -> (String, u64, String, u64) {
    (p.pack.clone(), p.pack_len, p.idx.clone(), p.idx_len)
}

/// LSM incremental-history configuration.
struct LsmConfig {
    /// When on, only the tail past the last sealed level is built each sync;
    /// prior levels are reused by hash from object storage (Tigris). On by
    /// default — disable with `RIPCLONE_LSM=0`.
    enabled: bool,
    /// Compact down to at most this many levels (merging the smallest adjacent
    /// pair) so the level count stays bounded under seal-every-sync.
    max_levels: usize,
}

fn lsm_config() -> LsmConfig {
    let enabled = std::env::var("RIPCLONE_LSM")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    let max_levels = std::env::var("RIPCLONE_LSM_MAX_LEVELS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16usize);
    LsmConfig {
        enabled,
        max_levels,
    }
}

/// Given the prior sealed levels and a freshly built tail, decide whether to
/// seal the tail into a new level, then compact the level set back under
/// `max_levels`. Returns `(history_packs, new_pack_tuples, new_levels)` where
/// `history_packs` is every history pack this manifest references (all levels
/// flattened — prior levels reused by hash), `new_pack_tuples` is the packs
/// freshly built this sync (the tail plus any compaction output to upload), and
/// `new_levels` is the levels to persist for the next sync.
/// `head_packs` is not included here (the caller handles the HEAD closure).
#[allow(clippy::too_many_arguments)]
async fn seal_and_compact(
    mirror_dir: &std::path::Path,
    cas: &Cas,
    commit: &str,
    prev_levels: Vec<crate::HistoryLevel>,
    sealed_tip: Option<String>,
    tail_packs: Vec<(String, u64, String, u64)>,
    history_target: u64,
    cfg: &LsmConfig,
) -> Result<(
    Vec<(String, u64, String, u64)>,
    Vec<(String, u64, String, u64)>,
    Vec<crate::HistoryLevel>,
)> {
    // Seal the tail into a new immutable level whenever HEAD advanced past the
    // last sealed tip and the tail is non-empty. The cold base (sealed_tip None)
    // always advances, so it always seals and becomes level 0. Compaction keeps
    // the level count bounded.
    let advances = sealed_tip.as_deref() != Some(commit);
    let seal = advances && !tail_packs.is_empty();
    let mut levels = prev_levels;
    let mut new_tuples = tail_packs.clone();
    if seal {
        levels.push(crate::HistoryLevel {
            tip_commit: commit.to_string(),
            packs: tail_packs.iter().map(tuple_to_sized).collect(),
        });
        let packed_mib: u64 =
            tail_packs.iter().map(|(_, plen, _, _)| plen).sum::<u64>() / (1024 * 1024);
        info!(
            "LSM: sealed level {} at {} ({} packs, {} MiB packed)",
            levels.len() - 1,
            &commit[..7.min(commit.len())],
            tail_packs.len(),
            packed_mib
        );
    }

    // Compact (off-thread; it re-packs ranges) so the level count stays bounded.
    if levels.len() > cfg.max_levels {
        let before = levels.len();
        let (md, c, levels_in, max, tgt) = (
            mirror_dir.to_path_buf(),
            cas.clone(),
            levels.clone(),
            cfg.max_levels,
            history_target,
        );
        let res = tokio::task::spawn_blocking(move || {
            PackBuilder::new(&md, &c).compact_levels(levels_in, max, tgt)
        })
        .await
        .context("compaction task")??;
        new_tuples.extend(res.new_packs.iter().cloned());
        levels = res.levels;
        info!("LSM: compacted {} levels -> {}", before, levels.len());
    }

    // Manifest history = every sealed level's packs (prior reused by hash + any
    // compaction output), flattened. We always seal an advancing non-empty tail,
    // so there is never an unsealed `(sealed_tip, HEAD]` remainder to append: the
    // only time `seal` is false is when the tail is empty (HEAD didn't advance).
    let history_packs: Vec<(String, u64, String, u64)> = levels
        .iter()
        .flat_map(|l| l.packs.iter().map(sized_to_tuple))
        .collect();
    Ok((history_packs, new_tuples, levels))
}

/// Load and decode a prior sync's metadata chunk and return its files table.
/// Bytes come from local CAS or object storage (the disposable local cache may
/// have been cleared after a prior upload). Returns `None` on any failure — the
/// caller then falls back to a full (non-incremental) build, so this is purely
/// best-effort optimization, never a correctness dependency.
fn load_metadata_files(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    metadata_hash: &str,
) -> Option<Vec<crate::clonepack::FileEntry>> {
    let bytes = cas
        .get(metadata_hash)
        .or_else(|_| storage.get(metadata_hash))
        .ok()?;
    let md = crate::clonepack::MetadataChunk::decode_and_validate(bytes.as_slice()).ok()?;
    Some(md.files)
}

/// Build one result's PackEntry list and concatenated idx bundle.
fn assemble_variant(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    tagged: &[(&(String, u64, String, u64), bool)],
) -> Result<(Vec<crate::clonepack::PackEntry>, Option<ChunkRef>, String)> {
    if tagged.is_empty() {
        return Ok((Vec::new(), None, String::new()));
    }
    use std::io::Write;

    let mut tmp = tempfile::Builder::new()
        .suffix(".idx-bundle")
        .tempfile_in(cas.root())
        .context("create idx bundle temp file")?;
    let mut entries = Vec::with_capacity(tagged.len());
    let mut len = 0u64;
    for &(pack, history_only) in tagged {
        let idx_bytes = cas.get(&pack.2).or_else(|_| storage.get(&pack.2))?;
        let offset = len;
        tmp.write_all(&idx_bytes)
            .context("write idx bytes to bundle")?;
        len += idx_bytes.len() as u64;
        entries.push(crate::clonepack::PackEntry {
            pack: Some(ChunkRef {
                hash: hash_from_hex(&pack.0)?,
                len: pack.1,
            }),
            idx: Some(ChunkRef {
                hash: hash_from_hex(&pack.2)?,
                len: pack.3,
            }),
            history_only,
            idx_bundle_offset: offset,
        });
    }
    tmp.flush().context("flush idx bundle temp file")?;
    let (hash, stored_len) = cas.put_file(tmp.path())?;
    anyhow::ensure!(
        stored_len == len,
        "idx bundle length changed while storing: expected {len}, got {stored_len}"
    );
    Ok((
        entries,
        Some(ChunkRef {
            hash: hash_from_hex(&hash)?,
            len,
        }),
        hash,
    ))
}

/// Build a multi-pack-index over `packs` from local CAS. Free-function form.
///
/// Reads each pack's bytes from the *local* CAS (no object-storage fallback), so
/// only call this when every pack was built this sync and is still local — e.g.
/// the head MIDX is shipped only when all head buckets were freshly built. For a
/// set with reused packs absent from the local cache, omit the MIDX and let the client
/// build its own.
fn assemble_midx(
    cas: &Cas,
    packs: &[(String, u64, String, u64)],
) -> Result<(Option<ChunkRef>, String)> {
    if packs.is_empty() {
        return Ok((None, String::new()));
    }
    use rayon::prelude::*;
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = packs
        .par_iter()
        .map(|(ph, _, ih, _)| Ok((cas.get(ph)?, cas.get(ih)?)))
        .collect::<Result<Vec<_>>>()?;
    let midx = crate::git::build_multi_pack_index_bytes(&pairs)?;
    let len = midx.len() as u64;
    let hash = cas.put(&midx)?;
    Ok((
        Some(ChunkRef {
            hash: hash_from_hex(&hash)?,
            len,
        }),
        hash,
    ))
}

#[allow(clippy::too_many_arguments)]
fn make_manifest(
    commit: &str,
    parent: &Option<String>,
    archive_chunks: &[ChunkRef],
    metadata_hash: &str,
    metadata_len: u64,
    packs: Vec<crate::clonepack::PackEntry>,
    midx: Option<ChunkRef>,
    idx_bundle: Option<ChunkRef>,
) -> Result<ClonepackManifest> {
    Ok(ClonepackManifest {
        commit: commit.to_string(),
        parent_commit: parent.clone(),
        metadata_chunk: Some(ChunkRef {
            hash: hash_from_hex(metadata_hash)?,
            len: metadata_len,
        }),
        archive_chunks: archive_chunks.to_vec(),
        packs,
        midx,
        idx_bundle,
        ..Default::default()
    })
}

/// Build the `[ChunkRef]` list for the archive chunks of a metadata chunk.
fn archive_chunk_refs(
    archive_chunk_hashes: &[String],
    metadata_chunk: &crate::clonepack::MetadataChunk,
) -> Result<Vec<ChunkRef>> {
    let lengths = crate::clonepack::archive_chunk_lengths(metadata_chunk)?;
    if lengths.len() != archive_chunk_hashes.len() {
        anyhow::bail!(
            "archive chunk hash/length mismatch: hashes={} lengths={}",
            archive_chunk_hashes.len(),
            lengths.len()
        );
    }
    archive_chunk_hashes
        .iter()
        .zip(lengths.iter())
        .map(|(hash, len)| {
            Ok(ChunkRef {
                hash: hash_from_hex(hash)?,
                len: *len,
            })
        })
        .collect()
}

/// Concurrency for artifact uploads. Defaults to 2x CPU cores.
fn upload_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(8)
        .max(1)
}

/// Upload `hashes` from CAS to storage with bounded concurrency.
/// `crash_commit`, when set, drives the direct worker-crash test: when the
/// `during_upload` barrier is armed for that commit, the first hash uploads
/// alone, the hook fires, and only then do the remaining hashes start. That
/// keeps exactly one upload path for tests and production while the crash
/// still lands with precisely one artifact in storage.
async fn upload_artifacts(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    hashes: Vec<String>,
    conc: usize,
    crash_commit: Option<&str>,
) -> Result<()> {
    let crash_gate = crash_commit
        .filter(|commit| test_build_crash_barrier_matches("during_upload", commit))
        .map(|commit| {
            let (opened, wait) = tokio::sync::watch::channel(false);
            (commit.to_string(), Arc::new(opened), wait)
        });
    futures::stream::iter(hashes.into_iter().enumerate().map(|(index, hash)| {
        let cas = cas.clone();
        let storage = storage.clone();
        let mut crash_gate = crash_gate.clone();
        async move {
            if index > 0
                && let Some((_, _, wait)) = crash_gate.as_mut()
            {
                wait.wait_for(|opened| *opened)
                    .await
                    .context("during_upload crash gate closed before opening")?;
            }
            let read_hash = hash.clone();
            let (path, len) = tokio::task::spawn_blocking(move || {
                let len = cas
                    .verify_object(&read_hash)
                    .with_context(|| format!("verify artifact {} before upload", read_hash))?;
                Ok::<_, anyhow::Error>((cas.path(&read_hash), len))
            })
            .await
            .context("verify artifact task")??;
            let upload_start = std::time::Instant::now();
            let _ = test_hook(TestStage::ArtifactUpload).await;
            storage
                .put_file_async(&hash, &path)
                .await
                .with_context(|| format!("upload artifact {}", hash))?;
            crate::perf::record_storage_upload(upload_start.elapsed(), len);
            if index == 0
                && let Some((commit, opened, _)) = crash_gate.as_ref()
            {
                test_hook(TestStage::BuildCrash {
                    stage: "during_upload",
                    commit,
                })
                .await?;
                let _ = opened.send(true);
            }
            Ok(())
        }
    }))
    .buffer_unordered(conc.max(1))
    .try_collect::<Vec<()>>()
    .await
    .map(|_| ())
}

fn archive_publish_upload_hashes(
    metadata_hash: &str,
    clonepack_hash: &str,
    download_bundle_hashes: &[String],
    new_reuse_frame_hashes: &[String],
) -> Vec<String> {
    let mut uploads: Vec<String> = vec![metadata_hash.to_string(), clonepack_hash.to_string()];
    uploads.extend(download_bundle_hashes.iter().cloned());
    uploads.extend(new_reuse_frame_hashes.iter().cloned());
    uploads.retain(|h| !h.is_empty());
    uploads.sort();
    uploads.dedup();
    uploads
}

/// After upload, a remote backend may drop local pack copies while keeping the
/// tiny idx files used by later bundle and MIDX builds. Local storage owns the
/// durable copy and keeps every uploaded artifact.
async fn settle_storage(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    uploaded: Vec<String>,
    keep_idx: std::collections::HashSet<String>,
) {
    if storage.is_remote() {
        for h in uploaded.iter().filter(|h| !keep_idx.contains(*h)) {
            let _ = cas.remove(h);
        }
    }
}

async fn do_sync(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    // Every request resolves to one immutable object before admission. Workers
    // fetch and build only this object; they never resolve a moving selector.
    admitted_commit: &str,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    provider: &ProviderInstance,
    credential: Option<&secrecy::SecretString>,
    // Validated repository config snapshotted into the durable job at admission.
    repo_config: &crate::repo_config::RepoConfig,
    // Per-repo lock. do_sync holds it only while mutating the mirror (fetch +
    // commit-graph), then drops it before the heavy read-only build, so different
    // repos build concurrently. Safe because auto-gc is off, so the build only
    // reads the mirror's packs.
    mirror_lock: &Arc<tokio::sync::Mutex<()>>,
    // Embedded workers use this one-shot to release their limited foreground
    // slot after Head is durably published. Dropping it on an earlier error
    // releases the slot as well; it is never queue or ownership state.
    mut foreground_release: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<SyncBuildResult> {
    let compression_level = repo_config.compression_level();
    info!("syncing {}@{}", repo_id.storage_key(), admitted_commit);

    // Per-phase timers so sync cost can be tuned with real numbers (RIPCLONE_LOG
    // shows them at INFO). `t_total` spans the whole build; `t` is reset at each
    // phase boundary.
    let t_total = Instant::now();
    let mut t = t_total;
    let mut phases = SyncPhases::default();

    let existing_before_fetch = ref_store.load_result(repo_id, admitted_commit).await?;
    if let Some(existing) = existing_before_fetch.as_ref()
        && exact_result_complete(existing, admitted_commit)
    {
        if let Some(release) = foreground_release.take() {
            let _ = release.send(());
        }
        return Ok(SyncBuildResult {
            info: existing.clone(),
            status: "no-op".to_string(),
            phases,
        });
    }

    // Best-effort: remove stale build temp dirs left by a previously killed
    // sync. `tempfile` cleans up on drop, but not on SIGKILL/OOM, so a crashed
    // build leaks a `.tmp*` dir in TMPDIR (= repo_root). Only sweep old ones so a
    // concurrent build's temp dir is never touched.
    if let Some(repo_root) = mirror_dir.parent() {
        sweep_stale_tempdirs(repo_root, Duration::from_secs(2 * 3600));
    }

    // Acquire the per-repo exclusive lock for the mirror-mutating prep below. The
    // ls-remote pre-check above is read-only (ref store + a network probe), so it
    // stayed lock-free. We hold this only through fetch + commit-graph [+ bitmap]
    // and drop it before the heavy read-only build (see the drop points below).
    let _guard = mirror_lock.lock().await;

    // Sync the bare mirror synchronously (blocking git call).
    let mirror_dir_sync = mirror_dir.to_path_buf();
    let mirror_dir = mirror_dir.to_path_buf();
    let provider_sync = provider.clone();
    let repo_id_sync = repo_id.clone();
    let admitted_commit_sync = admitted_commit.to_string();
    let credential_sync = credential.cloned();
    let _ = test_hook(TestStage::FetchEntry(Some(admitted_commit))).await;
    // Cap concurrent upstream fetches across the process (bandwidth + upstream
    // abuse limits). Held only across the fetch, not the build.
    let fetch_permit = fetch_semaphore()
        .acquire()
        .await
        .expect("fetch semaphore never closed");
    tokio::task::spawn_blocking(move || {
        git::sync_bare_mirror_admitted(
            &mirror_dir_sync,
            &provider_sync,
            &repo_id_sync,
            &admitted_commit_sync,
            credential_sync.as_ref(),
        )
    })
    .await
    .context("sync task")??;
    drop(fetch_permit);
    phases.mirror_fetch_ms = Some(duration_ms(t.elapsed()));
    info!("sync phase: mirror fetch {:?}", t.elapsed());
    t = Instant::now();

    // An empty upstream (no commits) mirrors fine but has nothing to build. Name
    // the cause instead of letting the rev-parse below fail with an opaque
    // "resolving rev 'HEAD'" error.
    if git::is_empty_repo(&mirror_dir).unwrap_or(false) {
        anyhow::bail!("repository has no commits (nothing to clone)");
    }

    crate::validation::validate_object_id(admitted_commit)
        .context("validate admitted commit before exact fetch")?;
    let resolved = git::resolve_commit(&mirror_dir, admitted_commit)
        .with_context(|| format!("resolve admitted commit {admitted_commit}"))?;
    if resolved != admitted_commit {
        anyhow::bail!(
            "exact admitted commit resolved unexpectedly: requested {admitted_commit}, got {resolved}"
        );
    }
    let commit = admitted_commit.to_string();
    let parent = git::parent_commit(&mirror_dir, &commit).ok().flatten();

    // Write a commit-graph so the rev-list walks in the skeleton + layered-pack
    // builds below are fast (a fresh --mirror clone has none). Best-effort.
    let cg_dir = mirror_dir.clone();
    let _ = tokio::task::spawn_blocking(move || git::write_commit_graph(&cg_dir)).await;
    phases.commit_graph_ms = Some(duration_ms(t.elapsed()));
    info!("sync phase: commit-graph {:?}", t.elapsed());

    info!("building missing results for {}", &commit[..7]);

    let existing = ref_store.load_result(repo_id, &commit).await?;
    if let Some(result) = existing
        && exact_result_ready(&result, ExactResultKind::Head, &commit)
        && let Some(head) = result.head.clone()
    {
        drop(_guard);
        if let Some(release) = foreground_release.take() {
            let _ = release.send(());
        }
        build_missing_full_and_files(
            cas,
            &mirror_dir,
            repo_id,
            &commit,
            head.parent_commit.clone().or(parent),
            head,
            ref_store,
            storage,
            compression_level,
            !exact_result_ready(&result, ExactResultKind::Full, &commit),
            !exact_result_ready(&result, ExactResultKind::Files, &commit),
            false,
        )
        .await?;
        ref_store.invalidate(repo_id, &commit).await;
        let info = ref_store
            .load_result(repo_id, &commit)
            .await?
            .context("exact result vanished after missing result build")?;
        return Ok(SyncBuildResult {
            info,
            status: "built".to_string(),
            phases,
        });
    }

    // Head is built and published first. Full and Files then read the mirror
    // concurrently, so release the mirror lock before artifact work begins.
    drop(_guard);
    build_and_publish_results(
        cas,
        &mirror_dir,
        repo_id,
        &commit,
        parent,
        ref_store,
        storage,
        t_total,
        compression_level,
        phases,
        foreground_release,
    )
    .await
}

fn pack_artifacts_of(packs: &[(String, u64, String, u64)]) -> Vec<crate::PackArtifact> {
    packs
        .iter()
        .map(|(p, _, i, _)| crate::PackArtifact {
            pack: p.clone(),
            idx: i.clone(),
        })
        .collect()
}

/// Result of the Head-closure build: a small delta pack against the
/// immutable base, or a fresh full base on a cold sync / rebase. See
/// `build_head_delta_pack` / `build_head_packs`.
struct HeadBuild {
    /// Every current HEAD pack (base + delta), manifest order — for the clonepack.
    all_packs: Vec<(String, u64, String, u64)>,
    /// Only the packs built this sync (to upload). Reused base packs are durable.
    new_built: Vec<(String, u64, String, u64)>,
    /// The commit whose closure `base_packs` covers (carried, or = commit on cold).
    base_commit: String,
    /// The base packs (closure of `base_commit`), carried unchanged across deltas.
    base_packs: Vec<crate::SizedPack>,
    /// True when every pack was built this sync (cold/rebase) → head MIDX buildable.
    all_local: bool,
    elapsed_ms: u64,
}

#[allow(clippy::too_many_arguments)]
async fn build_and_publish_results(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    commit: &str,
    parent: Option<String>,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    t_total: Instant,
    // zstd level for archive frames, from the effective repo config.
    compression_level: i32,
    mut phases: SyncPhases,
    mut foreground_release: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<SyncBuildResult> {
    let _ = test_hook(TestStage::HeadBuild).await;
    let _ = test_hook(TestStage::BuilderEntry(commit)).await;
    let upload_conc = upload_concurrency();

    // Load the previous synced ref once: used both for the files-table by-diff
    // below and for Option-A full-clonepack carry later in this phase.
    //
    // Head construction needs only the parent's exact Head result. Full and
    // Files select their own parent outputs independently below.
    let current = ref_store.load_result(repo_id, commit).await?;
    let prev_loaded = match parent.as_deref() {
        Some(parent_commit) => ref_store
            .load_result(repo_id, parent_commit)
            .await
            .ok()
            .flatten()
            .filter(|info| info.commit == parent_commit),
        None => None,
    };
    let prev = prev_loaded.filter(|candidate| {
        parent
            .as_deref()
            .is_some_and(|parent| exact_parent_head_ready(candidate, parent))
    });

    // Build the Head closure and shallow skeleton, then publish Head.
    let mut t = Instant::now();
    let sk_start = Instant::now();
    let (md1, c1, cm1) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let shallow_skeleton_handle = tokio::task::spawn_blocking(move || {
        let s = Instant::now();
        let r = PackBuilder::new(&md1, &c1).build_shallow_skeleton_pack(&cm1);
        info!("Head skeleton {:?}", s.elapsed());
        r
    });
    // Head-closure packs, incremental by delta against an immutable base: keep the
    // base packs (closure of `head_base_commit`) and pack only the depth-1 objects
    // new since that base (`closure(HEAD) − closure(base)`) into a delta pack. The
    // base and delta are disjoint by construction, so no object is ever in two HEAD
    // packs (which would double-materialize a worktree file). A cold sync (no base)
    // packs the full closure as the base. The cumulative delta grows as HEAD moves
    // from the base; a later Head build rebases once it exceeds
    // RIPCLONE_HEAD_REBASE_BYTES, off the depth=1 critical path.
    let head_target = 4 * 1024 * 1024;
    let prev_base_commit: Option<String> = prev
        .as_ref()
        .and_then(|result| result.head.as_ref())
        .map(|head| head.base_commit.clone())
        .filter(|c| !c.is_empty());
    let prev_base_packs: Vec<crate::SizedPack> = prev
        .as_ref()
        .and_then(|result| result.head.as_ref())
        .map(|head| head.base_packs.clone())
        .unwrap_or_default();
    let (md2, c2, cm2) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let head_handle = tokio::task::spawn_blocking(move || -> Result<HeadBuild> {
        let s = Instant::now();
        let b = PackBuilder::new(&md2, &c2);
        match (prev_base_packs.is_empty(), prev_base_commit) {
            // Delta path: a base exists; pack only what is new since the base.
            (false, Some(base_commit)) => {
                let delta = b.build_head_delta_pack(&cm2, &base_commit, head_target)?;
                let mut all_packs: Vec<(String, u64, String, u64)> =
                    prev_base_packs.iter().map(sized_to_tuple).collect();
                all_packs.extend(delta.iter().cloned());
                let elapsed = s.elapsed();
                info!(
                    "Head packs (delta vs base: {} new pack(s), {} total) {:?}",
                    delta.len(),
                    all_packs.len(),
                    elapsed
                );
                Ok(HeadBuild {
                    all_packs,
                    new_built: delta,
                    base_commit,
                    base_packs: prev_base_packs,
                    all_local: false,
                    elapsed_ms: duration_ms(elapsed),
                })
            }
            // Cold path: no base yet → pack the full closure as the base.
            _ => {
                let base = b.build_head_packs(&cm2, head_target)?;
                let base_packs = base.iter().map(tuple_to_sized).collect();
                let elapsed = s.elapsed();
                info!("Head packs (full base, {} packs) {:?}", base.len(), elapsed);
                Ok(HeadBuild {
                    all_packs: base.clone(),
                    new_built: base,
                    base_commit: cm2,
                    base_packs,
                    all_local: true,
                    elapsed_ms: duration_ms(elapsed),
                })
            }
        }
    });
    // Head builds only the cheap files table (no zstd frames): editable
    // depth=1 materializes the worktree from the HEAD-closure packs, so it does
    // not need the archive. The full zstd archive (for files mode) is built in
    // concurrently afterward, off the time-to-Head critical path.
    //
    // Files-table by-diff: when a prior sync exists, reuse its content hashes for
    // unchanged paths and read+hash only the blobs that changed since the prior
    // commit (O(changed) instead of O(worktree)). The no-op fast path in do_sync
    // guarantees commit != prev.commit here, so the diff is non-trivial. Falls
    // back to a full table when there is no prior table.
    let prev_files: Option<Vec<crate::clonepack::FileEntry>> = match prev.as_ref() {
        Some(result) if !result.commit.is_empty() => result
            .head
            .as_ref()
            .and_then(|head| load_metadata_files(cas, storage, &head.clonepack.metadata_chunk)),
        _ => None,
    };
    let prev_commit_for_diff = prev.as_ref().map(|p| p.commit.clone());
    // Carry the prior files table + commit so the bounded archive can hash only
    // changed files and reuse frames for the unchanged prefix/suffix.
    let (md3, cm3) = (mirror_dir.to_path_buf(), commit.to_string());
    let ft_start = Instant::now();
    let files_table_handle = match (prev_files, prev_commit_for_diff) {
        (Some(pf), Some(from)) if !from.is_empty() => {
            let (md, cm, frm) = (md3.clone(), cm3.clone(), from);
            tokio::task::spawn_blocking(move || {
                let s = Instant::now();
                // If the diff fails (e.g. prev.commit was pruned after a
                // force-push), fall back to a full rebuild rather than failing
                // the sync — reuse is purely an optimization.
                match crate::git::diff_name_set(&md, &frm, &cm) {
                    Ok(changed) => {
                        let r = ArchiveBuilder::new(&md)
                            .build_files_table_incremental(&cm, &pf, &changed);
                        info!(
                            "Head files table (incremental, {} changed) {:?}",
                            changed.len(),
                            s.elapsed()
                        );
                        r
                    }
                    Err(e) => {
                        warn!("files-table diff failed ({e:#}); full rebuild");
                        let r = ArchiveBuilder::new(&md).build_files_table(&cm);
                        info!("Head files table (full, diff fallback) {:?}", s.elapsed());
                        r
                    }
                }
            })
        }
        _ => tokio::task::spawn_blocking(move || {
            let s = Instant::now();
            let r = ArchiveBuilder::new(&md3).build_files_table(&cm3);
            info!("Head files table (full) {:?}", s.elapsed());
            r
        }),
    };
    let (shallow_skeleton_pack, shallow_skeleton_idx) = shallow_skeleton_handle
        .await
        .context("shallow skeleton")??;
    phases.skeleton_build_ms = Some(duration_ms(sk_start.elapsed()));
    let head_built = head_handle.await.context("head packs")??;
    phases.head_packs_ms = Some(head_built.elapsed_ms);
    let head_packs = head_built.all_packs.clone();
    let metadata_base = files_table_handle.await.context("files table")??;
    phases.files_table_ms = Some(duration_ms(ft_start.elapsed()));
    info!("Head result: packs+skeleton+files-table {:?}", t.elapsed());
    t = Instant::now();

    let (md4, c4, cm4, skp) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        commit.to_string(),
        shallow_skeleton_pack.clone(),
    );
    let idx_start = Instant::now();
    let shallow_prebuilt_index = tokio::task::spawn_blocking(move || {
        PackBuilder::new(&md4, &c4).build_prebuilt_index(&cm4, &skp)
    })
    .await
    .context("shallow prebuilt index")??;
    phases.prebuilt_index_ms = Some(duration_ms(idx_start.elapsed()));

    let mut shallow_meta = metadata_base.clone();
    shallow_meta.skeleton_pack = cas.get(&shallow_skeleton_pack)?;
    shallow_meta.skeleton_idx = cas.get(&shallow_skeleton_idx)?;
    shallow_meta.prebuilt_index = cas.get(&shallow_prebuilt_index)?;
    let shallow_meta_data = shallow_meta.encode_to_vec();
    let shallow_metadata_hash = cas.put(&shallow_meta_data)?;

    // Head has no archive frames. Files publishes its own archive result.
    let archive_chunks = archive_chunk_refs(&[], &metadata_base)?;
    let head_tagged: Vec<(&(String, u64, String, u64), bool)> =
        head_packs.iter().map(|p| (p, false)).collect();
    let (head_entries, head_idx_bundle_ref, head_idx_bundle_hash) =
        assemble_variant(cas, storage, &head_tagged)?;
    // Ship the head MIDX only on a cold full base (all pack bytes still local).
    // On a delta re-sync the base packs may be absent from local cache, so omit it — the
    // client builds its own MIDX from the per-pack idxs.
    let all_built = head_built.all_local;
    let (head_midx_ref, head_midx_hash) = if all_built {
        assemble_midx(cas, &head_packs)?
    } else {
        (None, String::new())
    };

    let shallow_manifest = make_manifest(
        commit,
        &parent,
        &archive_chunks,
        &shallow_metadata_hash,
        shallow_meta_data.len() as u64,
        head_entries,
        head_midx_ref,
        head_idx_bundle_ref,
    )?;
    let shallow_clonepack_hash = cas.put(&shallow_manifest.encode_to_vec())?;

    let mut info = RefInfo {
        commit: commit.to_string(),
        head: Some(crate::HeadResult {
            clonepack: crate::ClonepackArtifacts {
                manifest: shallow_clonepack_hash.clone(),
                metadata_chunk: shallow_metadata_hash.clone(),
                skeleton_pack: shallow_skeleton_pack.clone(),
                skeleton_idx: shallow_skeleton_idx.clone(),
                prebuilt_index: shallow_prebuilt_index.clone(),
                midx: head_midx_hash.clone(),
                idx_bundle: head_idx_bundle_hash.clone(),
                commit: commit.to_string(),
            },
            parent_commit: parent.clone(),
            packs: pack_artifacts_of(&head_packs),
            base_commit: head_built.base_commit.clone(),
            base_packs: head_built.base_packs.clone(),
        }),
        full: None,
        files: None,
    };

    // Upload Head artifacts (shallow skeleton/index/metadata, head idx-bundle
    // + midx, shallow manifest, and only the FRESHLY BUILT head packs+idx).
    // Reused bucket packs are already durable in storage from a prior sync.
    let mut head_uploads: Vec<String> = vec![
        shallow_skeleton_pack.clone(),
        shallow_skeleton_idx.clone(),
        shallow_prebuilt_index.clone(),
        shallow_metadata_hash.clone(),
        shallow_clonepack_hash.clone(),
        head_idx_bundle_hash.clone(),
        head_midx_hash.clone(),
    ];
    for (p, _, i, _) in &head_built.new_built {
        head_uploads.push(p.clone());
        head_uploads.push(i.clone());
    }
    head_uploads.retain(|h| !h.is_empty());
    let head_idx_keep: std::collections::HashSet<String> =
        head_packs.iter().map(|(_, _, ih, _)| ih.clone()).collect();
    let upload_start = Instant::now();
    test_hook(TestStage::BuildCrash {
        stage: "before_upload",
        commit,
    })
    .await?;
    upload_artifacts(
        cas,
        storage,
        head_uploads.clone(),
        upload_conc,
        Some(commit),
    )
    .await?;
    test_hook(TestStage::BuildCrash {
        stage: "after_upload",
        commit,
    })
    .await?;
    settle_storage(cas, storage, head_uploads, head_idx_keep).await;
    phases.upload_head_ms = Some(duration_ms(upload_start.elapsed()));

    let publish_start = Instant::now();
    test_hook(TestStage::BuildCrash {
        stage: "before_ready_publication",
        commit,
    })
    .await?;
    let _ = test_hook(TestStage::RefStoreWrite).await;
    let head_result = info
        .head
        .clone()
        .context("Head result missing before publication")?;
    anyhow::ensure!(
        ref_store
            .publish_head(repo_id, commit, head_result.clone())
            .await
            .with_context(|| format!(
                "persist Head result for {}@{commit}",
                repo_id.storage_key()
            ))?,
        "job no longer owns Head publication for {}@{commit}",
        repo_id.storage_key()
    );
    let _ = test_hook(TestStage::HeadPublished).await;
    phases.ref_publish_ms = Some(duration_ms(publish_start.elapsed()));
    info!(
        "published Head for {} in {:?}",
        &commit[..7.min(commit.len())],
        t_total.elapsed()
    );
    phases.publish_head_ms = Some(duration_ms(t_total.elapsed()));
    let _ = t; // Head assemble/upload time is folded into the total above.
    if let Some(release) = foreground_release.take() {
        let _ = release.send(());
    }
    test_hook(TestStage::AfterHeadBarrier(commit)).await?;

    // Every durable claim owns Head, Files, and Full. A process death before
    // this completes leaves the SQL claim stale for the next worker to reclaim.
    let need_full = current
        .as_ref()
        .is_none_or(|result| !exact_result_ready(result, ExactResultKind::Full, commit));
    let need_files = current
        .as_ref()
        .is_none_or(|result| !exact_result_ready(result, ExactResultKind::Files, commit));
    build_missing_full_and_files(
        cas,
        mirror_dir,
        repo_id,
        commit,
        parent,
        head_result,
        ref_store,
        storage,
        compression_level,
        need_full,
        need_files,
        true,
    )
    .await?;
    ref_store.invalidate(repo_id, commit).await;
    if let Some(updated) = ref_store.load_result(repo_id, commit).await?
        && updated.commit == commit
    {
        info = updated;
    }

    report_sync_phases(repo_id, commit, &phases);
    if std::env::var_os("RIPCLONE_BENCH").is_some() {
        report_sync_bench(repo_id, commit, &phases, storage, &info, mirror_dir);
    }

    Ok(SyncBuildResult {
        info,
        status: "built".to_string(),
        phases,
    })
}

/// Storage amplification for one ref: durable bytes in object storage divided by
/// the upstream bare-mirror size, split by artifact class. Content-addressed
/// storage may be shared across refs, so this attributes every hash reachable
/// from the given ref to that ref.
#[derive(Debug, Clone, serde::Serialize)]
struct StorageAmplification {
    repo_size_bytes: u64,
    head_pack_bytes: u64,
    history_pack_bytes: u64,
    archive_chunk_bytes: u64,
    metadata_bytes: u64,
    total_storage_bytes: u64,
    amplification: f64,
}

/// Recursively sum file sizes under `dir`.
fn dir_size(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    total += dir_size(&path);
                }
            }
        }
    }
    total
}

/// Classify every hash reachable from `info` by artifact class and sum its
/// bytes in `storage`.
fn measure_storage_amplification(
    storage: &crate::storage::StorageRef,
    info: &crate::RefInfo,
    mirror_dir: &std::path::Path,
) -> Option<StorageAmplification> {
    let repo_size = dir_size(mirror_dir);
    let mut head_pack_bytes = 0u64;
    let mut history_pack_bytes = 0u64;
    let mut archive_chunk_bytes = 0u64;
    let mut metadata_bytes = 0u64;

    let add = |hash: &str, bucket: &mut u64| {
        if hash.is_empty() {
            return;
        }
        if let Ok(size) = storage.size(hash) {
            *bucket += size;
        }
    };

    // Head closure packs (base + delta) and their idx files.
    if let Some(head) = &info.head {
        for pack in &head.packs {
            add(&pack.pack, &mut head_pack_bytes);
            add(&pack.idx, &mut head_pack_bytes);
        }
    }

    // Full-history / LSM sealed levels.
    if let Some(full) = &info.full {
        for level in &full.history_levels {
            for pack in &level.packs {
                add(&pack.pack, &mut history_pack_bytes);
                add(&pack.idx, &mut history_pack_bytes);
            }
        }
    }

    // Archive chunks referenced directly from the ref.
    if let Some(files) = &info.files {
        for hash in &files.archive_chunks {
            add(hash, &mut archive_chunk_bytes);
        }
    }

    // Metadata: manifests, metadata chunks, skeleton/index, prebuilt index,
    // idx bundle, and MIDX.
    for artifacts in [
        info.head.as_ref().map(|result| &result.clonepack),
        info.full.as_ref().map(|result| &result.clonepack),
        info.files.as_ref().map(|result| &result.clonepack),
    ]
    .into_iter()
    .flatten()
    {
        for hash in [
            &artifacts.manifest,
            &artifacts.metadata_chunk,
            &artifacts.skeleton_pack,
            &artifacts.skeleton_idx,
            &artifacts.prebuilt_index,
            &artifacts.idx_bundle,
            &artifacts.midx,
        ] {
            add(hash, &mut metadata_bytes);
        }
    }

    let total_storage_bytes = head_pack_bytes
        .saturating_add(history_pack_bytes)
        .saturating_add(archive_chunk_bytes)
        .saturating_add(metadata_bytes);
    let amplification = if repo_size == 0 {
        0.0
    } else {
        total_storage_bytes as f64 / repo_size as f64
    };

    Some(StorageAmplification {
        repo_size_bytes: repo_size,
        head_pack_bytes,
        history_pack_bytes,
        archive_chunk_bytes,
        metadata_bytes,
        total_storage_bytes,
        amplification,
    })
}

fn report_sync_phases(repo_id: &RepoId, commit: &str, phases: &SyncPhases) {
    let report = serde_json::json!({
        "kind": "sync-phases",
        "repo": repo_id.storage_key(),
        "commit": &commit[..7.min(commit.len())],
        "phases": phases,
    });
    info!("{}", report.to_string());
}

/// Print a JSON benchmark report when `RIPCLONE_BENCH` is set. Mirrors the
/// client-side `--bench` report style: one structured object per sync, emitted
/// at INFO so it can be scraped from logs.
fn report_sync_bench(
    repo_id: &RepoId,
    commit: &str,
    phases: &SyncPhases,
    storage: &crate::storage::StorageRef,
    info: &crate::RefInfo,
    mirror_dir: &std::path::Path,
) {
    let amplification = measure_storage_amplification(storage, info, mirror_dir);
    let report = serde_json::json!({
        "kind": "sync-bench",
        "repo": repo_id.storage_key(),
        "commit": &commit[..7.min(commit.len())],
        "phases": phases,
        "storage_amplification": amplification,
    });
    info!("{}", report.to_string());
}

/// Build every missing result after Head and let Full and Files publish as soon
/// as their own artifact work completes.
#[allow(clippy::too_many_arguments)]
async fn build_missing_full_and_files(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    commit: &str,
    parent: Option<String>,
    head: crate::HeadResult,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    compression_level: i32,
    need_full: bool,
    need_files: bool,
    allow_head_compaction: bool,
) -> Result<()> {
    let head_manifest_bytes = storage
        .get(&head.clonepack.manifest)
        .or_else(|_| cas.get(&head.clonepack.manifest))
        .context("read Head manifest for remaining results")?;
    let head_manifest = ClonepackManifest::decode(head_manifest_bytes.as_slice())
        .context("decode Head manifest for remaining results")?;
    anyhow::ensure!(
        head_manifest.commit == commit && head.clonepack.commit == commit,
        "Head result identity mismatch for {}@{commit}",
        repo_id.storage_key()
    );
    let head_packs = manifest_pack_tuples(&head_manifest)?;
    let parent_result = match parent.as_deref() {
        Some(parent_commit) => ref_store
            .load_result(repo_id, parent_commit)
            .await?
            .filter(|result| result.commit == parent_commit),
        None => None,
    };

    let full = async {
        if !need_full {
            return Ok(());
        }
        build_full_result(
            cas,
            mirror_dir,
            repo_id,
            commit,
            parent.clone(),
            &head,
            head_packs,
            parent_result
                .as_ref()
                .and_then(|result| {
                    if exact_result_ready(result, ExactResultKind::Full, &result.commit) {
                        result.full.as_ref()
                    } else {
                        None
                    }
                })
                .map(|full| full.history_levels.clone())
                .unwrap_or_default(),
            ref_store,
            storage,
            allow_head_compaction,
        )
        .await
    };
    let files_parent = parent.clone();
    let files = async {
        if !need_files {
            return Ok(());
        }
        build_files_result(
            cas,
            mirror_dir,
            repo_id,
            commit,
            files_parent,
            &head,
            parent_result.as_ref(),
            ref_store,
            storage,
            compression_level,
        )
        .await
    };

    let (full_result, files_result) = tokio::join!(full, files);
    match (full_result, files_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(full), Ok(())) => Err(full.context("Full build failed")),
        (Ok(()), Err(files)) => Err(files.context("Files build failed")),
        (Err(full), Err(files)) => Err(anyhow::anyhow!(
            "Full build failed: {full:#}; Files build failed: {files:#}"
        )),
    }
}

fn manifest_pack_tuples(manifest: &ClonepackManifest) -> Result<Vec<(String, u64, String, u64)>> {
    manifest
        .packs
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let pack = entry
                .pack
                .as_ref()
                .with_context(|| format!("Head manifest pack {index} missing pack"))?;
            let idx = entry
                .idx
                .as_ref()
                .with_context(|| format!("Head manifest pack {index} missing idx"))?;
            Ok((
                hash_to_hex(&pack.hash),
                pack.len,
                hash_to_hex(&idx.hash),
                idx.len,
            ))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn build_full_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    commit: &str,
    parent: Option<String>,
    head: &crate::HeadResult,
    head_packs: Vec<(String, u64, String, u64)>,
    previous_levels: Vec<crate::HistoryLevel>,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    allow_head_compaction: bool,
) -> Result<()> {
    if test_hook(TestStage::FullBuild(commit)).await? {
        anyhow::bail!("forced Full failure for {commit}");
    }

    // Full history walks are substantially faster on large repositories when
    // Git can use a reachability bitmap. This is best-effort and happens only
    // when Full itself is missing.
    let _ = test_hook(TestStage::BitmapWrite).await;
    let bitmap_mirror = mirror_dir.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || git::write_bitmap(&bitmap_mirror)).await;

    let history_target = 512 * 1024 * 1024;
    let lsm_cfg = lsm_config();
    let sealed_tip = lsm_cfg
        .enabled
        .then(|| previous_levels.last().map(|level| level.tip_commit.clone()))
        .flatten();
    let history_mirror = mirror_dir.to_path_buf();
    let history_cas = cas.clone();
    let history_commit = commit.to_string();
    let history_sealed_tip = sealed_tip.clone();
    let lsm_enabled = lsm_cfg.enabled;
    let (built_history, is_tail) = tokio::task::spawn_blocking(move || {
        let builder = PackBuilder::new(&history_mirror, &history_cas);
        if lsm_enabled {
            Ok::<_, anyhow::Error>((
                builder.build_history_tail(
                    &history_commit,
                    history_sealed_tip.as_deref(),
                    history_target,
                )?,
                true,
            ))
        } else {
            Ok((
                builder.build_history_packs(&history_commit, history_target)?,
                false,
            ))
        }
    })
    .await
    .context("Full history task")??;

    let (history_packs, new_history, levels) = if is_tail {
        seal_and_compact(
            mirror_dir,
            cas,
            commit,
            previous_levels,
            sealed_tip,
            built_history,
            history_target,
            &lsm_cfg,
        )
        .await?
    } else {
        (built_history.clone(), built_history, Vec::new())
    };
    let mut head = head.clone();
    let mut head_packs = head_packs;
    let rebase_bytes = env_u64("RIPCLONE_HEAD_REBASE_BYTES", 128 * 1024 * 1024);
    let delta_bytes: u64 = head_packs
        .iter()
        .skip(head.base_packs.len())
        .map(|(_, pack_len, _, _)| *pack_len)
        .sum();
    if allow_head_compaction && delta_bytes >= rebase_bytes {
        let compact_mirror = mirror_dir.to_path_buf();
        let compact_cas = cas.clone();
        let compact_commit = commit.to_string();
        let compact_packs = tokio::task::spawn_blocking(move || {
            PackBuilder::new(&compact_mirror, &compact_cas)
                .build_head_packs(&compact_commit, 4 * 1024 * 1024)
        })
        .await
        .context("compact Head packs")??;
        let compact_tagged: Vec<(&(String, u64, String, u64), bool)> =
            compact_packs.iter().map(|pack| (pack, false)).collect();
        let (compact_entries, compact_idx_ref, compact_idx_hash) =
            assemble_variant(cas, storage, &compact_tagged)?;
        let (compact_midx_ref, compact_midx_hash) = assemble_midx(cas, &compact_packs)?;
        let compact_manifest = make_manifest(
            commit,
            &parent,
            &[],
            &head.clonepack.metadata_chunk,
            storage
                .size(&head.clonepack.metadata_chunk)
                .context("size Head metadata for compaction")?,
            compact_entries,
            compact_midx_ref,
            compact_idx_ref,
        )?;
        let compact_manifest_hash = cas.put(&compact_manifest.encode_to_vec())?;
        let mut compact_uploads = vec![
            compact_manifest_hash.clone(),
            compact_idx_hash.clone(),
            compact_midx_hash.clone(),
        ];
        for (pack, _, idx, _) in &compact_packs {
            compact_uploads.push(pack.clone());
            compact_uploads.push(idx.clone());
        }
        compact_uploads.retain(|hash| !hash.is_empty());
        let compact_idx_keep = compact_packs
            .iter()
            .map(|(_, _, idx, _)| idx.clone())
            .collect();
        upload_artifacts(
            cas,
            storage,
            compact_uploads.clone(),
            upload_concurrency(),
            None,
        )
        .await?;
        let compact_head = crate::HeadResult {
            clonepack: crate::ClonepackArtifacts {
                manifest: compact_manifest_hash,
                metadata_chunk: head.clonepack.metadata_chunk.clone(),
                skeleton_pack: head.clonepack.skeleton_pack.clone(),
                skeleton_idx: head.clonepack.skeleton_idx.clone(),
                prebuilt_index: head.clonepack.prebuilt_index.clone(),
                midx: compact_midx_hash,
                idx_bundle: compact_idx_hash,
                commit: commit.to_string(),
            },
            parent_commit: parent.clone(),
            packs: pack_artifacts_of(&compact_packs),
            base_commit: commit.to_string(),
            base_packs: compact_packs.iter().map(tuple_to_sized).collect(),
        };
        let _ = test_hook(TestStage::RefStoreWrite).await;
        anyhow::ensure!(
            ref_store
                .publish_head(repo_id, commit, compact_head.clone())
                .await?,
            "job no longer owns compacted Head publication for {}@{commit}",
            repo_id.storage_key()
        );
        settle_storage(cas, storage, compact_uploads, compact_idx_keep).await;
        info!(
            "compacted Head result ({} MiB delta -> {} packs)",
            delta_bytes / (1024 * 1024),
            compact_packs.len()
        );
        head = compact_head;
        head_packs = compact_packs;
    }

    let tagged: Vec<(&(String, u64, String, u64), bool)> = head_packs
        .iter()
        .map(|pack| (pack, false))
        .chain(history_packs.iter().map(|pack| (pack, true)))
        .collect();
    let (entries, idx_bundle_ref, idx_bundle_hash) = assemble_variant(cas, storage, &tagged)?;
    let manifest = make_manifest(
        commit,
        &parent,
        &[],
        &head.clonepack.metadata_chunk,
        storage
            .size(&head.clonepack.metadata_chunk)
            .context("size Head metadata for Full")?,
        entries,
        None,
        idx_bundle_ref,
    )?;
    let manifest_hash = cas.put(&manifest.encode_to_vec())?;
    let mut uploads = vec![manifest_hash.clone(), idx_bundle_hash.clone()];
    for (pack, _, idx, _) in &new_history {
        uploads.push(pack.clone());
        uploads.push(idx.clone());
    }
    uploads.retain(|hash| !hash.is_empty());
    let keep_idx = new_history
        .iter()
        .map(|(_, _, idx, _)| idx.clone())
        .collect();
    upload_artifacts(cas, storage, uploads.clone(), upload_concurrency(), None).await?;

    let mut all_packs = head_packs;
    all_packs.extend(history_packs);
    let full = crate::FullResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: manifest_hash,
            metadata_chunk: head.clonepack.metadata_chunk.clone(),
            skeleton_pack: head.clonepack.skeleton_pack.clone(),
            skeleton_idx: head.clonepack.skeleton_idx.clone(),
            prebuilt_index: head.clonepack.prebuilt_index.clone(),
            midx: String::new(),
            idx_bundle: idx_bundle_hash,
            commit: commit.to_string(),
        },
        packs: pack_artifacts_of(&all_packs),
        history_levels: levels,
    };
    let _ = test_hook(TestStage::BeforeFullPublish).await;
    let _ = test_hook(TestStage::RefStoreWrite).await;
    anyhow::ensure!(
        ref_store.publish_full(repo_id, commit, full).await?,
        "job no longer owns Full publication for {}@{commit}",
        repo_id.storage_key()
    );
    settle_storage(cas, storage, uploads, keep_idx).await;
    let _ = test_hook(TestStage::FullPublished(commit)).await;
    let _ = test_hook(TestStage::AfterFullPublish).await;
    info!(
        "published Full result for {}@{}",
        repo_id.storage_key(),
        &commit[..7.min(commit.len())]
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn build_files_result(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    commit: &str,
    parent: Option<String>,
    head: &crate::HeadResult,
    parent_result: Option<&RefInfo>,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    compression_level: i32,
) -> Result<()> {
    if test_hook(TestStage::FilesBuild(commit)).await? {
        anyhow::bail!("forced Files failure for {commit}");
    }

    let parent_files = parent_result.and_then(|result| {
        if exact_result_ready(result, ExactResultKind::Files, &result.commit) {
            result.files.as_ref()
        } else {
            None
        }
    });
    let previous_frames = parent_files
        .map(|files| files.archive_frames.clone())
        .unwrap_or_default();
    let previous_files = match parent_files {
        Some(files) => {
            load_metadata_files(cas, storage, &files.clonepack.metadata_chunk).unwrap_or_default()
        }
        None => Vec::new(),
    };
    let previous_commit = parent_result.map(|result| result.commit.clone());
    let bounded = std::env::var("RIPCLONE_ARCHIVE_BOUNDED")
        .map(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
        && !previous_frames.is_empty()
        && previous_commit.is_some();
    let previous_frame_map: std::collections::HashMap<String, (String, u64)> = previous_frames
        .iter()
        .map(|frame| {
            (
                frame.raw_hash.clone(),
                (frame.chunk_hash.clone(), frame.compressed_len),
            )
        })
        .collect();
    let archive_mirror = mirror_dir.to_path_buf();
    let archive_cas = cas.clone();
    let archive_storage = storage.clone();
    let archive_commit = commit.to_string();
    let archive_output = tokio::task::spawn_blocking(move || {
        let builder = ArchiveBuilder::new(&archive_mirror);
        if bounded {
            builder.build_into_cas_bounded(
                &archive_commit,
                &archive_cas,
                Some(&archive_storage),
                compression_level,
                None,
                &previous_frames,
                &previous_files,
                previous_commit.as_deref().unwrap_or_default(),
                crate::archive::DEFAULT_ARCHIVE_CHUNK_SIZE,
            )
        } else {
            builder.build_into_cas_incremental(
                &archive_commit,
                &archive_cas,
                Some(&archive_storage),
                compression_level,
                None,
                &previous_frame_map,
                crate::archive::DEFAULT_ARCHIVE_CHUNK_SIZE,
            )
        }
    })
    .await
    .context("Files archive task")??;

    let archive_chunk_hashes = archive_output.download_bundle_hashes;
    let mut metadata = archive_output.metadata;
    let fetch = |hash: &str| cas.get(hash).or_else(|_| storage.get(hash));
    metadata.skeleton_pack = fetch(&head.clonepack.skeleton_pack)?;
    metadata.skeleton_idx = fetch(&head.clonepack.skeleton_idx)?;
    metadata.prebuilt_index = fetch(&head.clonepack.prebuilt_index)?;
    let metadata_bytes = metadata.encode_to_vec();
    let metadata_hash = cas.put(&metadata_bytes)?;
    let archive_chunks = archive_chunk_refs(&archive_chunk_hashes, &metadata)?;
    let manifest = make_manifest(
        commit,
        &parent,
        &archive_chunks,
        &metadata_hash,
        metadata_bytes.len() as u64,
        Vec::new(),
        None,
        None,
    )?;
    let manifest_hash = cas.put(&manifest.encode_to_vec())?;
    let uploads = archive_publish_upload_hashes(
        &metadata_hash,
        &manifest_hash,
        &archive_chunk_hashes,
        &archive_output.new_reuse_frame_hashes,
    );
    upload_artifacts(cas, storage, uploads.clone(), upload_concurrency(), None).await?;

    let files = crate::FilesResult {
        clonepack: crate::ClonepackArtifacts {
            manifest: manifest_hash,
            metadata_chunk: metadata_hash,
            skeleton_pack: head.clonepack.skeleton_pack.clone(),
            skeleton_idx: head.clonepack.skeleton_idx.clone(),
            prebuilt_index: head.clonepack.prebuilt_index.clone(),
            midx: String::new(),
            idx_bundle: String::new(),
            commit: commit.to_string(),
        },
        archive_chunks: archive_chunk_hashes,
        archive_frames: archive_output.archive_frames,
    };
    let _ = test_hook(TestStage::BeforeFilesPublish).await;
    let _ = test_hook(TestStage::RefStoreWrite).await;
    anyhow::ensure!(
        ref_store.publish_files(repo_id, commit, files).await?,
        "job no longer owns Files publication for {}@{commit}",
        repo_id.storage_key()
    );
    settle_storage(cas, storage, uploads, std::collections::HashSet::new()).await;
    let _ = test_hook(TestStage::FilesPublished).await;
    let _ = test_hook(TestStage::AfterFilesPublish).await;
    info!(
        "published Files result for {}@{}",
        repo_id.storage_key(),
        &commit[..7.min(commit.len())]
    );
    Ok(())
}
struct ClaimScopedRefStore {
    inner: Arc<dyn RefStore>,
    control: Option<Arc<crate::control::ControlDb>>,
    job_id: crate::queue::JobId,
    worker_id: String,
}

#[async_trait::async_trait]
impl RefStore for ClaimScopedRefStore {
    async fn load_result(&self, repo_id: &RepoId, commit: &str) -> Result<Option<RefInfo>> {
        self.inner.load_result(repo_id, commit).await
    }

    async fn save_result(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()> {
        self.inner.save_result(repo_id, info).await
    }

    async fn publish_head(
        &self,
        repo_id: &RepoId,
        commit: &str,
        head: crate::HeadResult,
    ) -> Result<bool> {
        let mut result = self
            .load_result(repo_id, commit)
            .await?
            .context("exact result missing before Head publication")?;
        result.head = Some(head.clone());
        self.inner
            .before_claimed_result_write(repo_id, &result)
            .await?;
        let updated = match &self.control {
            Some(control) => {
                control
                    .publish_head_for_claim(self.job_id, &self.worker_id, repo_id, commit, head)
                    .await?
            }
            None => {
                self.inner
                    .publish_claimed_head(repo_id, commit, head, self.job_id, &self.worker_id)
                    .await?
            }
        };
        anyhow::ensure!(updated, "worker no longer owns job {}", self.job_id);
        self.inner
            .after_claimed_result_write(repo_id, &result)
            .await?;
        Ok(true)
    }

    async fn publish_full(
        &self,
        repo_id: &RepoId,
        commit: &str,
        full: crate::FullResult,
    ) -> Result<bool> {
        let mut result = self
            .load_result(repo_id, commit)
            .await?
            .context("exact result missing before Full publication")?;
        result.full = Some(full.clone());
        self.inner
            .before_claimed_result_write(repo_id, &result)
            .await?;
        let updated = match &self.control {
            Some(control) => {
                control
                    .publish_full_for_claim(self.job_id, &self.worker_id, repo_id, commit, full)
                    .await?
            }
            None => {
                self.inner
                    .publish_claimed_full(repo_id, commit, full, self.job_id, &self.worker_id)
                    .await?
            }
        };
        anyhow::ensure!(updated, "worker no longer owns job {}", self.job_id);
        self.inner
            .after_claimed_result_write(repo_id, &result)
            .await?;
        Ok(true)
    }

    async fn publish_files(
        &self,
        repo_id: &RepoId,
        commit: &str,
        files: crate::FilesResult,
    ) -> Result<bool> {
        let mut result = self
            .load_result(repo_id, commit)
            .await?
            .context("exact result missing before Files publication")?;
        result.files = Some(files.clone());
        self.inner
            .before_claimed_result_write(repo_id, &result)
            .await?;
        let updated = match &self.control {
            Some(control) => {
                control
                    .publish_files_for_claim(self.job_id, &self.worker_id, repo_id, commit, files)
                    .await?
            }
            None => {
                self.inner
                    .publish_claimed_files(repo_id, commit, files, self.job_id, &self.worker_id)
                    .await?
            }
        };
        anyhow::ensure!(updated, "worker no longer owns job {}", self.job_id);
        self.inner
            .after_claimed_result_write(repo_id, &result)
            .await?;
        Ok(true)
    }

    async fn list_commits(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        self.inner.list_commits(repo_id).await
    }

    async fn add_repo(&self, repo: &AddedRepo) -> Result<()> {
        self.inner.add_repo(repo).await
    }

    async fn load_added_repo(&self, repo_id: &RepoId) -> Result<Option<AddedRepo>> {
        self.inner.load_added_repo(repo_id).await
    }

    async fn remove_added_repo(&self, repo_id: &RepoId) -> Result<()> {
        self.inner.remove_added_repo(repo_id).await
    }

    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>> {
        self.inner.list_added_repos().await
    }

    async fn invalidate(&self, repo_id: &RepoId, commit: &str) {
        self.inner.invalidate(repo_id, commit).await;
    }

    async fn health(&self) -> Result<()> {
        self.inner.health().await
    }
}

/// Run one durable exact-commit job to completion.
pub async fn process_build_job(
    state: &ServerState,
    job: &BuildJob,
    job_id: crate::queue::JobId,
    worker_id: &str,
) -> Result<SyncBuildResult, BuildError> {
    process_build_job_with_foreground_release(state, job, job_id, worker_id, None).await
}

async fn process_build_job_with_foreground_release(
    state: &ServerState,
    job: &BuildJob,
    job_id: crate::queue::JobId,
    worker_id: &str,
    foreground_release: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<SyncBuildResult, BuildError> {
    let ref_store: Arc<dyn RefStore> = Arc::new(ClaimScopedRefStore {
        inner: Arc::clone(&state.ref_store),
        control: state.control_db.clone(),
        job_id,
        worker_id: worker_id.to_string(),
    });
    let repo_id = &job.repo_id;
    let commit = &job.admitted_commit;
    if let Err(e) = crate::validation::validate_object_id(&job.admitted_commit) {
        return Err(BuildError::permanent(format!(
            "build job has invalid admitted commit: {e}"
        )));
    }
    if let Err(error) = job.repo_config.validate() {
        return Err(BuildError::permanent(format!(
            "build job has invalid repository config: {error:#}"
        )));
    }
    let start = std::time::Instant::now();
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let provider = match state.provider_registry.get(repo_id.provider.as_str()) {
        Some(p) => p.clone(),
        None => {
            let message = format!("unknown provider {}", repo_id.provider.as_str());
            warn!(
                "unknown provider {} for build job",
                repo_id.provider.as_str()
            );
            return Err(BuildError::permanent(message));
        }
    };
    // do_sync holds this per-repo lock only across the mirror-mutating prep and
    // releases it before the heavy read-only build, so distinct repos build
    // concurrently across the worker pool.
    // do_sync takes the per-repo lock itself (via `&lock`) and releases it before
    // the heavy build, so we don't hold a guard here.
    let lock = repo_lock(&state.sync_locks, repo_id).await;
    // Every accepted job owns Head, Files, and Full until terminal settlement.
    // No phase is detached from the durable claim.
    let result = do_sync(
        &state.cas,
        &mirror_dir,
        repo_id,
        &job.admitted_commit,
        &ref_store,
        &state.storage,
        &provider,
        job.credential.as_ref(),
        &job.repo_config,
        &lock,
        foreground_release,
    )
    .await;

    match &result {
        Ok(result) => {
            state.metrics.record_build_completed(start.elapsed());
            state.metrics.record_sync_phases((&result.phases).into());
            info!(
                "background build completed for {}@{}",
                repo_id.storage_key(),
                commit
            );
            Ok(result.clone())
        }
        Err(e) => {
            let classified = classify_build_error(e);
            if !classified.is_retryable() {
                state.metrics.record_build_failed();
                warn!(
                    "background build failed for {}@{commit}: {e}",
                    repo_id.storage_key()
                );
            } else {
                warn!(
                    "background build transient failure for {}@{commit} \
                     (queue will requeue if under attempt cap): {e}",
                    repo_id.storage_key()
                );
            }
            // The deterministic hook observes that artifact work stopped. The
            // embedded worker acknowledges the job immediately after this
            // function returns; tests that require the durable Failed state
            // must observe the queue as well.
            let _ = test_hook(TestStage::BuildFailure {
                commit: Some(&job.admitted_commit),
                message: classified.message(),
            })
            .await;
            Err(classified)
        }
    }
}

fn classify_build_error(error: &anyhow::Error) -> BuildError {
    for cause in error.chain() {
        if let Some(s3_error) = cause.downcast_ref::<s3::Error>() {
            let message = format!("{error:#}");
            return if s3_error.is_retryable() {
                BuildError::retryable(message)
            } else {
                BuildError::permanent(message)
            };
        }
        // ApiRefStore report failures (network / 5xx / 401). Must not be
        // swallowed: a silent success would drop the build result.
        if let Some(api_err) = cause.downcast_ref::<crate::api_ref_store::ApiReportError>() {
            let message = format!("{error:#}");
            return if api_err.is_retryable() {
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
        if cause.is::<tokio::time::error::Elapsed>() {
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

/// Concurrency cap for in-process builds. Builds are CPU-heavy (history
/// deltification + zstd), so the default is deliberately small; raise it on a big
/// box via `RIPCLONE_BUILD_CONCURRENCY`. Different repos build in parallel;
/// same-repo builds still serialize on the per-repo mirror lock.
fn build_concurrency() -> usize {
    std::env::var("RIPCLONE_BUILD_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(2)
}

/// Process-global cap on concurrent upstream fetches/clones. Separate from — and
/// usually a touch larger than — the build cap: a fetch is network/upstream
/// bound, a build is CPU bound, so they throttle independently.
fn fetch_semaphore() -> &'static tokio::sync::Semaphore {
    static SEM: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    SEM.get_or_init(|| tokio::sync::Semaphore::new(4))
}

/// Run embedded workers against the same durable jobs table used by admission.
/// A slot starts one Head at a time. After Head publishes, the claimed job keeps
/// running and heartbeating in its own task while the slot starts another Head.
fn spawn_durable_build_worker(state: ServerState, queue: Arc<crate::queue::SqlJobQueue>) {
    let admission_notify = state
        .control_db
        .as_ref()
        .expect("embedded workers require the server control database")
        .admission_notifier();
    for slot in 0..build_concurrency() {
        let state = state.clone();
        let queue = queue.clone();
        let admission_notify = admission_notify.clone();
        tokio::spawn(async move {
            let mut owner_sequence = 0u64;
            loop {
                owner_sequence = owner_sequence.wrapping_add(1);
                let worker_id = format!("embedded-{}-{slot}-{owner_sequence}", std::process::id());
                let claimed = loop {
                    // Register the notification before reading SQLite. If an
                    // admission commits during the claim attempt, the stored
                    // permit wins the select below instead of being lost.
                    let notified = admission_notify.notified();
                    tokio::pin!(notified);
                    let _ = test_hook(TestStage::BeforeClaim).await;
                    match queue.claim(&worker_id).await {
                        Ok(Some(claimed)) => break claimed,
                        Ok(None) => {
                            let _ = test_hook(TestStage::EmbeddedIdleWait).await;
                            tokio::select! {
                                () = &mut notified => {
                                    let _ = test_hook(TestStage::EmbeddedWake { fallback: false }).await;
                                }
                                () = tokio::time::sleep(Duration::from_millis(250)) => {
                                    let _ = test_hook(TestStage::EmbeddedWake { fallback: true }).await;
                                }
                            }
                        }
                        Err(error) => {
                            error!("embedded worker claim failed: {error:#}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                };
                let _ = test_hook(TestStage::AfterClaim).await;
                let (foreground_release, foreground_released) = tokio::sync::oneshot::channel();
                let worker_state = state.clone();
                let worker_queue = queue.clone();
                let owner = worker_id.clone();
                tokio::spawn(async move {
                    let repo_id = claimed.repo_id();
                    let admitted_commit = claimed.admitted_commit.clone();
                    let mut foreground_release = Some(foreground_release);
                    let build = match crate::validation::validate_object_id(&admitted_commit) {
                        Err(error) => Err(BuildError::permanent(format!(
                            "queued job has invalid admitted commit: {error}"
                        ))),
                        Ok(()) => match worker_state
                            .broker
                            .fetch_credential(&repo_id, claimed.credential.as_ref())
                        {
                            Err(error) => Err(BuildError::permanent(format!(
                                "fetch credential for queued job {}: {error:#}",
                                repo_id.storage_key()
                            ))),
                            Ok(credential) => {
                                let job = BuildJob {
                                    repo_id: repo_id.clone(),
                                    admitted_commit: admitted_commit.clone(),
                                    repo_config: claimed.repo_config,
                                    credential,
                                    size_bytes: None,
                                };
                                let state = worker_state.clone();
                                let release_for_build = foreground_release.take();
                                let build_owner = owner.clone();
                                Ok(tokio::spawn(async move {
                                    process_build_job_with_foreground_release(
                                        &state,
                                        &job,
                                        claimed.id,
                                        &build_owner,
                                        release_for_build,
                                    )
                                    .await
                                }))
                            }
                        },
                    };
                    let result = match build {
                        Err(error) => Err(error),
                        Ok(mut build) => {
                            let heartbeat_interval = Duration::from_secs(
                                (worker_queue
                                    .heartbeat_timeout_secs()
                                    .min(worker_queue.stale_claim_secs().max(1))
                                    / 3)
                                .max(1)
                                .unsigned_abs(),
                            );
                            let mut heartbeat = tokio::time::interval(heartbeat_interval);
                            heartbeat
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                            loop {
                                tokio::select! {
                                    joined = &mut build => {
                                        break match joined {
                                            Ok(result) => result,
                                            Err(error) => Err(BuildError::retryable(format!(
                                                "build task panicked: {error}"
                                            ))),
                                        };
                                    }
                                    _ = heartbeat.tick() => {
                                        if let Err(error) = worker_queue
                                            .heartbeat(&owner, Some(claimed.id))
                                            .await
                                        {
                                            error!(
                                                "embedded worker lost claim for job {}: {error:#}",
                                                claimed.id
                                            );
                                            let _ = test_hook(TestStage::ClaimLost).await;
                                            build.abort();
                                            let _ = build.await;
                                            break Err(BuildError::retryable(format!(
                                                "durable claim lost while building: {error:#}"
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    };
                    // An error before Head publication drops the sender here,
                    // releasing the foreground slot before terminal ack.
                    drop(foreground_release);
                    match worker_queue
                        .ack(claimed.id, &owner, result.map(|_| ()))
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => warn!(
                            "embedded job {} lost its claim before acknowledgement",
                            claimed.id
                        ),
                        Err(error) => error!(
                            "embedded worker failed to acknowledge job {}: {error:#}",
                            claimed.id
                        ),
                    }
                    if let Err(error) = worker_queue.remove_worker(&owner).await {
                        error!("embedded worker registry cleanup failed: {error:#}");
                    }
                });
                // The sender fires after Head is durably published. If the
                // build fails earlier, sender drop also releases this slot.
                let _ = foreground_released.await;
            }
        });
    }
}

/// One polling pass: for every added repository, cheaply resolve HEAD once
/// (`ls-remote`, under the fetch cap) and trigger a build if that commit isn't
/// already built. Catches pushes that arrived without a webhook/Actions trigger,
/// so build-before-clone still holds. Best-effort: per-repo errors are logged and
/// skipped. Returns the number of builds triggered. Exposed for tests.
pub async fn poll_once(state: &ServerState) -> usize {
    let repos = match state.ref_store.list_added_repos().await {
        Ok(r) => r,
        Err(e) => {
            warn!("poll: list repos failed: {e}");
            return 0;
        }
    };
    let mut triggered = 0;
    let mut seen_repos = std::collections::HashSet::new();
    for added in repos {
        let repo_id = added.repo_id;
        if !seen_repos.insert(repo_id.storage_key()) {
            continue;
        }
        let Some(provider) = state
            .provider_registry
            .get(repo_id.provider.as_str())
            .cloned()
        else {
            continue; // unknown provider; skip
        };
        // Cheap tip probe, under the same fetch cap as a real fetch so a sweep
        // can't become uncapped upstream chatter. Best-effort.
        let credential = match state.broker.fetch_credential(&repo_id, None) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "poll: credential fetch for {} failed: {e:#}",
                    repo_id.storage_key()
                );
                continue;
            }
        };
        let tip = {
            let _permit = fetch_semaphore()
                .acquire()
                .await
                .expect("fetch semaphore never closed");
            let _ = test_hook(TestStage::TipProbe).await;
            git::ls_remote_tip_async(&provider, &repo_id, "HEAD", credential.as_ref()).await
        };
        let Ok(Some(tip)) = tip else {
            continue; // unknown HEAD / probe failed
        };
        match trigger_build(state, &repo_id, tip.commit.clone()).await {
            Ok(EnqueueOutcome::Enqueued) => {
                triggered += 1;
                info!(
                    "poll: triggered HEAD build for {} at {}",
                    repo_id.storage_key(),
                    &tip.commit[..7.min(tip.commit.len())]
                );
            }
            Ok(EnqueueOutcome::Coalesced) => {}
            Ok(EnqueueOutcome::Full) => {}
            Err(e) => warn!("poll: trigger {}@HEAD failed: {e}", repo_id.storage_key()),
        }
    }
    triggered
}

/// Spawn the polling-fallback loop. `interval == 0` disables it.
fn spawn_poll_loop(state: ServerState, interval: Duration) {
    if interval.is_zero() {
        info!("poll fallback disabled (RIPCLONE_POLL_INTERVAL_SECS=0)");
        return;
    }
    info!("poll fallback enabled every {:?}", interval);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let n = poll_once(&state).await;
            if n > 0 {
                info!("poll fallback: triggered {n} build(s)");
            }
        }
    });
}

/// Hash the auth token, or fail if it is missing/empty. Pure (no env access) so
/// it is unit-testable without starting a server or touching global state.
fn auth_token_hash(raw: Option<String>) -> Result<String> {
    raw.filter(|t| !t.is_empty())
        .map(|t| hex::encode(Sha256::digest(t.as_bytes())))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "RIPCLONE_SERVER_TOKEN is not set. Refusing to start an unauthenticated server."
            )
        })
}

/// Read the server auth token from the environment.
///
/// Precedence:
///   1. RIPCLONE_SERVER_TOKEN_HASH (already hashed)
///   2. RIPCLONE_SERVER_TOKEN (raw)
fn read_server_auth_token() -> Result<String> {
    if let Some(hash) = env::var("RIPCLONE_SERVER_TOKEN_HASH")
        .ok()
        .filter(|t| !t.is_empty())
    {
        return Ok(hash);
    }
    if let Some(raw) = env::var("RIPCLONE_SERVER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
    {
        return Ok(hex::encode(Sha256::digest(raw.as_bytes())));
    }
    auth_token_hash(None)
}

async fn run_server_with_barrier_at_control(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    explicit_control_path: Option<&std::path::Path>,
    host: &str,
    port: u16,
    artifact_barrier: Option<ArtifactBarrier>,
) -> Result<()> {
    let default_control_path = repo_root.parent().unwrap_or(repo_root).join("control.db");
    let control_settings = crate::control::ControlSettings::from_sources(
        explicit_control_path,
        &default_control_path,
    )?;

    let token_hash = read_server_auth_token()?;
    info!("server auth token configured; auth middleware enabled");

    // Session-token signing key. Derived from the *raw* server token (or an
    // explicit RIPCLONE_JWT_SECRET) — never from the hash clients hold. Disabled
    // when only the hash is configured, so we never sign with client-known material.
    let raw_server_token = env::var("RIPCLONE_SERVER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    let jwt = crate::auth::jwt::JwtKeys::from_env(raw_server_token.as_deref()).map(Arc::new);
    if jwt.is_some() {
        info!("session tokens enabled: `ripclone auth login` issues short-lived JWTs");
    } else {
        info!(
            "session tokens disabled: set RIPCLONE_JWT_SECRET (or RIPCLONE_SERVER_TOKEN as raw, not _HASH) to enable `ripclone auth login`"
        );
    }

    let provider_registry = ProviderRegistry::load().context("load provider registry")?;
    info!(
        "provider registry loaded with {} instance(s)",
        provider_registry.iter().count()
    );
    let broker = broker_from_env(provider_registry.clone())?;

    let rate_burst_raw = env::var("RIPCLONE_RATE_LIMIT_BURST").ok();
    let rate_per_sec_raw = env::var("RIPCLONE_RATE_LIMIT_PER_SEC").ok();
    let (rate_burst, rate_per_sec) =
        parse_rate_limit_settings(rate_burst_raw.as_deref(), rate_per_sec_raw.as_deref())?;
    let rate_limiter = RateLimiter::new(rate_burst, rate_per_sec);
    info!(
        "rate limiter enabled: burst={}, restore={}/s",
        rate_burst, rate_per_sec
    );

    let metrics = Metrics::new();
    let control_db = Arc::new(
        crate::control::ControlDb::open(
            &control_settings.path,
            control_settings.turso,
            control_settings.size_classes,
        )
        .await?,
    );
    info!(
        path = %control_db.path().display(),
        turso_replica = control_db.is_turso_replica(),
        "server owns control database"
    );
    std::fs::create_dir_all(cas_dir)?;
    std::fs::create_dir_all(repo_root)?;
    let b = backends::Backends::from_env_with_ref_store(
        cas_dir,
        repo_root,
        &metrics,
        control_db.ref_store(),
    )
    .await?;
    b.cache_retention.clone().spawn_from_env();

    let oidc_audience = env::var("RIPCLONE_OIDC_AUDIENCE")
        .ok()
        .filter(|t| !t.is_empty());
    let oidc_verifier = oidc_audience.map(OidcVerifier::new);
    if oidc_verifier.is_some() {
        info!("OIDC verification enabled for audience configured via RIPCLONE_OIDC_AUDIENCE");
    }

    // Webhook receiver config: per-provider secret + optional allowlist (built
    // before the registry is moved into the state). A push to a configured
    // webhook triggers a build before any clone — no per-repo Actions workflow.
    let webhook_config = Arc::new(WebhookConfig::from_env(&provider_registry));
    let worker_queue = control_db.queue();
    let build_queue = worker_queue.clone() as JobQueueRef;

    let state = ServerState {
        cas: b.cas,
        storage: b.storage,
        repo_root: repo_root.to_path_buf(),
        ref_store: b.ref_store,
        provider_registry,
        broker,
        token_hash: Some(token_hash),
        jwt,
        metrics,
        rate_limiter,
        build_queue,
        control_db: Some(control_db),
        worker_queue: Some(worker_queue.clone()),
        build_queue_depth: Arc::new(AtomicUsize::new(0)),
        oidc_verifier,
        webhook_config,
        sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        artifact_fetch_count: Arc::new(AtomicUsize::new(0)),
        fail_first_fetches: fail_first_fetches_from_env(),
        artifact_barrier,
        readyz_cache: Arc::new(std::sync::Mutex::new(None)),
        access_verifier: Arc::new(HttpAccessVerifier::new()),
        require_repo_auth: require_repo_auth_from_env(),
    };

    spawn_durable_build_worker(state.clone(), worker_queue);

    // Polling fallback: catches pushes that arrived without a webhook/Actions
    // trigger so build-before-clone still holds. Defaults to 5 minutes so
    // webhook-less self-hosts still self-heal missed or stuck builds.
    const DEFAULT_POLL_INTERVAL_SECS: u64 = 300;
    let poll_interval = Duration::from_secs(
        env::var("RIPCLONE_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_POLL_INTERVAL_SECS),
    );
    spawn_poll_loop(state.clone(), poll_interval);

    let app = build_app(state);
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    if require_repo_auth_from_env() {
        info!(
            "per-repo access enforcement ON: private repos require the caller's credential on every read (RIPCLONE_TRUST_GATEWAY=1 to disable for single-tenant self-host)"
        );
    } else {
        warn!(
            "per-repo access enforcement OFF (RIPCLONE_TRUST_GATEWAY): any holder of the shared server token can read any cached repo — keep this backend network-isolated and single-tenant"
        );
    }
    info!("ripclone server listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// Run the server with any test artifact barrier installed via
/// [`set_test_artifact_barrier`].
pub async fn run_server(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    host: &str,
    port: u16,
) -> Result<()> {
    run_server_with_barrier_at_control(
        cas_dir,
        repo_root,
        None,
        host,
        port,
        take_test_artifact_barrier(),
    )
    .await
}

pub async fn run_server_with_control(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    control_path: &std::path::Path,
    host: &str,
    port: u16,
) -> Result<()> {
    run_server_with_barrier_at_control(
        cas_dir,
        repo_root,
        Some(control_path),
        host,
        port,
        take_test_artifact_barrier(),
    )
    .await
}

pub async fn run_server_with_barrier(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    host: &str,
    port: u16,
    artifact_barrier: Option<ArtifactBarrier>,
) -> Result<()> {
    run_server_with_barrier_at_control(cas_dir, repo_root, None, host, port, artifact_barrier).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::util::ServiceExt;

    fn ready_artifacts(commit: &str, label: &str) -> crate::ClonepackArtifacts {
        let hash = |suffix: &str| crate::cas::hash(format!("{label}-{suffix}").as_bytes());
        crate::ClonepackArtifacts {
            manifest: hash("manifest"),
            metadata_chunk: hash("metadata"),
            skeleton_pack: hash("skeleton-pack"),
            skeleton_idx: hash("skeleton-idx"),
            prebuilt_index: hash("index"),
            idx_bundle: hash("idx-bundle"),
            commit: commit.to_string(),
            ..Default::default()
        }
    }

    fn report_chunk(hash: &str) -> ChunkRef {
        ChunkRef {
            hash: hash_from_hex(hash).unwrap(),
            len: 1,
        }
    }

    fn put_report_manifest(
        storage: &StorageRef,
        manifest_commit: &str,
        artifacts: &mut crate::ClonepackArtifacts,
        packs: &[crate::PackArtifact],
        archives: &[String],
    ) {
        let manifest = ClonepackManifest {
            commit: manifest_commit.to_string(),
            metadata_chunk: Some(report_chunk(&artifacts.metadata_chunk)),
            archive_chunks: archives.iter().map(|hash| report_chunk(hash)).collect(),
            packs: packs
                .iter()
                .map(|pack| crate::clonepack::PackEntry {
                    pack: Some(report_chunk(&pack.pack)),
                    idx: Some(report_chunk(&pack.idx)),
                    ..Default::default()
                })
                .collect(),
            midx: (!artifacts.midx.is_empty()).then(|| report_chunk(&artifacts.midx)),
            idx_bundle: (!artifacts.idx_bundle.is_empty())
                .then(|| report_chunk(&artifacts.idx_bundle)),
            ..Default::default()
        }
        .encode_to_vec();
        let hash = crate::cas::hash(&manifest);
        storage.put(&hash, &manifest).unwrap();
        artifacts.manifest = hash;
    }

    #[test]
    fn exact_result_requires_the_requested_stored_result() {
        let commit = "a".repeat(40);
        let info = RefInfo {
            commit: commit.clone(),
            full: Some(crate::FullResult {
                clonepack: ready_artifacts(&commit, "full"),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!exact_result_ready(&info, ExactResultKind::Head, &commit));
        assert!(exact_result_ready(&info, ExactResultKind::Full, &commit));
        assert!(!exact_result_ready(&info, ExactResultKind::Files, &commit));
        assert!(!exact_result_ready(
            &info,
            ExactResultKind::Full,
            &"b".repeat(40)
        ));
    }
    #[tokio::test]
    async fn pending_body_includes_the_selected_branch() {
        let response = artifact_pending_response(&"a".repeat(40), "rélease/東京", 3).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some("r%C3%A9lease%2F%E6%9D%B1%E4%BA%AC")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read pending body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("pending JSON");
        let keys = body.as_object().expect("pending object");
        assert_eq!(keys.len(), 5);
        assert_eq!(body["code"], "artifact_pending");
        assert_eq!(body["commit"], "a".repeat(40));
        assert_eq!(body["branch"], "rélease/東京");
        assert_eq!(body["status"], "building");
        assert_eq!(body["queue_depth"], 3);
    }

    // Classification must be TYPE-based (downcast at the do_sync error boundary),
    // not string-matching. These pin the concrete-source → retryable mapping; a
    // regression to message-matching or a mis-mapped source flips a case.

    #[test]
    fn classify_s3_transport_error_is_retryable() {
        // A Tigris network blip surfaces as `s3::Error::Transport`. If the type
        // is lost (e.g. stringified in collect_stream) this falls through to
        // permanent — the stale-until-repush bug.
        let e = anyhow::Error::new(s3::Error::Transport {
            message: "connection reset".into(),
            source: None,
        })
        .context("S3 get_object");
        assert!(classify_build_error(&e).is_retryable());
    }

    #[test]
    fn classify_s3_5xx_is_retryable_and_config_is_permanent() {
        let five_xx = anyhow::Error::new(s3::Error::Api {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: None,
            message: None,
            request_id: None,
            host_id: None,
            body_snippet: None,
        });
        assert!(classify_build_error(&five_xx).is_retryable());

        let bad_config = anyhow::Error::new(s3::Error::InvalidConfig {
            message: "bad bucket".into(),
        });
        assert!(!classify_build_error(&bad_config).is_retryable());
    }

    #[test]
    fn classify_s3_404_is_permanent() {
        let not_found = anyhow::Error::new(s3::Error::Api {
            status: StatusCode::NOT_FOUND,
            code: None,
            message: None,
            request_id: None,
            host_id: None,
            body_snippet: None,
        });
        assert!(!classify_build_error(&not_found).is_retryable());
    }

    #[test]
    fn classify_retryable_io_error_is_retryable() {
        let e = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::ConnectionReset))
            .context("upload chunk");
        assert!(classify_build_error(&e).is_retryable());
    }

    #[test]
    fn classify_unknown_error_is_permanent() {
        // No recognized transient source in the chain → permanent, so a genuine
        // bad-repo/malformed failure fails fast instead of burning the cap.
        let e = anyhow::anyhow!("malformed pack index");
        assert!(!classify_build_error(&e).is_retryable());

        let not_found_io = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::NotFound));
        assert!(!classify_build_error(&not_found_io).is_retryable());
    }

    #[test]
    fn classify_api_report_error_retryable_and_permanent() {
        let retry = anyhow::Error::new(crate::api_ref_store::ApiReportError::retryable(
            "metadata report to http://x: network unreachable",
        ));
        assert!(classify_build_error(&retry).is_retryable());

        let permanent = anyhow::Error::new(crate::api_ref_store::ApiReportError::permanent(
            "metadata report unauthorized (401)",
        ));
        assert!(!classify_build_error(&permanent).is_retryable());
    }

    struct TestSqlQueue {
        path: PathBuf,
        inner: tokio::sync::OnceCell<Arc<crate::queue::SqlJobQueue>>,
        observer: Option<tokio::sync::mpsc::UnboundedSender<BuildJob>>,
    }

    struct TestSqlRefStore {
        path: PathBuf,
        inner: tokio::sync::OnceCell<Arc<crate::meta::SqlRefStore>>,
        fail_next_result_read: AtomicBool,
        fail_next_added_repo_read: AtomicBool,
    }

    impl TestSqlRefStore {
        fn new(path: PathBuf) -> Self {
            Self {
                path,
                inner: tokio::sync::OnceCell::new(),
                fail_next_result_read: AtomicBool::new(false),
                fail_next_added_repo_read: AtomicBool::new(false),
            }
        }

        fn fail_next_result_read(&self) {
            self.fail_next_result_read.store(true, Ordering::SeqCst);
        }

        fn fail_next_added_repo_read(&self) {
            self.fail_next_added_repo_read.store(true, Ordering::SeqCst);
        }

        async fn store(&self) -> anyhow::Result<&Arc<crate::meta::SqlRefStore>> {
            self.inner
                .get_or_try_init(|| async {
                    let db = crate::meta::LibsqlMeta::connect(&self.path.to_string_lossy()).await?;
                    Ok(Arc::new(crate::meta::SqlRefStore::new(Box::new(db)).await?))
                })
                .await
        }
    }

    #[async_trait::async_trait]
    impl RefStore for TestSqlRefStore {
        async fn load_result(
            &self,
            repo_id: &RepoId,
            commit: &str,
        ) -> anyhow::Result<Option<RefInfo>> {
            if self.fail_next_result_read.swap(false, Ordering::SeqCst) {
                anyhow::bail!("injected request-path exact-result read failure");
            }
            self.store().await?.load_result(repo_id, commit).await
        }
        async fn save_result(&self, repo_id: &RepoId, info: &RefInfo) -> anyhow::Result<()> {
            self.store().await?.save_result(repo_id, info).await
        }
        async fn publish_head(
            &self,
            repo_id: &RepoId,
            commit: &str,
            head: crate::HeadResult,
        ) -> anyhow::Result<bool> {
            self.store()
                .await?
                .publish_head(repo_id, commit, head)
                .await
        }
        async fn publish_full(
            &self,
            repo_id: &RepoId,
            commit: &str,
            full: crate::FullResult,
        ) -> anyhow::Result<bool> {
            self.store()
                .await?
                .publish_full(repo_id, commit, full)
                .await
        }
        async fn publish_files(
            &self,
            repo_id: &RepoId,
            commit: &str,
            files: crate::FilesResult,
        ) -> anyhow::Result<bool> {
            self.store()
                .await?
                .publish_files(repo_id, commit, files)
                .await
        }
        async fn list_commits(&self, repo_id: &RepoId) -> anyhow::Result<Vec<String>> {
            self.store().await?.list_commits(repo_id).await
        }
        async fn add_repo(&self, repo: &AddedRepo) -> anyhow::Result<()> {
            self.store().await?.add_repo(repo).await
        }
        async fn load_added_repo(&self, repo_id: &RepoId) -> anyhow::Result<Option<AddedRepo>> {
            if self.fail_next_added_repo_read.swap(false, Ordering::SeqCst) {
                anyhow::bail!("injected added-repository read failure");
            }
            self.store().await?.load_added_repo(repo_id).await
        }
        async fn remove_added_repo(&self, repo_id: &RepoId) -> anyhow::Result<()> {
            self.store().await?.remove_added_repo(repo_id).await
        }
        async fn list_added_repos(&self) -> anyhow::Result<Vec<AddedRepo>> {
            self.store().await?.list_added_repos().await
        }
        async fn health(&self) -> anyhow::Result<()> {
            self.store().await?.health().await
        }
    }

    impl TestSqlQueue {
        fn new(
            path: PathBuf,
            observer: Option<tokio::sync::mpsc::UnboundedSender<BuildJob>>,
        ) -> Self {
            Self {
                path,
                inner: tokio::sync::OnceCell::new(),
                observer,
            }
        }

        async fn queue(&self) -> anyhow::Result<&Arc<crate::queue::SqlJobQueue>> {
            self.inner
                .get_or_try_init(|| async {
                    let db = crate::queue::LibsqlDb::connect(&self.path.to_string_lossy()).await?;
                    Ok(Arc::new(
                        crate::queue::SqlJobQueue::new(Box::new(db)).await?,
                    ))
                })
                .await
        }
    }

    #[async_trait::async_trait]
    impl crate::queue::JobQueue for TestSqlQueue {
        async fn enqueue(&self, job: BuildJob) -> anyhow::Result<crate::queue::Enqueued> {
            let result = self.queue().await?.enqueue(job.clone()).await?;
            if result.outcome == EnqueueOutcome::Enqueued
                && let Some(observer) = &self.observer
            {
                let _ = observer.send(job);
            }
            Ok(result)
        }

        async fn job_status(&self, job_id: i64) -> anyhow::Result<JobState> {
            self.queue().await?.job_status(job_id).await
        }

        async fn depth(&self) -> usize {
            match self.queue().await {
                Ok(queue) => queue.depth().await,
                Err(_) => 0,
            }
        }
    }

    fn install_observed_queue(
        state: &mut ServerState,
    ) -> tokio::sync::mpsc::UnboundedReceiver<BuildJob> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        state.build_queue = Arc::new(TestSqlQueue::new(
            state.repo_root.join("test-control-jobs.db"),
            Some(sender),
        ));
        receiver
    }

    #[tokio::test]
    async fn request_result_read_failure_returns_error_without_dispatch() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(TestSqlRefStore::new(
            tmp.path().join("request-read-failure-refs.db"),
        ));
        let mut state = test_state_with_ref_store(&tmp, store.clone());
        let mut dispatched = install_observed_queue(&mut state);
        let repo_id = RepoId::github("acme/request-read-failure");
        mark_added(&state, repo_id.clone()).await;
        store.fail_next_result_read();

        let error = trigger_build(&state, &repo_id, "a".repeat(40))
            .await
            .expect_err("request exact-result read failure must be returned");
        assert!(error.contains("exact result lookup failed"), "{error}");
        assert!(
            dispatched.try_recv().is_err(),
            "failed readiness lookup must not dispatch a job"
        );
    }

    fn test_state(tmp: &tempfile::TempDir) -> ServerState {
        let ref_store: Arc<dyn RefStore> = Arc::new(TestSqlRefStore::new(
            tmp.path().join("repos").join("test-control-refs.db"),
        ));
        test_state_with_ref_store(tmp, ref_store)
    }

    fn test_state_with_ref_store(
        tmp: &tempfile::TempDir,
        ref_store: Arc<dyn RefStore>,
    ) -> ServerState {
        let cas_root = tmp.path().join("cas");
        let cas = Cas::new(&cas_root).unwrap();
        let storage = crate::storage::local(&cas_root).unwrap();
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&repo_root).unwrap();
        let token_hash = hex::encode(Sha256::digest("secret"));
        let metrics = Metrics::new();
        let build_queue: JobQueueRef = Arc::new(TestSqlQueue::new(
            repo_root.join("test-control-jobs.db"),
            None,
        ));
        let provider_registry = ProviderRegistry::new();
        let broker: Arc<dyn CredentialBroker> = Arc::new(crate::auth::broker::StaticBroker::new(
            provider_registry.clone(),
        ));
        ServerState {
            cas,
            storage,
            repo_root,
            ref_store,
            provider_registry,
            broker,
            token_hash: Some(token_hash),
            jwt: None,
            metrics,
            rate_limiter: RateLimiter::new(100, 100.0),
            build_queue,
            control_db: None,
            worker_queue: None,
            build_queue_depth: Arc::new(AtomicUsize::new(0)),
            oidc_verifier: None,
            // No webhook secret here (worker has no HTTP; tests install their own).
            webhook_config: Arc::new(WebhookConfig::empty()),
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            artifact_fetch_count: Arc::new(AtomicUsize::new(0)),
            fail_first_fetches: fail_first_fetches_from_env(),
            artifact_barrier: take_test_artifact_barrier(),
            readyz_cache: Arc::new(std::sync::Mutex::new(None)),
            // Default tests to single-tenant trust (no network access checks);
            // the authz-specific tests override these two fields with a fake.
            access_verifier: Arc::new(HttpAccessVerifier::new()),
            require_repo_auth: false,
        }
    }

    fn auth_header() -> String {
        format!("Ripclone {}", hex::encode(Sha256::digest("secret")))
    }

    #[tokio::test]
    async fn repository_list_is_authenticated_and_returns_repo_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let github = RepoId::github("zeta/repo");
        let gitlab = RepoId {
            provider: crate::provider::ProviderInstanceId::new("gitlab"),
            path: "group/sub/repo".to_string(),
        };
        mark_added(&state, github.clone()).await;
        mark_added(&state, gitlab.clone()).await;
        let store = Arc::clone(&state.ref_store);
        let app = build_app(state);

        let response = app
            .clone()
            .oneshot(test_request("GET", "/v1/repos"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let repos: Vec<RepoId> = serde_json::from_slice(&body).unwrap();
        assert_eq!(repos.len(), 2);
        assert!(repos.contains(&github));
        assert!(repos.contains(&gitlab));

        for auth in [None, Some("Ripclone wrong")] {
            let rejected = app
                .clone()
                .oneshot(request_with_auth("GET", "/v1/repos", auth))
                .await
                .unwrap();
            assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        }

        for auth in [None, Some("Ripclone wrong")] {
            let rejected_remove = app
                .clone()
                .oneshot(request_with_auth(
                    "DELETE",
                    "/v1/repos/github/zeta/repo/add",
                    auth,
                ))
                .await
                .unwrap();
            assert_eq!(rejected_remove.status(), StatusCode::UNAUTHORIZED);
            assert!(
                store.load_added_repo(&github).await.unwrap().is_some(),
                "rejected removal must preserve the registration"
            );
        }
    }

    #[tokio::test]
    async fn repository_remove_rejects_missing_invalid_and_lookup_failure_without_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(TestSqlRefStore::new(
            tmp.path().join("remove-lookup-failure-refs.db"),
        ));
        let state = test_state_with_ref_store(&tmp, store.clone());
        let repo_id = RepoId::github("acme/kept");
        mark_added(&state, repo_id.clone()).await;
        let app = build_app(state);

        let missing = app
            .clone()
            .oneshot(test_request("DELETE", "/v1/repos/github/acme/missing/add"))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let missing_body = axum::body::to_bytes(missing.into_body(), usize::MAX)
            .await
            .unwrap();
        let missing_body: serde_json::Value = serde_json::from_slice(&missing_body).unwrap();
        assert_eq!(missing_body["code"], "repo_not_added");
        assert!(store.load_added_repo(&repo_id).await.unwrap().is_some());

        let invalid = app
            .clone()
            .oneshot(test_request(
                "DELETE",
                "/v1/repos/github/acme/bad%7Frepo/add",
            ))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert!(
            store.load_added_repo(&repo_id).await.unwrap().is_some(),
            "invalid removal must leave other registrations intact"
        );

        store.fail_next_added_repo_read();
        let failed = app
            .oneshot(test_request("DELETE", "/v1/repos/github/acme/kept/add"))
            .await
            .unwrap();
        assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            store.load_added_repo(&repo_id).await.unwrap().is_some(),
            "lookup failure must leave the registration intact"
        );
    }

    #[tokio::test]
    async fn repository_remove_leaves_pending_and_claimed_jobs_active() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let queue_db = crate::queue::LibsqlDb::connect(
            &tmp.path().join("remove-running-jobs.db").to_string_lossy(),
        )
        .await
        .unwrap();
        let queue = Arc::new(
            crate::queue::SqlJobQueue::new(Box::new(queue_db))
                .await
                .unwrap(),
        );
        state.build_queue = queue.clone();
        let repo_id = RepoId::github("acme/pending");
        mark_added(&state, repo_id.clone()).await;
        let first = state
            .build_queue
            .enqueue(BuildJob {
                repo_id: repo_id.clone(),
                admitted_commit: "a".repeat(40),
                repo_config: crate::repo_config::RepoConfig::default(),
                credential: None,
                size_bytes: None,
            })
            .await
            .unwrap();
        let second = state
            .build_queue
            .enqueue(BuildJob {
                repo_id: repo_id.clone(),
                admitted_commit: "b".repeat(40),
                repo_config: crate::repo_config::RepoConfig::default(),
                credential: None,
                size_bytes: None,
            })
            .await
            .unwrap();
        let first_id = first.job_id.expect("first durable admitted job id");
        let second_id = second.job_id.expect("second durable admitted job id");
        let claimed = crate::queue::WorkerQueue::claim(queue.as_ref(), "worker-1")
            .await
            .unwrap()
            .expect("first job is claimable");
        assert_eq!(claimed.id, first_id);
        assert!(
            matches!(
                state.build_queue.job_status(first_id).await.unwrap(),
                JobState::Pending
            ),
            "a claimed job reports as active before removal"
        );
        assert!(
            matches!(
                state.build_queue.job_status(second_id).await.unwrap(),
                JobState::Pending
            ),
            "the second job must be queued before removal"
        );

        let response = remove_added_repo_inner(repo_id, state.clone()).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(
            matches!(
                state.build_queue.job_status(first_id).await.unwrap(),
                JobState::Pending
            ),
            "removal must not cancel a claimed job"
        );
        assert!(
            matches!(
                state.build_queue.job_status(second_id).await.unwrap(),
                JobState::Pending
            ),
            "removal must not cancel a queued job"
        );
        assert!(
            crate::queue::WorkerQueue::ack(queue.as_ref(), first_id, "worker-1", Ok(()))
                .await
                .unwrap(),
            "the worker must retain ownership of its claim after removal"
        );
        let claimed_after_remove = crate::queue::WorkerQueue::claim(queue.as_ref(), "worker-2")
            .await
            .unwrap()
            .expect("the queued job remains claimable after removal");
        assert_eq!(claimed_after_remove.id, second_id);
        assert!(
            crate::queue::WorkerQueue::ack(queue.as_ref(), second_id, "worker-2", Ok(()))
                .await
                .unwrap(),
            "the queued job must remain settleable after removal"
        );
    }

    #[test]
    fn build_error_classification_maps_storage_sources() {
        let retryable = classify_build_error(&anyhow::Error::new(s3::Error::Api {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: None,
            message: None,
            request_id: None,
            host_id: None,
            body_snippet: None,
        }));
        assert!(retryable.is_retryable());

        let retryable =
            classify_build_error(&anyhow::Error::new(s3::Error::transport("network", None)));
        assert!(retryable.is_retryable());

        let permanent = classify_build_error(&anyhow::Error::new(s3::Error::Api {
            status: StatusCode::NOT_FOUND,
            code: None,
            message: None,
            request_id: None,
            host_id: None,
            body_snippet: None,
        }));
        assert!(!permanent.is_retryable());
    }

    #[test]
    fn build_error_classification_maps_io_timeout_and_upstream_sources() {
        let timeout = classify_build_error(&anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timeout",
        )));
        assert!(timeout.is_retryable());

        let malformed = classify_build_error(&anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "bad input",
        )));
        assert!(!malformed.is_retryable());

        let upstream_429 = classify_build_error(&anyhow::Error::new(git::UpstreamGitError::new(
            "fetch", true,
        )));
        assert!(upstream_429.is_retryable());

        let upstream_not_found = classify_build_error(&anyhow::Error::new(
            git::UpstreamGitError::new("fetch", false),
        ));
        assert!(!upstream_not_found.is_retryable());
    }

    #[test]
    fn archive_publish_uploads_download_bundles_and_reuse_frames() {
        let uploads = archive_publish_upload_hashes(
            "metadata",
            "clonepack",
            &["bundle-b".to_string(), "bundle-a".to_string()],
            &["frame-a".to_string(), "bundle-a".to_string(), String::new()],
        );
        assert_eq!(
            uploads,
            vec![
                "bundle-a".to_string(),
                "bundle-b".to_string(),
                "clonepack".to_string(),
                "frame-a".to_string(),
                "metadata".to_string(),
            ]
        );
    }

    #[test]
    fn archive_chunk_refs_rejects_hash_length_mismatch() {
        let mut metadata = crate::clonepack::MetadataChunk::new();
        metadata.frames.push(crate::clonepack::FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: 4,
            raw_len: 10,
        });
        metadata.frames.push(crate::clonepack::FrameInfo {
            chunk_index: 1,
            chunk_offset: 0,
            compressed_len: 5,
            raw_len: 11,
        });

        let err = archive_chunk_refs(&["a".repeat(64)], &metadata).unwrap_err();
        assert!(
            err.to_string()
                .contains("archive chunk hash/length mismatch"),
            "{err:#}"
        );
    }

    #[test]
    fn validate_repo_id_accepts_github_identifiers() {
        assert!(validate_repo_id("ripclone").is_ok());
        assert!(validate_repo_id("ripclone-rs").is_ok());
        assert!(validate_repo_id("ripclone.rs").is_ok());
        assert!(validate_repo_id("rip_clone").is_ok());
    }

    #[test]
    fn validate_repo_id_rejects_path_traversal() {
        assert!(validate_repo_id("..").is_err());
        assert!(validate_repo_id("foo/bar").is_err());
        assert!(validate_repo_id("foo\\bar").is_err());
        assert!(validate_repo_id("foo\0bar").is_err());
        assert!(validate_repo_id("").is_err());
    }

    #[test]
    fn auth_token_hash_requires_a_nonempty_token() {
        // Missing or empty token must be rejected with a clear message...
        for missing in [None, Some(String::new())] {
            let err = auth_token_hash(missing).unwrap_err().to_string();
            assert!(
                err.contains("RIPCLONE_SERVER_TOKEN"),
                "error should mention missing token: {err}"
            );
        }
        // ...and a real token hashes to the same digest the auth middleware checks.
        let hash = auth_token_hash(Some("secret".to_string())).unwrap();
        assert_eq!(hash, hex::encode(Sha256::digest("secret")));
    }

    #[test]
    fn read_server_auth_token_prefers_new_env_vars() {
        // Clean deprecated vars.
        unsafe {
            env::remove_var("RIPCLONE_SERVER_TOKEN");
            env::remove_var("RIPCLONE_SERVER_TOKEN_HASH");
        }
        unsafe { env::set_var("RIPCLONE_SERVER_TOKEN", "new-secret") };
        assert_eq!(
            read_server_auth_token().unwrap(),
            hex::encode(Sha256::digest("new-secret"))
        );
        unsafe { env::set_var("RIPCLONE_SERVER_TOKEN_HASH", "prefixed-hash") };
        assert_eq!(read_server_auth_token().unwrap(), "prefixed-hash");
        unsafe {
            env::remove_var("RIPCLONE_SERVER_TOKEN");
            env::remove_var("RIPCLONE_SERVER_TOKEN_HASH");
        }
    }

    #[test]
    fn rate_limiter_keys_by_ip_and_is_bounded() {
        let limiter = RateLimiter::new(10, 10.0);
        let first = "192.168.1.1";
        let second = "192.168.1.2";
        assert!(limiter.check(first));
        assert!(limiter.check(second));

        // Exhaust the burst for a third IP and ensure it is rejected.
        let third = "192.168.1.3";
        for _ in 0..10 {
            assert!(limiter.check(third));
        }
        assert!(!limiter.check(third));

        // Many distinct IPs should not grow the map without bound.
        for i in 0..20_000u64 {
            let ip = format!("10.0.{}. {}", i / 256, i % 256);
            limiter.check(&ip);
        }
        let len = limiter.buckets.lock().unwrap().len();
        assert!(len <= 10_000, "rate limiter map grew unbounded: {}", len);
    }

    #[test]
    fn rate_limit_key_collapses_ipv6_and_honors_trusted_forwarded() {
        use std::net::Ipv6Addr;
        let socket = SocketAddr::from(([203, 0, 113, 7], 51000));

        // Untrusted: always the socket IP, ignore any forwarded-for header.
        let mut spoof = HeaderMap::new();
        spoof.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());
        assert_eq!(rate_limit_key(&spoof, socket, false), "203.0.113.7");

        // Trusted: take the rightmost forwarded-for entry (what our proxy saw),
        // ignoring entries a client prepends.
        let mut xff = HeaderMap::new();
        xff.insert("x-forwarded-for", "9.9.9.9, 198.51.100.23".parse().unwrap());
        assert_eq!(rate_limit_key(&xff, socket, true), "198.51.100.23");

        // IPv6 collapses to its /64 so an attacker can't rotate within a /64.
        let a = SocketAddr::new(
            std::net::IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xab, 0xcd, 1, 2, 3, 4)),
            0,
        );
        let b = SocketAddr::new(
            std::net::IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xab, 0xcd, 9, 9, 9, 9)),
            0,
        );
        let ka = rate_limit_key(&HeaderMap::new(), a, false);
        let kb = rate_limit_key(&HeaderMap::new(), b, false);
        assert_eq!(ka, kb, "same /64 must share a bucket");
        assert_eq!(ka, "2001:db8:ab:cd::/64");
    }

    #[tokio::test]
    async fn authenticated_artifact_fanout_does_not_consume_control_plane_rate_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        state.rate_limiter = RateLimiter::new(1, 0.0);

        let data = b"immutable artifact";
        let hash = hex::encode(Sha256::digest(data));
        state.storage.put(&hash, data).unwrap();
        let app = build_app(state);
        let artifact_path = format!("/v1/artifacts/{hash}");

        // A logical clone can request many authenticated immutable objects from
        // one IP. Even a one-token control-plane bucket must not reject them.
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(request_with_auth(
                    "GET",
                    &artifact_path,
                    Some(&auth_header()),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), data);
        }

        // The exemption is narrow: ordinary control-plane requests from the
        // same IP still consume the configured bucket and then receive 429.
        let first = app
            .clone()
            .oneshot(request_with_auth("GET", "/readyz", None))
            .await
            .unwrap();
        assert_ne!(first.status(), StatusCode::TOO_MANY_REQUESTS);
        let second = app
            .oneshot(request_with_auth("GET", "/readyz", None))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn local_storage_does_not_produce_signed_urls() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = crate::storage::local(tmp.path()).unwrap();
        let info = RefInfo {
            commit: "abc".to_string(),
            full: Some(crate::FullResult {
                clonepack: crate::ClonepackArtifacts {
                    manifest: "exact-manifest".to_string(),
                    metadata_chunk: "exact-metadata".to_string(),
                    commit: "exact-commit".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let provider = ProviderRegistry::new().default_provider().clone();
        let repo_id = RepoId::github("o/r");
        let resp = ref_response(
            &repo_id,
            &provider,
            "main".to_string(),
            &info,
            &storage,
            ExactResultKind::Full,
            false,
        );
        assert_eq!(resp.commit, "exact-commit");
        assert_eq!(resp.clonepack_manifest, "exact-manifest");
        assert_eq!(resp.metadata_chunk, "exact-metadata");
        assert!(resp.clonepack_manifest_url.is_none());
        assert!(resp.metadata_chunk_url.is_none());
        assert!(resp.archive_chunk_urls.is_none());
    }

    #[test]
    fn signed_url_ttl_is_shorter_for_private() {
        // Defaults (no env override): public 20m, private 5m. Private must be the
        // shorter window — it bounds how long a leaked/stale signed URL works
        // after a caller loses GitHub access.
        assert_eq!(ref_signed_url_ttl(false), Duration::from_secs(1200));
        assert_eq!(ref_signed_url_ttl(true), Duration::from_secs(300));
        assert!(ref_signed_url_ttl(true) < ref_signed_url_ttl(false));
    }

    #[test]
    fn visibility_header_is_parsed_case_insensitively() {
        use axum::http::HeaderValue;
        let mut h = HeaderMap::new();
        assert!(!visibility_is_private(&h)); // absent → public (self-host direct)
        h.insert("x-ripclone-visibility", HeaderValue::from_static("private"));
        assert!(visibility_is_private(&h));
        h.insert("x-ripclone-visibility", HeaderValue::from_static("PRIVATE"));
        assert!(visibility_is_private(&h));
        h.insert("x-ripclone-visibility", HeaderValue::from_static("public"));
        assert!(!visibility_is_private(&h));
        h.insert("x-ripclone-visibility", HeaderValue::from_static("wat"));
        assert!(visibility_is_private(&h));
        h.insert(
            "x-ripclone-visibility",
            HeaderValue::from_bytes(&[0xff]).unwrap(),
        );
        assert!(visibility_is_private(&h));
    }

    /// A canned [`AccessVerifier`] for the authz wiring tests.
    struct StubVerifier(AccessDecision);

    #[async_trait::async_trait]
    impl AccessVerifier for StubVerifier {
        async fn verify(
            &self,
            _p: &ProviderInstance,
            _path: &str,
            _c: Option<&secrecy::SecretString>,
        ) -> AccessDecision {
            self.0
        }
    }

    #[test]
    fn rate_limiter_sanitizes_invalid_restore_rates() {
        for rate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            let limiter = RateLimiter::new(1, rate);
            assert!(limiter.check("client"));
            assert!(!limiter.check("client"));
        }
    }

    #[test]
    fn rate_limit_config_rejects_invalid_values() {
        for rate in ["NaN", "inf", "-inf", "-1"] {
            let error = parse_rate_limit_settings(None, Some(rate)).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("RIPCLONE_RATE_LIMIT_PER_SEC must be finite and non-negative"),
                "got: {error:#}"
            );
        }

        let error = parse_rate_limit_settings(Some("not-a-number"), None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("RIPCLONE_RATE_LIMIT_BURST must be an unsigned integer"),
            "got: {error:#}"
        );
        assert_eq!(
            parse_rate_limit_settings(Some("0"), Some("0")).unwrap(),
            (0, 0.0)
        );
    }

    #[test]
    fn rate_limiter_retains_fractional_refill_credit() {
        let limiter = RateLimiter::new(1, 10.0);
        assert!(limiter.check("client"));

        for expected in [false, true] {
            limiter.buckets.lock().unwrap().get_mut("client").unwrap().0 =
                Instant::now() - Duration::from_millis(50);
            assert_eq!(limiter.check("client"), expected);
        }
    }

    /// AU1 gate decisions: trust mode falls back to the header; enforced mode
    /// maps the verifier's decision to public/private/403.
    #[tokio::test]
    async fn authorize_repo_read_maps_decisions() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let provider = state.provider_registry.get("github").unwrap().clone();
        let repo = RepoId::github("o/r");
        let headers = HeaderMap::new();

        // Trust mode: gate skipped, visibility from header (absent → public).
        state.require_repo_auth = false;
        assert!(
            !authorize_repo_read(&state, &provider, &repo, None, &headers)
                .await
                .unwrap()
        );

        // Enforced + public → served anonymously (private = false).
        state.require_repo_auth = true;
        state.access_verifier = Arc::new(StubVerifier(AccessDecision::Public));
        assert!(
            !authorize_repo_read(&state, &provider, &repo, None, &headers)
                .await
                .unwrap()
        );

        // Enforced + authorized private → private = true.
        state.access_verifier = Arc::new(StubVerifier(AccessDecision::PrivateAuthorized));
        assert!(
            authorize_repo_read(&state, &provider, &repo, None, &headers)
                .await
                .unwrap()
        );

        // Enforced + denied → 403.
        state.access_verifier = Arc::new(StubVerifier(AccessDecision::Denied));
        let resp = authorize_repo_read(&state, &provider, &repo, None, &headers)
            .await
            .unwrap_err();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    /// End-to-end: a `refs` read for a repo the caller can't access returns 403
    /// through the real route — and never reaches the build/mirror path. (Before
    /// AU1, a cached private repo here returned 200 to any shared-token holder.)
    #[tokio::test]
    async fn ref_read_is_forbidden_when_access_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        state.require_repo_auth = true;
        state.access_verifier = Arc::new(StubVerifier(AccessDecision::Denied));
        mark_added(&state, RepoId::github("o/r")).await;
        let app = build_app(state);

        let resp = app
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/o/r/refs/main?result=full",
                Some(&auth_header()),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    fn test_request(method: &str, uri: &str) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("Authorization", auth_header())
            .header("x-ripclone-protocol", crate::PROTOCOL_VERSION)
            .body(Body::empty())
            .unwrap()
    }

    /// Like `test_request` but with an explicit (or absent) `Authorization`
    /// header, for exercising the auth middleware's reject path.
    fn request_with_auth(method: &str, uri: &str, auth: Option<&str>) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("x-ripclone-protocol", crate::PROTOCOL_VERSION);
        if let Some(a) = auth {
            b = b.header("Authorization", a);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn session_tokens_disabled_without_signing_key() {
        // test_state has token_hash set but jwt = None (no signing key).
        let tmp = tempfile::tempdir().unwrap();
        let app = build_app(test_state(&tmp));
        // Login can't mint a token → 503, never a token.
        let login = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/auth/login")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(Body::from("secret=whatever"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(login.status(), StatusCode::SERVICE_UNAVAILABLE);
        // A bearer token is never accepted when issuance is disabled.
        let bearer = app
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/acme/secret/status",
                Some("Bearer anything.at.all"),
            ))
            .await
            .unwrap();
        assert_eq!(bearer.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn protected_route_rejects_missing_and_wrong_token() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        // No Authorization header.
        let missing = app
            .clone()
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/acme/secret/status",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        // Present but wrong token.
        let wrong = app
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/acme/secret/status",
                Some("Ripclone deadbeef"),
            ))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn public_endpoints_require_no_auth() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        // Liveness, readiness, and the Prometheus scrape must be reachable with
        // no credentials (load balancers / scrapers don't authenticate). They
        // must never return 401 from the auth middleware.
        for path in ["/healthz", "/readyz", "/metrics", "/v1/version"] {
            let resp = app
                .clone()
                .oneshot(request_with_auth("GET", path, None))
                .await
                .unwrap();
            assert_ne!(
                resp.status(),
                StatusCode::UNAUTHORIZED,
                "{path} must not require auth"
            );
        }
    }

    /// Return an observation receiver while the authoritative queue remains
    /// SQLite-backed.
    fn test_state_with_queue(
        tmp: &tempfile::TempDir,
    ) -> (ServerState, tokio::sync::mpsc::UnboundedReceiver<BuildJob>) {
        let mut state = test_state(tmp);
        let rx = install_observed_queue(&mut state);
        (state, rx)
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn ordinary_http_returns_after_enqueue_without_querying_queue_depth() {
        struct DepthMustNotRunQueue;

        #[async_trait::async_trait]
        impl crate::queue::JobQueue for DepthMustNotRunQueue {
            async fn enqueue(&self, _job: BuildJob) -> anyhow::Result<crate::queue::Enqueued> {
                Ok(crate::queue::Enqueued {
                    outcome: EnqueueOutcome::Enqueued,
                    job_id: Some(1),
                })
            }

            async fn depth(&self) -> usize {
                std::future::pending().await
            }
        }

        let _env = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base.path().join("acme").join("accepted.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let commit = crate::test_fixture::commit(&origin, &[("README.md", b"accepted")]);

        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        state.build_queue = Arc::new(DepthMustNotRunQueue);
        let repo_id = RepoId::github("acme/accepted");
        mark_added(&state, repo_id).await;
        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path()) };
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            build_app(state).oneshot(test_request(
                "POST",
                "/v1/repos/github/acme/accepted/sync?branch=main",
            )),
        )
        .await
        .expect("accepted response must not wait for queue depth")
        .unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["commit"], commit);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn oidc_build_wakeup_ignores_body_target_and_admits_one_probed_head() {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let head = crate::test_fixture::commit(&origin, &[("f.txt", b"HEAD\n")]);

        let tmp = tempfile::tempdir().unwrap();
        let (mut state, mut rx) = test_state_with_queue(&tmp);
        mark_added(&state, RepoId::github("acme/widget")).await;
        const AUDIENCE: &str = "ripclone-test-audience";
        const KID: &str = "ripclone-test-kid";
        state.oidc_verifier = Some(crate::oidc::OidcVerifier::new_for_test(
            AUDIENCE.to_string(),
            KID,
            crate::auth::broker::tests::TEST_PUBLIC_KEY.as_bytes(),
        ));
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({
            "sub": "repo:acme/widget:ref:refs/heads/main",
            "iss": "https://token.actions.githubusercontent.com",
            "aud": AUDIENCE,
            "repository": "acme/widget",
            "repository_owner": "acme",
            "repository_id": "123",
            "iat": now,
            "exp": now + 300
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(KID.to_string());
        let oidc = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(crate::auth::broker::tests::TEST_PRIVATE_KEY.as_bytes())
                .unwrap(),
        )
        .unwrap();
        let decoy = "ffffffffffffffffffffffffffffffffffffffff";
        let body = serde_json::json!({
            "owner": "acme",
            "repo": "widget",
            "commit": decoy,
            "ref": "refs/heads/body-decoy"
        });
        let probe = Arc::new(AdmissionTestProbe::default());
        let _probe_guard = install_admission_test_probe(Arc::clone(&probe));
        unsafe {
            std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path());
            std::env::set_var("RIPCLONE_TESTING", "1");
        }
        let response = build_app(state.clone())
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/build")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
                    .header("Authorization", format!("Bearer {oidc}"))
                    .header("X-Ripclone-Token", hex::encode(Sha256::digest("secret")))
                    .header("Content-Type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        unsafe {
            std::env::remove_var("RIPCLONE_ORIGIN_BASE");
            std::env::remove_var("RIPCLONE_TESTING");
        }

        let response_status = response.status();
        let response_body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert_eq!(
            response_status,
            StatusCode::ACCEPTED,
            "OIDC build response: {}",
            String::from_utf8_lossy(&response_body)
        );
        assert_eq!(probe.tip_probes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.queue_inserts.load(Ordering::SeqCst), 1);
        let job = rx.try_recv().expect("OIDC wakeup enqueued exact HEAD job");
        assert_eq!(job.repo_id, RepoId::github("acme/widget"));
        assert_eq!(job.admitted_commit, head);
        assert_ne!(job.admitted_commit, decoy);
        assert!(rx.try_recv().is_err(), "one HEAD probe admitted one job");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn missing_admitted_commit_fails_exact_fetch_without_branch_fallback() {
        let _env = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base.path().join("acme").join("missing.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let available = crate::test_fixture::commit(&origin, &[("README.md", b"available")]);
        let missing = "f".repeat(40);
        assert_ne!(available, missing);

        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let repo_id = RepoId::github("acme/missing");
        let provider = state.provider_registry.get("github").unwrap().clone();
        let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
        let lock = repo_lock(&state.sync_locks, &repo_id).await;
        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path()) };
        let result = do_sync(
            &state.cas,
            &mirror_dir,
            &repo_id,
            &missing,
            &state.ref_store,
            &state.storage,
            &provider,
            None,
            &crate::repo_config::RepoConfig::default(),
            &lock,
            None,
        )
        .await;
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };

        let error = result.expect_err("an unavailable exact target must fail");
        assert!(
            error.to_string().contains("fetch rev"),
            "failure must identify the exact fetch: {error:#}"
        );
        assert!(
            git::resolve_commit(&mirror_dir, "main").is_err(),
            "the worker must not fall back to the moving branch tip"
        );
        assert!(
            state
                .ref_store
                .load_result(&repo_id, &missing)
                .await
                .unwrap()
                .is_none(),
            "an unavailable exact target must not publish branch metadata"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn worker_crashes_at_artifact_boundaries_never_publish_false_ready_results() {
        let _env = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path());
            std::env::set_var("RIPCLONE_TESTING", "1");
        }
        let probe = Arc::new(AdmissionTestProbe::default());
        let _probe_guard = install_admission_test_probe(Arc::clone(&probe));

        for stage in [
            "before_upload",
            "during_upload",
            "after_upload",
            "before_ready_publication",
        ] {
            let repo_path = format!("acme/crash-{}", stage.replace('_', "-"));
            let origin_path = origin_base.path().join(format!("{repo_path}.git"));
            std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
            let origin = crate::test_fixture::init_bare(&origin_path);
            let admitted = crate::test_fixture::commit(
                &origin,
                &[("value.txt", format!("{stage}\n").as_bytes())],
            );
            let later = crate::test_fixture::commit(&origin, &[("value.txt", b"later\n")]);

            let tmp = tempfile::tempdir().unwrap();
            let state = test_state(&tmp);
            let repo_id = RepoId::github(&repo_path);
            let job = BuildJob {
                repo_id: repo_id.clone(),
                admitted_commit: admitted.clone(),
                repo_config: crate::repo_config::RepoConfig::default(),
                credential: None,
                size_bytes: None,
            };
            let pending = prepare_exact_admission(&state, &job)
                .await
                .unwrap()
                .expect("new exact result admission")
                .pending;
            state
                .ref_store
                .save_result(&repo_id, &pending)
                .await
                .unwrap();

            let barrier = tmp.path().join(format!("crash-{stage}"));
            unsafe {
                std::env::set_var("RIPCLONE_TEST_BUILD_CRASH_STAGE", stage);
                std::env::set_var("RIPCLONE_TEST_BUILD_CRASH_COMMIT", &admitted);
                std::env::set_var("RIPCLONE_TEST_BUILD_CRASH_BARRIER_DIR", &barrier);
            }
            let uploads_before = probe.artifact_uploads.load(Ordering::SeqCst);
            let crashed_state = state.clone();
            let crashed_job = job.clone();
            let attempt = tokio::spawn(async move {
                process_build_job(&crashed_state, &crashed_job, 0, "test-worker").await
            });
            tokio::time::timeout(Duration::from_secs(30), async {
                while !barrier.join("entered").exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("worker reached deterministic {stage} barrier"));
            attempt.abort();
            assert!(
                attempt.await.unwrap_err().is_cancelled(),
                "{stage}: the admitted worker task is the crashed attempt"
            );
            let uploads_started = probe.artifact_uploads.load(Ordering::SeqCst) - uploads_before;
            match stage {
                "before_upload" => assert_eq!(uploads_started, 0, "{stage}: no Head upload began"),
                "during_upload" => assert_eq!(
                    uploads_started, 1,
                    "{stage}: exactly one Head artifact uploaded before the crash"
                ),
                _ => assert!(
                    uploads_started > 1,
                    "{stage}: every Head artifact uploaded before the crash (saw {uploads_started})"
                ),
            }

            let interrupted = state
                .ref_store
                .load_result(&repo_id, &admitted)
                .await
                .unwrap()
                .expect("interrupted exact row remains retryable");
            assert_eq!(interrupted.commit, admitted, "{stage}: no wrong commit");
            assert!(
                interrupted.head.is_none(),
                "{stage}: Head must remain missing"
            );
            assert!(
                interrupted.full.is_none(),
                "{stage}: Full must remain missing"
            );
            assert!(
                state
                    .ref_store
                    .load_result(&repo_id, &later)
                    .await
                    .unwrap()
                    .is_none(),
                "{stage}: the worker must not publish the branch's later commit"
            );

            let recovered = process_build_job(&state, &job, 0, "test-worker")
                .await
                .unwrap_or_else(|error| panic!("{stage}: exact retry failed: {error:#}"));
            assert_eq!(recovered.info.commit, admitted);
            assert!(exact_result_ready(
                &recovered.info,
                ExactResultKind::Head,
                &admitted
            ));
            assert!(exact_result_ready(
                &recovered.info,
                ExactResultKind::Full,
                &admitted
            ));
            assert_eq!(
                state.ref_store.list_commits(&repo_id).await.unwrap(),
                vec![admitted],
                "{stage}: crash and retry must not create an extra result"
            );
        }

        unsafe {
            std::env::remove_var("RIPCLONE_TEST_BUILD_CRASH_STAGE");
            std::env::remove_var("RIPCLONE_TEST_BUILD_CRASH_COMMIT");
            std::env::remove_var("RIPCLONE_TEST_BUILD_CRASH_BARRIER_DIR");
            std::env::remove_var("RIPCLONE_TESTING");
            std::env::remove_var("RIPCLONE_ORIGIN_BASE");
        }
    }

    fn gh_sign(secret: &str, body: &[u8]) -> String {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    const WEBHOOK_SECRET: &str = "shhh-very-secret";

    fn webhook_request(
        provider: &str,
        event: &str,
        signature: Option<&str>,
        body: Vec<u8>,
    ) -> axum::http::Request<axum::body::Body> {
        let mut b = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/webhooks/{provider}"))
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-GitHub-Event", event);
        if let Some(sig) = signature {
            b = b.header("X-Hub-Signature-256", sig);
        }
        b.body(axum::body::Body::from(body)).unwrap()
    }

    fn gh_push_body(
        owner: &str,
        repo: &str,
        ref_: &str,
        after: &str,
        default_branch: &str,
        deleted: bool,
    ) -> Vec<u8> {
        serde_json::json!({
            "ref": ref_,
            "after": after,
            "deleted": deleted,
            "repository": {
                "name": repo,
                "owner": {"login": owner},
                "default_branch": default_branch,
                "private": false
            }
        })
        .to_string()
        .into_bytes()
    }

    /// A push payload that omits `repository.default_branch`, to exercise the
    /// mirror fallback.
    fn gh_push_body_no_default(owner: &str, repo: &str, ref_: &str, after: &str) -> Vec<u8> {
        serde_json::json!({
            "ref": ref_,
            "after": after,
            "deleted": false,
            "repository": {"name": repo, "owner": {"login": owner}, "private": false}
        })
        .to_string()
        .into_bytes()
    }

    /// A test state with a configured `github` webhook secret and a durable-job
    /// observer so a test can assert what `trigger_build` admitted.
    fn webhook_state(
        tmp: &tempfile::TempDir,
    ) -> (ServerState, tokio::sync::mpsc::UnboundedReceiver<BuildJob>) {
        let mut state = test_state(tmp);
        let rx = install_observed_queue(&mut state);
        state.webhook_config = Arc::new(WebhookConfig::with_secret("github", WEBHOOK_SECRET));
        (state, rx)
    }

    async fn mark_added(state: &ServerState, repo_id: RepoId) {
        state
            .ref_store
            .add_repo(&AddedRepo {
                repo_id,
                added_at: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                history_enabled: true,
                source: AddedRepoSource::Api,
                repo_size_bytes: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn webhook_without_secret_returns_503() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp); // no webhook secret configured
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn webhook_push_enqueues_build() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        mark_added(&state, RepoId::github("acme/widget")).await;
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let job = rx.try_recv().expect("a build job was enqueued");
        assert_eq!(job.repo_id, RepoId::github("acme/widget"));
        assert_eq!(job.admitted_commit, "1".repeat(40));
        assert!(rx.try_recv().is_err(), "exactly one job enqueued");
    }

    #[tokio::test]
    async fn webhook_invalid_signature_returns_401() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign("wrong-secret", &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(rx.try_recv().is_err(), "a bad signature must not enqueue");
    }

    #[tokio::test]
    async fn webhook_missing_signature_returns_401() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, _rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let resp = app
            .oneshot(webhook_request("github", "push", None, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn webhook_tampered_body_returns_401() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        // Sign body A with the correct secret, deliver body B. Proves the handler
        // verifies over the raw received bytes, not a re-serialized parse.
        let body_a = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body_a);
        let body_b = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"2".repeat(40),
            "main",
            false,
        );
        assert_ne!(body_a, body_b);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body_b))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(rx.try_recv().is_err(), "a tampered body must not enqueue");
    }

    #[tokio::test]
    async fn webhook_ping_is_acknowledged_without_build() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = br#"{"zen":"keep it simple"}"#.to_vec();
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "ping", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "ping must not enqueue");
    }

    #[tokio::test]
    async fn webhook_non_default_branch_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/feature",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "non-default push must not enqueue");
    }

    #[tokio::test]
    async fn webhook_allowlist_blocks_unlisted_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let mut rx = install_observed_queue(&mut state);
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                .with_allowlist(["acme/allowed".to_string()]),
        );
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "unlisted repo must not enqueue");
    }

    #[tokio::test]
    async fn webhook_allowlist_allows_listed_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let mut rx = install_observed_queue(&mut state);
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                .with_allowlist(["acme/widget".to_string()]),
        );
        mark_added(&state, RepoId::github("acme/widget")).await;
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/main",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            rx.try_recv().expect("listed repo enqueues").repo_id,
            RepoId::github("acme/widget")
        );
    }

    #[tokio::test]
    async fn webhook_github_allowlist_accepts_bare_and_prefixed() {
        // Both the canonical `owner/repo` and the forgiving `github/owner/repo`
        // forms admit a github repo, so github's bare-key asymmetry vs the
        // `gitlab/...` form isn't a silent footgun.
        for entry in ["acme/widget", "github/acme/widget"] {
            let tmp = tempfile::tempdir().unwrap();
            let mut state = test_state(&tmp);
            let mut rx = install_observed_queue(&mut state);
            state.webhook_config = Arc::new(
                WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                    .with_allowlist([entry.to_string()]),
            );
            mark_added(&state, RepoId::github("acme/widget")).await;
            let app = build_app(state);
            let body = gh_push_body(
                "acme",
                "widget",
                "refs/heads/main",
                &"1".repeat(40),
                "main",
                false,
            );
            let sig = gh_sign(WEBHOOK_SECRET, &body);
            let resp = app
                .oneshot(webhook_request("github", "push", Some(&sig), body))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "allowlist entry {entry}");
            assert!(rx.try_recv().is_ok(), "entry {entry} must admit the repo");
        }
    }

    #[tokio::test]
    async fn webhook_tag_push_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/tags/v1.0.0",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "a tag push must not enqueue");
    }

    #[tokio::test]
    async fn webhook_hostile_branch_name_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/--upload-pack=evil",
            &"1".repeat(40),
            "main",
            false,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(rx.try_recv().is_err(), "an invalid branch must not enqueue");
    }

    #[tokio::test]
    async fn webhook_branch_delete_preserves_exact_result() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let repo = RepoId::github("acme/widget");
        let commit = "d".repeat(40);
        let info = RefInfo {
            commit: commit.clone(),
            ..Default::default()
        };
        state.ref_store.save_result(&repo, &info).await.unwrap();
        let ref_store = state.ref_store.clone();
        let app = build_app(state);
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/feature",
            &"0".repeat(40),
            "main",
            true,
        );
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            ref_store
                .load_result(&repo, &commit)
                .await
                .unwrap()
                .is_some(),
            "branch deletion must not delete an exact commit result"
        );
        assert!(rx.try_recv().is_err(), "a delete must not enqueue a build");
    }

    #[tokio::test]
    async fn webhook_unknown_provider_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, _rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body = br#"{}"#.to_vec();
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("nope", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// Build state with a single non-default provider instance configured plus
    /// its webhook secret and a durable-job observer.
    fn provider_webhook_state(
        tmp: &tempfile::TempDir,
        id: &str,
        kind: &str,
        host: &str,
    ) -> (ServerState, tokio::sync::mpsc::UnboundedReceiver<BuildJob>) {
        let mut state = test_state(tmp);
        let rx = install_observed_queue(&mut state);
        let mut registry = ProviderRegistry::new();
        registry
            .merge_one(crate::provider::ProviderConfig {
                id: id.to_string(),
                kind: Some(kind.to_string()),
                host: Some(host.to_string()),
                auth_template: (kind == "generic").then(|| "token {token}".to_string()),
                ..Default::default()
            })
            .unwrap();
        state.provider_registry = registry;
        state.webhook_config = Arc::new(WebhookConfig::with_secret(id, WEBHOOK_SECRET));
        (state, rx)
    }

    #[tokio::test]
    async fn webhook_provider_without_adapter_returns_501() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, _rx) = provider_webhook_state(&tmp, "generic", "generic", "git.example.com");
        let app = build_app(state);
        let resp = app
            .oneshot(webhook_request(
                "generic",
                "push",
                Some("whatever"),
                br#"{}"#.to_vec(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn webhook_gitlab_push_enqueues() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = provider_webhook_state(&tmp, "gitlab", "gitlab", "gitlab.com");
        mark_added(
            &state,
            RepoId {
                provider: crate::provider::ProviderInstanceId::new("gitlab"),
                path: "group/sub/proj".to_string(),
            },
        )
        .await;
        let app = build_app(state);
        let body = br#"{"object_kind":"push","ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","project":{"path_with_namespace":"group/sub/proj","default_branch":"main","visibility_level":0}}"#.to_vec();
        // GitLab authenticates with the shared token in X-Gitlab-Token.
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/webhooks/gitlab")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-Gitlab-Event", "Push Hook")
            .header("X-Gitlab-Token", WEBHOOK_SECRET)
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let job = rx.try_recv().expect("gitlab default-branch push enqueues");
        assert_eq!(job.repo_id.path, "group/sub/proj");
        assert_eq!(job.admitted_commit, "1".repeat(40));
    }

    #[tokio::test]
    async fn webhook_gitlab_bad_token_returns_401() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = provider_webhook_state(&tmp, "gitlab", "gitlab", "gitlab.com");
        let app = build_app(state);
        let body = br#"{"ref":"refs/heads/main","after":"abc","project":{"path_with_namespace":"g/p","default_branch":"main"}}"#.to_vec();
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/webhooks/gitlab")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-Gitlab-Event", "Push Hook")
            .header("X-Gitlab-Token", "wrong-token")
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(rx.try_recv().is_err(), "a bad token must not enqueue");
    }

    #[tokio::test]
    async fn webhook_gitea_push_enqueues() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = provider_webhook_state(&tmp, "gitea", "gitea", "gitea.example.com");
        mark_added(
            &state,
            RepoId {
                provider: crate::provider::ProviderInstanceId::new("gitea"),
                path: "acme/widget".to_string(),
            },
        )
        .await;
        let app = build_app(state);
        let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","repository":{"full_name":"acme/widget","default_branch":"main","private":true}}"#.to_vec();
        // Gitea signs the raw body with HMAC-SHA256, bare hex in X-Gitea-Signature.
        let sig = {
            use hmac::{Hmac, KeyInit, Mac};
            use sha2::Sha256;
            let mut mac = Hmac::<Sha256>::new_from_slice(WEBHOOK_SECRET.as_bytes()).unwrap();
            mac.update(&body);
            hex::encode(mac.finalize().into_bytes())
        };
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/webhooks/gitea")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-Gitea-Event", "push")
            .header("X-Gitea-Signature", sig)
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let job = rx.try_recv().expect("gitea default-branch push enqueues");
        assert_eq!(job.repo_id.path, "acme/widget");
        assert_eq!(job.admitted_commit, "1".repeat(40));
    }

    #[tokio::test]
    async fn webhook_gitea_bad_signature_returns_401() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = provider_webhook_state(&tmp, "gitea", "gitea", "gitea.example.com");
        let app = build_app(state);
        let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","repository":{"full_name":"acme/widget","default_branch":"main"}}"#.to_vec();
        // Sign with the WRONG secret.
        let sig = {
            use hmac::{Hmac, KeyInit, Mac};
            use sha2::Sha256;
            let mut mac = Hmac::<Sha256>::new_from_slice(b"wrong-secret").unwrap();
            mac.update(&body);
            hex::encode(mac.finalize().into_bytes())
        };
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/webhooks/gitea")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-Gitea-Event", "push")
            .header("X-Gitea-Signature", sig)
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(
            rx.try_recv().is_err(),
            "a bad gitea signature must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_gitlab_allowlist_matches_natural_key() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut state, mut rx) = provider_webhook_state(&tmp, "gitlab", "gitlab", "gitlab.com");
        // The allowlist is written in the operator-facing natural form
        // (provider-prefixed, unescaped) — not the escaped storage key.
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("gitlab", WEBHOOK_SECRET)
                .with_allowlist(["gitlab/group/sub/proj".to_string()]),
        );
        mark_added(
            &state,
            RepoId {
                provider: crate::provider::ProviderInstanceId::new("gitlab"),
                path: "group/sub/proj".to_string(),
            },
        )
        .await;
        let app = build_app(state);
        let body = br#"{"ref":"refs/heads/main","after":"1111111111111111111111111111111111111111","project":{"path_with_namespace":"group/sub/proj","default_branch":"main"}}"#.to_vec();
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/webhooks/gitlab")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("X-Gitlab-Event", "Push Hook")
            .header("X-Gitlab-Token", WEBHOOK_SECRET)
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            rx.try_recv()
                .expect("allowlisted gitlab repo enqueues")
                .repo_id
                .path,
            "group/sub/proj"
        );
    }

    #[tokio::test]
    async fn webhook_missing_default_identity_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let app = build_app(state);
        let body =
            gh_push_body_no_default("acme", "widget", "refs/heads/whatever", &"1".repeat(40));
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            rx.try_recv().is_err(),
            "missing default-branch identity is ignored"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn poll_resolves_head_once_and_coalesces_the_exact_job() {
        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(origin.parent().unwrap()).unwrap();
        let repo = crate::test_fixture::init_bare(&origin);
        let tip = crate::test_fixture::commit(&repo, &[("f.txt", b"v1")]);
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut observed) = test_state_with_queue(&tmp);
        mark_added(&state, RepoId::github("acme/widget")).await;
        let probe = Arc::new(AdmissionTestProbe::default());
        let _probe_guard = install_admission_test_probe(Arc::clone(&probe));
        unsafe {
            std::env::set_var("RIPCLONE_ORIGIN_BASE", base.path());
            std::env::set_var("RIPCLONE_TESTING", "1");
        }
        assert_eq!(poll_once(&state).await, 1);
        let first = observed.try_recv().expect("HEAD poll admitted exact work");
        assert_eq!(first.admitted_commit, tip);
        assert_eq!(poll_once(&state).await, 0);
        assert!(
            observed.try_recv().is_err(),
            "second poll must not add a job"
        );
        unsafe {
            std::env::remove_var("RIPCLONE_ORIGIN_BASE");
            std::env::remove_var("RIPCLONE_TESTING");
        }
        assert_eq!(probe.tip_probes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn version_endpoint_reports_build_and_protocol() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app
            .oneshot(request_with_auth("GET", "/v1/version", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(v["protocol"], crate::PROTOCOL_VERSION);
    }

    fn protocol_request(uri: &str, protocol: Option<&str>) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method("GET")
            .uri(uri)
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("Authorization", auth_header());
        if let Some(p) = protocol {
            b = b.header("x-ripclone-protocol", p);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn protocol_guard_rejects_only_explicit_wrong_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        for protocol in [Some("1"), Some("999"), Some("invalid")] {
            let response = app
                .clone()
                .oneshot(protocol_request("/v1/repos/acme/secret/status", protocol))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
        }
        let response = app
            .clone()
            .oneshot(protocol_request(
                "/v1/repos/github/acme/secret/status",
                None,
            ))
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UPGRADE_REQUIRED);
        let current = crate::PROTOCOL_VERSION.to_string();
        let response = app
            .oneshot(protocol_request(
                "/v1/repos/github/acme/secret/status",
                Some(&current),
            ))
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UPGRADE_REQUIRED);
    }

    #[tokio::test]
    async fn repo_status_returns_empty_for_cold_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app
            .oneshot(test_request("GET", "/v1/repos/github/acme/secret/status"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.owner, "acme");
        assert_eq!(status.repo, "secret");
        assert!(status.refs.is_empty());
        assert_eq!(status.total_bytes, 0);
        assert_eq!(status.total_unique_bytes, 0);
        assert_eq!(status.regions.len(), 1);
        assert_eq!(status.regions[0].region, "local");
        assert_eq!(status.regions[0].unique_bytes, 0);
    }

    #[tokio::test]
    async fn readyz_ready_when_healthy() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_not_ready_when_storage_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        // Simulate the data volume being unmounted/removed under the server.
        std::fs::remove_dir_all(tmp.path().join("cas")).unwrap();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_endpoint_is_prometheus_text() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        state.metrics.record_ref_lookup();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/metrics")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let ct = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        assert!(ct.starts_with("text/plain"), "content-type was {ct}");
        assert!(ct.contains("version=0.0.4"), "content-type was {ct}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("# TYPE ripclone_ref_lookups_total counter"));
        assert!(text.contains("\nripclone_ref_lookups_total 1\n"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn readyz_not_ready_when_storage_read_only() {
        // root ignores directory permissions, so this probe can't be exercised
        // as root (common in CI containers); skip there.
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping read-only probe test: running as root");
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let cas = tmp.path().join("cas");
        // r-x only: the dir still stats as a directory, but writes fail — the
        // case the old is_dir() check missed.
        std::fs::set_permissions(&cas, std::fs::Permissions::from_mode(0o500)).unwrap();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        std::fs::set_permissions(&cas, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "read-only CAS must report not ready"
        );
    }

    #[tokio::test]
    async fn readyz_not_ready_when_ref_store_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        std::fs::remove_dir_all(tmp.path().join("repos")).unwrap();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn repo_status_reports_each_stored_result_and_current_job() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let repo_id = RepoId::github("acme/status-results");
        let commit = "a".repeat(40);
        let metadata = state.cas.put(b"status metadata").unwrap();
        let manifest =
            make_manifest(&commit, &None, &[], &metadata, 15, Vec::new(), None, None).unwrap();
        let manifest = state.cas.put(&manifest.encode_to_vec()).unwrap();
        let mut head_artifacts = ready_artifacts(&commit, "status-head");
        head_artifacts.manifest = manifest.clone();
        head_artifacts.metadata_chunk = metadata.clone();
        let mut full_artifacts = ready_artifacts(&commit, "status-full");
        full_artifacts.manifest = manifest;
        full_artifacts.metadata_chunk = metadata;
        let head = crate::HeadResult {
            clonepack: head_artifacts,
            ..Default::default()
        };
        state
            .ref_store
            .save_result(
                &repo_id,
                &RefInfo {
                    commit: commit.clone(),
                    head: Some(head.clone()),
                    full: Some(crate::FullResult {
                        clonepack: full_artifacts,
                        ..Default::default()
                    }),
                    files: None,
                },
            )
            .await
            .unwrap();
        state
            .ref_store
            .publish_head(&repo_id, &commit, head)
            .await
            .unwrap();

        let status = build_repo_status(&state, &repo_id, false, None)
            .await
            .unwrap();
        assert_eq!(status.refs.len(), 1);
        let exact = &status.refs[0];
        assert!(exact.head);
        assert!(exact.full);
        assert!(!exact.files);
        assert_eq!(exact.job, "none");
        assert!(exact.job_error.is_none());
    }
    #[tokio::test]
    async fn sync_rejects_invalid_branch_name() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let app = build_app(state);
        let response = app
            .oneshot(test_request(
                "POST",
                "/v1/repos/github/acme/secret/sync?branch=../evil",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    fn ref_report_request(
        token: Option<&str>,
        body: &serde_json::Value,
    ) -> axum::http::Request<Body> {
        let mut b = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/refs")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))))
            .header("Content-Type", "application/json")
            .header("x-ripclone-protocol", crate::PROTOCOL_VERSION);
        if let Some(t) = token {
            b = b.header("Authorization", format!("Bearer {t}"));
        }
        b.body(Body::from(serde_json::to_vec(body).unwrap()))
            .unwrap()
    }

    /// Authenticated worker output must match the exact commit and contain the
    /// hashes its result needs. A valid retry replaces invalid stored output.
    #[tokio::test]
    async fn ref_report_rejects_invalid_outputs_and_accepts_valid_retries() {
        let secret = crate::job_token::report_token_secret_from_env()
            .or_else(|| {
                // test_state does not set RIPCLONE_SERVER_TOKEN; plant one for the mint.
                unsafe { std::env::set_var("RIPCLONE_SERVER_TOKEN", "secret") };
                crate::job_token::report_token_secret_from_env()
            })
            .expect("job token secret");
        let repo_key = "github/acme%2Fwidget";
        let tok =
            crate::job_token::mint_job_token(&secret, std::time::Duration::from_secs(300)).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let ref_store = state.ref_store.clone();
        let rid = RepoId::github("acme/widget");
        let commit = "a".repeat(40);
        ref_store
            .save_result(
                &rid,
                &RefInfo {
                    commit: commit.clone(),
                    head: Some(crate::HeadResult::default()),
                    full: Some(crate::FullResult::default()),
                    files: Some(crate::FilesResult::default()),
                },
            )
            .await
            .unwrap();
        let storage = state.storage.clone();
        let app = build_app(state);
        let wrong_commit = "b".repeat(40);

        let wrong_head = crate::HeadResult {
            clonepack: ready_artifacts(&wrong_commit, "wrong-head"),
            ..Default::default()
        };
        let wrong_full = crate::FullResult {
            clonepack: ready_artifacts(&wrong_commit, "wrong-full"),
            ..Default::default()
        };
        let wrong_files = crate::FilesResult {
            clonepack: ready_artifacts(&wrong_commit, "wrong-files"),
            ..Default::default()
        };

        let missing_head = crate::HeadResult {
            clonepack: ready_artifacts(&commit, "missing-head-manifest"),
            ..Default::default()
        };
        let missing_full = crate::FullResult {
            clonepack: ready_artifacts(&commit, "missing-full-manifest"),
            ..Default::default()
        };
        let mut missing_files_artifacts = ready_artifacts(&commit, "missing-files-manifest");
        missing_files_artifacts.idx_bundle.clear();
        let missing_files = crate::FilesResult {
            clonepack: missing_files_artifacts,
            ..Default::default()
        };

        let mut wrong_manifest_head = crate::HeadResult {
            clonepack: ready_artifacts(&commit, "wrong-manifest-commit"),
            ..Default::default()
        };
        put_report_manifest(
            &storage,
            &wrong_commit,
            &mut wrong_manifest_head.clonepack,
            &[],
            &[],
        );

        let corrupt_manifest = [0x80];
        let corrupt_manifest_hash = crate::cas::hash(&corrupt_manifest);
        storage
            .put(&corrupt_manifest_hash, &corrupt_manifest)
            .unwrap();
        let mut corrupt_manifest_head = crate::HeadResult {
            clonepack: ready_artifacts(&commit, "corrupt-manifest"),
            ..Default::default()
        };
        corrupt_manifest_head.clonepack.manifest = corrupt_manifest_hash;

        let mut wrong_metadata_full = crate::FullResult {
            clonepack: ready_artifacts(&commit, "wrong-manifest-metadata"),
            ..Default::default()
        };
        put_report_manifest(
            &storage,
            &commit,
            &mut wrong_metadata_full.clonepack,
            &[],
            &[],
        );
        wrong_metadata_full.clonepack.metadata_chunk = crate::cas::hash(b"other metadata");

        let reported_pack = crate::PackArtifact {
            pack: crate::cas::hash(b"reported pack"),
            idx: crate::cas::hash(b"reported idx"),
        };
        let mut inconsistent_head = crate::HeadResult {
            clonepack: ready_artifacts(&commit, "inconsistent-head-packs"),
            packs: vec![reported_pack.clone()],
            ..Default::default()
        };
        put_report_manifest(
            &storage,
            &commit,
            &mut inconsistent_head.clonepack,
            &[],
            &[],
        );
        let mut inconsistent_full = crate::FullResult {
            clonepack: ready_artifacts(&commit, "inconsistent-full-packs"),
            packs: vec![reported_pack],
            ..Default::default()
        };
        put_report_manifest(
            &storage,
            &commit,
            &mut inconsistent_full.clonepack,
            &[],
            &[],
        );
        let mut inconsistent_files_artifacts = ready_artifacts(&commit, "inconsistent-files");
        inconsistent_files_artifacts.idx_bundle.clear();
        let mut inconsistent_files = crate::FilesResult {
            clonepack: inconsistent_files_artifacts,
            archive_chunks: vec![crate::cas::hash(b"reported archive")],
            ..Default::default()
        };
        put_report_manifest(
            &storage,
            &commit,
            &mut inconsistent_files.clonepack,
            &[],
            &[],
        );

        let invalid_reports = [
            serde_json::json!({"op":"publish_head","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"head":wrong_head}),
            serde_json::json!({"op":"publish_head","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"head":crate::HeadResult::default()}),
            serde_json::json!({"op":"publish_head","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"head":missing_head}),
            serde_json::json!({"op":"publish_head","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"head":corrupt_manifest_head}),
            serde_json::json!({"op":"publish_head","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"head":wrong_manifest_head}),
            serde_json::json!({"op":"publish_head","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"head":inconsistent_head}),
            serde_json::json!({"op":"publish_full","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"full":wrong_full}),
            serde_json::json!({"op":"publish_full","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"full":crate::FullResult::default()}),
            serde_json::json!({"op":"publish_full","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"full":missing_full}),
            serde_json::json!({"op":"publish_full","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"full":wrong_metadata_full}),
            serde_json::json!({"op":"publish_full","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"full":inconsistent_full}),
            serde_json::json!({"op":"publish_files","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"files":wrong_files}),
            serde_json::json!({"op":"publish_files","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"files":crate::FilesResult::default()}),
            serde_json::json!({"op":"publish_files","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"files":missing_files}),
            serde_json::json!({"op":"publish_files","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"files":inconsistent_files}),
        ];
        for body in &invalid_reports {
            let response = app
                .clone()
                .oneshot(ref_report_request(Some(&tok), body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        let unchanged = ref_store
            .load_result(&rid, &commit)
            .await
            .unwrap()
            .expect("invalid reports preserve the existing result");
        assert!(unchanged.head.unwrap().clonepack.manifest.is_empty());
        assert!(unchanged.full.unwrap().clonepack.manifest.is_empty());
        assert!(unchanged.files.unwrap().clonepack.manifest.is_empty());

        let mut valid_head = crate::HeadResult {
            clonepack: ready_artifacts(&commit, "valid-head"),
            ..Default::default()
        };
        put_report_manifest(&storage, &commit, &mut valid_head.clonepack, &[], &[]);
        let valid_head_manifest = valid_head.clonepack.manifest.clone();
        let mut valid_full = crate::FullResult {
            clonepack: ready_artifacts(&commit, "valid-full"),
            ..Default::default()
        };
        put_report_manifest(&storage, &commit, &mut valid_full.clonepack, &[], &[]);
        let valid_full_manifest = valid_full.clonepack.manifest.clone();
        let mut valid_files_artifacts = ready_artifacts(&commit, "valid-files");
        valid_files_artifacts.idx_bundle.clear();
        let mut valid_files = crate::FilesResult {
            clonepack: valid_files_artifacts,
            ..Default::default()
        };
        put_report_manifest(&storage, &commit, &mut valid_files.clonepack, &[], &[]);
        let valid_files_manifest = valid_files.clonepack.manifest.clone();
        let valid_reports = [
            serde_json::json!({"op":"publish_head","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"head":valid_head}),
            serde_json::json!({"op":"publish_full","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"full":valid_full}),
            serde_json::json!({"op":"publish_files","job_id":1,"worker_id":"test-worker","repo_key":repo_key,"commit":commit,"files":valid_files}),
        ];
        for body in &valid_reports {
            let response = app
                .clone()
                .oneshot(ref_report_request(Some(&tok), body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let stored = ref_store
            .load_result(&rid, &commit)
            .await
            .unwrap()
            .expect("valid retries must replace invalid stored outputs");
        assert!(crate::exact_result_complete(&stored, &commit));
        assert_eq!(stored.head.unwrap().clonepack.manifest, valid_head_manifest);
        assert_eq!(stored.full.unwrap().clonepack.manifest, valid_full_manifest);
        assert_eq!(
            stored.files.unwrap().clonepack.manifest,
            valid_files_manifest
        );
    }

    /// Auth gate: wrong / missing token → 401 and no write.
    #[tokio::test]
    async fn ref_report_bad_token_rejects_and_does_not_write() {
        let secret = {
            unsafe { std::env::set_var("RIPCLONE_SERVER_TOKEN", "secret") };
            crate::job_token::report_token_secret_from_env().expect("secret")
        };
        let repo_key = "github/acme%2Fnope";
        let good =
            crate::job_token::mint_job_token(&secret, std::time::Duration::from_secs(300)).unwrap();
        // Token signed with the wrong secret must not authorize this write.
        let wrong_secret = crate::job_token::mint_job_token(
            b"a-different-secret",
            std::time::Duration::from_secs(300),
        )
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let ref_store = state.ref_store.clone();
        let rid = RepoId::github("acme/nope");
        let commit = "b".repeat(40);
        ref_store
            .save_result(
                &rid,
                &RefInfo {
                    commit: commit.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let storage = state.storage.clone();
        let app = build_app(state);

        let mut head = crate::HeadResult {
            clonepack: ready_artifacts(&commit, "authorized-head"),
            ..Default::default()
        };
        put_report_manifest(&storage, &commit, &mut head.clonepack, &[], &[]);
        let body = serde_json::json!({
            "op": "publish_head",
            "job_id": 1,
            "worker_id": "test-worker",
            "repo_key": repo_key,
            "commit": commit,
            "head": head,
        });

        // Missing Authorization.
        let resp = app
            .clone()
            .oneshot(ref_report_request(None, &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Garbage bearer.
        let resp = app
            .clone()
            .oneshot(ref_report_request(Some("not-a-real-token"), &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Well-formed token signed with the wrong secret → bad signature.
        let resp = app
            .clone()
            .oneshot(ref_report_request(Some(&wrong_secret), &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        assert!(
            ref_store
                .load_result(&rid, &"b".repeat(40))
                .await
                .unwrap()
                .is_some_and(|result| result.head.is_none()),
            "rejected reports must not write"
        );

        // Sanity: the good token still works (proves the store itself is fine).
        let resp = app
            .oneshot(ref_report_request(Some(&good), &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            ref_store
                .load_result(&rid, &"b".repeat(40))
                .await
                .unwrap()
                .is_some_and(|result| result.head.is_some())
        );
    }
}
