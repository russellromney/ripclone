use crate::RefInfo;
use crate::archive::ArchiveBuilder;
use crate::auth::access::{AccessDecision, AccessVerifier, HttpAccessVerifier};
use crate::auth::broker::{CredentialBroker, broker_from_env};
use crate::backends;
use crate::cas::Cas;
use crate::clonepack::{
    ChunkRef, ClonepackManifest, collect_manifest_hashes, hash_from_hex, hash_to_hex,
    install_manifest_pack_bytes, manifest_chunk_refs, manifest_pack_idx_bytes,
};
use crate::git;
use crate::metrics::{Metrics, SyncPhaseMetrics};
use crate::oidc::OidcVerifier;
use crate::pack::PackBuilder;
use crate::provider::{ProviderInstance, ProviderRegistry, RepoId};
use crate::queue::{BuildError, BuildJob, EnqueueOutcome, JobQueue, JobQueueRef, JobState};
use crate::ref_store::{AddedRepo, AddedRepoSource, RefStore};
use crate::remote_gc::{GcConfig, RemoteGc};
use crate::retention::Retention;
use crate::storage::StorageRef;
use crate::validation;
use crate::webhook::{EventKind, WebhookConfig};
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

/// Test-only deterministic barrier for artifact downloads. The first artifact
/// response whose body is larger than `after_bytes` splits its body into a
/// stream: it sends the prefix, signals `entered`, waits on `proceed`, and then
/// either sends the remainder or closes the connection (when `close_on_proceed`).
/// This lets tests pause a download mid-body without relying on wall-clock
/// timing or first-request fault injection.
#[derive(Clone)]
pub struct ArtifactBarrier {
    pub after_bytes: usize,
    pub entered: Arc<StdMutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    pub proceed: Arc<StdMutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
    pub close_on_proceed: bool,
    pub consumed: Arc<AtomicBool>,
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
        self.signal.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
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
    pub phase2_entry: AdmissionTestBarrier,
    pub embedded_idle_wait: AdmissionTestBarrier,
    pub enqueue_attempts: AtomicUsize,
    pub queue_inserts: AtomicUsize,
    pub coalesces: AtomicUsize,
    pub tip_probes: AtomicUsize,
    pub exact_fetches: AtomicUsize,
    pub builder_entries: AtomicUsize,
    pub full_publishes: AtomicUsize,
    pub ref_store_writes: AtomicUsize,
    pub artifact_uploads: AtomicUsize,
    pub embedded_notification_wakes: AtomicUsize,
    pub embedded_fallback_polls: AtomicUsize,
    pub fetch_targets: StdMutex<Vec<String>>,
    pub builder_targets: StdMutex<Vec<String>>,
    pub failure_targets: StdMutex<Vec<(String, String)>>,
    pub http_trace: StdMutex<Vec<String>>,
    full_notify: Arc<tokio::sync::Notify>,
    failure_notify: Arc<tokio::sync::Notify>,
    http_notify: Arc<tokio::sync::Notify>,
}

impl Default for AdmissionTestProbe {
    fn default() -> Self {
        Self {
            before_claim: AdmissionTestBarrier::default(),
            after_claim: AdmissionTestBarrier::default(),
            fetch_entry: AdmissionTestBarrier::default(),
            builder_entry: AdmissionTestBarrier::default(),
            phase2_entry: AdmissionTestBarrier::default(),
            embedded_idle_wait: AdmissionTestBarrier::default(),
            enqueue_attempts: AtomicUsize::new(0),
            queue_inserts: AtomicUsize::new(0),
            coalesces: AtomicUsize::new(0),
            tip_probes: AtomicUsize::new(0),
            exact_fetches: AtomicUsize::new(0),
            builder_entries: AtomicUsize::new(0),
            full_publishes: AtomicUsize::new(0),
            ref_store_writes: AtomicUsize::new(0),
            artifact_uploads: AtomicUsize::new(0),
            embedded_notification_wakes: AtomicUsize::new(0),
            embedded_fallback_polls: AtomicUsize::new(0),
            fetch_targets: StdMutex::new(Vec::new()),
            builder_targets: StdMutex::new(Vec::new()),
            failure_targets: StdMutex::new(Vec::new()),
            http_trace: StdMutex::new(Vec::new()),
            full_notify: Arc::new(tokio::sync::Notify::new()),
            failure_notify: Arc::new(tokio::sync::Notify::new()),
            http_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl AdmissionTestProbe {
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
        self.probe.phase2_entry.disarm();
        self.probe.embedded_idle_wait.disarm();
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

pub async fn admission_test_before_claim() {
    if let Some(probe) = admission_test_probe() {
        probe.before_claim.wait().await;
    }
}

pub async fn admission_test_after_claim() {
    if let Some(probe) = admission_test_probe() {
        probe.after_claim.wait().await;
    }
}

fn admission_test_tip_probe() {
    if let Some(probe) = admission_test_probe() {
        probe.tip_probes.fetch_add(1, Ordering::SeqCst);
    }
}

fn admission_test_ref_store_write() {
    if let Some(probe) = admission_test_probe() {
        probe.ref_store_writes.fetch_add(1, Ordering::SeqCst);
    }
}

fn admission_test_artifact_upload() {
    if let Some(probe) = admission_test_probe() {
        probe.artifact_uploads.fetch_add(1, Ordering::SeqCst);
    }
}

async fn admission_test_fetch_entry(target: Option<&str>) {
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

async fn admission_test_builder_entry(commit: &str) {
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

async fn admission_test_phase2_entry() {
    if let Some(probe) = admission_test_probe() {
        probe.phase2_entry.wait().await;
    }
}

async fn admission_test_embedded_idle_wait() {
    if let Some(probe) = admission_test_probe() {
        probe.embedded_idle_wait.wait().await;
    }
}

fn admission_test_embedded_wake(fallback: bool) {
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

fn admission_test_full_published(commit: &str) {
    if let Some(probe) = admission_test_probe() {
        probe.full_publishes.fetch_add(1, Ordering::SeqCst);
        probe.full_notify.notify_waiters();
        tracing::debug!("admission test observed full publication for {commit}");
    }
}

fn admission_test_build_failure(commit: Option<&str>, message: &str) {
    if let Some(probe) = admission_test_probe() {
        probe
            .failure_targets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((commit.unwrap_or_default().to_string(), message.to_string()));
        probe.failure_notify.notify_waiters();
    }
}

fn admission_test_http(event: String) {
    if let Some(probe) = admission_test_probe() {
        probe
            .http_trace
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(event);
        probe.http_notify.notify_waiters();
    }
}

fn admission_test_enqueue(outcome: EnqueueOutcome) {
    let Some(probe) = admission_test_probe() else {
        return;
    };
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

#[derive(Clone)]
pub struct ServerState {
    pub cas: Cas,
    pub storage: StorageRef,
    pub repo_root: PathBuf,
    pub ref_store: Arc<dyn RefStore>,
    /// Per-repo / per-branch build configuration (ROADMAP §2a). Read at build
    /// time; absent config means today's default (shallow + full).
    pub repo_config: Arc<crate::repo_config::RepoConfigStore>,
    pub provider_registry: ProviderRegistry,
    pub broker: Arc<dyn CredentialBroker>,
    pub token_hash: Option<String>,
    /// Signing material for short-lived session tokens (`ripclone auth login`).
    /// `None` when no signing secret is available (only the token *hash* is
    /// configured); session-token issuance is then disabled.
    pub jwt: Option<Arc<crate::auth::jwt::JwtKeys>>,
    pub metrics: Arc<Metrics>,
    pub rate_limiter: RateLimiter,
    pub retention: Arc<Retention>,
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
    /// Last time each `owner/repo/branch` mirror was fetched. Used to skip a
    /// redundant `git fetch` on the resolve hot path when the mirror is fresh.
    pub mirror_freshness: Arc<std::sync::Mutex<HashMap<String, Instant>>>,
    /// How long a mirror fetch stays "fresh". Resolves within this window skip
    /// the fetch (default 60s).
    pub mirror_fresh_ttl: Duration,
    /// Short-lived cache of complete ref responses, including signed URLs.
    /// This smooths repeated clone startup latency when signing or ref-store
    /// lookup has a cold tail.
    pub ref_response_cache: Arc<std::sync::Mutex<HashMap<String, CachedRefResponse>>>,
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
            repo_config: Arc::new(crate::repo_config::RepoConfigStore::new(b.storage.clone())),
            storage: b.storage,
            repo_root: b.repo_root,
            ref_store: b.ref_store,
            provider_registry,
            broker,
            token_hash: None,
            jwt: None,
            metrics,
            rate_limiter: RateLimiter::new(60, 10.0),
            retention: b.retention,
            build_queue: queue,
            control_db: None,
            // A worker never serves the farm-out endpoints itself.
            worker_queue: None,
            build_queue_depth: Arc::new(AtomicUsize::new(0)),
            oidc_verifier: None,
            // No webhook secret here (worker has no HTTP; tests install their own).
            webhook_config: Arc::new(WebhookConfig::empty()),
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            mirror_freshness: Arc::new(std::sync::Mutex::new(HashMap::new())),
            mirror_fresh_ttl: Duration::from_secs(60),
            ref_response_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
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

fn mirror_fresh_ttl_from_env() -> Duration {
    let Some(ms) = std::env::var("RIPCLONE_TEST_MIRROR_FRESH_TTL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return Duration::from_secs(60);
    };
    tracing::warn!(
        "TEST MIRROR TTL ACTIVE: {ms}ms (RIPCLONE_TEST_MIRROR_FRESH_TTL_MS); this must NOT be set in production"
    );
    Duration::from_millis(ms)
}

#[derive(Clone)]
pub struct CachedRefResponse {
    response: RefResponse,
    inserted: Instant,
}

/// Simple in-memory token-bucket rate limiter keyed by real client IP.
/// The map is bounded and pruned periodically to avoid unbounded memory growth.
#[derive(Clone)]
pub struct RateLimiter {
    buckets: Arc<StdMutex<HashMap<String, (Instant, u32)>>>,
    max_burst: u32,
    restore_rate_per_sec: f64,
    max_entries: usize,
}

impl RateLimiter {
    pub fn new(max_burst: u32, restore_rate_per_sec: f64) -> Self {
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
            .or_insert_with(|| (now, self.max_burst));
        let elapsed = now.duration_since(entry.0).as_secs_f64();
        entry.1 =
            ((entry.1 as f64 + elapsed * self.restore_rate_per_sec) as u32).min(self.max_burst);
        entry.0 = now;
        if entry.1 == 0 {
            return false;
        }
        entry.1 -= 1;
        true
    }
}

#[derive(Deserialize)]
pub struct SyncRequest {
    #[serde(default = "default_branch_value")]
    pub branch: String,
    /// Optional git rev to build at instead of the branch tip (e.g. `HEAD~5` or
    /// a SHA). The branch is still fetched and used as the ref-store key; only
    /// the build commit is overridden. Useful for testing the incremental path
    /// (sync at HEAD~N, then HEAD~N-1) without waiting for upstream to advance.
    #[serde(default)]
    pub rev: Option<String>,
}

#[derive(Deserialize)]
pub struct AddRequest {
    #[serde(default = "default_branch_value")]
    pub branch: String,
    #[serde(default = "default_added_repo_source")]
    pub source: AddedRepoSource,
}

#[derive(Deserialize)]
pub struct RefQuery {
    /// Clonepack variant to return: "full" (all reachable history) or
    /// "shallow" (depth=1). Defaults to "full".
    #[serde(default = "default_clonepack_kind")]
    pub clonepack: String,
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
    /// Ignored on moving, rev-targeted, and non-Full requests.
    #[serde(default)]
    pub top_up: bool,
}

fn default_clonepack_kind() -> String {
    "full".to_string()
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
    pub upload_p1_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_publish_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_p1_ms: Option<u64>,
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
            upload_p1_ms: phases.upload_p1_ms,
            ref_publish_ms: phases.ref_publish_ms,
            publish_p1_ms: phases.publish_p1_ms,
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
    pub default_branch: String,
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
    /// True when the returned clonepack is a shallow (depth=1) snapshot.
    pub shallow: bool,
    /// True once the full clonepack's archive is built. The server publishes an
    /// editable clonepack first and adds the archive a moment later, so a files
    /// clone waits for this. Editable clones ignore it.
    pub archive_ready: bool,
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
    pub refs: Vec<BranchStatusEntry>,
    pub total_bytes: u64,
    pub total_unique_bytes: u64,
    pub regions: Vec<RegionStorageEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct BranchStatusEntry {
    pub branch: String,
    pub commit: String,
    pub manifest: String,
    pub bytes: u64,
    pub unique_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub built_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_status: Option<String>,
    /// RFC3339 timestamp of the ref's most recent access (build/reuse).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_accessed_at: Option<String>,
    /// True when the ref currently has non-evicted clonepack artifacts.
    pub warm: bool,
    /// True when the warm-TTL sweep is forbidden from evicting this ref.
    pub pinned: bool,
    pub depth1_ready: bool,
    pub archive_ready: bool,
    pub history: String,
}

#[derive(Serialize, Deserialize)]
pub struct RegionStorageEntry {
    pub region: String,
    pub unique_bytes: u64,
}

pub fn build_app(state: ServerState) -> Router {
    let protected = Router::new()
        // Single catch-all route for all provider-qualified repo endpoints.
        .route("/v1/repos/{*path}", get(dispatch_repos_get))
        .route("/v1/repos/{*path}", post(dispatch_repos_post))
        .route("/v1/repos/{*path}", delete(dispatch_repos_delete))
        // Refresh requires a still-valid session token (the auth layer verifies
        // the Bearer); it mints a fresh one before the current expires.
        .route("/v1/auth/refresh", post(auth_refresh_handler))
        .route("/v1/artifacts/{hash}", get(get_artifact))
        // Per-repo build config (ROADMAP §2a): read/write the repo-level config,
        // or a branch-level override via `?branch=`.
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
        .route("/v1/refs", post(ref_report_handler))
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
                branch: c.branch,
                admitted_commit: c.admitted_commit,
                admitted_default_branch: c.admitted_default_branch,
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

/// `POST /v1/refs` — farmed-out worker reports a ref write.
///
/// Auth is a signed, expiring HMAC bearer token (`Authorization: Bearer …`), not
/// the shared server token. There is no repo/job scope: the worker pool claims
/// any repo, so a scoped token cannot work. Missing / malformed / expired /
/// wrong-secret tokens return 401 and do **not** write. The write target comes
/// from the request body. On success the server's real `RefStore` (the one
/// holding DB credentials) performs the durable write.
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
        RefReport::SaveBranch { branch, info, .. } => {
            admission_test_ref_store_write();
            state
                .ref_store
                .save_branch(&repo_id, &branch, &info)
                .await
                .map(|_| RefReportResponse { updated: true })
        }
        RefReport::UpdateBuildStatus {
            branch,
            expected_commit,
            status,
            ..
        } => state
            .ref_store
            .update_build_status(&repo_id, &branch, &expected_commit, &status)
            .await
            .map(|updated| RefReportResponse { updated }),
        RefReport::DeleteBranch { branch, .. } => state
            .ref_store
            .delete_branch(&repo_id, &branch)
            .await
            .map(|_| RefReportResponse { updated: true }),
        RefReport::TouchLastAccessed {
            branch,
            expected_commit,
            ..
        } => state
            .ref_store
            .touch_last_accessed_at(&repo_id, &branch, &expected_commit)
            .await
            .map(|updated| RefReportResponse { updated }),
    };

    match result {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
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
    Query(params): Query<RefQuery>,
    headers: HeaderMap,
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    if let Some((repo_path, branch)) = path.rsplit_once("/refs/") {
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
        admission_test_http(format!("POST /v1/repos/{repo_path}/add"));
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
        admission_test_http(format!("POST /v1/repos/{repo_path}/sync"));
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

/// True if the mirror for `key` (`owner/repo/branch`) was fetched within the
/// freshness TTL, so a resolve can skip the `git fetch`.
fn mirror_is_fresh(state: &ServerState, key: &str) -> bool {
    state
        .mirror_freshness
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(key)
        .map(|t| t.elapsed() < state.mirror_fresh_ttl)
        .unwrap_or(false)
}

/// Record that the mirror for `key` was just fetched. Prunes expired entries so
/// the map stays bounded by the set of refs active within the TTL.
fn stamp_mirror_fresh(state: &ServerState, key: &str) {
    let ttl = state.mirror_fresh_ttl;
    let mut map = state
        .mirror_freshness
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    map.retain(|_, t| t.elapsed() < ttl);
    map.insert(key.to_string(), Instant::now());
}

fn invalidate_mirror_fresh(state: &ServerState, key: &str) {
    state
        .mirror_freshness
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(key);
}

const REF_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(30);

fn ref_response_cache_key(repo_id: &RepoId, branch: &str, clonepack: &str) -> String {
    format!("{}\0{branch}\0{clonepack}", repo_id.storage_key())
}

fn ref_response_cache_ttl(state: &ServerState) -> Duration {
    std::cmp::min(REF_RESPONSE_CACHE_TTL, state.mirror_fresh_ttl)
}

fn cached_ref_response(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
    clonepack: &str,
) -> Option<RefResponse> {
    let ttl = ref_response_cache_ttl(state);
    if ttl.is_zero() {
        return None;
    }
    let key = ref_response_cache_key(repo_id, branch, clonepack);
    let mut cache = state
        .ref_response_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    cache.retain(|_, cached| cached.inserted.elapsed() < ttl);
    cache.get(&key).map(|cached| cached.response.clone())
}

fn cache_ref_response(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
    clonepack: &str,
    response: &RefResponse,
) {
    let ttl = ref_response_cache_ttl(state);
    if ttl.is_zero() {
        return;
    }
    let key = ref_response_cache_key(repo_id, branch, clonepack);
    let mut cache = state
        .ref_response_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    cache.retain(|_, cached| cached.inserted.elapsed() < ttl);
    cache.insert(
        key,
        CachedRefResponse {
            response: response.clone(),
            inserted: Instant::now(),
        },
    );
}

fn invalidate_ref_response_cache(state: &ServerState, repo_id: &RepoId, branch: &str) {
    let prefix = format!("{}\0{branch}\0", repo_id.storage_key());
    let mut cache = state
        .ref_response_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    cache.retain(|key, _| !key.starts_with(&prefix));
}

fn exact_clonepack_artifacts<'a>(
    info: &'a RefInfo,
    clonepack_kind: &str,
) -> Option<&'a crate::ClonepackArtifacts> {
    match clonepack_kind {
        "shallow" => Some(&info.shallow_clonepack),
        "full" => Some(&info.full_clonepack),
        _ => None,
    }
}

fn exact_ref_info_serves_commit(info: &RefInfo, clonepack_kind: &str, commit: &str) -> bool {
    if info.build_status.as_deref() == Some(crate::remote_gc::EVICTED_BUILD_STATUS) {
        return false;
    }
    if info.commit != commit {
        return false;
    }
    matches!(
        exact_clonepack_artifacts(info, clonepack_kind),
        Some(artifacts)
            if !artifacts.manifest.is_empty()
                && !artifacts.metadata_chunk.is_empty()
                && !artifacts.idx_bundle.is_empty()
                && artifacts.commit == commit
    )
}

fn artifact_pending_response(commit: &str, branch: &str, queue_depth: usize) -> Response {
    artifact_pending_response_with_top_up(commit, branch, queue_depth, None, None)
}

fn artifact_pending_response_with_top_up(
    commit: &str,
    branch: &str,
    queue_depth: usize,
    top_up_supported: Option<bool>,
    top_up_base: Option<RefResponse>,
) -> Response {
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

/// Build an ordinary Full ref response for the carried base using only hashes
/// and ordering authenticated by that base manifest. The enclosing phase-one
/// row's top-level pack/archive lists belong to the pending target and are
/// intentionally never consulted here.
fn ref_response_from_manifest(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    manifest_hash: &str,
    manifest: &ClonepackManifest,
    storage: &crate::storage::StorageRef,
    private: bool,
) -> Option<RefResponse> {
    if manifest.default_branch.is_empty()
        || crate::validation::validate_git_rev(&manifest.default_branch).is_err()
    {
        return None;
    }
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
    let ttl = ref_signed_url_ttl(private);
    let signed = |hash: &str| signed_url(storage, ttl, hash);
    let signed_list = |hashes: &[String]| {
        let urls = hashes.iter().map(|hash| signed(hash)).collect::<Vec<_>>();
        (!urls.iter().all(Option::is_none)).then_some(urls)
    };
    let (owner, repo) = repo_id
        .github_owner_repo()
        .map(|(owner, repo)| (owner.to_string(), repo.to_string()))
        .unwrap_or_else(|| (repo_id.provider.as_str().to_string(), repo_id.path.clone()));
    Some(RefResponse {
        owner,
        repo,
        provider: provider.id.as_str().to_string(),
        host: provider.host.clone(),
        origin_url: provider.clone_url(&repo_id.path),
        branch,
        default_branch: manifest.default_branch.clone(),
        commit: manifest.commit.clone(),
        parent_commit: manifest.parent_commit.clone(),
        clonepack_manifest: manifest_hash.to_string(),
        clonepack_manifest_url: signed(manifest_hash),
        metadata_chunk_url: signed(&metadata_chunk),
        metadata_chunk,
        archive_chunk_urls: signed_list(&archive_hashes),
        head_blobs_chunk_urls: signed_list(&head_blob_hashes),
        head_blobs_idx_url: head_blobs_idx.as_deref().and_then(signed),
        pack_chunk_urls: signed_list(&pack_hashes),
        midx_url: midx.as_deref().and_then(signed),
        idx_bundle_url: signed(&idx_bundle),
        shallow: false,
        archive_ready: !manifest.archive_chunks.is_empty(),
    })
}

async fn carried_full_top_up_response(
    info: &RefInfo,
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: &str,
    pinned: &str,
    storage: &crate::storage::StorageRef,
    private: bool,
) -> Option<RefResponse> {
    // The caller supplies the same exact-row snapshot that is still building B.
    // Do not read it again here: Full(B) could publish between reads, which
    // would otherwise turn an exact-ready response into a pending top-up miss.
    if info.commit != pinned
        || info.build_status.as_deref() == Some(crate::remote_gc::EVICTED_BUILD_STATUS)
    {
        return None;
    }
    let parent = info.parent_commit.as_deref()?;
    let artifact = info.full_clonepack.commit.as_str();
    let manifest_hash = info.full_clonepack.manifest.as_str();
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

/// Returns true when the exact historical row matches the requested commit and
/// is evicted. Historical lookups remain schedulable after the moving branch
/// advances, but never read the moving row as an artifact source.
async fn branch_ref_is_evicted_for_commit(
    ref_store: &Arc<dyn RefStore>,
    repo_id: &RepoId,
    branch: &str,
    commit: &str,
) -> bool {
    let key = exact_ref_store_key(branch, commit);
    matches!(
        ref_store.load_branch(repo_id, &key).await.ok().flatten(),
        Some(info)
            if info.commit == commit
                && info.build_status.as_deref()
                    == Some(crate::remote_gc::EVICTED_BUILD_STATUS)
    )
}

fn full_clonepack_pending_for_tip(info: &RefInfo, clonepack_kind: &str, commit: &str) -> bool {
    // This metadata-only shortcut is exclusively for a phase-one row whose
    // background Full build is already active. Eviction also counts as pending,
    // but it has no active worker: it must fall through to the source/rebuild
    // path below so an initial GET admits work before it pins.
    clonepack_kind != "shallow"
        && info.commit == commit
        && info.build_status.as_deref() == Some(BUILDING_FULL_HISTORY)
        && !exact_ref_info_serves_commit(info, clonepack_kind, commit)
}

/// `build_status` set on a phase-1 row while the full history + archive build
/// runs in the background.
pub(crate) const BUILDING_FULL_HISTORY: &str = "full history building";

/// Load stored artifacts for the resolved commit. Returns the ref-store key
/// where the artifacts live alongside the `RefInfo`, so callers can atomically
/// bump `last_accessed_at` on the same row they served from.
async fn load_ref_info_for_resolved_commit(
    ref_store: &Arc<dyn RefStore>,
    repo_id: &RepoId,
    effective_branch: &str,
    commit: &str,
    clonepack_kind: &str,
) -> Option<(String, RefInfo)> {
    let exact_key = exact_ref_store_key(effective_branch, commit);
    ref_store
        .load_branch(repo_id, &exact_key)
        .await
        .ok()
        .flatten()
        .filter(|info| exact_ref_info_serves_commit(info, clonepack_kind, commit))
        .map(|info| (exact_key, info))
}

/// True when a rev-targeted request has a build in flight for `commit` whose
/// selected clonepack variant is not published yet.
///
/// The two-phase sync publishes the depth-1 clonepack first and the full
/// history + archive a moment later. For a branch-tip request the ref endpoint
/// answers `202` during that window so the client's poll loop waits. Rev
/// requests (`sync --at REV` / `clone --at REV`) used to skip that check and
/// fall through to a `200` carrying an empty full-clonepack manifest, so a
/// `clone --at REV` issued right after `sync --at REV` failed with "run sync
/// first" — the exact pairing the CLI docs recommend. Answer `202` instead and
/// let the client wait, same as the tip path. No enqueue: the sync that created
/// this row already has a worker on it.
async fn rev_build_pending_for_commit(
    ref_store: &Arc<dyn RefStore>,
    repo_id: &RepoId,
    effective_branch: &str,
    commit: &str,
    clonepack_kind: &str,
) -> bool {
    // Only the "still building" status, never EVICTED: an evicted rev has no
    // worker on it and is handled by the enqueue-a-rebuild path, so answering
    // 202 here would leave the client polling forever.
    let building = |info: &RefInfo| {
        info.commit == commit
            && info.build_status.as_deref() == Some(BUILDING_FULL_HISTORY)
            && !exact_ref_info_serves_commit(info, clonepack_kind, commit)
    };
    let key = exact_ref_store_key(effective_branch, commit);
    matches!(ref_store.load_branch(repo_id, &key).await, Ok(Some(info)) if building(&info))
}

async fn get_ref_inner(
    repo_id: RepoId,
    provider: ProviderInstance,
    branch: String,
    params: RefQuery,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if !matches!(params.clonepack.as_str(), "full" | "shallow") {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "clonepack must be full or shallow".to_string(),
            }),
        )
            .into_response();
    }
    if let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(&branch)) {
        return resp;
    }
    if let Some(rev) = params.rev.as_deref()
        && let Some(resp) = validation::reject_if_invalid(|| validation::validate_git_rev(rev))
    {
        return resp;
    }
    if let Some(pinned) = params.pinned.as_deref()
        && let Some(resp) = validation::reject_if_invalid(|| validation::validate_object_id(pinned))
    {
        return resp;
    }
    if let Some(pinned) = params.pinned.as_deref() {
        admission_test_http(format!(
            "GET /v1/repos/{}/refs/{branch}?pinned={pinned}&clonepack={}",
            repo_id.storage_key(),
            params.clonepack
        ));
    }
    match repo_is_added(&state, &repo_id).await {
        Ok(true) => {}
        Ok(false) => return repo_not_added_response(),
        Err(resp) => return resp,
    }
    state.metrics.record_ref_lookup();
    let key = format!("{}/{}", repo_id.storage_key(), branch);
    // Optional build-commit override: resolve this rev instead of the branch tip
    // so a clone can fetch the artifacts a `sync?rev=...` built. The response
    // cache is bypassed for rev requests (a testing path, low volume).
    let resolve_target = params.rev.clone().unwrap_or_else(|| branch.clone());

    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let request_token = upstream_token_from_headers(&headers);
    let credential = match state
        .broker
        .fetch_credential(&repo_id, request_token.as_ref())
    {
        Ok(c) => c,
        Err(e) => return credential_error_response(e),
    };

    // AU1: authorize the caller for this repo BEFORE the cache-hit return below,
    // so a cached private repo is never served to a caller without access.
    let private =
        match authorize_repo_read(&state, &provider, &repo_id, credential.as_ref(), &headers).await
        {
            Ok(p) => p,
            Err(resp) => return resp,
        };

    if let Some(pinned) = params.pinned.as_deref() {
        let exact_key = exact_ref_store_key(&branch, pinned);
        let exact_info = match state.ref_store.load_branch(&repo_id, &exact_key).await {
            Ok(info) => info.filter(|info| info.commit == pinned),
            Err(e) => {
                state.metrics.record_error();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("pinned ref lookup failed: {e}"),
                    }),
                )
                    .into_response();
            }
        };
        if let Some(info) = exact_info.as_ref()
            && let Some(failure) = info
                .build_status
                .as_deref()
                .and_then(|status| status.strip_prefix("failed: "))
        {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ErrorResponse {
                    error: format!("exact pinned commit {pinned} is unavailable: {failure}"),
                }),
            )
                .into_response();
        }
        let info = match exact_info {
            Some(info) if exact_ref_info_serves_commit(&info, &params.clonepack, pinned) => info,
            pending_info => {
                if params.rev.is_none() {
                    let size_bytes = enqueue_size_bytes(&state, &repo_id, &branch).await;
                    let job = BuildJob {
                        repo_id: repo_id.clone(),
                        branch: branch.clone(),
                        admitted_commit: pinned.to_string(),
                        admitted_default_branch: pending_info
                            .as_ref()
                            .filter(|info| !info.default_branch.is_empty())
                            .map(|info| info.default_branch.clone()),
                        credential: credential.clone(),
                        // Recovery is for the requested commit only. Do not
                        // launch the moving-tip freshness probe after it finishes.
                        size_bytes,
                    };
                    if let Err(error) = enqueue_direct_build(&state, job, false).await {
                        state.metrics.record_error();
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(ExactRevisionUnavailableResponse {
                                error: format!(
                                    "exact pinned commit {pinned} is unavailable: {error}"
                                ),
                                commit: pinned.to_string(),
                                branch: branch.clone(),
                            }),
                        )
                            .into_response();
                    }
                }
                if params.top_up && params.clonepack == "full" {
                    let base = match pending_info.as_ref() {
                        Some(info) => {
                            carried_full_top_up_response(
                                info,
                                &repo_id,
                                &provider,
                                &branch,
                                pinned,
                                &state.storage,
                                private,
                            )
                            .await
                        }
                        None => None,
                    };
                    return artifact_pending_response_with_top_up(
                        pinned,
                        &branch,
                        1,
                        Some(true),
                        base,
                    );
                }
                return artifact_pending_response(pinned, &branch, 1);
            }
        };
        let response = ref_response(
            &repo_id,
            &provider,
            branch.clone(),
            &info,
            &state.storage,
            &params.clonepack,
            private,
        );
        if response.commit != pinned || response.clonepack_manifest.is_empty() {
            return artifact_pending_response(pinned, &branch, 1);
        }
        if let Err(e) = state
            .ref_store
            .touch_last_accessed_at(&repo_id, &exact_key, &info.commit)
            .await
        {
            warn!(
                "failed to touch last_accessed_at for pinned {}@{}: {e:#}",
                exact_key, info.commit
            );
        }
        // This exact authenticated read is the readiness
        // boundary for a build that may have completed in another process.
        // Retire any unpinned response cached before admission so the caller's
        // next ordinary HEAD/branch clone cannot fall back to the old commit.
        invalidate_ref_response_cache(&state, &repo_id, &branch);
        invalidate_ref_response_cache(&state, &repo_id, "HEAD");
        return (StatusCode::OK, Json(response)).into_response();
    }

    // Serialize syncs for this repo so concurrent fetches do not corrupt the
    // bare mirror directory. Acquiring the lock also means any in-progress sync
    // for this repo has finished by the time we proceed.
    let fresh_key = format!("{}/{}", repo_id.storage_key(), branch);
    let lock = repo_lock(&state.sync_locks, &repo_id).await;
    let _guard = lock.lock().await;
    let cache_variant = params.clonepack.clone();
    if params.rev.is_none()
        && let Some(resp) = cached_ref_response(&state, &repo_id, &branch, &cache_variant)
    {
        // Cached responses remain sourced from their exact row. Never refresh
        // an unrelated moving projection.
        let served_key = exact_ref_store_key(&resp.branch, &resp.commit);
        if let Err(e) = state
            .ref_store
            .touch_last_accessed_at(&repo_id, &served_key, &resp.commit)
            .await
        {
            warn!(
                "failed to touch last_accessed_at for cached {}@{}: {e:#}",
                repo_id.storage_key(),
                resp.branch
            );
        }
        return (StatusCode::OK, Json(resp)).into_response();
    }
    // Skip the `git fetch` when the mirror was refreshed within the TTL — by a
    // recent resolve, or by the sync we just waited on while holding the lock.
    if !mirror_is_fresh(&state, &fresh_key) {
        if let Err(e) = ensure_mirror(
            &mirror_dir,
            &provider,
            &repo_id,
            &branch,
            params.rev.as_deref(),
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
        stamp_mirror_fresh(&state, &fresh_key);
    }
    drop(_guard);

    // Resolve HEAD to the default branch for artifact lookup and the response.
    // The client may request refs/HEAD; we still need to hand back the artifacts
    // stored under the concrete default branch name.
    let detected_default_branch = git::default_branch(&mirror_dir)
        .ok()
        .filter(|branch| !branch.is_empty() && branch != "HEAD");
    let default_branch = if branch != "HEAD" {
        detected_default_branch.unwrap_or_else(|| "HEAD".to_string())
    } else {
        let detected_row = match detected_default_branch {
            // An explicit historical selector resolves against the mirror, so
            // its readable symbolic HEAD is enough to identify the concrete
            // exact-result key. This does not promote internal metadata: if the
            // mirror cannot identify HEAD, the public-row fallback below still
            // rejects an exact-only layout.
            Some(candidate) if params.rev.is_some() => Some(candidate),
            Some(candidate) => state
                .ref_store
                .load_branch(&repo_id, &candidate)
                .await
                .ok()
                .flatten()
                .filter(|info| !info.internal_exact_result)
                .map(|_| candidate),
            None => None,
        };
        if let Some(branch_name) = detected_row {
            branch_name
        } else {
            // A mirror may have the concrete branch ref without a readable
            // symbolic HEAD. Recover the exact default branch from the
            // already-persisted ref metadata; this is a metadata-only fallback
            // and does not probe or mutate the source.
            let mut candidates = std::collections::BTreeSet::new();
            if let Ok(keys) = state.ref_store.list_branches(&repo_id).await {
                for key in keys {
                    if key == "HEAD" {
                        continue;
                    }
                    if let Ok(Some(info)) = state.ref_store.load_branch(&repo_id, &key).await
                        && !info.internal_exact_result
                        && key == info.default_branch
                        && !info.default_branch.is_empty()
                        && info.default_branch != "HEAD"
                        && validation::validate_git_rev(&info.default_branch).is_ok()
                    {
                        candidates.insert(info.default_branch);
                    }
                }
            }
            match candidates.len() {
                1 => candidates.into_iter().next().unwrap(),
                0 => {
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(ErrorResponse {
                            error: "cannot determine a current public default branch".to_string(),
                        }),
                    )
                        .into_response();
                }
                _ => {
                    return (
                        StatusCode::CONFLICT,
                        Json(ErrorResponse {
                            error: "ambiguous current public default branch".to_string(),
                        }),
                    )
                        .into_response();
                }
            }
        }
    };
    let effective_branch = if branch == "HEAD" {
        default_branch.clone()
    } else {
        branch.clone()
    };

    // The admission path canonicalizes HEAD to the exact advertised default
    // branch. Resolve the same concrete branch here as well: a mirror may
    // carry the branch ref without a local symbolic HEAD (notably for valid
    // delimiter-bearing default branch names).
    let resolve_target2 = if params.rev.is_none() && branch == "HEAD" {
        effective_branch.clone()
    } else {
        resolve_target.clone()
    };
    let mirror_dir2 = mirror_dir.clone();
    match tokio::task::spawn_blocking(move || git::resolve_commit(&mirror_dir2, &resolve_target2))
        .await
    {
        Ok(Ok(commit)) => {
            // Load only the exact commit-keyed result. Never source artifacts
            // from a moving row or another branch/revision key.
            let resolved = load_ref_info_for_resolved_commit(
                &state.ref_store,
                &repo_id,
                &effective_branch,
                &commit,
                &params.clonepack,
            )
            .await;
            let ref_key = resolved
                .as_ref()
                .map(|(k, _)| k.clone())
                .unwrap_or_default();
            let info = resolved.map(|(_, i)| i).unwrap_or_else(|| RefInfo {
                internal_exact_result: true,
                require_matching_commit: false,
                moving_publication_predecessors: Vec::new(),
                moving_admission_successors: Vec::new(),
                commit: commit.clone(),
                parent_commit: None,
                default_branch: default_branch.clone(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                head_blobs_idx: String::new(),
                head_blobs_chunks: Vec::new(),
                packs: Vec::new(),
                prebuilt_index: String::new(),
                archive: String::new(),
                manifest: String::new(),
                archive_chunks: Vec::new(),
                full_clonepack: crate::ClonepackArtifacts::default(),
                shallow_clonepack: crate::ClonepackArtifacts::default(),
                history_levels: Vec::new(),
                head_base_commit: String::new(),
                head_base_packs: Vec::new(),
                archive_frames: Vec::new(),
                build_status: None,
                build_ms: None,
                synced_at: None,
                last_accessed_at: None,
                generation: None,
                warm_pinned: false,
            });
            let evicted_for_rev = params.rev.is_some()
                && branch_ref_is_evicted_for_commit(
                    &state.ref_store,
                    &repo_id,
                    &effective_branch,
                    &commit,
                )
                .await;
            let is_evicted =
                info.build_status.as_deref() == Some(crate::remote_gc::EVICTED_BUILD_STATUS);
            // A rev request whose selected clonepack is still building must wait
            // too, otherwise the documented `sync --at REV` → `clone --at REV`
            // pairing races the background full-history phase.
            let pending_for_rev = match params.rev.as_deref() {
                Some(_) if !evicted_for_rev => {
                    rev_build_pending_for_commit(
                        &state.ref_store,
                        &repo_id,
                        &effective_branch,
                        &commit,
                        &params.clonepack,
                    )
                    .await
                }
                _ => false,
            };
            let exact_variant_pending =
                !exact_ref_info_serves_commit(&info, &params.clonepack, &commit);
            if let Some(failure) = info
                .build_status
                .as_deref()
                .and_then(|status| status.strip_prefix("failed: "))
            {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(ErrorResponse {
                        error: format!("exact commit {commit} is unavailable: {failure}"),
                    }),
                )
                    .into_response();
            }
            if (params.rev.is_none()
                && full_clonepack_pending_for_tip(&info, &params.clonepack, &commit))
                || pending_for_rev
                || evicted_for_rev
                || exact_variant_pending
            {
                // Evicted refs have no artifacts to serve; enqueue a rebuild so a
                // client that only polls GET will eventually see a 200. A ref that
                // is simply still building already has a worker on it, so do not
                // enqueue a duplicate.
                if params.rev.is_none() || is_evicted || evicted_for_rev {
                    // Build under the concrete branch selected before admission so
                    // every status and artifact write addresses :branch#commit.
                    let size_bytes = enqueue_size_bytes(&state, &repo_id, &branch).await;
                    let job = BuildJob {
                        repo_id: repo_id.clone(),
                        branch: effective_branch.clone(),
                        admitted_commit: commit.clone(),
                        admitted_default_branch: Some(default_branch.clone()),
                        credential: credential.clone(),
                        size_bytes,
                    };
                    let moving_authorized = params.rev.is_none();
                    if let Err(error) = enqueue_direct_build(&state, job, moving_authorized).await {
                        warn!(
                            "failed to enqueue rebuild for evicted {}@{}: {error}",
                            repo_id.storage_key(),
                            effective_branch
                        );
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(ExactRevisionUnavailableResponse {
                                error,
                                commit: commit.clone(),
                                branch: effective_branch.clone(),
                            }),
                        )
                            .into_response();
                    }
                }
                let queue_depth = state.build_queue_depth.load(Ordering::Relaxed);
                return artifact_pending_response(&commit, &effective_branch, queue_depth);
            }
            // A successful read of an existing ref keeps it warm: atomically bump
            // last_accessed_at without clobbering a concurrent sync's newer data.
            if !ref_key.is_empty() {
                if let Err(e) = state
                    .ref_store
                    .touch_last_accessed_at(&repo_id, &ref_key, &info.commit)
                    .await
                {
                    warn!(
                        "failed to touch last_accessed_at for {}@{}: {e:#}",
                        repo_id.storage_key(),
                        ref_key
                    );
                }
            }
            let resp = ref_response(
                &repo_id,
                &provider,
                effective_branch.clone(),
                &info,
                &state.storage,
                &params.clonepack,
                private,
            );
            if matches!(info.build_status.as_deref(), None | Some("done")) && params.rev.is_none() {
                cache_ref_response(&state, &repo_id, &effective_branch, &cache_variant, &resp);
            }
            (StatusCode::OK, Json(resp)).into_response()
        }
        _ => {
            state.metrics.record_error();
            // Distinguish an empty upstream (no commits at all) from a genuinely
            // missing branch/ref: the former resolves to nothing because there is
            // nothing to clone, and deserves an actionable message rather than a
            // bare "ref not found".
            if git::is_empty_repo(&mirror_dir).unwrap_or(false) {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: "repository has no commits (nothing to clone)".to_string(),
                    }),
                )
                    .into_response();
            }
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("ref not found: {}", key),
                }),
            )
                .into_response()
        }
    }
}

/// TTL for signed chunk URLs returned in ref responses. Public repos get a long
/// window (20 min) so slow clones and large archives finish. Private repos get a
/// short window (5 min) so a leaked signed URL — or a caller who later loses
/// GitHub access — only works briefly; this is the revocation window for the
/// direct-to-storage path that bypasses the gateway. The cloud gateway tags the
/// request with `X-Ripclone-Visibility`; absent (e.g. a self-hosted client
/// talking to the backend directly) means the public TTL, but an unrecognized
/// value fails closed to the private TTL.
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

fn explicit_test_mode(value: Option<&std::ffi::OsStr>) -> bool {
    value == Some(std::ffi::OsStr::new("1"))
}

async fn wait_test_phase_two_barrier(commit: &str) -> Result<()> {
    if !explicit_test_mode(std::env::var_os("RIPCLONE_TESTING").as_deref()) {
        return Ok(());
    }
    admission_test_phase2_entry().await;
    let Some(dir) = std::env::var_os("RIPCLONE_TEST_PHASE2_BARRIER_DIR").map(PathBuf::from) else {
        return Ok(());
    };
    if let Some(target) = std::env::var_os("RIPCLONE_TEST_PHASE2_BARRIER_COMMIT")
        && target.to_str() != Some(commit)
    {
        return Ok(());
    }
    std::fs::create_dir_all(&dir).context("create test phase-two barrier directory")?;
    std::fs::write(dir.join("entered"), format!("{commit}\n"))
        .context("signal test phase-two barrier")?;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !dir.join("proceed").exists() {
        if Instant::now() >= deadline {
            anyhow::bail!("test phase-two barrier was not released within 60 seconds");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

fn ref_response(
    repo_id: &RepoId,
    provider: &ProviderInstance,
    branch: String,
    info: &RefInfo,
    storage: &crate::storage::StorageRef,
    clonepack_kind: &str,
    private: bool,
) -> RefResponse {
    let ttl = ref_signed_url_ttl(private);
    let artifacts = exact_clonepack_artifacts(info, clonepack_kind)
        .expect("clonepack kind is validated before response construction");
    let clonepack_manifest = artifacts.manifest.clone();
    let metadata_chunk = artifacts.metadata_chunk.clone();

    let clonepack_manifest_url = signed_url(storage, ttl, &clonepack_manifest);
    let metadata_chunk_url = signed_url(storage, ttl, &metadata_chunk);
    let archive_chunk_urls = if info.archive_chunks.is_empty() {
        None
    } else {
        let urls: Vec<Option<String>> = info
            .archive_chunks
            .iter()
            .map(|h| signed_url(storage, ttl, h))
            .collect();
        if urls.iter().all(|u| u.is_none()) {
            None
        } else {
            Some(urls)
        }
    };

    let head_blobs_chunk_urls = if info.head_blobs_chunks.is_empty() {
        None
    } else {
        let urls: Vec<Option<String>> = info
            .head_blobs_chunks
            .iter()
            .map(|h| signed_url(storage, ttl, h))
            .collect();
        if urls.iter().all(|u| u.is_none()) {
            None
        } else {
            Some(urls)
        }
    };
    let head_blobs_idx_url = signed_url(storage, ttl, &info.head_blobs_idx);

    // Sign each editable pack so the client fetches it straight from
    // object storage. `None` entries (e.g. local backend) fall back to the
    // gateway. Ordered to match the manifest's `packs` list.
    let pack_chunk_urls = if info.packs.is_empty() {
        None
    } else {
        let packs: Vec<Option<String>> = info
            .packs
            .iter()
            .map(|p| signed_url(storage, ttl, &p.pack))
            .collect();
        if packs.iter().all(Option::is_none) {
            None
        } else {
            Some(packs)
        }
    };

    // Sign the pre-built MIDX for the selected variant so the client installs it
    // directly instead of running `git multi-pack-index write`.
    let midx_url = signed_url(storage, ttl, &artifacts.midx);
    // Sign the idx bundle so the client fetches all idx in one GET.
    let idx_bundle_url = signed_url(storage, ttl, &artifacts.idx_bundle);

    let (owner, repo) = repo_id
        .github_owner_repo()
        .map(|(o, r)| (o.to_string(), r.to_string()))
        .unwrap_or_else(|| (repo_id.provider.as_str().to_string(), repo_id.path.clone()));
    let origin_url = provider.clone_url(&repo_id.path);
    RefResponse {
        owner,
        repo,
        provider: provider.id.as_str().to_string(),
        host: provider.host.clone(),
        origin_url,
        branch,
        default_branch: info.default_branch.clone(),
        commit: artifacts.commit.clone(),
        parent_commit: info.parent_commit.clone(),
        clonepack_manifest,
        clonepack_manifest_url,
        metadata_chunk,
        metadata_chunk_url,
        archive_chunk_urls,
        head_blobs_chunk_urls,
        head_blobs_idx_url,
        pack_chunk_urls,
        midx_url,
        idx_bundle_url,
        shallow: clonepack_kind == "shallow",
        archive_ready: !info.archive_chunks.is_empty(),
    }
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
    clonepack_kind: &str,
    private: bool,
    status: impl Into<String>,
) -> SyncResponse {
    SyncResponse {
        ref_info: ref_response(
            repo_id,
            provider,
            branch,
            info,
            storage,
            clonepack_kind,
            private,
        ),
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
    let branches = state.ref_store.list_branches(repo_id).await?;
    let mut refs = Vec::new();
    let mut unique_chunks: HashMap<String, u64> = HashMap::new();

    for branch in branches {
        let Some(info) = state.ref_store.load_branch(repo_id, &branch).await? else {
            continue;
        };

        let manifest_hashes = collect_manifest_hashes(&info);
        let is_evicted =
            info.build_status.as_deref() == Some(crate::remote_gc::EVICTED_BUILD_STATUS);
        if manifest_hashes.is_empty() && info.history_levels.is_empty() && !is_evicted {
            continue;
        }

        let mut ref_bytes = 0u64;

        // Evicted refs have no reachable artifacts; do not try to read manifest
        // or pack hashes that the GC phase already deleted.
        if !is_evicted {
            // Manifest-based clonepack variants (shallow and full). Each
            // manifest is itself a stored artifact and references chunks.
            for manifest_hash in manifest_hashes {
                // Read the manifest from the authoritative storage backend rather
                // than the local CAS, because remote backends remove local copies
                // after upload to save disk.
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

            // LSM sealed history levels: each level stores its own pack/idx hashes.
            for level in &info.history_levels {
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

        if info.internal_exact_result {
            continue;
        }

        let built_at = info.synced_at.and_then(|secs| {
            chrono::DateTime::from_timestamp(secs as i64, 0).map(|dt| dt.to_rfc3339())
        });
        let last_accessed_at = info.last_accessed_at.and_then(|secs| {
            chrono::DateTime::from_timestamp(secs as i64, 0).map(|dt| dt.to_rfc3339())
        });

        // Report the primary manifest: prefer full, then shallow.
        // Evicted refs have no artifacts, so the manifest hash is meaningless.
        let primary_manifest = if is_evicted {
            String::new()
        } else if !info.full_clonepack.manifest.is_empty() {
            info.full_clonepack.manifest.clone()
        } else if !info.shallow_clonepack.manifest.is_empty() {
            info.shallow_clonepack.manifest.clone()
        } else {
            String::new()
        };

        let warm = !is_evicted && ref_bytes > 0;
        let depth1_ready = !is_evicted && !info.shallow_clonepack.manifest.is_empty();
        let archive_ready = !is_evicted && !info.archive_chunks.is_empty();
        let history = if is_evicted {
            "cold"
        } else if info
            .build_status
            .as_deref()
            .is_some_and(|s| s.starts_with("failed: "))
        {
            "failed"
        } else if !info.full_clonepack.manifest.is_empty() {
            "ready"
        } else {
            "building"
        }
        .to_string();

        // Public forks of public projects receive zero unique-byte allocation;
        // every other ref reports its logical bytes for now.
        let is_public_fork = public && fork_of.is_some();
        let branch_unique_bytes = if is_public_fork { 0 } else { ref_bytes };

        refs.push(BranchStatusEntry {
            branch,
            commit: info.commit,
            manifest: primary_manifest,
            bytes: ref_bytes,
            unique_bytes: branch_unique_bytes,
            built_at,
            build_ms: info.build_ms,
            build_status: info.build_status.clone(),
            last_accessed_at,
            warm,
            pinned: info.warm_pinned,
            depth1_ready,
            archive_ready,
            history,
        });
    }

    refs.sort_by(|a, b| a.branch.cmp(&b.branch));
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
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&params.branch))
    {
        return resp;
    }
    if params.rev.is_some() {
        return sync_repo_at_revision(repo_id, provider, params, headers, state).await;
    }
    let added_repo = match state.ref_store.load_added_repo(&repo_id).await {
        Ok(Some(added)) => added,
        Ok(None) => return repo_not_added_response(),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("added repo lookup failed: {e}"),
                }),
            )
                .into_response();
        }
    };

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
    admission_test_tip_probe();
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
    let admitted_default_branch = tip.default_branch.clone();
    let commit = tip.commit;
    let effective_branch = if requested_branch == "HEAD" {
        tip.default_branch
            .filter(|branch| !branch.is_empty())
            .unwrap_or_else(|| requested_branch.clone())
    } else {
        requested_branch.clone()
    };

    // Admission is intentionally an exact-row read only. A ready unchanged
    // sync after its one probe has no queue, source, builder, or metadata
    // mutation side effect.
    let exact_key = exact_ref_store_key(&effective_branch, &commit);
    let loaded_exact = match state.ref_store.load_branch(&repo_id, &exact_key).await {
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
    let ready = loaded_exact.as_ref().filter(|info| {
        exact_ref_info_serves_commit(info, "full", &commit)
            && !info
                .build_status
                .as_deref()
                .is_some_and(|status| status.starts_with("failed: "))
    });
    if let Some(info) = ready {
        state.metrics.record_sync(start.elapsed());
        let resp = sync_response_without_storage_read(
            &repo_id,
            &provider,
            effective_branch,
            info,
            &state.storage,
            "full",
            private,
            "no-op",
        );
        return (StatusCode::OK, Json(resp)).into_response();
    }

    let admitted_branch = effective_branch.clone();
    let moving_info = state
        .ref_store
        .load_branch(&repo_id, &effective_branch)
        .await
        .ok()
        .flatten();
    let prior_size_bytes = moving_info.as_ref().and_then(|info| {
        let bytes = crate::queue::prior_clonepack_bytes(info);
        (bytes > 0).then_some(bytes)
    });
    let size_bytes =
        crate::queue::resolve_job_size_bytes(prior_size_bytes, added_repo.repo_size_bytes);
    // A prior unpinned HEAD/branch clone may have populated this process's
    // caches with A. Clear those read caches before admission so the response
    // boundary remains exactly the queue call; later reads can observe B from
    // a separate worker instead of continuing to serve A.
    invalidate_ref_response_cache(&state, &repo_id, &admitted_branch);
    invalidate_ref_response_cache(&state, &repo_id, "HEAD");
    state.ref_store.invalidate(&repo_id, &admitted_branch).await;
    state.ref_store.invalidate(&repo_id, "HEAD").await;
    invalidate_mirror_fresh(
        &state,
        &format!("{}/{}", repo_id.storage_key(), admitted_branch),
    );
    invalidate_mirror_fresh(&state, &format!("{}/HEAD", repo_id.storage_key()));
    let job = BuildJob {
        repo_id: repo_id.clone(),
        branch: admitted_branch.clone(),
        admitted_commit: commit.clone(),
        admitted_default_branch,
        credential,
        size_bytes,
    };
    let outcome = match enqueue_admitted_build(&state, job, true).await {
        Ok(outcome) => outcome,
        Err(error) => {
            state.metrics.record_error();
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse { error }),
            )
                .into_response();
        }
    };
    (
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
        .into_response()
}

/// First-class exact-revision sync used by `sync --at REV`.
async fn sync_repo_at_revision(
    repo_id: RepoId,
    provider: ProviderInstance,
    params: SyncRequest,
    headers: HeaderMap,
    state: ServerState,
) -> Response {
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&params.branch))
    {
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
    // polls use only the selected object id. A concrete object id with a known
    // branch can skip this fetch entirely; HEAD on a cold mirror needs one fetch
    // to recover the concrete default branch used by the exact-result key.
    let local_default_branch = (branch == "HEAD")
        .then(|| git::default_branch(&mirror_dir).ok())
        .flatten()
        .filter(|candidate| !candidate.is_empty() && candidate != "HEAD");
    let selector_is_exact = crate::validation::validate_object_id(&at_rev).is_ok();
    let needs_resolution_fetch =
        !selector_is_exact || (branch == "HEAD" && local_default_branch.is_none());
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
        if branch == "HEAD"
            && let Some(default_branch) = local_default_branch
        {
            branch = default_branch;
        }
        at_rev.clone()
    };
    let at_rev = selected_commit;

    // A retry after phase one resolves entirely from the local mirror and the
    // exact result row. Full readiness is the selected artifact's identity,
    // never the presence of a phase-one row or its top-level fields.
    let resolve_exact_target = || {
        let effective_branch = if branch == "HEAD" {
            git::default_branch(&mirror_dir)
                .ok()
                .filter(|candidate| !candidate.is_empty())?
        } else {
            branch.clone()
        };
        Some((effective_branch, at_rev.clone()))
    };
    if let Some((effective_branch, commit)) = resolve_exact_target() {
        let exact_key = exact_ref_store_key(&effective_branch, &commit);
        match state.ref_store.load_branch(&repo_id, &exact_key).await {
            Ok(Some(info)) if exact_ref_info_serves_commit(&info, "full", &commit) => {
                if let Err(error) = state
                    .ref_store
                    .touch_last_accessed_at(&repo_id, &exact_key, &commit)
                    .await
                {
                    warn!(
                        "failed to touch historical exact result {}@{}: {error:#}",
                        repo_id.storage_key(),
                        exact_key
                    );
                }
                state.metrics.record_sync(start.elapsed());
                let response = sync_response_without_storage_read(
                    &repo_id,
                    &provider,
                    effective_branch,
                    &info,
                    &state.storage,
                    "full",
                    private,
                    "no-op",
                );
                return (StatusCode::OK, Json(response)).into_response();
            }
            Ok(Some(info)) => {
                if let Some(failure) = info
                    .build_status
                    .as_deref()
                    .and_then(|status| status.strip_prefix("failed: "))
                {
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(ErrorResponse {
                            error: format!("exact commit {commit} is unavailable: {failure}"),
                        }),
                    )
                        .into_response();
                }
            }
            Ok(None) => {}
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
        }
    }

    // Exact revisions use the same admitted-SHA lane as ordinary requests.
    // Queue implementations coalesce the same repo/branch/commit identity.
    let size_bytes = enqueue_size_bytes(&state, &repo_id, &branch).await;
    let job = BuildJob {
        repo_id: repo_id.clone(),
        branch: branch.clone(),
        admitted_commit: at_rev.clone(),
        admitted_default_branch: Some(branch.clone()),
        credential,
        size_bytes,
    };
    match enqueue_admitted_build(&state, job, false).await {
        Ok(_) => artifact_pending_response(&at_rev, &branch, state.build_queue.depth().await),
        Err(error) => {
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
    if let Some(resp) =
        validation::reject_if_invalid(|| validation::validate_git_rev(&params.branch))
    {
        return resp;
    }

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
            branch: params.branch,
            rev: None,
        },
        headers,
        state,
    )
    .await
}

async fn remove_added_repo_inner(repo_id: RepoId, state: ServerState) -> Response {
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

/// Byte size for size-class classification at enqueue. Prefers re-sync data
/// (prior clonepack byte total already on the ref) over the tiered-add preflight
/// repo size stored on [`AddedRepo`]. Unknown → `None` → largest class. No new
/// API calls here — both signals are data already in hand.
async fn enqueue_size_bytes(state: &ServerState, repo_id: &RepoId, branch: &str) -> Option<u64> {
    let prior = match state.ref_store.load_branch(repo_id, branch).await {
        Ok(Some(info)) => {
            let n = crate::queue::prior_clonepack_bytes(&info);
            if n > 0 { Some(n) } else { None }
        }
        _ => None,
    };
    let preflight = match state.ref_store.load_added_repo(repo_id).await {
        Ok(Some(added)) => added.repo_size_bytes,
        _ => None,
    };
    crate::queue::resolve_job_size_bytes(prior, preflight)
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

/// Admit one exact ordinary-tip job. The local marker spans the whole
/// durable job; the database active-key constraint covers queued and claimed
/// rows.
async fn enqueue_admitted_build(
    state: &ServerState,
    job: BuildJob,
    moving_authorized: bool,
) -> Result<EnqueueOutcome, String> {
    crate::validation::validate_object_id(&job.admitted_commit)
        .map_err(|e| format!("invalid admitted commit: {e}"))?;
    let Some(admission) = prepare_exact_admission(state, &job, moving_authorized).await? else {
        state.metrics.record_build_accepted();
        admission_test_enqueue(EnqueueOutcome::Coalesced);
        return Ok(EnqueueOutcome::Coalesced);
    };
    if let Some(control) = &state.control_db {
        state.metrics.record_build_queued();
        let tail = admission
            .tail
            .as_ref()
            .map(|(branch, info)| (branch.as_str(), info));
        return match control
            .admit_exact_and_job(&job, &admission.exact_branch, &admission.pending, tail)
            .await
        {
            Ok(enqueued) => {
                state.metrics.record_build_accepted();
                admission_test_enqueue(enqueued.outcome);
                Ok(enqueued.outcome)
            }
            Err(error) => {
                state.metrics.record_build_rejected();
                Err(format!("durable admission failed: {error:#}"))
            }
        };
    }
    state
        .ref_store
        .save_branch(&job.repo_id, &admission.exact_branch, &admission.pending)
        .await
        .map_err(|error| format!("exact admission persistence failed: {error}"))?;
    if let Some((tail_branch, tail_info)) = admission.tail {
        state
            .ref_store
            .save_branch(&job.repo_id, &tail_branch, &tail_info)
            .await
            .map_err(|error| format!("ordinary admission link failed: {error}"))?;
    }
    state.metrics.record_build_queued();
    match state.build_queue.enqueue(job).await {
        Ok(enq) if enq.outcome != EnqueueOutcome::Full => {
            state.metrics.record_build_accepted();
            admission_test_enqueue(enq.outcome);
            Ok(enq.outcome)
        }
        Ok(_) => {
            state.metrics.record_build_rejected();
            admission_test_enqueue(EnqueueOutcome::Full);
            Err("build queue full".to_string())
        }
        Err(e) => {
            state.metrics.record_build_rejected();
            Err(format!("build queue unavailable: {e}"))
        }
    }
}

/// Persist the moving-row identity at the operation's resolution point before
/// a worker can publish. The first missing-row admission is insert-only, so a
/// duplicate cannot replace the fence selected by the active job.
struct ExactAdmissionPlan {
    exact_branch: String,
    pending: RefInfo,
    tail: Option<(String, RefInfo)>,
}

async fn prepare_exact_admission(
    state: &ServerState,
    job: &BuildJob,
    moving_authorized: bool,
) -> Result<Option<ExactAdmissionPlan>, String> {
    let commit = job.admitted_commit.as_str();
    let exact_key = exact_ref_store_key(&job.branch, commit);
    let existing = state
        .ref_store
        .load_branch(&job.repo_id, &exact_key)
        .await
        .map_err(|e| format!("exact admission lookup failed: {e}"))?;
    let active = existing.as_ref().is_some_and(|info| {
        matches!(
            info.build_status.as_deref(),
            None | Some("done")
                | Some("building")
                | Some(BUILDING_FULL_HISTORY)
                | Some("archive building")
        )
    });
    if active {
        if moving_authorized
            && let Some(mut promoted) = existing
                .as_ref()
                .filter(|info| info.moving_publication_predecessors.is_empty())
                .cloned()
        {
            let (predecessors, tail) = moving_publication_identity(state, job).await?;
            link_moving_admission(state, job, tail.as_deref()).await?;
            promoted.require_matching_commit = false;
            promoted.moving_publication_predecessors = predecessors;
            promoted.last_accessed_at = Some(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            );
            state
                .ref_store
                .save_branch(&job.repo_id, &exact_key, &promoted)
                .await
                .map_err(|e| format!("exact admission promotion failed: {e}"))?;
        }
        return Ok(None);
    }
    let evicted = existing.is_some();
    let (moving_publication_predecessors, admission_tail) = if evicted {
        (
            existing
                .as_ref()
                .map(|info| info.moving_publication_predecessors.clone())
                .unwrap_or_default(),
            None,
        )
    } else if moving_authorized {
        moving_publication_identity(state, job).await?
    } else {
        (Vec::new(), None)
    };
    let pending = RefInfo {
        internal_exact_result: true,
        require_matching_commit: !evicted,
        moving_publication_predecessors,
        commit: commit.to_string(),
        default_branch: job
            .admitted_default_branch
            .clone()
            .unwrap_or_else(|| job.branch.clone()),
        build_status: evicted.then(|| crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
        warm_pinned: existing.as_ref().is_some_and(|info| info.warm_pinned),
        last_accessed_at: Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ),
        ..Default::default()
    };
    let tail = prepare_moving_admission_tail(state, job, admission_tail.as_deref()).await?;
    Ok(Some(ExactAdmissionPlan {
        exact_branch: exact_key,
        pending,
        tail,
    }))
}

/// Follow only the outstanding ordinary-admission chain from the current
/// moving projection. The returned commits are exactly the projections the new
/// admission may replace; `tail` is linked to the new exact result before the
/// request reports successful queue admission.
async fn moving_publication_identity(
    state: &ServerState,
    job: &BuildJob,
) -> Result<(Vec<String>, Option<String>), String> {
    let result_branch = if job.branch == "HEAD" {
        job.admitted_default_branch
            .as_deref()
            .unwrap_or(job.branch.as_str())
    } else {
        job.branch.as_str()
    };
    let Some(moving) = state
        .ref_store
        .load_branch(&job.repo_id, result_branch)
        .await
        .map_err(|e| format!("moving admission lookup failed: {e}"))?
    else {
        return Ok((
            vec![crate::ref_store::INITIAL_MOVING_PROJECTION_PREDECESSOR.to_string()],
            None,
        ));
    };
    if moving.commit == job.admitted_commit {
        return Ok((moving.moving_publication_predecessors, None));
    }

    let mut predecessors = vec![moving.commit.clone()];
    let mut tail = moving.commit;
    let mut seen = std::collections::HashSet::new();
    loop {
        if !seen.insert(tail.clone()) {
            return Err("ordinary admission chain contains a cycle".to_string());
        }
        let key = exact_ref_store_key(result_branch, &tail);
        let info = state
            .ref_store
            .load_branch(&job.repo_id, &key)
            .await
            .map_err(|e| format!("ordinary admission chain lookup failed: {e}"))?
            .ok_or_else(|| format!("ordinary admission chain is missing exact {tail}"))?;
        let Some(next) = info.moving_admission_successors.last().cloned() else {
            return Ok((predecessors, Some(tail)));
        };
        crate::validation::validate_object_id(&next)
            .map_err(|e| format!("ordinary admission chain has invalid successor: {e}"))?;
        if next == job.admitted_commit {
            return Ok((predecessors, None));
        }
        predecessors.push(next.clone());
        tail = next;
    }
}

async fn link_moving_admission(
    state: &ServerState,
    job: &BuildJob,
    tail: Option<&str>,
) -> Result<(), String> {
    let Some(tail) = tail else {
        return Ok(());
    };
    let result_branch = if job.branch == "HEAD" {
        job.admitted_default_branch
            .as_deref()
            .unwrap_or(job.branch.as_str())
    } else {
        job.branch.as_str()
    };
    let key = exact_ref_store_key(result_branch, tail);
    let mut info = state
        .ref_store
        .load_branch(&job.repo_id, &key)
        .await
        .map_err(|e| format!("ordinary admission tail lookup failed: {e}"))?
        .ok_or_else(|| format!("ordinary admission tail {tail} disappeared"))?;
    if !info
        .moving_admission_successors
        .contains(&job.admitted_commit)
    {
        // The insert-only bit protects creation of this exact row. The row now
        // exists, so this same-commit metadata update must be allowed to merge.
        info.require_matching_commit = false;
        info.moving_admission_successors
            .push(job.admitted_commit.clone());
        state
            .ref_store
            .save_branch(&job.repo_id, &key, &info)
            .await
            .map_err(|e| format!("ordinary admission link failed: {e}"))?;
    }
    Ok(())
}

async fn prepare_moving_admission_tail(
    state: &ServerState,
    job: &BuildJob,
    tail: Option<&str>,
) -> Result<Option<(String, RefInfo)>, String> {
    let Some(tail) = tail else {
        return Ok(None);
    };
    let result_branch = if job.branch == "HEAD" {
        job.admitted_default_branch
            .as_deref()
            .unwrap_or(job.branch.as_str())
    } else {
        job.branch.as_str()
    };
    let key = exact_ref_store_key(result_branch, tail);
    let mut info = state
        .ref_store
        .load_branch(&job.repo_id, &key)
        .await
        .map_err(|error| format!("ordinary admission tail lookup failed: {error}"))?
        .ok_or_else(|| format!("ordinary admission tail {tail} disappeared"))?;
    if !info
        .moving_admission_successors
        .contains(&job.admitted_commit)
    {
        info.require_matching_commit = false;
        info.moving_admission_successors
            .push(job.admitted_commit.clone());
    }
    Ok(Some((key, info)))
}

/// Fire-and-forget: enqueue a build for `(repo_id, branch)` at the exact
/// admitted commit and return immediately — the build runs ahead of any clone
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
    branch: &str,
    admitted_commit: String,
    admitted_default_branch: Option<String>,
    moving_authorized: bool,
) -> Result<EnqueueOutcome, String> {
    match state.ref_store.load_added_repo(repo_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return Ok(EnqueueOutcome::Coalesced),
        Err(e) => return Err(format!("added repo lookup failed: {e}")),
    }
    crate::validation::validate_object_id(&admitted_commit)
        .map_err(|e| format!("invalid admitted commit: {e}"))?;
    let exact_key = exact_ref_store_key(branch, &admitted_commit);
    let existing = state
        .ref_store
        .load_branch(repo_id, &exact_key)
        .await
        .ok()
        .flatten();
    if let Some(info) = existing
        && exact_ref_info_serves_commit(&info, "full", &admitted_commit)
        && !info
            .build_status
            .as_deref()
            .is_some_and(|status| status.starts_with("failed: "))
    {
        // A signed replay/poller wakeup for a branch that already serves this
        // exact full commit is a read-only no-op. Do not fetch credentials or
        // touch the queue merely because the trusted trigger was repeated.
        return Ok(EnqueueOutcome::Coalesced);
    }
    let credential = state
        .broker
        .fetch_credential(repo_id, None)
        .map_err(|e| e.to_string())?;
    let size_bytes = enqueue_size_bytes(state, repo_id, branch).await;
    let job = BuildJob {
        repo_id: repo_id.clone(),
        branch: branch.to_string(),
        admitted_commit,
        admitted_default_branch,
        credential,
        size_bytes,
    };

    enqueue_admitted_build(state, job, moving_authorized).await
}

/// Query for the admin config endpoints: an optional branch selects a
/// branch-level override; absent means the repo-level config.
#[derive(Deserialize)]
struct AdminConfigQuery {
    #[serde(default)]
    branch: Option<String>,
}

/// `GET /v1/admin/config/{owner}/{repo}` — return the stored repo- or
/// branch-level config (404 if none is stored).
async fn admin_get_config(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<AdminConfigQuery>,
    State(state): State<ServerState>,
) -> Response {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
    }
    let repo_id = RepoId::github(format!("{owner}/{repo}"));
    let loaded = match query.branch.as_deref().filter(|b| !b.is_empty()) {
        Some(branch) => state.repo_config.get_branch(&repo_id, branch).await,
        None => state.repo_config.get_repo(&repo_id).await,
    };
    match loaded {
        Ok(Some(config)) => (StatusCode::OK, Json(config)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "no config stored for this repo/branch".to_string(),
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

/// `POST /v1/admin/config/{owner}/{repo}` — store the repo- or branch-level
/// config. The body is a `RepoConfig`; it is validated before being written.
/// The next sync/build for the repo reads it.
async fn admin_put_config(
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<AdminConfigQuery>,
    State(state): State<ServerState>,
    Json(config): Json<crate::repo_config::RepoConfig>,
) -> Response {
    if let Some(resp) = reject_invalid_repo_ids(&owner, &repo) {
        return resp;
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
    let stored = match query.branch.as_deref().filter(|b| !b.is_empty()) {
        Some(branch) => {
            state
                .repo_config
                .put_branch(&repo_id, branch, &config)
                .await
        }
        None => state.repo_config.put_repo(&repo_id, &config).await,
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
    admission_test_tip_probe();
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
    match trigger_build(
        &state,
        &job_repo_id,
        &admitted_branch,
        tip.commit.clone(),
        tip.default_branch.clone(),
        true,
    )
    .await
    {
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
        EventKind::BranchDelete => webhook_dispatch_delete(&state, &provider, event).await,
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

/// Push → warm. Applies the allowlist gate and the branch policy, then triggers
/// a build via the shared fire-and-forget path and returns immediately.
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
    // Phase 1 warms branches only; tags and other refs are ignored.
    let Some(branch) = event
        .ref_
        .strip_prefix("refs/heads/")
        .filter(|b| !b.is_empty())
    else {
        return webhook_ignored("non-branch ref");
    };
    let branch = branch.to_string();
    // Validate the payload-derived branch before it reaches the queue / git.
    if validation::validate_git_rev(&branch).is_err() {
        return webhook_ignored("invalid branch");
    }
    let Some(after) = event.after.as_deref() else {
        return webhook_ignored("push event has no after commit");
    };
    if validation::validate_object_id(after).is_err() {
        return webhook_ignored("push event has invalid after commit");
    }
    // Policy: always warm the default branch; warm other branches only if a
    // build for them already exists — unless RIPCLONE_WEBHOOK_WARM_ALL=1, which
    // warms every pushed branch. The default branch comes from the payload, or
    // the local mirror's HEAD when a provider omits it.
    if !state.webhook_config.warm_all() {
        let default_branch = event.default_branch.clone().or_else(|| {
            let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
            git::default_branch(&mirror_dir)
                .ok()
                .filter(|b| !b.is_empty())
        });
        let is_default = default_branch.as_deref() == Some(branch.as_str());
        if !is_default {
            let tracked = matches!(
                state.ref_store.load_branch(&repo_id, &branch).await,
                Ok(Some(_))
            );
            if !tracked {
                return webhook_ignored("non-default branch not tracked");
            }
        }
    }
    // The signed, validated `after` is the admission target. No second
    // upstream probe is permitted on this path.
    match trigger_build(
        state,
        &repo_id,
        &branch,
        after.to_string(),
        event.default_branch.clone(),
        false,
    )
    .await
    {
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

/// Branch delete → clean up that ref's stored metadata. Never builds.
async fn webhook_dispatch_delete(
    state: &ServerState,
    provider: &ProviderInstance,
    event: crate::webhook::CanonicalEvent,
) -> Response {
    let repo_id = RepoId {
        provider: provider.id.clone(),
        path: event.repo.clone(),
    };
    if validation::validate_repo_path(provider, &repo_id).is_err() {
        return webhook_ignored("invalid repo path");
    }
    // Gate cleanup by the same allowlist as push so the receiver only acts on
    // in-scope repos (an out-of-scope repo was never warmed, so this is a no-op
    // either way — the gate just keeps push and delete symmetric).
    if !webhook_repo_allowed(state, &repo_id) {
        return webhook_ignored("repo not in webhook allowlist");
    }
    let Some(branch) = event
        .ref_
        .strip_prefix("refs/heads/")
        .filter(|b| !b.is_empty())
    else {
        return webhook_ignored("non-branch ref");
    };
    if validation::validate_git_rev(branch).is_err() {
        return webhook_ignored("invalid branch");
    }
    // Drop the stored ref and any cached copy. Best-effort: a delete we can't
    // complete is logged, not surfaced to the provider (it would just retry).
    if let Err(e) = state.ref_store.delete_branch(&repo_id, branch).await {
        warn!(
            "webhook: failed to delete ref {}@{branch}: {e}",
            repo_id.storage_key()
        );
    }
    state.ref_store.invalidate(&repo_id, branch).await;
    invalidate_ref_response_cache(state, &repo_id, branch);
    // The webhook is authoritative for the moving branch. Force the next
    // unpinned read to refresh its mirror so `git fetch --prune` removes the
    // deleted upstream ref instead of resolving a stale local copy. Immutable
    // commit-keyed metadata remains available to callers that already pinned.
    invalidate_mirror_fresh(state, &format!("{}/{branch}", repo_id.storage_key()));
    invalidate_mirror_fresh(state, &format!("{}/HEAD", repo_id.storage_key()));
    info!(
        "webhook: cleaned up deleted branch {}@{branch}",
        repo_id.storage_key()
    );
    (StatusCode::OK, Json(WebhookAccepted { ok: true })).into_response()
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

/// Return a response body that streams `data` but pauses after `barrier.after_bytes`
/// bytes, signals the test, waits for the test to release it, and then either
/// sends the rest of the bytes or errors out so the client sees a retryable
/// transport failure.
fn barrier_body(data: Vec<u8>, barrier: ArtifactBarrier) -> Body {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
    tokio::spawn(async move {
        let after = barrier.after_bytes.min(data.len());
        let _ = tx.send(Ok(Bytes::from(data[..after].to_vec()))).await;
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
        if should_continue && after < data.len() {
            let _ = tx.send(Ok(Bytes::from(data[after..].to_vec()))).await;
        } else {
            // Error out so reqwest surfaces a body-read failure rather than a
            // clean short body. That makes the client's retry path run again with
            // the now-expired credential.
            let _ = tx
                .send(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "injected test barrier close",
                )))
                .await;
        }
    });
    Body::from_stream(rx)
}

async fn serve_artifact(
    hash: String,
    state: ServerState,
    headers: Option<axum::http::HeaderMap>,
) -> impl IntoResponse {
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
    let range = headers.as_ref().and_then(|h| {
        h.get(axum::http::header::RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| parse_byte_range(v, total_size))
    });

    match range {
        Some((start, end)) => {
            let len = end - start + 1;
            let hash_clone = hash.clone();
            match tokio::task::spawn_blocking(move || {
                state.storage.get_range(&hash_clone, start, len)
            })
            .await
            {
                Ok(Ok(data)) => {
                    state.metrics.record_artifact_request(data.len() as u64);
                    let content_range = format!("bytes {}-{}/{}", start, end, total_size);
                    (
                        StatusCode::PARTIAL_CONTENT,
                        [
                            ("content-range", content_range.as_str()),
                            ("content-length", &data.len().to_string()),
                        ],
                        data,
                    )
                        .into_response()
                }
                _ => {
                    state.metrics.record_error();
                    (
                        StatusCode::NOT_FOUND,
                        Json(ErrorResponse {
                            error: format!("artifact not found: {}", hash),
                        }),
                    )
                        .into_response()
                }
            }
        }
        None => {
            let hash_clone = hash.clone();
            match tokio::task::spawn_blocking(move || state.storage.get(&hash_clone)).await {
                Ok(Ok(data)) => {
                    state.metrics.record_artifact_request(data.len() as u64);
                    if let Some(barrier) = state.artifact_barrier.clone() {
                        if !barrier.consumed.load(Ordering::SeqCst)
                            && data.len() > barrier.after_bytes
                        {
                            barrier.consumed.store(true, Ordering::SeqCst);
                            return (StatusCode::OK, barrier_body(data, barrier)).into_response();
                        }
                    }
                    (StatusCode::OK, data).into_response()
                }
                _ => {
                    state.metrics.record_error();
                    (
                        StatusCode::NOT_FOUND,
                        Json(ErrorResponse {
                            error: format!("artifact not found: {}", hash),
                        }),
                    )
                        .into_response()
                }
            }
        }
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

/// Commit-keyed artifact row shared by ordinary pins and explicit revisions.
fn exact_ref_store_key(branch: &str, commit: &str) -> String {
    crate::ref_store::exact_ref_key(branch, commit)
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
/// freshly built this sync (the tail plus any compaction output — what to
/// upload/evict), and `new_levels` is the levels to persist for the next sync.
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
/// Bytes come from local CAS or object storage (the metadata may have been
/// evicted locally after a prior upload). Returns `None` on any failure — the
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

/// Build one variant's PackEntry list + concatenated idx bundle. Free-function
/// form (used by the two-phase sync path and its background phase-2 task).
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

/// TEST-ONLY: hand each phase-2 build a distinct sequence number when
/// `RIPCLONE_TEST_PHASE2_RACE` is set. A regression test uses it to drive two
/// overlapping same-commit phase-2 builds that produce *different* idx bundles —
/// reproducing on a small fixture the divergence that `git pack-objects`'
/// run-to-run non-determinism produces between two concurrent builds of a large
/// repo on real infra. Returns `None` (no-op) when the hook is unset.
fn phase2_race_seq() -> Option<u64> {
    if std::env::var_os("RIPCLONE_TEST_PHASE2_RACE").is_some() {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Some(SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    } else {
        None
    }
}

/// TEST-ONLY companion to [`phase2_race_seq`]: append the build's sequence number
/// to the idx bundle so two concurrent same-commit builds store *different*
/// bundles. The nonce is trailing, so pack idx slices (indexed by offset) are
/// unchanged; only the bundle `ChunkRef` length grows so the client's size check
/// still matches. No-op when `seq` is `None`.
fn phase2_race_salt_bundle(
    cas: &Cas,
    bundle_ref: Option<ChunkRef>,
    bundle_hash: String,
    seq: Option<u64>,
) -> Result<(Option<ChunkRef>, String)> {
    let (Some(seq), Some(mut r)) = (seq, bundle_ref.clone()) else {
        return Ok((bundle_ref, bundle_hash));
    };
    if bundle_hash.is_empty() {
        return Ok((bundle_ref, bundle_hash));
    }
    let mut bytes = cas.get(&bundle_hash)?;
    bytes.extend_from_slice(&seq.to_le_bytes());
    let hash = cas.put(&bytes)?;
    r.hash = hash_from_hex(&hash)?;
    r.len = bytes.len() as u64;
    Ok((Some(r), hash))
}

/// TEST-ONLY: per-sequence `(pre-editable, pre-files)` sleeps (ms) that bracket
/// build 0's two publishes around build 1's, so the current partial files publish
/// lands build 0's manifest on top of build 1's idx bundle — the exact divergent
/// interleave. No-op unless `RIPCLONE_TEST_PHASE2_RACE` is set.
fn phase2_race_delays(seq: Option<u64>) -> (u64, u64) {
    match seq {
        Some(0) => (3000, 8000),
        Some(1) => (4000, 0),
        _ => (0, 0),
    }
}

/// Build a multi-pack-index over `packs` from local CAS. Free-function form.
///
/// Reads each pack's bytes from the *local* CAS (no object-storage fallback), so
/// only call this when every pack was built this sync and is still local — e.g.
/// the head MIDX is shipped only when all head buckets were freshly built. For a
/// set with reused (already-evicted) packs, omit the MIDX and let the client
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
    default_branch: &str,
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
        default_branch: default_branch.to_string(),
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
async fn upload_artifacts(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    hashes: Vec<String>,
    conc: usize,
) -> Result<()> {
    futures::stream::iter(hashes.into_iter().map(|hash| {
        let cas = cas.clone();
        let storage = storage.clone();
        async move {
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
            admission_test_artifact_upload();
            storage
                .put_file_async(&hash, &path)
                .await
                .with_context(|| format!("upload artifact {}", hash))?;
            crate::perf::record_storage_upload(upload_start.elapsed(), len);
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

/// After upload: on a remote backend drop local pack copies (keeping the tiny
/// idx files for future bundle/MIDX rebuilds); on a local backend protect them
/// from retention instead.
async fn settle_storage(
    cas: &Cas,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    uploaded: Vec<String>,
    keep_idx: std::collections::HashSet<String>,
) {
    if storage.is_remote() {
        for h in uploaded.iter().filter(|h| !keep_idx.contains(*h)) {
            let _ = cas.remove(h);
        }
    } else {
        retention.protect(uploaded).await;
    }
}

async fn clonepack_seed_info(
    ref_store: &Arc<dyn RefStore>,
    repo_id: &RepoId,
    branch: &str,
) -> Option<RefInfo> {
    let moving = ref_store
        .load_branch(repo_id, branch)
        .await
        .ok()
        .flatten()?;
    let exact_key = exact_ref_store_key(branch, &moving.commit);
    let info = ref_store
        .load_branch(repo_id, &exact_key)
        .await
        .ok()
        .flatten()?;
    exact_ref_info_serves_commit(&info, "full", &moving.commit).then_some(info)
}

fn validate_clonepack_seed_identity<'a>(
    info: &'a RefInfo,
    manifest: &ClonepackManifest,
) -> Result<&'a str> {
    let seed_commit = info.full_clonepack.commit.as_str();
    if seed_commit.is_empty() {
        anyhow::bail!("seed clonepack is missing its artifact commit");
    }
    validation::validate_object_id(seed_commit).context("invalid seed artifact commit")?;
    if info.commit != seed_commit {
        anyhow::bail!(
            "seed exact row commit {} does not match artifact commit {seed_commit}",
            info.commit
        );
    }
    if manifest.commit != seed_commit {
        anyhow::bail!(
            "seed manifest commit {} does not match artifact commit {seed_commit}",
            manifest.commit
        );
    }
    Ok(seed_commit)
}

fn seed_bare_mirror_from_clonepack(
    mirror_dir: &std::path::Path,
    storage: &StorageRef,
    provider: &ProviderInstance,
    repo_id: &RepoId,
    branch: &str,
    credential: Option<&secrecy::SecretString>,
    info: &RefInfo,
) -> Result<u64> {
    let manifest_hash = &info.full_clonepack.manifest;
    if manifest_hash.is_empty() {
        anyhow::bail!("previous ref has no full clonepack manifest");
    }
    if mirror_dir.exists() {
        anyhow::bail!("mirror already exists");
    }

    let manifest_bytes = storage
        .get(manifest_hash)
        .with_context(|| format!("fetch seed clonepack manifest {manifest_hash}"))?;
    let manifest =
        ClonepackManifest::decode(manifest_bytes.as_slice()).context("decode seed clonepack")?;
    let seed_commit = validate_clonepack_seed_identity(info, &manifest)?;
    if manifest.packs.is_empty() {
        anyhow::bail!("seed clonepack has no manifest packs");
    }

    let parent = mirror_dir
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create mirror parent {}", parent.display()))?;
    let tmp = tempfile::Builder::new()
        .prefix(".seed-mirror-")
        .tempdir_in(parent)
        .with_context(|| format!("create seed tempdir in {}", parent.display()))?;

    git::init_bare_mirror_origin(tmp.path(), provider, repo_id, credential)?;

    let idx_bundle_ref = manifest
        .idx_bundle
        .as_ref()
        .context("seed clonepack manifest is missing required idx bundle")?;
    let idx_bundle_hash = hash_to_hex(&idx_bundle_ref.hash);
    let idx_bundle = Bytes::from(
        storage
            .get(&idx_bundle_hash)
            .with_context(|| format!("fetch seed idx bundle {idx_bundle_hash}"))?,
    );

    let mut pack_pairs = Vec::with_capacity(manifest.packs.len());
    for (i, entry) in manifest.packs.iter().enumerate() {
        let pack_ref = entry
            .pack
            .as_ref()
            .with_context(|| format!("pack {i} missing pack ref"))?;
        let pack_hash = hash_to_hex(&pack_ref.hash);
        let pack_bytes = Bytes::from(
            storage
                .get(&pack_hash)
                .with_context(|| format!("fetch seed pack {i} ({pack_hash})"))?,
        );
        if pack_bytes.len() as u64 != pack_ref.len {
            anyhow::bail!(
                "seed pack {i} size mismatch: expected {}, got {}",
                pack_ref.len,
                pack_bytes.len()
            );
        }
        let actual_pack_hash = crate::cas::hash(&pack_bytes);
        if actual_pack_hash != pack_hash {
            anyhow::bail!(
                "seed pack {i} hash mismatch: expected {pack_hash}, got {actual_pack_hash}"
            );
        }
        let idx_bytes = manifest_pack_idx_bytes(entry, i, &idx_bundle)?;
        pack_pairs.push((pack_bytes, idx_bytes));
    }

    let bytes = install_manifest_pack_bytes(&tmp.path().join("objects").join("pack"), pack_pairs)?;
    let seed_branch = if branch == "HEAD" {
        if info.default_branch.is_empty() {
            "main"
        } else {
            &info.default_branch
        }
    } else {
        branch
    };
    validation::validate_git_rev(seed_branch).context("invalid seed branch")?;
    let seed_ref = format!("refs/heads/{seed_branch}");
    let status = std::process::Command::new("git")
        .arg("-C")
        .arg(tmp.path())
        .args(["update-ref", &seed_ref, seed_commit])
        .status()
        .context("seed mirror ref")?;
    if !status.success() {
        anyhow::bail!("seed mirror ref failed");
    }
    let status = std::process::Command::new("git")
        .arg("-C")
        .arg(tmp.path())
        .args(["symbolic-ref", "HEAD", &seed_ref])
        .status()
        .context("seed mirror HEAD")?;
    if !status.success() {
        anyhow::bail!("seed mirror HEAD failed");
    }
    git::fsck_connectivity(tmp.path()).context("validate seeded mirror")?;

    let tmp_path = tmp.path().to_path_buf();
    std::fs::rename(&tmp_path, mirror_dir).with_context(|| {
        format!(
            "promote seeded mirror {} -> {}",
            tmp_path.display(),
            mirror_dir.display()
        )
    })?;
    Ok(bytes)
}

#[allow(clippy::too_many_arguments)]
/// Load the effective per-repo/branch build config, falling back to the default
/// (today's behavior) if the store read fails — a config-store hiccup must never
/// block a build.
async fn effective_repo_config(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
) -> crate::repo_config::RepoConfig {
    match state.repo_config.effective(repo_id, branch).await {
        Ok(cfg) => cfg,
        Err(e) => {
            warn!(
                "repo config load for {} failed ({e:#}); using defaults",
                repo_id.storage_key()
            );
            crate::repo_config::RepoConfig::default()
        }
    }
}

async fn do_sync(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    branch: &str,
    // Every request resolves to one immutable object before admission. Workers
    // fetch and build only this object; they never resolve a moving selector.
    admitted_commit: &str,
    // Trusted default branch advertised alongside a HEAD admission. This is
    // separate from the requested/build branch so concrete-branch jobs keep
    // their existing metadata semantics.
    admitted_default_branch: Option<&str>,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    provider: &ProviderInstance,
    credential: Option<&secrecy::SecretString>,
    // Effective per-repo/branch build config (ROADMAP §2a). Default config
    // reproduces today's build exactly.
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
    info!("syncing {}@{}", repo_id.storage_key(), branch);

    // Per-phase timers so sync cost can be tuned with real numbers (RIPCLONE_LOG
    // shows them at INFO). `t_total` spans the whole build; `t` is reset at each
    // phase boundary.
    let t_total = Instant::now();
    let mut t = t_total;
    let mut phases = SyncPhases::default();

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

    if !mirror_dir.exists()
        && let Some(seed_info) = clonepack_seed_info(ref_store, repo_id, branch).await
    {
        let mirror_dir_seed = mirror_dir.to_path_buf();
        let storage_seed = storage.clone();
        let provider_seed = provider.clone();
        let repo_id_seed = repo_id.clone();
        let branch_seed = branch.to_string();
        let credential_seed = credential.cloned();
        let seed_result = tokio::task::spawn_blocking(move || {
            git::with_mirror_lock(&mirror_dir_seed, || {
                seed_bare_mirror_from_clonepack(
                    &mirror_dir_seed,
                    &storage_seed,
                    &provider_seed,
                    &repo_id_seed,
                    &branch_seed,
                    credential_seed.as_ref(),
                    &seed_info,
                )
            })
        })
        .await
        .context("seed mirror task")?;
        match seed_result {
            Ok(bytes) => info!(
                "seeded cold mirror for {} from storage clonepack ({} bytes)",
                repo_id.storage_key(),
                bytes
            ),
            Err(e) => warn!(
                "cold mirror seed unavailable for {}; falling back to full upstream clone: {e:#}",
                repo_id.storage_key()
            ),
        }
    }

    // Sync the bare mirror synchronously (blocking git call).
    let mirror_dir_sync = mirror_dir.to_path_buf();
    let mirror_dir = mirror_dir.to_path_buf();
    let provider_sync = provider.clone();
    let repo_id_sync = repo_id.clone();
    let admitted_commit_sync = admitted_commit.to_string();
    let admitted_default_branch_sync = admitted_default_branch.map(str::to_string);
    let credential_sync = credential.cloned();
    admission_test_fetch_entry(Some(admitted_commit)).await;
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
            admitted_default_branch_sync.as_deref(),
            credential_sync.as_ref(),
        )
    })
    .await
    .context("sync task")??;
    drop(fetch_permit);
    // Stamp the ordering timestamp at fetch time, not when the build finishes.
    // Fetches are serialized by the per-repo lock, so fetch time orders syncs
    // correctly; a timestamp set at build completion lets a build that finishes
    // out of order look newer and wrongly move the branch back. This feeds the
    // save_ordered check and makes a force-push (the later fetch) win.
    let fetched_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs());
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
    let default_branch = admitted_default_branch
        .map(str::to_string)
        .or_else(|| {
            git::default_branch(&mirror_dir)
                .ok()
                .filter(|default_branch| !default_branch.is_empty() && default_branch != "HEAD")
        })
        .unwrap_or_else(|| {
            // Exact ordinary admission already resolved HEAD to the concrete
            // branch before queueing. Some bare mirrors cannot expose a symbolic
            // HEAD for delimiter-bearing names, so retain that trusted admission
            // branch in metadata instead of falling back to the literal `HEAD`.
            if branch != "HEAD" {
                branch.to_string()
            } else {
                "HEAD".to_string()
            }
        });

    // If the caller asked for HEAD, store artifacts under the concrete default
    // branch name so both /refs/HEAD and /refs/<branch> find the same build.
    // Ordinary admitted work keeps that concrete branch as the moving projection,
    // while all build state below uses the exact commit-keyed row.
    let moving_branch = Some({
        if branch == "HEAD" {
            default_branch.clone()
        } else {
            branch.to_string()
        }
    });
    let storage_branch = moving_branch.as_deref().expect("moving branch is concrete");
    // Ref-store key. Rev builds and ordinary admitted builds share the existing
    // commit-keyed rolling lane; only the moving projection stays plain.
    // The mirror fetch + commit resolution above used the real branch/rev.
    let ref_key = exact_ref_store_key(storage_branch, &commit);
    let branch = ref_key.as_str();

    // No-op fast path: if a *completed full* build already exists for exactly
    // this commit, the prior clonepack artifacts are still valid — reuse them and
    // build nothing (skips commit-graph/bitmap/skeleton/history/archive), so a
    // poke-to-check sync of an unchanged repo returns near-instantly. Reuse is by
    // this branch first, then any branch built at this commit (commit-keyed).
    // Keying on `full_clonepack.commit == commit` (not `build_status`) is robust:
    // it is set only once the full clonepack is published for this commit, so it
    // excludes the Option-A carried-prior case, an unpublished/failed phase 2, and
    // the async worker's transient "building" status. (It does *not* require the
    // archive sub-phase to be done — a files-mode client re-resolves until the
    // archive is ready, so reusing an archive-pending build is safe.)
    let reusable = ref_store
        .load_branch(repo_id, branch)
        .await?
        .filter(|info| {
            let archive_in_progress = info.archive_chunks.is_empty()
                && info.build_status.as_ref().is_some_and(|status| {
                    status == "full history building" || status == "archive building"
                });
            info.full_clonepack.commit == commit
                && !info.full_clonepack.manifest.is_empty()
                && info.build_status.as_deref() != Some(crate::remote_gc::EVICTED_BUILD_STATUS)
                && !archive_in_progress
        });
    if let Some(prev) = reusable {
        info!(
            "sync no-op: {} already current at {} (reusing prior clonepack)",
            repo_id.storage_key(),
            &commit[..7.min(commit.len())]
        );
        if let Some(release) = foreground_release.take() {
            let _ = release.send(());
        }
        return Ok(SyncBuildResult {
            info: prev,
            status: "no-op".to_string(),
            phases,
        });
    }

    // Write a commit-graph so the rev-list walks in the skeleton + layered-pack
    // builds below are fast (a fresh --mirror clone has none). Best-effort.
    let cg_dir = mirror_dir.clone();
    let _ = tokio::task::spawn_blocking(move || git::write_commit_graph(&cg_dir)).await;
    phases.commit_graph_ms = Some(duration_ms(t.elapsed()));
    info!("sync phase: commit-graph {:?}", t.elapsed());

    info!("building artifacts for {}", &commit[..7]);

    // Two-phase publish: build + publish the depth=1 clonepack now, build full
    // history in the background. Removes the dominant history-deltification cost
    // from "time to clonable". Mirror prep (fetch + commit-graph) is done; the
    // depth=1 build and the background phase-2 are read-only over the mirror, so
    // release the lock so other repos' builds (and this repo's resolves) proceed
    // concurrently.
    drop(_guard);
    build_and_publish_two_phase(
        cas,
        &mirror_dir,
        repo_id,
        branch,
        moving_branch.as_deref(),
        &commit,
        parent,
        &default_branch,
        ref_store,
        storage,
        retention,
        t_total,
        fetched_at,
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

/// Two-part publish. The foreground work builds and publishes the depth=1
/// clonepack, then releases its worker slot. The same durable owner continues
/// full history and upgrades the full clonepack in the background. depth=0
/// keeps serving the previous commit until Full finishes.
/// Result of the phase-1 HEAD-closure build: a small delta pack against the
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

/// Publish the current unpinned branch projection. Completed publications are
/// commit-fenced atomically so a delayed exact build cannot replace a branch
/// that has since selected another commit.
async fn publish_moving_refs(
    ref_store: &Arc<dyn RefStore>,
    repo_id: &RepoId,
    exact_branch: &str,
    moving_branch: Option<&str>,
    default_branch: &str,
    info: &RefInfo,
) -> Result<()> {
    let Some(moving_branch) = moving_branch else {
        return Ok(());
    };
    let Some(exact) = ref_store.load_branch(repo_id, exact_branch).await? else {
        return Ok(());
    };
    if exact.moving_publication_predecessors.is_empty() {
        return Ok(());
    }
    let mut projection = info.clone();
    projection.internal_exact_result = false;
    projection.require_matching_commit = true;
    projection.moving_publication_predecessors = exact.moving_publication_predecessors;
    if moving_branch != exact_branch {
        admission_test_ref_store_write();
        ref_store
            .save_branch(repo_id, moving_branch, &projection)
            .await
            .with_context(|| {
                format!(
                    "persist moving ref for {}@{moving_branch}",
                    repo_id.storage_key()
                )
            })?;
    }
    if moving_branch == default_branch {
        ref_store.invalidate(repo_id, moving_branch).await;
        let current = ref_store.load_branch(repo_id, moving_branch).await?;
        if let Some(current) = current
            && current.commit == info.commit
        {
            admission_test_ref_store_write();
            ref_store
                .save_branch(repo_id, "HEAD", &projection)
                .await
                .with_context(|| format!("persist HEAD ref for {}", repo_id.storage_key()))?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn build_and_publish_two_phase(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    branch: &str,
    moving_branch: Option<&str>,
    commit: &str,
    parent: Option<String>,
    default_branch: &str,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    t_total: Instant,
    // Publish-ordering key, stamped at fetch time by the caller (see do_sync).
    fetched_at: Option<u64>,
    // zstd level for archive frames, from the effective repo config.
    compression_level: i32,
    mut phases: SyncPhases,
    mut foreground_release: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<SyncBuildResult> {
    admission_test_builder_entry(commit).await;
    let history_target = 512 * 1024 * 1024;
    let upload_conc = upload_concurrency();

    // Load the previous synced ref once: used both for the files-table by-diff
    // below and for Option-A full-clonepack carry later in this phase.
    //
    // Ignore an *evicted* prev. Warm-TTL eviction marks the ref `evicted` and the
    // remote-GC pass then deletes its clonepack/pack/archive objects, but leaves
    // the ref's artifact-pointer fields (head_base_packs, full_clonepack, history
    // levels, archive frames) intact. Carrying any of those into this rebuild
    // would reference objects storage no longer has, so the published manifest
    // would point at deleted packs and the next clone 404s. Treat an evicted prev
    // as absent so the rebuild is cold and re-uploads everything it references.
    let exact_prev = ref_store.load_branch(repo_id, branch).await.ok().flatten();
    let placeholder_only = exact_prev.as_ref().is_some_and(|info| {
        info.build_status.as_deref() == Some("building")
            && info.full_clonepack.manifest.is_empty()
            && info.shallow_clonepack.manifest.is_empty()
    });
    let prev_loaded = match exact_prev {
        Some(info) if !placeholder_only => Some(info),
        _ => match moving_branch {
            Some(moving) if moving != branch => ref_store
                .load_branch(repo_id, moving)
                .await
                .ok()
                .flatten()
                .filter(|info| parent.as_deref() == Some(info.commit.as_str())),
            _ => None,
        },
    };
    // Preserve the repo's warm pin across a cold rebuild. The pin is an
    // out-of-band flag an operator or external control plane may set; an evicted
    // `prev` (whose
    // artifacts GC already reclaimed) is treated as absent for artifact carry
    // below, but dropping its pin would let the freshly rebuilt ref come back
    // un-pinned and be re-evicted every idle cycle. Read the pin from the raw
    // prev, before the evicted filter.
    let prev_warm_pinned = prev_loaded.as_ref().map(|p| p.warm_pinned).unwrap_or(false);
    let prev = prev_loaded
        .filter(|p| p.build_status.as_deref() != Some(crate::remote_gc::EVICTED_BUILD_STATUS));

    // ---- PHASE 1: HEAD closure + archive + shallow skeleton -> publish depth=1 ----
    let mut t = Instant::now();
    let sk_start = Instant::now();
    let (md1, c1, cm1) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
    let shallow_skeleton_handle = tokio::task::spawn_blocking(move || {
        let s = Instant::now();
        let r = PackBuilder::new(&md1, &c1).build_shallow_skeleton_pack(&cm1);
        info!("p1 sub: shallow skeleton {:?}", s.elapsed());
        r
    });
    // Head-closure packs, incremental by delta against an immutable base: keep the
    // base packs (closure of `head_base_commit`) and pack only the depth-1 objects
    // new since that base (`closure(HEAD) − closure(base)`) into a delta pack. The
    // base and delta are disjoint by construction, so no object is ever in two HEAD
    // packs (which would double-materialize a worktree file). A cold sync (no base)
    // packs the full closure as the base. The cumulative delta grows as HEAD moves
    // from the base; phase 2 rebases (rebuilds the base at HEAD) once it exceeds
    // RIPCLONE_HEAD_REBASE_BYTES, off the depth=1 critical path.
    let head_target = 4 * 1024 * 1024;
    let prev_base_commit: Option<String> = prev
        .as_ref()
        .map(|p| p.head_base_commit.clone())
        .filter(|c| !c.is_empty());
    let prev_base_packs: Vec<crate::SizedPack> = prev
        .as_ref()
        .map(|p| p.head_base_packs.clone())
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
                    "p1 sub: head packs (delta vs base: {} new pack(s), {} total) {:?}",
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
                info!(
                    "p1 sub: head packs (full base, {} packs) {:?}",
                    base.len(),
                    elapsed
                );
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
    // Phase 1 builds only the cheap files table (no zstd frames): editable
    // depth=1 materializes the worktree from the HEAD-closure packs, so it does
    // not need the archive. The full zstd archive (for files mode) is built in
    // phase 2, off the time-to-depth=1 critical path.
    //
    // Files-table by-diff: when a prior sync exists, reuse its content hashes for
    // unchanged paths and read+hash only the blobs that changed since the prior
    // commit (O(changed) instead of O(worktree)). The no-op fast path in do_sync
    // guarantees commit != prev.commit here, so the diff is non-trivial. Falls
    // back to a full table when there is no prior table.
    let prev_files: Option<Vec<crate::clonepack::FileEntry>> = match prev.as_ref() {
        Some(p) if !p.commit.is_empty() && !p.shallow_clonepack.metadata_chunk.is_empty() => {
            load_metadata_files(cas, storage, &p.shallow_clonepack.metadata_chunk)
        }
        _ => None,
    };
    let prev_commit_for_diff = prev.as_ref().map(|p| p.commit.clone());
    // Carry the prior files table + commit so the bounded archive can hash only
    // changed files and reuse frames for the unchanged prefix/suffix.
    let prev_files_for_archive: Vec<crate::clonepack::FileEntry> =
        prev_files.clone().unwrap_or_default();
    let prev_archive_commit: Option<String> = prev
        .as_ref()
        .map(|p| p.commit.clone())
        .filter(|c| !c.is_empty());
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
                            "p1 sub: files-table (incremental, {} changed) {:?}",
                            changed.len(),
                            s.elapsed()
                        );
                        r
                    }
                    Err(e) => {
                        warn!("files-table diff failed ({e:#}); full rebuild");
                        let r = ArchiveBuilder::new(&md).build_files_table(&cm);
                        info!(
                            "p1 sub: files-table (full, diff fallback) {:?}",
                            s.elapsed()
                        );
                        r
                    }
                }
            })
        }
        _ => tokio::task::spawn_blocking(move || {
            let s = Instant::now();
            let r = ArchiveBuilder::new(&md3).build_files_table(&cm3);
            info!("p1 sub: files-table (full) {:?}", s.elapsed());
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
    info!(
        "two-phase p1: head+shallow-skeleton+files-table {:?}",
        t.elapsed()
    );
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

    // No archive frames in phase 1 (files mode is served by the full variant
    // after phase 2). Editable depth=1 ignores archive chunks.
    let archive_chunks = archive_chunk_refs(&[], &metadata_base)?;
    let head_tagged: Vec<(&(String, u64, String, u64), bool)> =
        head_packs.iter().map(|p| (p, false)).collect();
    let (head_entries, head_idx_bundle_ref, head_idx_bundle_hash) =
        assemble_variant(cas, storage, &head_tagged)?;
    // Ship the head MIDX only on a cold full base (all pack bytes still local).
    // On a delta re-sync the base packs are already evicted, so omit it — the
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
        default_branch,
        &archive_chunks,
        &shallow_metadata_hash,
        shallow_meta_data.len() as u64,
        head_entries,
        head_midx_ref,
        head_idx_bundle_ref,
    )?;
    let shallow_clonepack_hash = cas.put(&shallow_manifest.encode_to_vec())?;

    // Option A: carry the previous commit's full clonepack so depth=0 keeps
    // working (one commit stale) until phase 2 publishes the new full. (`prev`
    // was loaded at the top of this phase.)
    let carried_full = prev
        .as_ref()
        .map(|p| p.full_clonepack.clone())
        .unwrap_or_default();
    let carried_levels = prev
        .as_ref()
        .map(|p| p.history_levels.clone())
        .unwrap_or_default();
    // Prior sealed levels for phase 2's incremental build (reuse from Tigris).
    let prev_levels_for_p2 = carried_levels.clone();
    // Carry the prior archive frame index so phase 2 can reuse unchanged frames;
    // phase 2 overwrites this with the freshly built index.
    let carried_archive_frames = prev
        .as_ref()
        .map(|p| p.archive_frames.clone())
        .unwrap_or_default();
    let prev_archive_frames_for_p2 = carried_archive_frames.clone();

    let generation = git::commit_depth(mirror_dir, commit).ok();

    let mut info = RefInfo {
        internal_exact_result: true,
        require_matching_commit: false,
        moving_publication_predecessors: Vec::new(),
        moving_admission_successors: Vec::new(),
        commit: commit.to_string(),
        parent_commit: parent.clone(),
        default_branch: default_branch.to_string(),
        skeleton_pack: shallow_skeleton_pack.clone(),
        skeleton_idx: shallow_skeleton_idx.clone(),
        head_blobs_idx: String::new(),
        head_blobs_chunks: Vec::new(),
        packs: pack_artifacts_of(&head_packs),
        prebuilt_index: shallow_prebuilt_index.clone(),
        archive: String::new(),
        manifest: shallow_metadata_hash.clone(),
        archive_chunks: Vec::new(),
        full_clonepack: carried_full,
        shallow_clonepack: crate::ClonepackArtifacts {
            manifest: shallow_clonepack_hash.clone(),
            metadata_chunk: shallow_metadata_hash.clone(),
            skeleton_pack: shallow_skeleton_pack.clone(),
            skeleton_idx: shallow_skeleton_idx.clone(),
            prebuilt_index: shallow_prebuilt_index.clone(),
            midx: head_midx_hash.clone(),
            idx_bundle: head_idx_bundle_hash.clone(),
            commit: commit.to_string(),
        },
        history_levels: carried_levels,
        head_base_commit: head_built.base_commit.clone(),
        head_base_packs: head_built.base_packs.clone(),
        archive_frames: carried_archive_frames,
        build_status: Some("full history building".to_string()),
        build_ms: None,
        synced_at: fetched_at,
        last_accessed_at: fetched_at,
        generation,
        warm_pinned: prev_warm_pinned,
    };

    // Upload phase-1 artifacts (shallow skeleton/index/metadata, head idx-bundle
    // + midx, shallow manifest, and only the FRESHLY BUILT head packs+idx).
    // Reused bucket packs are already durable in storage from a prior sync.
    let mut p1: Vec<String> = vec![
        shallow_skeleton_pack.clone(),
        shallow_skeleton_idx.clone(),
        shallow_prebuilt_index.clone(),
        shallow_metadata_hash.clone(),
        shallow_clonepack_hash.clone(),
        head_idx_bundle_hash.clone(),
        head_midx_hash.clone(),
    ];
    for (p, _, i, _) in &head_built.new_built {
        p1.push(p.clone());
        p1.push(i.clone());
    }
    p1.retain(|h| !h.is_empty());
    let head_idx_keep: std::collections::HashSet<String> =
        head_packs.iter().map(|(_, _, ih, _)| ih.clone()).collect();
    let upload_start = Instant::now();
    upload_artifacts(cas, storage, p1.clone(), upload_conc).await?;
    settle_storage(cas, storage, retention, p1, head_idx_keep).await;
    phases.upload_p1_ms = Some(duration_ms(upload_start.elapsed()));

    let publish_start = Instant::now();
    admission_test_ref_store_write();
    ref_store
        .save_branch(repo_id, branch, &info)
        .await
        .with_context(|| format!("persist depth=1 ref for {}@{branch}", repo_id.storage_key()))?;
    publish_moving_refs(
        ref_store,
        repo_id,
        branch,
        moving_branch,
        default_branch,
        &info,
    )
    .await?;
    phases.ref_publish_ms = Some(duration_ms(publish_start.elapsed()));
    info!(
        "two-phase p1: published depth-1 for {} in {:?} (full history building in background)",
        &commit[..7.min(commit.len())],
        t_total.elapsed()
    );
    phases.publish_p1_ms = Some(duration_ms(t_total.elapsed()));
    let _ = t; // p1 assemble/upload time folded into the total above
    if let Some(release) = foreground_release.take() {
        let _ = release.send(());
    }
    wait_test_phase_two_barrier(commit).await?;

    // ---- PHASE 2: full history, in the background (survives the request) ----
    let cas2 = cas.clone();
    let storage2 = storage.clone();
    let ref_store2 = ref_store.clone();
    let retention2 = retention.clone();
    let mirror2 = mirror_dir.to_path_buf();
    let repo_id2 = repo_id.clone();
    let branch2 = branch.to_string();
    let moving_branch2 = moving_branch.map(str::to_string);
    let commit2 = commit.to_string();
    let parent2 = parent.clone();
    let default_branch2 = default_branch.to_string();
    let sk_pack = shallow_skeleton_pack.clone();
    let sk_idx = shallow_skeleton_idx.clone();
    let sk_prebuilt = shallow_prebuilt_index.clone();
    let sk_meta = shallow_metadata_hash.clone();
    let sk_meta_len = shallow_meta_data.len() as u64;
    let head_base_pack_count_for_p2 = head_built.base_packs.len();
    let phase2 = async move {
        let started = Instant::now();
        let res = build_full_in_background(
            &cas2,
            &mirror2,
            &repo_id2,
            &branch2,
            moving_branch2.as_deref(),
            &commit2,
            parent2,
            &default_branch2,
            &ref_store2,
            &storage2,
            &retention2,
            head_packs,
            sk_pack,
            sk_idx,
            sk_prebuilt,
            sk_meta,
            sk_meta_len,
            head_idx_bundle_hash,
            head_midx_hash,
            history_target,
            upload_conc,
            prev_levels_for_p2,
            prev_archive_frames_for_p2,
            prev_files_for_archive,
            prev_archive_commit,
            head_base_pack_count_for_p2,
            compression_level,
            t_total,
        )
        .await;
        match &res {
            Ok(()) => {
                admission_test_full_published(&commit2);
                info!(
                    "full clone ready for {} in {:?}",
                    &commit2[..7.min(commit2.len())],
                    started.elapsed()
                );
            }
            Err(e) => {
                error!(
                    "full clone build failed for {}: {e:#}",
                    repo_id2.storage_key()
                );
            }
        }
        res
    };

    // Every durable claim owns Head, Files, and Full. A process death before
    // this completes leaves the SQL claim stale for the next worker to reclaim.
    phase2.await.context("phase 2 (full history) build")?;
    ref_store.invalidate(repo_id, branch).await;
    if let Some(updated) = ref_store.load_branch(repo_id, branch).await?
        && updated.commit == commit
    {
        info = updated;
    }

    report_sync_phases(repo_id, branch, commit, &phases);
    if std::env::var_os("RIPCLONE_BENCH").is_some() {
        report_sync_bench(repo_id, branch, commit, &phases, storage, &info, mirror_dir);
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
    for p in &info.packs {
        add(&p.pack, &mut head_pack_bytes);
        add(&p.idx, &mut head_pack_bytes);
    }

    // Full-history / LSM sealed levels.
    for level in &info.history_levels {
        for p in &level.packs {
            add(&p.pack, &mut history_pack_bytes);
            add(&p.idx, &mut history_pack_bytes);
        }
    }

    // Archive chunks referenced directly from the ref.
    for h in &info.archive_chunks {
        add(h, &mut archive_chunk_bytes);
    }

    // Metadata: manifests, metadata chunks, skeleton/index, prebuilt index,
    // idx bundle, and MIDX.
    for hash in [
        &info.manifest,
        &info.shallow_clonepack.manifest,
        &info.shallow_clonepack.metadata_chunk,
        &info.shallow_clonepack.skeleton_pack,
        &info.shallow_clonepack.skeleton_idx,
        &info.shallow_clonepack.prebuilt_index,
        &info.shallow_clonepack.idx_bundle,
        &info.shallow_clonepack.midx,
        &info.full_clonepack.manifest,
        &info.full_clonepack.metadata_chunk,
    ] {
        add(hash, &mut metadata_bytes);
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

fn report_sync_phases(repo_id: &RepoId, branch: &str, commit: &str, phases: &SyncPhases) {
    let report = serde_json::json!({
        "kind": "sync-phases",
        "repo": repo_id.storage_key(),
        "branch": branch,
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
    branch: &str,
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
        "branch": branch,
        "commit": &commit[..7.min(commit.len())],
        "phases": phases,
        "storage_amplification": amplification,
    });
    info!("{}", report.to_string());
}

/// Phase 2 of two-phase publish: build the full-history artifacts and upgrade
/// the ref's full clonepack. The depth=1 clonepack is already live.
#[allow(clippy::too_many_arguments)]
async fn build_full_in_background(
    cas: &Cas,
    mirror_dir: &std::path::Path,
    repo_id: &RepoId,
    branch: &str,
    moving_branch: Option<&str>,
    commit: &str,
    parent: Option<String>,
    default_branch: &str,
    ref_store: &Arc<dyn RefStore>,
    storage: &crate::storage::StorageRef,
    retention: &Arc<Retention>,
    head_packs: Vec<(String, u64, String, u64)>,
    // Phase 1's shallow skeleton + prebuilt index (HEAD trees + HEAD index). The
    // full variant reuses these — the full history's commits+trees are already in
    // the history packs, so a separate full skeleton is redundant. (hashes)
    shallow_skeleton_pack: String,
    shallow_skeleton_idx: String,
    shallow_prebuilt_index: String,
    // Phase 1's shallow metadata chunk (files table + skeleton, no archive frames).
    // The editable full clonepack (phase 2a) reuses it verbatim: an editable depth=0
    // clone reads only the files table + packs, never the archive frames. Phase 2b
    // builds a frames-bearing metadata for files mode.
    shallow_metadata_hash: String,
    shallow_metadata_len: u64,
    _head_idx_bundle_hash: String,
    _head_midx_hash: String,
    history_target: u64,
    upload_conc: usize,
    prev_levels: Vec<crate::HistoryLevel>,
    prev_archive_frames: Vec<crate::ArchiveFrame>,
    // The prior sync's files table + commit, for the bounded archive: it hashes
    // only changed files and reuses frames for the unchanged prefix/suffix.
    prev_files: Vec<crate::clonepack::FileEntry>,
    prev_archive_commit: Option<String>,
    // How many of `head_packs` (above) are base packs; the rest are the cumulative
    // delta. When the delta's byte size exceeds RIPCLONE_HEAD_REBASE_BYTES this
    // phase rebases — rebuilds a fresh base at the current commit (off the depth=1
    // critical path) — so the delta never grows unbounded.
    head_base_pack_count: usize,
    // zstd level for archive frames, from the effective repo config.
    compression_level: i32,
    build_started_at: Instant,
) -> Result<()> {
    // Incremental history: build only the tail past the last sealed level; prior
    // levels are reused by hash from object storage (Tigris) — never rebuilt.
    let lsm_cfg = lsm_config();
    let sealed_tip: Option<String> = if lsm_cfg.enabled {
        prev_levels.last().map(|l| l.tip_commit.clone())
    } else {
        None
    };
    let archive_bundle_size = crate::archive::DEFAULT_ARCHIVE_CHUNK_SIZE;

    // Write a reachability bitmap once, before the heavy full enumerations
    // (skeleton + history). This is in the background phase, so it never delays
    // the depth=1 publish. Best-effort.
    let bm_dir = mirror_dir.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || git::write_bitmap(&bm_dir)).await;

    // History tail + the full zstd archive (deferred from phase 1), concurrently.
    // No full skeleton: the full variant reuses phase 1's shallow skeleton, and
    // the full history's commits+trees live in the history packs.
    //
    // The archive reuses unchanged frames (their raw bytes hash the same, so the
    // prior compressed chunk is reused — no recompress, no re-upload). When a prior
    // commit + frames are known, the bounded build also skips *reading* the
    // unchanged prefix/suffix and only touches the changed middle; it falls back to
    // the full read on its own when that doesn't apply. RIPCLONE_ARCHIVE_BOUNDED=0
    // forces the full read.
    let bounded = std::env::var("RIPCLONE_ARCHIVE_BOUNDED")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
        && !prev_archive_frames.is_empty()
        && prev_archive_commit.is_some();
    let prev_frame_map: std::collections::HashMap<String, (String, u64)> = prev_archive_frames
        .iter()
        .map(|f| (f.raw_hash.clone(), (f.chunk_hash.clone(), f.compressed_len)))
        .collect();
    let (mda, ca, cma, archive_storage) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        commit.to_string(),
        storage.clone(),
    );
    let archive_handle = tokio::task::spawn_blocking(move || {
        let b = ArchiveBuilder::new(&mda);
        if bounded {
            b.build_into_cas_bounded(
                &cma,
                &ca,
                Some(&archive_storage),
                compression_level,
                None,
                &prev_archive_frames,
                &prev_files,
                prev_archive_commit.as_deref().unwrap_or_default(),
                archive_bundle_size,
            )
        } else {
            b.build_into_cas_incremental(
                &cma,
                &ca,
                Some(&archive_storage),
                compression_level,
                None,
                &prev_frame_map,
                archive_bundle_size,
            )
        }
    });
    let (md2, c2, cm2, st2, lsm2) = (
        mirror_dir.to_path_buf(),
        cas.clone(),
        commit.to_string(),
        sealed_tip.clone(),
        lsm_cfg.enabled,
    );
    type BuiltHistory = (Vec<(String, u64, String, u64)>, bool);
    let history_handle = tokio::task::spawn_blocking(move || -> Result<BuiltHistory> {
        let b = PackBuilder::new(&md2, &c2);
        if lsm2 {
            let tail = b.build_history_tail(&cm2, st2.as_deref(), history_target)?;
            Ok((tail, true))
        } else {
            Ok((b.build_history_packs(&cm2, history_target)?, false))
        }
    });
    // History is enough to publish an editable clone: it reads only the files
    // table and the packs, never the archive. So publish as soon as history is
    // ready instead of waiting for the zstd archive (which only files mode needs).
    let t_editable = Instant::now();
    let (built_history, is_tail) = history_handle.await.context("history packs")??;

    // Once the cumulative delta grows past the threshold, rebuild a fresh base at
    // the current commit. depth=1 is already live, so this never blocks a clone.
    // The fresh base is kept only if the ref still points at our commit.
    let rebase_bytes = env_u64("RIPCLONE_HEAD_REBASE_BYTES", 128 * 1024 * 1024);
    let delta_bytes: u64 = head_packs
        .iter()
        .skip(head_base_pack_count)
        .map(|(_, pack_len, _, _)| *pack_len)
        .sum();
    let (head_packs, new_head_packs, rebased_base): (
        Vec<(String, u64, String, u64)>,
        Vec<(String, u64, String, u64)>,
        Option<Vec<crate::SizedPack>>,
    ) = if delta_bytes >= rebase_bytes {
        let head_target = 4 * 1024 * 1024;
        let (mdc, cc, cmc) = (mirror_dir.to_path_buf(), cas.clone(), commit.to_string());
        let base = tokio::task::spawn_blocking(move || {
            PackBuilder::new(&mdc, &cc).build_head_packs(&cmc, head_target)
        })
        .await
        .context("rebase head base")??;
        info!(
            "rebased HEAD base ({} MiB delta -> fresh base of {} packs)",
            delta_bytes / (1024 * 1024),
            base.len()
        );
        let sized: Vec<crate::SizedPack> = base.iter().map(tuple_to_sized).collect();
        (base.clone(), base, Some(sized))
    } else {
        (head_packs, Vec::new(), None)
    };

    // Flatten the history levels for the manifest; collect the freshly built packs
    // to upload and the levels to persist.
    let (history_packs, new_history_tuples, new_levels) = if is_tail {
        seal_and_compact(
            mirror_dir,
            cas,
            commit,
            prev_levels,
            sealed_tip,
            built_history,
            history_target,
            &lsm_cfg,
        )
        .await?
    } else {
        (built_history.clone(), built_history, Vec::new())
    };

    // Pack entries + idx bundle over head + history. Built once; the files manifest
    // below reuses them, since the packs are the same. MIDX is omitted (head packs
    // were evicted) — the client builds it.
    let full_tagged: Vec<(&(String, u64, String, u64), bool)> = head_packs
        .iter()
        .map(|p| (p, false))
        .chain(history_packs.iter().map(|p| (p, true)))
        .collect();
    let (full_entries, full_idx_bundle_ref, full_idx_bundle_hash) =
        assemble_variant(cas, storage, &full_tagged)?;
    // TEST-ONLY: give overlapping same-commit builds distinct idx bundles.
    let race_seq = phase2_race_seq();
    let (race_pre_editable_ms, race_pre_files_ms) = phase2_race_delays(race_seq);
    let (full_idx_bundle_ref, full_idx_bundle_hash) =
        phase2_race_salt_bundle(cas, full_idx_bundle_ref, full_idx_bundle_hash, race_seq)?;

    // The shallow metadata already has the files table and skeleton, and an
    // editable clone ignores the archive, so reuse it and leave archive_chunks
    // empty.
    let editable_manifest = make_manifest(
        commit,
        &parent,
        default_branch,
        &[],
        &shallow_metadata_hash,
        shallow_metadata_len,
        full_entries.clone(),
        None,
        full_idx_bundle_ref.clone(),
    )?;
    let editable_clonepack_hash = cas.put(&editable_manifest.encode_to_vec())?;

    // Upload the history packs+idx, the idx bundle, the manifest, and any rebase
    // base. Non-rebase head packs + the shallow skeleton are already in storage.
    let mut uploads: Vec<String> = vec![
        editable_clonepack_hash.clone(),
        full_idx_bundle_hash.clone(),
    ];
    for (p, _, i, _) in &new_history_tuples {
        uploads.push(p.clone());
        uploads.push(i.clone());
    }
    for (p, _, i, _) in &new_head_packs {
        uploads.push(p.clone());
        uploads.push(i.clone());
    }
    uploads.retain(|h| !h.is_empty());
    let idx_keep: std::collections::HashSet<String> = new_history_tuples
        .iter()
        .map(|(_, _, ih, _)| ih.clone())
        .chain(new_head_packs.iter().map(|(_, _, ih, _)| ih.clone()))
        .collect();
    upload_artifacts(cas, storage, uploads.clone(), upload_conc).await?;

    if let Ok(delay_for) = std::env::var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_COMMIT")
        && delay_for == commit
        && let Ok(ms) = std::env::var("RIPCLONE_TEST_EDITABLE_PUBLISH_DELAY_MS")
        && let Ok(ms) = ms.parse::<u64>()
    {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
    // TEST-ONLY: hold this build's editable publish so a concurrent same-commit
    // build passes its reuse check and starts (see phase2_race_delays).
    if race_pre_editable_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(race_pre_editable_ms)).await;
    }

    // Publish the editable full clonepack. archive_chunks stays empty until the
    // archive is built below; a files clone waits for it.
    {
        let mut info = ref_store
            .load_branch(repo_id, branch)
            .await?
            .ok_or_else(|| anyhow::anyhow!("ref vanished before editable publish"))?;
        if info.commit == commit {
            let mut all_packs = head_packs.clone();
            all_packs.extend(history_packs.iter().cloned());
            info.packs = pack_artifacts_of(&all_packs);
            info.skeleton_pack = shallow_skeleton_pack.clone();
            info.skeleton_idx = shallow_skeleton_idx.clone();
            info.prebuilt_index = shallow_prebuilt_index.clone();
            info.manifest = shallow_metadata_hash.clone();
            info.archive = String::new();
            info.archive_chunks = Vec::new();
            info.full_clonepack = crate::ClonepackArtifacts {
                manifest: editable_clonepack_hash.clone(),
                metadata_chunk: shallow_metadata_hash.clone(),
                skeleton_pack: shallow_skeleton_pack.clone(),
                skeleton_idx: shallow_skeleton_idx.clone(),
                prebuilt_index: shallow_prebuilt_index.clone(),
                midx: String::new(),
                idx_bundle: full_idx_bundle_hash.clone(),
                commit: commit.to_string(),
            };
            info.history_levels = new_levels;
            if let Some(sized) = rebased_base {
                info.head_base_commit = commit.to_string();
                info.head_base_packs = sized;
            }
            info.build_status = Some("archive building".to_string());
            admission_test_ref_store_write();
            ref_store
                .save_branch(repo_id, branch, &info)
                .await
                .with_context(|| {
                    format!(
                        "persist editable ref for {}@{branch}",
                        repo_id.storage_key()
                    )
                })?;
        }
    }
    settle_storage(cas, storage, retention, uploads, idx_keep).await;
    info!(
        "published editable full clone for {} in {:?}",
        &commit[..7.min(commit.len())],
        t_editable.elapsed()
    );

    // Test hook: hold the archive back so a test can observe the editable clone
    // being ready while files mode is not.
    if let Ok(ms) = std::env::var("RIPCLONE_TEST_ARCHIVE_DELAY_MS")
        && let Ok(ms) = ms.parse::<u64>()
    {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
    if let Ok(fail_for) = std::env::var("RIPCLONE_TEST_PHASE2_FAIL_COMMIT")
        && fail_for == commit
    {
        anyhow::bail!("forced phase-2 failure for {commit}");
    }
    // Test hook: panic (rather than return Err) inside the background Full task,
    // to exercise that a panicking background build is surfaced + marked failed
    // instead of silently stranding the ref at "full history building".
    if let Ok(panic_for) = std::env::var("RIPCLONE_TEST_PHASE2_PANIC_COMMIT")
        && panic_for == commit
    {
        panic!("forced phase-2 panic for {commit}");
    }

    // Now the zstd archive, which files mode needs.
    let t_archive = Instant::now();
    let archive_output = archive_handle.await.context("full archive")??;
    let archive_chunk_hashes = archive_output.download_bundle_hashes;
    let archive_meta = archive_output.metadata;
    let new_archive_chunks = archive_output.new_reuse_frame_hashes;
    let archive_frames = archive_output.archive_frames;
    info!(
        "archive {} frames ({} rebuilt)",
        archive_frames.len(),
        new_archive_chunks.len()
    );
    let fetch = |h: &str| -> Result<Vec<u8>> { cas.get(h).or_else(|_| storage.get(h)) };
    let mut full_meta = archive_meta;
    full_meta.skeleton_pack = fetch(&shallow_skeleton_pack)?;
    full_meta.skeleton_idx = fetch(&shallow_skeleton_idx)?;
    full_meta.prebuilt_index = fetch(&shallow_prebuilt_index)?;
    let full_meta_data = full_meta.encode_to_vec();
    let files_metadata_hash = cas.put(&full_meta_data)?;
    let archive_chunks = archive_chunk_refs(&archive_chunk_hashes, &full_meta)?;
    // Same packs + idx bundle as the editable manifest, now with the archive.
    let files_manifest = make_manifest(
        commit,
        &parent,
        default_branch,
        &archive_chunks,
        &files_metadata_hash,
        full_meta_data.len() as u64,
        full_entries,
        None,
        full_idx_bundle_ref,
    )?;
    let files_clonepack_hash = cas.put(&files_manifest.encode_to_vec())?;

    let uploads = archive_publish_upload_hashes(
        &files_metadata_hash,
        &files_clonepack_hash,
        &archive_chunk_hashes,
        &new_archive_chunks,
    );
    upload_artifacts(cas, storage, uploads.clone(), upload_conc).await?;

    // TEST-ONLY: hold this build's files publish so a concurrent same-commit build
    // completes its editable+files publishes in between (see phase2_race_delays).
    if race_pre_files_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(race_pre_files_ms)).await;
    }

    // Add the archive to the full clonepack, only if the ref still points at our
    // commit AND still carries *this* build's idx bundle. The second guard is the
    // ownership check: this is a load-modify-save with no lock, and same-commit
    // builds may overlap (each background Full build keeps running after `/sync`
    // returns, and should_replace_ref lets an equal-commit save win). This publish
    // only re-points `manifest`/`metadata_chunk`; it must not do that on top of
    // another build's `idx_bundle`, or the served idx_bundle_url (that build's
    // bundle) and manifest.idx_bundle (this build's) would diverge and every
    // editable clone would fail the idx-bundle hash check. If another same-commit
    // build now owns the full clonepack, skip — it publishes its own consistent
    // files variant.
    {
        let mut info = ref_store
            .load_branch(repo_id, branch)
            .await?
            .ok_or_else(|| anyhow::anyhow!("ref vanished before archive publish"))?;
        if info.commit == commit && info.full_clonepack.idx_bundle == full_idx_bundle_hash {
            info.manifest = files_metadata_hash.clone();
            info.archive = archive_chunk_hashes.first().cloned().unwrap_or_default();
            info.archive_chunks = archive_chunk_hashes.clone();
            info.full_clonepack.manifest = files_clonepack_hash.clone();
            info.full_clonepack.metadata_chunk = files_metadata_hash.clone();
            info.archive_frames = archive_frames;
            info.build_status = None;
            info.build_ms = Some(duration_ms(build_started_at.elapsed()));
            // Loud integrity guard: the served idx_bundle (full_clonepack.idx_bundle,
            // signed into idx_bundle_url) MUST equal the idx bundle inside the
            // manifest we now point clients at, or the clone's hash check fails.
            let manifest_bundle = files_manifest
                .idx_bundle
                .as_ref()
                .map(|c| crate::clonepack::hash_to_hex(&c.hash))
                .unwrap_or_default();
            anyhow::ensure!(
                manifest_bundle == info.full_clonepack.idx_bundle,
                "full clonepack idx_bundle integrity for {}@{branch}: served {} != manifest {manifest_bundle}",
                repo_id.storage_key(),
                info.full_clonepack.idx_bundle,
            );
            admission_test_ref_store_write();
            ref_store
                .save_branch(repo_id, branch, &info)
                .await
                .with_context(|| {
                    format!("persist files ref for {}@{branch}", repo_id.storage_key())
                })?;
            publish_moving_refs(
                ref_store,
                repo_id,
                branch,
                moving_branch,
                default_branch,
                &info,
            )
            .await?;
        }
    }
    settle_storage(
        cas,
        storage,
        retention,
        uploads,
        std::collections::HashSet::new(),
    )
    .await;
    info!(
        "published files archive for {} in {:?}",
        &commit[..7.min(commit.len())],
        t_archive.elapsed()
    );
    Ok(())
}

/// Run one durable claim to completion: mark `building`, sync all artifact
/// phases, then mark `done` or `failed` before acknowledgement.
///
/// This is shared by embedded and API-only workers.
pub async fn process_build_job(
    state: &ServerState,
    job: &BuildJob,
) -> Result<SyncBuildResult, BuildError> {
    process_build_job_with_foreground_release(state, job, None).await
}

async fn process_build_job_with_foreground_release(
    state: &ServerState,
    job: &BuildJob,
    foreground_release: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<SyncBuildResult, BuildError> {
    let repo_id = &job.repo_id;
    let branch = &job.branch;
    if let Err(e) = crate::validation::validate_object_id(&job.admitted_commit) {
        return Err(BuildError::permanent(format!(
            "build job has invalid admitted commit: {e}"
        )));
    }
    // Mark as building in the shared metadata store.
    if let Err(e) = update_job_build_status(state, job, branch, "building").await {
        error!(
            "build status update failed for {}@{branch}: {e:#}",
            repo_id.storage_key()
        );
    }
    invalidate_ref_response_cache(state, repo_id, branch);

    let start = std::time::Instant::now();
    let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
    let provider = match state.provider_registry.get(repo_id.provider.as_str()) {
        Some(p) => p.clone(),
        None => {
            let message = format!("unknown provider {}", repo_id.provider.as_str());
            if let Err(e) =
                update_job_build_status(state, job, branch, &format!("failed: {message}")).await
            {
                error!(
                    "build status update failed for {}@{branch}: {e:#}",
                    repo_id.storage_key()
                );
            }
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
    let repo_config = effective_repo_config(state, repo_id, branch).await;
    let result = do_sync(
        &state.cas,
        &mirror_dir,
        repo_id,
        branch,
        &job.admitted_commit,
        job.admitted_default_branch.as_deref(),
        &state.ref_store,
        &state.storage,
        &state.retention,
        &provider,
        job.credential.as_ref(),
        &repo_config,
        &lock,
        foreground_release,
    )
    .await;

    // Resolve HEAD to the concrete default branch for cache/log keys.
    let effective_branch = match &result {
        Ok(result) if branch == "HEAD" => result.info.default_branch.clone(),
        _ => branch.clone(),
    };
    match &result {
        Ok(result) => {
            let info = &result.info;
            state.metrics.record_build_completed(start.elapsed());
            state.metrics.record_sync_phases((&result.phases).into());
            if let Err(e) = update_job_build_status(state, job, &effective_branch, "done").await {
                error!(
                    "build status update failed for {}@{effective_branch} {}: {e:#}",
                    repo_id.storage_key(),
                    info.commit
                );
            }
            invalidate_mirror_fresh(
                state,
                &format!("{}/{effective_branch}", repo_id.storage_key()),
            );
            invalidate_mirror_fresh(state, &format!("{}/HEAD", repo_id.storage_key()));
            invalidate_ref_response_cache(state, repo_id, &effective_branch);
            info!(
                "background build completed for {}@{effective_branch}",
                repo_id.storage_key()
            );
            Ok(result.clone())
        }
        Err(e) => {
            // Classify first: only *permanent* failures are terminal in the
            // metadata store. A retryable error is requeued by `SqlJobQueue::ack`
            // (bounded by attempts); writing `failed: …` here would make
            // `/status` look terminal while the queue still has the job — the
            // stale-until-repushed mode A7 was meant to kill.
            let classified = classify_build_error(e);
            if let Some(status) = terminal_metadata_status(&classified) {
                state.metrics.record_build_failed();
                if let Err(status_err) =
                    update_job_build_status(state, job, &effective_branch, &status).await
                {
                    error!(
                        "build status update failed for {}@{effective_branch}: {status_err:#}",
                        repo_id.storage_key()
                    );
                }
                warn!(
                    "background build failed for {}@{effective_branch}: {e}",
                    repo_id.storage_key()
                );
            } else {
                warn!(
                    "background build transient failure for {}@{effective_branch} \
                     (queue will requeue if under attempt cap): {e}",
                    repo_id.storage_key()
                );
            }
            invalidate_ref_response_cache(state, repo_id, &effective_branch);
            // The deterministic failure hook represents a settled build
            // result. Signal it only after terminal metadata (when applicable)
            // and cache invalidation are visible to observers.
            admission_test_build_failure(Some(&job.admitted_commit), classified.message());
            Err(classified)
        }
    }
}

/// Metadata status string for a terminal build failure, or `None` when the
/// queue may still requeue the job (retryable). Callers must not write a
/// terminal `failed: …` for retryable errors.
fn terminal_metadata_status(err: &BuildError) -> Option<String> {
    if err.is_retryable() {
        None
    } else {
        Some(format!("failed: {}", err.message()))
    }
}

/// Write a terminal `failed: …` build status for the job's admitted commit. Used by the
/// cross-process worker after `ack` dead-letters a retryable error at the
/// attempts cap — `process_build_job` intentionally leaves metadata non-terminal
/// for retryable failures so intermediate retries don't look permanent.
pub async fn mark_admitted_build_failed(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
    admitted_commit: &str,
    message: &str,
) -> Result<()> {
    let exact_branch = exact_ref_store_key(branch, admitted_commit);
    let _ = update_build_status(
        state,
        repo_id,
        &exact_branch,
        admitted_commit,
        &format!("failed: {message}"),
    )
    .await?;
    Ok(())
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

async fn enqueue_direct_build(
    state: &ServerState,
    job: BuildJob,
    moving_authorized: bool,
) -> Result<(), String> {
    enqueue_admitted_build(state, job, moving_authorized)
        .await
        .map(|_| ())
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
                    admission_test_before_claim().await;
                    match queue.claim(&worker_id).await {
                        Ok(Some(claimed)) => break claimed,
                        Ok(None) => {
                            admission_test_embedded_idle_wait().await;
                            tokio::select! {
                                () = &mut notified => {
                                    admission_test_embedded_wake(false);
                                }
                                () = tokio::time::sleep(Duration::from_millis(250)) => {
                                    admission_test_embedded_wake(true);
                                }
                            }
                        }
                        Err(error) => {
                            error!("embedded worker claim failed: {error:#}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                };
                admission_test_after_claim().await;
                let (foreground_release, foreground_released) = tokio::sync::oneshot::channel();
                let worker_state = state.clone();
                let worker_queue = queue.clone();
                let owner = worker_id.clone();
                tokio::spawn(async move {
                    let repo_id = claimed.repo_id();
                    let branch = claimed.branch.clone();
                    let admitted_commit = claimed.admitted_commit.clone();
                    let heartbeat_queue = worker_queue.clone();
                    let heartbeat_worker = owner.clone();
                    let heartbeat_job = claimed.id;
                    let heartbeat_interval = Duration::from_secs(
                        (worker_queue.heartbeat_timeout_secs() / 3).max(1) as u64,
                    );
                    let heartbeat = tokio::spawn(async move {
                        loop {
                            if let Err(error) = heartbeat_queue
                                .heartbeat(&heartbeat_worker, Some(heartbeat_job))
                                .await
                            {
                                error!("embedded worker heartbeat failed: {error:#}");
                            }
                            tokio::time::sleep(heartbeat_interval).await;
                        }
                    });
                    let mut foreground_release = Some(foreground_release);
                    let result = match crate::validation::validate_object_id(&admitted_commit) {
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
                                    branch: branch.clone(),
                                    admitted_commit: admitted_commit.clone(),
                                    admitted_default_branch: claimed.admitted_default_branch,
                                    credential,
                                    size_bytes: None,
                                };
                                let state = worker_state.clone();
                                let release_for_build = foreground_release.take();
                                match tokio::spawn(async move {
                                    process_build_job_with_foreground_release(
                                        &state,
                                        &job,
                                        release_for_build,
                                    )
                                    .await
                                })
                                .await
                                {
                                    Ok(result) => result,
                                    Err(error) => Err(BuildError::retryable(format!(
                                        "build task panicked: {error}"
                                    ))),
                                }
                            }
                        },
                    };
                    // An error before Head publication drops the sender here,
                    // releasing the foreground slot before terminal ack.
                    drop(foreground_release);
                    let retryable = result.as_ref().err().is_some_and(BuildError::is_retryable);
                    match worker_queue
                        .ack(claimed.id, &owner, result.map(|_| ()))
                        .await
                    {
                        Ok(true) if retryable => {
                            if let Ok(JobState::Failed(error)) =
                                worker_queue.job_status(claimed.id).await
                                && let Err(status_error) = mark_admitted_build_failed(
                                    &worker_state,
                                    &repo_id,
                                    &branch,
                                    &admitted_commit,
                                    &error,
                                )
                                .await
                            {
                                error!(
                                    "failed to mark embedded job {} dead-lettered: {status_error:#}",
                                    claimed.id
                                );
                            }
                        }
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
                    heartbeat.abort();
                    let _ = heartbeat.await;
                    if let Err(error) = worker_queue.heartbeat(&owner, None).await {
                        error!("embedded worker idle heartbeat failed: {error:#}");
                    }
                });
                // The sender fires after Head is durably published. If the
                // build fails earlier, sender drop also releases this slot.
                let _ = foreground_released.await;
            }
        });
    }
}

/// One polling pass: for every known repo+branch, cheaply check the upstream tip
/// (`ls-remote`, under the fetch cap) and trigger a build if that commit isn't
/// already built. Catches pushes that arrived without a webhook/Actions trigger,
/// so build-before-clone still holds. Best-effort: per-repo errors are logged and
/// skipped. Returns the number of builds triggered. Exposed for tests.
pub async fn poll_once(state: &ServerState) -> usize {
    let repos = match state.ref_store.list().await {
        Ok(r) => r,
        Err(e) => {
            warn!("poll: list repos failed: {e}");
            return 0;
        }
    };
    let mut triggered = 0;
    let mut seen_repos = std::collections::HashSet::new();
    for repo_id in repos {
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
        let branches = match state.ref_store.list_branches(&repo_id).await {
            Ok(b) => b,
            Err(e) => {
                warn!("poll: list_branches {} failed: {e}", repo_id.storage_key());
                continue;
            }
        };
        for branch in branches {
            if matches!(
                state.ref_store.load_branch(&repo_id, &branch).await,
                Ok(Some(info)) if info.internal_exact_result
            ) {
                continue;
            }
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
                admission_test_tip_probe();
                git::ls_remote_tip_async(&provider, &repo_id, &branch, credential.as_ref()).await
            };
            let Ok(Some(tip)) = tip else {
                continue; // unknown ref / probe failed
            };
            // Tip moved and isn't built — admit this exact target. Readiness is
            // checked by the shared admission path; it does not mutate metadata.
            let admitted_branch = if branch == "HEAD" {
                tip.default_branch.clone().unwrap_or_else(|| branch.clone())
            } else {
                branch.clone()
            };
            match trigger_build(
                state,
                &repo_id,
                &admitted_branch,
                tip.commit.clone(),
                tip.default_branch.clone(),
                true,
            )
            .await
            {
                Ok(EnqueueOutcome::Enqueued) => {
                    triggered += 1;
                    info!(
                        "poll: triggered build for {}@{branch} at {}",
                        repo_id.storage_key(),
                        &tip.commit[..7.min(tip.commit.len())]
                    );
                }
                Ok(EnqueueOutcome::Coalesced) => {}
                Ok(EnqueueOutcome::Full) => {}
                Err(e) => warn!(
                    "poll: trigger {}@{branch} failed: {e}",
                    repo_id.storage_key()
                ),
            }
        }
    }
    triggered
}

/// Spawn the polling-fallback loop. `interval == 0` disables it. Mirrors the
/// retention/remote-gc interval-loop pattern.
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

async fn update_build_status(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
    commit: &str,
    status: &str,
) -> Result<bool> {
    state
        .ref_store
        .update_build_status(repo_id, branch, commit, status)
        .await
        .with_context(|| {
            format!(
                "update build status for {}@{branch} {commit}",
                repo_id.storage_key()
            )
        })
}

/// Status writes for every immutable job are fenced by its admitted commit.
/// A job that fails before publication must update only its exact result, never
/// a previously served or later moving projection.
async fn update_job_build_status(
    state: &ServerState,
    job: &BuildJob,
    branch: &str,
    status: &str,
) -> Result<Option<String>> {
    let commit = job.admitted_commit.as_str();
    let exact_branch = exact_ref_store_key(branch, commit);
    if status == "building" {
        let existing = state
            .ref_store
            .load_branch(&job.repo_id, &exact_branch)
            .await?;
        if existing.as_ref().is_some_and(|info| {
            info.build_status.as_deref() == Some(crate::remote_gc::EVICTED_BUILD_STATUS)
                || (!info.full_clonepack.manifest.is_empty()
                    && info.full_clonepack.commit == commit)
        }) {
            return Ok(Some(commit.to_string()));
        }
        if existing.is_none() {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|duration| duration.as_secs());
            let pending = RefInfo {
                internal_exact_result: true,
                commit: commit.to_string(),
                default_branch: job
                    .admitted_default_branch
                    .clone()
                    .unwrap_or_else(|| branch.to_string()),
                build_status: Some(status.to_string()),
                synced_at: now,
                last_accessed_at: now,
                ..Default::default()
            };
            state
                .ref_store
                .save_branch(&job.repo_id, &exact_branch, &pending)
                .await
                .with_context(|| {
                    format!(
                        "create exact build status for {}@{exact_branch} {commit}",
                        job.repo_id.storage_key()
                    )
                })?;
            return Ok(Some(commit.to_string()));
        }
        return update_current_build_status(state, &job.repo_id, &exact_branch, status).await;
    }
    update_build_status(state, &job.repo_id, &exact_branch, commit, status)
        .await
        .map(|updated| updated.then(|| commit.to_string()))
}

async fn update_current_build_status(
    state: &ServerState,
    repo_id: &RepoId,
    branch: &str,
    status: &str,
) -> Result<Option<String>> {
    let Some(info) = state.ref_store.load_branch(repo_id, branch).await? else {
        return Ok(None);
    };
    if info.commit.is_empty() {
        return Ok(None);
    }
    // An evicted ref's artifacts were deleted, but its artifact-pointer fields
    // (full_clonepack, archive_chunks) are left intact. The plain "building"
    // marker set at the start of a rebuild produces no fresh artifacts yet, so
    // overwriting the eviction sentinel with it would hide that those pointers
    // no longer exist. Keep it evicted until phase 1 replaces the row with
    // freshly built artifacts.
    if status == "building"
        && info.build_status.as_deref() == Some(crate::remote_gc::EVICTED_BUILD_STATUS)
    {
        return Ok(Some(info.commit));
    }
    let commit = info.commit.clone();
    state
        .ref_store
        .update_build_status(repo_id, branch, &commit, status)
        .await
        .with_context(|| {
            format!(
                "update build status for {}@{branch} {commit}",
                repo_id.storage_key()
            )
        })?;
    Ok(Some(commit))
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

    let rate_burst: u32 = env::var("RIPCLONE_RATE_LIMIT_BURST")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let rate_per_sec: f64 = env::var("RIPCLONE_RATE_LIMIT_PER_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10.0);
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
    let retention_interval: Duration = env::var("RIPCLONE_RETENTION_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(300));
    Retention::clone(&b.retention).spawn(retention_interval);

    const DEFAULT_REMOTE_GC_INTERVAL_SECS: u64 = 3600;
    let remote_gc_interval: Duration = env::var("RIPCLONE_REMOTE_GC_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_REMOTE_GC_INTERVAL_SECS));
    let mut gc_config = GcConfig::from_env();
    // Floor the grace at the longest signed-URL lifetime, read from the same
    // place ref responses sign URLs, so a client still holding a valid URL can
    // always finish its clone before any of its chunks become collectible.
    let url_ttl_floor = ref_signed_url_ttl(false).max(ref_signed_url_ttl(true));
    let configured_grace = gc_config.grace_period;
    gc_config.floor_grace(url_ttl_floor);
    info!(
        "remote GC effective grace = {:?} (configured {:?}, signed-URL TTL floor {:?}), warm TTL = {:?}",
        gc_config.grace_period, configured_grace, url_ttl_floor, gc_config.warm_ttl
    );
    let remote_gc = RemoteGc::new(b.storage.clone(), b.ref_store.clone(), gc_config);
    remote_gc.spawn(remote_gc_interval);

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
        repo_config: Arc::new(crate::repo_config::RepoConfigStore::new(b.storage.clone())),
        storage: b.storage,
        repo_root: repo_root.to_path_buf(),
        ref_store: b.ref_store,
        provider_registry,
        broker,
        token_hash: Some(token_hash),
        jwt,
        metrics,
        rate_limiter,
        retention: b.retention,
        build_queue,
        control_db: Some(control_db),
        worker_queue: Some(worker_queue.clone()),
        build_queue_depth: Arc::new(AtomicUsize::new(0)),
        oidc_verifier,
        webhook_config,
        sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        mirror_freshness: Arc::new(std::sync::Mutex::new(HashMap::new())),
        mirror_fresh_ttl: mirror_fresh_ttl_from_env(),
        ref_response_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
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

    #[test]
    fn exact_result_requires_the_requested_clonepack_variant() {
        let full = crate::ClonepackArtifacts {
            commit: "a".repeat(40),
            manifest: "full-manifest".to_string(),
            metadata_chunk: "full-metadata".to_string(),
            idx_bundle: "full-idx-bundle".to_string(),
            ..Default::default()
        };
        let shallow = crate::ClonepackArtifacts {
            commit: "a".repeat(40),
            manifest: "shallow-manifest".to_string(),
            metadata_chunk: "shallow-metadata".to_string(),
            idx_bundle: "shallow-idx-bundle".to_string(),
            ..Default::default()
        };
        let full_only = RefInfo {
            commit: "a".repeat(40),
            full_clonepack: full.clone(),
            ..Default::default()
        };
        assert!(!exact_ref_info_serves_commit(
            &full_only,
            "shallow",
            &"a".repeat(40)
        ));
        assert!(exact_ref_info_serves_commit(
            &full_only,
            "full",
            &"a".repeat(40)
        ));

        let shallow_only = RefInfo {
            commit: "a".repeat(40),
            shallow_clonepack: shallow,
            ..Default::default()
        };
        assert!(exact_ref_info_serves_commit(
            &shallow_only,
            "shallow",
            &"a".repeat(40)
        ));
        assert!(!exact_ref_info_serves_commit(
            &shallow_only,
            "full",
            &"a".repeat(40)
        ));
    }

    #[test]
    fn exact_result_rejects_empty_evicted_and_mismatched_artifacts() {
        let commit = "a".repeat(40);
        let mut info = RefInfo {
            commit: commit.clone(),
            full_clonepack: crate::ClonepackArtifacts {
                commit: commit.clone(),
                manifest: "manifest".to_string(),
                metadata_chunk: "metadata".to_string(),
                idx_bundle: "idx-bundle".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(exact_ref_info_serves_commit(&info, "full", &commit));
        info.full_clonepack.manifest.clear();
        assert!(!exact_ref_info_serves_commit(&info, "full", &commit));
        info.full_clonepack.manifest = "manifest".to_string();
        info.full_clonepack.metadata_chunk.clear();
        assert!(!exact_ref_info_serves_commit(&info, "full", &commit));
        info.full_clonepack.metadata_chunk = "metadata".to_string();
        info.full_clonepack.idx_bundle.clear();
        assert!(!exact_ref_info_serves_commit(&info, "full", &commit));
        info.full_clonepack.idx_bundle = "idx-bundle".to_string();
        info.full_clonepack.commit = "b".repeat(40);
        assert!(!exact_ref_info_serves_commit(&info, "full", &commit));
        info.full_clonepack.commit = commit.clone();
        info.build_status = Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string());
        assert!(!exact_ref_info_serves_commit(&info, "full", &commit));
    }

    #[test]
    fn clonepack_seed_requires_one_explicit_matching_commit() {
        let commit = "a".repeat(40);
        let mut info = RefInfo {
            commit: commit.clone(),
            full_clonepack: crate::ClonepackArtifacts {
                commit: commit.clone(),
                manifest: "manifest".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut manifest = ClonepackManifest {
            commit: commit.clone(),
            ..Default::default()
        };
        assert_eq!(
            validate_clonepack_seed_identity(&info, &manifest).unwrap(),
            commit
        );

        info.full_clonepack.commit.clear();
        assert!(
            validate_clonepack_seed_identity(&info, &manifest)
                .unwrap_err()
                .to_string()
                .contains("missing its artifact commit")
        );

        info.full_clonepack.commit = commit.clone();
        manifest.commit = "b".repeat(40);
        assert!(
            validate_clonepack_seed_identity(&info, &manifest)
                .unwrap_err()
                .to_string()
                .contains("manifest commit")
        );
    }

    #[tokio::test]
    async fn pending_body_includes_the_selected_branch() {
        let response = artifact_pending_response(&"a".repeat(40), "rélease/東京", 3);
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
    fn terminal_metadata_only_for_permanent_build_errors() {
        // Retryable must not write a terminal metadata status — /status would
        // look failed while SqlJobQueue::ack still requeues under the cap.
        assert_eq!(
            terminal_metadata_status(&BuildError::retryable("storage 503")),
            None
        );
        assert_eq!(
            terminal_metadata_status(&BuildError::permanent("bad repo")),
            Some("failed: bad repo".to_string())
        );
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
    }

    impl TestSqlRefStore {
        fn new(path: PathBuf) -> Self {
            Self {
                path,
                inner: tokio::sync::OnceCell::new(),
            }
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
        async fn load(&self, repo_id: &RepoId) -> anyhow::Result<Option<RefInfo>> {
            self.store().await?.load(repo_id).await
        }
        async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> anyhow::Result<()> {
            self.store().await?.save(repo_id, info).await
        }
        async fn list(&self) -> anyhow::Result<Vec<RepoId>> {
            self.store().await?.list().await
        }
        async fn load_branch(
            &self,
            repo_id: &RepoId,
            branch: &str,
        ) -> anyhow::Result<Option<RefInfo>> {
            self.store().await?.load_branch(repo_id, branch).await
        }
        async fn save_branch(
            &self,
            repo_id: &RepoId,
            branch: &str,
            info: &RefInfo,
        ) -> anyhow::Result<()> {
            self.store().await?.save_branch(repo_id, branch, info).await
        }
        async fn update_build_status(
            &self,
            repo_id: &RepoId,
            branch: &str,
            expected_commit: &str,
            status: &str,
        ) -> anyhow::Result<bool> {
            self.store()
                .await?
                .update_build_status(repo_id, branch, expected_commit, status)
                .await
        }
        async fn touch_last_accessed_at(
            &self,
            repo_id: &RepoId,
            branch: &str,
            expected_commit: &str,
        ) -> anyhow::Result<bool> {
            self.store()
                .await?
                .touch_last_accessed_at(repo_id, branch, expected_commit)
                .await
        }
        async fn delete_branch(&self, repo_id: &RepoId, branch: &str) -> anyhow::Result<()> {
            self.store().await?.delete_branch(repo_id, branch).await
        }
        async fn list_branches(&self, repo_id: &RepoId) -> anyhow::Result<Vec<String>> {
            self.store().await?.list_branches(repo_id).await
        }
        async fn add_repo(&self, repo: &AddedRepo) -> anyhow::Result<()> {
            self.store().await?.add_repo(repo).await
        }
        async fn load_added_repo(&self, repo_id: &RepoId) -> anyhow::Result<Option<AddedRepo>> {
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

    fn test_state(tmp: &tempfile::TempDir) -> ServerState {
        let cas_root = tmp.path().join("cas");
        let cas = Cas::new(&cas_root).unwrap();
        let storage = crate::storage::local(&cas_root).unwrap();
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&repo_root).unwrap();
        let ref_store: Arc<dyn RefStore> =
            Arc::new(TestSqlRefStore::new(repo_root.join("test-control-refs.db")));
        let token_hash = hex::encode(Sha256::digest("secret"));
        let metrics = Metrics::new();
        let retention = Arc::new(Retention::new(cas.clone(), metrics.clone()).unwrap());
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
            repo_config: Arc::new(crate::repo_config::RepoConfigStore::new(storage.clone())),
            storage,
            repo_root,
            ref_store,
            provider_registry,
            broker,
            token_hash: Some(token_hash),
            jwt: None,
            metrics,
            rate_limiter: RateLimiter::new(100, 100.0),
            retention,
            build_queue,
            control_db: None,
            worker_queue: None,
            build_queue_depth: Arc::new(AtomicUsize::new(0)),
            oidc_verifier: None,
            // No webhook secret here (worker has no HTTP; tests install their own).
            webhook_config: Arc::new(WebhookConfig::empty()),
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            mirror_freshness: Arc::new(std::sync::Mutex::new(HashMap::new())),
            mirror_fresh_ttl: Duration::from_secs(60),
            ref_response_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
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

    struct CountingGetStorage {
        inner: StorageRef,
        target_hash: String,
        hits: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::storage::StorageBackend for CountingGetStorage {
        fn get(&self, hash: &str) -> Result<Vec<u8>> {
            if hash == self.target_hash {
                self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            self.inner.get(hash)
        }

        fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>> {
            self.inner.get_range(hash, start, len)
        }

        fn put(&self, hash: &str, data: &[u8]) -> Result<()> {
            self.inner.put(hash, data)
        }

        async fn put_async(&self, hash: &str, data: &[u8]) -> Result<()> {
            self.inner.put_async(hash, data).await
        }

        async fn put_file_async(&self, hash: &str, path: &std::path::Path) -> Result<()> {
            self.inner.put_file_async(hash, path).await
        }

        async fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get_meta(key).await
        }

        async fn put_meta(&self, key: &str, data: &[u8]) -> Result<()> {
            self.inner.put_meta(key, data).await
        }

        fn size(&self, hash: &str) -> Result<u64> {
            self.inner.size(hash)
        }

        fn signed_url(&self, hash: &str, expires_in: Duration) -> Option<String> {
            self.inner.signed_url(hash, expires_in)
        }

        fn is_remote(&self) -> bool {
            self.inner.is_remote()
        }

        fn regions(&self) -> Vec<String> {
            self.inner.regions()
        }

        fn delete(&self, hash: &str) -> Result<()> {
            self.inner.delete(hash)
        }

        fn delete_batch(&self, hashes: &[String]) -> Result<u64> {
            self.inner.delete_batch(hashes)
        }

        fn list_hashes(&self) -> Result<Vec<crate::storage::HashEntry>> {
            self.inner.list_hashes()
        }

        fn health(&self) -> Result<()> {
            self.inner.health()
        }
    }

    /// Storage wrapper that returns byte-corrupted (but same-length) data for one
    /// target hash, simulating clonepack bit-rot / a partially corrupt seed object.
    struct CorruptingGetStorage {
        inner: StorageRef,
        target_hash: String,
        hits: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::storage::StorageBackend for CorruptingGetStorage {
        fn get(&self, hash: &str) -> Result<Vec<u8>> {
            let mut data = self.inner.get(hash)?;
            if hash == self.target_hash && !data.is_empty() {
                self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Flip a byte: keeps the length (so the size check passes) but
                // makes the content hash mismatch what the manifest recorded.
                data[0] ^= 0xff;
            }
            Ok(data)
        }

        fn get_range(&self, hash: &str, start: u64, len: u64) -> Result<Vec<u8>> {
            self.inner.get_range(hash, start, len)
        }

        fn put(&self, hash: &str, data: &[u8]) -> Result<()> {
            self.inner.put(hash, data)
        }

        async fn put_async(&self, hash: &str, data: &[u8]) -> Result<()> {
            self.inner.put_async(hash, data).await
        }

        async fn put_file_async(&self, hash: &str, path: &std::path::Path) -> Result<()> {
            self.inner.put_file_async(hash, path).await
        }

        async fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get_meta(key).await
        }

        async fn put_meta(&self, key: &str, data: &[u8]) -> Result<()> {
            self.inner.put_meta(key, data).await
        }

        fn size(&self, hash: &str) -> Result<u64> {
            self.inner.size(hash)
        }

        fn signed_url(&self, hash: &str, expires_in: Duration) -> Option<String> {
            self.inner.signed_url(hash, expires_in)
        }

        fn is_remote(&self) -> bool {
            self.inner.is_remote()
        }

        fn regions(&self) -> Vec<String> {
            self.inner.regions()
        }

        fn delete(&self, hash: &str) -> Result<()> {
            self.inner.delete(hash)
        }

        fn delete_batch(&self, hashes: &[String]) -> Result<u64> {
            self.inner.delete_batch(hashes)
        }

        fn list_hashes(&self) -> Result<Vec<crate::storage::HashEntry>> {
            self.inner.list_hashes()
        }

        fn health(&self) -> Result<()> {
            self.inner.health()
        }
    }

    fn git_stdout(repo: &std::path::Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    async fn do_sync_for_test(
        state: &ServerState,
        repo_id: &RepoId,
        branch: &str,
        provider: &ProviderInstance,
    ) -> SyncBuildResult {
        let tip = git::ls_remote_tip_async(provider, repo_id, branch, None)
            .await
            .unwrap()
            .expect("test branch tip");
        let effective_branch = if branch == "HEAD" {
            tip.default_branch
                .clone()
                .filter(|branch| !branch.is_empty())
                .unwrap_or_else(|| branch.to_string())
        } else {
            branch.to_string()
        };
        let job = BuildJob {
            repo_id: repo_id.clone(),
            branch: effective_branch.clone(),
            admitted_commit: tip.commit.clone(),
            admitted_default_branch: tip.default_branch.clone(),
            credential: None,
            size_bytes: None,
        };
        if let Some(admission) = prepare_exact_admission(state, &job, true)
            .await
            .expect("prepare exact test admission")
        {
            state
                .ref_store
                .save_branch(&job.repo_id, &admission.exact_branch, &admission.pending)
                .await
                .expect("save exact test admission");
            if let Some((branch, tail)) = admission.tail {
                state
                    .ref_store
                    .save_branch(&job.repo_id, &branch, &tail)
                    .await
                    .expect("save moving test admission");
            }
        }
        let mirror_dir = state.repo_root.join(repo_id.mirror_dir_name());
        let lock = repo_lock(&state.sync_locks, repo_id).await;
        do_sync(
            &state.cas,
            &mirror_dir,
            repo_id,
            &effective_branch,
            &tip.commit,
            tip.default_branch.as_deref(),
            &state.ref_store,
            &state.storage,
            &state.retention,
            provider,
            None,
            &crate::repo_config::RepoConfig::default(),
            &lock,
            None,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn cold_sync_seeds_missing_mirror_from_storage_clonepack_then_fetches_delta() {
        let _env = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base.path().join("acme").join("seed.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let c1 = crate::test_fixture::commit(&origin, &[("a.txt", b"1\n")]);

        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let repo_id = RepoId::github("acme/seed");
        let provider = state.provider_registry.get("github").unwrap().clone();
        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path()) };

        let first = do_sync_for_test(&state, &repo_id, "main", &provider).await;
        assert_eq!(first.info.commit, c1);
        let seed_manifest = first.info.full_clonepack.manifest.clone();
        assert!(
            !seed_manifest.is_empty(),
            "first build published full clonepack"
        );

        // The exact pack filenames the seed will install, derived from the stored
        // full clonepack. Used below to prove the delta fetch reused the seeded
        // objects rather than silently doing a full re-clone.
        let seed_pack_names: std::collections::HashSet<String> = {
            let manifest_bytes = state.storage.get(&seed_manifest).unwrap();
            let manifest = ClonepackManifest::decode(manifest_bytes.as_slice()).unwrap();
            manifest
                .packs
                .iter()
                .map(|entry| {
                    let ph = hash_to_hex(&entry.pack.as_ref().unwrap().hash);
                    let pack_bytes = state.storage.get(&ph).unwrap();
                    format!(
                        "pack-{}.pack",
                        hex::encode(&pack_bytes[pack_bytes.len() - 20..])
                    )
                })
                .collect()
        };
        assert!(!seed_pack_names.is_empty());

        std::fs::remove_dir_all(state.repo_root.join(repo_id.mirror_dir_name())).unwrap();
        let c2 =
            crate::test_fixture::commit(&origin, &[("a.txt", b"2\n"), ("dir/b.txt", b"delta\n")]);

        let seed_hits = Arc::new(AtomicUsize::new(0));
        state.storage = Arc::new(CountingGetStorage {
            inner: state.storage.clone(),
            target_hash: seed_manifest,
            hits: seed_hits.clone(),
        });

        let second = do_sync_for_test(&state, &repo_id, "main", &provider).await;
        assert_eq!(second.info.commit, c2);
        assert_eq!(
            seed_hits.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "cold sync should attempt the prior exact clonepack seed once"
        );

        let seeded_mirror = state.repo_root.join(repo_id.mirror_dir_name());
        let full_mirror = tmp.path().join("full-clone-baseline.git");
        git::sync_bare_mirror(&full_mirror, &provider, &repo_id, "main", None, None).unwrap();
        assert_eq!(
            git_stdout(
                &seeded_mirror,
                &["rev-parse", &format!("{}^{{tree}}", second.info.commit)],
            ),
            git_stdout(&full_mirror, &["rev-parse", "main^{tree}"]),
            "seeded-fetch mirror tree must match full-clone mirror tree"
        );
        // Byte-identical guarantee also requires the resolved branch commit to
        // match, not just its tree (two distinct commits can share a tree).
        assert_eq!(
            git_stdout(&seeded_mirror, &["rev-parse", &second.info.commit]),
            git_stdout(&full_mirror, &["rev-parse", "main"]),
            "seeded-fetch mirror commit must match full-clone mirror commit"
        );
        // Delta efficiency: the seeded pack must survive the fetch, proving the
        // fetch only negotiated the delta on top of the reused seed objects rather
        // than re-downloading the full history.
        let after_pack_names: std::collections::HashSet<String> =
            std::fs::read_dir(seeded_mirror.join("objects").join("pack"))
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".pack"))
                .collect();
        assert!(
            seed_pack_names.iter().any(|n| after_pack_names.contains(n)),
            "seeded pack must survive the delta fetch (proves reuse, not full re-clone): \
             seeded={seed_pack_names:?} after={after_pack_names:?}"
        );
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn cold_sync_corrupt_seed_pack_falls_back_to_full_clone() {
        let _env = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base.path().join("acme").join("corruptseed.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        crate::test_fixture::commit(&origin, &[("a.txt", b"1\n")]);

        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let repo_id = RepoId::github("acme/corruptseed");
        let provider = state.provider_registry.get("github").unwrap().clone();
        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path()) };

        let first = do_sync_for_test(&state, &repo_id, "main", &provider).await;
        let seed_manifest = first.info.full_clonepack.manifest.clone();

        // Pick the first seed pack hash and arrange for storage to return a
        // corrupt (same-length) copy of it: a partially corrupt clonepack.
        let manifest_bytes = state.storage.get(&seed_manifest).unwrap();
        let manifest = ClonepackManifest::decode(manifest_bytes.as_slice()).unwrap();
        let corrupt_pack_hash = hash_to_hex(&manifest.packs[0].pack.as_ref().unwrap().hash);

        std::fs::remove_dir_all(state.repo_root.join(repo_id.mirror_dir_name())).unwrap();
        let c2 = crate::test_fixture::commit(
            &origin,
            &[("a.txt", b"2\n"), ("corrupt.txt", b"detect me\n")],
        );

        let seed_attempts = Arc::new(AtomicUsize::new(0));
        state.storage = Arc::new(CorruptingGetStorage {
            inner: state.storage.clone(),
            target_hash: corrupt_pack_hash,
            hits: seed_attempts.clone(),
        });

        // The seed must DETECT the corruption and fall back to a clean full clone,
        // never silently promote a corrupt mirror.
        let second = do_sync_for_test(&state, &repo_id, "main", &provider).await;
        assert_eq!(second.info.commit, c2);
        assert_eq!(
            seed_attempts.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "cold sync should attempt and reject the corrupt exact clonepack seed once"
        );

        let recovered_mirror = state.repo_root.join(repo_id.mirror_dir_name());
        let full_mirror = tmp.path().join("full-clone-corruptseed.git");
        git::sync_bare_mirror(&full_mirror, &provider, &repo_id, "main", None, None).unwrap();
        assert_eq!(
            git_stdout(
                &recovered_mirror,
                &["rev-parse", &format!("{}^{{tree}}", second.info.commit)],
            ),
            git_stdout(&full_mirror, &["rev-parse", "main^{tree}"]),
            "corrupt-seed fallback tree must match full-clone mirror tree"
        );
        // The recovered mirror must be connectivity-clean (no corrupt objects).
        git::fsck_connectivity(&recovered_mirror).unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn cold_sync_seed_miss_falls_back_to_full_clone() {
        let _env = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base.path().join("acme").join("seedmiss.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        crate::test_fixture::commit(&origin, &[("a.txt", b"1\n")]);

        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let repo_id = RepoId::github("acme/seedmiss");
        let provider = state.provider_registry.get("github").unwrap().clone();
        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path()) };

        let first = do_sync_for_test(&state, &repo_id, "main", &provider).await;
        let seed_manifest = first.info.full_clonepack.manifest.clone();
        state.storage.delete(&seed_manifest).unwrap();
        std::fs::remove_dir_all(state.repo_root.join(repo_id.mirror_dir_name())).unwrap();
        let c2 = crate::test_fixture::commit(
            &origin,
            &[("a.txt", b"2\n"), ("fallback.txt", b"full clone\n")],
        );

        let seed_attempts = Arc::new(AtomicUsize::new(0));
        state.storage = Arc::new(CountingGetStorage {
            inner: state.storage.clone(),
            target_hash: seed_manifest,
            hits: seed_attempts.clone(),
        });

        let second = do_sync_for_test(&state, &repo_id, "main", &provider).await;
        assert_eq!(second.info.commit, c2);
        assert_eq!(
            seed_attempts.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "cold sync should attempt the missing exact clonepack seed once"
        );

        let fallback_mirror = state.repo_root.join(repo_id.mirror_dir_name());
        let full_mirror = tmp.path().join("full-clone-seedmiss.git");
        git::sync_bare_mirror(&full_mirror, &provider, &repo_id, "main", None, None).unwrap();
        assert_eq!(
            git_stdout(
                &fallback_mirror,
                &["rev-parse", &format!("{}^{{tree}}", second.info.commit)],
            ),
            git_stdout(&full_mirror, &["rev-parse", "main^{tree}"]),
            "seed-miss fallback tree must match full-clone mirror tree"
        );
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };
    }

    fn auth_header() -> String {
        format!("Ripclone {}", hex::encode(Sha256::digest("secret")))
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

    fn complete_ref(commit: &str, manifest: &str) -> RefInfo {
        RefInfo {
            internal_exact_result: true,
            commit: commit.to_string(),
            default_branch: "main".to_string(),
            archive_chunks: vec!["archive".to_string()],
            full_clonepack: crate::ClonepackArtifacts {
                commit: commit.to_string(),
                manifest: manifest.to_string(),
                metadata_chunk: "metadata".to_string(),
                idx_bundle: "idx-bundle".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn rev_ref_lookup_reads_only_the_exact_result() {
        let tmp = tempfile::tempdir().unwrap();
        let store: Arc<dyn RefStore> = Arc::new(TestSqlRefStore::new(tmp.path().join("refs.db")));
        let repo = RepoId::github("o/r");
        let commit = "1111111111111111111111111111111111111111";
        let exact_key = exact_ref_store_key("main", commit);

        store
            .save_branch(&repo, &exact_key, &complete_ref(commit, "manifest-exact"))
            .await
            .unwrap();
        let mut moving = complete_ref(commit, "manifest-moving");
        moving.internal_exact_result = false;
        store.save_branch(&repo, "main", &moving).await.unwrap();

        let (key, info) = load_ref_info_for_resolved_commit(&store, &repo, "main", commit, "full")
            .await
            .expect("exact result");
        assert_eq!(key, exact_key);
        assert_eq!(info.full_clonepack.manifest, "manifest-exact");
        assert_eq!(info.full_clonepack.commit, commit);

        store.delete_branch(&repo, &exact_key).await.unwrap();
        let missing =
            load_ref_info_for_resolved_commit(&store, &repo, "main", commit, "full").await;
        assert!(
            missing.is_none(),
            "moving metadata is not an artifact source"
        );
    }

    #[tokio::test]
    async fn rev_ref_lookup_rejects_carried_full_clonepack_for_new_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let store: Arc<dyn RefStore> = Arc::new(TestSqlRefStore::new(tmp.path().join("refs.db")));
        let repo = RepoId::github("o/r");
        let new_commit = "4444444444444444444444444444444444444444";
        let old_commit = "5555555555555555555555555555555555555555";
        let info = RefInfo {
            internal_exact_result: true,
            commit: new_commit.to_string(),
            default_branch: "main".to_string(),
            archive_chunks: vec!["old-archive".to_string()],
            full_clonepack: crate::ClonepackArtifacts {
                commit: old_commit.to_string(),
                manifest: "old-full".to_string(),
                metadata_chunk: "old-metadata".to_string(),
                ..Default::default()
            },
            shallow_clonepack: crate::ClonepackArtifacts {
                commit: new_commit.to_string(),
                manifest: "new-shallow".to_string(),
                metadata_chunk: "new-metadata".to_string(),
                idx_bundle: "new-idx-bundle".to_string(),
                ..Default::default()
            },
            build_status: Some("full history building".to_string()),
            ..Default::default()
        };
        let exact_key = exact_ref_store_key("main", new_commit);
        store.save_branch(&repo, &exact_key, &info).await.unwrap();

        let full =
            load_ref_info_for_resolved_commit(&store, &repo, "main", new_commit, "full").await;
        assert!(
            full.is_none(),
            "rev full lookup must not serve the carried previous full clonepack"
        );

        let (_, shallow) =
            load_ref_info_for_resolved_commit(&store, &repo, "main", new_commit, "shallow")
                .await
                .expect("shallow variant is exact for the requested commit");
        assert_eq!(shallow.shallow_clonepack.manifest, "new-shallow");
    }

    #[test]
    fn branch_tip_full_clonepack_is_pending_when_carried_from_previous_commit() {
        let new_commit = "6666666666666666666666666666666666666666";
        let old_commit = "7777777777777777777777777777777777777777";
        let info = RefInfo {
            internal_exact_result: true,
            commit: new_commit.to_string(),
            default_branch: "main".to_string(),
            full_clonepack: crate::ClonepackArtifacts {
                commit: old_commit.to_string(),
                manifest: "old-full".to_string(),
                metadata_chunk: "old-metadata".to_string(),
                ..Default::default()
            },
            shallow_clonepack: crate::ClonepackArtifacts {
                commit: new_commit.to_string(),
                manifest: "new-shallow".to_string(),
                metadata_chunk: "new-metadata".to_string(),
                idx_bundle: "new-idx-bundle".to_string(),
                ..Default::default()
            },
            build_status: Some("full history building".to_string()),
            ..Default::default()
        };
        assert!(
            full_clonepack_pending_for_tip(&info, "full", new_commit),
            "normal branch-tip full lookup should poll while the selected full clonepack serves the previous commit"
        );
        assert!(
            !full_clonepack_pending_for_tip(&info, "shallow", new_commit),
            "shallow is already exact for the new commit"
        );
        assert!(
            exact_ref_info_serves_commit(&info, "shallow", new_commit),
            "the exact phase-one row serves its own shallow artifact"
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
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: Vec::new(),
            packs: Vec::new(),
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            archive_chunks: vec!["chunk1".to_string(), "chunk2".to_string()],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: "exact-manifest".to_string(),
                metadata_chunk: "exact-metadata".to_string(),
                commit: "exact-commit".to_string(),
                ..Default::default()
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            build_ms: None,
            synced_at: None,
            generation: None,
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
            "full",
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
                "/v1/repos/github/o/r/refs/main",
                Some(&auth_header()),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn ref_response_cache_hits_and_invalidates_by_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let resp = RefResponse {
            owner: "acme".to_string(),
            repo: "secret".to_string(),
            provider: "github".to_string(),
            host: "github.com".to_string(),
            origin_url: "https://github.com/acme/secret.git".to_string(),
            branch: "main".to_string(),
            default_branch: "main".to_string(),
            commit: "commit1".to_string(),
            parent_commit: None,
            clonepack_manifest: "manifest".to_string(),
            clonepack_manifest_url: Some("https://example.invalid/manifest".to_string()),
            metadata_chunk: "metadata".to_string(),
            metadata_chunk_url: Some("https://example.invalid/metadata".to_string()),
            archive_chunk_urls: Some(vec![Some("https://example.invalid/archive".to_string())]),
            head_blobs_chunk_urls: None,
            head_blobs_idx_url: None,
            pack_chunk_urls: None,
            midx_url: None,
            idx_bundle_url: None,
            shallow: true,
            archive_ready: true,
        };

        let cache_repo_id = RepoId::github("acme/secret");
        cache_ref_response(&state, &cache_repo_id, "main", "shallow", &resp);
        let cached = cached_ref_response(&state, &cache_repo_id, "main", "shallow")
            .expect("cached ref response");
        assert_eq!(cached.commit, "commit1");
        assert_eq!(
            cached.clonepack_manifest_url.as_deref(),
            Some("https://example.invalid/manifest")
        );
        assert!(cached_ref_response(&state, &cache_repo_id, "main", "full").is_none());

        invalidate_ref_response_cache(&state, &cache_repo_id, "main");
        assert!(cached_ref_response(&state, &cache_repo_id, "main", "shallow").is_none());

        let mut no_cache_state = state;
        no_cache_state.mirror_fresh_ttl = Duration::ZERO;
        cache_ref_response(&no_cache_state, &cache_repo_id, "main", "shallow", &resp);
        assert!(cached_ref_response(&no_cache_state, &cache_repo_id, "main", "shallow").is_none());
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

    /// Compatibility helper for request tests that do not inspect the durable
    /// job after admission.
    fn test_state_draining(tmp: &tempfile::TempDir) -> ServerState {
        test_state(tmp)
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
    async fn dead_lettered_b_cannot_mark_newer_c_failed() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let repo_id = RepoId::github("acme/fenced-failure");
        let b = "b".repeat(40);
        let c = "c".repeat(40);
        state
            .ref_store
            .save_branch(
                &repo_id,
                "main",
                &RefInfo {
                    commit: c.clone(),
                    build_status: Some("done".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        mark_admitted_build_failed(&state, &repo_id, "main", &b, "attempts exhausted")
            .await
            .unwrap();

        let current = state
            .ref_store
            .load_branch(&repo_id, "main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.commit, c);
        assert_eq!(current.build_status.as_deref(), Some("done"));
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
        assert_eq!(job.branch, "main");
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
            "main",
            &missing,
            Some("main"),
            &state.ref_store,
            &state.storage,
            &state.retention,
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
                .load_branch(&repo_id, "main")
                .await
                .unwrap()
                .is_none(),
            "an unavailable exact target must not publish branch metadata"
        );
        assert!(
            state.storage.list_hashes().unwrap().is_empty(),
            "an unavailable exact target must not publish artifacts"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn historical_source_failure_is_terminal_on_exact_row_without_reenqueue() {
        let _env = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base
            .path()
            .join("acme")
            .join("historical-missing.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let current = crate::test_fixture::commit(&origin, &[("README.md", b"current")]);
        let missing = "f".repeat(40);

        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = test_state_with_queue(&tmp);
        let repo_id = RepoId::github("acme/historical-missing");
        mark_added(&state, repo_id.clone()).await;
        state
            .ref_store
            .save_branch(
                &repo_id,
                "main",
                &RefInfo {
                    commit: current.clone(),
                    build_status: Some("done".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let job = BuildJob {
            repo_id: repo_id.clone(),
            branch: "main".to_string(),
            admitted_commit: missing.clone(),
            admitted_default_branch: Some("main".to_string()),
            credential: None,
            size_bytes: None,
        };
        prepare_exact_admission(&state, &job, false).await.unwrap();
        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path()) };
        process_build_job(&state, &job)
            .await
            .expect_err("missing historical object must fail");

        let exact_key = exact_ref_store_key("main", &missing);
        let failed = state
            .ref_store
            .load_branch(&repo_id, &exact_key)
            .await
            .unwrap()
            .expect("terminal exact failure row");
        assert_eq!(failed.commit, missing);
        assert!(
            failed
                .build_status
                .as_deref()
                .is_some_and(|status| status.starts_with("failed: "))
        );
        let moving = state
            .ref_store
            .load_branch(&repo_id, "main")
            .await
            .unwrap()
            .expect("moving projection remains");
        assert_eq!(moving.commit, current);
        assert_eq!(moving.build_status.as_deref(), Some("done"));
        assert!(state.storage.list_hashes().unwrap().is_empty());

        let response = build_app(state)
            .oneshot(test_request(
                "POST",
                &format!("/v1/repos/github/acme/historical-missing/sync?branch=main&rev={missing}"),
            ))
            .await
            .unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains(&missing), "clear exact failure: {body}");
        assert!(
            rx.try_recv().is_err(),
            "terminal failure cannot enqueue again"
        );
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
        assert_eq!(job.branch, "main");
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
    async fn webhook_untracked_non_default_branch_is_ignored() {
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
        assert!(
            rx.try_recv().is_err(),
            "untracked non-default must not enqueue"
        );
    }

    #[tokio::test]
    async fn webhook_tracked_non_default_branch_enqueues() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let repo = RepoId::github("acme/widget");
        mark_added(&state, repo.clone()).await;
        let info = RefInfo {
            commit: "deadbeef".to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&repo, "feature", &info)
            .await
            .unwrap();
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
        let job = rx.try_recv().expect("tracked branch enqueues");
        assert_eq!(job.branch, "feature");
    }

    #[tokio::test]
    async fn webhook_warm_all_warms_every_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let mut rx = install_observed_queue(&mut state);
        state.webhook_config =
            Arc::new(WebhookConfig::with_secret("github", WEBHOOK_SECRET).with_warm_all(true));
        mark_added(&state, RepoId::github("acme/widget")).await;
        let app = build_app(state);
        // An untracked, non-default branch is warmed when warm-all is on.
        let body = gh_push_body(
            "acme",
            "widget",
            "refs/heads/random",
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
        let job = rx
            .try_recv()
            .expect("warm-all warms an untracked non-default branch");
        assert_eq!(job.branch, "random");
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
    async fn webhook_branch_delete_cleans_up_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        let repo = RepoId::github("acme/widget");
        let info = RefInfo {
            commit: "deadbeef".to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&repo, "feature", &info)
            .await
            .unwrap();
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
                .load_branch(&repo, "feature")
                .await
                .unwrap()
                .is_none(),
            "deleted branch ref is cleaned up"
        );
        assert!(rx.try_recv().is_err(), "a delete must not enqueue a build");
    }

    #[tokio::test]
    async fn webhook_delete_outside_allowlist_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        state.webhook_config = Arc::new(
            WebhookConfig::with_secret("github", WEBHOOK_SECRET)
                .with_allowlist(["acme/allowed".to_string()]),
        );
        let repo = RepoId::github("acme/widget"); // not allowlisted
        let info = RefInfo {
            commit: "deadbeef".to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&repo, "feature", &info)
            .await
            .unwrap();
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
                .load_branch(&repo, "feature")
                .await
                .unwrap()
                .is_some(),
            "out-of-scope delete must not mutate refs"
        );
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
        assert_eq!(job.branch, "main");
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
        assert_eq!(job.branch, "main");
    }

    #[tokio::test]
    async fn webhook_gitea_branch_delete_cleans_up_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = provider_webhook_state(&tmp, "gitea", "gitea", "gitea.example.com");
        let repo = RepoId {
            provider: crate::provider::ProviderInstanceId::new("gitea"),
            path: "acme/widget".to_string(),
        };
        let info = RefInfo {
            commit: "deadbeef".to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&repo, "feature", &info)
            .await
            .unwrap();
        let ref_store = state.ref_store.clone();
        let app = build_app(state);
        // Gitea's `delete` event uses a bare branch name + ref_type.
        let body = br#"{"ref":"feature","ref_type":"branch","repository":{"full_name":"acme/widget","default_branch":"main"}}"#.to_vec();
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
            .header("X-Gitea-Event", "delete")
            .header("X-Gitea-Signature", sig)
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            ref_store
                .load_branch(&repo, "feature")
                .await
                .unwrap()
                .is_none(),
            "gitea delete cleans up the stored ref"
        );
        assert!(rx.try_recv().is_err(), "a delete must not enqueue");
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
    async fn webhook_gitlab_branch_delete_cleans_up_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = provider_webhook_state(&tmp, "gitlab", "gitlab", "gitlab.com");
        let repo = RepoId {
            provider: crate::provider::ProviderInstanceId::new("gitlab"),
            path: "group/sub/proj".to_string(),
        };
        let info = RefInfo {
            commit: "deadbeef".to_string(),
            default_branch: "main".to_string(),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&repo, "feature", &info)
            .await
            .unwrap();
        let ref_store = state.ref_store.clone();
        let app = build_app(state);
        // GitLab signals a branch delete with an all-zeros `after` on a Push Hook.
        let body = br#"{"ref":"refs/heads/feature","after":"0000000000000000000000000000000000000000","project":{"path_with_namespace":"group/sub/proj","default_branch":"main"}}"#.to_vec();
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
        assert!(
            ref_store
                .load_branch(&repo, "feature")
                .await
                .unwrap()
                .is_none(),
            "gitlab delete cleans up the stored ref"
        );
        assert!(rx.try_recv().is_err(), "a delete must not enqueue");
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
    async fn webhook_default_branch_resolved_from_mirror_when_payload_omits_it() {
        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = webhook_state(&tmp);
        mark_added(&state, RepoId::github("acme/widget")).await;
        let mirror = state
            .repo_root
            .join(RepoId::github("acme/widget").mirror_dir_name());
        gix::init_bare(&mirror).unwrap();
        std::fs::write(mirror.join("HEAD"), b"ref: refs/heads/trunk\n").unwrap();
        let app = build_app(state);
        let body = gh_push_body_no_default("acme", "widget", "refs/heads/trunk", &"1".repeat(40));
        let sig = gh_sign(WEBHOOK_SECRET, &body);
        let resp = app
            .oneshot(webhook_request("github", "push", Some(&sig), body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            rx.try_recv()
                .expect("default branch from mirror is warmed")
                .branch,
            "trunk"
        );
    }

    #[tokio::test]
    async fn webhook_no_default_no_mirror_untracked_is_ignored() {
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
            "unknowable default + untracked → ignored"
        );
    }

    /// The polling fallback triggers a build when the upstream tip isn't built,
    /// and is a no-op once it is. This is the missed-webhook catch-up path.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn poll_triggers_on_unbuilt_tip_and_skips_when_built() {
        // Serialize with every other test that mutates the process-global
        // RIPCLONE_ORIGIN_BASE (git.rs env tests, ref_read_bumps_last_accessed_at),
        // so a sibling's set/remove can't race this test's origin-base window.
        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(origin.parent().unwrap()).unwrap();
        let repo = crate::test_fixture::init_bare(&origin);
        let tip = crate::test_fixture::commit(&repo, &[("f.txt", b"v1")]);

        let tmp = tempfile::tempdir().unwrap();
        let state = test_state_draining(&tmp);
        let rid = RepoId::github("acme/widget");
        mark_added(&state, rid.clone()).await;
        let probe = Arc::new(AdmissionTestProbe::default());
        let _probe_guard = install_admission_test_probe(Arc::clone(&probe));

        // Seed a stale ref so the repo is enumerable and its tip looks unbuilt.
        let stale = RefInfo {
            commit: "0".repeat(40),
            synced_at: Some(1),
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&rid, "HEAD", &stale)
            .await
            .unwrap();
        let exact_stale = RefInfo {
            internal_exact_result: true,
            ..stale.clone()
        };
        state
            .ref_store
            .save_branch(
                &rid,
                &crate::ref_store::exact_ref_key("main", &"a".repeat(40)),
                &exact_stale,
            )
            .await
            .unwrap();
        let listed = state.ref_store.list_branches(&rid).await.unwrap();
        assert_eq!(listed.len(), 2, "fixture has a source and an exact row");

        // A repository populated only by `sync --at` must be enumerable for
        // retention/accounting without turning its exact key into a source ref.
        let exact_only_rid = RepoId::github("acme/exact-only");
        mark_added(&state, exact_only_rid.clone()).await;
        state
            .ref_store
            .save_branch(
                &exact_only_rid,
                &crate::ref_store::exact_ref_key("main", &"b".repeat(40)),
                &RefInfo {
                    internal_exact_result: true,
                    default_branch: "main".to_string(),
                    ..exact_stale
                },
            )
            .await
            .unwrap();

        unsafe {
            std::env::set_var("RIPCLONE_ORIGIN_BASE", base.path());
            std::env::set_var("RIPCLONE_TESTING", "1");
        }
        let probes_before = probe.tip_probes.load(Ordering::SeqCst);
        let on_change = poll_once(&state).await;
        assert_eq!(
            probe.tip_probes.load(Ordering::SeqCst) - probes_before,
            1,
            "internal exact results, including exact-only repositories, are not provider source refs"
        );

        // Mark it built at the real tip → the next poll is a no-op.
        let built = RefInfo {
            commit: tip.clone(),
            synced_at: Some(2),
            full_clonepack: crate::ClonepackArtifacts {
                commit: tip.clone(),
                manifest: "m".to_string(),
                ..Default::default()
            },
            archive_chunks: vec!["a".to_string()],
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&rid, "HEAD", &built)
            .await
            .unwrap();
        let probes_before_built = probe.tip_probes.load(Ordering::SeqCst);
        let when_built = poll_once(&state).await;
        assert_eq!(
            probe.tip_probes.load(Ordering::SeqCst) - probes_before_built,
            1,
            "only the source ref remains a polling candidate"
        );
        unsafe {
            std::env::remove_var("RIPCLONE_ORIGIN_BASE");
            std::env::remove_var("RIPCLONE_TESTING");
        }

        assert_eq!(on_change, 1, "poll triggers a build for an unbuilt tip");
        assert_eq!(when_built, 0, "poll is a no-op once the tip is built");
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
    async fn repo_status_reports_warmed_branch_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let metadata = ChunkRef {
            hash: hash_from_hex(&"a".repeat(64)).unwrap(),
            len: 100,
        };
        let archive = ChunkRef {
            hash: hash_from_hex(&"b".repeat(64)).unwrap(),
            len: 200,
        };
        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            metadata_chunk: Some(metadata),
            archive_chunks: vec![archive],
            head_blobs_idx: None,
            head_blobs_chunks: vec![],
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            archive_chunks: vec!["b".repeat(64)],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: "a".repeat(64),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            build_ms: None,
            synced_at: Some(1_718_812_800),
            generation: None,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();

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
        assert_eq!(status.refs.len(), 1);
        let branch = &status.refs[0];
        assert_eq!(branch.branch, "main");
        assert_eq!(branch.commit, "commit1");
        assert_eq!(branch.manifest, manifest_hash);
        let expected_bytes = 300 + manifest_data.len() as u64;
        assert_eq!(branch.bytes, expected_bytes);
        assert_eq!(branch.unique_bytes, expected_bytes); // fallback until cross-repo dedup
        assert!(branch.built_at.is_some());
        assert_eq!(status.total_bytes, expected_bytes);
        assert_eq!(status.total_unique_bytes, expected_bytes);
        assert_eq!(status.regions.len(), 1);
        assert_eq!(status.regions[0].region, "local");
        assert_eq!(status.regions[0].unique_bytes, expected_bytes);
    }

    #[tokio::test]
    async fn repo_status_hides_exact_rows_and_dedups_shared_chunks() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let metadata_hash = "a".repeat(64);
        let archive_hash = "b".repeat(64);
        let metadata = ChunkRef {
            hash: hash_from_hex(&metadata_hash).unwrap(),
            len: 100,
        };
        let archive = ChunkRef {
            hash: hash_from_hex(&archive_hash).unwrap(),
            len: 200,
        };
        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            metadata_chunk: Some(metadata),
            archive_chunks: vec![archive],
            head_blobs_idx: None,
            head_blobs_chunks: vec![],
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            archive_chunks: vec![archive_hash.clone()],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: metadata_hash.clone(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            head_base_commit: String::new(),
            head_base_packs: Vec::new(),
            archive_frames: Vec::new(),
            build_status: None,
            build_ms: None,
            synced_at: None,
            generation: None,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();
        let exact_info = RefInfo {
            internal_exact_result: true,
            ..info.clone()
        };
        state
            .ref_store
            .save_branch(
                &RepoId::github("acme/secret"),
                &crate::ref_store::exact_ref_key(
                    "main",
                    "1111111111111111111111111111111111111111",
                ),
                &exact_info,
            )
            .await
            .unwrap();

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
        assert_eq!(status.refs.len(), 1);
        assert_eq!(status.refs[0].branch, "main");
        let expected_total = 300 + manifest_data.len() as u64;
        assert_eq!(status.total_bytes, expected_total);
        assert_eq!(status.total_unique_bytes, expected_total); // fallback: no dedup
        for branch in &status.refs {
            assert_eq!(branch.bytes, expected_total);
            assert_eq!(branch.unique_bytes, expected_total); // per-branch fallback
        }
        assert_eq!(status.regions.len(), 1);
        assert_eq!(status.regions[0].unique_bytes, expected_total);
    }

    #[tokio::test]
    async fn repo_status_public_fork_has_zero_unique_byte_allocation() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let metadata = ChunkRef {
            hash: hash_from_hex(&"a".repeat(64)).unwrap(),
            len: 100,
        };
        let archive = ChunkRef {
            hash: hash_from_hex(&"b".repeat(64)).unwrap(),
            len: 200,
        };
        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            metadata_chunk: Some(metadata),
            archive_chunks: vec![archive],
            head_blobs_idx: None,
            head_blobs_chunks: vec![],
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            archive_chunks: vec!["b".repeat(64)],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: String::new(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            build_status: None,
            synced_at: None,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/fork"), "main", &info)
            .await
            .unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(test_request(
                "GET",
                "/v1/repos/github/acme/fork/status?public=true&fork_of=oven-sh/bun",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: RepoStatusResponse = serde_json::from_slice(&body).unwrap();
        let expected_total = 300 + manifest_data.len() as u64;
        assert_eq!(status.total_bytes, expected_total);
        assert_eq!(status.total_unique_bytes, 0);
        assert_eq!(status.refs[0].bytes, expected_total);
        assert_eq!(status.refs[0].unique_bytes, 0);
        assert_eq!(status.regions[0].unique_bytes, 0);
    }

    #[tokio::test]
    async fn repo_status_counts_history_levels() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            archive_chunks: vec![],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: String::new(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: vec![crate::HistoryLevel {
                tip_commit: "older".to_string(),
                packs: vec![
                    crate::SizedPack {
                        pack: "p1".to_string(),
                        pack_len: 500,
                        idx: "i1".to_string(),
                        idx_len: 50,
                    },
                    crate::SizedPack {
                        pack: "p2".to_string(),
                        pack_len: 700,
                        idx: "i2".to_string(),
                        idx_len: 70,
                    },
                ],
            }],
            build_status: None,
            synced_at: None,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();

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
        let expected = manifest_data.len() as u64 + 500 + 50 + 700 + 70;
        assert_eq!(status.refs.len(), 1);
        assert_eq!(status.refs[0].bytes, expected);
        assert_eq!(status.total_bytes, expected);
        assert_eq!(status.total_unique_bytes, expected);
    }

    #[tokio::test]
    async fn repo_status_counts_pack_entries_in_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        let pack = crate::clonepack::PackEntry {
            pack: Some(ChunkRef {
                hash: hash_from_hex(&"c".repeat(64)).unwrap(),
                len: 400,
            }),
            idx: Some(ChunkRef {
                hash: hash_from_hex(&"d".repeat(64)).unwrap(),
                len: 40,
            }),
            ..Default::default()
        };
        let manifest = ClonepackManifest {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            packs: vec![pack],
            ..Default::default()
        };
        let manifest_data = manifest.encode_to_vec();
        let manifest_hash = state.cas.put(&manifest_data).unwrap();

        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: manifest_hash.clone(),
            archive_chunks: vec![],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: manifest_hash.clone(),
                metadata_chunk: String::new(),
                skeleton_pack: String::new(),
                skeleton_idx: String::new(),
                prebuilt_index: String::new(),
                midx: String::new(),
                idx_bundle: String::new(),
                commit: String::new(),
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            build_status: None,
            synced_at: None,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();

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
        let expected = manifest_data.len() as u64 + 400 + 40;
        assert_eq!(status.refs[0].bytes, expected);
        assert_eq!(status.total_bytes, expected);
    }

    #[tokio::test]
    async fn repo_status_reports_evicted_ref_as_not_warm() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);

        // Leave a non-empty manifest hash in the evicted ref. The status path
        // must not try to read the deleted artifact; doing so would fail the
        // whole repo status request.
        let info = RefInfo {
            commit: "commit1".to_string(),
            parent_commit: None,
            default_branch: "main".to_string(),
            skeleton_pack: String::new(),
            skeleton_idx: String::new(),
            head_blobs_idx: String::new(),
            head_blobs_chunks: vec![],
            packs: vec![],
            prebuilt_index: String::new(),
            archive: String::new(),
            manifest: String::new(),
            archive_chunks: vec![],
            full_clonepack: crate::ClonepackArtifacts {
                manifest: "0000000000000000000000000000000000000000".to_string(),
                commit: "commit1".to_string(),
                ..Default::default()
            },
            shallow_clonepack: crate::ClonepackArtifacts::default(),
            history_levels: Vec::new(),
            build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
            build_ms: None,
            synced_at: Some(1_718_812_800),
            last_accessed_at: Some(1_718_812_700),
            generation: None,
            warm_pinned: false,
            ..Default::default()
        };
        state
            .ref_store
            .save_branch(&RepoId::github("acme/secret"), "main", &info)
            .await
            .unwrap();

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
        assert_eq!(status.refs.len(), 1);
        let branch = &status.refs[0];
        assert_eq!(branch.branch, "main");
        assert_eq!(branch.commit, "commit1");
        assert!(!branch.warm);
        assert!(!branch.pinned);
        assert_eq!(branch.bytes, 0);
        assert!(branch.manifest.is_empty());
        assert!(branch.last_accessed_at.is_some());
    }

    #[test]
    fn exact_ref_info_rejects_evicted_ref() {
        let info = RefInfo {
            commit: "commit1".to_string(),
            build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
            full_clonepack: crate::ClonepackArtifacts {
                manifest: "manifest-hash".to_string(),
                commit: "commit1".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(!exact_ref_info_serves_commit(&info, "full", "commit1"));
    }

    #[tokio::test]
    async fn carried_top_up_rejects_evicted_row_before_signing() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = crate::storage::local(tmp.path()).unwrap();
        let base = "a".repeat(40);
        let target = "b".repeat(40);
        let info = RefInfo {
            commit: target.clone(),
            parent_commit: Some(base.clone()),
            full_clonepack: crate::ClonepackArtifacts {
                commit: base,
                manifest: "0".repeat(64),
                ..Default::default()
            },
            build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
            ..Default::default()
        };
        let provider = ProviderInstance {
            id: crate::provider::ProviderInstanceId::new("github"),
            kind: crate::provider::ProviderKind::GitHub,
            host: "github.com".to_string(),
            auth_template: None,
            auth_header_name: None,
        };
        assert!(
            carried_full_top_up_response(
                &info,
                &RepoId::github("acme/widget"),
                &provider,
                "main",
                &target,
                &storage,
                false,
            )
            .await
            .is_none(),
            "an evicted carried row must not issue signed base URLs"
        );
    }

    #[test]
    fn phase_two_barrier_requires_explicit_test_mode() {
        assert!(!explicit_test_mode(None));
        assert!(!explicit_test_mode(Some(std::ffi::OsStr::new("0"))));
        assert!(!explicit_test_mode(Some(std::ffi::OsStr::new("true"))));
        assert!(explicit_test_mode(Some(std::ffi::OsStr::new("1"))));
    }

    #[test]
    fn metadata_only_full_pending_excludes_evicted_rows() {
        let info = RefInfo {
            commit: "commit1".to_string(),
            build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
            full_clonepack: crate::ClonepackArtifacts {
                manifest: "manifest-hash".to_string(),
                commit: "commit1".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            !full_clonepack_pending_for_tip(&info, "full", "commit1"),
            "evicted tips must reach rebuild admission instead of the metadata-only shortcut"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn ordinary_evicted_get_enqueues_once_and_becomes_ready() {
        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base.path().join("acme").join("evicted.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let commit = crate::test_fixture::commit(&origin, &[("f.txt", b"ready")]);

        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = test_state_with_queue(&tmp);
        let repo_id = RepoId::github("acme/evicted");
        mark_added(&state, repo_id.clone()).await;
        let exact_key = exact_ref_store_key("main", &commit);
        state
            .ref_store
            .save_branch(
                &repo_id,
                &exact_key,
                &RefInfo {
                    internal_exact_result: true,
                    commit: commit.clone(),
                    build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path()) };
        let app = build_app(state.clone());
        let first = app
            .clone()
            .oneshot(protocol_request(
                "/v1/repos/github/acme/evicted/refs/main?clonepack=shallow",
                Some("2"),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::ACCEPTED);
        let first_body = axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap();
        let first_body: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_body["commit"], commit);

        let job = rx
            .try_recv()
            .expect("initial GET must admit the evicted ordinary ref");
        assert_eq!(job.branch, "main");
        assert_eq!(job.admitted_commit, commit);
        assert!(rx.try_recv().is_err(), "exactly one rebuild is admitted");

        let built = process_build_job(&state, &job)
            .await
            .expect("admitted evicted ref rebuild");
        assert_eq!(built.info.commit, commit);
        let ready = app
            .oneshot(protocol_request(
                &format!(
                    "/v1/repos/github/acme/evicted/refs/main?clonepack=shallow&pinned={commit}"
                ),
                Some("2"),
            ))
            .await
            .unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };
        assert_eq!(ready.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn evicted_get_returns_503_when_queue_errors() {
        struct ErrorQueue {
            attempts: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl crate::queue::JobQueue for ErrorQueue {
            async fn enqueue(&self, _job: BuildJob) -> anyhow::Result<crate::queue::Enqueued> {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("forced queue outage")
            }

            async fn depth(&self) -> usize {
                0
            }
        }

        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let origin_base = tempfile::tempdir().unwrap();
        let origin_path = origin_base.path().join("acme").join("evicted-error.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let commit = crate::test_fixture::commit(&origin, &[("f.txt", b"ready")]);

        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let attempts = Arc::new(AtomicUsize::new(0));
        state.build_queue = Arc::new(ErrorQueue {
            attempts: Arc::clone(&attempts),
        });
        let repo_id = RepoId::github("acme/evicted-error");
        mark_added(&state, repo_id.clone()).await;
        let exact_key = exact_ref_store_key("main", &commit);
        state
            .ref_store
            .save_branch(
                &repo_id,
                &exact_key,
                &RefInfo {
                    internal_exact_result: true,
                    commit: commit.clone(),
                    build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", origin_base.path()) };

        let response = build_app(state.clone())
            .oneshot(protocol_request(
                "/v1/repos/github/acme/evicted-error/refs/main?clonepack=shallow",
                Some("2"),
            ))
            .await
            .unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("forced queue outage"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(state.storage.list_hashes().unwrap().is_empty());
        assert_eq!(
            state
                .ref_store
                .load_branch(&repo_id, &exact_key)
                .await
                .unwrap()
                .unwrap()
                .build_status
                .as_deref(),
            Some(crate::remote_gc::EVICTED_BUILD_STATUS),
            "queue outage cannot claim that a rebuild started"
        );
    }

    #[tokio::test]
    async fn branch_ref_is_evicted_for_commit_reads_only_exact_row() {
        let tmp = tempfile::tempdir().unwrap();
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&repo_root).unwrap();
        let ref_store: Arc<dyn RefStore> =
            Arc::new(TestSqlRefStore::new(repo_root.join("refs.db")));
        let rid = RepoId::github("o/r");

        let evicted = RefInfo {
            commit: "abc123".to_string(),
            build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
            ..Default::default()
        };
        ref_store
            .save_branch(&rid, &exact_ref_store_key("main", "abc123"), &evicted)
            .await
            .unwrap();

        assert!(
            branch_ref_is_evicted_for_commit(&ref_store, &rid, "main", "abc123").await,
            "must detect evicted ref for matching commit"
        );
        assert!(
            !branch_ref_is_evicted_for_commit(&ref_store, &rid, "main", "other").await,
            "must not flag a different commit"
        );
        assert!(
            !branch_ref_is_evicted_for_commit(&ref_store, &rid, "feature", "abc123").await,
            "must not flag a missing branch"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn ref_read_bumps_last_accessed_at() {
        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(origin.parent().unwrap()).unwrap();
        let repo = crate::test_fixture::init_bare(&origin);
        let tip = crate::test_fixture::commit(&repo, &[("f.txt", b"v1")]);

        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let ref_store = state.ref_store.clone();
        let rid = RepoId::github("acme/widget");
        mark_added(&state, rid.clone()).await;

        let old_ts = 1_000_000u64;
        let exact_key = exact_ref_store_key("main", &tip);
        let info = RefInfo {
            internal_exact_result: true,
            commit: tip.clone(),
            synced_at: Some(old_ts),
            last_accessed_at: Some(old_ts),
            full_clonepack: crate::ClonepackArtifacts {
                commit: tip.clone(),
                manifest: "0000000000000000000000000000000000000000".to_string(),
                metadata_chunk: "1111111111111111111111111111111111111111".to_string(),
                idx_bundle: "2222222222222222222222222222222222222222".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        ref_store
            .save_branch(&rid, &exact_key, &info)
            .await
            .unwrap();

        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", base.path()) };
        let app = build_app(state);
        let response = app
            .oneshot(protocol_request(
                "/v1/repos/github/acme/widget/refs/main?clonepack=full",
                Some("2"),
            ))
            .await
            .unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };

        assert_eq!(response.status(), StatusCode::OK);
        let updated = ref_store
            .load_branch(&rid, &exact_key)
            .await
            .unwrap()
            .unwrap();
        assert!(
            updated.last_accessed_at.unwrap() > old_ts,
            "last_accessed_at must advance on a successful ref read"
        );
    }

    #[tokio::test]
    async fn pinned_read_uses_existing_exact_artifact_without_mutating_moving_row() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let ref_store = state.ref_store.clone();
        let rid = RepoId::github("acme/widget");
        mark_added(&state, rid.clone()).await;

        let pinned = "a".repeat(40);
        let newer = "b".repeat(40);
        let old_ts = 1;
        let exact_key = exact_ref_store_key("main", &pinned);
        let mut exact = complete_ref(&pinned, "manifest-a");
        exact.last_accessed_at = Some(old_ts);
        let mut moving = complete_ref(&newer, "manifest-b");
        moving.internal_exact_result = false;
        moving.last_accessed_at = Some(old_ts);
        ref_store
            .save_branch(&rid, &exact_key, &exact)
            .await
            .unwrap();
        ref_store.save_branch(&rid, "main", &moving).await.unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(protocol_request(
                &format!("/v1/repos/github/acme/widget/refs/main?clonepack=full&pinned={pinned}"),
                Some("2"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let exact_after = ref_store
            .load_branch(&rid, &exact_key)
            .await
            .unwrap()
            .unwrap();
        let moving_after = ref_store.load_branch(&rid, "main").await.unwrap().unwrap();
        assert!(
            exact_after.last_accessed_at.unwrap() > old_ts,
            "a successful exact-artifact read performs the normal access-time touch"
        );
        assert!(
            !exact_after.warm_pinned,
            "an exact read must not create a pin"
        );
        assert_eq!(
            moving_after.last_accessed_at,
            Some(old_ts),
            "a pending pinned lookup must not touch the unrelated moving row"
        );
    }

    #[tokio::test]
    async fn pinned_pending_read_touches_no_candidate_row() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state_draining(&tmp);
        let ref_store = state.ref_store.clone();
        let rid = RepoId::github("acme/widget");
        mark_added(&state, rid.clone()).await;

        let pinned = "a".repeat(40);
        let other = "b".repeat(40);
        let old_ts = 1;
        let exact_key = exact_ref_store_key("main", &pinned);
        let mut mismatched_exact = complete_ref(&other, "manifest-b-exact");
        mismatched_exact.last_accessed_at = Some(old_ts);
        let mut moving = complete_ref(&other, "manifest-b-moving");
        moving.internal_exact_result = false;
        moving.last_accessed_at = Some(old_ts);
        ref_store
            .save_branch(&rid, &exact_key, &mismatched_exact)
            .await
            .unwrap();
        ref_store.save_branch(&rid, "main", &moving).await.unwrap();

        let app = build_app(state);
        let response = app
            .oneshot(protocol_request(
                &format!("/v1/repos/github/acme/widget/refs/main?clonepack=full&pinned={pinned}"),
                Some("2"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let exact_after = ref_store
            .load_branch(&rid, &exact_key)
            .await
            .unwrap()
            .unwrap();
        let moving_after = ref_store.load_branch(&rid, "main").await.unwrap().unwrap();
        assert_eq!(
            exact_after.last_accessed_at,
            Some(old_ts),
            "a mismatched exact candidate must not be kept warm"
        );
        assert_eq!(
            moving_after.last_accessed_at,
            Some(old_ts),
            "a pending pinned lookup must not touch an unrelated moving row"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn ref_read_cache_hit_bumps_last_accessed_at() {
        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(origin.parent().unwrap()).unwrap();
        let repo = crate::test_fixture::init_bare(&origin);
        let tip = crate::test_fixture::commit(&repo, &[("f.txt", b"v1")]);

        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp);
        let ref_store = state.ref_store.clone();
        let rid = RepoId::github("acme/widget");
        mark_added(&state, rid.clone()).await;

        let old_ts = 1_000_000u64;
        let exact_key = exact_ref_store_key("main", &tip);
        let info = RefInfo {
            internal_exact_result: true,
            commit: tip.clone(),
            synced_at: Some(old_ts),
            last_accessed_at: Some(old_ts),
            full_clonepack: crate::ClonepackArtifacts {
                commit: tip.clone(),
                manifest: "0000000000000000000000000000000000000000".to_string(),
                metadata_chunk: "1111111111111111111111111111111111111111".to_string(),
                idx_bundle: "2222222222222222222222222222222222222222".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        ref_store
            .save_branch(&rid, &exact_key, &info)
            .await
            .unwrap();

        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", base.path()) };
        let app = build_app(state);
        let first = app
            .clone()
            .oneshot(protocol_request(
                "/v1/repos/github/acme/widget/refs/main?clonepack=full",
                Some("2"),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let after_first = ref_store
            .load_branch(&rid, &exact_key)
            .await
            .unwrap()
            .unwrap();
        let first_ts = after_first.last_accessed_at.unwrap();
        assert!(first_ts > old_ts);

        let mut aged = after_first;
        aged.last_accessed_at = Some(old_ts);
        ref_store
            .save_branch(&rid, &exact_key, &aged)
            .await
            .unwrap();
        // The second response is cached, but its access refresh still targets
        // the exact row that supplied the cached artifact.
        let second = app
            .oneshot(protocol_request(
                "/v1/repos/github/acme/widget/refs/main?clonepack=full",
                Some("2"),
            ))
            .await
            .unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };

        assert_eq!(second.status(), StatusCode::OK);
        let after_second = ref_store
            .load_branch(&rid, &exact_key)
            .await
            .unwrap()
            .unwrap();
        assert!(
            after_second.last_accessed_at.unwrap() > old_ts,
            "last_accessed_at must advance on a cached ref read"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn ref_read_for_evicted_exact_revision_enqueues_rebuild() {
        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let base = tempfile::tempdir().unwrap();
        let origin = base.path().join("acme").join("widget.git");
        std::fs::create_dir_all(origin.parent().unwrap()).unwrap();
        let repo = crate::test_fixture::init_bare(&origin);
        let tip = crate::test_fixture::commit(&repo, &[("f.txt", b"v1")]);

        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = test_state_with_queue(&tmp);
        let rid = RepoId::github("acme/widget");
        mark_added(&state, rid.clone()).await;

        let evicted = RefInfo {
            commit: tip.clone(),
            build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
            synced_at: Some(1),
            last_accessed_at: Some(1),
            ..Default::default()
        };
        let exact_key = exact_ref_store_key("main", &tip);
        state
            .ref_store
            .save_branch(&rid, &exact_key, &evicted)
            .await
            .unwrap();

        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", base.path()) };
        let app = build_app(state);
        let response = app
            .oneshot(request_with_auth(
                "GET",
                &format!("/v1/repos/github/acme/widget/refs/main?rev={}", tip),
                Some(&auth_header()),
            ))
            .await
            .unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let job = rx
            .try_recv()
            .expect("evicted ref read must enqueue a rebuild");
        assert_eq!(job.repo_id, rid);
        assert_eq!(job.branch, "main");
        assert_eq!(job.admitted_commit, tip);
        assert!(
            rx.try_recv().is_err(),
            "exactly one rebuild must be enqueued"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn evicted_symbolic_history_is_pinned_and_rebuilds_original_commit() {
        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let base = tempfile::tempdir().unwrap();
        let origin_path = base.path().join("acme").join("history.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let b = crate::test_fixture::commit(&origin, &[("f.txt", b"B")]);
        let _c = crate::test_fixture::commit(&origin, &[("f.txt", b"C")]);
        let d = crate::test_fixture::commit(&origin, &[("f.txt", b"D")]);

        let tmp = tempfile::tempdir().unwrap();
        let (state, mut rx) = test_state_with_queue(&tmp);
        let rid = RepoId::github("acme/history");
        mark_added(&state, rid.clone()).await;
        state
            .ref_store
            .save_branch(
                &rid,
                "main",
                &RefInfo {
                    commit: d.clone(),
                    build_status: Some("done".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let historical_key = exact_ref_store_key("main", &b);
        state
            .ref_store
            .save_branch(
                &rid,
                &historical_key,
                &RefInfo {
                    commit: b.clone(),
                    build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", base.path()) };
        let response = build_app(state.clone())
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/acme/history/refs/main?rev=HEAD~2&clonepack=shallow",
                Some(&auth_header()),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        // The job remains queued (strictly before claim) while upstream moves.
        // Its historical target must already be the resolved B, not HEAD~2.
        let e = crate::test_fixture::commit(&origin, &[("f.txt", b"E")]);
        let job = rx.try_recv().expect("historical B rebuild admitted");
        assert_eq!(job.branch, "main");
        assert_eq!(job.admitted_commit, b);
        let built = process_build_job(&state, &job)
            .await
            .expect("historical B rebuild after branch advancement");
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };
        assert_eq!(
            built.info.commit, b,
            "queued HEAD~2 must remain pinned to B"
        );
        assert_eq!(
            state
                .ref_store
                .load_branch(&rid, "main")
                .await
                .unwrap()
                .unwrap()
                .commit,
            d,
            "historical admission must not move the current branch"
        );
        assert_ne!(
            built.info.commit, e,
            "historical B must not fall forward to E"
        );
        assert!(
            rx.try_recv().is_err(),
            "exactly one historical job is admitted"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn evicted_exact_revision_uses_the_shared_sql_queue() {
        let _lock = crate::git::ORIGIN_BASE_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let base = tempfile::tempdir().unwrap();
        let origin_path = base.path().join("acme").join("history-sql.git");
        std::fs::create_dir_all(origin_path.parent().unwrap()).unwrap();
        let origin = crate::test_fixture::init_bare(&origin_path);
        let b = crate::test_fixture::commit(&origin, &[("f.txt", b"B")]);
        let _c = crate::test_fixture::commit(&origin, &[("f.txt", b"C")]);
        let d = crate::test_fixture::commit(&origin, &[("f.txt", b"D")]);

        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(&tmp);
        let queue_path = tmp.path().join("historical-queue.db");
        let queue = Arc::new(
            crate::queue::SqlJobQueue::new(Box::new(
                crate::queue::LibsqlDb::connect(&queue_path.to_string_lossy())
                    .await
                    .unwrap(),
            ))
            .await
            .unwrap(),
        );
        state.build_queue = queue.clone();
        state.worker_queue = Some(Arc::clone(&queue));
        let rid = RepoId::github("acme/history-sql");
        mark_added(&state, rid.clone()).await;
        state
            .ref_store
            .save_branch(
                &rid,
                "main",
                &RefInfo {
                    commit: d,
                    build_status: Some("done".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        state
            .ref_store
            .save_branch(
                &rid,
                &exact_ref_store_key("main", &b),
                &RefInfo {
                    internal_exact_result: true,
                    commit: b.clone(),
                    build_status: Some(crate::remote_gc::EVICTED_BUILD_STATUS.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        unsafe { std::env::set_var("RIPCLONE_ORIGIN_BASE", base.path()) };
        let response = build_app(state)
            .oneshot(request_with_auth(
                "GET",
                "/v1/repos/github/acme/history-sql/refs/main?rev=HEAD~2",
                Some(&auth_header()),
            ))
            .await
            .unwrap();
        unsafe { std::env::remove_var("RIPCLONE_ORIGIN_BASE") };

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            crate::queue::JobQueue::depth(queue.as_ref()).await,
            1,
            "explicit and ordinary exact work use the same durable queue"
        );
        let claimed = crate::queue::WorkerQueue::claim(queue.as_ref(), "test-worker")
            .await
            .unwrap()
            .expect("exact revision job is claimable");
        assert_eq!(claimed.branch, "main");
        assert_eq!(claimed.admitted_commit, b);
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

    /// Happy path: valid job token → durable write through the server's RefStore.
    #[tokio::test]
    async fn ref_report_valid_token_writes_ref() {
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
        let app = build_app(state);

        let info = RefInfo {
            commit: "abc123".into(),
            default_branch: "main".into(),
            manifest: "m1".into(),
            ..Default::default()
        };
        let body = serde_json::json!({
            "op": "save_branch",
            "repo_key": repo_key,
            "branch": "main",
            "info": info,
        });
        let resp = app
            .oneshot(ref_report_request(Some(&tok), &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "valid token must write");

        let rid = RepoId::github("acme/widget");
        let stored = ref_store
            .load_branch(&rid, "main")
            .await
            .unwrap()
            .expect("ref must land in store");
        assert_eq!(stored.commit, "abc123");
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
        let app = build_app(state);

        let info = RefInfo {
            commit: "should-not-land".into(),
            default_branch: "main".into(),
            ..Default::default()
        };
        let body = serde_json::json!({
            "op": "save_branch",
            "repo_key": repo_key,
            "branch": "main",
            "info": info,
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

        let rid = RepoId::github("acme/nope");
        assert!(
            ref_store.load_branch(&rid, "main").await.unwrap().is_none(),
            "rejected reports must not write"
        );

        // Sanity: the good token still works (proves the store itself is fine).
        let resp = app
            .oneshot(ref_report_request(Some(&good), &body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(ref_store.load_branch(&rid, "main").await.unwrap().is_some());
    }
}
