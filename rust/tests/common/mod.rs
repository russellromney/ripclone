//! Shared harness for the in-process end-to-end tests.
//!
//! Spins up a real `ripclone` server in the current process backed by local
//! storage, and mirrors from local `file://` git origins (no network). The
//! `ripclone::client::Client` then drives real sync + clone round-trips.
#![allow(dead_code)]

use ripclone::client::Client;
use ripclone::server::{
    ArtifactBarrier, RateLimiter, ServerState, build_app, run_server_with_barrier,
};
use ripclone::storage::{HashEntry, StorageBackend, StorageRef};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::sync::{Arc, Once};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

pub const TOKEN: &str = "ripclone-e2e-token";

/// Process-global temp dir holding all `file://` origins for this test binary.
pub fn origin_root() -> &'static Path {
    use std::sync::OnceLock;
    static ROOT: OnceLock<TempDir> = OnceLock::new();
    ROOT.get_or_init(|| tempfile::tempdir().expect("origin root"))
        .path()
}

/// Configure process env once. `lsm` enables the incremental build with an
/// aggressive (1-byte) seal threshold so every non-empty tail seals a level.
pub fn init(lsm: bool) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // SAFETY: set once, before any server/client/sync reads them.
        unsafe {
            std::env::set_var("RIPCLONE_SERVER_TOKEN", TOKEN);
            std::env::set_var("RIPCLONE_NO_CACHE", "1");
            // Tests poll clones aggressively; don't let the default rate limiter
            // (burst 60) throttle them. Test-only — production keeps its limits.
            std::env::set_var("RIPCLONE_RATE_LIMIT_BURST", "1000000");
            std::env::set_var("RIPCLONE_RATE_LIMIT_PER_SEC", "1000000");
            std::env::set_var(
                "RIPCLONE_ORIGIN_BASE",
                format!("file://{}", origin_root().display()),
            );
            // Per-repo access enforcement (AU1) probes the provider over HTTP,
            // which can't reach these file:// test origins. These are
            // single-tenant local e2e tests, so use the documented trust-mode
            // escape hatch (the shared token is the only auth here).
            std::env::set_var("RIPCLONE_TRUST_GATEWAY", "1");
            // Two-phase publish and async builds are always on (no env toggle);
            // the helpers below poll for the background full/files variants.
            if lsm {
                std::env::set_var("RIPCLONE_LSM", "1");
            }
        }
    });
}

pub fn token_hash() -> String {
    hex::encode(Sha256::digest(TOKEN.as_bytes()))
}

/// Hand out a distinct loopback port per call, process-wide.
///
/// Binding `:0`, reading the port, then dropping the listener leaves a window
/// where the OS can re-hand the same freed port to a *concurrent* test's
/// `free_port()` before the first test's server binds it. Two servers then
/// target one port: the loser's bind fails silently, but its readiness probe
/// connects to the winner's listener and passes — so the loser runs against the
/// winner and, once the winner's `#[tokio::test]` runtime drops (killing its
/// listener), the loser's next request hits a dead port ("Connection refused").
/// CI runs the test binaries sequentially, so a monotonically-growing issued set
/// (ports stay reserved by live servers for the whole run) makes collisions
/// within a binary impossible.
fn free_port() -> u16 {
    use std::collections::HashSet;
    use std::sync::Mutex;
    static ISSUED: Mutex<Option<HashSet<u16>>> = Mutex::new(None);
    let mut guard = ISSUED.lock().unwrap_or_else(|e| e.into_inner());
    let issued = guard.get_or_insert_with(HashSet::new);
    for _ in 0..1000 {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if issued.insert(port) {
            return port;
        }
    }
    panic!("free_port: no unused loopback port after 1000 attempts");
}

/// A running in-process server. Keeps its storage dir alive for the test.
pub struct Server {
    pub url: String,
    pub cas_dir: PathBuf,
    pub storage_dir: PathBuf,
    pub repo_root: PathBuf,
    pub _dir: TempDir,
}

impl Server {
    pub fn client(&self) -> Client {
        Client::new_with_token(self.url.clone(), Some(token_hash()))
    }

    pub fn client_with_provider(&self, provider: &str, upstream_token: Option<&str>) -> Client {
        let mut client = Client::new_with_token(self.url.clone(), Some(token_hash()));
        client = client.with_provider(provider);
        if let Some(token) = upstream_token {
            client = client.with_upstream_token(token);
        }
        client
    }

    /// Path of a CAS object, for negative tests that tamper with storage.
    pub fn cas_path(&self, hash: &str) -> PathBuf {
        self.cas_dir.join(&hash[..2]).join(hash)
    }

    pub fn storage_path(&self, hash: &str) -> PathBuf {
        self.storage_dir.join(&hash[..2]).join(hash)
    }
}

/// Initialize a tracing subscriber once so server-side `info!`/`warn!`/`error!`
/// (e.g. background phase-2 failures) surface under `RUST_LOG` during tests.
fn init_tracing() {
    static T: Once = Once::new();
    T.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();
    });
}

pub async fn start_server() -> Server {
    start_server_inner(0, &[], None).await
}

/// Start a server with extra env vars (e.g. `RIPCLONE_WEBHOOK_SECRET`,
/// `RIPCLONE_POLL_INTERVAL_SECS`) set only during construction, under
/// `SERVER_START_LOCK`, so they can't leak into a concurrently-starting server.
pub async fn start_server_env(extra: &[(&str, &str)]) -> Server {
    start_server_inner(0, extra, None).await
}

/// Like `start_server`, but the server fails its first `fail_first` artifact
/// GETs (via `RIPCLONE_TEST_FAIL_FIRST_FETCHES`) so client retry/backoff can be
/// exercised end to end. The fault threshold is read once at construction; the
/// env var is set only while holding `SERVER_START_LOCK` and removed before the
/// lock drops, so no other server is constructed while it is set — keeping the
/// suite correct under parallel `cargo test`.
pub async fn start_server_faulting(fail_first: usize) -> Server {
    start_server_faulting_env(fail_first, &[]).await
}

/// Combine fault injection with extra server-construction env vars (e.g.
/// `RIPCLONE_JWT_TTL_SECS`) set under `SERVER_START_LOCK`.
pub async fn start_server_faulting_env(fail_first: usize, extra: &[(&str, &str)]) -> Server {
    start_server_inner(fail_first, extra, None).await
}

/// Start a server with a deterministic artifact download barrier installed.
/// See [`ripclone::server::ArtifactBarrier`].
pub async fn start_server_with_barrier(barrier: ArtifactBarrier) -> Server {
    start_server_inner(0, &[], Some(barrier)).await
}

pub async fn start_server_split_storage() -> Server {
    start_server_split_storage_inner(None, None, None).await
}

/// Start a split-storage server with a deterministic artifact download barrier.
/// See [`ripclone::server::ArtifactBarrier`].
pub async fn start_server_split_storage_barrier(barrier: ArtifactBarrier) -> Server {
    start_server_split_storage_inner(Some(barrier), None, None).await
}

/// Start a split-storage server whose durable-storage uploads fail after a
/// configurable number of successful writes. Used by failure-injection e2e
/// tests to prove failed builds do not publish refs that point at missing
/// artifacts.
pub async fn start_server_split_storage_failing_put(
    fail_after_successes: usize,
    failures: usize,
) -> Server {
    start_server_split_storage_inner(None, Some((fail_after_successes, failures)), None).await
}

/// Start a split-storage server whose ref-store writes fail after a configurable
/// number of successful writes. This gives failure-injection tests deterministic
/// coverage of metadata/DB publish failures without relying on filesystem
/// permissions or process-global environment.
pub async fn start_server_split_storage_failing_ref_save(
    fail_after_successes: usize,
    failures: usize,
) -> Server {
    start_server_split_storage_inner(None, None, Some((fail_after_successes, failures))).await
}

async fn start_server_split_storage_inner(
    barrier: Option<ArtifactBarrier>,
    fail_put: Option<(usize, usize)>,
    fail_ref: Option<(usize, usize)>,
) -> Server {
    init_tracing();
    let dir = tempfile::tempdir().expect("server dir");
    let cas_dir = dir.path().join("cas");
    let storage_dir = dir.path().join("storage");
    let repo_root = dir.path().join("repos");
    std::fs::create_dir_all(&repo_root).unwrap();
    let port = free_port();

    let cas = ripclone::cas::Cas::new(&cas_dir).unwrap();
    let base_storage: StorageRef = Arc::new(RemoteLocalStorage {
        inner: ripclone::storage::local(&storage_dir).unwrap(),
    });
    let storage: StorageRef = if let Some((fail_after_successes, failures)) = fail_put {
        Arc::new(FailingPutStorage::new(
            base_storage,
            fail_after_successes,
            failures,
        ))
    } else {
        base_storage
    };
    let base_ref_store: Arc<dyn ripclone::ref_store::RefStore> =
        Arc::new(ripclone::ref_store::FileRefStore::new(&repo_root));
    let ref_store: Arc<dyn ripclone::ref_store::RefStore> =
        if let Some((fail_after_successes, failures)) = fail_ref {
            Arc::new(FailingRefStore::new(
                base_ref_store,
                fail_after_successes,
                failures,
            ))
        } else {
            base_ref_store
        };
    let metrics = ripclone::metrics::Metrics::new();
    let retention = Arc::new(
        ripclone::retention::Retention::with_config_and_storage(
            cas.clone(),
            metrics.clone(),
            None,
            None,
            Some(storage.clone()),
        )
        .unwrap()
        .with_ref_store(ref_store.clone(), storage.clone()),
    );
    let (local_queue, mut rx, depth) = ripclone::queue::LocalJobQueue::new(16);
    let build_queue: ripclone::queue::JobQueueRef = Arc::new(local_queue);
    let provider_registry = ripclone::provider::ProviderRegistry::new();
    let broker: Arc<dyn ripclone::auth::broker::CredentialBroker> = Arc::new(
        ripclone::auth::broker::StaticBroker::new(provider_registry.clone()),
    );
    let state = ServerState {
        cas,
        repo_config: Arc::new(ripclone::repo_config::RepoConfigStore::new(storage.clone())),
        storage,
        repo_root: repo_root.clone(),
        ref_store,
        provider_registry,
        broker,
        token_hash: Some(token_hash()),
        jwt: None,
        metrics,
        rate_limiter: RateLimiter::new(1000000, 1000000.0),
        retention,
        build_queue,
        build_queue_depth: depth,
        build_waiters: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        oidc_verifier: None,
        webhook_config: Arc::new(ripclone::webhook::WebhookConfig::empty()),
        sync_locks: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        mirror_freshness: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        mirror_fresh_ttl: Duration::from_secs(60),
        ref_response_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        artifact_fetch_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        fail_first_fetches: 0,
        artifact_barrier: barrier,
        readyz_cache: Arc::new(std::sync::Mutex::new(None)),
        access_verifier: Arc::new(ripclone::auth::access::HttpAccessVerifier::new()),
        require_repo_auth: false,
    };

    let worker_state = state.clone();
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let state = worker_state.clone();
            tokio::spawn(async move {
                let key = format!(
                    "{}/{}#{}",
                    job.repo_id.storage_key(),
                    job.branch,
                    job.rev.as_deref().unwrap_or("")
                );
                let st = state.clone();
                let result = match tokio::spawn(async move {
                    ripclone::server::process_build_job(&st, &job).await
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => Err(ripclone::queue::BuildError::retryable(format!(
                        "build task panicked: {e}"
                    ))),
                };
                state
                    .build_queue_depth
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                if let Some(senders) = state.build_waiters.lock().await.remove(&key) {
                    for sender in senders {
                        let _ = sender.send(result.clone());
                    }
                }
            });
        }
    });

    let app = build_app(state);
    tokio::spawn(async move {
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    let mut ready = false;
    for _ in 0..400 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        ready,
        "split-storage server on port {port} did not become ready"
    );
    Server {
        url: format!("http://127.0.0.1:{port}"),
        cas_dir,
        storage_dir,
        repo_root,
        _dir: dir,
    }
}

/// A local filesystem storage backend that reports `is_remote() = true` so
/// `RemoteGc` can be exercised in tests without an S3-compatible store.
pub struct RemoteLocalStorage {
    inner: StorageRef,
}

impl RemoteLocalStorage {
    pub fn new(inner: StorageRef) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl StorageBackend for RemoteLocalStorage {
    fn get(&self, hash: &str) -> anyhow::Result<Vec<u8>> {
        self.inner.get(hash)
    }

    fn get_range(&self, hash: &str, start: u64, len: u64) -> anyhow::Result<Vec<u8>> {
        self.inner.get_range(hash, start, len)
    }

    fn put(&self, hash: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put(hash, data)
    }

    async fn put_async(&self, hash: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_async(hash, data).await
    }

    async fn put_file_async(&self, hash: &str, path: &std::path::Path) -> anyhow::Result<()> {
        self.inner.put_file_async(hash, path).await
    }

    async fn get_meta(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_meta(key).await
    }

    async fn put_meta(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_meta(key, data).await
    }

    fn size(&self, hash: &str) -> anyhow::Result<u64> {
        self.inner.size(hash)
    }

    fn is_remote(&self) -> bool {
        true
    }

    fn regions(&self) -> Vec<String> {
        vec!["test-remote-local".to_string()]
    }

    fn delete(&self, hash: &str) -> anyhow::Result<()> {
        self.inner.delete(hash)
    }

    fn delete_batch(&self, hashes: &[String]) -> anyhow::Result<u64> {
        self.inner.delete_batch(hashes)
    }

    fn list_hashes(&self) -> anyhow::Result<Vec<HashEntry>> {
        self.inner.list_hashes()
    }

    fn health(&self) -> anyhow::Result<()> {
        self.inner.health()
    }
}

pub struct FailingPutStorage {
    inner: StorageRef,
    fail_after_successes: Mutex<usize>,
    failures_remaining: Mutex<usize>,
}

impl FailingPutStorage {
    pub fn new(inner: StorageRef, fail_after_successes: usize, failures: usize) -> Self {
        Self {
            inner,
            fail_after_successes: Mutex::new(fail_after_successes),
            failures_remaining: Mutex::new(failures),
        }
    }

    fn should_fail_put(&self) -> bool {
        let mut successes = self.fail_after_successes.lock().unwrap();
        if *successes > 0 {
            *successes -= 1;
            return false;
        }
        drop(successes);

        let mut failures = self.failures_remaining.lock().unwrap();
        if *failures > 0 {
            *failures -= 1;
            return true;
        }
        false
    }
}

pub struct FailingRefStore {
    inner: Arc<dyn ripclone::ref_store::RefStore>,
    fail_after_successes: Mutex<usize>,
    failures_remaining: Mutex<usize>,
}

impl FailingRefStore {
    pub fn new(
        inner: Arc<dyn ripclone::ref_store::RefStore>,
        fail_after_successes: usize,
        failures: usize,
    ) -> Self {
        Self {
            inner,
            fail_after_successes: Mutex::new(fail_after_successes),
            failures_remaining: Mutex::new(failures),
        }
    }

    fn should_fail_write(&self) -> bool {
        let mut successes = self.fail_after_successes.lock().unwrap();
        if *successes > 0 {
            *successes -= 1;
            return false;
        }
        drop(successes);

        let mut failures = self.failures_remaining.lock().unwrap();
        if *failures > 0 {
            *failures -= 1;
            return true;
        }
        false
    }
}

#[async_trait::async_trait]
impl ripclone::ref_store::RefStore for FailingRefStore {
    async fn load(
        &self,
        repo_id: &ripclone::provider::RepoId,
    ) -> anyhow::Result<Option<ripclone::RefInfo>> {
        self.inner.load(repo_id).await
    }

    async fn save(
        &self,
        repo_id: &ripclone::provider::RepoId,
        info: &ripclone::RefInfo,
    ) -> anyhow::Result<()> {
        if self.should_fail_write() {
            anyhow::bail!(
                "injected ref-store save failure for {}",
                repo_id.storage_key()
            );
        }
        self.inner.save(repo_id, info).await
    }

    async fn list(&self) -> anyhow::Result<Vec<ripclone::provider::RepoId>> {
        self.inner.list().await
    }

    async fn add_repo(&self, repo: &ripclone::ref_store::AddedRepo) -> anyhow::Result<()> {
        self.inner.add_repo(repo).await
    }

    async fn load_added_repo(
        &self,
        repo_id: &ripclone::provider::RepoId,
    ) -> anyhow::Result<Option<ripclone::ref_store::AddedRepo>> {
        self.inner.load_added_repo(repo_id).await
    }

    async fn list_added_repos(&self) -> anyhow::Result<Vec<ripclone::ref_store::AddedRepo>> {
        self.inner.list_added_repos().await
    }

    async fn load_branch(
        &self,
        repo_id: &ripclone::provider::RepoId,
        branch: &str,
    ) -> anyhow::Result<Option<ripclone::RefInfo>> {
        self.inner.load_branch(repo_id, branch).await
    }

    async fn load_build(
        &self,
        repo_id: &ripclone::provider::RepoId,
        commit: &str,
    ) -> anyhow::Result<Option<ripclone::RefInfo>> {
        self.inner.load_build(repo_id, commit).await
    }

    async fn save_branch(
        &self,
        repo_id: &ripclone::provider::RepoId,
        branch: &str,
        info: &ripclone::RefInfo,
    ) -> anyhow::Result<()> {
        if self.should_fail_write() {
            anyhow::bail!(
                "injected ref-store save_branch failure for {}@{branch}",
                repo_id.storage_key()
            );
        }
        self.inner.save_branch(repo_id, branch, info).await
    }

    async fn update_build_status(
        &self,
        repo_id: &ripclone::provider::RepoId,
        branch: &str,
        expected_commit: &str,
        status: &str,
    ) -> anyhow::Result<bool> {
        self.inner
            .update_build_status(repo_id, branch, expected_commit, status)
            .await
    }

    async fn touch_last_accessed_at(
        &self,
        repo_id: &ripclone::provider::RepoId,
        branch: &str,
        expected_commit: &str,
    ) -> anyhow::Result<bool> {
        self.inner
            .touch_last_accessed_at(repo_id, branch, expected_commit)
            .await
    }

    async fn delete_branch(
        &self,
        repo_id: &ripclone::provider::RepoId,
        branch: &str,
    ) -> anyhow::Result<()> {
        self.inner.delete_branch(repo_id, branch).await
    }

    async fn list_branches(
        &self,
        repo_id: &ripclone::provider::RepoId,
    ) -> anyhow::Result<Vec<String>> {
        self.inner.list_branches(repo_id).await
    }

    async fn invalidate(&self, repo_id: &ripclone::provider::RepoId, branch: &str) {
        self.inner.invalidate(repo_id, branch).await
    }

    async fn health(&self) -> anyhow::Result<()> {
        self.inner.health().await
    }
}

#[async_trait::async_trait]
impl StorageBackend for FailingPutStorage {
    fn get(&self, hash: &str) -> anyhow::Result<Vec<u8>> {
        self.inner.get(hash)
    }

    fn get_range(&self, hash: &str, start: u64, len: u64) -> anyhow::Result<Vec<u8>> {
        self.inner.get_range(hash, start, len)
    }

    fn put(&self, hash: &str, data: &[u8]) -> anyhow::Result<()> {
        if self.should_fail_put() {
            anyhow::bail!("injected durable-storage put failure for {hash}");
        }
        self.inner.put(hash, data)
    }

    async fn put_async(&self, hash: &str, data: &[u8]) -> anyhow::Result<()> {
        if self.should_fail_put() {
            anyhow::bail!("injected durable-storage put_async failure for {hash}");
        }
        self.inner.put_async(hash, data).await
    }

    async fn put_file_async(&self, hash: &str, path: &std::path::Path) -> anyhow::Result<()> {
        if self.should_fail_put() {
            anyhow::bail!("injected durable-storage put_file_async failure for {hash}");
        }
        self.inner.put_file_async(hash, path).await
    }

    async fn get_meta(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_meta(key).await
    }

    async fn put_meta(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_meta(key, data).await
    }

    fn size(&self, hash: &str) -> anyhow::Result<u64> {
        self.inner.size(hash)
    }

    fn is_remote(&self) -> bool {
        self.inner.is_remote()
    }

    fn regions(&self) -> Vec<String> {
        self.inner.regions()
    }

    fn delete(&self, hash: &str) -> anyhow::Result<()> {
        self.inner.delete(hash)
    }

    fn delete_batch(&self, hashes: &[String]) -> anyhow::Result<u64> {
        self.inner.delete_batch(hashes)
    }

    fn list_hashes(&self) -> anyhow::Result<Vec<HashEntry>> {
        self.inner.list_hashes()
    }

    fn health(&self) -> anyhow::Result<()> {
        self.inner.health()
    }
}

/// Serializes server *construction* (the brief startup window only, not whole
/// tests) so the per-server `RIPCLONE_TEST_FAIL_FIRST_FETCHES` env var, read
/// once at construction, can't leak into a concurrently-starting server.
static SERVER_START_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_server_inner(
    fail_first: usize,
    extra_env: &[(&str, &str)],
    artifact_barrier: Option<ArtifactBarrier>,
) -> Server {
    init_tracing();
    let dir = tempfile::tempdir().expect("server dir");
    let cas_dir = dir.path().join("cas");
    let repo_root = dir.path().join("repos");
    let port = free_port();
    let (cas2, repos2) = (cas_dir.clone(), repo_root.clone());

    let _start_guard = SERVER_START_LOCK.lock().await;
    if fail_first > 0 {
        // SAFETY: set under SERVER_START_LOCK and removed before it drops, so no
        // concurrently-constructing server observes it.
        unsafe {
            std::env::set_var("RIPCLONE_TEST_FAIL_FIRST_FETCHES", fail_first.to_string());
        }
    }
    // Same SAFETY: set under the lock, removed once the server has read them.
    for (k, v) in extra_env {
        unsafe {
            std::env::set_var(k, v);
        }
    }
    tokio::spawn(async move {
        let _ = run_server_with_barrier(&cas2, &repos2, "127.0.0.1", port, artifact_barrier).await;
    });
    // Wait until the port accepts connections. The server state (including the
    // fault threshold read) is constructed before the listener binds, so by the
    // time the port is up the env var has been consumed.
    let mut ready = false;
    for _ in 0..400 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if fail_first > 0 {
        unsafe {
            std::env::remove_var("RIPCLONE_TEST_FAIL_FIRST_FETCHES");
        }
    }
    for (k, _) in extra_env {
        unsafe {
            std::env::remove_var(k);
        }
    }
    drop(_start_guard);
    assert!(ready, "server on port {port} did not become ready");
    Server {
        url: format!("http://127.0.0.1:{port}"),
        storage_dir: cas_dir.clone(),
        cas_dir,
        repo_root,
        _dir: dir,
    }
}

// ---- local git origin ----------------------------------------------------

pub fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} in {} failed: {}",
        args,
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

pub fn git_ok(dir: &Path, args: &[&str]) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// A local origin: a working repo plus a bare repo published under the origin
/// root at `<owner>/<repo>.git`, which the server mirrors via `file://`.
pub struct Origin {
    pub owner: String,
    pub repo: String,
    pub work: PathBuf,
    pub bare: PathBuf,
    _dir: TempDir,
}

impl Origin {
    pub fn commit(&self, files: &[(&str, &str)], msg: &str) -> String {
        for (name, content) in files {
            let p = self.work.join(name);
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(p, content).unwrap();
        }
        git(&self.work, &["add", "-A"]);
        git(
            &self.work,
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                msg,
            ],
        );
        git(&self.work, &["rev-parse", "HEAD"])
    }

    pub fn commit_bytes(&self, files: &[(&str, &[u8])], msg: &str) -> String {
        for (name, content) in files {
            let p = self.work.join(name);
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(p, content).unwrap();
        }
        git(&self.work, &["add", "-A"]);
        git(
            &self.work,
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                msg,
            ],
        );
        git(&self.work, &["rev-parse", "HEAD"])
    }

    /// Publish the current work tree to the bare origin the server mirrors.
    pub fn publish(&self) {
        git(
            &self.work,
            &["push", "-q", "--force", self.bare_str(), "main"],
        );
        // Keep the bare repo's HEAD pointing at main so `--mirror` resolves it.
        git(&self.bare, &["symbolic-ref", "HEAD", "refs/heads/main"]);
    }

    pub fn bare_str(&self) -> &str {
        self.bare.to_str().unwrap()
    }
}

/// Create a local origin under the process origin root.
pub fn make_origin(owner: &str, repo: &str) -> Origin {
    let dir = tempfile::tempdir().expect("origin work dir");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &["init", "-q", "-b", "main"]);
    let bare = origin_root().join(owner).join(format!("{repo}.git"));
    std::fs::create_dir_all(bare.parent().unwrap()).unwrap();
    git(
        &PathBuf::from("."),
        &["init", "--bare", "-q", "-b", "main", bare.to_str().unwrap()],
    );
    Origin {
        owner: owner.to_string(),
        repo: repo.to_string(),
        work,
        bare,
        _dir: dir,
    }
}

// ---- local HTTP origin (multi-provider e2e) -------------------------------

/// An HTTP-served git origin for testing non-github providers.
///
/// The bare repo is served by `python3 -m http.server` from its directory.
/// Dumb HTTP is enabled with `git update-server-info`.
pub struct HttpOrigin {
    pub path: String,
    pub work: PathBuf,
    pub bare: PathBuf,
    pub url: String,
    pub port: u16,
    auth_log: Option<PathBuf>,
    _dir: TempDir,
    _server: std::process::Child,
}

impl HttpOrigin {
    pub fn commit(&self, files: &[(&str, &str)], msg: &str) -> String {
        for (name, content) in files {
            let p = self.work.join(name);
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(p, content).unwrap();
        }
        git(&self.work, &["add", "-A"]);
        git(
            &self.work,
            &[
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                msg,
            ],
        );
        git(&self.work, &["rev-parse", "HEAD"])
    }

    pub fn publish(&self) {
        git(
            &self.work,
            &["push", "-q", "--force", self.bare_str(), "main"],
        );
        git(&self.bare, &["symbolic-ref", "HEAD", "refs/heads/main"]);
        git(&self.bare, &["update-server-info"]);
    }

    pub fn bare_str(&self) -> &str {
        self.bare.to_str().unwrap()
    }

    pub fn auth_reject_count(&self) -> usize {
        self.auth_status_count("403")
    }

    fn auth_status_count(&self, status: &str) -> usize {
        let Some(path) = &self.auth_log else {
            return 0;
        };
        let Ok(log) = std::fs::read_to_string(path) else {
            return 0;
        };
        log.lines()
            .filter(|line| line.split('\t').next() == Some(status))
            .count()
    }
}

/// Create a bare repo under `<repo_path>.git`, enable dumb HTTP, and serve the
/// parent directory on a free port. The resulting clone URL is
/// `http://127.0.0.1:<port>/<repo_path>.git`, matching the generic provider's
/// `clone_url(path)` shape.
pub fn make_http_origin(repo_path: &str) -> HttpOrigin {
    make_http_origin_inner(repo_path, None)
}

/// Like [`make_http_origin`], but the server rejects any request whose
/// `Authorization` header is not exactly `expected_auth`. Used to prove that
/// ripclone injects the provider-specific auth header on the upstream fetch.
pub fn make_http_origin_with_auth(repo_path: &str, expected_auth: &str) -> HttpOrigin {
    make_http_origin_inner(repo_path, Some(expected_auth))
}

fn make_http_origin_inner(repo_path: &str, expected_auth: Option<&str>) -> HttpOrigin {
    let dir = tempfile::tempdir().expect("http origin dir");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &["init", "-q", "-b", "main"]);

    let bare = dir.path().join(format!("{}.git", repo_path));
    std::fs::create_dir_all(bare.parent().unwrap()).unwrap();
    git(
        &PathBuf::from("."),
        &["init", "--bare", "-q", "-b", "main", bare.to_str().unwrap()],
    );

    // Enable dumb HTTP.
    git(&bare, &["config", "http.receivepack", "true"]);
    git(&bare, &["update-server-info"]);

    let port = free_port();
    let auth_log = expected_auth.map(|_| dir.path().join("auth.log"));
    let server = if let Some(auth) = expected_auth {
        // A tiny real HTTP server that gates every request on the exact
        // Authorization header ripclone is expected to inject.
        let script = dir.path().join("auth_server.py");
        let script_body = r#"import http.server
import os
import socketserver
import sys

EXPECTED_AUTH = sys.argv[1]
PORT = int(sys.argv[2])
ROOT = sys.argv[3]
LOG = sys.argv[4]

class AuthHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def check_auth(self):
        return self.headers.get('Authorization') == EXPECTED_AUTH

    def record(self, status):
        with open(LOG, 'a', encoding='utf-8') as f:
            f.write(f"{status}\t{self.command}\t{self.path}\t{self.headers.get('Authorization', '')}\n")

    def do_GET(self):
        if not self.check_auth():
            self.record(403)
            self.send_error(403, 'Forbidden')
            return
        self.record(200)
        super().do_GET()

    def do_HEAD(self):
        if not self.check_auth():
            self.record(403)
            self.send_error(403, 'Forbidden')
            return
        self.record(200)
        super().do_HEAD()

    def log_message(self, format, *args):
        pass

class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

with ReusableTCPServer(('', PORT), AuthHandler) as httpd:
    httpd.serve_forever()
"#;
        std::fs::write(&script, script_body).unwrap();
        let log = auth_log.as_ref().expect("auth log path");
        std::fs::write(log, "").unwrap();
        Command::new("python3")
            .arg(script.to_str().unwrap())
            .arg(auth)
            .arg(port.to_string())
            .arg(dir.path().to_str().unwrap())
            .arg(log.to_str().unwrap())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("start python auth http.server")
    } else {
        Command::new("python3")
            .arg("-m")
            .arg("http.server")
            .arg(port.to_string())
            .current_dir(dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("start python http.server")
    };

    // Wait for the server to accept connections.
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    HttpOrigin {
        path: repo_path.to_string(),
        work,
        bare,
        url: format!("http://127.0.0.1:{port}"),
        port,
        auth_log,
        _dir: dir,
        _server: server,
    }
}

/// Install (clone) without syncing first — returns Result so callers can retry
/// (e.g. waiting for two-phase phase 2 to publish the full clonepack).
pub async fn clone_only(
    server: &Server,
    owner: &str,
    repo: &str,
    depth: usize,
    mode: ripclone::mode::CloneMode,
) -> anyhow::Result<(TempDir, std::path::PathBuf)> {
    clone_only_at(server, owner, repo, None, depth, mode).await
}

/// Like [`clone_only`] but clones the artifacts built for `rev` (e.g. "HEAD~2"),
/// pairing with a `sync` at that rev.
pub async fn clone_only_at(
    server: &Server,
    owner: &str,
    repo: &str,
    rev: Option<&str>,
    depth: usize,
    mode: ripclone::mode::CloneMode,
) -> anyhow::Result<(TempDir, std::path::PathBuf)> {
    let client = server.client();
    let repo_path = format!("{owner}/{repo}");
    ensure_added(server, &repo_path).await?;
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("clone");
    let kind = ripclone::mode::clonepack_kind_for_depth(depth);
    client
        .install_repo_with_mode_at(&repo_path, "HEAD", rev, &target, mode, Some(kind), None)
        .await?;
    Ok((out, target))
}

pub async fn ensure_added(server: &Server, repo: &str) -> anyhow::Result<()> {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};

    static ADDED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let key = format!("{} {repo}", server.url);
    if ADDED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .contains(&key)
    {
        return Ok(());
    }
    server.client().add_repo(repo).await?;
    ADDED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .insert(key);
    Ok(())
}

pub async fn register_added_without_build(server: &Server, repo: &str) -> anyhow::Result<()> {
    use ripclone::provider::RepoId;
    register_added_without_build_repo_id(server, RepoId::github(repo)).await
}

/// Same as [`register_added_without_build`] but for a repo that belongs to a
/// non-default provider instance (e.g. `github-bad`, `gitlab-auth`). The gate's
/// added-repo lookup keys on the full `RepoId` (provider + path), so a repo
/// synced through a provider client must be registered under that same provider.
pub async fn register_added_without_build_for_provider(
    server: &Server,
    provider: &str,
    path: &str,
) -> anyhow::Result<()> {
    use ripclone::provider::{ProviderInstanceId, RepoId};
    let repo_id = RepoId {
        provider: ProviderInstanceId::new(provider),
        path: path.to_string(),
    };
    register_added_without_build_repo_id(server, repo_id).await
}

async fn register_added_without_build_repo_id(
    server: &Server,
    repo_id: ripclone::provider::RepoId,
) -> anyhow::Result<()> {
    use ripclone::ref_store::{AddedRepo, AddedRepoSource, FileRefStore, RefStore};

    let added = AddedRepo {
        repo_id,
        added_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        history_enabled: true,
        source: AddedRepoSource::Api,
        repo_size_bytes: None,
    };
    if std::env::var("RIPCLONE_METADATA").ok().as_deref() == Some("sqlite") {
        use ripclone::meta::{SqlRefStore, SqliteMeta};

        let url = std::env::var("RIPCLONE_METADATA_DB_URL")?;
        return SqlRefStore::new(Box::new(SqliteMeta::connect(&url).await?))
            .await?
            .add_repo(&added)
            .await;
    }

    FileRefStore::new(&server.repo_root).add_repo(&added).await
}

/// Read a file from a clone (panics if missing).
pub fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name))
        .unwrap_or_else(|e| panic!("read {} in {}: {e}", name, dir.display()))
}

/// Unified per-binary configuration: base env plus the LSM build flag. Two-phase
/// publish and async builds are always on. Because the flags are read from
/// process env, each test binary pins exactly one config; call this at the top of
/// every test in the binary. The LSM build seals every advancing tail and
/// compacts at `max_levels` (16).
pub fn setup(lsm: bool) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| unsafe {
        std::env::set_var("RIPCLONE_SERVER_TOKEN", TOKEN);
        std::env::set_var("RIPCLONE_NO_CACHE", "1");
        // Tests poll clones aggressively; don't let the default rate limiter
        // throttle them. Test-only — production keeps its configured limits.
        std::env::set_var("RIPCLONE_RATE_LIMIT_BURST", "1000000");
        std::env::set_var("RIPCLONE_RATE_LIMIT_PER_SEC", "1000000");
        std::env::set_var(
            "RIPCLONE_ORIGIN_BASE",
            format!("file://{}", origin_root().display()),
        );
        // Single-tenant local e2e: AU1 access enforcement can't probe file://
        // origins over HTTP, so use the documented trust-mode escape hatch.
        std::env::set_var("RIPCLONE_TRUST_GATEWAY", "1");
        std::env::set_var("RIPCLONE_LSM", if lsm { "1" } else { "0" });
    });
}

/// Clone the full (depth=0) editable variant, waiting for it to reach
/// `want_count` commits. The full variant is built in the background (phase 2),
/// so poll until it lands.
pub async fn clone_full_at(
    server: &Server,
    owner: &str,
    repo: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    let attempts = 160;
    let mut last = String::from("<no successful clone>");
    for _ in 0..attempts {
        match clone_only(server, owner, repo, 0, ripclone::mode::CloneMode::Editable).await {
            Ok((g, d)) => {
                let c = git(&d, &["rev-list", "--count", "HEAD"]);
                if c == want_count {
                    return (g, d);
                }
                last = format!("count={c}");
            }
            Err(e) => last = format!("clone err: {e:#}"),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("depth=0 never reached {want_count} for {owner}/{repo} (last: {last})");
}

/// Clone files mode, waiting until `probe` exists with `want` contents (the
/// full archive is built in phase 2).
pub async fn clone_files_when(
    server: &Server,
    owner: &str,
    repo: &str,
    probe: &str,
    want: &str,
) -> (TempDir, PathBuf) {
    let attempts = 160;
    for _ in 0..attempts {
        if let Ok((g, d)) =
            clone_only(server, owner, repo, 0, ripclone::mode::CloneMode::Files).await
            && d.join(probe).exists()
            && read(&d, probe) == want
        {
            return (g, d);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("files-mode clone never materialized {probe}={want:?} for {owner}/{repo}");
}

/// Assert a depth=1 editable clone is immediately correct: a real, shallow git
/// repo with HEAD content present and a clean status.
pub async fn assert_depth1(server: &Server, owner: &str, repo: &str, files: &[(&str, &str)]) {
    let (_g, d) = clone_only(server, owner, repo, 1, ripclone::mode::CloneMode::Editable)
        .await
        .expect("depth=1 clone right after sync");
    assert!(d.join(".git/shallow").exists(), "depth=1 is shallow");
    assert_eq!(
        git(&d, &["rev-list", "--count", "HEAD"]),
        "1",
        "depth=1 count"
    );
    for (name, want) in files {
        assert_eq!(&read(&d, name), want, "depth=1 {name}");
    }
    assert_eq!(
        git(&d, &["status", "--porcelain"]),
        "",
        "depth=1 status clean"
    );
}

/// Assert a full clone is a real, usable git repo: history walks, `show` works,
/// and a fresh local commit lands on top.
pub fn assert_repo_usable(dir: &Path, want_count: &str) {
    assert!(
        git_ok(dir, &["rev-list", "--objects", "HEAD"]),
        "full object closure complete"
    );
    assert!(
        git_ok(dir, &["fsck", "--connectivity-only", "HEAD"]),
        "fsck"
    );
    assert_eq!(
        git(dir, &["rev-list", "--count", "HEAD"]),
        want_count,
        "count"
    );
    assert!(!dir.join(".git/shallow").exists(), "full clone not shallow");
    assert_eq!(git(dir, &["status", "--porcelain"]), "", "status clean");
    assert!(git_ok(dir, &["show", "--stat", "HEAD"]), "git show HEAD");
    assert!(git_ok(dir, &["log", "--oneline"]), "git log");
    // A new local commit must succeed and advance the count.
    std::fs::write(dir.join("WORKED.txt"), b"local edit\n").unwrap();
    git(dir, &["add", "WORKED.txt"]);
    git(
        dir,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "local",
        ],
    );
    let want_plus_one = (want_count.parse::<u64>().unwrap() + 1).to_string();
    assert_eq!(
        git(dir, &["rev-list", "--count", "HEAD"]),
        want_plus_one,
        "local commit advances history"
    );
}

/// The full correctness lifecycle for one server config: first sync, re-sync on
/// a new commit, and multi-commit growth — each verified across depth=1,
/// depth=0 (real usable repo), and files mode. The full/files variants build in
/// the background (phase 2), so they are polled for.
pub async fn lifecycle_battery(server: &Server, origin: &Origin) {
    let client = server.client();
    let (o, r) = (origin.owner.clone(), origin.repo.clone());

    // ---- initial history: c1, c2 ----
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.commit(&[("a.txt", "2\n"), ("dir/b.txt", "B\n")], "c2");
    origin.publish();
    ensure_added(server, &format!("{o}/{r}"))
        .await
        .expect("add lifecycle repo");
    client
        .sync_repo(&format!("{o}/{r}"), None)
        .await
        .expect("sync c2");

    assert_depth1(server, &o, &r, &[("a.txt", "2\n"), ("dir/b.txt", "B\n")]).await;
    {
        let (_g, d) = clone_full_at(server, &o, &r, "2").await;
        assert_eq!(read(&d, "a.txt"), "2\n");
        assert_eq!(read(&d, "dir/b.txt"), "B\n");
        assert_repo_usable(&d, "2");
    }
    {
        let (_g, d) = clone_files_when(server, &o, &r, "a.txt", "2\n").await;
        assert_eq!(read(&d, "dir/b.txt"), "B\n");
    }

    // ---- re-sync: new commit c3 (incremental tail under LSM) ----
    origin.commit(&[("a.txt", "3\n"), ("c.txt", "C\n")], "c3");
    origin.publish();
    client
        .sync_repo(&format!("{o}/{r}"), None)
        .await
        .expect("resync c3");

    assert_depth1(server, &o, &r, &[("a.txt", "3\n"), ("c.txt", "C\n")]).await;
    {
        let (_g, d) = clone_full_at(server, &o, &r, "3").await;
        assert_eq!(read(&d, "a.txt"), "3\n");
        assert_eq!(read(&d, "c.txt"), "C\n");
        assert_repo_usable(&d, "3");
    }
    {
        let (_g, d) = clone_files_when(server, &o, &r, "a.txt", "3\n").await;
        assert_eq!(read(&d, "c.txt"), "C\n");
    }

    // ---- multi-commit growth: exercises level accumulation + reuse +
    // compaction (LSM) or repeated full rebuild (non-LSM). ----
    for i in 4..=8u32 {
        let f = format!("f{i}.txt");
        let c = format!("{i}\n");
        let m = format!("c{i}");
        origin.commit(&[(f.as_str(), c.as_str())], &m);
        origin.publish();
        client
            .sync_repo(&format!("{o}/{r}"), None)
            .await
            .expect("resync loop");
        // The full variant builds in the background; wait for each step to land
        // before advancing so successive phase-2 builds don't run concurrently on
        // the same mirror (the async queue serializes this in production). Also
        // verifies the full clone at every incremental step.
        let _ = clone_full_at(server, &o, &r, &i.to_string()).await;
    }
    let (_g, d) = clone_full_at(server, &o, &r, "8").await;
    for i in 4..=8u32 {
        assert!(
            d.join(format!("f{i}.txt")).exists(),
            "f{i}.txt after growth"
        );
    }
    assert_repo_usable(&d, "8");
}

/// Clone helper: sync, then install with the given depth, returning the dir.
///
/// Builds are two-phase: depth=1 is ready as soon as `sync` returns, but the
/// full (depth=0) and files variants build in the background (phase 2) and, on a
/// resync, serve the previous commit until phase 2 lands. So poll the install
/// until the clone's HEAD matches the just-published origin HEAD.
pub async fn sync_and_clone(
    server: &Server,
    origin: &Origin,
    depth: usize,
    mode: ripclone::mode::CloneMode,
) -> (TempDir, PathBuf) {
    let client = server.client();
    ensure_added(server, &format!("{}/{}", origin.owner, origin.repo))
        .await
        .expect("add before sync_and_clone");
    client
        .sync_repo(&format!("{}/{}", origin.owner, origin.repo), None)
        .await
        .expect("sync");
    let want = git(&origin.bare, &["rev-parse", "HEAD"]);
    let kind = ripclone::mode::clonepack_kind_for_depth(depth);
    // Files mode materializes a worktree only (intentionally not a git repo), so
    // it has no HEAD to compare; the git modes resolve HEAD to the built commit.
    let files_mode = matches!(mode, ripclone::mode::CloneMode::Files);
    let mut last = String::from("<no successful install>");
    for _ in 0..160 {
        let out = tempfile::tempdir().unwrap();
        let target = out.path().join("clone");
        match client
            .install_repo_with_mode(
                &origin.owner,
                &origin.repo,
                "HEAD",
                &target,
                mode,
                Some(kind),
                None,
            )
            .await
        {
            Ok(_) => {
                let ready = if files_mode {
                    dir_has_file(&target)
                } else {
                    git_ok(&target, &["rev-parse", "--verify", "HEAD"])
                        && git(&target, &["rev-parse", "HEAD"]) == want
                };
                if ready {
                    return (out, target);
                }
                last = "clone not yet current".to_string();
            }
            Err(e) => last = format!("install err: {e:#}"),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "sync_and_clone never reached HEAD {want} for {}/{} (last: {last})",
        origin.owner, origin.repo
    );
}

/// Poll `/sync` until the clonepack manifest for the current commit is published,
/// returning the ref response. Builds are two-phase: depth=1 publishes first and
/// the full clonepack (with its manifest + archive) builds in the background, so
/// the first sync's `clonepack_manifest` can be empty.
pub async fn sync_until_manifest(
    server: &Server,
    owner: &str,
    repo: &str,
) -> ripclone::client::RefResponse {
    let client = server.client();
    ensure_added(server, &format!("{owner}/{repo}"))
        .await
        .expect("add before sync_until_manifest");
    let mut last = String::from("<no successful sync>");
    for _ in 0..160 {
        match client.sync_repo(&format!("{owner}/{repo}"), None).await {
            Ok(resp) if !resp.clonepack_manifest.is_empty() => return resp,
            Ok(resp) => last = format!("manifest empty at commit {}", resp.commit),
            Err(e) => last = format!("sync err: {e:#}"),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("clonepack manifest never published for {owner}/{repo} (last: {last})");
}

/// Poll `/sync` until the build has fully settled: the archive is published, so
/// `clonepack_manifest` has reached its final, stable hash for this commit.
///
/// Phase 2 publishes the full clonepack manifest *twice* — first an editable
/// manifest (`build_status = "archive building"`), then, once the zstd archive
/// finishes, a distinct files manifest with `archive_ready = true`. A test that
/// captures the manifest hash before the archive lands would tamper with the
/// transient editable hash while the clone goes on to fetch the final files
/// hash. Waiting for `archive_ready` pins the returned `clonepack_manifest` to
/// the exact artifact the subsequent clone resolves, so negative tests that
/// corrupt/remove that hash are deterministic under parallel load.
pub async fn sync_until_archive_ready(
    server: &Server,
    owner: &str,
    repo: &str,
) -> ripclone::client::RefResponse {
    let client = server.client();
    ensure_added(server, &format!("{owner}/{repo}"))
        .await
        .expect("add before sync_until_archive_ready");
    let mut last = String::from("<no successful sync>");
    for _ in 0..160 {
        match client.sync_repo(&format!("{owner}/{repo}"), None).await {
            Ok(resp) if resp.archive_ready && !resp.clonepack_manifest.is_empty() => return resp,
            Ok(resp) => {
                last = format!(
                    "archive_ready={} manifest_empty={} at commit {}",
                    resp.archive_ready,
                    resp.clonepack_manifest.is_empty(),
                    resp.commit
                )
            }
            Err(e) => last = format!("sync err: {e:#}"),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("archive never became ready for {owner}/{repo} (last: {last})");
}

/// Shared B5 seam: make a repo warm enough that a subsequent clone can fetch
/// real bytes, not just observe that `/sync` returned. Today this means polling
/// until the full clonepack manifest exists; B5 can change add/sync semantics in
/// one place.
pub async fn warm_repo_until_cloneable(
    server: &Server,
    owner: &str,
    repo: &str,
) -> ripclone::client::RefResponse {
    sync_until_manifest(server, owner, repo).await
}

/// Wait until an already-triggered build has published cloneable artifacts.
/// This deliberately clones bytes as the probe, so a stale/ref-only success
/// cannot pass.
pub async fn wait_repo_cloneable(
    server: &Server,
    owner: &str,
    repo: &str,
    want_count: &str,
) -> (TempDir, PathBuf) {
    clone_full_at(server, owner, repo, want_count).await
}

/// True when `dir` contains at least one regular file (recursively) — used to
/// tell a materialized files-mode worktree from an empty/not-yet-built one.
fn dir_has_file(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        match std::fs::symlink_metadata(&path) {
            Ok(m) if m.is_dir() => {
                if dir_has_file(&path) {
                    return true;
                }
            }
            Ok(_) => return true,
            Err(_) => {}
        }
    }
    false
}

// ---- standalone worker process -------------------------------------------

/// A spawned `ripclone-worker` binary, killed when dropped.
pub struct WorkerProc(Child);

impl WorkerProc {
    /// Kill the worker process and wait for it to exit. Tests use this to model
    /// SIGKILL-style loss of the build owner while the SQL queue row remains
    /// claimed.
    pub fn kill_and_wait(mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }

    /// Hard-kill the worker now and wait for the OS process to exit.
    pub fn kill_now(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }

    /// Poll until the worker exits on its own (idle-exit / max-jobs) or `timeout`
    /// elapses. Returns `true` if the process exited.
    pub fn wait_exit(&mut self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            match self.0.try_wait() {
                Ok(Some(_)) => return true,
                Ok(None) if start.elapsed() >= timeout => return false,
                Ok(None) => std::thread::sleep(Duration::from_millis(50)),
                Err(_) => return false,
            }
        }
    }

    /// Whether the OS process has already exited (non-blocking).
    pub fn has_exited(&mut self) -> bool {
        matches!(self.0.try_wait(), Ok(Some(_)))
    }
}

impl Drop for WorkerProc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn the real `ripclone-worker` binary sharing `cas_dir` + `repo_root` with
/// the in-process server. It inherits the test process env (RIPCLONE_QUEUE,
/// RIPCLONE_QUEUE_DB_URL, RIPCLONE_ORIGIN_BASE, RIPCLONE_SERVER_TOKEN, …) but
/// **clears** lifecycle vars (`RIPCLONE_IDLE_EXIT_SECS`, `RIPCLONE_MAX_JOBS`) so
/// a developer shell or a prior test cannot force scale-to-zero on a forever
/// worker under test.
pub fn spawn_worker(cas_dir: &Path, repo_root: &Path) -> WorkerProc {
    spawn_worker_with(cas_dir, repo_root, &[], &[])
}

/// Like [`spawn_worker`], with extra CLI args (e.g. `--idle-exit-secs`, `--max-jobs`).
pub fn spawn_worker_args(cas_dir: &Path, repo_root: &Path, extra: &[&str]) -> WorkerProc {
    spawn_worker_with(cas_dir, repo_root, extra, &[])
}

/// Spawn the worker with CLI args and/or child-only env (for the env-bag path:
/// compute providers set lifecycle via env without flags). Lifecycle env from
/// the parent process is cleared first, then `extra_env` is applied, so tests
/// don't leak `RIPCLONE_MAX_JOBS` into forever workers.
/// Path to a cargo-built binary. Prefers the runtime env var (set by `cargo
/// test`, or by CI when running a prebuilt test binary against downloaded
/// bins) over the compile-time `env!("CARGO_BIN_EXE_*")` path, which is baked
/// to the *build* machine and breaks after artifact download.
pub fn cargo_bin(name: &str) -> std::path::PathBuf {
    let key = format!("CARGO_BIN_EXE_{name}");
    if let Ok(p) = std::env::var(&key) {
        return std::path::PathBuf::from(p);
    }
    // Only the names we actually spawn from this helper are listed; expand if
    // a new binary is needed at runtime from prebuilt e2e.
    match name {
        "ripclone-worker" => std::path::PathBuf::from(env!("CARGO_BIN_EXE_ripclone-worker")),
        "ripclone" => std::path::PathBuf::from(env!("CARGO_BIN_EXE_ripclone")),
        "ripclone-server" => std::path::PathBuf::from(env!("CARGO_BIN_EXE_ripclone-server")),
        "ripclone-dispatcher" => {
            std::path::PathBuf::from(env!("CARGO_BIN_EXE_ripclone-dispatcher"))
        }
        other => panic!("unknown cargo bin {other}; set CARGO_BIN_EXE_{other}"),
    }
}

pub fn spawn_worker_with(
    cas_dir: &Path,
    repo_root: &Path,
    extra_args: &[&str],
    extra_env: &[(&str, &str)],
) -> WorkerProc {
    let mut cmd = Command::new(cargo_bin("ripclone-worker"));
    cmd.arg("--cas-dir")
        .arg(cas_dir)
        .arg("--repo-root")
        .arg(repo_root)
        .arg("--idle-poll-ms")
        .arg("100")
        .args(extra_args)
        // Lifecycle is flag-or-env; pin the child to an explicit bag so parent
        // process pollution can't change forever vs one-shot vs idle-exit.
        .env_remove("RIPCLONE_IDLE_EXIT_SECS")
        .env_remove("RIPCLONE_MAX_JOBS")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let child = cmd.spawn().expect("spawn ripclone-worker binary");
    WorkerProc(child)
}
