//! The ripclone server: app state, router assembly and startup.

use crate::auth::broker::{CredentialBroker, broker_from_env};
use crate::backends;
use crate::cas::Cas;
use crate::control::ControlStore;
use crate::metrics::Metrics;
use crate::provider::ProviderRegistry;
use crate::storage::StorageRef;
use crate::webhook::WebhookConfig;
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

mod api;
mod artifacts;
mod auth;
mod builder;
mod poller;
mod uploads;
mod webhooks;
mod worker;

pub use api::{
    BuildRequest, ExactStatusEntry, RefResponse, RegionStorageEntry, RepoStatusResponse,
};
pub use auth::RateLimiter;
pub use builder::SyncPhases;
pub use poller::poll_once;

use api::{
    admin_get_config, admin_put_config, dispatch_repos_delete, dispatch_repos_get,
    dispatch_repos_post, list_added_repos,
};
use artifacts::get_artifact;
use auth::{
    auth_middleware, parse_rate_limit_settings, protocol_guard, rate_limit_middleware,
    read_server_auth_token,
};
use poller::spawn_poll_loop;
use webhooks::webhook_handler;

#[derive(Clone)]
pub struct ServerState {
    pub cas: Cas,
    pub storage: StorageRef,
    pub repo_root: PathBuf,
    /// Builds, added repositories and repository config. Also holds the
    /// server's ownership lock on the control database.
    pub control: Arc<dyn ControlStore>,
    pub provider_registry: ProviderRegistry,
    pub broker: Arc<dyn CredentialBroker>,
    pub token_hash: Option<String>,
    pub metrics: Arc<Metrics>,
    pub rate_limiter: RateLimiter,
    /// Wakes a Head slot after an admission that left Head pending.
    pub head_wake: Arc<tokio::sync::Notify>,
    /// Wakes a Files slot after a Head build releases its row.
    pub files_wake: Arc<tokio::sync::Notify>,
    /// Wakes a Full slot after a Files build releases its row.
    pub full_wake: Arc<tokio::sync::Notify>,
    /// Webhook receiver config: per-provider HMAC secret + optional repo
    /// allowlist. A provider with no configured secret returns 503. Reads
    /// `RIPCLONE_WEBHOOK_SECRET_<provider>`.
    pub webhook_config: Arc<WebhookConfig>,
    /// Per-repo mirror locks, so concurrent builds of one repo cannot
    /// corrupt or lose its bare mirror.
    pub sync_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<RepoLock>>>>,
    /// Cached `/readyz` result `(checked_at, ready)`. Bounds backend probe cost
    /// (S3 round-trips) and damps load-balancer flapping on a transient blip.
    pub readyz_cache: Arc<std::sync::Mutex<Option<(Instant, bool)>>>,
    /// Lifetime of the signed artifact URLs the server hands out
    /// (`RIPCLONE_SIGNED_URL_TTL_SECS`).
    pub signed_url_ttl: Duration,
}

/// The locks of one repo's bare mirror.
///
/// Fetches and the commit-graph only add files or atomically replace them,
/// so builds read the mirror while they run; `write` serializes them.
/// Compaction deletes packs, so it also needs `read`
/// exclusively, which it only tries for: it never waits for a build, and no
/// build waits long for it. Order: `read`, then `write`.
#[derive(Default)]
pub struct RepoLock {
    pub write: tokio::sync::Mutex<()>,
    /// Shared by every build while it reads the mirror.
    pub read: tokio::sync::RwLock<()>,
    /// When compaction last failed, so a failing one is not retried by every
    /// Full.
    pub compact_failed: std::sync::Mutex<Option<Instant>>,
}

impl ServerState {
    /// The broker's credential for `repo_id`, read on a blocking thread: a
    /// broker cache miss makes a blocking HTTP call.
    pub async fn fetch_credential(
        &self,
        repo_id: &crate::provider::RepoId,
    ) -> Result<Option<secrecy::SecretString>> {
        let (broker, repo_id) = (self.broker.clone(), repo_id.clone());
        tokio::task::spawn_blocking(move || broker.fetch_credential(&repo_id))
            .await
            .context("credential task")?
    }

    /// The wake-up for `pool`'s idle slots.
    pub fn wake(&self, pool: crate::control::Pool) -> &Arc<tokio::sync::Notify> {
        match pool {
            crate::control::Pool::Head => &self.head_wake,
            crate::control::Pool::Files => &self.files_wake,
            crate::control::Pool::Full => &self.full_wake,
        }
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub fn build_app(state: ServerState) -> Router {
    let protected = Router::new()
        .route("/v1/repos", get(list_added_repos))
        // Single catch-all route for all provider-qualified repo endpoints.
        .route("/v1/repos/{*path}", get(dispatch_repos_get))
        .route("/v1/repos/{*path}", post(dispatch_repos_post))
        .route("/v1/repos/{*path}", delete(dispatch_repos_delete))
        .route("/v1/artifacts/{hash}", get(get_artifact))
        // Repository build settings, addressed like `/v1/repos`.
        .route(
            "/v1/admin/config/{*path}",
            get(admin_get_config).post(admin_put_config),
        )
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

/// Maximum request body size accepted by the server.
const MAX_REQUEST_BODY_BYTES: usize = 256 * 1024 * 1024;

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

/// Readiness probe: 200 only when storage and the control store are reachable,
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

    if let Err(e) = state.control.health().await {
        problems.push(format!("control: {e:#}"));
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

/// Process-global cap on concurrent `ls-remote` resolves (API admissions and
/// the poller). Each is a short, bounded child; the cap only stops a burst of
/// requests from starting hundreds at once. Mirror fetches are not counted
/// here: they run inside build slots, which bound them, so a slow fetch never
/// delays an `/add` or `POST /builds`.
fn ls_remote_semaphore() -> &'static tokio::sync::Semaphore {
    static SEM: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    SEM.get_or_init(|| tokio::sync::Semaphore::new(16))
}

/// Put every temporary file and directory this process makes in a fresh
/// directory under `root`, and remove what earlier processes left there.
/// Builds stage packs and archives in temp directories; a killed or crashed
/// server cannot remove them, and for a large repo each is gigabytes. Under
/// the cache directory they are cleared on the next start instead of filling
/// the system temp directory. A killed server's git children can still be
/// writing there, so a leftover that won't go is logged and left for the next
/// start; it never stops this one. Call only after this process owns the
/// control database, so no other server is using `root`.
fn use_scratch_dir(root: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(root)
        .with_context(|| format!("create scratch dir {}", root.display()))?;
    let entries =
        std::fs::read_dir(root).with_context(|| format!("list scratch dir {}", root.display()))?;
    for entry in entries {
        let path = entry
            .with_context(|| format!("list scratch dir {}", root.display()))?
            .path();
        let removed = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        match removed {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => warn!(
                "left stale scratch {} for the next start: {error}",
                path.display()
            ),
        }
    }
    let dir = tempfile::Builder::new()
        .prefix("run-")
        .tempdir_in(root)
        .with_context(|| format!("create scratch dir in {}", root.display()))?
        .keep();
    if let Err(current) = tempfile::env::override_temp_dir(&dir) {
        anyhow::bail!("temp dir already set to {}", current.display());
    }
    Ok(())
}

async fn serve(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    explicit_control_path: Option<&std::path::Path>,
    host: &str,
    port: u16,
) -> Result<()> {
    let default_control_path = repo_root.parent().unwrap_or(repo_root).join("control.db");
    let control_settings = crate::control::ControlSettings::from_sources(
        explicit_control_path,
        &default_control_path,
    )?;

    let token_hash = read_server_auth_token()?;
    info!("server auth token configured; auth middleware enabled");

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

    backends::Backends::validate_environment()?;
    let worker_settings = worker::WorkerSettings::from_env()?;
    let retention = crate::retention::RetentionSettings::from_env()?;
    // Polling fallback: catches pushes that arrived without a webhook/Actions
    // trigger so build-before-clone still holds. Defaults to 5 minutes so
    // webhook-less self-hosts still self-heal missed or stuck builds.
    let poll_interval =
        Duration::from_secs(crate::env::number("RIPCLONE_POLL_INTERVAL_SECS", 300)?);
    let signed_url_ttl = api::signed_url_ttl_from_env()?;
    // Read where they are used; checked here so a bad value stops startup.
    crate::git::ls_remote_timeout()?;
    crate::git::fetch_timeout()?;
    builder::check_environment()?;

    let metrics = Metrics::new();
    let control: Arc<dyn ControlStore> = Arc::new(
        crate::control::SqliteControlStore::open(
            &control_settings.path,
            worker_settings.max_attempts,
            Arc::new(crate::control::SystemClock),
        )
        .await?,
    );
    // This process holds the control database's ownership lock, so no other
    // worker holds a lease: every lease is from a previous process.
    let reset = control.reset_leases().await?;
    info!(
        path = %control_settings.path.display(),
        reset_leases = reset,
        "server owns control database"
    );
    std::fs::create_dir_all(cas_dir)?;
    std::fs::create_dir_all(repo_root)?;
    use_scratch_dir(&cas_dir.join("tmp"))?;
    let b = backends::Backends::from_env(cas_dir, &metrics, &retention)?;
    b.cache_retention.clone().spawn(retention.interval);

    // Webhook receiver config: per-provider secret + optional allowlist (built
    // before the registry is moved into the state). A push to a configured
    // webhook triggers a build before any clone — no per-repo Actions workflow.
    let webhook_config = Arc::new(WebhookConfig::from_env(&provider_registry));

    let state = ServerState {
        cas: b.cas,
        storage: b.storage,
        repo_root: repo_root.to_path_buf(),
        control,
        head_wake: Arc::new(tokio::sync::Notify::new()),
        files_wake: Arc::new(tokio::sync::Notify::new()),
        full_wake: Arc::new(tokio::sync::Notify::new()),
        provider_registry,
        broker,
        token_hash: Some(token_hash),
        metrics,
        rate_limiter,
        webhook_config,
        sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        readyz_cache: Arc::new(std::sync::Mutex::new(None)),
        signed_url_ttl,
    };

    worker::spawn_workers(&state, &worker_settings);

    spawn_poll_loop(state.clone(), poll_interval);

    let app = build_app(state);

    // Bind only after config and control ownership are checked, so a rejected
    // server never takes the port.
    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    // Log the bound address, not the requested one: with `--port 0` this line
    // is how a caller learns the port.
    info!(
        "ripclone server listening on http://{}",
        listener.local_addr()?
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

pub async fn run_server(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    host: &str,
    port: u16,
) -> Result<()> {
    serve(cas_dir, repo_root, None, host, port).await
}

pub async fn run_server_with_control(
    cas_dir: &std::path::Path,
    repo_root: &std::path::Path,
    control_path: &std::path::Path,
    host: &str,
    port: u16,
) -> Result<()> {
    serve(cas_dir, repo_root, Some(control_path), host, port).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::RepoId;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use sha2::{Digest, Sha256};
    use tower::util::ServiceExt;

    /// A `ServerState` on a real control store in `tmp`, with no workers.
    pub(super) async fn test_state(tmp: &tempfile::TempDir) -> ServerState {
        let cas_root = tmp.path().join("cas");
        let cas = Cas::new(&cas_root).unwrap();
        let storage = crate::storage::local(&cas_root).unwrap();
        let repo_root = tmp.path().join("repos");
        std::fs::create_dir_all(&repo_root).unwrap();
        let control: Arc<dyn ControlStore> = Arc::new(
            crate::control::SqliteControlStore::open(
                &tmp.path().join("control.db"),
                5,
                Arc::new(crate::control::SystemClock),
            )
            .await
            .unwrap(),
        );
        let provider_registry = ProviderRegistry::new();
        let broker: Arc<dyn CredentialBroker> = Arc::new(crate::auth::broker::StaticBroker::new(
            provider_registry.clone(),
        ));
        ServerState {
            cas,
            storage,
            repo_root,
            control,
            head_wake: Arc::new(tokio::sync::Notify::new()),
            files_wake: Arc::new(tokio::sync::Notify::new()),
            full_wake: Arc::new(tokio::sync::Notify::new()),
            provider_registry,
            broker,
            token_hash: Some(hex::encode(Sha256::digest("secret"))),
            metrics: Metrics::new(),
            rate_limiter: RateLimiter::new(100, 100.0),
            // No webhook secret here; webhook tests install their own.
            webhook_config: Arc::new(WebhookConfig::default()),
            sync_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            readyz_cache: Arc::new(std::sync::Mutex::new(None)),
            signed_url_ttl: api::DEFAULT_SIGNED_URL_TTL,
        }
    }

    pub(super) fn auth_header() -> String {
        format!("Ripclone {}", hex::encode(Sha256::digest("secret")))
    }

    pub(super) fn test_request(method: &str, uri: &str) -> axum::http::Request<Body> {
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
    pub(super) fn request_with_auth(
        method: &str,
        uri: &str,
        auth: Option<&str>,
    ) -> axum::http::Request<Body> {
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
    async fn public_endpoints_require_no_auth() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
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

    pub(super) async fn mark_added(state: &ServerState, repo_id: RepoId) {
        state.control.add_repo(&repo_id).await.unwrap();
    }

    #[tokio::test]
    async fn readyz_ready_when_healthy() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_not_ready_when_storage_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
        // Simulate the data volume being unmounted/removed under the server.
        std::fs::remove_dir_all(tmp.path().join("cas")).unwrap();
        let app = build_app(state);
        let response = app.oneshot(test_request("GET", "/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_endpoint_is_prometheus_text() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(&tmp).await;
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
        let state = test_state(&tmp).await;
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
}
