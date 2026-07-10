//! Fly Machines impl of [`ComputeProvider`].
//!
//! Pooling is internal: list machines of the right size class, `POST …/start`
//! on a stopped (or suspended) one when any remain. Each call may start one
//! additional machine so depth-based autoscale can grow past a single live
//! peer. When the pool has no startable machine left, an already-live peer is
//! an idempotent `Ok` no-op (not an error).
//!
//! Pre-provisioned machines carry the env bag via Fly secrets / machine config.
//! `WorkerSpec.env` is accepted for interface parity; per-job env injection is
//! a later concern (ApiRefStore tokens).

use super::{ComputeProvider, WorkerSpec, validate_dispatch_url};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use tracing::{error, info};

const DEFAULT_API_BASE: &str = "https://api.machines.dev";
const SIZE_CLASS_KEY: &str = "ripclone_size_class";
const PROCESS_GROUP_KEY: &str = "fly_process_group";

/// Machine is already up or coming up — `ensure_worker` is a no-op.
fn live_states() -> &'static HashSet<&'static str> {
    static S: OnceLock<HashSet<&'static str>> = OnceLock::new();
    S.get_or_init(|| HashSet::from(["started", "starting", "created", "replacing"]))
}

/// Machine can be woken with `POST …/start`.
fn startable_states() -> &'static HashSet<&'static str> {
    static S: OnceLock<HashSet<&'static str>> = OnceLock::new();
    S.get_or_init(|| HashSet::from(["stopped", "suspended"]))
}

/// Subset of the Fly Machines list response we care about.
#[derive(Debug, Clone, Deserialize)]
pub struct FlyMachine {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub state: String,
    #[serde(default)]
    pub config: Option<FlyMachineConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FlyMachineConfig {
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

/// Injectable Fly Machines API surface (tests mock this without HTTP).
#[async_trait]
pub trait FlyMachinesClient: Send + Sync {
    async fn list_machines(&self, app: &str) -> Result<Vec<FlyMachine>>;
    async fn start_machine(&self, app: &str, id: &str) -> Result<()>;
}

/// Real HTTP client against the Fly Machines API.
pub struct HttpFlyMachinesClient {
    token: String,
    api_base: String,
    http: reqwest::Client,
}

impl HttpFlyMachinesClient {
    /// Build a client. `api_base` defaults to the Fly Machines API; when set it
    /// must pass the same http(s) / SSRF URL guard as the http dispatch backend.
    pub fn new(token: impl Into<String>, api_base: Option<String>) -> Result<Self> {
        let api_base = api_base
            .unwrap_or_else(|| DEFAULT_API_BASE.to_string())
            .trim_end_matches('/')
            .to_string();
        validate_dispatch_url(&api_base)
            .with_context(|| format!("invalid Fly API base '{api_base}'"))?;
        Ok(Self {
            token: token.into(),
            api_base,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .expect("reqwest client"),
        })
    }
}

#[async_trait]
impl FlyMachinesClient for HttpFlyMachinesClient {
    async fn list_machines(&self, app: &str) -> Result<Vec<FlyMachine>> {
        let url = format!(
            "{}/v1/apps/{}/machines",
            self.api_base,
            urlencoding::encode(app)
        );
        let res = self
            .http
            .get(&url)
            .header("authorization", format!("Bearer {}", self.token))
            .header("content-type", "application/json")
            .send()
            .await
            .context("Fly list_machines request")?;
        let status = res.status();
        if !status.is_success() {
            let body = res.text().await.unwrap_or_default();
            bail!(
                "Fly list_machines failed: {status}{}",
                if body.is_empty() {
                    String::new()
                } else {
                    format!(" — {body}")
                }
            );
        }
        res.json::<Vec<FlyMachine>>()
            .await
            .context("decode Fly list_machines response")
    }

    async fn start_machine(&self, app: &str, id: &str) -> Result<()> {
        let url = format!(
            "{}/v1/apps/{}/machines/{}/start",
            self.api_base,
            urlencoding::encode(app),
            urlencoding::encode(id)
        );
        let res = self
            .http
            .post(&url)
            .header("authorization", format!("Bearer {}", self.token))
            .header("content-type", "application/json")
            .send()
            .await
            .context("Fly start_machine request")?;
        let status = res.status();
        if !status.is_success() {
            let body = res.text().await.unwrap_or_default();
            bail!(
                "Fly start_machine({id}) failed: {status}{}",
                if body.is_empty() {
                    String::new()
                } else {
                    format!(" — {body}")
                }
            );
        }
        Ok(())
    }
}

/// Construction options for [`FlyProvider`].
pub struct FlyProviderConfig {
    pub app: String,
    pub token: String,
    /// Default: `https://api.machines.dev`.
    pub api_base: Option<String>,
    /// Injected client (tests). When `None`, a real HTTP client is built.
    pub client: Option<Arc<dyn FlyMachinesClient>>,
    /// Metadata key matching `WorkerSpec.size_class`. Default: `ripclone_size_class`.
    pub size_class_metadata_key: Option<String>,
    /// Only machines with `fly_process_group` equal to this value are candidates.
    /// Default: `"worker"`. Empty string disables the filter.
    pub process_group: Option<String>,
}

/// Fly impl of [`ComputeProvider`]: start a pre-provisioned stopped machine.
pub struct FlyProvider {
    app: String,
    client: Arc<dyn FlyMachinesClient>,
    size_class_key: String,
    process_group: Option<String>,
}

impl FlyProvider {
    pub fn new(cfg: FlyProviderConfig) -> Result<Self> {
        let client = match cfg.client {
            Some(c) => c,
            None => Arc::new(HttpFlyMachinesClient::new(cfg.token, cfg.api_base)?)
                as Arc<dyn FlyMachinesClient>,
        };
        let process_group = match cfg.process_group {
            Some(s) if s.is_empty() => None,
            Some(s) => Some(s),
            None => Some("worker".to_string()),
        };
        if cfg.app.trim().is_empty() {
            bail!("Fly worker app name must not be empty");
        }
        Ok(Self {
            app: cfg.app,
            client,
            size_class_key: cfg
                .size_class_metadata_key
                .unwrap_or_else(|| SIZE_CLASS_KEY.to_string()),
            process_group,
        })
    }

    /// Build from env: `FLY_WORKER_APP`, `FLY_API_TOKEN`, optional `FLY_API_HOSTNAME`.
    pub fn from_env() -> Result<Self> {
        let app = std::env::var("FLY_WORKER_APP")
            .context("FLY_WORKER_APP is required for RIPCLONE_DISPATCH=fly")?;
        let token = std::env::var("FLY_API_TOKEN")
            .context("FLY_API_TOKEN is required for RIPCLONE_DISPATCH=fly")?;
        let api_base = std::env::var("FLY_API_HOSTNAME")
            .ok()
            .filter(|s| !s.is_empty());
        Self::new(FlyProviderConfig {
            app,
            token,
            api_base,
            client: None,
            size_class_metadata_key: None,
            process_group: None,
        })
    }

    fn is_pool_candidate(&self, m: &FlyMachine, size_class: &str) -> bool {
        let meta = m
            .config
            .as_ref()
            .map(|c| &c.metadata)
            .cloned()
            .unwrap_or_default();

        if let Some(ref wanted_pg) = self.process_group {
            // When the app uses process groups, only wake workers. Unlabeled
            // machines (single-role apps) still qualify.
            if let Some(pg) = meta.get(PROCESS_GROUP_KEY) {
                if pg != wanted_pg {
                    return false;
                }
            }
        }

        // Unlabeled pool = single-lane: any worker matches any size_class.
        // Labeled machines match only their lane.
        if let Some(labeled) = meta.get(&self.size_class_key) {
            if labeled != size_class {
                return false;
            }
        }
        true
    }
}

#[async_trait]
impl ComputeProvider for FlyProvider {
    fn name(&self) -> &str {
        "fly"
    }

    async fn ensure_worker(&self, spec: &WorkerSpec) -> Result<()> {
        spec.validate()?;
        let machines = self.client.list_machines(&self.app).await?;
        let pool: Vec<&FlyMachine> = machines
            .iter()
            .filter(|m| self.is_pool_candidate(m, &spec.size_class))
            .collect();

        // Prefer scale-out: start one stopped machine when the pool has one.
        // Depth-based autoscale calls ensure_worker N times for N slots; if we
        // no-op whenever *any* peer is live, the fleet can never grow past 1.
        let startable: Vec<&&FlyMachine> = pool
            .iter()
            .filter(|m| startable_states().contains(m.state.as_str()))
            .collect();
        if let Some(target) = startable.first() {
            info!(
                size_class = %spec.size_class,
                machine_id = %target.id,
                state = %target.state,
                live_peers = pool
                    .iter()
                    .filter(|m| live_states().contains(m.state.as_str()))
                    .count(),
                "fly.ensure_worker starting stopped machine"
            );
            return self.client.start_machine(&self.app, &target.id).await;
        }

        let live: Vec<&&FlyMachine> = pool
            .iter()
            .filter(|m| live_states().contains(m.state.as_str()))
            .collect();
        if !live.is_empty() {
            // Pool exhausted but something is already up — idempotent success.
            info!(
                size_class = %spec.size_class,
                live = ?live.iter().map(|m| (&m.id, &m.state)).collect::<Vec<_>>(),
                "fly.ensure_worker no-op: no startable capacity, peers live"
            );
            return Ok(());
        }

        // Loud fail: no capacity to wake. Job stays queued; reconcile retries.
        error!(
            size_class = %spec.size_class,
            pool_size = pool.len(),
            states = ?pool.iter().map(|m| m.state.as_str()).collect::<Vec<_>>(),
            "fly.ensure_worker: no startable machine in pool"
        );
        bail!(
            "FlyProvider: no stopped/suspended machine for size_class={}",
            spec.size_class
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    fn machine(
        id: &str,
        state: &str,
        size: Option<&str>,
        process_group: Option<&str>,
    ) -> FlyMachine {
        let mut metadata = HashMap::new();
        if let Some(pg) = process_group {
            metadata.insert(PROCESS_GROUP_KEY.into(), pg.into());
        } else {
            metadata.insert(PROCESS_GROUP_KEY.into(), "worker".into());
        }
        if let Some(sc) = size {
            metadata.insert(SIZE_CLASS_KEY.into(), sc.into());
        } else {
            metadata.insert(SIZE_CLASS_KEY.into(), "small".into());
        }
        FlyMachine {
            id: id.into(),
            name: Some(id.into()),
            state: state.into(),
            config: Some(FlyMachineConfig {
                metadata,
                env: HashMap::new(),
            }),
        }
    }

    struct MockFlyClient {
        machines: Mutex<Vec<FlyMachine>>,
        starts: Mutex<Vec<String>>,
        list_calls: Mutex<usize>,
        list_err: Option<String>,
    }

    impl MockFlyClient {
        fn new(machines: Vec<FlyMachine>) -> Self {
            Self {
                machines: Mutex::new(machines),
                starts: Mutex::new(Vec::new()),
                list_calls: Mutex::new(0),
                list_err: None,
            }
        }

        fn with_list_err(msg: &str) -> Self {
            Self {
                machines: Mutex::new(Vec::new()),
                starts: Mutex::new(Vec::new()),
                list_calls: Mutex::new(0),
                list_err: Some(msg.into()),
            }
        }

        fn starts(&self) -> Vec<String> {
            self.starts.lock().unwrap().clone()
        }

        fn list_calls(&self) -> usize {
            *self.list_calls.lock().unwrap()
        }
    }

    #[async_trait]
    impl FlyMachinesClient for MockFlyClient {
        async fn list_machines(&self, _app: &str) -> Result<Vec<FlyMachine>> {
            *self.list_calls.lock().unwrap() += 1;
            if let Some(ref e) = self.list_err {
                bail!("{e}");
            }
            Ok(self.machines.lock().unwrap().clone())
        }

        async fn start_machine(&self, _app: &str, id: &str) -> Result<()> {
            self.starts.lock().unwrap().push(id.to_string());
            Ok(())
        }
    }

    fn provider(client: Arc<MockFlyClient>) -> FlyProvider {
        FlyProvider::new(FlyProviderConfig {
            app: "ripclone-workers".into(),
            token: "test-token".into(),
            api_base: None,
            client: Some(client),
            size_class_metadata_key: None,
            process_group: None,
        })
        .expect("fly provider")
    }

    fn spec() -> WorkerSpec {
        let mut env = BTreeMap::new();
        env.insert("RIPCLONE_QUEUE".into(), "libsql".into());
        WorkerSpec::new("small", env)
    }

    #[tokio::test]
    async fn issues_start_stopped_for_matching_machine() {
        let client = Arc::new(MockFlyClient::new(vec![
            machine("m-stopped", "stopped", Some("small"), Some("worker")),
            machine("m-server", "started", None, Some("server")),
        ]));
        let fly = provider(client.clone());
        fly.ensure_worker(&spec()).await.unwrap();
        assert_eq!(client.starts(), vec!["m-stopped".to_string()]);
        assert_eq!(client.list_calls(), 1);
    }

    #[tokio::test]
    async fn scale_out_starts_stopped_even_when_peer_live() {
        // Depth-based autoscale needs N starts; a live peer must not block
        // waking another stopped machine in the pool.
        for state in ["starting", "started", "created", "replacing"] {
            let client = Arc::new(MockFlyClient::new(vec![
                machine("m-live", state, Some("small"), Some("worker")),
                machine("m-stopped", "stopped", Some("small"), Some("worker")),
            ]));
            let fly = provider(client.clone());
            fly.ensure_worker(&spec()).await.unwrap();
            assert_eq!(
                client.starts(),
                vec!["m-stopped".to_string()],
                "expected scale-out start while peer state={state}"
            );
        }
    }

    #[tokio::test]
    async fn pool_exhausted_with_live_peer_is_idempotent_ok() {
        let client = Arc::new(MockFlyClient::new(vec![machine(
            "m-live",
            "started",
            Some("small"),
            Some("worker"),
        )]));
        let fly = provider(client.clone());
        fly.ensure_worker(&spec()).await.unwrap();
        assert!(
            client.starts().is_empty(),
            "no startable capacity → no-op Ok, not Err"
        );
    }

    #[tokio::test]
    async fn repeated_ensure_starts_multiple_stopped_machines() {
        let client = Arc::new(MockFlyClient::new(vec![
            machine("m1", "stopped", Some("small"), Some("worker")),
            machine("m2", "stopped", Some("small"), Some("worker")),
            machine("m3", "stopped", Some("small"), Some("worker")),
        ]));
        // Mock does not flip state on start — each call re-picks the first
        // stopped machine. Simulate sequential starts by updating state.
        let fly = provider(client.clone());
        for _ in 0..3 {
            let machines = client.machines.lock().unwrap().clone();
            let next = machines
                .iter()
                .find(|m| m.state == "stopped")
                .map(|m| m.id.clone());
            fly.ensure_worker(&spec()).await.unwrap();
            if let Some(id) = next {
                let mut machines = client.machines.lock().unwrap();
                if let Some(m) = machines.iter_mut().find(|m| m.id == id) {
                    m.state = "starting".into();
                }
            }
        }
        assert_eq!(client.starts().len(), 3, "three ensure → three starts");
    }

    #[tokio::test]
    async fn starts_suspended_when_none_live() {
        let client = Arc::new(MockFlyClient::new(vec![machine(
            "m-susp",
            "suspended",
            Some("small"),
            Some("worker"),
        )]));
        let fly = provider(client.clone());
        fly.ensure_worker(&spec()).await.unwrap();
        assert_eq!(client.starts(), vec!["m-susp".to_string()]);
    }

    #[tokio::test]
    async fn matches_size_class_via_metadata() {
        let client = Arc::new(MockFlyClient::new(vec![
            machine("m-large", "stopped", Some("large"), Some("worker")),
            machine("m-small", "stopped", Some("small"), Some("worker")),
        ]));
        let fly = provider(client.clone());
        fly.ensure_worker(&WorkerSpec::new("small", BTreeMap::new()))
            .await
            .unwrap();
        assert_eq!(client.starts(), vec!["m-small".to_string()]);
    }

    #[tokio::test]
    async fn throws_when_no_startable_machine() {
        let client = Arc::new(MockFlyClient::new(vec![machine(
            "m-stopping",
            "stopping",
            Some("small"),
            Some("worker"),
        )]));
        let fly = provider(client.clone());
        let err = fly.ensure_worker(&spec()).await.unwrap_err();
        assert!(
            err.to_string().contains("no stopped/suspended machine"),
            "got: {err}"
        );
        assert!(client.starts().is_empty());
    }

    #[tokio::test]
    async fn propagates_list_failures() {
        let client = Arc::new(MockFlyClient::with_list_err(
            "Fly list_machines failed: 401 Unauthorized",
        ));
        let fly = provider(client);
        let err = fly.ensure_worker(&spec()).await.unwrap_err();
        assert!(err.to_string().contains("401"), "got: {err}");
    }

    /// End-to-end against a mock Fly Machines HTTP API (reqwest path).
    #[tokio::test]
    async fn http_client_posts_start_on_selected_machine() {
        use axum::extract::Path;
        use axum::http::HeaderMap;
        use axum::routing::{get, post};
        use axum::{Json, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let starts = Arc::new(Mutex::new(Vec::<String>::new()));
        let starts_h = starts.clone();
        let list_hits = Arc::new(AtomicUsize::new(0));
        let list_hits_h = list_hits.clone();
        let auth_seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let auth_list = auth_seen.clone();
        let auth_start = auth_seen.clone();

        let app = Router::new()
            .route(
                "/v1/apps/{app}/machines",
                get(move |Path(app): Path<String>, headers: HeaderMap| {
                    let list_hits_h = list_hits_h.clone();
                    let auth_list = auth_list.clone();
                    async move {
                        assert_eq!(app, "my-workers");
                        if let Some(a) = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                        {
                            auth_list.lock().unwrap().push(a.to_string());
                        }
                        list_hits_h.fetch_add(1, Ordering::SeqCst);
                        Json(vec![serde_json::json!({
                            "id": "e21781960b2896",
                            "state": "stopped",
                            "config": {
                                "metadata": {
                                    "fly_process_group": "worker",
                                    "ripclone_size_class": "small",
                                }
                            }
                        })])
                    }
                }),
            )
            .route(
                "/v1/apps/{app}/machines/{id}/start",
                post(
                    move |Path((app, id)): Path<(String, String)>, headers: HeaderMap| {
                        let starts_h = starts_h.clone();
                        let auth_start = auth_start.clone();
                        async move {
                            assert_eq!(app, "my-workers");
                            if let Some(a) = headers
                                .get(axum::http::header::AUTHORIZATION)
                                .and_then(|v| v.to_str().ok())
                            {
                                auth_start.lock().unwrap().push(a.to_string());
                            }
                            starts_h.lock().unwrap().push(id);
                            Json(serde_json::json!({ "ok": true }))
                        }
                    },
                ),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let api_base = format!("http://{addr}");
        let client: Arc<dyn FlyMachinesClient> = Arc::new(
            HttpFlyMachinesClient::new("fly_token_xyz", Some(api_base)).expect("fly http client"),
        );
        let fly = FlyProvider::new(FlyProviderConfig {
            app: "my-workers".into(),
            token: "fly_token_xyz".into(),
            api_base: None,
            client: Some(client),
            size_class_metadata_key: None,
            process_group: None,
        })
        .expect("fly provider");
        fly.ensure_worker(&spec()).await.unwrap();

        assert_eq!(list_hits.load(Ordering::SeqCst), 1);
        assert_eq!(*starts.lock().unwrap(), vec!["e21781960b2896".to_string()]);
        let auths = auth_seen.lock().unwrap().clone();
        assert_eq!(
            auths,
            vec![
                "Bearer fly_token_xyz".to_string(),
                "Bearer fly_token_xyz".to_string()
            ],
            "list + start must both send the Fly API token"
        );
    }

    #[tokio::test]
    async fn empty_size_class_rejected_before_api() {
        let client = Arc::new(MockFlyClient::new(vec![machine(
            "m",
            "stopped",
            Some("small"),
            Some("worker"),
        )]));
        let fly = provider(client.clone());
        let err = fly
            .ensure_worker(&WorkerSpec::new("", BTreeMap::new()))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("size_class must not be empty"),
            "got: {err}"
        );
        assert_eq!(
            client.list_calls(),
            0,
            "must not call Fly with empty size_class"
        );
    }

    #[test]
    fn from_env_requires_app_and_token() {
        let saved_app = std::env::var("FLY_WORKER_APP").ok();
        let saved_tok = std::env::var("FLY_API_TOKEN").ok();
        unsafe {
            std::env::remove_var("FLY_WORKER_APP");
            std::env::remove_var("FLY_API_TOKEN");
        }
        let err = match FlyProvider::from_env() {
            Err(e) => e,
            Ok(_) => panic!("expected missing FLY_WORKER_APP to fail"),
        };
        assert!(err.to_string().contains("FLY_WORKER_APP"), "got: {err}");
        unsafe {
            std::env::set_var("FLY_WORKER_APP", "workers");
            std::env::remove_var("FLY_API_TOKEN");
        }
        let err = match FlyProvider::from_env() {
            Err(e) => e,
            Ok(_) => panic!("expected missing FLY_API_TOKEN to fail"),
        };
        assert!(err.to_string().contains("FLY_API_TOKEN"), "got: {err}");
        match saved_app {
            Some(v) => unsafe { std::env::set_var("FLY_WORKER_APP", v) },
            None => unsafe { std::env::remove_var("FLY_WORKER_APP") },
        }
        match saved_tok {
            Some(v) => unsafe { std::env::set_var("FLY_API_TOKEN", v) },
            None => unsafe { std::env::remove_var("FLY_API_TOKEN") },
        }
    }
}
