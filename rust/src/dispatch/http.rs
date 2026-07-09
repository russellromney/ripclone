//! `http` self-host escape hatch: POST [`WorkerSpec`] JSON to a configured URL.
//!
//! Covers anything callable (Lambda, Cloud Function, Modal webhook). The
//! receiver starts the compute; ripclone only delivers the spec.

use super::{ComputeProvider, WorkerSpec, validate_dispatch_url};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tracing::info;

/// Configuration for [`HttpProvider`].
#[derive(Debug, Clone)]
pub struct HttpProviderConfig {
    /// Absolute `http`/`https` URL that accepts `POST` with a JSON [`WorkerSpec`] body.
    pub url: String,
    /// Optional bearer token (`Authorization: Bearer …`).
    pub token: Option<String>,
    /// Injected client (tests). Defaults to a fresh reqwest client.
    pub client: Option<reqwest::Client>,
}

/// POSTs the worker spec as JSON.
pub struct HttpProvider {
    url: String,
    token: Option<String>,
    client: reqwest::Client,
}

impl HttpProvider {
    /// Build a provider. Fails loudly if the URL is empty, not absolute http(s),
    /// or points at a link-local / unspecified host (metadata SSRF guard).
    pub fn new(cfg: HttpProviderConfig) -> Result<Self> {
        validate_dispatch_url(&cfg.url)?;
        Ok(Self {
            url: cfg.url,
            token: cfg.token,
            client: cfg.client.unwrap_or_else(|| {
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(15))
                    .build()
                    .expect("reqwest client")
            }),
        })
    }

    /// `RIPCLONE_DISPATCH_URL` required; optional `RIPCLONE_DISPATCH_TOKEN`.
    pub fn from_env() -> Result<Self> {
        let url = std::env::var("RIPCLONE_DISPATCH_URL")
            .context("RIPCLONE_DISPATCH_URL is required for RIPCLONE_DISPATCH=http")?;
        let token = std::env::var("RIPCLONE_DISPATCH_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        Self::new(HttpProviderConfig {
            url,
            token,
            client: None,
        })
    }
}

#[async_trait]
impl ComputeProvider for HttpProvider {
    fn name(&self) -> &str {
        "http"
    }

    async fn ensure_worker(&self, spec: &WorkerSpec) -> Result<()> {
        spec.validate()?;
        info!(url = %self.url, size_class = %spec.size_class, "http.ensure_worker POST");
        let mut req = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .json(spec);
        if let Some(ref token) = self.token {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        let res = req.send().await.context("http dispatch POST")?;
        let status = res.status();
        if !status.is_success() {
            let body = res.text().await.unwrap_or_default();
            bail!(
                "http dispatch failed: {status}{}",
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    async fn spawn_wake_server(
        on_body: Arc<Mutex<Option<WorkerSpec>>>,
        on_auth: Arc<Mutex<Option<String>>>,
    ) -> String {
        let state = (on_body, on_auth);
        let app = Router::new()
            .route(
                "/wake",
                post(
                    |State(state): State<(
                        Arc<Mutex<Option<WorkerSpec>>>,
                        Arc<Mutex<Option<String>>>,
                    )>,
                     headers: axum::http::HeaderMap,
                     Json(body): Json<WorkerSpec>| async move {
                        *state.1.lock().unwrap() = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        *state.0.lock().unwrap() = Some(body);
                        Json(serde_json::json!({ "ok": true }))
                    },
                ),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        // Ready: listener is bound before serve; one short yield for the task.
        tokio::task::yield_now().await;
        format!("http://{addr}/wake")
    }

    #[tokio::test]
    async fn posts_worker_spec_as_json() {
        let received: Arc<Mutex<Option<WorkerSpec>>> = Arc::new(Mutex::new(None));
        let auth_hdr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let url = spawn_wake_server(received.clone(), auth_hdr.clone()).await;

        let provider = HttpProvider::new(HttpProviderConfig {
            url,
            token: Some("dispatch-secret".into()),
            client: None,
        })
        .unwrap();

        let mut env = BTreeMap::new();
        env.insert("RIPCLONE_QUEUE".into(), "libsql".into());
        let spec = WorkerSpec::new("large", env);
        provider.ensure_worker(&spec).await.unwrap();

        let got = received.lock().unwrap().clone().expect("body");
        assert_eq!(got, spec);
        assert_eq!(
            auth_hdr.lock().unwrap().as_deref(),
            Some("Bearer dispatch-secret")
        );
    }

    #[tokio::test]
    async fn non_2xx_is_error() {
        let app = Router::new().route(
            "/wake",
            post(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "busy") }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        tokio::task::yield_now().await;

        let provider = HttpProvider::new(HttpProviderConfig {
            url: format!("http://{addr}/wake"),
            token: None,
            client: None,
        })
        .unwrap();
        let err = provider
            .ensure_worker(&WorkerSpec::new("small", BTreeMap::new()))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("http dispatch failed"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_metadata_ssrf_url() {
        let err = match HttpProvider::new(HttpProviderConfig {
            url: "http://169.254.169.254/latest/meta-data".into(),
            token: None,
            client: None,
        }) {
            Err(e) => e,
            Ok(_) => panic!("expected link-local URL to fail"),
        };
        assert!(err.to_string().contains("link-local"), "got: {err}");
    }

    #[test]
    fn rejects_relative_and_empty_url() {
        let err = match HttpProvider::new(HttpProviderConfig {
            url: "".into(),
            token: None,
            client: None,
        }) {
            Err(e) => e,
            Ok(_) => panic!("expected empty URL to fail"),
        };
        assert!(err.to_string().contains("empty") || err.to_string().contains("invalid"));

        let err = match HttpProvider::new(HttpProviderConfig {
            url: "/local/wake".into(),
            token: None,
            client: None,
        }) {
            Err(e) => e,
            Ok(_) => panic!("expected relative URL to fail"),
        };
        assert!(
            err.to_string().contains("invalid dispatch URL"),
            "got: {err}"
        );
    }
}
