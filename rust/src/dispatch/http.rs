//! `http` self-host escape hatch: POST [`WorkerSpec`] JSON to a configured URL.
//!
//! Covers anything callable (Lambda, Cloud Function, Modal webhook). The
//! receiver starts the compute; ripclone only delivers the spec.

use super::{ComputeProvider, WorkerSpec};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tracing::info;

/// Configuration for [`HttpProvider`].
#[derive(Debug, Clone)]
pub struct HttpProviderConfig {
    /// Absolute URL that accepts `POST` with a JSON [`WorkerSpec`] body.
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
    pub fn new(cfg: HttpProviderConfig) -> Self {
        Self {
            url: cfg.url,
            token: cfg.token,
            client: cfg.client.unwrap_or_else(|| {
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(15))
                    .build()
                    .expect("reqwest client")
            }),
        }
    }

    /// `RIPCLONE_DISPATCH_URL` required; optional `RIPCLONE_DISPATCH_TOKEN`.
    pub fn from_env() -> Result<Self> {
        let url = std::env::var("RIPCLONE_DISPATCH_URL")
            .context("RIPCLONE_DISPATCH_URL is required for RIPCLONE_DISPATCH=http")?;
        if url.is_empty() {
            bail!("RIPCLONE_DISPATCH_URL must not be empty");
        }
        let token = std::env::var("RIPCLONE_DISPATCH_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());
        Ok(Self::new(HttpProviderConfig {
            url,
            token,
            client: None,
        }))
    }
}

#[async_trait]
impl ComputeProvider for HttpProvider {
    fn name(&self) -> &str {
        "http"
    }

    async fn ensure_worker(&self, spec: &WorkerSpec) -> Result<()> {
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

    #[tokio::test]
    async fn posts_worker_spec_as_json() {
        let received: Arc<Mutex<Option<WorkerSpec>>> = Arc::new(Mutex::new(None));
        let auth_hdr: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let state = (received.clone(), auth_hdr.clone());

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

        let provider = HttpProvider::new(HttpProviderConfig {
            url: format!("http://{addr}/wake"),
            token: Some("dispatch-secret".into()),
            client: None,
        });

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

        let provider = HttpProvider::new(HttpProviderConfig {
            url: format!("http://{addr}/wake"),
            token: None,
            client: None,
        });
        let err = provider
            .ensure_worker(&WorkerSpec::new("small", BTreeMap::new()))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("http dispatch failed"),
            "got: {err}"
        );
    }
}
