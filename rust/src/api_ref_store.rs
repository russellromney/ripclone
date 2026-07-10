//! `RefStore` that reports writes to a ripclone server over HTTP.
//!
//! Selected with `RIPCLONE_METADATA=api`. The worker holds only a report URL
//! and a signed, expiring bearer token — never database credentials. The token
//! carries no repo/job scope (the worker pool claims any repo); the write
//! target is the `repo_key` in each report body. The server that holds the real
//! metadata store performs the durable write after checking the token
//! (`POST /v1/refs`).
//!
//! Reads return empty: a farmed-out worker builds cold and only needs the write
//! path for publish. A failed report is never swallowed — network/5xx map to
//! retryable errors so the job requeues, and 401/403 to an unauthorized error so
//! the worker exits cleanly for respawn with a fresh token.
//!
//! The token is a durable, operator-provisioned value (`ripclone
//! mint-worker-token`), so `api` mode is deployable for real farm-out. The
//! queue-side twin is [`ApiJobQueue`](crate::api_job_queue) (`RIPCLONE_QUEUE=api`).

use crate::RefInfo;
use crate::provider::RepoId;
use crate::ref_store::{AddedRepo, RefStore};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;
use tracing::info;

/// Marker error so `classify_build_error` can mark report failures retryable
/// without depending on reqwest status alone (a 5xx response is not a
/// `reqwest::Error`).
#[derive(Debug)]
pub struct ApiReportError {
    message: String,
    retryable: bool,
    /// The server rejected our bearer token (401/403). Not retryable, and a
    /// distinct signal so a farmed-out worker can exit cleanly on an expired
    /// token (the dispatcher respawns it with a fresh one) instead of spinning.
    unauthorized: bool,
}

impl ApiReportError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            unauthorized: false,
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            unauthorized: false,
        }
    }

    /// Auth was rejected (401/403). Permanent, and flagged so the worker knows
    /// its token expired.
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            unauthorized: true,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    pub fn is_unauthorized(&self) -> bool {
        self.unauthorized
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ApiReportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for ApiReportError {}

/// Wire body for `POST /v1/refs`. One variant per `RefStore` write.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RefReport {
    SaveBranch {
        repo_key: String,
        branch: String,
        info: Box<RefInfo>,
    },
    UpdateBuildStatus {
        repo_key: String,
        branch: String,
        expected_commit: String,
        status: String,
    },
    DeleteBranch {
        repo_key: String,
        branch: String,
    },
    TouchLastAccessed {
        repo_key: String,
        branch: String,
        expected_commit: String,
    },
}

impl RefReport {
    pub fn repo_key(&self) -> &str {
        match self {
            Self::SaveBranch { repo_key, .. }
            | Self::UpdateBuildStatus { repo_key, .. }
            | Self::DeleteBranch { repo_key, .. }
            | Self::TouchLastAccessed { repo_key, .. } => repo_key,
        }
    }
}

/// JSON response for ops that return a boolean (update/touch).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefReportResponse {
    #[serde(default)]
    pub updated: bool,
}

/// Worker-side `RefStore`: every write is a POST to the report URL.
///
/// Keeps a process-local write-through map so multi-step publish (phase 1 save
/// → phase 2 load → save) can re-read what this worker just reported. The map
/// is not shared with other workers and is not authoritative — the server is.
pub struct ApiRefStore {
    report_url: String,
    job_token: String,
    client: reqwest::Client,
    local: tokio::sync::RwLock<std::collections::HashMap<(String, String), RefInfo>>,
}

impl ApiRefStore {
    /// Build from env: requires `RIPCLONE_METADATA_REPORT_URL` and
    /// `RIPCLONE_METADATA_JOB_TOKEN`. Fails loudly if either is missing.
    pub fn from_env() -> Result<Self> {
        let report_url = std::env::var("RIPCLONE_METADATA_REPORT_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .context(
                "RIPCLONE_METADATA=api requires RIPCLONE_METADATA_REPORT_URL \
                 (the server's POST /v1/refs endpoint)",
            )?;
        let job_token = std::env::var("RIPCLONE_METADATA_JOB_TOKEN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .context(
                "RIPCLONE_METADATA=api requires RIPCLONE_METADATA_JOB_TOKEN \
                 (signed, expiring bearer token from job_token::mint_job_token; \
                 the server does not mint or inject this automatically yet)",
            )?;
        Self::new(report_url, job_token)
    }

    pub fn new(report_url: impl Into<String>, job_token: impl Into<String>) -> Result<Self> {
        let report_url = report_url.into();
        let job_token = job_token.into();
        if report_url.trim().is_empty() {
            bail!("RIPCLONE_METADATA_REPORT_URL must not be empty");
        }
        if job_token.trim().is_empty() {
            bail!("RIPCLONE_METADATA_JOB_TOKEN must not be empty");
        }
        // Reject obvious non-http URLs early (fail loud at start, not on first write).
        if !(report_url.starts_with("http://") || report_url.starts_with("https://")) {
            bail!("RIPCLONE_METADATA_REPORT_URL must be http(s), got {report_url:?}");
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("build HTTP client for ApiRefStore")?;
        info!(url = %report_url, "metadata store: api (report writes to server)");
        Ok(Self {
            report_url,
            job_token,
            client,
            local: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        })
    }

    fn local_key(repo_id: &RepoId, branch: &str) -> (String, String) {
        (repo_id.storage_key(), branch.to_string())
    }

    async fn post_report(&self, body: &RefReport) -> Result<RefReportResponse> {
        let resp = self
            .client
            .post(&self.report_url)
            .header("Authorization", format!("Bearer {}", self.job_token))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|e| {
                // reqwest connect/timeout errors are also recognized by
                // classify_build_error; keep ApiReportError as a clear marker.
                ApiReportError::retryable(format!("metadata report to {}: {e}", self.report_url))
            })?;
        let status = resp.status();
        if status.is_success() {
            // Body may be empty on pure-ack ops; default updated=true.
            let text = resp.text().await.unwrap_or_default();
            if text.trim().is_empty() {
                return Ok(RefReportResponse { updated: true });
            }
            match serde_json::from_str::<RefReportResponse>(&text) {
                Ok(r) => Ok(r),
                Err(_) => Ok(RefReportResponse { updated: true }),
            }
        } else if status.as_u16() == 401 || status.as_u16() == 403 {
            let body = resp.text().await.unwrap_or_default();
            Err(ApiReportError::unauthorized(format!(
                "metadata report unauthorized ({status}): {body}"
            ))
            .into())
        } else if status.is_server_error() || status.as_u16() == 429 {
            let body = resp.text().await.unwrap_or_default();
            Err(ApiReportError::retryable(format!(
                "metadata report retryable HTTP {status}: {body}"
            ))
            .into())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(
                ApiReportError::permanent(format!("metadata report rejected ({status}): {body}"))
                    .into(),
            )
        }
    }
}

#[async_trait]
impl RefStore for ApiRefStore {
    async fn load(&self, repo_id: &RepoId) -> Result<Option<RefInfo>> {
        self.load_branch(repo_id, "HEAD").await
    }

    async fn save(&self, repo_id: &RepoId, info: &RefInfo) -> Result<()> {
        self.save_branch(repo_id, "HEAD", info).await
    }

    async fn list(&self) -> Result<Vec<RepoId>> {
        Ok(Vec::new())
    }

    async fn load_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RefInfo>> {
        // Only what this worker has written this process — no remote reads.
        let map = self.local.read().await;
        Ok(map.get(&Self::local_key(repo_id, branch)).cloned())
    }

    async fn save_branch(&self, repo_id: &RepoId, branch: &str, info: &RefInfo) -> Result<()> {
        self.post_report(&RefReport::SaveBranch {
            repo_key: repo_id.storage_key(),
            branch: branch.to_string(),
            info: Box::new(info.clone()),
        })
        .await?;
        // Write-through: phase 2 reloads the phase-1 ref from this map.
        let mut map = self.local.write().await;
        map.insert(Self::local_key(repo_id, branch), info.clone());
        Ok(())
    }

    async fn update_build_status(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
        status: &str,
    ) -> Result<bool> {
        let r = self
            .post_report(&RefReport::UpdateBuildStatus {
                repo_key: repo_id.storage_key(),
                branch: branch.to_string(),
                expected_commit: expected_commit.to_string(),
                status: status.to_string(),
            })
            .await?;
        if r.updated {
            let mut map = self.local.write().await;
            if let Some(info) = map.get_mut(&Self::local_key(repo_id, branch))
                && info.commit == expected_commit
            {
                info.build_status = Some(status.to_string());
            }
        }
        Ok(r.updated)
    }

    async fn touch_last_accessed_at(
        &self,
        repo_id: &RepoId,
        branch: &str,
        expected_commit: &str,
    ) -> Result<bool> {
        let r = self
            .post_report(&RefReport::TouchLastAccessed {
                repo_key: repo_id.storage_key(),
                branch: branch.to_string(),
                expected_commit: expected_commit.to_string(),
            })
            .await?;
        Ok(r.updated)
    }

    async fn delete_branch(&self, repo_id: &RepoId, branch: &str) -> Result<()> {
        self.post_report(&RefReport::DeleteBranch {
            repo_key: repo_id.storage_key(),
            branch: branch.to_string(),
        })
        .await?;
        let mut map = self.local.write().await;
        map.remove(&Self::local_key(repo_id, branch));
        Ok(())
    }

    async fn list_branches(&self, repo_id: &RepoId) -> Result<Vec<String>> {
        let key = repo_id.storage_key();
        let map = self.local.read().await;
        Ok(map
            .keys()
            .filter(|(r, _)| r == &key)
            .map(|(_, b)| b.clone())
            .collect())
    }

    async fn add_repo(&self, _repo: &AddedRepo) -> Result<()> {
        // Added-repo state is a server concern; workers never call this.
        bail!("ApiRefStore does not support add_repo (server-only operation)")
    }

    async fn load_added_repo(&self, _repo_id: &RepoId) -> Result<Option<AddedRepo>> {
        Ok(None)
    }

    async fn list_added_repos(&self) -> Result<Vec<AddedRepo>> {
        Ok(Vec::new())
    }

    async fn health(&self) -> Result<()> {
        // A cheap GET to the report URL is not defined; connectivity is proven
        // on the first write. Report healthy so /readyz is not used on workers.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::RepoId;
    use std::sync::Arc;

    #[tokio::test]
    async fn dead_url_is_retryable_api_report_error() {
        let store = ApiRefStore::new(
            "http://127.0.0.1:1/v1/refs", // nothing listening
            "test-token",
        )
        .unwrap();
        let repo = RepoId::github("acme/dead");
        let info = RefInfo {
            commit: "abc".into(),
            default_branch: "main".into(),
            ..Default::default()
        };
        let err = store.save_branch(&repo, "main", &info).await.unwrap_err();
        let api = err
            .downcast_ref::<ApiReportError>()
            .expect("ApiReportError in chain");
        assert!(api.is_retryable(), "network failure must be retryable");
    }

    #[test]
    fn from_env_fails_loudly_without_url_or_token() {
        // Process-global env: run both checks under one lock so parallel
        // lib tests can't clobber each other mid-assertion.
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let key_url = "RIPCLONE_METADATA_REPORT_URL";
        let key_tok = "RIPCLONE_METADATA_JOB_TOKEN";
        let prev_url = std::env::var(key_url).ok();
        let prev_tok = std::env::var(key_tok).ok();

        // Missing URL.
        unsafe {
            std::env::remove_var(key_url);
            std::env::set_var(key_tok, "tok");
        }
        let err = match ApiRefStore::from_env() {
            Ok(_) => panic!("expected missing REPORT_URL to fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("RIPCLONE_METADATA_REPORT_URL"),
            "got: {err}"
        );

        // Missing token.
        unsafe {
            std::env::set_var(key_url, "http://127.0.0.1:9/v1/refs");
            std::env::remove_var(key_tok);
        }
        let err = match ApiRefStore::from_env() {
            Ok(_) => panic!("expected missing JOB_TOKEN to fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("RIPCLONE_METADATA_JOB_TOKEN"),
            "got: {err}"
        );

        unsafe {
            match prev_url {
                Some(v) => std::env::set_var(key_url, v),
                None => std::env::remove_var(key_url),
            }
            match prev_tok {
                Some(v) => std::env::set_var(key_tok, v),
                None => std::env::remove_var(key_tok),
            }
        }
    }

    /// Prove the type is object-safe and usable as Arc<dyn RefStore>.
    #[test]
    fn is_dyn_ref_store() {
        let store: Arc<dyn RefStore> =
            Arc::new(ApiRefStore::new("http://127.0.0.1:9/v1/refs", "t").unwrap());
        let _ = store;
    }
}
