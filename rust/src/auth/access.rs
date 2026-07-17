//! Per-repo read authorization (AU1).
//!
//! ripclone has no separate gateway: public repos are served anonymously and
//! private repos are gated by the caller's own git credential (the
//! `X-Upstream-Token` the client passes, which is its credential for the
//! upstream provider). The bug this closes is that cached content was served
//! *without* re-checking that credential — so any holder of the shared server
//! token could read any already-cached private repo. The shared server token
//! authenticates "you may talk to this backend"; it is NOT per-tenant authz.
//!
//! [`AccessVerifier`] answers, for one `(repo, caller-credential)`, whether the
//! repo is public (serve anonymously), private-and-this-caller-may-read-it, or
//! denied. The HTTP implementation proves access the same way a clone would —
//! a `git-upload-pack` `info/refs` probe against the provider — so it works for
//! every provider kind with no host-specific API. Results are cached for a
//! short TTL so a repeat clone is not a fresh provider round-trip.

use crate::provider::ProviderInstance;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::warn;

/// Outcome of a per-repo access check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessDecision {
    /// The repo is public; serve it anonymously.
    Public,
    /// The repo is private and the caller's credential grants read access.
    PrivateAuthorized,
    /// The caller may not read this repo (private + missing/invalid credential).
    Denied,
}

/// Decides whether a caller may read a given repo.
#[async_trait::async_trait]
pub trait AccessVerifier: Send + Sync {
    async fn verify(
        &self,
        provider: &ProviderInstance,
        repo_path: &str,
        credential: Option<&SecretString>,
    ) -> AccessDecision;

    /// Return a still-fresh cached decision without contacting the provider.
    /// `None` means there is no decision that can be reused safely.
    async fn verify_cached(
        &self,
        _provider: &ProviderInstance,
        _repo_path: &str,
        _credential: Option<&SecretString>,
    ) -> Option<AccessDecision> {
        None
    }
}

fn credential_fingerprint(cred: &SecretString) -> u64 {
    // A non-reversible cache key for the credential, so the authz cache can be
    // partitioned per caller without storing the secret. Cache-keying only; not
    // a security boundary.
    let mut h = std::collections::hash_map::DefaultHasher::new();
    cred.expose_secret().hash(&mut h);
    h.finish()
}

/// Proves access via a `git-upload-pack` `info/refs` probe against the provider
/// — exactly the request a clone makes — so it is provider-agnostic. A `2xx`
/// anonymously means public; a `2xx` with the caller's credential means the
/// caller may read a private repo. Both answers are cached for `ttl`.
pub struct HttpAccessVerifier {
    client: reqwest::Client,
    ttl: Duration,
    /// A clone may poll for ~80s and refresh a private URL later in the same
    /// operation. Reuse the already-proved decision for that bounded window;
    /// ordinary moving reads still refresh at `ttl`.
    pinned_ttl: Duration,
    /// repo clone URL -> (cached_at, is_public)
    public_cache: RwLock<HashMap<String, (Instant, bool)>>,
    /// (repo clone URL, credential fingerprint) -> (cached_at, authorized)
    authz_cache: RwLock<HashMap<(String, u64), (Instant, bool)>>,
}

impl HttpAccessVerifier {
    pub fn new() -> Self {
        let ttl = Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            client,
            ttl,
            // Matches the default private signed-URL exposure window. A pinned
            // request after this bound fails closed instead of probing upstream.
            pinned_ttl: Duration::from_secs(300),
            public_cache: RwLock::new(HashMap::new()),
            authz_cache: RwLock::new(HashMap::new()),
        }
    }

    fn info_refs_url(provider: &ProviderInstance, repo_path: &str) -> String {
        // `clone_url` renders `https://host/path.git`; the smart-HTTP probe is
        // that URL plus `/info/refs?service=git-upload-pack`.
        format!(
            "{}/info/refs?service=git-upload-pack",
            provider.clone_url(repo_path)
        )
    }

    /// Run one `info/refs` probe; `Some(true)` = readable, `Some(false)` =
    /// not readable (401/403/404), `None` = the probe itself failed (network),
    /// which the caller treats as "fail closed".
    async fn probe(&self, url: &str, auth: Option<(String, String)>) -> Option<bool> {
        let mut req = self.client.get(url);
        if let Some((name, value)) = auth {
            req = req.header(name, value);
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    Some(true)
                } else if status.as_u16() == 401 || status.as_u16() == 403 || status.as_u16() == 404
                {
                    Some(false)
                } else {
                    // 5xx / unexpected: don't cache, fail closed for this call.
                    warn!("access probe for {url} returned {status}; treating as not authorized");
                    None
                }
            }
            Err(e) => {
                warn!("access probe for {url} failed: {e}; treating as not authorized");
                None
            }
        }
    }

    async fn is_public(&self, url: &str) -> bool {
        if let Some((at, v)) = self.public_cache.read().await.get(url)
            && at.elapsed() < self.ttl
        {
            return *v;
        }
        let public = self.probe(url, None).await.unwrap_or(false);
        self.public_cache
            .write()
            .await
            .insert(url.to_string(), (Instant::now(), public));
        public
    }

    async fn caller_authorized(
        &self,
        provider: &ProviderInstance,
        url: &str,
        cred: &SecretString,
    ) -> bool {
        let key = (url.to_string(), credential_fingerprint(cred));
        if let Some((at, v)) = self.authz_cache.read().await.get(&key)
            && at.elapsed() < self.ttl
        {
            return *v;
        }
        let token = cred.expose_secret();
        let authorized = self
            .probe(url, provider.auth_header(token))
            .await
            .unwrap_or(false);
        self.authz_cache
            .write()
            .await
            .insert(key, (Instant::now(), authorized));
        authorized
    }
}

impl Default for HttpAccessVerifier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl AccessVerifier for HttpAccessVerifier {
    async fn verify(
        &self,
        provider: &ProviderInstance,
        repo_path: &str,
        credential: Option<&SecretString>,
    ) -> AccessDecision {
        let url = Self::info_refs_url(provider, repo_path);
        if self.is_public(&url).await {
            return AccessDecision::Public;
        }
        match credential {
            Some(cred) if self.caller_authorized(provider, &url, cred).await => {
                AccessDecision::PrivateAuthorized
            }
            _ => AccessDecision::Denied,
        }
    }

    async fn verify_cached(
        &self,
        provider: &ProviderInstance,
        repo_path: &str,
        credential: Option<&SecretString>,
    ) -> Option<AccessDecision> {
        let url = Self::info_refs_url(provider, repo_path);
        let public = self
            .public_cache
            .read()
            .await
            .get(&url)
            .filter(|(at, _)| at.elapsed() < self.pinned_ttl)
            .map(|(_, value)| *value)?;
        if public {
            return Some(AccessDecision::Public);
        }
        let credential = credential?;
        let key = (url, credential_fingerprint(credential));
        self.authz_cache
            .read()
            .await
            .get(&key)
            .filter(|(at, _)| at.elapsed() < self.pinned_ttl)
            .map(|(_, authorized)| {
                if *authorized {
                    AccessDecision::PrivateAuthorized
                } else {
                    AccessDecision::Denied
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{ProviderInstance, ProviderInstanceId, ProviderKind};

    fn gh() -> ProviderInstance {
        ProviderInstance {
            id: ProviderInstanceId::new("github"),
            kind: ProviderKind::GitHub,
            host: "github.com".to_string(),
            auth_template: None,
            auth_header_name: None,
        }
    }

    #[test]
    fn info_refs_url_is_smart_http_probe() {
        assert_eq!(
            HttpAccessVerifier::info_refs_url(&gh(), "o/r"),
            "https://github.com/o/r.git/info/refs?service=git-upload-pack"
        );
    }

    #[test]
    fn credential_fingerprint_is_stable_and_distinct() {
        let a = SecretString::new("tok-a".to_string().into());
        let a2 = SecretString::new("tok-a".to_string().into());
        let b = SecretString::new("tok-b".to_string().into());
        assert_eq!(credential_fingerprint(&a), credential_fingerprint(&a2));
        assert_ne!(credential_fingerprint(&a), credential_fingerprint(&b));
    }

    #[tokio::test]
    async fn cached_verification_never_needs_a_provider_request() {
        let verifier = HttpAccessVerifier::new();
        let provider = gh();
        let url = HttpAccessVerifier::info_refs_url(&provider, "o/r");
        verifier.public_cache.write().await.insert(
            url.clone(),
            (Instant::now() - Duration::from_secs(61), false),
        );
        let credential = SecretString::new("token".to_string().into());
        verifier.authz_cache.write().await.insert(
            (url, credential_fingerprint(&credential)),
            (Instant::now() - Duration::from_secs(61), true),
        );
        assert_eq!(
            verifier
                .verify_cached(&provider, "o/r", Some(&credential))
                .await,
            Some(AccessDecision::PrivateAuthorized)
        );
        assert_eq!(
            verifier.verify_cached(&provider, "missing", None).await,
            None
        );
    }
}
