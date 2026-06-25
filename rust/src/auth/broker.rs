//! Credential broker seam.
//!
//! v1 (`StaticBroker`) is Tier-B passthrough: a request-scoped token from the
//! `X-Upstream-Token` header wins, then a per-instance configured token, then
//! (for the github default) the legacy `RIPCLONE_GITHUB_TOKEN`. Later phases
//! will add Tier-A brokers that mint short-lived scoped tokens (GitHub App,
//! GitLab OAuth, etc.) and a `Principal`-aware broker that enforces policy.

use crate::provider::{ProviderRegistry, RepoId};
use secrecy::SecretString;

/// Abstraction over how ripclone obtains an upstream git credential.
///
/// Implementations must be `Send + Sync` because they live in `ServerState` and
/// are used from async handlers and the background build worker.
pub trait CredentialBroker: Send + Sync {
    /// Return a token to use when syncing `repo_id`, or `None` if the repo
    /// should be mirrored anonymously.
    ///
    /// `request_token` is the token supplied by the caller (e.g. the
    /// `X-Upstream-Token` header). It takes precedence over any configured
    /// token so that per-request overrides work.
    fn fetch_credential(
        &self,
        repo_id: &RepoId,
        request_token: Option<&SecretString>,
    ) -> Option<SecretString>;
}

/// Tier-B passthrough broker: request token → configured instance token → none.
#[derive(Clone)]
pub struct StaticBroker {
    registry: ProviderRegistry,
}

impl StaticBroker {
    pub fn new(registry: ProviderRegistry) -> Self {
        Self { registry }
    }
}

impl CredentialBroker for StaticBroker {
    fn fetch_credential(
        &self,
        repo_id: &RepoId,
        request_token: Option<&SecretString>,
    ) -> Option<SecretString> {
        if let Some(token) = request_token {
            return Some(token.clone());
        }
        self.registry.token(repo_id.provider.as_str()).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn static_broker_prefers_request_token() {
        let registry = ProviderRegistry::new();
        let broker = StaticBroker::new(registry);
        let request = SecretString::new("request".into());
        let token = broker
            .fetch_credential(&RepoId::github("o/r"), Some(&request))
            .unwrap();
        assert_eq!(token.expose_secret(), "request");
    }

    #[test]
    fn static_broker_falls_back_to_none() {
        let registry = ProviderRegistry::new();
        let broker = StaticBroker::new(registry);
        assert!(
            broker
                .fetch_credential(&RepoId::github("o/r"), None)
                .is_none()
        );
    }
}
