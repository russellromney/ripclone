//! Provider-agnostic webhook receiver: a signed default-branch push → exact admission.
//!
//! A webhook is a thin **front door**. Everything heavy already exists — the
//! control store, the build workers, storage. The receiver does three things:
//! **verify → normalize → admit**. This module owns the normalize step
//! (per-provider parsing) and the config (a secret per provider);
//! `server/webhooks.rs` wires it into the router and the admission path.
//!
//! GitHub, GitLab and Gitea have `WebhookProvider` impls behind the same
//! trait — adding a provider is implementing this one trait, nothing else.

use crate::auth::credential::{env_var, secret};
use crate::provider::{ProviderKind, ProviderRegistry};
use axum::http::HeaderMap;
use secrecy::SecretString;
use std::collections::HashMap;
use tracing::info;

mod gitea;
mod github;
mod gitlab;

/// True for a git "null" object id (a deleted ref's `after`): non-empty and all
/// ASCII zeros (40 chars for SHA-1, 64 for SHA-256). Shared by the adapters.
pub(super) fn is_zero_sha(sha: &str) -> bool {
    !sha.is_empty() && sha.bytes().all(|b| b == b'0')
}

/// What a provider push tells us, normalized across providers. The receiver
/// trusts these fields only for **routing** (which repo / ref to warm) — never
/// to choose a credential or escalate access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalEvent {
    pub kind: EventKind,
    /// Provider-normalized repo path, e.g. `owner/name` for GitHub. Combined
    /// with the path's `ProviderInstance` to form a `RepoId` in the handler.
    pub repo: String,
    /// Full ref, e.g. `refs/heads/main`. Empty for events with no ref (ping).
    pub ref_: String,
    /// New tip sha. `None` (or an all-zeros sha) means the ref was deleted.
    pub after: Option<String>,
    /// The repo's default branch, when the payload carries it.
    pub default_branch: Option<String>,
    /// Repo visibility, when the payload carries it. Currently **informational**
    /// only: the warm path always resolves the upstream credential from the
    /// server's credentials regardless of visibility, so this does not gate
    /// behavior today. Kept for parity across adapters and future use.
    pub private: Option<bool>,
}

/// The actions the receiver knows how to take. Anything a provider sends that
/// is not one of these parses to `None` (ignored with a `200`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// A branch advanced — warm it.
    Push,
    /// A branch was deleted — acknowledge without changing durable exact results.
    BranchDelete,
    /// A provider connectivity check — acknowledge with `200`.
    Ping,
}

/// One provider's webhook adapter: verify a signature and normalize a payload.
/// This is the single thing you implement to add a provider.
pub trait WebhookProvider {
    /// Constant-time signature/secret check over the **raw** body. Must return
    /// `false` for a missing or malformed signature (fail closed).
    fn verify(&self, headers: &HeaderMap, raw: &[u8], secret: &str) -> bool;

    /// Parse a provider payload into the canonical shape. `None` means "an
    /// event we don't act on" — the handler answers `200 {"ignored":…}`.
    fn parse(&self, headers: &HeaderMap, raw: &[u8]) -> Option<CanonicalEvent>;
}

/// The webhook adapter for a provider kind, or `None` if that kind has no
/// adapter yet. The adapter is `Send + Sync` so it can be held across `.await`
/// points inside the (Send) request handler.
pub fn provider_for(kind: ProviderKind) -> Option<Box<dyn WebhookProvider + Send + Sync>> {
    match kind {
        ProviderKind::GitHub => Some(Box::new(github::GitHub)),
        ProviderKind::GitLab => Some(Box::new(gitlab::GitLab)),
        // `Gitea` covers Forgejo/Codeberg (same payload + signature scheme).
        ProviderKind::Gitea => Some(Box::new(gitea::Gitea)),
        // Config-only `Generic` hosts have no adapter.
        ProviderKind::Generic => None,
    }
}

/// Webhook receiver configuration: a secret per provider instance. Built once
/// at startup from the environment. A push builds only an added repo.
#[derive(Debug, Clone, Default)]
pub struct WebhookConfig {
    /// Per-provider-instance webhook secret, keyed by instance id. A provider
    /// with no entry here has no configured secret ⇒ its endpoint returns 503.
    secrets: HashMap<String, SecretString>,
}

impl WebhookConfig {
    /// Build from the environment.
    ///
    /// Secret per provider instance: `RIPCLONE_WEBHOOK_SECRET_<ID>`, where
    /// `<ID>` is the instance id upper-cased with every non-alphanumeric byte
    /// replaced by `_` (e.g. instance `company-gitea` → `..._COMPANY_GITEA`).
    pub fn from_env(registry: &ProviderRegistry) -> Self {
        let mut secrets = HashMap::new();
        for instance in registry.iter() {
            let id = instance.id.as_str();
            let value = std::env::var(env_var("RIPCLONE_WEBHOOK_SECRET", id)).ok();
            // Blank is no secret, so the handler fails closed: for GitLab the
            // secret is the bare token, and a blank one is trivially guessed.
            if let Some(secret) = secret(value) {
                secrets.insert(id.to_string(), secret);
            }
        }

        if secrets.is_empty() {
            info!(
                "webhooks: no RIPCLONE_WEBHOOK_SECRET_<provider> configured — \
                 every /webhooks/<provider> returns 503 until a secret is set"
            );
        } else {
            let ids: Vec<&str> = secrets.keys().map(|s| s.as_str()).collect();
            info!("webhooks: enabled for provider(s): {}", ids.join(", "));
        }

        Self { secrets }
    }

    /// The configured secret for a provider instance, if any. No secret ⇒ the
    /// handler must fail closed with 503.
    pub fn secret(&self, provider_id: &str) -> Option<&SecretString> {
        self.secrets.get(provider_id)
    }
}

#[cfg(test)]
impl WebhookConfig {
    /// A config with a single provider secret.
    pub(crate) fn with_secret(provider_id: &str, secret: &str) -> Self {
        let mut secrets = HashMap::new();
        secrets.insert(
            provider_id.to_string(),
            SecretString::new(secret.to_string().into()),
        );
        Self { secrets }
    }
}
