//! Provider-agnostic webhook receiver: a provider push → enqueue a sync → warm.
//!
//! A webhook is a thin **front door**. Everything heavy already exists — the
//! build queue, the worker, storage, the metadata store. The receiver does
//! three things: **verify → normalize → enqueue**. This module owns the
//! normalize step (per-provider parsing) and the config (per-provider secret +
//! optional allowlist); `server.rs` wires it into the router and the enqueue
//! path.
//!
//! Phase 1 ships GitHub only. GitLab and Gitea are later `WebhookProvider`
//! impls behind the same trait — adding a provider is implementing this one
//! trait, nothing else.

use crate::provider::{ProviderKind, ProviderRegistry};
use axum::http::HeaderMap;
use secrecy::SecretString;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

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
    /// configured broker regardless of visibility, so this does not gate
    /// behavior today. Kept for parity across adapters and future use.
    pub private: Option<bool>,
}

/// The actions the receiver knows how to take. Anything a provider sends that
/// is not one of these parses to `None` (ignored with a `200`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// A branch advanced — warm it.
    Push,
    /// A branch was deleted — clean up its stored ref, do not build.
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
        // Bitbucket and config-only `Generic` hosts have no adapter yet.
        ProviderKind::Bitbucket | ProviderKind::Generic => None,
    }
}

/// Webhook receiver configuration: a secret per provider instance and an
/// optional repo allowlist. Built once at startup from the environment.
#[derive(Debug, Clone, Default)]
pub struct WebhookConfig {
    /// Per-provider-instance webhook secret, keyed by instance id. A provider
    /// with no entry here has no configured secret ⇒ its endpoint returns 503.
    secrets: HashMap<String, SecretString>,
    /// Allowlist of repo storage keys that may be warmed. `None` ⇒ allow all
    /// (single-tenant trust); `Some` ⇒ only listed repos.
    allowlist: Option<HashSet<String>>,
    /// When true, warm every pushed branch (the original receiver's behavior)
    /// instead of the default policy (default branch always; others only if
    /// already tracked). Set by `RIPCLONE_WEBHOOK_WARM_ALL=1`.
    warm_all: bool,
}

impl WebhookConfig {
    /// An empty config: no secrets, allow-all. Used by the worker (which never
    /// serves webhooks) and as the test default.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build a config with a single provider secret and allow-all. Handy for
    /// tests and programmatic setup.
    pub fn with_secret(provider_id: &str, secret: &str) -> Self {
        let mut secrets = HashMap::new();
        secrets.insert(
            provider_id.to_string(),
            SecretString::new(secret.to_string().into()),
        );
        Self {
            secrets,
            allowlist: None,
            warm_all: false,
        }
    }

    /// Set the repo allowlist (chainable). Repos are matched by storage key.
    pub fn with_allowlist(mut self, repos: impl IntoIterator<Item = String>) -> Self {
        self.allowlist = Some(repos.into_iter().collect());
        self
    }

    /// Set the warm-all policy (chainable).
    pub fn with_warm_all(mut self, warm_all: bool) -> Self {
        self.warm_all = warm_all;
        self
    }

    /// Build from the environment.
    ///
    /// Secret per provider instance: `RIPCLONE_WEBHOOK_SECRET_<ID>`, where
    /// `<ID>` is the instance id upper-cased with every non-alphanumeric byte
    /// replaced by `_` (e.g. instance `company-gitea` → `..._COMPANY_GITEA`).
    ///
    /// Allowlist: `RIPCLONE_WEBHOOK_ALLOWLIST`, a comma-separated list of repo
    /// storage keys (e.g. `owner/repo,other/repo`). Unset or empty ⇒ allow all,
    /// with a loud startup log so the operator knows every pushed repo warms.
    pub fn from_env(registry: &ProviderRegistry) -> Self {
        let mut secrets = HashMap::new();
        for instance in registry.iter() {
            let id = instance.id.as_str();
            let var = format!("RIPCLONE_WEBHOOK_SECRET_{}", env_suffix(id));
            let mut value = std::env::var(var).ok();
            // Back-compat: the original GitHub-only receiver used a single
            // `RIPCLONE_WEBHOOK_SECRET`. Honor it as the secret for the built-in
            // `github` instance when the per-provider var isn't set, so existing
            // deployments keep working.
            if value.is_none() && id == "github" {
                value = std::env::var("RIPCLONE_WEBHOOK_SECRET").ok();
            }
            if let Some(secret) = parse_secret(value) {
                secrets.insert(id.to_string(), secret);
            }
        }

        let allowlist = parse_allowlist(std::env::var("RIPCLONE_WEBHOOK_ALLOWLIST").ok());
        let warm_all = std::env::var("RIPCLONE_WEBHOOK_WARM_ALL")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        // Resolve the open questions with the recommended defaults, loudly.
        if secrets.is_empty() {
            info!(
                "webhooks: no RIPCLONE_WEBHOOK_SECRET_<provider> configured — \
                 every /webhooks/<provider> returns 503 until a secret is set"
            );
        } else {
            let ids: Vec<&str> = secrets.keys().map(|s| s.as_str()).collect();
            info!("webhooks: enabled for provider(s): {}", ids.join(", "));
            match &allowlist {
                Some(set) => info!(
                    "webhooks: repo allowlist active ({} repo(s)); pushes to other repos are ignored",
                    set.len()
                ),
                None => warn!(
                    "webhooks: NO allowlist (RIPCLONE_WEBHOOK_ALLOWLIST unset) — \
                     warming ALL repos pushed via configured providers"
                ),
            }
        }

        Self {
            secrets,
            allowlist,
            warm_all,
        }
    }

    /// The configured secret for a provider instance, if any. No secret ⇒ the
    /// handler must fail closed with 503.
    pub fn secret(&self, provider_id: &str) -> Option<&SecretString> {
        self.secrets.get(provider_id)
    }

    /// Whether to warm every pushed branch (vs the default-or-tracked policy).
    pub fn warm_all(&self) -> bool {
        self.warm_all
    }

    /// Whether a repo (by storage key) may be warmed. Allow-all when no
    /// allowlist is configured.
    pub fn allows(&self, storage_key: &str) -> bool {
        match &self.allowlist {
            Some(set) => set.contains(storage_key),
            None => true,
        }
    }
}

/// Turn a raw env value into a secret. An absent, empty, **or all-whitespace**
/// value yields no secret — fail closed: a blank `RIPCLONE_WEBHOOK_SECRET_*`
/// must never be treated as a usable key (for GitLab the secret *is* the bare
/// token, so a whitespace-only token would be a trivially guessable credential).
/// The secret is kept verbatim (not trimmed) so a token that legitimately
/// contains spaces still matches what the provider sends.
fn parse_secret(raw: Option<String>) -> Option<SecretString> {
    raw.filter(|s| !s.trim().is_empty())
        .map(|s| SecretString::new(s.into()))
}

/// Parse the comma-separated `RIPCLONE_WEBHOOK_ALLOWLIST` into a set of repo
/// storage keys. Entries are trimmed; empty entries are dropped. An absent value
/// or one that yields no entries returns `None` (allow-all).
fn parse_allowlist(raw: Option<String>) -> Option<HashSet<String>> {
    raw.map(|raw| {
        raw.split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect::<HashSet<_>>()
    })
    .filter(|set| !set.is_empty())
}

/// Normalize a provider instance id into the `RIPCLONE_WEBHOOK_SECRET_<...>`
/// env-var suffix: upper-case, with any non-alphanumeric byte mapped to `_`.
fn env_suffix(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_suffix_normalizes_instance_ids() {
        assert_eq!(env_suffix("github"), "GITHUB");
        assert_eq!(env_suffix("company-gitea"), "COMPANY_GITEA");
        assert_eq!(env_suffix("my.gitlab"), "MY_GITLAB");
    }

    #[test]
    fn allowlist_none_allows_all() {
        let cfg = WebhookConfig::empty();
        assert!(cfg.allows("anyone/anything"));
    }

    #[test]
    fn allowlist_some_gates_by_storage_key() {
        let cfg = WebhookConfig {
            secrets: HashMap::new(),
            allowlist: Some(HashSet::from(["acme/widget".to_string()])),
            warm_all: false,
        };
        assert!(cfg.allows("acme/widget"));
        assert!(!cfg.allows("acme/other"));
    }

    #[test]
    fn no_secret_for_provider_is_none() {
        let cfg = WebhookConfig::empty();
        assert!(cfg.secret("github").is_none());
    }

    #[test]
    fn parse_secret_rejects_absent_empty_and_whitespace() {
        use secrecy::ExposeSecret;
        // Absent / empty / all-whitespace ⇒ no secret (fail closed).
        assert!(parse_secret(None).is_none());
        assert!(parse_secret(Some(String::new())).is_none());
        assert!(parse_secret(Some("   ".to_string())).is_none());
        assert!(parse_secret(Some("\t\n".to_string())).is_none());
        // A real value is kept verbatim (not trimmed).
        let s = parse_secret(Some(" hunter2 ".to_string())).expect("non-blank secret");
        assert_eq!(s.expose_secret(), " hunter2 ");
    }

    #[test]
    fn parse_allowlist_trims_and_drops_empties() {
        // Absent ⇒ allow-all.
        assert!(parse_allowlist(None).is_none());
        // A value with only separators/whitespace yields no entries ⇒ allow-all.
        assert!(parse_allowlist(Some("  , ,".to_string())).is_none());
        // Entries are trimmed and empties dropped.
        let set = parse_allowlist(Some(" a/b , c/d ,, ".to_string())).expect("non-empty set");
        assert_eq!(set.len(), 2);
        assert!(set.contains("a/b"));
        assert!(set.contains("c/d"));
        assert!(!set.contains(" a/b "), "entries must be trimmed");
    }
}
