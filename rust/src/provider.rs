//! Provider / identity abstraction for multi-provider auth (Phases 0–2).
//!
//! This module introduces the addressing seam that lets ripclone move from the
//! hard-coded GitHub `owner/repo` pair to arbitrary git hosts (GitLab
//! subgroups, sourcehut `~user/repo`, Launchpad `+git` paths, self-hosted
//! Gitea/Forgejo, etc.).
//!
//! Phase 0 is intentionally minimal: only the built-in `github` default
//! instance exists, and all routes still parse `{owner}/{repo}` into a GitHub
//! `RepoId`. The back-compat invariant is that a GitHub `RepoId` renders to the
//! *exact* legacy storage keys and mirror directory names, so existing ref-store
//! data and on-disk mirrors need no migration.
//!
//! Phase 3+ handoff notes:
//! - Add OIDC / `Principal` / `authorize()` integration; `CredentialBroker` is
//!   already the seam where a verified principal can influence credential
//!   selection.
//! - Add Tier-A token minting (`AppTokenBroker`) behind the same
//!   `CredentialBroker` trait.
//! - Add per-provider id charset validation rules once opaque paths can contain
//!   `/`, `~`, `+`, etc. (currently `validation::validate_repo_id` is still the
//!   GitHub-only check).

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::net::IpAddr;

pub use crate::provider_config::load_registry_with_token_store;

/// Built-in default instance id. All legacy `{owner}/{repo}` routes resolve to
/// this instance.
const DEFAULT_PROVIDER_ID: &str = "github";

/// Supported git host kinds. `Gitea` covers Forgejo/Codeberg; `Generic` is a
/// config-only host that uses an explicit credential template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    GitHub,
    GitLab,
    Bitbucket,
    Gitea,
    Generic,
}

impl ProviderKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderKind::GitHub => "github",
            ProviderKind::GitLab => "gitlab",
            ProviderKind::Bitbucket => "bitbucket",
            ProviderKind::Gitea => "gitea",
            ProviderKind::Generic => "generic",
        }
    }
}

impl std::str::FromStr for ProviderKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "github" => Ok(ProviderKind::GitHub),
            "gitlab" => Ok(ProviderKind::GitLab),
            "bitbucket" => Ok(ProviderKind::Bitbucket),
            "gitea" | "forgejo" | "codeberg" => Ok(ProviderKind::Gitea),
            "generic" => Ok(ProviderKind::Generic),
            _ => anyhow::bail!("unknown provider kind: {}", s),
        }
    }
}

/// Identifies a configured provider instance (e.g. `"github"`, `"gitlab"`,
/// `"company-gitea"`). This is a string newtype so callers cannot pass an
/// arbitrary `&str` where an instance id is required.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ProviderInstanceId(String);

impl ProviderInstanceId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ProviderInstanceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ProviderInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A configured git provider instance.
///
/// `auth_template` is optional for preset kinds (GitHub/GitLab/Bitbucket/Gitea)
/// and required for `Generic`. When present it must contain exactly one
/// `{token}` placeholder; it overrides the preset's default header.
#[derive(Debug, Clone)]
pub struct ProviderInstance {
    pub id: ProviderInstanceId,
    pub kind: ProviderKind,
    pub host: String,
    /// Optional credential template for `Generic` hosts, or an override for
    /// preset kinds. Example: `"token {token}"` or `"Bearer {token}"`.
    pub auth_template: Option<String>,
    /// Optional header name for the credential. Defaults to `Authorization`.
    /// Some hosts expect a custom name such as `PRIVATE-TOKEN` (GitLab) or a
    /// proxy header.
    pub auth_header_name: Option<String>,
}

/// True for a byte that may appear in an HTTP header field name (an RFC 7230
/// token): visible ASCII excluding separators.
fn is_header_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

impl ProviderInstance {
    /// Build a clean HTTPS clone URL for an opaque repo path.
    ///
    /// Phase 0 back-compat: the github default instance renders
    /// `https://github.com/{owner}/{repo}.git`.
    pub fn clone_url(&self, path: &str) -> String {
        let host = self.host.trim_end_matches('/');
        let path = path.trim_start_matches('/');
        if host.starts_with("http://") || host.starts_with("https://") {
            format!("{}/{}.git", host, path)
        } else {
            format!("https://{}/{}.git", host, path)
        }
    }

    /// Build the `Authorization` (or other) header for the given token.
    ///
    /// Returns `None` for `Generic` hosts that have no credential template; the
    /// caller must treat that as a configuration error.
    ///
    /// Returns `(header_name, header_value)` as strings so it can be passed to
    /// git's `http.extraHeader` config without an extra dependency.
    pub fn auth_header(&self, token: &str) -> Option<(String, String)> {
        // The header name is `Authorization` unless the instance overrides it.
        let name = self.auth_header_name.as_deref().unwrap_or("Authorization");
        // A configured name must be a valid HTTP field name: ASCII visible chars
        // with no separators, whitespace, or control bytes. Reject anything else
        // so a bad config can't smuggle a second header or a malformed request.
        if name.is_empty() || !name.bytes().all(is_header_name_byte) {
            return None;
        }
        let name = name.to_string();
        let value = match &self.auth_template {
            Some(template) => template.replace("{token}", token),
            None => match self.kind {
                ProviderKind::GitHub => {
                    let credentials = format!("x-access-token:{}", token);
                    let encoded = base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        credentials.as_bytes(),
                    );
                    format!("Basic {}", encoded)
                }
                ProviderKind::GitLab => {
                    let credentials = format!("oauth2:{}", token);
                    let encoded = base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        credentials.as_bytes(),
                    );
                    format!("Basic {}", encoded)
                }
                ProviderKind::Bitbucket => format!("Bearer {}", token),
                ProviderKind::Gitea => format!("token {}", token),
                ProviderKind::Generic => return None,
            },
        };
        // AU5: never emit a header value carrying CR/LF (or NUL) — a token or
        // `auth_template` containing those would smuggle extra headers into the
        // upstream git HTTP request. Base64-encoded preset values can't contain
        // these, so this only fires on a malicious/malformed token or template.
        if value.bytes().any(|b| b == b'\r' || b == b'\n' || b == 0) {
            return None;
        }
        Some((name, value))
    }

    /// True for the built-in GitHub default instance.
    pub fn is_github_default(&self) -> bool {
        self.id.as_str() == DEFAULT_PROVIDER_ID
    }
}

/// Defense-in-depth SSRF guard for a configured provider host (AU4). The server
/// fetches the provider host on sync, so reject the literal-internal targets
/// that are *never* a legitimate git host: the link-local range (which includes
/// the cloud metadata endpoint `169.254.169.254`) and the unspecified address
/// (`0.0.0.0` / `::`). Also reject malformed hosts (empty, userinfo, control
/// characters).
///
/// Loopback (`127.0.0.1`, `localhost`, `::1`) and private LAN ranges
/// (`10/172.16/192.168`) are intentionally allowed: same-box and on-prem LAN
/// self-host are legitimate, and providers are operator-configured (not
/// attacker-supplied), so the residual risk is an operator misconfiguring their
/// own deployment. This is a guard against the classic metadata-SSRF mistake,
/// not full DNS-rebinding protection.
fn validate_provider_host(id: &str, host: &str) -> Result<()> {
    let h = host.trim();
    if h.is_empty() {
        anyhow::bail!("provider '{id}' has an empty host");
    }
    if h.bytes().any(|b| b.is_ascii_control() || b == b' ') {
        anyhow::bail!("provider '{id}' host contains control or space characters");
    }
    // Allow an optional scheme; everything else must be a bare authority.
    let after_scheme = h
        .strip_prefix("https://")
        .or_else(|| h.strip_prefix("http://"))
        .unwrap_or(h);
    let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
    if authority.contains('@') {
        anyhow::bail!("provider '{id}' host must not contain userinfo ('@'): {host}");
    }
    // Best-effort hostname: drop a `[v6]` bracket or a trailing numeric `:port`.
    let host_part = if let Some(rest) = authority.strip_prefix('[') {
        rest.split(']').next().unwrap_or(rest)
    } else {
        match authority.rsplit_once(':') {
            Some((head, tail)) if tail.chars().all(|c| c.is_ascii_digit()) && !head.is_empty() => {
                head
            }
            _ => authority,
        }
    };
    // Check both the trimmed host part and the raw authority (covers a bare,
    // unbracketed IPv6 literal that the port-split above leaves intact).
    for candidate in [host_part, authority] {
        if let Ok(ip) = candidate.parse::<IpAddr>() {
            if ip.is_unspecified() {
                anyhow::bail!("provider '{id}' host '{host}' is the unspecified address");
            }
            let link_local = match ip {
                // 169.254.0.0/16 — includes the cloud metadata endpoint.
                IpAddr::V4(v4) => v4.is_link_local(),
                IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
            };
            if link_local {
                anyhow::bail!(
                    "provider '{id}' host '{host}' is a link-local address (SSRF / metadata risk)"
                );
            }
        }
    }
    Ok(())
}

/// Raw configuration for one provider instance, as loaded from config or env.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ProviderConfig {
    pub id: String,
    /// Preset kind. Defaults to `"generic"`.
    pub kind: Option<String>,
    /// Hostname used in clone URLs, e.g. `"gitlab.com"` or `"gitea.example.com"`.
    pub host: Option<String>,
    /// Optional per-instance static token. If present, it is used as a Tier-B
    /// passthrough credential for syncs of repos in this instance.
    pub token: Option<String>,
    /// Optional credential template for `generic` hosts or overrides for presets.
    /// Must contain exactly one `{token}` placeholder.
    pub auth_template: Option<String>,
    /// Optional header name for the credential. Defaults to `Authorization`.
    /// Example: `"PRIVATE-TOKEN"`.
    pub auth_header_name: Option<String>,
}

/// Registry of configured provider instances.
#[derive(Debug, Clone)]
pub struct ProviderRegistry {
    providers: HashMap<String, ProviderInstance>,
    /// Tier-B passthrough tokens keyed by instance id. Kept separate from
    /// `ProviderInstance` so the broker can decide whether to use a request
    /// token, a configured token, or (later) a minted Tier-A token.
    tokens: HashMap<String, secrecy::SecretString>,
}

impl ProviderRegistry {
    /// Build a registry containing only the built-in GitHub default instance.
    pub fn new() -> Self {
        let mut providers = HashMap::new();
        let mut tokens = HashMap::new();
        providers.insert(
            DEFAULT_PROVIDER_ID.to_string(),
            ProviderInstance {
                id: ProviderInstanceId::new(DEFAULT_PROVIDER_ID),
                kind: ProviderKind::GitHub,
                host: "github.com".to_string(),
                auth_template: None,
                auth_header_name: None,
            },
        );
        if let Some(token) = std::env::var("RIPCLONE_GITHUB_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())
        {
            tokens.insert(
                DEFAULT_PROVIDER_ID.to_string(),
                secrecy::SecretString::new(token.into()),
            );
        }
        Self { providers, tokens }
    }

    /// Load from configuration.
    ///
    /// Reads `RIPCLONE_PROVIDERS` as JSON first, then merges a config file at
    /// `RIPCLONE_PROVIDERS_CONFIG` if present, then merges the default
    /// `~/.config/ripclone/providers.json`. Tokens are resolved from the
    /// `RIPCLONE_PROVIDER_<ID>_TOKEN` env var or the ripclone token file. The
    /// built-in `github` default is always present and can be
    /// overridden by config (host, token, template).
    pub fn load() -> Result<Self> {
        let token_store = crate::auth::token_store::FileBackedTokenStore::new()
            .context("initialize token store")?;
        crate::provider_config::load_registry_with_token_store(&token_store)
    }

    /// Merge a single provider config into the registry.
    pub fn merge_one(&mut self, cfg: ProviderConfig) -> Result<()> {
        let id = cfg.id;
        if id.is_empty() {
            anyhow::bail!("provider config entry missing id");
        }

        let kind = match cfg.kind.as_deref() {
            Some(k) => k.parse()?,
            None => ProviderKind::Generic,
        };

        let host = match cfg.host {
            Some(h) => h,
            None => match kind {
                ProviderKind::GitHub => "github.com".to_string(),
                ProviderKind::GitLab => "gitlab.com".to_string(),
                ProviderKind::Bitbucket => "bitbucket.org".to_string(),
                ProviderKind::Gitea => {
                    anyhow::bail!("gitea provider '{}' requires a host", id)
                }
                ProviderKind::Generic => {
                    anyhow::bail!("generic provider '{}' requires a host", id)
                }
            },
        };

        validate_provider_host(&id, &host)?;

        if kind == ProviderKind::Generic && cfg.auth_template.is_none() {
            anyhow::bail!(
                "generic provider '{}' requires auth_template (e.g. 'token {{token}}')",
                id
            );
        }

        if let Some(token) = cfg.token {
            self.tokens
                .insert(id.clone(), secrecy::SecretString::new(token.into()));
        }

        self.providers.insert(
            id.clone(),
            ProviderInstance {
                id: ProviderInstanceId::new(id),
                kind,
                host,
                auth_template: cfg.auth_template,
                auth_header_name: cfg.auth_header_name,
            },
        );
        Ok(())
    }

    fn merge_configs(&mut self, configs: Vec<ProviderConfig>) -> Result<()> {
        for cfg in configs {
            self.merge_one(cfg)?;
        }
        Ok(())
    }

    /// The default `github` instance. Always present.
    pub fn default_provider(&self) -> &ProviderInstance {
        self.providers
            .get(DEFAULT_PROVIDER_ID)
            .expect("github default instance is always present")
    }

    /// Look up an instance by id.
    pub fn get(&self, id: &str) -> Option<&ProviderInstance> {
        self.providers.get(id)
    }

    /// Configured passthrough token for an instance, if any.
    pub fn token(&self, id: &str) -> Option<&secrecy::SecretString> {
        self.tokens.get(id)
    }

    /// Iterate over all configured instances.
    pub fn iter(&self) -> impl Iterator<Item = &ProviderInstance> {
        self.providers.values()
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Fully-qualified repository identity.
///
/// `path` is opaque and variable-depth. For the `github` default instance it is
/// exactly `owner/repo`; for other providers it may contain additional slashes
/// (subgroups, `~user/repo`, `+git/repo`, etc.). Callers must NOT split `path`
/// into owner/repo segments except when they know they are dealing with the
/// legacy GitHub shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoId {
    pub provider: ProviderInstanceId,
    pub path: String,
}

impl RepoId {
    /// Create a `RepoId` for the default `github` instance.
    pub fn github(path: impl Into<String>) -> Self {
        Self {
            provider: ProviderInstanceId::new(DEFAULT_PROVIDER_ID),
            path: path.into(),
        }
    }

    /// True when this repo belongs to the built-in `github` default instance.
    pub fn is_github_default(&self) -> bool {
        self.provider.as_str() == DEFAULT_PROVIDER_ID
    }

    /// Storage key used by `RefStore` implementations.
    ///
    /// Back-compat invariant: the `github` default instance renders to the bare
    /// `owner/repo` key used before this refactor, so existing ref-store data
    /// needs no migration. Non-default providers are prefixed with the provider
    /// id and the opaque path is slash-escaped.
    pub fn storage_key(&self) -> String {
        if self.is_github_default() {
            self.path.clone()
        } else {
            format!("{}/{}", self.provider.as_str(), escape_path(&self.path))
        }
    }

    /// Human-readable key for operator-facing config (the webhook allowlist).
    ///
    /// Unlike [`storage_key`](Self::storage_key) this does NOT slash-escape the
    /// path, so it matches what an operator naturally writes. The `github`
    /// default renders to the bare `owner/repo`; other providers are prefixed
    /// with the instance id: `gitlab/group/sub/proj`.
    pub fn natural_key(&self) -> String {
        if self.is_github_default() {
            self.path.clone()
        } else {
            format!("{}/{}", self.provider.as_str(), self.path)
        }
    }

    /// Directory name for the local bare mirror.
    ///
    /// Back-compat invariant: the `github` default instance renders to
    /// `{owner}_{repo}.git`, matching `server.rs` before this refactor.
    /// Non-default providers use `{provider}_{escaped_path}.git`.
    pub fn mirror_dir_name(&self) -> String {
        if self.is_github_default() {
            // The legacy route guarantees exactly one slash for GitHub in Phase
            // 0. Fall back to a safe escaped form if that ever changes.
            match self.path.split_once('/') {
                Some((owner, repo)) => format!("{}_{}.git", owner, repo),
                None => format!("{}_.git", self.path),
            }
        } else {
            format!("{}_{}.git", self.provider.as_str(), escape_path(&self.path))
        }
    }

    /// Convenience accessor for callers that still need the legacy owner/repo
    /// pair (e.g. tests or legacy helpers). Returns `None` for non-default
    /// providers or non-legacy paths.
    pub fn github_owner_repo(&self) -> Option<(&str, &str)> {
        if !self.is_github_default() {
            return None;
        }
        self.path.split_once('/')
    }
}

/// Escape path segments so they are safe to embed in filesystem paths and S3
/// keys without colliding on slash boundaries.
///
/// The escape is minimal and round-trippable:
/// - `%` -> `%25`
/// - `/` -> `%2F`
/// - `\` -> `%5C`
///
/// Because `%` is escaped first, a literal `%2F` in the input becomes
/// `%252F`, which cannot collide with an encoded slash.
fn escape_path(path: &str) -> String {
    path.replace('%', "%25")
        .replace('/', "%2F")
        .replace('\\', "%5C")
}

/// Reverse of [`escape_path`].
fn unescape_path(escaped: &str) -> String {
    let mut out = String::with_capacity(escaped.len());
    let mut chars = escaped.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(a) = chars.next() else {
            out.push(ch);
            break;
        };
        let Some(b) = chars.next() else {
            out.push(ch);
            out.push(a);
            break;
        };
        if let Some(byte) = decode_hex_byte(a, b) {
            out.push(byte as char);
        } else {
            out.push(ch);
            out.push(a);
            out.push(b);
        }
    }
    out
}

fn decode_hex_byte(a: char, b: char) -> Option<u8> {
    fn nibble(c: char) -> Option<u8> {
        match c {
            '0'..='9' => Some(c as u8 - b'0'),
            'a'..='f' => Some(c as u8 - b'a' + 10),
            'A'..='F' => Some(c as u8 - b'A' + 10),
            _ => None,
        }
    }
    Some(nibble(a)? << 4 | nibble(b)?)
}

/// Parse a storage key back into a `RepoId`.
///
/// Used by tools that list the ref store; not required by the hot path. A bare
/// `owner/repo` key is ambiguous with a `{provider}/{path}` key, so the
/// registry is required to decide whether the first segment is a known provider
/// id.
///
/// Storage-key ambiguity resolution: we never infer a provider id from a bare
/// key on the hot path. In listing/GC contexts the registry disambiguates: if
/// the first segment matches a configured provider id, it is treated as
/// `{provider}/{path}`; otherwise the whole key is a GitHub `owner/repo` path.
/// Because GitHub is the default, a GitHub org literally named "gitlab" would
/// be parsed as a GitLab provider path when a `gitlab` instance is registered.
/// That collision is accepted and documented: operators who create provider
/// ids that shadow GitHub org names must avoid legacy addressing for those
/// orgs (use explicit `github/...` routes once they exist) or pick a different
/// instance id.
pub fn parse_storage_key(key: &str, registry: &ProviderRegistry) -> Option<RepoId> {
    if let Some((provider, rest)) = key.split_once('/')
        && registry.get(provider).is_some()
    {
        Some(RepoId {
            provider: ProviderInstanceId::new(provider),
            path: unescape_path(rest),
        })
    } else {
        // Bare key -> github default.
        Some(RepoId::github(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(kind: ProviderKind, host: &str, tmpl: Option<&str>) -> ProviderInstance {
        ProviderInstance {
            id: ProviderInstanceId::new("test"),
            kind,
            host: host.to_string(),
            auth_template: tmpl.map(|s| s.to_string()),
            auth_header_name: None,
        }
    }

    #[test]
    fn auth_header_rejects_crlf_injection() {
        // A token with CR/LF must not be emitted as a header value (AU5).
        let gh = provider(ProviderKind::GitHub, "github.com", None);
        // Base64 of the preset Basic value can never contain CR/LF, so a CRLF
        // token is neutralized there regardless — but a generic template feeds
        // the token straight in, which is the real injection vector:
        let generic = provider(ProviderKind::Gitea, "gitea.example.com", None);
        assert!(generic.auth_header("good-token").is_some());
        assert!(
            generic.auth_header("evil\r\nX-Injected: 1").is_none(),
            "CR/LF token must be rejected"
        );
        assert!(generic.auth_header("evil\nX: 1").is_none());
        // A template with embedded CR/LF is likewise refused.
        let tmpl = provider(ProviderKind::Generic, "h", Some("token {token}\r\nEvil: 1"));
        assert!(tmpl.auth_header("t").is_none());
        // Sanity: the normal GitHub path still works.
        assert!(gh.auth_header("ghs_token").is_some());
    }

    #[test]
    fn validate_provider_host_blocks_internal_targets() {
        // Legit hosts pass — incl. on-prem LAN, loopback/same-box self-host and
        // local test origins, and a scheme prefix.
        for ok in [
            "github.com",
            "gitlab.com",
            "https://gitea.example.com",
            "git.internal.example.com",
            "192.168.1.10",
            "10.0.0.5:3000",
            "localhost",
            "http://127.0.0.1:8080",
            "[::1]:443",
        ] {
            assert!(validate_provider_host("p", ok).is_ok(), "should allow {ok}");
        }
        // The never-legitimate internal targets are rejected.
        for bad in [
            "169.254.169.254",       // cloud metadata endpoint
            "https://169.254.0.1",   // link-local
            "0.0.0.0",               // unspecified
            "https://user@evil.com", // userinfo
            "host with space",
            "",
        ] {
            assert!(
                validate_provider_host("p", bad).is_err(),
                "should reject {bad}"
            );
        }
    }

    #[test]
    fn github_default_storage_key_is_legacy_owner_repo() {
        let repo = RepoId::github("ripclone/test");
        assert_eq!(repo.storage_key(), "ripclone/test");
        assert_eq!(repo.mirror_dir_name(), "ripclone_test.git");
    }

    #[test]
    fn natural_key_is_unescaped_and_provider_prefixed() {
        // github default: bare owner/repo (same as storage_key).
        assert_eq!(RepoId::github("acme/widget").natural_key(), "acme/widget");
        // non-github: provider-prefixed, NOT slash-escaped (unlike storage_key).
        let gl = RepoId {
            provider: ProviderInstanceId::new("gitlab"),
            path: "group/sub/proj".to_string(),
        };
        assert_eq!(gl.natural_key(), "gitlab/group/sub/proj");
        assert_eq!(gl.storage_key(), "gitlab/group%2Fsub%2Fproj");
    }

    #[test]
    fn gitlab_subgroup_path_is_escaped_and_prefixed() {
        let repo = RepoId {
            provider: ProviderInstanceId::new("gitlab"),
            path: "g/sub/proj".to_string(),
        };
        assert_eq!(repo.storage_key(), "gitlab/g%2Fsub%2Fproj");
        assert_eq!(repo.mirror_dir_name(), "gitlab_g%2Fsub%2Fproj.git");
    }

    #[test]
    fn sourcehut_user_path_is_escaped_and_prefixed() {
        let repo = RepoId {
            provider: ProviderInstanceId::new("sourcehut"),
            path: "~user/repo".to_string(),
        };
        assert_eq!(repo.storage_key(), "sourcehut/~user%2Frepo");
        assert_eq!(repo.mirror_dir_name(), "sourcehut_~user%2Frepo.git");
    }

    #[test]
    fn launchpad_git_path_is_escaped_and_prefixed() {
        let repo = RepoId {
            provider: ProviderInstanceId::new("launchpad"),
            path: "~owner/project/+git/repo".to_string(),
        };
        // `+` does not need escaping for collision freedom; only `/`, `\`, `%`
        // are escaped.
        assert_eq!(
            repo.storage_key(),
            "launchpad/~owner%2Fproject%2F+git%2Frepo"
        );
        assert_eq!(
            repo.mirror_dir_name(),
            "launchpad_~owner%2Fproject%2F+git%2Frepo.git"
        );
    }

    #[test]
    fn escape_is_collision_free_around_encoded_slash() {
        // "a/b" and "a%2Fb" must not collide after escaping.
        let plain = RepoId {
            provider: ProviderInstanceId::new("gitea"),
            path: "a/b".to_string(),
        };
        let encoded = RepoId {
            provider: ProviderInstanceId::new("gitea"),
            path: "a%2Fb".to_string(),
        };
        assert_ne!(plain.storage_key(), encoded.storage_key());
        assert_ne!(plain.mirror_dir_name(), encoded.mirror_dir_name());
    }

    #[test]
    fn storage_key_round_trips() {
        let mut registry = ProviderRegistry::new();
        registry.providers.insert(
            "gitlab".to_string(),
            ProviderInstance {
                id: ProviderInstanceId::new("gitlab"),
                kind: ProviderKind::GitLab,
                host: "gitlab.com".to_string(),
                auth_template: None,
                auth_header_name: None,
            },
        );
        registry.providers.insert(
            "sourcehut".to_string(),
            ProviderInstance {
                id: ProviderInstanceId::new("sourcehut"),
                kind: ProviderKind::Generic,
                host: "git.sr.ht".to_string(),
                auth_template: Some("token {token}".to_string()),
                auth_header_name: None,
            },
        );
        registry.providers.insert(
            "gitea".to_string(),
            ProviderInstance {
                id: ProviderInstanceId::new("gitea"),
                kind: ProviderKind::Gitea,
                host: "gitea.example.com".to_string(),
                auth_template: None,
                auth_header_name: None,
            },
        );

        let cases = vec![
            RepoId::github("owner/repo"),
            RepoId {
                provider: ProviderInstanceId::new("gitlab"),
                path: "g/sub/proj".to_string(),
            },
            RepoId {
                provider: ProviderInstanceId::new("sourcehut"),
                path: "~user/repo".to_string(),
            },
            RepoId {
                provider: ProviderInstanceId::new("gitea"),
                path: "a%2Fb".to_string(),
            },
        ];
        for repo in cases {
            let key = repo.storage_key();
            let parsed = parse_storage_key(&key, &registry).expect("round-trippable key");
            assert_eq!(parsed, repo, "round-trip failed for {key}");
        }
    }

    #[test]
    fn github_auth_header_is_basic_x_access_token() {
        let github = ProviderRegistry::new().default_provider().clone();
        let (name, value) = github.auth_header("pat123").unwrap();
        assert_eq!(name, "Authorization");
        let expected = format!(
            "Basic {}",
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b"x-access-token:pat123",
            )
        );
        assert_eq!(value, expected);
    }

    #[test]
    fn gitlab_auth_header_is_basic_oauth2() {
        let gitlab = ProviderInstance {
            id: ProviderInstanceId::new("gitlab"),
            kind: ProviderKind::GitLab,
            host: "gitlab.com".to_string(),
            auth_template: None,
            auth_header_name: None,
        };
        let (name, value) = gitlab.auth_header("gltok").unwrap();
        assert_eq!(name, "Authorization");
        let expected = format!(
            "Basic {}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"oauth2:gltok",)
        );
        assert_eq!(value, expected);
    }

    #[test]
    fn bitbucket_auth_header_is_bearer() {
        let bb = ProviderInstance {
            id: ProviderInstanceId::new("bb"),
            kind: ProviderKind::Bitbucket,
            host: "bitbucket.org".to_string(),
            auth_template: None,
            auth_header_name: None,
        };
        let (name, value) = bb.auth_header("bbtok").unwrap();
        assert_eq!(name, "Authorization");
        assert_eq!(value, "Bearer bbtok");
    }

    #[test]
    fn gitea_auth_header_is_token() {
        let gitea = ProviderInstance {
            id: ProviderInstanceId::new("gitea"),
            kind: ProviderKind::Gitea,
            host: "gitea.example.com".to_string(),
            auth_template: None,
            auth_header_name: None,
        };
        let (name, value) = gitea.auth_header("gtok").unwrap();
        assert_eq!(name, "Authorization");
        assert_eq!(value, "token gtok");
    }

    #[test]
    fn generic_auth_template_overrides_preset() {
        let generic = ProviderInstance {
            id: ProviderInstanceId::new("myhost"),
            kind: ProviderKind::Generic,
            host: "git.example.com".to_string(),
            auth_template: Some("X-Custom {token}".to_string()),
            auth_header_name: None,
        };
        let (name, value) = generic.auth_header("sekrit").unwrap();
        assert_eq!(name, "Authorization");
        assert_eq!(value, "X-Custom sekrit");
    }

    #[test]
    fn custom_auth_header_name_overrides_authorization() {
        let p = ProviderInstance {
            id: ProviderInstanceId::new("gl"),
            kind: ProviderKind::Generic,
            host: "git.example.com".to_string(),
            auth_template: Some("{token}".to_string()),
            auth_header_name: Some("PRIVATE-TOKEN".to_string()),
        };
        let (name, value) = p.auth_header("sekrit").unwrap();
        assert_eq!(name, "PRIVATE-TOKEN");
        assert_eq!(value, "sekrit");
    }

    #[test]
    fn custom_auth_header_name_applies_to_preset() {
        let p = ProviderInstance {
            id: ProviderInstanceId::new("gh"),
            kind: ProviderKind::GitHub,
            host: "github.com".to_string(),
            auth_template: None,
            auth_header_name: Some("X-Proxy-Auth".to_string()),
        };
        let (name, _value) = p.auth_header("pat").unwrap();
        assert_eq!(name, "X-Proxy-Auth");
    }

    #[test]
    fn malformed_auth_header_name_is_rejected() {
        for bad in ["bad name", "has:colon", "line\r\nfeed", ""] {
            let p = ProviderInstance {
                id: ProviderInstanceId::new("x"),
                kind: ProviderKind::GitHub,
                host: "github.com".to_string(),
                auth_template: None,
                auth_header_name: Some(bad.to_string()),
            };
            assert!(
                p.auth_header("pat").is_none(),
                "header name {bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn github_clone_url_is_clean() {
        let github = ProviderRegistry::new().default_provider().clone();
        assert_eq!(
            github.clone_url("owner/repo"),
            "https://github.com/owner/repo.git"
        );
    }

    #[test]
    fn generic_clone_url_preserves_http_scheme() {
        let generic = ProviderInstance {
            id: ProviderInstanceId::new("local"),
            kind: ProviderKind::Generic,
            host: "http://127.0.0.1:8080".to_string(),
            auth_template: Some("token {token}".to_string()),
            auth_header_name: None,
        };
        assert_eq!(
            generic.clone_url("acme/http"),
            "http://127.0.0.1:8080/acme/http.git"
        );
    }

    #[test]
    fn registry_loads_github_token_from_env() {
        // Ensure RIPCLONE_GITHUB_TOKEN is not leaking from the environment.
        // We can't assert the token is present because tests may run without it,
        // but we can assert the registry structure is valid.
        let registry = ProviderRegistry::new();
        let github = registry.default_provider();
        assert_eq!(github.id.as_str(), "github");
        assert_eq!(github.kind, ProviderKind::GitHub);
        assert_eq!(github.host, "github.com");
    }
}
