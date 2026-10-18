//! Provider / identity abstraction for multi-provider auth (Phases 0–2).
//!
//! This module introduces the addressing seam that lets ripclone move from the
//! hard-coded GitHub `owner/repo` pair to arbitrary git hosts (GitLab
//! subgroups, sourcehut `~user/repo`, Launchpad `+git` paths, self-hosted
//! Gitea/Forgejo, etc.).
//!
//! Provider-qualified routes identify a configured provider instance; the CLI
//! accepts bare `owner/repo` input for its configured default provider.
//!
//! Phase 3+ handoff notes:
//! - Add per-provider id charset validation rules once opaque paths can contain
//!   `/`, `~`, `+`, etc. (currently `validation::validate_repo_id` is still the
//!   GitHub-only check).

use anyhow::Result;
use std::collections::HashMap;
use std::net::IpAddr;

/// Built-in default instance id used for unqualified CLI input.
const DEFAULT_PROVIDER_ID: &str = "github";

/// Supported git host kinds. `Gitea` covers Forgejo/Codeberg; `Generic` is a
/// config-only host that uses an explicit credential template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    GitHub,
    GitLab,
    Gitea,
    Generic,
}

impl ProviderKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderKind::GitHub => "github",
            ProviderKind::GitLab => "gitlab",
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
/// `auth_template` is optional for preset kinds (GitHub/GitLab/Gitea) and
/// required for `Generic`. When present it must contain exactly one `{token}`
/// placeholder; it overrides the preset's default header.
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
    /// The GitHub default instance renders `https://github.com/{owner}/{repo}.git`.
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

/// Raw configuration for one provider instance, from the config file.
#[derive(Debug, Clone, Default)]
pub struct ProviderConfig {
    pub id: String,
    /// Preset kind. Defaults to `"generic"`.
    pub kind: Option<String>,
    /// Hostname used in clone URLs, e.g. `"gitlab.com"` or `"gitea.example.com"`.
    pub host: Option<String>,
    /// GitHub App id, for a `github` kind provider whose server reads
    /// repositories as that app. Not a secret: the key is in the environment.
    pub app_id: Option<u64>,
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
    /// GitHub App ids by instance id.
    app_ids: HashMap<String, u64>,
}

impl ProviderRegistry {
    /// Build a registry containing only the built-in GitHub default instance.
    pub fn new() -> Self {
        let mut providers = HashMap::new();
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
        Self {
            providers,
            app_ids: HashMap::new(),
        }
    }

    /// The built-in `github` instance plus the config file's `[providers]`
    /// entries. An entry named `github` overrides the built-in one.
    pub fn from_config(config: &crate::config::Config) -> Result<Self> {
        let mut registry = Self::new();
        for entry in config.provider_configs() {
            registry.merge_one(entry)?;
        }
        Ok(registry)
    }

    /// [`Self::from_config`] of the config file.
    pub fn load() -> Result<Self> {
        Self::from_config(&crate::config::load())
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

        match cfg.app_id {
            Some(_) if kind != ProviderKind::GitHub => {
                anyhow::bail!("provider '{id}' has app_id, but only github providers take one")
            }
            Some(app_id) => {
                self.app_ids.insert(id.clone(), app_id);
            }
            None => {
                self.app_ids.remove(&id);
            }
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

    /// Look up an instance by id.
    pub fn get(&self, id: &str) -> Option<&ProviderInstance> {
        self.providers.get(id)
    }

    /// The GitHub App id configured for an instance, if any.
    pub fn app_id(&self, id: &str) -> Option<u64> {
        self.app_ids.get(id).copied()
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
/// into owner/repo segments except for the built-in GitHub provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

    /// Storage key: how the control store and logs name the repository.
    ///
    /// All providers use `{provider}/{escaped_path}`.
    pub fn storage_key(&self) -> String {
        format!("{}/{}", self.provider.as_str(), escape_path(&self.path))
    }

    /// Inverse of [`storage_key`](Self::storage_key). The escaped path never
    /// contains `/`, so the last `/` separates provider from path.
    pub fn from_storage_key(key: &str) -> Result<Self> {
        let invalid = || anyhow::anyhow!("invalid repository storage key {key:?}");
        let (provider, escaped) = key.rsplit_once('/').ok_or_else(invalid)?;
        let mut path = String::with_capacity(escaped.len());
        let mut rest = escaped;
        while let Some(index) = rest.find('%') {
            path.push_str(&rest[..index]);
            path.push(match rest.get(index + 1..index + 3) {
                Some("25") => '%',
                Some("2F") => '/',
                Some("5C") => '\\',
                _ => return Err(invalid()),
            });
            rest = &rest[index + 3..];
        }
        path.push_str(rest);
        if provider.is_empty() || path.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            provider: ProviderInstanceId::new(provider),
            path,
        })
    }

    /// Repository name accepted by the CLI for the selected default provider:
    /// bare `path` for it, `provider:path` for any other.
    pub fn cli_name(&self, default_provider: &str) -> String {
        if self.provider.as_str() == default_provider {
            self.path.clone()
        } else {
            format!("{}:{}", self.provider.as_str(), self.path)
        }
    }

    /// Directory name for the local bare mirror.
    ///
    /// All providers use `{provider}_{escaped_path}.git`.
    pub fn mirror_dir_name(&self) -> String {
        format!("{}_{}.git", self.provider.as_str(), escape_path(&self.path))
    }

    /// Return the owner/repo pair for the built-in GitHub provider. Returns
    /// `None` for other providers or paths without exactly two components.
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

    /// Storage keys and mirror dirs escape the path and carry the provider.
    #[test]
    fn repo_id_keys() {
        let id = |provider: &str, path: &str| RepoId {
            provider: ProviderInstanceId::new(provider),
            path: path.to_string(),
        };
        // (repo, storage key, mirror dir)
        let rows = [
            (
                RepoId::github("ripclone/test"),
                "github/ripclone%2Ftest",
                "github_ripclone%2Ftest.git",
            ),
            (
                id("gitlab", "g/sub/proj"),
                "gitlab/g%2Fsub%2Fproj",
                "gitlab_g%2Fsub%2Fproj.git",
            ),
            (
                id("sourcehut", "~user/repo"),
                "sourcehut/~user%2Frepo",
                "sourcehut_~user%2Frepo.git",
            ),
            (
                id("launchpad", "~owner/project/+git/repo"),
                "launchpad/~owner%2Fproject%2F+git%2Frepo",
                "launchpad_~owner%2Fproject%2F+git%2Frepo.git",
            ),
            (id("gitea", "a/b"), "gitea/a%2Fb", "gitea_a%2Fb.git"),
            (id("gitea", "a%2Fb"), "gitea/a%252Fb", "gitea_a%252Fb.git"),
            (id("gitea", "a\\b"), "gitea/a%5Cb", "gitea_a%5Cb.git"),
        ];
        for (repo, storage, mirror) in &rows {
            assert_eq!(repo.storage_key(), *storage);
            assert_eq!(repo.mirror_dir_name(), *mirror);
            assert_eq!(&RepoId::from_storage_key(storage).unwrap(), repo);
        }
        for bad in ["noslash", "/path", "github/", "github/a%2", "github/a%41"] {
            assert!(RepoId::from_storage_key(bad).is_err(), "{bad}");
        }
        let mut keys: Vec<String> = rows.iter().map(|r| r.0.storage_key()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), rows.len(), "two repos share a storage key");
    }

    #[test]
    fn cli_name_uses_colon_only_outside_selected_default_provider() {
        let github = RepoId::github("acme/widget");
        let gitlab = RepoId {
            provider: ProviderInstanceId::new("gitlab"),
            path: "group/sub/proj".to_string(),
        };
        for (repo, default, want) in [
            (&github, "github", "acme/widget"),
            (&github, "gitlab", "github:acme/widget"),
            (&gitlab, "github", "gitlab:group/sub/proj"),
            (&gitlab, "gitlab", "group/sub/proj"),
        ] {
            assert_eq!(repo.cli_name(default), want);
        }
    }

    #[test]
    fn registry_is_github_plus_config_entries() {
        use crate::config::{Config, ProviderEntry};
        let entry = |kind: &str, host: &str, app_id: Option<u64>| ProviderEntry {
            kind: kind.into(),
            host: Some(host.into()),
            app_id,
            auth_template: None,
            auth_header_name: None,
        };
        let config = Config {
            providers: HashMap::from([
                (
                    "my-gitea".to_string(),
                    entry("gitea", "https://gitea.example.com", None),
                ),
                (
                    "github".to_string(),
                    entry("github", "github.com", Some(42)),
                ),
            ]),
            ..Config::default()
        };
        let registry = ProviderRegistry::from_config(&config).unwrap();
        for (id, host, app_id) in [
            ("github", "github.com", Some(42)),
            ("my-gitea", "https://gitea.example.com", None),
        ] {
            assert_eq!(
                registry.get(id).map(|p| p.host.as_str()),
                Some(host),
                "{id}"
            );
            assert_eq!(registry.app_id(id), app_id, "{id}");
        }
        assert_eq!(registry.iter().count(), 2);

        // Only a github provider reads as an app.
        let config = Config {
            providers: HashMap::from([(
                "my-gitea".to_string(),
                entry("gitea", "https://gitea.example.com", Some(42)),
            )]),
            ..Config::default()
        };
        let error = ProviderRegistry::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("only github"), "{error}");
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
}
