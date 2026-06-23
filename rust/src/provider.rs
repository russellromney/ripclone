//! Provider / identity abstraction for multi-provider auth (Phase 0).
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
//! Phase 1/2 handoff notes:
//! - Add per-provider `clone_url(path)` and `auth_header(token)` methods to
//!   `ProviderInstance` (and a `ProviderKind` enum) when credential-header
//!   injection replaces the URL userinfo form.
//! - Load extra provider instances from config in `ProviderRegistry::load`.
//! - Add wildcard routes `/v1/repos/{provider}/{*path}/...` and map legacy
//!   2-segment routes to the `github` default instance.
//! - Loosen `validation::validate_repo_id` to a per-provider charset check once
//!   opaque paths can contain `/`, `~`, `+`, etc.

use std::collections::HashMap;

/// Built-in default instance id. All legacy `{owner}/{repo}` routes resolve to
/// this instance in Phase 0.
const DEFAULT_PROVIDER_ID: &str = "github";

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
/// Phase 0 only needs `host`; later phases will add `kind`, `credential`,
/// `oidc`, and capability methods here.
#[derive(Debug, Clone)]
pub struct ProviderInstance {
    pub id: ProviderInstanceId,
    pub host: String,
}

/// Registry of configured provider instances.
///
/// Phase 0 hardcodes the `github` default. Phase 2 will load instances from
/// config (file or `RIPCLONE_PROVIDERS` JSON) and merge them with presets.
#[derive(Debug, Clone)]
pub struct ProviderRegistry {
    providers: HashMap<String, ProviderInstance>,
}

impl ProviderRegistry {
    /// Build a registry containing only the built-in GitHub default instance.
    pub fn new() -> Self {
        let mut providers = HashMap::new();
        providers.insert(
            DEFAULT_PROVIDER_ID.to_string(),
            ProviderInstance {
                id: ProviderInstanceId::new(DEFAULT_PROVIDER_ID),
                host: "github.com".to_string(),
            },
        );
        Self { providers }
    }

    /// Load from configuration.
    ///
    /// TODO(phase2): implement config-driven provider loading. For now this is
    /// equivalent to `ProviderRegistry::new()`.
    pub fn load() -> Self {
        Self::new()
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
    fn is_github_default(&self) -> bool {
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
    /// pair (e.g. `git.rs::sync_bare_mirror` in Phase 0). Returns `None` for
    /// non-default providers or non-legacy paths.
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

    #[test]
    fn github_default_storage_key_is_legacy_owner_repo() {
        let repo = RepoId::github("ripclone/test");
        assert_eq!(repo.storage_key(), "ripclone/test");
        assert_eq!(repo.mirror_dir_name(), "ripclone_test.git");
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
                host: "gitlab.com".to_string(),
            },
        );
        registry.providers.insert(
            "sourcehut".to_string(),
            ProviderInstance {
                id: ProviderInstanceId::new("sourcehut"),
                host: "git.sr.ht".to_string(),
            },
        );
        registry.providers.insert(
            "gitea".to_string(),
            ProviderInstance {
                id: ProviderInstanceId::new("gitea"),
                host: "gitea.example.com".to_string(),
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
    fn registry_has_github_default() {
        let registry = ProviderRegistry::new();
        let github = registry.default_provider();
        assert_eq!(github.id.as_str(), "github");
        assert_eq!(github.host, "github.com");
        assert!(registry.get("github").is_some());
        assert!(registry.get("gitlab").is_none());
    }
}
