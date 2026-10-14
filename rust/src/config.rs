//! Unified ripclone configuration.
//!
//! Supports a global TOML file at `~/.config/ripclone/config.toml` and an
//! optional project-level `ripclone.toml` discovered by walking up from the
//! current directory. Environment variables and CLI flags still take precedence.
//!
//! Provider tokens may be declared here for self-host/server-side syncs. The
//! CLI's saved server token is stored separately in the ripclone token file.

use crate::provider::ProviderConfig;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

/// Top-level ripclone configuration.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Ripclone server URL.
    pub server: Option<String>,
    /// Default provider instance id (e.g. "github", "my-gitea").
    pub default_provider: Option<String>,
    /// Agent-fleet mode default. When true (or `RIPCLONE_AGENT` is set in the
    /// environment), clones use fleet-sane defaults: depth-1 history and no
    /// interactive prompts. The env var wins over this. Deliberately explicit —
    /// never a silent size-based switch, which would surprise humans.
    pub agent: Option<bool>,
    /// Default clone options.
    pub clone: CloneConfig,
    /// Custom/self-hosted provider declarations. Built-in presets (github and
    /// gitlab) are implicit and do not need to be declared.
    pub providers: HashMap<String, ProviderEntry>,
    /// Server-side artifact storage backend (`[storage]`).
    pub storage: StorageConfig,
    /// Server-owned SQLite control database (`[control]`).
    pub control: ControlConfig,
    /// Captured solely so removed control sections can fail closed before any
    /// runtime side effect. They are never serialized or interpreted.
    #[serde(rename = "metadata", skip_serializing)]
    pub(crate) removed_metadata: Option<toml::Value>,
    #[serde(rename = "queue", skip_serializing)]
    pub(crate) removed_queue: Option<toml::Value>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CloneConfig {
    pub depth: Option<usize>,
    pub mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderEntry {
    pub kind: String,
    pub host: Option<String>,
    pub token: Option<String>,
    pub auth_template: Option<String>,
    /// Optional header name for the credential. Defaults to `Authorization`.
    pub auth_header_name: Option<String>,
}

/// Server-side artifact storage. These are read only by `ripclone-server`; the
/// matching `RIPCLONE_S3_*` env vars always override them.
/// Credentials (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) are read from the
/// environment only — never from this file.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// `local` | `s3`. Unset auto-detects: S3 when an endpoint is configured,
    /// else local.
    pub backend: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
}

/// Server-owned SQLite control database at `path`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ControlConfig {
    pub path: Option<String>,
}

/// Path to the global config file (`~/.config/ripclone/config.toml`).
pub fn global_config_path() -> Option<PathBuf> {
    global_config_path_from(
        std::env::var_os("RIPCLONE_CONFIG"),
        std::env::var_os("HOME"),
    )
}

/// [`global_config_path`] from the `RIPCLONE_CONFIG` and `HOME` values.
fn global_config_path_from(
    explicit: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> Option<PathBuf> {
    // An explicit override wins, so a daemon/container can point at a fixed file
    // (e.g. /etc/ripclone/config.toml) instead of depending on $HOME.
    if let Some(p) = explicit.filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(p));
    }
    home.map(|home| {
        let mut p = PathBuf::from(home);
        p.push(".config");
        p.push("ripclone");
        p.push("config.toml");
        p
    })
}

/// Discover a project-level `ripclone.toml` by walking up from `start`.
pub fn project_config_path(start: &Path) -> Option<PathBuf> {
    let mut dir = if start.is_dir() {
        Some(start)
    } else {
        start.parent()
    };
    while let Some(d) = dir {
        let candidate = d.join("ripclone.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
        dir = d.parent();
    }
    None
}

/// Resolve whether agent-fleet mode is active.
///
/// `RIPCLONE_AGENT` in the environment wins: a truthy value turns it on, and an
/// explicit falsey value (`0`/`false`/`no`/`off`) turns it off even when the
/// config default is on. An empty or unset var falls back to `config_default`,
/// then to off. This is intentionally an explicit opt-in, never inferred from
/// repo size — a silent switch would surprise humans on giant repos.
pub fn agent_mode(config_default: Option<bool>) -> bool {
    agent_mode_from(
        std::env::var("RIPCLONE_AGENT").ok().as_deref(),
        config_default,
    )
}

/// [`agent_mode`] from the `RIPCLONE_AGENT` value.
fn agent_mode_from(env: Option<&str>, config_default: Option<bool>) -> bool {
    match env.filter(|v| !v.trim().is_empty()) {
        Some(v) => is_truthy(v),
        None => config_default.unwrap_or(false),
    }
}

fn is_truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Load the merged configuration: project `ripclone.toml` overrides global
/// `config.toml`, which overrides built-in defaults.
pub fn load() -> Config {
    let global = load_global();
    let project = std::env::current_dir()
        .ok()
        .and_then(|cwd| project_config_path(&cwd))
        .map(|path| load_from(&path))
        .unwrap_or_default();
    merge(project, global)
}

/// Load the global TOML configuration.
pub fn load_global() -> Config {
    match global_config_path() {
        Some(path) if path.exists() => load_from(&path),
        _ => Config::default(),
    }
}

fn load_from(path: &Path) -> Config {
    try_load_from(path).unwrap_or_else(|e| {
        eprintln!("error: {e:#}");
        eprintln!(
            "fix the TOML in {}, or point RIPCLONE_CONFIG at a valid file",
            path.display()
        );
        std::process::exit(2);
    })
}

fn try_load_from(path: &Path) -> Result<Config> {
    let data = match std::fs::read_to_string(path) {
        Ok(data) => data,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Config::default()),
        Err(e) => return Err(e).with_context(|| format!("read config {}", path.display())),
    };
    toml::from_str(&data).with_context(|| format!("parse config {}", path.display()))
}

/// Save the global configuration. The saved server token is not written; use
/// the token store for it.
pub fn save(config: &Config) -> Result<()> {
    let path = global_config_path().context("no HOME for config path")?;
    save_to(&path, config)
}

fn save_to(path: &Path, config: &Config) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).context("create config dir")?;
    }
    let data = toml::to_string_pretty(config).context("serialize config")?;
    crate::secure_file::with_file_lock(path, || {
        crate::secure_file::write_0600_atomic(path, data.as_bytes())
            .with_context(|| format!("write {}", path.display()))
    })
}

/// Merge two configs: `overrides` wins over `base`.
impl Config {
    /// Convert declared provider entries into the internal `ProviderConfig` shape.
    pub fn provider_configs(&self) -> Vec<ProviderConfig> {
        self.providers
            .iter()
            .map(|(id, entry)| ProviderConfig {
                id: id.clone(),
                kind: Some(entry.kind.clone()),
                host: entry.host.clone(),
                token: entry.token.clone(),
                auth_template: entry.auth_template.clone(),
                auth_header_name: entry.auth_header_name.clone(),
            })
            .collect()
    }
}

fn merge(overrides: Config, base: Config) -> Config {
    Config {
        server: overrides.server.or(base.server),
        default_provider: overrides.default_provider.or(base.default_provider),
        agent: overrides.agent.or(base.agent),
        clone: CloneConfig {
            depth: overrides.clone.depth.or(base.clone.depth),
            mode: overrides.clone.mode.or(base.clone.mode),
        },
        providers: {
            let mut merged = base.providers;
            merged.extend(overrides.providers);
            merged
        },
        storage: StorageConfig {
            backend: overrides.storage.backend.or(base.storage.backend),
            endpoint: overrides.storage.endpoint.or(base.storage.endpoint),
            region: overrides.storage.region.or(base.storage.region),
            bucket: overrides.storage.bucket.or(base.storage.bucket),
            prefix: overrides.storage.prefix.or(base.storage.prefix),
        },
        control: ControlConfig {
            path: overrides.control.path.or(base.control.path),
        },
        removed_metadata: overrides.removed_metadata.or(base.removed_metadata),
        removed_queue: overrides.removed_queue.or(base.removed_queue),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_config_discovered_by_walking_up() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("b");
        std::fs::create_dir_all(&nested).unwrap();
        let cfg = dir.path().join("ripclone.toml");
        std::fs::write(&cfg, "server = \"https://example.com\"\n").unwrap();

        assert_eq!(project_config_path(&nested), Some(cfg));
    }

    #[test]
    fn merge_prefers_project_over_global() {
        let global = Config {
            server: Some("https://global.example.com".into()),
            default_provider: Some("github".into()),
            clone: CloneConfig {
                depth: Some(1),
                mode: Some("editable".into()),
            },
            providers: HashMap::new(),
            ..Default::default()
        };
        let project = Config {
            server: Some("https://project.example.com".into()),
            default_provider: None,
            clone: CloneConfig {
                depth: Some(10),
                mode: None,
            },
            providers: HashMap::new(),
            ..Default::default()
        };
        let merged = merge(project, global);
        assert_eq!(
            merged.server.as_deref(),
            Some("https://project.example.com")
        );
        assert_eq!(merged.default_provider.as_deref(), Some("github"));
        assert_eq!(merged.clone.depth, Some(10));
        assert_eq!(merged.clone.mode.as_deref(), Some("editable"));
    }

    #[test]
    fn missing_file_is_default_and_malformed_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let cfg = try_load_from(&path).unwrap();
        assert!(cfg.server.is_none());
        assert!(cfg.default_provider.is_none());

        std::fs::write(&path, "server = [").unwrap();
        assert!(try_load_from(&path).is_err());
    }

    #[test]
    fn agent_mode_env_wins_over_config() {
        // (RIPCLONE_AGENT, config default, active)
        for (env, config, want) in [
            (Some("1"), None, true),
            (Some("1"), Some(false), true),
            (Some("TRUE"), Some(false), true),
            (Some("0"), Some(true), false),
            (Some("off"), Some(true), false),
            (Some(""), Some(true), true),
            (Some("  "), Some(false), false),
            (Some(""), None, false),
            (None, Some(true), true),
            (None, None, false),
        ] {
            assert_eq!(agent_mode_from(env, config), want, "{env:?} {config:?}");
        }
    }

    #[test]
    fn global_config_path_prefers_ripclone_config_over_home() {
        let os = |v: &str| Some(std::ffi::OsString::from(v));
        for (explicit, home, want) in [
            (
                os("/etc/ripclone/config.toml"),
                os("/home/u"),
                Some("/etc/ripclone/config.toml"),
            ),
            (
                os(""),
                os("/home/u"),
                Some("/home/u/.config/ripclone/config.toml"),
            ),
            (
                None,
                os("/home/u"),
                Some("/home/u/.config/ripclone/config.toml"),
            ),
            (None, None, None),
        ] {
            assert_eq!(
                global_config_path_from(explicit, home),
                want.map(PathBuf::from)
            );
        }
    }
}
