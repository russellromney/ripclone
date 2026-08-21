//! Unified ripclone configuration.
//!
//! Supports a global TOML file at `~/.config/ripclone/config.toml` and an
//! optional project-level `ripclone.toml` discovered by walking up from the
//! current directory. Environment variables and CLI flags still take precedence.
//!
//! Provider tokens may be declared here for self-host/server-side syncs. CLI
//! session tokens are stored separately in the ripclone token file.

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

/// Server-side artifact storage. These are read only by `ripclone-server` /
/// `ripclone-worker`; the matching `RIPCLONE_S3_*` env vars always override them.
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
    pub cache_dir: Option<String>,
}

/// Server-owned SQLite control database. A Turso URL and token together enable
/// the embedded-replica mode; otherwise `path` is plain local SQLite.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ControlConfig {
    pub path: Option<String>,
    pub turso_url: Option<String>,
    pub turso_token: Option<String>,
    /// Ordered worker size classes. Empty uses `small | large`; the
    /// `RIPCLONE_SIZE_CLASSES` environment override remains supported.
    pub size_classes: Vec<crate::queue::SizeClass>,
}

/// Path to the global config file (`~/.config/ripclone/config.toml`).
pub fn global_config_path() -> Option<PathBuf> {
    // An explicit override wins, so a daemon/container can point at a fixed file
    // (e.g. /etc/ripclone/config.toml) instead of depending on $HOME.
    if let Some(p) = std::env::var_os("RIPCLONE_CONFIG").filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(p));
    }
    std::env::var_os("HOME").map(|home| {
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
    match std::env::var("RIPCLONE_AGENT")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        Some(v) => is_truthy(&v),
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

/// Save the global configuration. CLI session tokens are not written; use the
/// token store for those.
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
            cache_dir: overrides.storage.cache_dir.or(base.storage.cache_dir),
        },
        control: ControlConfig {
            path: overrides.control.path.or(base.control.path),
            turso_url: overrides.control.turso_url.or(base.control.turso_url),
            turso_token: overrides.control.turso_token.or(base.control.turso_token),
            size_classes: if overrides.control.size_classes.is_empty() {
                base.control.size_classes
            } else {
                overrides.control.size_classes
            },
        },
        removed_metadata: overrides.removed_metadata.or(base.removed_metadata),
        removed_queue: overrides.removed_queue.or(base.removed_queue),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Process environment mutations must not race under parallel tests.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
    fn size_classes_parse_from_toml_array() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[control]
path = "/tmp/control.db"

[[control.size_classes]]
name = "small"
max_bytes = 1000
machine = "s"

[[control.size_classes]]
name = "medium"
max_bytes = 5000
machine = "m"

[[control.size_classes]]
name = "large"
max_bytes = 18446744073709551615
machine = "l"
"#,
        )
        .unwrap();
        let cfg = try_load_from(&path).unwrap();
        assert_eq!(cfg.control.size_classes.len(), 3);
        assert_eq!(cfg.control.size_classes[0].name, "small");
        assert_eq!(cfg.control.size_classes[0].max_bytes, 1000);
        assert_eq!(cfg.control.size_classes[1].name, "medium");
        assert_eq!(cfg.control.size_classes[2].name, "large");
        assert_eq!(cfg.control.size_classes[2].max_bytes, u64::MAX);
        crate::queue::size_class::validate_size_classes(&cfg.control.size_classes).unwrap();
        let loaded = crate::queue::load_size_classes(&cfg.control.size_classes).unwrap();
        assert_eq!(loaded.len(), 3);
    }

    #[test]
    fn round_trip_toml_provider_token() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut providers = HashMap::new();
        providers.insert(
            "my-gitea".to_string(),
            ProviderEntry {
                kind: "gitea".into(),
                host: Some("https://gitea.example.com".into()),
                token: Some("provider-token".into()),
                auth_template: None,
                auth_header_name: None,
            },
        );
        let cfg = Config {
            server: Some("https://ripclone.example.com".into()),
            default_provider: Some("my-gitea".into()),
            agent: None,
            clone: CloneConfig {
                depth: Some(1),
                mode: Some("editable".into()),
            },
            providers,
            storage: StorageConfig {
                backend: Some("s3".into()),
                bucket: Some("my-bucket".into()),
                ..Default::default()
            },
            control: ControlConfig {
                path: Some("/var/lib/ripclone/control.db".into()),
                ..Default::default()
            },
            removed_metadata: None,
            removed_queue: None,
        };
        save_to(&path, &cfg).unwrap();

        let loaded = load_from(&path);
        assert_eq!(
            loaded.server.as_deref(),
            Some("https://ripclone.example.com")
        );
        assert_eq!(loaded.default_provider.as_deref(), Some("my-gitea"));
        assert!(loaded.providers.contains_key("my-gitea"));
        assert_eq!(
            loaded
                .providers
                .get("my-gitea")
                .and_then(|entry| entry.token.as_deref()),
            Some("provider-token")
        );
        assert_eq!(
            loaded.control.path.as_deref(),
            Some("/var/lib/ripclone/control.db")
        );
        assert_eq!(loaded.storage.backend.as_deref(), Some("s3"));
        assert_eq!(loaded.storage.bucket.as_deref(), Some("my-bucket"));
    }

    #[test]
    fn existing_toml_parse_error_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "server = [").unwrap();

        let err = try_load_from(&path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("parse config"));
        assert!(msg.contains(path.to_str().unwrap()));
    }

    #[test]
    fn missing_toml_loads_default_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        let cfg = try_load_from(&path).unwrap();
        assert!(cfg.server.is_none());
        assert!(cfg.default_provider.is_none());
    }

    #[test]
    fn provider_configs_maps_entries() {
        let mut providers = HashMap::new();
        providers.insert(
            "my-gitea".to_string(),
            ProviderEntry {
                kind: "gitea".into(),
                host: Some("https://gitea.example.com".into()),
                token: Some("secret".into()),
                auth_template: Some("token {{token}}".into()),
                auth_header_name: None,
            },
        );
        let cfg = Config {
            providers,
            ..Config::default()
        };

        let configs = cfg.provider_configs();
        assert_eq!(configs.len(), 1);
        let p = &configs[0];
        assert_eq!(p.id, "my-gitea");
        assert_eq!(p.kind.as_deref(), Some("gitea"));
        assert_eq!(p.host.as_deref(), Some("https://gitea.example.com"));
        assert_eq!(p.auth_template.as_deref(), Some("token {{token}}"));
        assert_eq!(p.token.as_deref(), Some("secret"));
    }

    #[test]
    fn agent_mode_env_wins_over_config() {
        let _guard = ENV_LOCK.lock().unwrap();
        let old = std::env::var_os("RIPCLONE_AGENT");

        // Truthy env turns it on regardless of config.
        unsafe { std::env::set_var("RIPCLONE_AGENT", "1") };
        assert!(agent_mode(None));
        assert!(agent_mode(Some(false)));

        // Explicit falsey env turns it off even when config default is on.
        unsafe { std::env::set_var("RIPCLONE_AGENT", "0") };
        assert!(!agent_mode(Some(true)));

        // Empty/unset env falls back to the config default.
        unsafe { std::env::set_var("RIPCLONE_AGENT", "") };
        assert!(agent_mode(Some(true)));
        assert!(!agent_mode(Some(false)));
        assert!(!agent_mode(None));

        unsafe { std::env::remove_var("RIPCLONE_AGENT") };
        assert!(agent_mode(Some(true)));
        assert!(!agent_mode(None));

        match old {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_AGENT", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_AGENT") },
        }
    }

    #[test]
    fn ripclone_config_env_overrides_home_path() {
        let _guard = ENV_LOCK.lock().unwrap();
        let old = std::env::var_os("RIPCLONE_CONFIG");
        unsafe { std::env::set_var("RIPCLONE_CONFIG", "/etc/ripclone/config.toml") };
        assert_eq!(
            global_config_path(),
            Some(PathBuf::from("/etc/ripclone/config.toml")),
            "RIPCLONE_CONFIG must override the $HOME-based path"
        );
        match old {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_CONFIG", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_CONFIG") },
        }
    }
}
