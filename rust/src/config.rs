//! Unified ripclone configuration.
//!
//! Supports a global TOML file at `~/.config/ripclone/config.toml` and an
//! optional project-level `ripclone.toml` discovered by walking up from the
//! current directory. Environment variables and CLI flags still take precedence.
//!
//! This file intentionally does **not** contain secrets. Server and provider
//! tokens are stored separately via environment variables or the ripclone token file.

use crate::provider::ProviderConfig;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Top-level ripclone configuration.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Ripclone server URL.
    pub server: Option<String>,
    /// Default provider instance id (e.g. "github", "my-gitea").
    pub default_provider: Option<String>,
    /// Default clone options.
    pub clone: CloneConfig,
    /// Custom/self-hosted provider declarations. Built-in presets (github,
    /// gitlab, bitbucket) are implicit and do not need to be declared.
    pub providers: HashMap<String, ProviderEntry>,
    /// Server-side artifact storage backend (`[storage]`).
    pub storage: StorageConfig,
    /// Server-side metadata (ref) store backend (`[metadata]`).
    pub metadata: MetadataConfig,
    /// Server-side build queue backend (`[queue]`).
    pub queue: QueueConfig,
    /// Legacy raw server token, only populated when reading old `config.json`.
    #[serde(skip)]
    pub token: Option<String>,
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

/// Server-side metadata (ref) store. `RIPCLONE_METADATA*` env vars override.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetadataConfig {
    /// `file` | `s3` | `sqlite` | `postgres` | `mysql` | `libsql`. Unset follows
    /// storage (s3 if configured, else file).
    pub backend: Option<String>,
    /// Connection URL for the SQL backends.
    pub url: Option<String>,
    /// Auth token for `libsql` (remote), stored as written.
    pub token: Option<String>,
}

/// Server-side build queue. `RIPCLONE_QUEUE*` env vars override.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    /// `local` | `sqlite` | `postgres` | `mysql` | `libsql`. Unset = `local`.
    pub backend: Option<String>,
    /// Connection URL for the SQL backends.
    pub url: Option<String>,
    /// Auth token for `libsql` (remote), stored as written.
    pub token: Option<String>,
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

/// Path to the legacy global JSON config file.
pub fn legacy_config_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| {
        let mut p = PathBuf::from(home);
        p.push(".config");
        p.push("ripclone");
        p.push("config.json");
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

/// Load the global configuration (TOML, with legacy JSON fallback).
pub fn load_global() -> Config {
    let mut cfg = match global_config_path() {
        Some(path) if path.exists() => load_from(&path),
        _ => Config::default(),
    };

    // If no token was found in the new config, fall back to the legacy JSON
    // file so existing logins keep working after the TOML migration.
    if cfg.token.is_none()
        && let Some(path) = legacy_config_path().filter(|p| p.exists())
    {
        let legacy = load_legacy_json(&path);
        cfg.token = legacy.token;
    }

    cfg
}

fn load_from(path: &Path) -> Config {
    match std::fs::read_to_string(path) {
        Ok(data) => toml::from_str(&data).unwrap_or_else(|e| {
            tracing::warn!("failed to parse config {}: {}", path.display(), e);
            Config::default()
        }),
        Err(_) => Config::default(),
    }
}

fn load_legacy_json(path: &Path) -> Config {
    #[derive(serde::Deserialize)]
    struct Legacy {
        token: Option<String>,
        server: Option<String>,
    }
    std::fs::read_to_string(path)
        .ok()
        .and_then(|data| serde_json::from_str::<Legacy>(&data).ok())
        .map(|legacy| Config {
            server: legacy.server,
            token: legacy.token,
            ..Config::default()
        })
        .unwrap_or_default()
}

/// Save the global configuration. Secrets are not written; use the token store
/// for server/provider tokens.
pub fn save(config: &Config) -> Result<()> {
    let path = global_config_path().context("no HOME for config path")?;
    save_to(&path, config)
}

fn save_to(path: &Path, config: &Config) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).context("create config dir")?;
    }
    // Strip any transient token before saving.
    let mut to_save = config.clone();
    to_save.token = None;
    let data = toml::to_string_pretty(&to_save).context("serialize config")?;
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
                token: None,
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
        metadata: MetadataConfig {
            backend: overrides.metadata.backend.or(base.metadata.backend),
            url: overrides.metadata.url.or(base.metadata.url),
            token: overrides.metadata.token.or(base.metadata.token),
        },
        queue: QueueConfig {
            backend: overrides.queue.backend.or(base.queue.backend),
            url: overrides.queue.url.or(base.queue.url),
            token: overrides.queue.token.or(base.queue.token),
        },
        token: overrides.token.or(base.token),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Changing HOME is otherwise racy under parallel test execution.
    static HOME_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_home<F, R>(home: &Path, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _guard = HOME_LOCK.lock().unwrap();
        let old = std::env::var_os("HOME");
        unsafe { std::env::set_var("HOME", home) };
        let result = f();
        match old {
            Some(v) => unsafe { std::env::set_var("HOME", v) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        result
    }

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
            token: None,
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
            token: None,
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
    fn round_trip_toml_no_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut providers = HashMap::new();
        providers.insert(
            "my-gitea".to_string(),
            ProviderEntry {
                kind: "gitea".into(),
                host: Some("https://gitea.example.com".into()),
                auth_template: None,
                auth_header_name: None,
            },
        );
        let cfg = Config {
            server: Some("https://ripclone.example.com".into()),
            default_provider: Some("my-gitea".into()),
            clone: CloneConfig {
                depth: Some(1),
                mode: Some("editable".into()),
            },
            providers,
            queue: QueueConfig {
                backend: Some("postgres".into()),
                url: Some("postgres://db/ripclone".into()),
                token: None,
            },
            metadata: MetadataConfig {
                backend: Some("postgres".into()),
                url: Some("postgres://db/ripclone".into()),
                token: None,
            },
            storage: StorageConfig {
                backend: Some("s3".into()),
                bucket: Some("my-bucket".into()),
                ..Default::default()
            },
            token: Some("should-not-be-saved".into()),
        };
        save_to(&path, &cfg).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            !text.contains("should-not-be-saved"),
            "token must not be written to config"
        );

        let loaded = load_from(&path);
        assert_eq!(
            loaded.server.as_deref(),
            Some("https://ripclone.example.com")
        );
        assert_eq!(loaded.default_provider.as_deref(), Some("my-gitea"));
        assert!(loaded.providers.contains_key("my-gitea"));
        assert_eq!(loaded.queue.backend.as_deref(), Some("postgres"));
        assert_eq!(loaded.queue.url.as_deref(), Some("postgres://db/ripclone"));
        assert_eq!(loaded.metadata.backend.as_deref(), Some("postgres"));
        assert_eq!(loaded.storage.backend.as_deref(), Some("s3"));
        assert_eq!(loaded.storage.bucket.as_deref(), Some("my-bucket"));
        assert!(loaded.token.is_none());
    }

    #[test]
    fn legacy_json_loads_token_and_server() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(
            &path,
            r#"{"token":"rc_legacy_token","server":"https://legacy.example.com"}"#,
        )
        .unwrap();

        let loaded = load_legacy_json(&path);
        assert_eq!(loaded.token.as_deref(), Some("rc_legacy_token"));
        assert_eq!(loaded.server.as_deref(), Some("https://legacy.example.com"));
    }

    #[test]
    fn toml_takes_precedence_but_legacy_token_is_merged() {
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join(".config").join("ripclone");
        std::fs::create_dir_all(&config_dir).unwrap();

        let toml_path = config_dir.join("config.toml");
        std::fs::write(
            &toml_path,
            r#"server = "https://toml.example.com"
default_provider = "my-gitea"
"#,
        )
        .unwrap();

        let json_path = config_dir.join("config.json");
        std::fs::write(
            &json_path,
            r#"{"token":"rc_legacy_token","server":"https://legacy.example.com"}"#,
        )
        .unwrap();

        let loaded = with_home(dir.path(), load_global);
        assert_eq!(loaded.server.as_deref(), Some("https://toml.example.com"));
        assert_eq!(loaded.default_provider.as_deref(), Some("my-gitea"));
        assert_eq!(loaded.token.as_deref(), Some("rc_legacy_token"));
    }

    #[test]
    fn provider_configs_maps_entries() {
        let mut providers = HashMap::new();
        providers.insert(
            "my-gitea".to_string(),
            ProviderEntry {
                kind: "gitea".into(),
                host: Some("https://gitea.example.com".into()),
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
        assert!(p.token.is_none(), "token must not leak into ProviderConfig");
    }

    #[test]
    fn ripclone_config_env_overrides_home_path() {
        // Shares the HOME mutation lock so the env order can't race other tests.
        let _guard = HOME_LOCK.lock().unwrap();
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
