//! The config file, `~/.config/ripclone/config.toml` or the path in
//! `RIPCLONE_CONFIG`: the server URL and provider instances.
//!
//! It holds no secrets, so it can be committed or templated. Upstream
//! credentials come from the server's environment (see
//! [`crate::auth::credential`]), and the CLI's saved server token is in the
//! token file.

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
    /// Custom/self-hosted provider declarations. Built-in presets (github and
    /// gitlab) are implicit and do not need to be declared.
    pub providers: HashMap<String, ProviderEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEntry {
    pub kind: String,
    pub host: Option<String>,
    /// GitHub App id, for a `github` provider the server reads as an app.
    pub app_id: Option<u64>,
    pub auth_template: Option<String>,
    /// Optional header name for the credential. Defaults to `Authorization`.
    pub auth_header_name: Option<String>,
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

/// Load the config file; a missing file is the default config.
pub fn load() -> Config {
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

impl Config {
    /// Convert declared provider entries into the internal `ProviderConfig` shape.
    pub fn provider_configs(&self) -> Vec<ProviderConfig> {
        self.providers
            .iter()
            .map(|(id, entry)| ProviderConfig {
                id: id.clone(),
                kind: Some(entry.kind.clone()),
                host: entry.host.clone(),
                app_id: entry.app_id,
                auth_template: entry.auth_template.clone(),
                auth_header_name: entry.auth_header_name.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_is_default_and_malformed_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let cfg = try_load_from(&path).unwrap();
        assert!(cfg.server.is_none());
        assert!(cfg.providers.is_empty());

        std::fs::write(&path, "server = [").unwrap();
        assert!(try_load_from(&path).is_err());

        // A secret has no place in the file: a provider `token` is refused,
        // not ignored.
        std::fs::write(
            &path,
            "[providers.forge]\nkind = \"gitea\"\nhost = \"h\"\ntoken = \"t\"\n",
        )
        .unwrap();
        let error = format!("{:#}", try_load_from(&path).unwrap_err());
        assert!(error.contains("unknown field `token`"), "{error}");
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
