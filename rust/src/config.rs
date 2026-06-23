//! On-disk CLI config (`~/.config/ripclone/config.json`) — written by
//! `ripclone login`, read to authenticate subsequent commands. The token is a
//! secret, so the file is mode 0600.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    /// The raw ripclone token (`rc_live_…`). Hashed before it's sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// The server this token belongs to (for reference / self-host).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
}

pub fn config_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| {
        let mut p = PathBuf::from(home);
        p.push(".config");
        p.push("ripclone");
        p.push("config.json");
        p
    })
}

/// Load the config, or a default (empty) one if it's missing/unreadable.
pub fn load() -> Config {
    match config_path() {
        Some(path) => load_from(&path),
        None => Config::default(),
    }
}

pub fn save(config: &Config) -> Result<()> {
    let path = config_path().context("no HOME for config path")?;
    save_to(&path, config)
}

fn load_from(path: &Path) -> Config {
    match std::fs::read_to_string(path) {
        Ok(data) => serde_json::from_str(&data).unwrap_or_default(),
        Err(_) => Config::default(),
    }
}

fn save_to(path: &Path, config: &Config) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).context("create config dir")?;
    }
    let data = serde_json::to_string_pretty(config)?;
    std::fs::write(path, data).with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// The configured token, if any (non-empty).
pub fn token() -> Option<String> {
    load().token.filter(|t| !t.is_empty())
}

/// Remove just the token (keep any other config).
pub fn clear_token() -> Result<()> {
    let mut c = load();
    c.token = None;
    save(&c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_clears_token_at_0600() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ripclone/config.json");

        // Missing file → default.
        assert!(load_from(&path).token.is_none());

        let cfg = Config {
            token: Some("rc_live_abc".into()),
            server: Some("https://ripclone.com".into()),
        };
        save_to(&path, &cfg).unwrap();

        let loaded = load_from(&path);
        assert_eq!(loaded.token.as_deref(), Some("rc_live_abc"));
        assert_eq!(loaded.server.as_deref(), Some("https://ripclone.com"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // Clearing the token leaves the rest intact.
        let mut cleared = load_from(&path);
        cleared.token = None;
        save_to(&path, &cleared).unwrap();
        let after = load_from(&path);
        assert!(after.token.is_none());
        assert_eq!(after.server.as_deref(), Some("https://ripclone.com"));
    }

    #[test]
    fn garbage_file_loads_as_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "not json {{{").unwrap();
        assert!(load_from(&path).token.is_none());
    }
}
