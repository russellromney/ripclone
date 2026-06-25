//! Provider configuration and token resolution.
//!
//! Custom/self-hosted providers are declared in the unified TOML config
//! (`~/.config/ripclone/config.toml` and optional project `ripclone.toml`).
//! Tokens are stored separately via [`crate::auth::token_store::TokenStore`] so
//! the config file never needs to contain plaintext secrets.
//!
//! The legacy `providers.json` path is still read for backward compatibility,
//! but the CLI now writes provider changes to `config.toml`.

use crate::auth::token_store::{TokenStore, env_token};
use crate::provider::{ProviderConfig, ProviderRegistry};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProvidersFile {
    pub providers: Vec<ProviderConfig>,
}

pub fn providers_path() -> Option<PathBuf> {
    config_dir().map(|d| d.join("providers.json"))
}

pub fn config_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| {
        let mut p = PathBuf::from(h);
        p.push(".config");
        p.push("ripclone");
        p
    })
}

/// Load the providers file, if it exists.
///
/// Supports both the current `{"providers": [...]}` object form and the
/// legacy `[{...}, ...]` array form for backward compatibility.
pub fn load_providers_file(path: &Path) -> Result<ProvidersFile> {
    if !path.exists() {
        return Ok(ProvidersFile::default());
    }
    let data = std::fs::read_to_string(path)
        .with_context(|| format!("read providers config {}", path.display()))?;
    parse_providers_json(&data)
        .with_context(|| format!("parse providers config {}", path.display()))
}

fn parse_providers_json(data: &str) -> Result<ProvidersFile> {
    let trimmed = data.trim();
    if trimmed.starts_with('[') {
        let providers: Vec<ProviderConfig> = serde_json::from_str(trimmed)?;
        Ok(ProvidersFile { providers })
    } else {
        Ok(serde_json::from_str(trimmed)?)
    }
}

/// Save the providers file (without tokens).
pub fn save_providers_file(path: &Path, file: &ProvidersFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create providers dir {}", parent.display()))?;
    }
    let data = serde_json::to_string_pretty(file)?;
    std::fs::write(path, data)
        .with_context(|| format!("write providers config {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Add or update a provider in the config file and persist its token via the
/// token store.
pub fn add_provider(
    path: &Path,
    token_store: &dyn TokenStore,
    mut cfg: ProviderConfig,
    token: Option<&str>,
) -> Result<()> {
    let mut file = load_providers_file(path)?;
    // Strip any plaintext token from the config; we store it separately.
    cfg.token = None;

    file.providers.retain(|p| p.id != cfg.id);
    file.providers.push(cfg);
    save_providers_file(path, &file)?;

    if let Some(token) = token {
        token_store.set(&file.providers.last().unwrap().id, token)?;
    }
    Ok(())
}

/// Remove a provider and delete its stored token.
pub fn remove_provider(path: &Path, token_store: &dyn TokenStore, id: &str) -> Result<()> {
    let mut file = load_providers_file(path)?;
    file.providers.retain(|p| p.id != id);
    save_providers_file(path, &file)?;
    token_store.delete(id)?;
    Ok(())
}

/// Resolve the token for a provider: explicit config token first, then env,
/// then the token store.
pub fn resolve_provider_token(
    cfg: &ProviderConfig,
    token_store: &dyn TokenStore,
) -> Result<Option<String>> {
    if let Some(token) = cfg.token.as_deref() {
        return Ok(Some(token.to_string()));
    }
    if let Some(token) = env_token(&cfg.id) {
        return Ok(Some(token));
    }
    token_store.get(&cfg.id)
}

/// Build a registry from env JSON, legacy file config, the provided unified
/// config, and tokens resolved through the token store.
pub fn load_registry_with_config(
    token_store: &dyn TokenStore,
    config: &crate::config::Config,
) -> Result<ProviderRegistry> {
    let mut registry = ProviderRegistry::new();

    if let Some(json) = std::env::var("RIPCLONE_PROVIDERS")
        .ok()
        .filter(|t| !t.is_empty())
    {
        let file = parse_providers_json(&json).with_context(|| "parse RIPCLONE_PROVIDERS JSON")?;
        merge_with_tokens(&mut registry, file.providers, token_store)?;
    }

    if let Some(path) = std::env::var("RIPCLONE_PROVIDERS_CONFIG")
        .ok()
        .filter(|t| !t.is_empty())
    {
        let file = load_providers_file(Path::new(&path))?;
        merge_with_tokens(&mut registry, file.providers, token_store)?;
    }

    if let Some(path) = providers_path()
        && path.exists()
    {
        let file = load_providers_file(&path)?;
        merge_with_tokens(&mut registry, file.providers, token_store)?;
    }

    // Finally, merge providers declared in the unified TOML config (global + project).
    merge_with_tokens(&mut registry, config.provider_configs(), token_store)?;

    Ok(registry)
}

/// Build a registry from env JSON, legacy file config, the current unified
/// TOML config, and tokens resolved through the token store.
pub fn load_registry_with_token_store(token_store: &dyn TokenStore) -> Result<ProviderRegistry> {
    load_registry_with_config(token_store, &crate::config::load())
}

fn merge_with_tokens(
    registry: &mut ProviderRegistry,
    configs: Vec<ProviderConfig>,
    token_store: &dyn TokenStore,
) -> Result<()> {
    for mut cfg in configs {
        let token = resolve_provider_token(&cfg, token_store)?;
        if let Some(token) = token {
            cfg.token = Some(token);
        }
        registry.merge_one(cfg)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token_store::FileTokenStore;
    use secrecy::ExposeSecret;

    #[test]
    fn save_and_load_providers_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.json");
        let file = ProvidersFile {
            providers: vec![ProviderConfig {
                id: "gitlab".into(),
                kind: Some("gitlab".into()),
                host: Some("gitlab.com".into()),
                token: None,
                auth_template: None,
            }],
        };
        save_providers_file(&path, &file).unwrap();
        let loaded = load_providers_file(&path).unwrap();
        assert_eq!(loaded.providers[0].id, "gitlab");
    }

    #[test]
    fn add_provider_stores_token_separately() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.json");
        let token_store = FileTokenStore::new(dir.path().join("tokens.json"));
        let cfg = ProviderConfig {
            id: "gitlab".into(),
            kind: Some("gitlab".into()),
            host: Some("gitlab.com".into()),
            token: None,
            auth_template: None,
        };
        add_provider(&path, &token_store, cfg, Some("secret")).unwrap();
        let file = load_providers_file(&path).unwrap();
        assert!(file.providers[0].token.is_none());
        assert_eq!(
            token_store.get("gitlab").unwrap().as_deref(),
            Some("secret")
        );
    }

    #[test]
    fn load_registry_resolves_env_token_over_file_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.json");
        let token_store = FileTokenStore::new(dir.path().join("tokens.json"));
        let cfg = ProviderConfig {
            id: "gitlab".into(),
            kind: Some("gitlab".into()),
            host: Some("gitlab.com".into()),
            token: None,
            auth_template: None,
        };
        add_provider(&path, &token_store, cfg, Some("from-file")).unwrap();

        unsafe {
            std::env::set_var("RIPCLONE_PROVIDERS_CONFIG", &path);
            std::env::set_var("RIPCLONE_PROVIDER_GITLAB_TOKEN", "from-env");
        }
        let registry =
            load_registry_with_config(&token_store, &crate::config::Config::default()).unwrap();
        let token = registry.token("gitlab").unwrap().expose_secret();
        assert_eq!(token, "from-env");
        unsafe {
            std::env::remove_var("RIPCLONE_PROVIDERS_CONFIG");
            std::env::remove_var("RIPCLONE_PROVIDER_GITLAB_TOKEN");
        }
    }

    #[test]
    fn legacy_array_providers_file_is_supported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("providers.json");
        std::fs::write(
            &path,
            r#"[{"id":"gitlab","kind":"gitlab","host":"gitlab.com"}]"#,
        )
        .unwrap();
        let file = load_providers_file(&path).unwrap();
        assert_eq!(file.providers.len(), 1);
        assert_eq!(file.providers[0].id, "gitlab");
    }

    #[test]
    fn load_registry_with_config_merges_toml_providers() {
        use crate::config::{Config, ProviderEntry};
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let token_store = FileTokenStore::new(dir.path().join("tokens.json"));
        token_store.set("my-gitea", "gitea-secret").unwrap();

        let mut providers = HashMap::new();
        providers.insert(
            "my-gitea".to_string(),
            ProviderEntry {
                kind: "gitea".into(),
                host: Some("https://gitea.example.com".into()),
                auth_template: None,
            },
        );
        let config = Config {
            providers,
            ..Config::default()
        };

        let registry = load_registry_with_config(&token_store, &config).unwrap();
        let token = registry.token("my-gitea").unwrap().expose_secret();
        assert_eq!(token, "gitea-secret");
        assert_eq!(
            registry.get("my-gitea").map(|p| p.host.as_str()),
            Some("https://gitea.example.com")
        );
    }
}
