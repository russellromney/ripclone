//! Provider configuration and token resolution.
//!
//! Custom/self-hosted providers are declared in the unified TOML config
//! (`~/.config/ripclone/config.toml` and optional project `ripclone.toml`) or
//! the `RIPCLONE_PROVIDERS` JSON environment variable.

use crate::provider::{ProviderConfig, ProviderRegistry};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProvidersFile {
    pub providers: Vec<ProviderConfig>,
}

fn parse_providers_json(data: &str) -> Result<ProvidersFile> {
    Ok(serde_json::from_str(data.trim())?)
}

/// Build a registry from env JSON and the provided unified config.
pub fn load_registry_with_config(config: &crate::config::Config) -> Result<ProviderRegistry> {
    let mut registry = ProviderRegistry::new();

    // File configuration provides the baseline. Environment configuration is
    // merged afterward so operator overrides follow `env > config > defaults`.
    merge_configs(&mut registry, config.provider_configs())?;

    if let Some(json) = std::env::var("RIPCLONE_PROVIDERS")
        .ok()
        .filter(|t| !t.is_empty())
    {
        let file = parse_providers_json(&json).with_context(|| "parse RIPCLONE_PROVIDERS JSON")?;
        merge_configs(&mut registry, file.providers)?;
    }

    if let Some(token) = std::env::var("RIPCLONE_GITHUB_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
    {
        registry.set_token("github", token);
    }

    Ok(registry)
}

/// Build a registry from env JSON and the current unified TOML config.
pub fn load_registry() -> Result<ProviderRegistry> {
    load_registry_with_config(&crate::config::load())
}

fn merge_configs(registry: &mut ProviderRegistry, configs: Vec<ProviderConfig>) -> Result<()> {
    for cfg in configs {
        registry.merge_one(cfg)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ProviderEntry};
    use secrecy::ExposeSecret;
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap()
    }

    fn restore_env(key: &str, old: Option<std::ffi::OsString>) {
        unsafe {
            match old {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }

    #[test]
    fn load_registry_with_config_merges_toml_providers() {
        let mut providers = HashMap::new();
        providers.insert(
            "my-gitea".to_string(),
            ProviderEntry {
                kind: "gitea".into(),
                host: Some("https://gitea.example.com".into()),
                token: Some("gitea-secret".into()),
                auth_template: None,
                auth_header_name: None,
            },
        );
        let config = Config {
            providers,
            ..Config::default()
        };

        let registry = load_registry_with_config(&config).unwrap();
        let token = registry.token("my-gitea").unwrap().expose_secret();
        assert_eq!(token, "gitea-secret");
        assert_eq!(
            registry.get("my-gitea").map(|p| p.host.as_str()),
            Some("https://gitea.example.com")
        );
    }

    #[test]
    fn load_registry_merges_env_json() {
        let _guard = lock_env();
        let old = std::env::var_os("RIPCLONE_PROVIDERS");
        unsafe {
            std::env::set_var(
                "RIPCLONE_PROVIDERS",
                r#"{"providers":[{"id":"gitlab","kind":"gitlab","host":"gitlab.com","token":"gl-token"}]}"#,
            );
        }

        let registry = load_registry_with_config(&Config::default()).unwrap();
        restore_env("RIPCLONE_PROVIDERS", old);

        assert_eq!(
            registry.get("gitlab").map(|p| p.host.as_str()),
            Some("gitlab.com")
        );
        assert_eq!(
            registry.token("gitlab").unwrap().expose_secret(),
            "gl-token"
        );
    }

    #[test]
    fn env_json_overrides_toml_provider_fields_and_token() {
        let _guard = lock_env();
        let old = std::env::var_os("RIPCLONE_PROVIDERS");
        unsafe {
            std::env::set_var(
                "RIPCLONE_PROVIDERS",
                r#"{"providers":[{"id":"gitea","kind":"gitea","host":"http://127.0.0.1:4242","token":"env-token"}]}"#,
            );
        }
        let mut providers = HashMap::new();
        providers.insert(
            "gitea".to_string(),
            ProviderEntry {
                kind: "gitea".into(),
                host: Some("http://localhost:3000".into()),
                token: Some("config-token".into()),
                auth_template: None,
                auth_header_name: None,
            },
        );

        let registry = load_registry_with_config(&Config {
            providers,
            ..Config::default()
        })
        .unwrap();
        restore_env("RIPCLONE_PROVIDERS", old);

        assert_eq!(
            registry.get("gitea").map(|provider| provider.host.as_str()),
            Some("http://127.0.0.1:4242")
        );
        assert_eq!(
            registry.token("gitea").unwrap().expose_secret(),
            "env-token"
        );
    }

    #[test]
    fn github_env_token_fills_default_when_config_has_no_token() {
        let _guard = lock_env();
        let old = std::env::var_os("RIPCLONE_GITHUB_TOKEN");
        unsafe {
            std::env::set_var("RIPCLONE_GITHUB_TOKEN", "gh-env");
        }

        let registry = load_registry_with_config(&Config::default()).unwrap();
        restore_env("RIPCLONE_GITHUB_TOKEN", old);

        assert_eq!(registry.token("github").unwrap().expose_secret(), "gh-env");
    }

    #[test]
    fn github_env_token_overrides_config_token() {
        let _guard = lock_env();
        let old = std::env::var_os("RIPCLONE_GITHUB_TOKEN");
        unsafe {
            std::env::set_var("RIPCLONE_GITHUB_TOKEN", "gh-env");
        }

        let mut providers = HashMap::new();
        providers.insert(
            "github".to_string(),
            ProviderEntry {
                kind: "github".into(),
                host: None,
                token: Some("gh-config".into()),
                auth_template: None,
                auth_header_name: None,
            },
        );
        let registry = load_registry_with_config(&Config {
            providers,
            ..Config::default()
        })
        .unwrap();
        restore_env("RIPCLONE_GITHUB_TOKEN", old);

        assert_eq!(registry.token("github").unwrap().expose_secret(), "gh-env");
    }
}
