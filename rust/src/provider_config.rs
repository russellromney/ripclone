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
    registry_from(std::env::var("RIPCLONE_PROVIDERS").ok().as_deref(), config)
}

/// [`load_registry_with_config`] with the `RIPCLONE_PROVIDERS` value given.
/// Config entries merge after the JSON ones, so they win on the same id.
fn registry_from(
    providers_json: Option<&str>,
    config: &crate::config::Config,
) -> Result<ProviderRegistry> {
    let mut registry = ProviderRegistry::new();

    if let Some(json) = providers_json.filter(|t| !t.is_empty()) {
        let file = parse_providers_json(json).with_context(|| "parse RIPCLONE_PROVIDERS JSON")?;
        merge_configs(&mut registry, file.providers)?;
    }

    merge_configs(&mut registry, config.provider_configs())?;

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

    #[test]
    fn registry_merges_env_json_then_config() {
        let entry = |host: &str, token: &str| ProviderEntry {
            kind: "gitea".into(),
            host: Some(host.into()),
            token: Some(token.into()),
            auth_template: None,
            auth_header_name: None,
        };
        let config = Config {
            providers: HashMap::from([
                (
                    "my-gitea".to_string(),
                    entry("https://gitea.example.com", "gitea-secret"),
                ),
                (
                    "shared".to_string(),
                    entry("https://from-config.example.com", "config-token"),
                ),
            ]),
            ..Config::default()
        };
        let json = r#"{"providers":[
            {"id":"gitlab","kind":"gitlab","host":"gitlab.com","token":"gl-token"},
            {"id":"shared","kind":"gitea","host":"https://from-json.example.com","token":"json-token"}
        ]}"#;

        let registry = registry_from(Some(json), &config).unwrap();
        for (id, host, token) in [
            ("gitlab", "gitlab.com", "gl-token"),
            ("my-gitea", "https://gitea.example.com", "gitea-secret"),
            ("shared", "https://from-config.example.com", "config-token"),
        ] {
            assert_eq!(
                registry.get(id).map(|p| p.host.as_str()),
                Some(host),
                "{id}"
            );
            assert_eq!(registry.token(id).unwrap().expose_secret(), token, "{id}");
        }
        assert!(registry_from(Some("not json"), &config).is_err());
    }
}
