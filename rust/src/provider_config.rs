//! Workspace upstream configuration and token resolution.
//!
//! Canonical configuration uses `[workspace]` / `RIPCLONE_WORKSPACE`. Legacy
//! provider declarations are migrated one-to-one into workspaces with the same
//! IDs.

use crate::provider::{ProviderConfig, WorkspaceRegistry};
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
pub fn load_registry_with_config(config: &crate::config::Config) -> Result<WorkspaceRegistry> {
    let mut registry = WorkspaceRegistry::new();
    let mut env_workspace_id = None;

    // File configuration is the base layer. Environment declarations below
    // override it, matching the rest of ripclone's config precedence.
    merge_configs(&mut registry, config.provider_configs())?;

    // Legacy plural migration input: each provider instance becomes a
    // workspace with the same id.
    if let Some(json) = std::env::var("RIPCLONE_PROVIDERS")
        .ok()
        .filter(|t| !t.is_empty())
    {
        let file = parse_providers_json(&json).with_context(|| "parse RIPCLONE_PROVIDERS JSON")?;
        merge_configs(&mut registry, file.providers)?;
    }

    // Canonical deployment form wins over both TOML and the deprecated env.
    if let Some(json) = std::env::var("RIPCLONE_WORKSPACE")
        .ok()
        .filter(|t| !t.trim().is_empty())
    {
        let workspace: crate::config::WorkspaceEntry =
            serde_json::from_str(json.trim()).with_context(|| "parse RIPCLONE_WORKSPACE JSON")?;
        env_workspace_id = Some(workspace.id.clone());
        registry.merge_workspace(
            workspace.id.clone(),
            ProviderConfig {
                id: workspace.id,
                kind: Some(workspace.provider),
                host: workspace.host,
                token: workspace.token,
                auth_template: workspace.auth_template,
                auth_header_name: workspace.auth_header_name,
            },
        )?;
    }

    if let Some(selected) = config
        .selected_workspace()
        .map(str::to_owned)
        .or(env_workspace_id)
    {
        registry
            .select_workspace(&selected)
            .with_context(|| format!("select default workspace '{selected}'"))?;
    }

    if registry.token("github").is_none()
        && let Some(token) = std::env::var("RIPCLONE_GITHUB_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())
    {
        registry.set_token("github", token);
    }

    Ok(registry)
}

/// Build a registry from env JSON and the current unified TOML config.
pub fn load_registry() -> Result<WorkspaceRegistry> {
    load_registry_with_config(&crate::config::load())
}

fn merge_configs(registry: &mut WorkspaceRegistry, configs: Vec<ProviderConfig>) -> Result<()> {
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
    fn config_github_token_beats_legacy_env() {
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

        assert_eq!(
            registry.token("github").unwrap().expose_secret(),
            "gh-config"
        );
    }

    #[test]
    fn canonical_workspace_env_builds_one_workspace_upstream() {
        let _guard = lock_env();
        let old_workspace = std::env::var_os("RIPCLONE_WORKSPACE");
        let old_providers = std::env::var_os("RIPCLONE_PROVIDERS");
        unsafe {
            std::env::set_var(
                "RIPCLONE_WORKSPACE",
                r#"{"id":"acme","provider":"gitea","host":"gitea.example.com","token":"ws-token"}"#,
            );
            std::env::remove_var("RIPCLONE_PROVIDERS");
        }

        let registry = load_registry_with_config(&Config::default()).unwrap();
        restore_env("RIPCLONE_WORKSPACE", old_workspace);
        restore_env("RIPCLONE_PROVIDERS", old_providers);

        let workspace = registry.workspace("acme").unwrap();
        assert_eq!(registry.selected_workspace().id.as_str(), "acme");
        assert_eq!(
            workspace.upstream.kind,
            crate::provider::ProviderKind::Gitea
        );
        assert_eq!(workspace.upstream.host, "gitea.example.com");
        assert_eq!(registry.token("acme").unwrap().expose_secret(), "ws-token");
    }

    #[test]
    fn canonical_workspace_env_overrides_same_id_toml_upstream() {
        let _guard = lock_env();
        let old_workspace = std::env::var_os("RIPCLONE_WORKSPACE");
        let old_providers = std::env::var_os("RIPCLONE_PROVIDERS");
        unsafe {
            std::env::set_var(
                "RIPCLONE_WORKSPACE",
                r#"{"id":"acme","provider":"gitlab","host":"env.example.com"}"#,
            );
            std::env::remove_var("RIPCLONE_PROVIDERS");
        }
        let cfg = Config {
            workspace: Some(crate::config::WorkspaceEntry {
                id: "acme".into(),
                provider: "gitea".into(),
                host: Some("toml.example.com".into()),
                token: None,
                auth_template: None,
                auth_header_name: None,
            }),
            ..Config::default()
        };
        let registry = load_registry_with_config(&cfg).unwrap();
        restore_env("RIPCLONE_WORKSPACE", old_workspace);
        restore_env("RIPCLONE_PROVIDERS", old_providers);
        let upstream = &registry.workspace("acme").unwrap().upstream;
        assert_eq!(upstream.kind, crate::provider::ProviderKind::GitLab);
        assert_eq!(upstream.host, "env.example.com");
    }

    #[test]
    fn rejects_unknown_selected_workspace() {
        let _guard = lock_env();
        let old_workspace = std::env::var_os("RIPCLONE_WORKSPACE");
        let old_providers = std::env::var_os("RIPCLONE_PROVIDERS");
        unsafe {
            std::env::remove_var("RIPCLONE_WORKSPACE");
            std::env::remove_var("RIPCLONE_PROVIDERS");
        }
        let cfg = Config {
            default_workspace: Some("missing".into()),
            ..Config::default()
        };
        let err = load_registry_with_config(&cfg).unwrap_err();
        restore_env("RIPCLONE_WORKSPACE", old_workspace);
        restore_env("RIPCLONE_PROVIDERS", old_providers);
        assert!(
            err.to_string()
                .contains("select default workspace 'missing'")
        );
    }

    #[test]
    fn rejects_malformed_workspace_env_without_falling_back() {
        let _guard = lock_env();
        let old_workspace = std::env::var_os("RIPCLONE_WORKSPACE");
        unsafe { std::env::set_var("RIPCLONE_WORKSPACE", "{not-json") };
        let err = load_registry_with_config(&Config::default()).unwrap_err();
        restore_env("RIPCLONE_WORKSPACE", old_workspace);
        assert!(err.to_string().contains("parse RIPCLONE_WORKSPACE JSON"));
    }
}
