//! File-based token storage for provider credentials.
//!
//! Reads per-provider environment variables first, then a token file in the
//! ripclone config directory. It never talks to the OS keychain/keyring.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;

/// Abstract storage for provider tokens.
pub trait TokenStore: Send + Sync {
    /// Return the token for `id`, or `None` if not found.
    fn get(&self, id: &str) -> Result<Option<String>>;
    /// Persist `token` for `id`.
    fn set(&self, id: &str, token: &str) -> Result<()>;
    /// Delete the stored token for `id`.
    fn delete(&self, id: &str) -> Result<()>;
}

/// Read a token from the environment: `RIPCLONE_PROVIDER_<ID>_TOKEN`.
pub fn env_token(id: &str) -> Option<String> {
    let var = provider_env_var(id);
    std::env::var(&var).ok().filter(|t| !t.is_empty())
}

/// The env-var name for provider `id`.
pub fn provider_env_var(id: &str) -> String {
    let normalized: String = id
        .chars()
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("RIPCLONE_PROVIDER_{}_TOKEN", normalized)
}

/// Token store backed by a JSON file with restrictive permissions.
pub struct FileTokenStore {
    path: PathBuf,
}

impl FileTokenStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn default_path() -> Option<PathBuf> {
        config_dir().map(|d| d.join("tokens.json"))
    }

    fn load(&self) -> Result<HashMap<String, String>> {
        if !self.path.exists() {
            return Ok(HashMap::new());
        }
        let data = std::fs::read_to_string(&self.path)
            .with_context(|| format!("read token file {}", self.path.display()))?;
        serde_json::from_str(&data)
            .with_context(|| format!("parse token file {}", self.path.display()))
    }

    fn save(&self, map: &HashMap<String, String>) -> Result<()> {
        let data = serde_json::to_string_pretty(map)?;
        crate::secure_file::write_0600_atomic(&self.path, data.as_bytes())
            .with_context(|| format!("write token file {}", self.path.display()))
    }
}

impl TokenStore for FileTokenStore {
    fn get(&self, id: &str) -> Result<Option<String>> {
        Ok(self.load()?.remove(id))
    }

    fn set(&self, id: &str, token: &str) -> Result<()> {
        crate::secure_file::with_file_lock(&self.path, || {
            let mut map = self.load()?;
            map.insert(id.to_string(), token.to_string());
            self.save(&map)
        })
    }

    fn delete(&self, id: &str) -> Result<()> {
        crate::secure_file::with_file_lock(&self.path, || {
            let mut map = self.load()?;
            map.remove(id);
            self.save(&map)
        })
    }
}

/// Default token store: reads from env, then a file; writes to the file.
pub struct FileBackedTokenStore {
    file: FileTokenStore,
}

impl FileBackedTokenStore {
    pub fn new() -> Result<Self> {
        let file =
            FileTokenStore::new(FileTokenStore::default_path().context("no HOME for token file")?);
        Ok(Self { file })
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self {
            file: FileTokenStore::new(path),
        }
    }
}

impl TokenStore for FileBackedTokenStore {
    fn get(&self, id: &str) -> Result<Option<String>> {
        if let Some(t) = env_token(id) {
            return Ok(Some(t));
        }
        self.file.get(id)
    }

    fn set(&self, id: &str, token: &str) -> Result<()> {
        self.file.set(id, token)
    }

    fn delete(&self, id: &str) -> Result<()> {
        self.file.delete(id)
    }
}

fn config_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| {
        let mut p = PathBuf::from(h);
        p.push(".config");
        p.push("ripclone");
        p
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_var_normalization() {
        assert_eq!(provider_env_var("gitlab"), "RIPCLONE_PROVIDER_GITLAB_TOKEN");
        assert_eq!(
            provider_env_var("my-gitea"),
            "RIPCLONE_PROVIDER_MY_GITEA_TOKEN"
        );
        assert_eq!(
            provider_env_var("company.gitea"),
            "RIPCLONE_PROVIDER_COMPANY_GITEA_TOKEN"
        );
    }

    #[test]
    fn file_store_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileTokenStore::new(dir.path().join("tokens.json"));
        assert!(store.get("gitlab").unwrap().is_none());
        store.set("gitlab", "glpat-xyz").unwrap();
        assert_eq!(store.get("gitlab").unwrap().as_deref(), Some("glpat-xyz"));
        store.delete("gitlab").unwrap();
        assert!(store.get("gitlab").unwrap().is_none());
    }

    #[test]
    fn env_reads_before_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBackedTokenStore::with_path(dir.path().join("tokens.json"));
        store.set("gitlab", "from-file").unwrap();
        unsafe { std::env::set_var("RIPCLONE_PROVIDER_GITLAB_TOKEN", "from-env") };
        assert_eq!(store.get("gitlab").unwrap().as_deref(), Some("from-env"));
        unsafe { std::env::remove_var("RIPCLONE_PROVIDER_GITLAB_TOKEN") };
    }

    #[test]
    fn concurrent_file_store_set_loses_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        let mut handles = Vec::new();
        for i in 0..16 {
            let path = path.clone();
            handles.push(std::thread::spawn(move || {
                let store = FileTokenStore::new(path);
                store
                    .set(&format!("provider-{i}"), &format!("token-{i}"))
                    .unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let store = FileTokenStore::new(path);
        for i in 0..16 {
            let token = format!("token-{i}");
            assert_eq!(
                store.get(&format!("provider-{i}")).unwrap().as_deref(),
                Some(token.as_str())
            );
        }
    }
}
