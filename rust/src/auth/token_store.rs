//! Secure-ish token storage for provider credentials.
//!
//! Tries the OS keyring first, falls back to a file in the ripclone config
//! directory, and also honors per-provider environment variables for CI and
//! tests.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;

const SERVICE: &str = "ripclone";

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
        .map(|c| if c.is_alphanumeric() { c.to_ascii_uppercase() } else { '_' })
        .collect();
    format!("RIPCLONE_PROVIDER_{}_TOKEN", normalized)
}

/// Token store backed by the OS keyring.
pub struct KeyringTokenStore;

impl KeyringTokenStore {
    pub fn new() -> Self {
        Self
    }
}

impl Default for KeyringTokenStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyringTokenStore {
    fn entry(id: &str) -> Result<keyring::Entry> {
        keyring::Entry::new(SERVICE, &format!("provider/{}", id))
            .with_context(|| format!("open keyring entry for provider '{}'", id))
    }
}

impl TokenStore for KeyringTokenStore {
    fn get(&self, id: &str) -> Result<Option<String>> {
        match Self::entry(id)?.get_password() {
            Ok(t) => Ok(Some(t)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(anyhow::anyhow!("keyring get failed: {}", e)),
        }
    }

    fn set(&self, id: &str, token: &str) -> Result<()> {
        Self::entry(id)?.set_password(token)?;
        Ok(())
    }

    fn delete(&self, id: &str) -> Result<()> {
        match Self::entry(id)?.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("keyring delete failed: {}", e)),
        }
    }
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
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create token dir {}", parent.display()))?;
        }
        let data = serde_json::to_string_pretty(map)?;
        std::fs::write(&self.path, data)
            .with_context(|| format!("write token file {}", self.path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }
}

impl TokenStore for FileTokenStore {
    fn get(&self, id: &str) -> Result<Option<String>> {
        Ok(self.load()?.remove(id))
    }

    fn set(&self, id: &str, token: &str) -> Result<()> {
        let mut map = self.load()?;
        map.insert(id.to_string(), token.to_string());
        self.save(&map)
    }

    fn delete(&self, id: &str) -> Result<()> {
        let mut map = self.load()?;
        map.remove(id);
        self.save(&map)
    }
}

/// Token store that reads from env, then the keyring, then a file; writes to
/// the keyring if available, otherwise the file.
pub struct FallbackTokenStore {
    keyring: KeyringTokenStore,
    file: FileTokenStore,
}

impl FallbackTokenStore {
    pub fn new() -> Result<Self> {
        let file = FileTokenStore::new(
            FileTokenStore::default_path().context("no HOME for token file")?,
        );
        Ok(Self {
            keyring: KeyringTokenStore::new(),
            file,
        })
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self {
            keyring: KeyringTokenStore::new(),
            file: FileTokenStore::new(path),
        }
    }
}

impl TokenStore for FallbackTokenStore {
    fn get(&self, id: &str) -> Result<Option<String>> {
        if let Some(t) = env_token(id) {
            return Ok(Some(t));
        }
        match self.keyring.get(id) {
            Ok(Some(t)) => return Ok(Some(t)),
            Ok(None) => {}
            Err(e) => {
                tracing::debug!("keyring read failed for {}: {}", id, e);
            }
        }
        self.file.get(id)
    }

    fn set(&self, id: &str, token: &str) -> Result<()> {
        match self.keyring.set(id, token) {
            Ok(()) => {
                // If a fallback file token exists, clean it up.
                let _ = self.file.delete(id);
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(
                    "keyring write failed for {}, falling back to file: {}",
                    id,
                    e
                );
            }
        }
        self.file.set(id, token)
    }

    fn delete(&self, id: &str) -> Result<()> {
        let _ = self.keyring.delete(id);
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
    fn fallback_reads_env_over_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = FallbackTokenStore::with_path(dir.path().join("tokens.json"));
        store.set("gitlab", "from-file").unwrap();
        unsafe { std::env::set_var("RIPCLONE_PROVIDER_GITLAB_TOKEN", "from-env") };
        assert_eq!(store.get("gitlab").unwrap().as_deref(), Some("from-env"));
        unsafe { std::env::remove_var("RIPCLONE_PROVIDER_GITLAB_TOKEN") };
    }
}
