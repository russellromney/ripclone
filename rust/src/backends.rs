//! Artifact storage and build-local paths.
//!
//! Control state is supplied by the server-owned SQLite database. Artifact
//! bytes remain local or S3-compatible and are deliberately independent of
//! control state.

use crate::cas::Cas;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::retention::{LocalCacheRetention, RetentionSettings};
use crate::storage::{S3Storage, StorageRef, local};
use anyhow::{Context, Result};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tracing::info;

fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(crate::config::load_global)
}

pub struct Backends {
    pub cas: Cas,
    pub storage: StorageRef,
    pub cache_retention: LocalCacheRetention,
}

impl Backends {
    pub fn validate_environment() -> Result<()> {
        S3Storage::validate_env_or_config(&config().storage)
            .context("validate S3 storage configuration")
    }

    pub fn from_env(
        cas_dir: &Path,
        metrics: &Arc<Metrics>,
        retention: &RetentionSettings,
    ) -> Result<Self> {
        let cas = Cas::new(cas_dir)?;
        let s3_storage =
            S3Storage::from_env_or_config(&config().storage).context("initialize S3 storage")?;
        let storage: StorageRef = if let Some(s3) = s3_storage {
            info!(
                "using S3-compatible storage with build CAS at {}",
                cas_dir.display()
            );
            Arc::new(s3)
        } else {
            info!("using local storage at {}", cas_dir.display());
            local(cas_dir)?
        };
        let cache_retention =
            LocalCacheRetention::new(cas.clone(), metrics.clone(), storage.clone(), retention);
        Ok(Self {
            cas,
            storage,
            cache_retention,
        })
    }
}
