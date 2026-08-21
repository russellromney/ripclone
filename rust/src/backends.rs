//! Artifact storage and build-local paths.
//!
//! Control state is supplied by the server-owned SQLite database, or by the
//! authenticated ref API in a standalone worker. Artifact bytes remain local
//! or S3-compatible and are deliberately independent of control state.

use crate::cas::Cas;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::ref_store::RefStore;
use crate::retention::Retention;
use crate::storage::{S3Storage, StorageRef, local};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tracing::info;

fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(crate::config::load_global)
}

pub struct Backends {
    pub cas: Cas,
    pub storage: StorageRef,
    pub ref_store: Arc<dyn RefStore>,
    pub retention: Arc<Retention>,
    pub repo_root: PathBuf,
}

impl Backends {
    pub async fn from_env_with_ref_store(
        cas_dir: &Path,
        repo_root: &Path,
        metrics: &Arc<Metrics>,
        ref_store: Arc<dyn RefStore>,
    ) -> Result<Self> {
        let cas = Cas::new(cas_dir)?;
        let s3_storage =
            S3Storage::from_env_or_config(&config().storage).context("initialize S3 storage")?;
        let storage: StorageRef = if let Some(s3) = s3_storage {
            info!(
                "using S3-compatible storage with local cache at {}",
                cas_dir.display()
            );
            Arc::new(s3)
        } else {
            info!("using local storage at {}", cas_dir.display());
            local(cas_dir)?
        };
        let retention = Arc::new(
            Retention::with_config_and_storage(
                cas.clone(),
                metrics.clone(),
                Retention::parse_age(),
                Retention::parse_size(),
                Some(storage.clone()),
            )?
            .with_ref_store(ref_store.clone(), storage.clone()),
        );
        Ok(Self {
            cas,
            storage,
            ref_store,
            retention,
            repo_root: repo_root.to_path_buf(),
        })
    }
}
