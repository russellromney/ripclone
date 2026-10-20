//! Artifact storage and build-local paths.
//!
//! Control state is supplied by the server-owned SQLite database. Artifact
//! bytes remain local or S3-compatible and are deliberately independent of
//! control state.

use crate::cas::Cas;
use crate::metrics::Metrics;
use crate::retention::{LocalCacheRetention, RetentionSettings};
use crate::storage::{S3Storage, StorageRef, local};
use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

pub struct Backends {
    pub cas: Cas,
    pub storage: StorageRef,
    pub cache_retention: LocalCacheRetention,
}

impl Backends {
    pub fn validate_environment() -> Result<()> {
        S3Storage::from_env()
            .map(|_| ())
            .context("validate S3 storage configuration")
    }

    pub fn from_env(
        cas_dir: &Path,
        metrics: &Arc<Metrics>,
        retention: &RetentionSettings,
    ) -> Result<Self> {
        let s3_storage = S3Storage::from_env().context("initialize S3 storage")?;
        // With remote storage the build CAS is a cache of what was uploaded,
        // so it skips directory syncs. Local storage is the CAS directory
        // itself, so every object it reports stored must survive a crash.
        let (cas, storage): (Cas, StorageRef) = if let Some(s3) = s3_storage {
            info!(
                "using S3-compatible storage with build CAS at {}",
                cas_dir.display()
            );
            (Cas::cache(cas_dir)?, Arc::new(s3))
        } else {
            info!("using local storage at {}", cas_dir.display());
            (Cas::new(cas_dir)?, local(cas_dir)?)
        };
        let cache_retention =
            LocalCacheRetention::new(cas.clone(), metrics.clone(), storage.clone(), retention)?;
        Ok(Self {
            cas,
            storage,
            cache_retention,
        })
    }
}
