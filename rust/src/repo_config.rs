//! Server-side per-repo / per-branch build configuration (ROADMAP §2a).
//!
//! A `RepoConfig` tells the server how to build a repo's clonepacks: which depth
//! variants to produce, the zstd compression level, archive/head-blobs chunk
//! sizes, hot-file count, an optional dictionary, and which clone modes are
//! enabled. It is stored in the same backend as artifacts (file locally, S3 in
//! production) via the storage layer's keyed metadata objects, written live by
//! the admin endpoint and read fresh on each build — no restart needed.
//!
//! Config is read only at build time. The build records the resulting variant
//! names and enabled modes into the `RefInfo`, so the resolve/clone hot path
//! never has to read config.
//!
//! A repo with no stored config uses [`RepoConfig::default`], which reproduces
//! today's behavior exactly: a `shallow` (depth 1) and a `full` (unlimited)
//! clonepack, zstd level 6, all modes enabled.

use crate::provider::RepoId;
use crate::storage::StorageRef;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Default zstd compression level used for archive frames (matches the level the
/// build used before this config existed).
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 6;
/// Name of the built-in depth-1 variant.
pub const SHALLOW_VARIANT: &str = "shallow";
/// Name of the built-in unlimited-history variant.
pub const FULL_VARIANT: &str = "full";
/// All clone modes the server knows how to serve.
pub const ALL_MODES: [&str; 4] = ["full", "fast", "hybrid", "skeleton"];

/// One named clonepack depth. `depth: None` means unlimited (full history);
/// `depth: Some(n)` bounds it to the last `n` commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthSpec {
    pub name: String,
    #[serde(default)]
    pub depth: Option<usize>,
}

/// Per-repo / per-branch build configuration. Every field is optional so a
/// partial config (e.g. only `compression_level`) merges cleanly over the
/// defaults; an empty config behaves exactly like today.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RepoConfig {
    /// Named depth variants to build. Empty = the default `shallow` + `full`.
    #[serde(default)]
    pub clonepack_depths: Vec<DepthSpec>,
    /// zstd compression level for archive frames.
    #[serde(default)]
    pub compression_level: Option<i32>,
    /// Identifier of a trained zstd dictionary to compress with (stored for
    /// forward compatibility; dictionary lookup is a follow-up).
    #[serde(default)]
    pub dictionary_id: Option<String>,
    /// Number of hot files to surface in snapshot/hotfiles responses.
    #[serde(default)]
    pub hot_files: Option<usize>,
    /// Target compressed size of each archive chunk, in bytes.
    #[serde(default)]
    pub archive_chunk_size: Option<u64>,
    /// Target size of each head-blobs pack chunk, in bytes.
    #[serde(default)]
    pub head_blobs_chunk_size: Option<u64>,
    /// Clone modes a client may request. `None` = all modes enabled.
    #[serde(default)]
    pub enabled_modes: Option<Vec<String>>,
}

impl RepoConfig {
    /// The depth variants to build: the configured set, or the built-in
    /// `shallow` + `full` when none are configured.
    pub fn effective_depths(&self) -> Vec<DepthSpec> {
        if self.clonepack_depths.is_empty() {
            vec![
                DepthSpec {
                    name: SHALLOW_VARIANT.to_string(),
                    depth: Some(1),
                },
                DepthSpec {
                    name: FULL_VARIANT.to_string(),
                    depth: None,
                },
            ]
        } else {
            self.clonepack_depths.clone()
        }
    }

    /// The single finite-depth ("shallow"-slot) variant, if configured.
    pub fn shallow_variant(&self) -> Option<DepthSpec> {
        self.effective_depths()
            .into_iter()
            .find(|d| d.depth.is_some())
    }

    /// The unlimited-depth ("full"-slot) variant, if configured.
    pub fn full_variant(&self) -> Option<DepthSpec> {
        self.effective_depths()
            .into_iter()
            .find(|d| d.depth.is_none())
    }

    /// zstd level to compress archive frames with.
    pub fn compression_level(&self) -> i32 {
        self.compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL)
    }

    /// True if `mode` may be served under this config.
    pub fn mode_enabled(&self, mode: &str) -> bool {
        match &self.enabled_modes {
            None => true,
            Some(modes) => modes.iter().any(|m| m == mode),
        }
    }

    /// Field-level overlay of `branch` config over `self` (the repo-level
    /// config): each field the branch sets wins; unset branch fields keep the
    /// repo value. This is how branch entries override repo entries.
    pub fn overlay(&self, branch: &RepoConfig) -> RepoConfig {
        RepoConfig {
            clonepack_depths: if branch.clonepack_depths.is_empty() {
                self.clonepack_depths.clone()
            } else {
                branch.clonepack_depths.clone()
            },
            compression_level: branch.compression_level.or(self.compression_level),
            dictionary_id: branch
                .dictionary_id
                .clone()
                .or_else(|| self.dictionary_id.clone()),
            hot_files: branch.hot_files.or(self.hot_files),
            archive_chunk_size: branch.archive_chunk_size.or(self.archive_chunk_size),
            head_blobs_chunk_size: branch.head_blobs_chunk_size.or(self.head_blobs_chunk_size),
            enabled_modes: branch
                .enabled_modes
                .clone()
                .or_else(|| self.enabled_modes.clone()),
        }
    }

    /// Validate the config. Returns an error describing the first problem.
    ///
    /// Option A supports exactly the two structural variants the build can emit
    /// today: one finite-depth ("shallow") variant and one unlimited ("full")
    /// variant. Configs that would need three-plus simultaneous depths are
    /// rejected with a clear message until the multi-variant build lands.
    pub fn validate(&self) -> Result<()> {
        if let Some(level) = self.compression_level
            && !(1..=22).contains(&level)
        {
            anyhow::bail!("compression_level must be between 1 and 22, got {level}");
        }
        if let Some(0) = self.archive_chunk_size {
            anyhow::bail!("archive_chunk_size must be greater than zero");
        }
        if let Some(0) = self.head_blobs_chunk_size {
            anyhow::bail!("head_blobs_chunk_size must be greater than zero");
        }
        if let Some(modes) = &self.enabled_modes {
            if modes.is_empty() {
                anyhow::bail!("enabled_modes must list at least one mode");
            }
            for m in modes {
                if !ALL_MODES.contains(&m.as_str()) {
                    anyhow::bail!(
                        "unknown mode {m:?}; valid modes are {}",
                        ALL_MODES.join(", ")
                    );
                }
            }
        }

        let mut names = std::collections::HashSet::new();
        let mut finite = 0usize;
        let mut unlimited = 0usize;
        for spec in &self.clonepack_depths {
            if spec.name.trim().is_empty() {
                anyhow::bail!("clonepack depth name must not be empty");
            }
            if !names.insert(spec.name.clone()) {
                anyhow::bail!("duplicate clonepack depth name {:?}", spec.name);
            }
            match spec.depth {
                Some(0) => anyhow::bail!("clonepack depth for {:?} must be >= 1", spec.name),
                Some(_) => finite += 1,
                None => unlimited += 1,
            }
        }
        if finite > 1 || unlimited > 1 {
            anyhow::bail!(
                "at most one finite-depth and one unlimited (full) clonepack are supported \
                 today; multiple simultaneous finite depths require the multi-variant build \
                 (not yet implemented)"
            );
        }
        Ok(())
    }
}

/// Storage key for a repo-level config object.
fn repo_key(repo_id: &RepoId) -> String {
    format!("repo-config/{}.json", repo_id.storage_key())
}

/// Storage key for a branch-level config object. The branch is slugified so
/// `feature/x` can't introduce extra path segments.
fn branch_key(repo_id: &RepoId, branch: &str) -> String {
    format!(
        "repo-config/{}/branches/{}.json",
        repo_id.storage_key(),
        branch_slug(branch)
    )
}

/// Make a branch name safe for a single key segment (mirrors the ref store).
fn branch_slug(branch: &str) -> String {
    branch
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => c,
            _ => '_',
        })
        .collect()
}

/// Reads and writes [`RepoConfig`] objects in the same storage backend as
/// artifacts (file or S3), via the keyed-metadata API. Config is read at build
/// time only, so this is deliberately cache-free: a write is visible to the next
/// build immediately, across processes.
pub struct RepoConfigStore {
    storage: StorageRef,
}

impl RepoConfigStore {
    pub fn new(storage: StorageRef) -> Self {
        Self { storage }
    }

    /// Load the raw repo-level config, if any.
    pub async fn get_repo(&self, repo_id: &RepoId) -> Result<Option<RepoConfig>> {
        self.load(&repo_key(repo_id)).await
    }

    /// Load the raw branch-level config, if any.
    pub async fn get_branch(&self, repo_id: &RepoId, branch: &str) -> Result<Option<RepoConfig>> {
        self.load(&branch_key(repo_id, branch)).await
    }

    /// The effective config for `repo_id`@`branch`: the repo-level config with
    /// the branch-level config overlaid on top, or [`RepoConfig::default`] when
    /// neither is stored.
    pub async fn effective(&self, repo_id: &RepoId, branch: &str) -> Result<RepoConfig> {
        let repo = self.get_repo(repo_id).await?.unwrap_or_default();
        let merged = match self.get_branch(repo_id, branch).await? {
            Some(branch_cfg) => repo.overlay(&branch_cfg),
            None => repo,
        };
        Ok(merged)
    }

    /// Store a repo-level config (validated by the caller).
    pub async fn put_repo(&self, repo_id: &RepoId, config: &RepoConfig) -> Result<()> {
        self.store(&repo_key(repo_id), config).await
    }

    /// Store a branch-level config (validated by the caller).
    pub async fn put_branch(
        &self,
        repo_id: &RepoId,
        branch: &str,
        config: &RepoConfig,
    ) -> Result<()> {
        self.store(&branch_key(repo_id, branch), config).await
    }

    async fn load(&self, key: &str) -> Result<Option<RepoConfig>> {
        match self.storage.get_meta(key).await? {
            Some(bytes) => {
                let cfg = serde_json::from_slice(&bytes)
                    .with_context(|| format!("parse repo config {key}"))?;
                Ok(Some(cfg))
            }
            None => Ok(None),
        }
    }

    async fn store(&self, key: &str, config: &RepoConfig) -> Result<()> {
        let data = serde_json::to_vec_pretty(config).context("serialize repo config")?;
        self.storage.put_meta(key, &data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_reproduces_shallow_and_full() {
        let cfg = RepoConfig::default();
        let depths = cfg.effective_depths();
        assert_eq!(depths.len(), 2);
        assert_eq!(
            cfg.shallow_variant().unwrap(),
            DepthSpec {
                name: "shallow".into(),
                depth: Some(1)
            }
        );
        assert_eq!(
            cfg.full_variant().unwrap(),
            DepthSpec {
                name: "full".into(),
                depth: None
            }
        );
        assert_eq!(cfg.compression_level(), DEFAULT_COMPRESSION_LEVEL);
        assert!(cfg.mode_enabled("full"));
        assert!(cfg.mode_enabled("skeleton"));
        cfg.validate().unwrap();
    }

    #[test]
    fn branch_overlay_overrides_only_set_fields() {
        let repo = RepoConfig {
            compression_level: Some(9),
            hot_files: Some(20),
            ..Default::default()
        };
        let branch = RepoConfig {
            hot_files: Some(50),
            ..Default::default()
        };
        let merged = repo.overlay(&branch);
        // Branch overrides hot_files but inherits the repo compression level.
        assert_eq!(merged.hot_files, Some(50));
        assert_eq!(merged.compression_level, Some(9));
    }

    #[test]
    fn branch_depths_replace_repo_depths_wholesale() {
        let repo = RepoConfig {
            clonepack_depths: vec![DepthSpec {
                name: "shallow".into(),
                depth: Some(1),
            }],
            ..Default::default()
        };
        let branch = RepoConfig {
            clonepack_depths: vec![DepthSpec {
                name: "full".into(),
                depth: None,
            }],
            ..Default::default()
        };
        let merged = repo.overlay(&branch);
        assert_eq!(merged.clonepack_depths, branch.clonepack_depths);
    }

    #[test]
    fn validate_rejects_bad_values() {
        assert!(
            RepoConfig {
                compression_level: Some(99),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            RepoConfig {
                enabled_modes: Some(vec!["bogus".into()]),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            RepoConfig {
                enabled_modes: Some(vec![]),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            RepoConfig {
                clonepack_depths: vec![DepthSpec {
                    name: "x".into(),
                    depth: Some(0)
                }],
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn validate_rejects_more_than_two_structural_variants() {
        // Two finite depths need the deferred multi-variant build.
        let cfg = RepoConfig {
            clonepack_depths: vec![
                DepthSpec {
                    name: "shallow".into(),
                    depth: Some(1),
                },
                DepthSpec {
                    name: "recent".into(),
                    depth: Some(50),
                },
                DepthSpec {
                    name: "full".into(),
                    depth: None,
                },
            ],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_names() {
        let cfg = RepoConfig {
            clonepack_depths: vec![
                DepthSpec {
                    name: "dup".into(),
                    depth: Some(1),
                },
                DepthSpec {
                    name: "dup".into(),
                    depth: None,
                },
            ],
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn mode_gating() {
        let cfg = RepoConfig {
            enabled_modes: Some(vec!["full".into(), "skeleton".into()]),
            ..Default::default()
        };
        assert!(cfg.mode_enabled("full"));
        assert!(!cfg.mode_enabled("fast"));
    }

    #[tokio::test]
    async fn store_round_trips_repo_and_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = crate::storage::local(tmp.path()).unwrap();
        let store = RepoConfigStore::new(storage);
        let repo = RepoId::github("acme/widget");

        // Absent until written.
        assert!(store.get_repo(&repo).await.unwrap().is_none());
        assert_eq!(
            store.effective(&repo, "main").await.unwrap(),
            RepoConfig::default()
        );

        let repo_cfg = RepoConfig {
            compression_level: Some(10),
            hot_files: Some(7),
            ..Default::default()
        };
        store.put_repo(&repo, &repo_cfg).await.unwrap();
        assert_eq!(store.get_repo(&repo).await.unwrap().unwrap(), repo_cfg);

        // Branch override merges over the repo config.
        let branch_cfg = RepoConfig {
            hot_files: Some(99),
            ..Default::default()
        };
        store
            .put_branch(&repo, "release", &branch_cfg)
            .await
            .unwrap();
        let effective = store.effective(&repo, "release").await.unwrap();
        assert_eq!(effective.hot_files, Some(99));
        assert_eq!(effective.compression_level, Some(10));

        // A different branch with no entry sees only the repo config.
        let other = store.effective(&repo, "main").await.unwrap();
        assert_eq!(other.hot_files, Some(7));
    }
}
