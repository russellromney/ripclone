//! Server-side repository build configuration.
//!
//! A `RepoConfig` tells the server how to build a repository's clonepacks:
//! which depth variants to produce, the zstd compression level,
//! archive/head-blobs chunk sizes, and an optional dictionary. The server-owned
//! control database stores one record per repository. Admission snapshots the
//! validated result into the durable job, so workers never read live config.
//!
//! A repo with no stored config uses [`RepoConfig::default`], which reproduces
//! today's behavior exactly: a `shallow` (depth 1) and a `full` (unlimited)
//! clonepack and zstd level 6.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Default zstd compression level used for archive frames (matches the level the
/// build used before this config existed).
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 6;
/// Name of the built-in depth-1 variant.
pub const SHALLOW_VARIANT: &str = "shallow";
/// Name of the built-in unlimited-history variant.
pub const FULL_VARIANT: &str = "full";
/// One named clonepack depth. `depth: None` means unlimited (full history);
/// `depth: Some(n)` bounds it to the last `n` commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSpec {
    pub name: String,
    #[serde(default)]
    pub depth: Option<usize>,
}

/// Per-repository build configuration. Every field is optional; an empty
/// config behaves exactly like the documented defaults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Target compressed size of each archive chunk, in bytes.
    #[serde(default)]
    pub archive_chunk_size: Option<u64>,
    /// Target size of each head-blobs pack chunk, in bytes.
    #[serde(default)]
    pub head_blobs_chunk_size: Option<u64>,
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
        cfg.validate().unwrap();
    }

    #[test]
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
    fn deserialize_rejects_removed_configuration_fields() {
        let hot_files = serde_json::from_str::<RepoConfig>(r#"{"hot_files":["src/**"]}"#)
            .expect_err("removed hot_files must not be silently ignored");
        assert!(hot_files.to_string().contains("unknown field `hot_files`"));

        let enabled_modes = serde_json::from_str::<RepoConfig>(r#"{"enabled_modes":["files"]}"#)
            .expect_err("removed enabled_modes must not be silently ignored");
        assert!(
            enabled_modes
                .to_string()
                .contains("unknown field `enabled_modes`")
        );

        let nested = serde_json::from_str::<RepoConfig>(
            r#"{"clonepack_depths":[{"name":"shallow","depth":1,"unexpected":true}]}"#,
        )
        .expect_err("nested unknown clonepack fields must not be silently ignored");
        assert!(nested.to_string().contains("unknown field `unexpected`"));
    }
}
