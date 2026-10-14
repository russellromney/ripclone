//! Server-side repository build configuration.
//!
//! A `RepoConfig` tells the server how to build a repository's clonepacks:
//! which depth variants to produce, the zstd compression level,
//! and the archive chunk size. The server-owned
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
    /// Target compressed size of each archive chunk, in bytes.
    #[serde(default)]
    pub archive_chunk_size: Option<u64>,
    /// Removed: nothing read it. Accepted and dropped so a stored config that
    /// still has the key loads; never written.
    #[serde(default, rename = "head_blobs_chunk_size", skip_serializing)]
    pub removed_head_blobs_chunk_size: serde::de::IgnoredAny,
}

impl RepoConfig {
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
    fn validate_accepts_defaults_and_rejects_bad_values() {
        let depth = |name: &str, depth| DepthSpec {
            name: name.into(),
            depth,
        };
        let depths = |clonepack_depths| RepoConfig {
            clonepack_depths,
            ..Default::default()
        };
        for (case, cfg, ok) in [
            ("default", RepoConfig::default(), true),
            (
                "compression 99",
                RepoConfig {
                    compression_level: Some(99),
                    ..Default::default()
                },
                false,
            ),
            ("depth 0", depths(vec![depth("x", Some(0))]), false),
            (
                "three structural variants",
                depths(vec![
                    depth("shallow", Some(1)),
                    depth("recent", Some(50)),
                    depth("full", None),
                ]),
                false,
            ),
            (
                "duplicate names",
                depths(vec![depth("dup", Some(1)), depth("dup", None)]),
                false,
            ),
        ] {
            assert_eq!(cfg.validate().is_ok(), ok, "{case}");
        }
    }

    #[test]
    fn removed_head_blobs_chunk_size_is_accepted_and_dropped() {
        let stored = r#"{"compression_level":9,"head_blobs_chunk_size":4194304}"#;
        let config: RepoConfig = serde_json::from_str(stored).unwrap();
        assert_eq!(
            config,
            RepoConfig {
                compression_level: Some(9),
                ..Default::default()
            }
        );
        let written = serde_json::to_string(&config).unwrap();
        assert!(!written.contains("head_blobs"), "{written}");
        assert!(serde_json::from_str::<RepoConfig>(r#"{"other":1}"#).is_err());
    }
}
