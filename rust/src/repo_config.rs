//! Server-side repository build configuration.
//!
//! A `RepoConfig` holds the zstd compression level for a repository's Files
//! archive. The server-owned control database stores one record per
//! repository; a Full and Files build reads it when it starts.
//!
//! A repo with no stored config uses [`RepoConfig::default`]: zstd level 6.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Default zstd compression level used for archive frames.
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 6;

/// Per-repository build configuration. Every field is optional; an empty
/// config uses the defaults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepoConfig {
    /// zstd compression level for archive frames.
    #[serde(default)]
    pub compression_level: Option<i32>,
    /// Removed keys: nothing read them. Each is accepted and dropped so a
    /// stored config that still has it loads; never written.
    #[serde(default, rename = "clonepack_depths", skip_serializing)]
    pub removed_clonepack_depths: serde::de::IgnoredAny,
    #[serde(default, rename = "archive_chunk_size", skip_serializing)]
    pub removed_archive_chunk_size: serde::de::IgnoredAny,
    #[serde(default, rename = "head_blobs_chunk_size", skip_serializing)]
    pub removed_head_blobs_chunk_size: serde::de::IgnoredAny,
}

impl RepoConfig {
    /// zstd level to compress archive frames with.
    pub fn compression_level(&self) -> i32 {
        self.compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL)
    }

    /// Validate the config. Returns an error describing the first problem.
    pub fn validate(&self) -> Result<()> {
        if let Some(level) = self.compression_level
            && !(1..=22).contains(&level)
        {
            anyhow::bail!("compression_level must be between 1 and 22, got {level}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_defaults_and_rejects_bad_levels() {
        let level = |compression_level| RepoConfig {
            compression_level,
            ..Default::default()
        };
        for (cfg, ok) in [
            (RepoConfig::default(), true),
            (level(Some(1)), true),
            (level(Some(22)), true),
            (level(Some(0)), false),
            (level(Some(99)), false),
        ] {
            assert_eq!(cfg.validate().is_ok(), ok, "{cfg:?}");
        }
    }

    #[test]
    fn removed_keys_are_accepted_and_dropped() {
        let stored = r#"{
            "compression_level": 9,
            "clonepack_depths": [{"name": "shallow", "depth": 1}, {"name": "full"}],
            "archive_chunk_size": 8388608,
            "head_blobs_chunk_size": 4194304
        }"#;
        let config: RepoConfig = serde_json::from_str(stored).unwrap();
        assert_eq!(
            config,
            RepoConfig {
                compression_level: Some(9),
                ..Default::default()
            }
        );
        assert_eq!(
            serde_json::to_string(&config).unwrap(),
            r#"{"compression_level":9}"#
        );
        assert!(serde_json::from_str::<RepoConfig>(r#"{"other":1}"#).is_err());
    }
}
