use clap::ValueEnum;
use std::str::FromStr;

/// User-facing clone mode. Determines which artifacts the client downloads and
/// how the working tree is materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum CloneMode {
    /// Default. Complete `.git` with head-blobs pack; `git checkout-index`
    /// materializes the working tree. Matches normal `git clone` expectations.
    #[default]
    #[value(name = "full")]
    Full,

    /// Working tree only, materialized directly from archive chunks. HEAD blobs
    /// are not present in `.git/objects`, so `git diff`/`git show` do not work.
    #[value(name = "fast")]
    Fast,

    /// Both archive extraction and head-blobs download run concurrently. The CLI
    /// blocks until the working tree is written and the head-blobs pack is in
    /// `.git`.
    #[value(name = "hybrid")]
    Hybrid,

    /// `.git` skeleton only (commit + tree objects, prebuilt index). No working
    /// tree and no head blobs.
    #[value(name = "skeleton")]
    Skeleton,
}

impl CloneMode {
    pub fn needs_head_blobs(self) -> bool {
        matches!(self, CloneMode::Full | CloneMode::Hybrid)
    }

    pub fn needs_archive(self) -> bool {
        matches!(self, CloneMode::Fast | CloneMode::Hybrid)
    }

    pub fn needs_worktree(self) -> bool {
        matches!(self, CloneMode::Full | CloneMode::Fast | CloneMode::Hybrid)
    }

    pub fn needs_checkout(self) -> bool {
        // Skeleton has no working tree; fast/hybrid use archive extraction.
        matches!(self, CloneMode::Full)
    }
}

impl FromStr for CloneMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "full" => Ok(CloneMode::Full),
            "fast" => Ok(CloneMode::Fast),
            "hybrid" => Ok(CloneMode::Hybrid),
            "skeleton" => Ok(CloneMode::Skeleton),
            other => anyhow::bail!("unknown clone mode: {}", other),
        }
    }
}

/// Resolve a mode from the CLI argument or the `RIPCLONE_MODE` environment
/// variable, falling back to `Full`.
pub fn resolve_mode(cli: Option<CloneMode>) -> CloneMode {
    cli.or_else(|| {
        std::env::var("RIPCLONE_MODE")
            .ok()
            .and_then(|s| s.parse().ok())
    })
    .unwrap_or_default()
}
