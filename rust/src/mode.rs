use clap::ValueEnum;
use std::str::FromStr;

/// User-facing clone mode. Determines which artifacts the client downloads and
/// how the working tree is materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum CloneMode {
    /// Default. Complete `.git`: the working tree is materialized directly from
    /// archive chunks, and HEAD blobs are written into `.git/objects` as a
    /// locally-built packfile. Matches normal `git clone` expectations without
    /// the redundant head-blobs download.
    #[default]
    #[value(name = "full")]
    Full,

    /// Working tree only, materialized directly from archive chunks. HEAD blobs
    /// are not present in `.git/objects`, so `git diff`/`git show` do not work.
    #[value(name = "fast")]
    Fast,

    /// Complete `.git` like `Full`, but HEAD blobs come from a pre-built
    /// head-blobs pack downloaded in parallel with the archive chunks instead
    /// of being recompressed locally. Faster when bandwidth is plentiful;
    /// slower on very constrained links because it downloads extra bytes.
    #[value(name = "hybrid")]
    Hybrid,

    /// `.git` skeleton only (commit + tree objects, prebuilt index). No working
    /// tree and no head blobs.
    #[value(name = "skeleton")]
    Skeleton,
}

impl CloneMode {
    /// True for modes that build a local blob pack from extracted archive bytes.
    pub fn needs_blob_pack(self) -> bool {
        matches!(self, CloneMode::Full)
    }

    /// True for modes that download a pre-built head-blobs pack in parallel
    /// with the archive.
    pub fn needs_prebuilt_blob_pack(self) -> bool {
        matches!(self, CloneMode::Hybrid)
    }

    pub fn needs_archive(self) -> bool {
        matches!(self, CloneMode::Full | CloneMode::Fast | CloneMode::Hybrid)
    }

    pub fn needs_worktree(self) -> bool {
        matches!(self, CloneMode::Full | CloneMode::Fast | CloneMode::Hybrid)
    }

    pub fn needs_checkout(self) -> bool {
        // The working tree is always materialized directly from the archive;
        // no separate git checkout-index step is required.
        false
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
