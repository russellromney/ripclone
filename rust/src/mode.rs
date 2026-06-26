use clap::ValueEnum;
use std::str::FromStr;

/// User-facing clone mode. Determines which artifacts the client downloads and
/// how the working tree is materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum CloneMode {
    /// Default. A real, editable git repo. Downloads one undeltified pack (the
    /// depth pack: commit + tree + every blob for the requested depth), installs
    /// it into `.git/objects`, and materializes the working tree by reading
    /// blobs straight out of it. `git diff`/`show`/`log` and edits/commits all
    /// work. One download of HEAD content, no archive, no local pack rebuild.
    #[default]
    #[value(name = "editable", alias = "full", alias = "hybrid")]
    Editable,

    /// Working tree only, materialized from the zstd files artifact. No git
    /// object database, so `git diff`/`show` do not work. Fastest path for CI
    /// jobs that only need the files.
    #[value(name = "files", alias = "fast")]
    Files,
}

impl CloneMode {
    /// True for modes that build a local blob pack from extracted archive bytes.
    /// Always false now: `Editable` installs a prebuilt depth pack instead.
    pub fn needs_blob_pack(self) -> bool {
        false
    }

    /// True for modes that download and install the prebuilt depth pack.
    pub fn needs_prebuilt_blob_pack(self) -> bool {
        matches!(self, CloneMode::Editable)
    }

    /// True for modes that materialize the working tree from the zstd files
    /// artifact (archive chunks).
    pub fn needs_archive(self) -> bool {
        matches!(self, CloneMode::Files)
    }

    pub fn needs_worktree(self) -> bool {
        matches!(self, CloneMode::Editable | CloneMode::Files)
    }

    /// True for modes that materialize the working tree by reading blobs out of
    /// the installed depth pack (the single-download editable path).
    pub fn needs_pack_worktree(self) -> bool {
        matches!(self, CloneMode::Editable)
    }

    pub fn needs_checkout(self) -> bool {
        // The working tree is always materialized directly (from the pack or the
        // files artifact); no separate git checkout-index step is required.
        false
    }
}

impl FromStr for CloneMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            // Current names.
            "editable" => Ok(CloneMode::Editable),
            "files" => Ok(CloneMode::Files),
            // Deprecated aliases.
            "full" | "hybrid" => Ok(CloneMode::Editable),
            "fast" => Ok(CloneMode::Files),
            "skeleton" => anyhow::bail!(
                "skeleton mode is no longer exposed; use mount for skeleton-backed access"
            ),
            other => anyhow::bail!("unknown clone mode: {}", other),
        }
    }
}

/// Resolve a mode from the CLI argument, the `RIPCLONE_MODE` environment
/// variable, or a config file value, falling back to `Editable`.
pub fn resolve_mode(cli: Option<CloneMode>, config: Option<&str>) -> CloneMode {
    cli.or_else(|| {
        std::env::var("RIPCLONE_MODE")
            .ok()
            .and_then(|s| s.parse().ok())
    })
    .or_else(|| config.and_then(|s| s.parse().ok()))
    .unwrap_or_default()
}

/// Map a requested clone depth to the clonepack variant the server should
/// return. depth == 1 → the shallow (HEAD-only) clonepack; depth == 0 (full
/// history) → the full clonepack.
///
/// Only depths 0 and 1 are meaningful: arbitrary depth-N shallow clones are not
/// implemented, and callers (the CLI and the git remote helper) reject `N > 1`
/// with a clear error rather than silently serving full history that git would
/// record as a complete, non-shallow clone (P1). This mapping still treats any
/// other value as full as a defensive default.
pub fn clonepack_kind_for_depth(depth: usize) -> &'static str {
    if depth == 1 { "shallow" } else { "full" }
}
