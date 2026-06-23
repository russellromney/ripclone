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
    #[value(name = "editable")]
    Editable,

    /// Working tree only, materialized from the zstd files artifact. No git
    /// object database, so `git diff`/`show` do not work. Fastest path for CI
    /// jobs that only need the files.
    #[value(name = "files")]
    Files,

    /// `.git` skeleton only (commit + tree objects, prebuilt index). No working
    /// tree and no blobs.
    #[value(name = "skeleton")]
    Skeleton,
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
            "editable" => Ok(CloneMode::Editable),
            "files" => Ok(CloneMode::Files),
            "skeleton" => Ok(CloneMode::Skeleton),
            other => anyhow::bail!("unknown clone mode: {}", other),
        }
    }
}

/// Resolve a mode from the CLI argument or the `RIPCLONE_MODE` environment
/// variable, falling back to `Editable`.
pub fn resolve_mode(cli: Option<CloneMode>) -> CloneMode {
    cli.or_else(|| {
        std::env::var("RIPCLONE_MODE")
            .ok()
            .and_then(|s| s.parse().ok())
    })
    .unwrap_or_default()
}

/// Map a requested clone depth to the clonepack variant the server should
/// return. depth == 1 → the shallow (HEAD-only) clonepack; any other depth
/// (including 0 = full history) → the full clonepack.
///
/// Phase 1 only ships the HEAD-closure pack, so deeper depths currently
/// materialize the same HEAD snapshot; the plumbing is in place for Phase 2.
pub fn clonepack_kind_for_depth(depth: usize) -> &'static str {
    if depth == 1 { "shallow" } else { "full" }
}
