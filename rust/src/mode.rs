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
}

impl CloneMode {
    /// True for modes that download and install the prebuilt depth pack.
    pub fn needs_prebuilt_blob_pack(self) -> bool {
        matches!(self, CloneMode::Editable)
    }
}

impl FromStr for CloneMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "editable" => Ok(CloneMode::Editable),
            "files" => Ok(CloneMode::Files),
            other => anyhow::bail!("unknown clone mode: {}", other),
        }
    }
}
