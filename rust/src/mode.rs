use clap::ValueEnum;
use std::str::FromStr;

/// User-facing clone mode. Determines which artifacts the client downloads and
/// how the working tree is materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum CloneMode {
    /// Default. A git repository: log, diff, commit, pull and push work. Full
    /// history, or only the commit with `--depth 1`.
    #[default]
    #[value(name = "editable")]
    Editable,

    /// The files only, with no `.git`, so git commands do not work. The
    /// fastest mode, for jobs that only read the files.
    #[value(name = "files")]
    Files,
}

impl CloneMode {
    /// True for modes that install git objects.
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
