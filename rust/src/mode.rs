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

/// Resolve a mode from the CLI argument, the `RIPCLONE_MODE` environment
/// variable, or a config file value, falling back to `Editable`.
pub fn resolve_mode(cli: Option<CloneMode>, config: Option<&str>) -> anyhow::Result<CloneMode> {
    resolve_mode_from(cli, std::env::var("RIPCLONE_MODE"), config)
}

/// [`resolve_mode`] with the `RIPCLONE_MODE` lookup result given.
fn resolve_mode_from(
    cli: Option<CloneMode>,
    env: Result<String, std::env::VarError>,
    config: Option<&str>,
) -> anyhow::Result<CloneMode> {
    if let Some(mode) = cli {
        return Ok(mode);
    }
    match env {
        Ok(value) => {
            return value.parse().map_err(|error| {
                anyhow::anyhow!("invalid RIPCLONE_MODE value {value:?}: {error}")
            });
        }
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(anyhow::Error::new(error).context("read RIPCLONE_MODE")),
    }
    match config {
        Some(value) => value
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid clone.mode value {value:?}: {error}")),
        None => Ok(CloneMode::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::VarError;

    /// CLI wins, then `RIPCLONE_MODE`, then config, then Editable. An invalid
    /// value at the level that applies is an error, never a fallback.
    #[test]
    fn resolve_mode_precedence_and_invalid_values() {
        let set = |v: &str| Ok(v.to_string());
        let unset = || Err(VarError::NotPresent);
        for (case, cli, env, config, want) in [
            (
                "cli wins",
                Some(CloneMode::Files),
                set("fast"),
                Some("fast"),
                Some(CloneMode::Files),
            ),
            (
                "env",
                None,
                set("files"),
                Some("editable"),
                Some(CloneMode::Files),
            ),
            ("env invalid", None, set("fast"), Some("files"), None),
            (
                "env not unicode",
                None,
                Err(VarError::NotUnicode("x".into())),
                None,
                None,
            ),
            (
                "config",
                None,
                unset(),
                Some("files"),
                Some(CloneMode::Files),
            ),
            ("config invalid", None, unset(), Some("fast"), None),
            ("default", None, unset(), None, Some(CloneMode::Editable)),
        ] {
            assert_eq!(resolve_mode_from(cli, env, config).ok(), want, "{case}");
        }
    }
}
