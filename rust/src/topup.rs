//! Exact-commit Git top-ups from an already installed cached base.
//!
//! This module deliberately knows nothing about provider credentials. The base
//! installer must configure a named remote, and authentication must be supplied
//! out of band by Git (for example a credential helper) or by a ripclone Git
//! proxy. In particular, callers should use the proxy for private GitHub App
//! repositories: installation tokens must never be embedded in a clone plan,
//! remote URL, command argument, or client configuration.
//!
//! A top-up is a transaction. The cached base is installed beside the final
//! destination, the exact caller-pinned object id is fetched and verified, and
//! only a complete, checked-out repository is published. Ref movement during
//! the operation cannot change the selected commit because no branch name is
//! fetched or resolved here.

use anyhow::{Context, Result, bail};
use std::ffi::CString;
use std::path::Path;
use std::process::{Command, Output};

/// The semantic repository shape required after the top-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopUpMode {
    /// An editable depth-one snapshot. Objects from the older base may remain
    /// dangling, but history traversal from `HEAD` stops at the pinned target.
    Head,
    /// Complete history reachable from the pinned target. The cached base must
    /// itself be a non-shallow repository.
    Full,
}

/// Inputs which are safe to receive in a clone plan.
///
/// There is intentionally no token, authorization header, or arbitrary remote
/// URL in this type. `remote` names a remote already configured by the trusted
/// base installer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedTopUp {
    pub target_commit: String,
    pub branch: String,
    pub remote: String,
    pub mode: TopUpMode,
}

impl PinnedTopUp {
    pub fn new(
        target_commit: impl Into<String>,
        branch: impl Into<String>,
        mode: TopUpMode,
    ) -> Self {
        Self {
            target_commit: target_commit.into(),
            branch: branch.into(),
            remote: "origin".to_owned(),
            mode,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopUpOutcome {
    pub target_commit: String,
    pub branch: String,
    pub mode: TopUpMode,
}

/// Fetching the pinned object failed. This is intentionally distinct from ref
/// resolution: callers must report/re-resolve rather than substitute the
/// remote's current branch tip.
#[derive(Debug)]
pub struct PinnedFetchFailed {
    pub target_commit: String,
    pub remote: String,
    pub stderr: String,
}

impl std::fmt::Display for PinnedFetchFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not fetch pinned commit {} from remote {}",
            self.target_commit, self.remote
        )?;
        if !self.stderr.trim().is_empty() {
            write!(f, ": {}", self.stderr.trim())?;
        }
        write!(
            f,
            "; the target may have become unavailable after a force-push; re-resolve explicitly"
        )
    }
}

impl std::error::Error for PinnedFetchFailed {}

/// Install `base` into private staging, top it up to one exact commit, and
/// atomically publish it at `target`.
///
/// `install_base` receives a path which does not yet exist. It must install a
/// normal non-bare Git worktree there and configure `request.remote`. It may
/// install any older compatible HEAD/full artifact; this function never mutates
/// the artifact source itself.
pub fn install_pinned_from_base<F>(
    target: impl AsRef<Path>,
    request: &PinnedTopUp,
    install_base: F,
) -> Result<TopUpOutcome>
where
    F: FnOnce(&Path) -> Result<()>,
{
    let target = target.as_ref();
    validate_request(request)?;
    if std::fs::symlink_metadata(target).is_ok() {
        bail!("clone destination already exists: {}", target.display());
    }

    let parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create clone parent {}", parent.display()))?;
    let temp = tempfile::Builder::new()
        .prefix(&format!(
            "{}.",
            target
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("ripclone"))
                .to_string_lossy()
        ))
        .suffix(".topup.tmp")
        .tempdir_in(parent)
        .context("create top-up staging directory")?;
    let staging = temp.path().join("repo");

    install_base(&staging).context("install cached base into staging")?;
    top_up_staged_repo(&staging, request)?;
    atomic_rename_noreplace(&staging, target).with_context(|| {
        format!(
            "publish completed top-up {} at {}",
            staging.display(),
            target.display()
        )
    })?;

    Ok(TopUpOutcome {
        target_commit: request.target_commit.to_ascii_lowercase(),
        branch: request.branch.clone(),
        mode: request.mode,
    })
}

fn validate_request(request: &PinnedTopUp) -> Result<()> {
    let sha = request.target_commit.as_bytes();
    if !matches!(sha.len(), 40 | 64) || !sha.iter().all(u8::is_ascii_hexdigit) {
        bail!("target_commit must be a full 40- or 64-character hexadecimal object id");
    }
    if request.remote.is_empty()
        || request.remote.starts_with('-')
        || !request
            .remote
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        bail!("remote must be a safe configured Git remote name");
    }
    if request.branch.is_empty()
        || request.branch.starts_with('-')
        || request.branch.contains('\0')
        || request.branch.contains("..")
    {
        bail!("branch is not a safe Git branch name");
    }
    let refname = format!("refs/heads/{}", request.branch);
    let status = Command::new("git")
        .args(["check-ref-format", "--branch", &request.branch])
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .context("run git check-ref-format")?;
    if !status.status.success() || refname.ends_with('/') {
        bail!("branch is not a valid Git branch name");
    }
    Ok(())
}

fn top_up_staged_repo(repo: &Path, request: &PinnedTopUp) -> Result<()> {
    ensure_worktree(repo)?;
    ensure_remote_exists(repo, &request.remote)?;

    if request.mode == TopUpMode::Full && is_shallow(repo)? {
        bail!("full top-up requires a non-shallow cached base");
    }

    let target = request.target_commit.to_ascii_lowercase();
    let mut args = vec!["fetch", "--no-tags", "--no-write-fetch-head"];
    if request.mode == TopUpMode::Head {
        args.push("--depth=1");
    }
    args.push(&request.remote);
    args.push(&target);
    let fetched = git_output(repo, &args)?;
    if !fetched.status.success() {
        return Err(anyhow::Error::new(PinnedFetchFailed {
            target_commit: target,
            remote: request.remote.clone(),
            stderr: String::from_utf8_lossy(&fetched.stderr).into_owned(),
        }));
    }

    let peeled = git_stdout(
        repo,
        &["rev-parse", "--verify", &format!("{target}^{{commit}}")],
    )
    .context("pinned object is unavailable or is not a commit")?;
    if !peeled.eq_ignore_ascii_case(&target) {
        bail!("pinned commit identity mismatch: requested {target}, Git resolved {peeled}");
    }

    let verify_ref = "refs/ripclone/topup/target";
    git_ok(repo, &["update-ref", verify_ref, &target])?;
    let fsck = git_output(
        repo,
        &["fsck", "--connectivity-only", "--no-dangling", &target],
    )?;
    if !fsck.status.success() {
        let _ = git_output(repo, &["update-ref", "-d", verify_ref]);
        bail!(
            "pinned commit {target} failed connectivity validation: {}",
            String::from_utf8_lossy(&fsck.stderr).trim()
        );
    }

    if request.mode == TopUpMode::Head {
        let count = git_stdout(repo, &["rev-list", "--count", &target])?;
        if count != "1" {
            bail!("depth-one top-up exposed {count} commits from the pinned target");
        }
    } else if is_shallow(repo)? {
        bail!("full top-up unexpectedly produced a shallow repository");
    }

    let branch_ref = format!("refs/heads/{}", request.branch);
    let remote_ref = format!("refs/remotes/{}/{}", request.remote, request.branch);
    git_ok(repo, &["update-ref", &branch_ref, &target])?;
    git_ok(repo, &["update-ref", &remote_ref, &target])?;
    git_ok(repo, &["symbolic-ref", "HEAD", &branch_ref])?;
    git_ok(repo, &["reset", "--hard", &target])?;
    git_ok(repo, &["update-ref", "-d", verify_ref])?;

    let actual = git_stdout(repo, &["rev-parse", "HEAD"])?;
    if !actual.eq_ignore_ascii_case(&target) {
        bail!("checkout substituted commit {actual} for pinned target {target}");
    }
    let clean = git_stdout(repo, &["status", "--porcelain"])?;
    if !clean.is_empty() {
        bail!("top-up produced a dirty working tree");
    }
    Ok(())
}

fn ensure_worktree(repo: &Path) -> Result<()> {
    let inside = git_stdout(repo, &["rev-parse", "--is-inside-work-tree"])
        .context("cached base is not a Git worktree")?;
    if inside != "true" {
        bail!("cached base is not a Git worktree");
    }
    Ok(())
}

fn ensure_remote_exists(repo: &Path, remote: &str) -> Result<()> {
    let output = git_output(repo, &["remote", "get-url", remote])?;
    if !output.status.success() {
        bail!("cached base does not configure remote {remote}");
    }
    let configured_url = String::from_utf8_lossy(&output.stdout);
    if let Ok(url) = url::Url::parse(configured_url.trim())
        && matches!(url.scheme(), "http" | "https")
        && (!url.username().is_empty() || url.password().is_some())
    {
        bail!(
            "cached base remote embeds HTTP credentials; use a credential helper or ripclone proxy"
        );
    }
    Ok(())
}

fn is_shallow(repo: &Path) -> Result<bool> {
    Ok(git_stdout(repo, &["rev-parse", "--is-shallow-repository"])? == "true")
}

fn git_stdout(repo: &Path, args: &[&str]) -> Result<String> {
    let output = git_output(repo, args)?;
    if !output.status.success() {
        bail!(
            "git {} failed in {}: {}",
            args.join(" "),
            repo.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_ok(repo: &Path, args: &[&str]) -> Result<()> {
    git_stdout(repo, args).map(|_| ())
}

fn git_output(repo: &Path, args: &[&str]) -> Result<Output> {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .with_context(|| format!("run git {} in {}", args.join(" "), repo.display()))
}

#[cfg(target_os = "linux")]
fn atomic_rename_noreplace(from: &Path, to: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let from = CString::new(from.as_os_str().as_bytes()).context("staging path contains NUL")?;
    let to = CString::new(to.as_os_str().as_bytes()).context("target path contains NUL")?;
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("atomic no-replace rename")
    }
}

#[cfg(target_os = "macos")]
fn atomic_rename_noreplace(from: &Path, to: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let from = CString::new(from.as_os_str().as_bytes()).context("staging path contains NUL")?;
    let to = CString::new(to.as_os_str().as_bytes()).context("target path contains NUL")?;
    let rc = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("atomic no-replace rename")
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn atomic_rename_noreplace(from: &Path, to: &Path) -> Result<()> {
    // Best available std fallback. Supported production Unix targets use the
    // genuinely atomic implementations above.
    if to.exists() {
        bail!("clone destination appeared while top-up was running");
    }
    std::fs::rename(from, to).context("rename completed top-up")
}
