//! Exact-commit Git top-ups from an already installed cached base.
//!
//! This module deliberately knows nothing about provider credentials. The base
//! installer must configure a named remote. Private authentication must be
//! supplied by a ripclone Git proxy; ambient and cached-base Git credential
//! configuration is deliberately disabled. Callers must use the proxy for private GitHub App
//! repositories: installation tokens must never be embedded in a clone plan,
//! remote URL, command argument, or client configuration.
//!
//! A top-up is a transaction. The cached base is installed beside the final
//! destination, the exact caller-pinned object id is fetched and verified, and
//! only a complete, checked-out repository is published. Ref movement during
//! the operation cannot change the selected commit because no branch name is
//! fetched or resolved here.

use anyhow::{Context, Result, bail};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::CString;
use std::path::{Path, PathBuf};
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
}

impl std::fmt::Display for PinnedFetchFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not fetch pinned commit {} from remote {}",
            self.target_commit, self.remote
        )?;
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
    top_up_staged_repo(&staging, temp.path(), request)?;
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

fn top_up_staged_repo(repo: &Path, staging_root: &Path, request: &PinnedTopUp) -> Result<()> {
    let layout = validate_self_contained_layout(repo, staging_root)?;
    let config = read_and_validate_local_config(&layout.config)?;
    validate_all_remote_urls(&config)?;
    let remote_url = configured_remote_url(&config, &request.remote)?;
    validate_remote_url(&remote_url)?;
    reject_alternate_partial_and_replace_state(&layout, &config)?;
    remove_cached_hooks(&layout.common_dir)?;
    ensure_worktree(repo)?;

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
    git_ok(
        repo,
        &[
            "config",
            &format!("branch.{}.remote", request.branch),
            &request.remote,
        ],
    )?;
    git_ok(
        repo,
        &[
            "config",
            &format!("branch.{}.merge", request.branch),
            &format!("refs/heads/{}", request.branch),
        ],
    )?;
    // Cached artifacts are allowed to contain only Git-tracked state. Remove
    // ignored and untracked residue before checkout, including nested repos.
    git_ok(repo, &["clean", "-ffdx"])?;
    git_ok(repo, &["reset", "--hard", &target])?;
    git_ok(repo, &["update-ref", "-d", verify_ref])?;

    let actual = git_stdout(repo, &["rev-parse", "HEAD"])?;
    if !actual.eq_ignore_ascii_case(&target) {
        bail!("checkout substituted commit {actual} for pinned target {target}");
    }
    let clean = git_stdout(
        repo,
        &[
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignored=matching",
        ],
    )?;
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

#[derive(Debug)]
struct RepoLayout {
    common_dir: PathBuf,
    objects: PathBuf,
    config: PathBuf,
}

/// Validate repository paths without invoking a repo-scoped Git command. The
/// base installer is untrusted: it must not redirect `.git`, the common dir, or
/// object storage outside the disposable staging tree.
fn validate_self_contained_layout(repo: &Path, staging_root: &Path) -> Result<RepoLayout> {
    let staging_root = staging_root
        .canonicalize()
        .context("canonicalize top-up staging root")?;
    let repo_meta = std::fs::symlink_metadata(repo).context("cached base did not create repo")?;
    if !repo_meta.file_type().is_dir() || repo_meta.file_type().is_symlink() {
        bail!("cached base repository must be a real directory, not a symlink");
    }
    let repo = repo.canonicalize().context("canonicalize cached base")?;
    if !repo.starts_with(&staging_root) {
        bail!("cached base repository escapes its staging directory");
    }

    let dot_git = repo.join(".git");
    let dot_git_meta = std::fs::symlink_metadata(&dot_git)
        .context("cached base must contain a physical .git directory")?;
    if !dot_git_meta.file_type().is_dir() || dot_git_meta.file_type().is_symlink() {
        bail!("cached base .git must be a real directory contained in staging");
    }
    let git_dir = dot_git
        .canonicalize()
        .context("canonicalize cached base .git")?;
    ensure_contained(&git_dir, &repo, ".git")?;

    let common_dir = canonicalize_git_path(&git_dir, &git_dir.join("commondir"), &git_dir)?;
    ensure_contained(&common_dir, &repo, "Git common directory")?;
    let objects_path = common_dir.join("objects");
    let objects = objects_path
        .canonicalize()
        .context("canonicalize Git object directory")?;
    ensure_contained(&objects, &repo, "Git object directory")?;
    let objects_meta = std::fs::symlink_metadata(&objects_path)
        .context("cached base is missing its object directory")?;
    if !objects_meta.file_type().is_dir() || objects_meta.file_type().is_symlink() {
        bail!("Git object directory must be a real directory contained in staging");
    }

    let config = common_dir.join("config");
    let config_meta =
        std::fs::symlink_metadata(&config).context("cached base has no local config")?;
    if !config_meta.file_type().is_file() || config_meta.file_type().is_symlink() {
        bail!("cached base local config must be a real file");
    }
    ensure_contained(
        &config.canonicalize().context("canonicalize local config")?,
        &repo,
        "Git config",
    )?;

    Ok(RepoLayout {
        common_dir,
        objects,
        config,
    })
}

fn canonicalize_git_path(base: &Path, marker: &Path, default: &Path) -> Result<PathBuf> {
    let path = if marker.is_file() {
        let raw = std::fs::read_to_string(marker)
            .with_context(|| format!("read Git path marker {}", marker.display()))?;
        let value = raw.trim();
        if value.is_empty() {
            bail!("Git path marker {} is empty", marker.display());
        }
        let value = Path::new(value);
        if value.is_absolute() {
            value.to_owned()
        } else {
            base.join(value)
        }
    } else {
        default.to_owned()
    };
    path.canonicalize()
        .with_context(|| format!("canonicalize Git path {}", path.display()))
}

fn ensure_contained(path: &Path, root: &Path, label: &str) -> Result<()> {
    if !path.starts_with(root) {
        bail!("{label} escapes the cached base staging directory");
    }
    Ok(())
}

type LocalConfig = Vec<(String, String)>;

fn read_and_validate_local_config(path: &Path) -> Result<LocalConfig> {
    let mut command = sanitized_git_command();
    let output = command
        .args(["config", "--file"])
        .arg(path)
        .args(["--null", "--list", "--no-includes"])
        .output()
        .context("parse cached base local config")?;
    if !output.status.success() {
        bail!("cached base local config is invalid");
    }
    let mut result = Vec::new();
    for entry in output.stdout.split(|b| *b == 0).filter(|e| !e.is_empty()) {
        let entry = std::str::from_utf8(entry).context("local Git config is not UTF-8")?;
        let (key, value) = entry.split_once('\n').unwrap_or((entry, ""));
        let normalized = key.to_ascii_lowercase();
        if dangerous_config_key(&normalized) || value.trim_start().starts_with('!') {
            bail!("cached base contains forbidden Git config key {key}");
        }
        result.push((normalized, value.to_owned()));
    }
    Ok(result)
}

fn dangerous_config_key(key: &str) -> bool {
    key.starts_with("alias.")
        || key.starts_with("credential.")
        || key.starts_with("filter.")
        || key.starts_with("http.")
        || key.starts_with("include.")
        || key.starts_with("includeif.")
        || key.starts_with("url.")
        || key.starts_with("protocol.")
        || key.starts_with("gpg.")
        || key == "core.fsmonitor"
        || key == "core.hookspath"
        || key == "core.sshcommand"
        || key == "core.askpass"
        || key == "core.gitproxy"
        || key == "core.worktree"
        || key == "core.attributesfile"
        || key == "core.excludesfile"
        || key == "extensions.worktreeconfig"
        || key == "commit.gpgsign"
        || key == "tag.gpgsign"
        || (key.starts_with("diff.")
            && (key.ends_with(".command")
                || key.ends_with(".external")
                || key.ends_with(".textconv")))
        || (key.starts_with("merge.") && key.ends_with(".driver"))
        || (key.starts_with("remote.")
            && (key.ends_with(".proxy")
                || key.ends_with(".uploadpack")
                || key.ends_with(".receivepack")
                || key.ends_with(".vcs")))
        || (key.starts_with("submodule.") && key.ends_with(".update"))
}

fn configured_remote_url(config: &LocalConfig, remote: &str) -> Result<String> {
    let key = format!("remote.{}.url", remote.to_ascii_lowercase());
    let urls: Vec<_> = config
        .iter()
        .filter(|(candidate, _)| candidate == &key)
        .map(|(_, value)| value.trim())
        .collect();
    match urls.as_slice() {
        [url] if !url.is_empty() => Ok((*url).to_owned()),
        [] => bail!("cached base does not configure remote {remote}"),
        _ => bail!("cached base configures multiple URLs for remote {remote}"),
    }
}

fn validate_all_remote_urls(config: &LocalConfig) -> Result<()> {
    for (key, value) in config {
        if key.starts_with("remote.") && (key.ends_with(".url") || key.ends_with(".pushurl")) {
            validate_remote_url(value)?;
        }
    }
    Ok(())
}

fn validate_remote_url(value: &str) -> Result<()> {
    if value.starts_with('-') || value.contains("::") {
        bail!("cached base remote uses an unsafe Git transport");
    }
    if let Ok(url) = url::Url::parse(value) {
        if !matches!(url.scheme(), "http" | "https" | "ssh" | "git" | "file") {
            bail!("cached base remote uses an unsupported Git URL scheme");
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!(
                "cached base remote URL must not contain userinfo, query parameters, or a fragment"
            );
        }
    } else if value.contains(['?', '#']) {
        bail!("cached base remote URL must not contain a query or fragment");
    } else if !(value.starts_with('/') || value.contains(':')) {
        bail!("cached base remote is not an accepted URL or path");
    }
    Ok(())
}

fn reject_alternate_partial_and_replace_state(
    layout: &RepoLayout,
    config: &LocalConfig,
) -> Result<()> {
    let alternates = layout.objects.join("info/alternates");
    if alternates.exists() {
        bail!("cached base uses forbidden alternate object storage");
    }
    if config.iter().any(|(key, value)| {
        key == "extensions.partialclone"
            || (key.starts_with("remote.") && key.ends_with(".promisor") && value != "false")
            || (key.starts_with("remote.") && key.ends_with(".partialclonefilter"))
    }) {
        bail!("cached base is partial/promisor and is not self-contained");
    }
    let pack_dir = layout.objects.join("pack");
    if pack_dir.is_dir()
        && std::fs::read_dir(&pack_dir)
            .context("inspect object packs")?
            .any(|entry| {
                entry.is_ok_and(|e| e.path().extension().is_some_and(|ext| ext == "promisor"))
            })
    {
        bail!("cached base contains a promisor pack");
    }
    let replace = layout.common_dir.join("refs/replace");
    if replace.is_dir()
        && std::fs::read_dir(&replace)
            .context("inspect replace refs")?
            .next()
            .is_some()
    {
        bail!("cached base contains forbidden replace refs");
    }
    let packed_refs = layout.common_dir.join("packed-refs");
    if packed_refs.is_file()
        && std::fs::read_to_string(&packed_refs)
            .context("read packed refs")?
            .lines()
            .any(|line| line.contains(" refs/replace/"))
    {
        bail!("cached base contains forbidden packed replace refs");
    }
    let grafts = layout.common_dir.join("info/grafts");
    if grafts.is_file() && std::fs::metadata(&grafts)?.len() > 0 {
        bail!("cached base contains forbidden grafts");
    }
    Ok(())
}

fn remove_cached_hooks(common_dir: &Path) -> Result<()> {
    let hooks = common_dir.join("hooks");
    match std::fs::symlink_metadata(&hooks) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            std::fs::remove_dir_all(&hooks).context("remove cached-base hooks")?;
        }
        Ok(_) => std::fs::remove_file(&hooks).context("remove cached-base hooks indirection")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect cached-base hooks"),
    }
    std::fs::create_dir(&hooks).context("create sanitized empty hooks directory")
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
    let mut command = sanitized_git_command();
    command
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(["-c", "core.fsmonitor=false"])
        .args(["-c", "credential.helper="])
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .with_context(|| format!("run git {} in {}", args.join(" "), repo.display()))
}

fn sanitized_git_command() -> Command {
    let mut command = Command::new("git");
    let path = std::env::var_os("PATH");
    let system_root = std::env::var_os("SystemRoot");
    command.env_clear();
    if let Some(path) = path {
        command.env("PATH", path);
    }
    if let Some(system_root) = system_root {
        command.env("SystemRoot", system_root);
    }
    command
        .env("HOME", "/nonexistent")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_PAGER", "cat")
        .env("LC_ALL", "C");
    command
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
        atomic_rename_error(std::io::Error::last_os_error())
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
        atomic_rename_error(std::io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn atomic_rename_noreplace(_from: &Path, _to: &Path) -> Result<()> {
    bail!("atomic no-replace publication is unsupported on this platform")
}

fn atomic_rename_error(error: std::io::Error) -> Result<()> {
    // ENOSYS/EINVAL are deliberately terminal: never degrade to a racy
    // exists()+rename sequence which could overwrite a concurrently-created
    // destination.
    Err(error).context("atomic no-replace rename")
}

#[cfg(test)]
mod tests {
    #[test]
    fn unsupported_atomic_rename_is_a_terminal_error() {
        let err = super::atomic_rename_error(std::io::Error::from_raw_os_error(libc::ENOSYS))
            .unwrap_err();
        assert!(err.to_string().contains("atomic no-replace rename"));
    }
}
