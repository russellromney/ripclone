//! Exact-commit installation from a server-issued pinned top-up bundle.
//!
//! This primitive never reads a cached remote and never talks to a provider.
//! The server resolves and pins the target against its authenticated mirror,
//! then returns content-addressed non-thin packs plus target checkout metadata.
//! An installer verifies those artifacts through ripclone's authenticated CAS
//! path and installs them into private staging. We retain only object/index
//! material, rebuild Git control state from an allowlist, verify the exact
//! target and closure, materialize every tracked path, and atomically publish.
//!
//! Threat boundary: [`PinnedBundleInstaller`] is trusted client code. It must
//! authenticate the server response, verify every artifact hash/length, remain
//! quiescent after returning, and never delegate installation to an untrusted
//! process. The installed repository is still normalized defensively; arbitrary
//! cached config, refs, hooks, modules, worktree metadata, sparse state, and
//! provider credentials are discarded rather than interpreted.

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::CString;
use std::path::Path;
use std::process::{Command, Output};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TopUpMode {
    Head,
    Full,
}

/// Authenticated server plan. `manifest_hash` is the CAS hash of the signed
/// manifest whose artifacts the installer verifies.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedTopUpBundle {
    pub format_version: u32,
    pub workspace_id: String,
    pub repo_path: String,
    pub base_commit: String,
    pub target_commit: String,
    pub branch: String,
    pub mode: TopUpMode,
    /// Canonical provider URL written for future user-initiated fetches. It is
    /// metadata only and is never contacted during top-up.
    pub canonical_origin: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedBundleRequest {
    pub manifest_hash: String,
    /// Request-specific renewable transport lease. It is deliberately outside
    /// the content-addressed bundle semantics so concurrent clones of the same
    /// root cannot release one another's retention lease.
    #[serde(default)]
    pub transport_session: String,
    /// Exact semantic identity expected from the content-addressed manifest.
    /// These fields are repeated deliberately: the manifest hash alone is not
    /// sufficient to prevent a valid bundle for another request from being
    /// substituted at an untrusted transport boundary.
    pub format_version: u32,
    pub workspace_id: String,
    pub repo_path: String,
    pub base_commit: String,
    pub target_commit: String,
    pub branch: String,
    pub mode: TopUpMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PinnedArtifactKind {
    BasePack,
    BasePackIndex,
    OverlayPack,
    OverlayPackIndex,
    PrebuiltIndex,
    CheckoutMetadata,
    WorktreeArchive,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedArtifactDescriptor {
    pub kind: PinnedArtifactKind,
    pub hash: String,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedPinnedBundle {
    pub manifest_hash: String,
    pub semantic_digest: String,
    pub bundle: PinnedTopUpBundle,
    pub artifacts: Vec<PinnedArtifactDescriptor>,
}

pub trait PinnedBundleInstaller {
    /// Exact canonical provider origin authorized by the workspace adapter,
    /// supplied outside the server bundle.
    fn approved_canonical_origin(&self) -> &str;

    fn install_verified(
        &self,
        destination: &Path,
        request: &PinnedBundleRequest,
    ) -> std::result::Result<VerifiedPinnedBundle, BundleInstallFailure>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleInstallFailure {
    Unauthorized,
    Expired,
    Unavailable,
    Integrity,
    Transport,
}

impl std::fmt::Display for BundleInstallFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unauthorized => "bundle authorization denied",
            Self::Expired => "bundle plan expired",
            Self::Unavailable => "pinned bundle unavailable",
            Self::Integrity => "bundle integrity verification failed",
            Self::Transport => "bundle transport failed",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopUpOutcome {
    pub target_commit: String,
    pub branch: String,
    pub mode: TopUpMode,
}

pub fn install_pinned_bundle(
    target: impl AsRef<Path>,
    request: &PinnedBundleRequest,
    installer: &dyn PinnedBundleInstaller,
) -> Result<TopUpOutcome> {
    validate_hash("requested manifest", &request.manifest_hash)?;
    let target = target.as_ref();
    if std::fs::symlink_metadata(target).is_ok() {
        bail!("clone destination already exists: {}", target.display());
    }
    let parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temp = tempfile::Builder::new()
        .prefix("ripclone-pinned.")
        .suffix(".tmp")
        .tempdir_in(parent)
        .context("create pinned-bundle staging")?;
    let staging = temp.path().join("repo");

    let verified = installer
        .install_verified(&staging, request)
        .map_err(|reason| {
            anyhow::anyhow!("authenticated pinned-bundle installation failed: {reason}")
        })?;
    if verified.manifest_hash != request.manifest_hash {
        bail!("verified bundle does not match the requested manifest");
    }
    validate_request_binding(request, &verified.bundle)?;
    validate_bundle(&verified.bundle, installer.approved_canonical_origin())?;
    validate_artifacts(&verified.artifacts)?;
    if verified.semantic_digest
        != pinned_bundle_semantic_digest(&verified.bundle, &verified.artifacts)
    {
        bail!("verified bundle semantic digest mismatch");
    }

    normalize_fresh_control_dir(&staging)?;
    finalize_and_verify(&staging, &verified.bundle)?;
    atomic_rename_noreplace(&staging, target).context("publish verified pinned bundle")?;
    Ok(TopUpOutcome {
        target_commit: verified.bundle.target_commit.to_ascii_lowercase(),
        branch: verified.bundle.branch.clone(),
        mode: verified.bundle.mode,
    })
}

pub(crate) fn validate_request_binding(
    request: &PinnedBundleRequest,
    bundle: &PinnedTopUpBundle,
) -> Result<()> {
    if request.format_version != bundle.format_version
        || request.workspace_id != bundle.workspace_id
        || request.repo_path != bundle.repo_path
        || request.base_commit != bundle.base_commit
        || request.target_commit != bundle.target_commit
        || request.branch != bundle.branch
        || request.mode != bundle.mode
    {
        bail!("verified bundle semantic identity does not match the request");
    }
    Ok(())
}

fn validate_bundle(bundle: &PinnedTopUpBundle, approved_origin: &str) -> Result<()> {
    if bundle.format_version != 1 {
        bail!("unsupported pinned-bundle format version");
    }
    let repo_components = bundle.repo_path.split('/').collect::<Vec<_>>();
    if bundle.workspace_id.is_empty()
        || bundle.workspace_id.bytes().any(|b| b.is_ascii_control())
        || repo_components.len() < 2
        || repo_components
            .iter()
            .any(|component| component.is_empty() || matches!(*component, "." | ".."))
        || bundle.repo_path.contains('\\')
        || bundle.repo_path.bytes().any(|b| b.is_ascii_control())
    {
        bail!("pinned bundle workspace/repository identity is invalid");
    }
    validate_oid("base_commit", &bundle.base_commit)?;
    validate_oid("target_commit", &bundle.target_commit)?;
    if bundle.base_commit.len() != bundle.target_commit.len() {
        bail!("base and target object formats differ");
    }
    if bundle.branch.is_empty()
        || !bundle
            .branch
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-'))
    {
        bail!("bundle branch contains characters unsafe for generated Git config");
    }
    let status = sanitized_git_command()
        .args(["check-ref-format", "--branch", &bundle.branch])
        .output()
        .context("validate branch")?;
    if !status.status.success() {
        bail!("bundle branch is invalid");
    }
    let origin = url::Url::parse(&bundle.canonical_origin)
        .context("canonical provider origin is not a URL")?;
    if bundle.canonical_origin != approved_origin
        || origin.scheme() != "https"
        || bundle
            .canonical_origin
            .bytes()
            .any(|b| b.is_ascii_control())
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        bail!("canonical origin is not the workspace-approved HTTPS provider origin");
    }
    Ok(())
}

fn validate_artifacts(artifacts: &[PinnedArtifactDescriptor]) -> Result<()> {
    if artifacts.is_empty() {
        bail!("verified bundle has no artifact descriptors");
    }
    for artifact in artifacts {
        validate_hash("artifact", &artifact.hash)?;
        if artifact.len == 0 {
            bail!("verified bundle contains a zero-length artifact");
        }
    }
    Ok(())
}

fn validate_hash(label: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("{label} must be a full SHA-256 hash");
    }
    Ok(())
}

/// Stable length-delimited binding over all authenticated semantics and exact
/// artifact descriptors. Any field, order, hash, or length change alters it.
pub fn pinned_bundle_semantic_digest(
    bundle: &PinnedTopUpBundle,
    artifacts: &[PinnedArtifactDescriptor],
) -> String {
    fn field(hasher: &mut Sha256, bytes: &[u8]) {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"ripclone-pinned-bundle-semantics-v1\0");
    hasher.update(bundle.format_version.to_be_bytes());
    field(&mut hasher, bundle.workspace_id.as_bytes());
    field(&mut hasher, bundle.repo_path.as_bytes());
    field(&mut hasher, bundle.base_commit.as_bytes());
    field(&mut hasher, bundle.target_commit.as_bytes());
    field(&mut hasher, bundle.branch.as_bytes());
    hasher.update([match bundle.mode {
        TopUpMode::Head => 1,
        TopUpMode::Full => 2,
    }]);
    field(&mut hasher, bundle.canonical_origin.as_bytes());
    hasher.update((artifacts.len() as u64).to_be_bytes());
    for artifact in artifacts {
        hasher.update([match artifact.kind {
            PinnedArtifactKind::BasePack => 1,
            PinnedArtifactKind::BasePackIndex => 2,
            PinnedArtifactKind::OverlayPack => 3,
            PinnedArtifactKind::OverlayPackIndex => 4,
            PinnedArtifactKind::PrebuiltIndex => 5,
            PinnedArtifactKind::CheckoutMetadata => 6,
            PinnedArtifactKind::WorktreeArchive => 7,
        }]);
        field(&mut hasher, artifact.hash.as_bytes());
        hasher.update(artifact.len.to_be_bytes());
    }
    hex::encode(hasher.finalize())
}

fn validate_oid(label: &str, value: &str) -> Result<()> {
    if value.len() != 40 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("{label} must be a full SHA-1 object id");
    }
    Ok(())
}

/// Keep only physical `.git/objects` and `.git/index`, then create all other
/// control state ourselves. This is an allowlist, not a config blacklist.
fn normalize_fresh_control_dir(repo: &Path) -> Result<()> {
    let repo_meta = std::fs::symlink_metadata(repo).context("installer did not create repo")?;
    if !repo_meta.file_type().is_dir() || repo_meta.file_type().is_symlink() {
        bail!("installer repository must be a physical directory");
    }
    let git = repo.join(".git");
    require_physical_dir(&git, ".git")?;
    let objects = git.join("objects");
    require_physical_dir(&objects, "object directory")?;
    reject_symlinks_recursively(&objects)?;
    if objects.join("info/alternates").exists()
        || objects.join("info/http-alternates").exists()
        || contains_extension(&objects.join("pack"), "promisor")?
    {
        bail!("bundle object store is alternate/promisor and not self-contained");
    }
    let index = git.join("index");
    let index_meta =
        std::fs::symlink_metadata(&index).context("bundle is missing prebuilt index")?;
    if !index_meta.file_type().is_file() || index_meta.file_type().is_symlink() {
        bail!("bundle index must be a physical file");
    }

    let saved_objects = repo.join(".ripclone-objects");
    let saved_index = repo.join(".ripclone-index");
    std::fs::rename(&objects, &saved_objects).context("isolate verified objects")?;
    std::fs::rename(&index, &saved_index).context("isolate verified index")?;
    std::fs::remove_dir_all(&git).context("discard installed Git control state")?;
    std::fs::create_dir_all(git.join("objects"))?;
    std::fs::rename(&saved_objects, git.join("objects"))?;
    std::fs::rename(&saved_index, git.join("index"))?;
    std::fs::create_dir_all(git.join("refs/heads"))?;
    std::fs::create_dir_all(git.join("refs/remotes/origin"))?;
    std::fs::create_dir_all(git.join("info"))?;
    std::fs::write(git.join("info/exclude"), b".ripclone/\n")?;
    Ok(())
}

fn finalize_and_verify(repo: &Path, bundle: &PinnedTopUpBundle) -> Result<()> {
    let git = repo.join(".git");
    let target = bundle.target_commit.to_ascii_lowercase();
    let base = bundle.base_commit.to_ascii_lowercase();
    let branch_ref = format!("refs/heads/{}", bundle.branch);
    let remote_ref = format!("refs/remotes/origin/{}", bundle.branch);
    std::fs::write(git.join("HEAD"), format!("ref: {branch_ref}\n"))?;
    write_ref(&git, &branch_ref, &target)?;
    write_ref(&git, &remote_ref, &target)?;
    let config = format!(
        "[core]\n\tsymlinks = true\n\tcheckStat = minimal\n\tsparseCheckout = false\n[remote \"origin\"]\n\turl = {}\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n[branch \"{}\"]\n\tremote = origin\n\tmerge = refs/heads/{}\n",
        bundle.canonical_origin, bundle.branch, bundle.branch
    );
    std::fs::write(git.join("config"), config)?;
    match bundle.mode {
        TopUpMode::Head => std::fs::write(git.join("shallow"), format!("{target}\n"))?,
        TopUpMode::Full => {
            let _ = std::fs::remove_file(git.join("shallow"));
        }
    }

    git_ok(repo, &["cat-file", "-e", &format!("{base}^{{commit}}")])
        .context("bundle does not contain its declared base commit")?;
    git_ok(repo, &["cat-file", "-e", &format!("{target}^{{commit}}")])
        .context("bundle does not contain its exact target commit")?;
    git_ok(
        repo,
        &["fsck", "--connectivity-only", "--no-dangling", &target],
    )
    .context("bundle target closure is incomplete or corrupt")?;

    // Replace any sparse/skip-worktree index state with the target's complete
    // tree, then remove every non-target residue before and after checkout.
    git_ok(repo, &["clean", "-ffdx"])?;
    git_ok(repo, &["read-tree", "--reset", &target])?;
    crate::git::clear_skip_worktree_all(repo).context("clear all sparse skip-worktree entries")?;
    git_ok(repo, &["checkout-index", "-a", "-f"])?;
    git_ok(repo, &["reset", "--hard", &target])?;
    git_ok(repo, &["clean", "-ffdx"])?;
    if bundle.mode == TopUpMode::Head {
        if git_stdout(repo, &["rev-list", "--count", "HEAD"])? != "1" {
            bail!("HEAD bundle did not produce depth-one semantics");
        }
    } else if git_stdout(repo, &["rev-parse", "--is-shallow-repository"])? != "false" {
        bail!("full bundle produced a shallow repository");
    }
    let status = git_stdout(
        repo,
        &[
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignored=matching",
        ],
    )?;
    if !status.is_empty() || git_stdout(repo, &["rev-parse", "HEAD"])? != target {
        bail!("bundle checkout is not the exact clean target");
    }
    let index = gix::index::File::at(
        git.join("index"),
        gix::hash::Kind::Sha1,
        false,
        gix::index::decode::Options::default(),
    )?;
    if index
        .entries()
        .iter()
        .any(|e| e.flags.contains(gix::index::entry::Flags::SKIP_WORKTREE))
    {
        bail!("bundle checkout retained sparse skip-worktree entries");
    }
    Ok(())
}

fn write_ref(git: &Path, reference: &str, oid: &str) -> Result<()> {
    let path = git.join(reference);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, format!("{oid}\n"))?;
    Ok(())
}

fn require_physical_dir(path: &Path, label: &str) -> Result<()> {
    let meta = std::fs::symlink_metadata(path).with_context(|| format!("missing {label}"))?;
    if !meta.file_type().is_dir() || meta.file_type().is_symlink() {
        bail!("{label} must be a physical directory");
    }
    Ok(())
}

fn reject_symlinks_recursively(root: &Path) -> Result<()> {
    for entry in walkdir::WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_symlink() {
            bail!("bundle object store contains a symlink");
        }
    }
    Ok(())
}

fn contains_extension(dir: &Path, extension: &str) -> Result<bool> {
    if !dir.is_dir() {
        return Ok(false);
    }
    Ok(std::fs::read_dir(dir)?
        .any(|e| e.is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == extension))))
}

fn git_stdout(repo: &Path, args: &[&str]) -> Result<String> {
    let output = git_output(repo, args)?;
    if !output.status.success() {
        bail!("Git validation failed");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_ok(repo: &Path, args: &[&str]) -> Result<()> {
    git_stdout(repo, args).map(|_| ())
}

fn git_output(repo: &Path, args: &[&str]) -> Result<Output> {
    sanitized_git_command()
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
        ])
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .with_context(|| format!("run Git validation in {}", repo.display()))
}

fn sanitized_git_command() -> Command {
    let mut command = Command::new("git");
    let path = std::env::var_os("PATH");
    let system_root = std::env::var_os("SystemRoot");
    command.env_clear();
    if let Some(path) = path {
        command.env("PATH", path);
    }
    if let Some(root) = system_root {
        command.env("SystemRoot", root);
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
    let from = CString::new(from.as_os_str().as_bytes())?;
    let to = CString::new(to.as_os_str().as_bytes())?;
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
    let from = CString::new(from.as_os_str().as_bytes())?;
    let to = CString::new(to.as_os_str().as_bytes())?;
    let rc = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("atomic no-replace rename")
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn atomic_rename_noreplace(_from: &Path, _to: &Path) -> Result<()> {
    bail!("atomic no-replace publication is unsupported on this platform")
}
