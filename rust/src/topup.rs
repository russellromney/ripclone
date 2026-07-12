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
use std::collections::HashMap;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::CString;
use std::io::{BufRead, BufReader, Write};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::OsStrExt;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::{Command, Output};
use unicode_normalization::UnicodeNormalization;

#[cfg(unix)]
thread_local! {
    static INSTALL_STAGING_FD: std::cell::Cell<Option<libc::c_int>> = const { std::cell::Cell::new(None) };
}

#[cfg(all(test, target_os = "macos"))]
static FORCE_STAGING_RESTORE_FAILURE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

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
    install_pinned_bundle_cancellable(
        target,
        request,
        installer,
        &tokio_util::sync::CancellationToken::new(),
    )
}

pub fn install_pinned_bundle_cancellable(
    target: impl AsRef<Path>,
    request: &PinnedBundleRequest,
    installer: &dyn PinnedBundleInstaller,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<TopUpOutcome> {
    install_pinned_bundle_transaction(target, request, installer, false, cancelled)
}

/// Files-mode top-up. Verification and checkout happen with Git available in
/// private staging; the administrative directory is discarded before the
/// same atomic publish used by ordinary pinned installs.
pub fn install_pinned_bundle_discard_git(
    target: impl AsRef<Path>,
    request: &PinnedBundleRequest,
    installer: &dyn PinnedBundleInstaller,
) -> Result<TopUpOutcome> {
    install_pinned_bundle_discard_git_cancellable(
        target,
        request,
        installer,
        &tokio_util::sync::CancellationToken::new(),
    )
}

pub fn install_pinned_bundle_discard_git_cancellable(
    target: impl AsRef<Path>,
    request: &PinnedBundleRequest,
    installer: &dyn PinnedBundleInstaller,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<TopUpOutcome> {
    install_pinned_bundle_transaction(target, request, installer, true, cancelled)
}

fn install_pinned_bundle_transaction(
    target: impl AsRef<Path>,
    request: &PinnedBundleRequest,
    installer: &dyn PinnedBundleInstaller,
    discard_git: bool,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<TopUpOutcome> {
    ensure_not_cancelled(cancelled)?;
    validate_hash("requested manifest", &request.manifest_hash)?;
    let target = target.as_ref();
    let publication = BoundInstall::new(target, "pinned")?;
    let staging_scope = publication.enter_staging()?;
    let staging = publication.staging_root();

    let verified = installer
        .install_verified(&staging, request)
        .map_err(|reason| {
            anyhow::anyhow!("authenticated pinned-bundle installation failed: {reason}")
        })?;
    ensure_not_cancelled(cancelled)?;
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
    ensure_not_cancelled(cancelled)?;
    finalize_and_verify(&staging, &verified.bundle, cancelled)?;
    if discard_git {
        let git = staging.join(".git");
        require_physical_dir(&git, ".git")?;
        std::fs::remove_dir_all(&git).context("discard Git control state in Files staging")?;
        if std::fs::symlink_metadata(&git).is_ok() {
            bail!("Files top-up retained Git administrative state");
        }
    }
    ensure_not_cancelled(cancelled)?;
    staging_scope.finish()?;
    publication
        .publish_repo()
        .context("publish verified pinned bundle")?;
    Ok(TopUpOutcome {
        target_commit: verified.bundle.target_commit.to_ascii_lowercase(),
        branch: verified.bundle.branch.clone(),
        mode: verified.bundle.mode,
    })
}

fn ensure_not_cancelled(cancelled: &tokio_util::sync::CancellationToken) -> Result<()> {
    if cancelled.is_cancelled() {
        bail!("clone installation cancelled")
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
/// The random mode-0700 directory is itself the candidate repository. Success
/// moves that exact root to the destination. Failure only closes capabilities
/// and abandons the recognizable `.ripclone-<kind>-<nonce>` root: no pathname
/// is ever unlinked, so later bounded orphan recovery can be implemented as a
/// separate maintenance operation with an age/ownership policy.
pub(crate) struct BoundInstall {
    parent: OwnedFd,
    staging: OwnedFd,
    parent_path: std::path::PathBuf,
    target_name: std::ffi::OsString,
    staging_name: std::ffi::OsString,
    parent_dev: u64,
    parent_ino: u64,
    staging_dev: u64,
    staging_ino: u64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl BoundInstall {
    pub(crate) fn new(target: &Path, prefix: &str) -> Result<Self> {
        let target_name = target
            .file_name()
            .context("clone destination has no final component")?
            .to_os_string();
        if target_name.as_bytes().is_empty() || matches!(target_name.as_bytes(), b"." | b"..") {
            bail!("clone destination final component is unsafe")
        }
        let parent_path = target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let (parent, parent_path) = open_parent_chain(&parent_path)?;
        ensure_absent_at(parent.as_raw_fd(), &target_name)?;
        let staging_name = std::ffi::OsString::from(format!(
            ".ripclone-{prefix}-{}",
            hex::encode(rand::random::<[u8; 16]>())
        ));
        let staging_c = cstring(&staging_name)?;
        if unsafe { libc::mkdirat(parent.as_raw_fd(), staging_c.as_ptr(), 0o700) } != 0 {
            return Err(std::io::Error::last_os_error()).context("create fd-bound clone staging");
        }
        let staging = openat_dir(parent.as_raw_fd(), &staging_name)?;
        let parent_stat = fd_stat(parent.as_raw_fd())?;
        let staging_stat = fd_stat(staging.as_raw_fd())?;
        Ok(Self {
            parent,
            staging,
            parent_path,
            target_name,
            staging_name,
            parent_dev: parent_stat.st_dev as u64,
            parent_ino: parent_stat.st_ino as u64,
            staging_dev: staging_stat.st_dev as u64,
            staging_ino: staging_stat.st_ino as u64,
        })
    }
    pub(crate) fn staging_root(&self) -> std::path::PathBuf {
        #[cfg(target_os = "linux")]
        return std::path::PathBuf::from(format!("/proc/self/fd/{}", self.staging.as_raw_fd()));
        #[cfg(target_os = "macos")]
        return std::path::PathBuf::from(".");
    }
    pub(crate) fn enter_staging(&self) -> Result<StagingScope> {
        let staging = dup_cloexec(self.staging.as_raw_fd())
            .context("duplicate installer staging descriptor")?;
        #[cfg(target_os = "linux")]
        {
            let previous = INSTALL_STAGING_FD.with(|slot| slot.replace(Some(staging.as_raw_fd())));
            return Ok(StagingScope {
                staging,
                previous,
                restored: false,
            });
        }
        #[cfg(target_os = "macos")]
        {
            let dot = CString::new(".").unwrap();
            let old = unsafe {
                libc::open(
                    dot.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
                )
            };
            if old < 0 {
                return Err(std::io::Error::last_os_error())
                    .context("save installer thread working directory");
            }
            let old = unsafe { OwnedFd::from_raw_fd(old) };
            if unsafe { pthread_fchdir_np(staging.as_raw_fd()) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("bind installer thread to staging descriptor");
            }
            let previous = INSTALL_STAGING_FD.with(|slot| slot.replace(Some(staging.as_raw_fd())));
            Ok(StagingScope {
                old,
                staging,
                previous,
                restored: false,
            })
        }
    }
    pub(crate) fn publish_repo(&self) -> Result<()> {
        let current = std::fs::symlink_metadata(&self.parent_path)
            .context("clone destination parent was replaced")?;
        if !current.file_type().is_dir()
            || current.file_type().is_symlink()
            || current.dev() != self.parent_dev
            || current.ino() != self.parent_ino
        {
            bail!("clone destination parent changed during installation")
        }
        ensure_absent_at(self.parent.as_raw_fd(), &self.target_name)?;
        let bound = entry_stat(self.parent.as_raw_fd(), &self.staging_name)?;
        if bound.st_dev as u64 != self.staging_dev || bound.st_ino != self.staging_ino {
            bail!("clone staging root changed before publication")
        }
        let from = cstring(&self.staging_name)?;
        let to = cstring(&self.target_name)?;
        #[cfg(target_os = "linux")]
        let rc = unsafe {
            libc::renameat2(
                self.parent.as_raw_fd(),
                from.as_ptr(),
                self.parent.as_raw_fd(),
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(target_os = "macos")]
        let rc = unsafe {
            libc::renameatx_np(
                self.parent.as_raw_fd(),
                from.as_ptr(),
                self.parent.as_raw_fd(),
                to.as_ptr(),
                libc::RENAME_EXCL,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error())
                .context("fd-bound atomic no-replace publication");
        }
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) struct BoundInstall;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl BoundInstall {
    pub(crate) fn new(_: &Path, _: &str) -> Result<Self> {
        bail!("fd-bound clone publication is unsupported on this platform")
    }
    pub(crate) fn staging_root(&self) -> std::path::PathBuf {
        unreachable!()
    }
    pub(crate) fn enter_staging(&self) -> Result<StagingScope> {
        bail!("fd-bound clone publication is unsupported on this platform")
    }
    pub(crate) fn publish_repo(&self) -> Result<()> {
        bail!("fd-bound clone publication is unsupported on this platform")
    }
}

#[cfg(target_os = "linux")]
pub(crate) struct StagingScope {
    staging: OwnedFd,
    previous: Option<libc::c_int>,
    restored: bool,
}

#[cfg(target_os = "linux")]
impl StagingScope {
    pub(crate) fn finish(mut self) -> Result<()> {
        self.restore();
        Ok(())
    }
    fn restore(&mut self) {
        if !self.restored {
            INSTALL_STAGING_FD.with(|slot| slot.set(self.previous));
            self.restored = true;
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for StagingScope {
    fn drop(&mut self) {
        self.restore();
    }
}

#[cfg(target_os = "macos")]
pub(crate) struct StagingScope {
    old: OwnedFd,
    staging: OwnedFd,
    previous: Option<libc::c_int>,
    restored: bool,
}

#[cfg(target_os = "macos")]
impl StagingScope {
    pub(crate) fn finish(mut self) -> Result<()> {
        self.restore_or_abort();
        Ok(())
    }
    fn restore_or_abort(&mut self) {
        if self.restored {
            return;
        }
        let _staging_lifetime = self.staging.as_raw_fd();
        #[cfg(test)]
        let injected = FORCE_STAGING_RESTORE_FAILURE.load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let injected = false;
        if injected || unsafe { pthread_fchdir_np(self.old.as_raw_fd()) } != 0 {
            // Continuing on this pthread would resolve every relative path
            // through attacker-controlled staging. There is no safe recovery.
            std::process::abort();
        }
        INSTALL_STAGING_FD.with(|slot| slot.set(self.previous));
        self.restored = true;
    }
}

#[cfg(target_os = "macos")]
impl Drop for StagingScope {
    fn drop(&mut self) {
        self.restore_or_abort();
    }
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn pthread_fchdir_np(fd: libc::c_int) -> libc::c_int;
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) struct StagingScope {}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl StagingScope {
    pub(crate) fn finish(self) -> Result<()> {
        bail!("fd-bound clone publication is unsupported on this platform")
    }
}

pub(crate) fn bind_child_to_staging(command: &mut Command) {
    #[cfg(unix)]
    INSTALL_STAGING_FD.with(|slot| {
        if let Some(fd) = slot.get() {
            use std::os::unix::process::CommandExt;
            unsafe {
                command.pre_exec(move || {
                    if libc::fchdir(fd) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
    });
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn cstring(value: &std::ffi::OsStr) -> Result<CString> {
    CString::new(value.as_bytes()).context("path contains NUL")
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn dup_cloexec(fd: libc::c_int) -> Result<OwnedFd> {
    let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error()).context("duplicate close-on-exec descriptor");
    }
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn openat_dir(parent: libc::c_int, name: &std::ffi::OsStr) -> Result<OwnedFd> {
    let name = cstring(name)?;
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error())
            .context("open physical clone parent component");
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_parent_chain(path: &Path) -> Result<(OwnedFd, std::path::PathBuf)> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match std::fs::symlink_metadata(existing) {
            Ok(metadata) => {
                if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
                    bail!("clone parent ancestor is not a physical directory")
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(
                    existing
                        .file_name()
                        .context("clone parent has no existing ancestor")?
                        .to_os_string(),
                );
                existing = existing
                    .parent()
                    .context("clone parent has no existing ancestor")?
            }
            Err(error) => return Err(error).context("inspect clone parent ancestor"),
        }
    }
    let canonical = existing
        .canonicalize()
        .context("canonicalize physical clone parent ancestor")?;
    let start = std::ffi::OsStr::new("/");
    let start_c = cstring(start)?;
    let fd = unsafe {
        libc::open(
            start_c.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open clone parent root");
    };
    let mut current = unsafe { OwnedFd::from_raw_fd(fd) };
    for component in canonical.components() {
        use std::path::Component;
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => name,
            Component::ParentDir => std::ffi::OsStr::new(".."),
            Component::Prefix(_) => bail!("unsupported clone parent prefix"),
        };
        match openat_dir(current.as_raw_fd(), name) {
            Ok(next) => current = next,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                let name_c = cstring(name)?;
                if unsafe { libc::mkdirat(current.as_raw_fd(), name_c.as_ptr(), 0o755) } != 0 {
                    return Err(std::io::Error::last_os_error())
                        .context("create physical clone parent component");
                }
                current = openat_dir(current.as_raw_fd(), name)?
            }
            Err(error) => return Err(error),
        }
    }
    let mut physical = canonical;
    for name in missing.into_iter().rev() {
        let name_c = cstring(&name)?;
        if unsafe { libc::mkdirat(current.as_raw_fd(), name_c.as_ptr(), 0o755) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(error).context("create physical clone parent component");
            }
        }
        current = openat_dir(current.as_raw_fd(), &name)?;
        physical.push(name)
    }
    Ok((current, physical))
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn ensure_absent_at(parent: libc::c_int, name: &std::ffi::OsStr) -> Result<()> {
    let name = cstring(name)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let rc = unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc == 0 {
        bail!("clone destination already exists")
    }
    let error = std::io::Error::last_os_error();
    if error.kind() != std::io::ErrorKind::NotFound {
        return Err(error).context("inspect clone destination through parent handle");
    }
    Ok(())
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn fd_stat(fd: libc::c_int) -> Result<libc::stat> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("stat clone parent handle");
    }
    Ok(unsafe { stat.assume_init() })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn entry_stat(parent: libc::c_int, name: &std::ffi::OsStr) -> Result<libc::stat> {
    let name = cstring(name)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error()).context("stat staging entry");
    }
    Ok(unsafe { stat.assume_init() })
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

/// Reject worktree names that a case-insensitive or Unicode-normalizing
/// filesystem can alias. Every implicit directory component participates, so
/// `Foo/a` and `foo/b` conflict even though their complete path keys differ.
pub(crate) fn validate_portable_path_components<'a>(
    paths: impl IntoIterator<Item = &'a [u8]>,
) -> Result<()> {
    #[derive(Clone)]
    struct Node {
        spelling: Vec<u8>,
        leaf: bool,
    }
    let mut nodes: HashMap<String, Node> = HashMap::new();
    for path in paths {
        if path.is_empty() || path[0] == b'/' || path.ends_with(b"/") {
            bail!("worktree path is not normalized")
        }
        let components = path.split(|byte| *byte == b'/').collect::<Vec<_>>();
        let mut normalized = String::new();
        let mut spelling = Vec::new();
        for (index, component) in components.iter().enumerate() {
            if component.is_empty() || matches!(*component, b"." | b"..") {
                bail!("worktree path has an unsafe component")
            }
            if index != 0 {
                normalized.push('/');
                spelling.push(b'/');
            }
            normalized.extend(
                String::from_utf8_lossy(component)
                    .nfc()
                    .flat_map(char::to_lowercase),
            );
            spelling.extend_from_slice(component);
            let leaf = index + 1 == components.len();
            match nodes.get_mut(&normalized) {
                Some(existing) => {
                    if existing.spelling != spelling || existing.leaf || leaf {
                        bail!("worktree paths have a case/Unicode or file/directory collision")
                    }
                }
                None => {
                    nodes.insert(
                        normalized.clone(),
                        Node {
                            spelling: spelling.clone(),
                            leaf,
                        },
                    );
                }
            }
        }
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

fn finalize_and_verify(
    repo: &Path,
    bundle: &PinnedTopUpBundle,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    ensure_not_cancelled(cancelled)?;
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

    git_ok_cancelled(
        repo,
        &["cat-file", "-e", &format!("{base}^{{commit}}")],
        cancelled,
    )
    .context("bundle does not contain its declared base commit")?;
    git_ok_cancelled(
        repo,
        &["cat-file", "-e", &format!("{target}^{{commit}}")],
        cancelled,
    )
    .context("bundle does not contain its exact target commit")?;
    git_ok_cancelled(
        repo,
        &["fsck", "--connectivity-only", "--no-dangling", &target],
        cancelled,
    )
    .context("bundle target closure is incomplete or corrupt")?;
    if bundle.mode == TopUpMode::Full {
        verify_exact_full_object_store(repo, &target, 50_000_000, cancelled)?;
    }
    validate_target_path_components(repo, &target, cancelled)?;

    // Replace any sparse/skip-worktree index state with the target's complete
    // tree, then remove every non-target residue before and after checkout.
    git_ok_cancelled(repo, &["clean", "-ffdx"], cancelled)?;
    git_ok_cancelled(repo, &["read-tree", "--reset", &target], cancelled)?;
    crate::git::clear_skip_worktree_all(repo).context("clear all sparse skip-worktree entries")?;
    git_ok_cancelled(repo, &["checkout-index", "-a", "-f"], cancelled)?;
    git_ok_cancelled(repo, &["reset", "--hard", &target], cancelled)?;
    git_ok_cancelled(repo, &["clean", "-ffdx"], cancelled)?;
    if bundle.mode == TopUpMode::Head {
        if git_stdout_cancelled(repo, &["rev-list", "--count", "HEAD"], cancelled)? != "1" {
            bail!("HEAD bundle did not produce depth-one semantics");
        }
    } else if git_stdout_cancelled(repo, &["rev-parse", "--is-shallow-repository"], cancelled)?
        != "false"
    {
        bail!("full bundle produced a shallow repository");
    }
    let status = git_stdout_cancelled(
        repo,
        &[
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignored=matching",
        ],
        cancelled,
    )?;
    if !status.is_empty()
        || git_stdout_cancelled(repo, &["rev-parse", "HEAD"], cancelled)? != target
    {
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

fn verify_exact_full_object_store(
    repo: &Path,
    target: &str,
    maximum: u64,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    ensure_not_cancelled(cancelled)?;
    let scratch = tempfile::tempdir()?;
    let reachable_path = scratch.path().join("reachable");
    let odb_path = scratch.path().join("odb");

    let mut reachable_command = sanitized_git_command();
    reachable_command
        .args([
            "-C",
            repo.to_str().context("non-UTF8 repository path")?,
            "rev-list",
            "--objects",
            "--no-object-names",
            "--end-of-options",
            target,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    configure_process_group(&mut reachable_command);
    let reachable_child = reachable_command.spawn()?;
    let mut reachable_file = std::io::BufWriter::new(std::fs::File::create(&reachable_path)?);
    let mut reachable_count = 0u64;
    reachable_count = consume_reachable_inventory(
        reachable_child,
        &mut reachable_file,
        maximum,
        reachable_count,
        cancelled,
    )?;
    reachable_file.flush()?;

    let mut odb_command = sanitized_git_command();
    odb_command
        .args([
            "-C",
            repo.to_str().context("non-UTF8 repository path")?,
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    configure_process_group(&mut odb_command);
    let odb_child = odb_command.spawn()?;
    let mut odb_file = std::io::BufWriter::new(std::fs::File::create(&odb_path)?);
    let mut odb_count = 0u64;
    odb_count = consume_odb_inventory(odb_child, &mut odb_file, maximum, odb_count, cancelled)?;
    odb_file.flush()?;
    sort_file(&reachable_path, cancelled)?;
    sort_file(&odb_path, cancelled)?;
    let mut reachable = BufReader::new(std::fs::File::open(reachable_path)?).lines();
    let mut odb = BufReader::new(std::fs::File::open(odb_path)?).lines();
    loop {
        ensure_not_cancelled(cancelled)?;
        let left = reachable.next().transpose()?;
        let right = odb.next().transpose()?;
        if left != right {
            bail!("full clone object database is not the exact target closure")
        }
        if left.is_none() {
            break;
        }
    }
    let _ = (reachable_count, odb_count);
    Ok(())
}

fn consume_reachable_inventory(
    child: std::process::Child,
    writer: &mut dyn Write,
    maximum: u64,
    mut count: u64,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<u64> {
    crate::git::consume_child_lines_cancellable(child, cancelled, |oid| {
        validate_oid("reachable object", &oid)?;
        count = count
            .checked_add(1)
            .context("reachable object count overflow")?;
        if count > maximum {
            bail!("reachable object count exceeds installer limit")
        }
        writeln!(writer, "{oid}")?;
        Ok(())
    })?;
    Ok(count)
}

fn consume_odb_inventory(
    child: std::process::Child,
    writer: &mut dyn Write,
    maximum: u64,
    mut count: u64,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<u64> {
    crate::git::consume_child_lines_cancellable(child, cancelled, |line| {
        let mut fields = line.split_ascii_whitespace();
        let oid = fields.next().context("ODB object has no id")?;
        validate_oid("ODB object", oid)?;
        let kind = fields.next().context("ODB object has no type")?;
        if !matches!(kind, "commit" | "tree" | "blob" | "tag") {
            bail!("ODB object has an unsupported type")
        }
        let _: u64 = fields
            .next()
            .context("ODB object has no size")?
            .parse()
            .context("ODB object size is invalid")?;
        if fields.next().is_some() {
            bail!("ODB object record has trailing fields")
        }
        count = count.checked_add(1).context("ODB object count overflow")?;
        if count > maximum {
            bail!("ODB object count exceeds installer limit")
        }
        writeln!(writer, "{oid}")?;
        Ok(())
    })?;
    Ok(count)
}

fn sort_file(path: &Path, cancelled: &tokio_util::sync::CancellationToken) -> Result<()> {
    let mut command = sanitized_sort_command();
    command.arg("-o").arg(path).arg(path);
    configure_process_group(&mut command);
    let child = command.spawn()?;
    let mut child = ReapedChild::new(child);
    let status = loop {
        if cancelled.is_cancelled() {
            bail!("clone installation cancelled")
        }
        if let Some(status) = child.child_mut().try_wait()? {
            child.mark_reaped();
            break status;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    if !status.success() {
        bail!("object inventory sort failed")
    }
    Ok(())
}

struct ReapedChild {
    child: Option<std::process::Child>,
}

impl ReapedChild {
    fn new(child: std::process::Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut std::process::Child {
        self.child.as_mut().expect("child is present until reaped")
    }

    fn mark_reaped(&mut self) {
        self.child = None;
    }
}

impl Drop for ReapedChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            #[cfg(unix)]
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGKILL);
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn configure_process_group(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
}

fn sanitized_sort_command() -> Command {
    let mut command = Command::new("sort");
    let path = std::env::var_os("PATH");
    command.env_clear();
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command.env("LC_ALL", "C");
    command
}

fn validate_target_path_components(
    repo: &Path,
    target: &str,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    let output = git_output_cancelled(
        repo,
        &["ls-tree", "-rz", "--full-tree", "-r", target],
        cancelled,
    )?;
    if !output.status.success() {
        bail!("cannot enumerate exact target paths")
    }
    let mut paths = Vec::new();
    for record in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let tab = record
            .iter()
            .position(|byte| *byte == b'\t')
            .context("malformed target tree entry")?;
        paths.push(&record[tab + 1..]);
    }
    validate_portable_path_components(paths)
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

fn git_stdout_cancelled(
    repo: &Path,
    args: &[&str],
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<String> {
    let output = git_output_cancelled(repo, args, cancelled)?;
    if !output.status.success() {
        bail!("Git validation failed")
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_ok_cancelled(
    repo: &Path,
    args: &[&str],
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    git_stdout_cancelled(repo, args, cancelled).map(|_| ())
}

fn git_output_cancelled(
    repo: &Path,
    args: &[&str],
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<Output> {
    ensure_not_cancelled(cancelled)?;
    let scratch = tempfile::tempdir()?;
    let stdout_path = scratch.path().join("stdout");
    let stderr_path = scratch.path().join("stderr");
    let stdout = std::fs::File::create(&stdout_path)?;
    let stderr = std::fs::File::create(&stderr_path)?;
    let mut command = sanitized_git_command();
    command
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
        ])
        .arg("-C")
        .arg(repo)
        .args(args)
        .stdout(stdout)
        .stderr(stderr);
    let mut child = command
        .spawn()
        .with_context(|| format!("run Git validation in {}", repo.display()))?;
    let status = wait_child_cancelled(&mut child, cancelled)?;
    Ok(Output {
        status,
        stdout: std::fs::read(stdout_path)?,
        stderr: std::fs::read(stderr_path)?,
    })
}

fn wait_child_cancelled(
    child: &mut std::process::Child,
    cancelled: &tokio_util::sync::CancellationToken,
) -> Result<std::process::ExitStatus> {
    loop {
        if cancelled.is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            bail!("clone installation cancelled")
        }
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(test)]
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
    bind_child_to_staging(&mut command);
    command
}

#[cfg(test)]
mod blocker_tests {
    use super::*;

    #[test]
    fn component_aliases_and_prefix_variants_fail_before_materialization() {
        assert!(
            validate_portable_path_components([b"Foo/a".as_slice(), b"foo/b".as_slice()]).is_err()
        );
        assert!(
            validate_portable_path_components(["café/a".as_bytes(), "cafe\u{301}/b".as_bytes()])
                .is_err()
        );
        assert!(
            validate_portable_path_components([b"link".as_slice(), b"link/child".as_slice()])
                .is_err()
        );
        assert!(
            validate_portable_path_components([b"Foo/a".as_slice(), b"Foo/b".as_slice()]).is_ok()
        );
    }

    #[test]
    fn full_object_inventory_rejects_unrelated_loose_object() {
        let temp = tempfile::tempdir().unwrap();
        crate::git::init(temp.path()).unwrap();
        std::fs::write(temp.path().join("tracked"), b"tracked\n").unwrap();
        let run = |args: &[&str]| {
            let output = git_output(temp.path(), args).unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        run(&["config", "user.name", "Test"]);
        run(&["config", "user.email", "test@example.com"]);
        run(&["add", "tracked"]);
        run(&["commit", "-m", "tracked"]);
        let target = String::from_utf8(
            git_output(temp.path(), &["rev-parse", "HEAD"])
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        verify_exact_full_object_store(
            temp.path(),
            &target,
            1000,
            &tokio_util::sync::CancellationToken::new(),
        )
        .unwrap();
        let mut child = sanitized_git_command()
            .arg("-C")
            .arg(temp.path())
            .args(["hash-object", "-w", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(b"unrelated").unwrap();
        assert!(child.wait().unwrap().success());
        assert!(
            verify_exact_full_object_store(
                temp.path(),
                &target,
                1000,
                &tokio_util::sync::CancellationToken::new()
            )
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn parent_replacement_is_detected_before_dirfd_publication() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        let transaction = BoundInstall::new(&parent.join("repo"), "swap").unwrap();
        let scope = transaction.enter_staging().unwrap();
        let staging_name = transaction.staging_name.clone();
        let moved = root.path().join("moved");
        std::fs::rename(&parent, &moved).unwrap();
        std::fs::create_dir(&parent).unwrap();
        let replacement_staging = parent.join(&staging_name);
        std::fs::create_dir(&replacement_staging).unwrap();
        std::fs::write(replacement_staging.join("sentinel"), b"replacement").unwrap();
        std::fs::write(transaction.staging_root().join("original"), b"bound").unwrap();
        let moved_again = root.path().join("moved-again");
        std::fs::rename(&moved, &moved_again).unwrap();
        std::fs::create_dir(&moved).unwrap();
        std::fs::write(
            transaction.staging_root().join("after-second-swap"),
            b"still-bound",
        )
        .unwrap();
        assert!(transaction.publish_repo().is_err());
        assert!(!parent.join("repo").exists());
        assert_eq!(
            std::fs::read(replacement_staging.join("sentinel")).unwrap(),
            b"replacement"
        );
        assert!(moved_again.join(&staging_name).join("original").exists());
        assert!(
            moved_again
                .join(&staging_name)
                .join("after-second-swap")
                .exists()
        );
        drop(scope);
        drop(transaction);
        assert!(moved_again.join(staging_name).exists());
        assert_eq!(
            std::fs::read(replacement_staging.join("sentinel")).unwrap(),
            b"replacement"
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn failed_staging_is_abandoned_without_deleting_swaps_or_unrelated_names() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        std::fs::write(parent.join("unrelated"), b"safe").unwrap();
        let transaction = BoundInstall::new(&parent.join("destination"), "abandon").unwrap();
        let original_name = transaction.staging_name.clone();
        let original = parent.join(&original_name);
        std::fs::write(original.join("original"), b"bound").unwrap();
        let moved = parent.join("same-uid-moved-root");
        std::fs::rename(&original, &moved).unwrap();
        std::fs::create_dir(&original).unwrap();
        std::fs::write(original.join("replacement-sentinel"), b"replacement").unwrap();
        drop(transaction);
        assert_eq!(std::fs::read(moved.join("original")).unwrap(), b"bound");
        assert_eq!(
            std::fs::read(original.join("replacement-sentinel")).unwrap(),
            b"replacement"
        );
        assert_eq!(std::fs::read(parent.join("unrelated")).unwrap(), b"safe");
        assert!(!parent.join("destination").exists());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn failures_leave_bounded_recognizable_private_roots_and_success_moves_its_root() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        for _ in 0..3 {
            let transaction = BoundInstall::new(&parent.join("destination"), "failed").unwrap();
            let scope = transaction.enter_staging().unwrap();
            std::fs::write(transaction.staging_root().join("sentinel"), b"abandoned").unwrap();
            scope.finish().unwrap();
            drop(transaction);
        }
        let abandoned = std::fs::read_dir(&parent)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".ripclone-failed-")
            })
            .collect::<Vec<_>>();
        assert_eq!(abandoned.len(), 3);
        assert!(abandoned.iter().all(|entry| {
            entry.metadata().unwrap().permissions().mode() & 0o777 == 0o700
                && entry.path().join("sentinel").exists()
        }));

        let transaction = BoundInstall::new(&parent.join("destination"), "success").unwrap();
        let staging_name = transaction.staging_name.clone();
        let scope = transaction.enter_staging().unwrap();
        std::fs::write(transaction.staging_root().join("published"), b"exact").unwrap();
        scope.finish().unwrap();
        transaction.publish_repo().unwrap();
        drop(transaction);
        assert_eq!(
            std::fs::read(parent.join("destination/published")).unwrap(),
            b"exact"
        );
        assert!(!parent.join(staging_name).exists());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn staging_capability_is_close_on_exec_after_child_fchdir() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        let transaction = BoundInstall::new(&parent.join("destination"), "cloexec").unwrap();
        let scope = transaction.enter_staging().unwrap();
        let mut command = std::process::Command::new("sh");
        #[cfg(target_os = "linux")]
        command.args([
            "-c",
            "for f in /proc/self/fd/*; do readlink \"$f\" || true; done",
        ]);
        #[cfg(target_os = "macos")]
        command.args(["-c", "for f in /dev/fd/*; do readlink \"$f\" || true; done"]);
        bind_child_to_staging(&mut command);
        let output = command.output().unwrap();
        assert!(output.status.success());
        let listed = String::from_utf8_lossy(&output.stdout);
        assert!(
            !listed.contains(&transaction.staging_name.to_string_lossy().to_string()),
            "staging descriptor leaked across exec: {listed}"
        );
        scope.finish().unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn thread_bound_staging_is_isolated_concurrent_and_restored() {
        let process_cwd = std::env::current_dir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let first_parent = root.path().join("first");
        let second_parent = root.path().join("second");
        std::fs::create_dir(&first_parent).unwrap();
        std::fs::create_dir(&second_parent).unwrap();
        let first = BoundInstall::new(&first_parent.join("repo"), "one").unwrap();
        let second = BoundInstall::new(&second_parent.join("repo"), "two").unwrap();
        let first_path = first_parent.join(&first.staging_name);
        let second_path = second_parent.join(&second.staging_name);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let spawn = |transaction: BoundInstall,
                     barrier: std::sync::Arc<std::sync::Barrier>,
                     value: &'static [u8]| {
            std::thread::spawn(move || {
                let before = std::env::current_dir().unwrap();
                let scope = transaction.enter_staging().unwrap();
                barrier.wait();
                std::fs::write("value", value).unwrap();
                let mut child = std::process::Command::new("sh");
                child.args(["-c", "pwd > child-cwd"]);
                bind_child_to_staging(&mut child);
                assert!(child.status().unwrap().success());
                barrier.wait();
                drop(scope);
                assert_eq!(std::env::current_dir().unwrap(), before);
                transaction
            })
        };
        let one = spawn(first, barrier.clone(), b"one");
        let two = spawn(second, barrier.clone(), b"two");
        barrier.wait();
        assert_eq!(std::env::current_dir().unwrap(), process_cwd);
        barrier.wait();
        let first = one.join().unwrap();
        let second = two.join().unwrap();
        assert_eq!(std::fs::read(first_path.join("value")).unwrap(), b"one");
        assert_eq!(std::fs::read(second_path.join("value")).unwrap(), b"two");
        assert!(first_path.join("child-cwd").is_file());
        assert!(second_path.join("child-cwd").is_file());
        drop(first);
        drop(second);
        assert_eq!(std::env::current_dir().unwrap(), process_cwd);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn thread_bound_staging_restores_after_error_and_panic() {
        let root = tempfile::tempdir().unwrap();
        for panic in [false, true] {
            let parent = root.path().join(if panic { "panic" } else { "error" });
            std::fs::create_dir(&parent).unwrap();
            let transaction = BoundInstall::new(&parent.join("repo"), "restore").unwrap();
            let before = std::env::current_dir().unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _scope = transaction.enter_staging().unwrap();
                if panic {
                    panic!("installer panic");
                }
                Err::<(), _>(anyhow::anyhow!("installer error"))
            }));
            if panic {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(std::env::current_dir().unwrap(), before);
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn pthread_restore_failure_aborts_instead_of_reusing_bound_thread() {
        const CHILD: &str = "RIPCLONE_TEST_STAGING_RESTORE_ABORT_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let root = tempfile::tempdir().unwrap();
            let parent = root.path().join("parent");
            std::fs::create_dir(&parent).unwrap();
            let transaction =
                BoundInstall::new(&parent.join("destination"), "restore-abort").unwrap();
            let scope = transaction.enter_staging().unwrap();
            FORCE_STAGING_RESTORE_FAILURE.store(true, std::sync::atomic::Ordering::SeqCst);
            drop(scope);
            panic!("failed CWD restoration did not abort");
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "topup::blocker_tests::pthread_restore_failure_aborts_instead_of_reusing_bound_thread",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(!status.success(), "restoration failure subprocess survived");
    }

    #[test]
    fn cancellation_kills_and_drains_a_running_git_child() {
        let repo = tempfile::tempdir().unwrap();
        crate::git::init(repo.path()).unwrap();
        let mut child = sanitized_git_command()
            .arg("-C")
            .arg(repo.path())
            .args(["cat-file", "--batch"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let token = tokio_util::sync::CancellationToken::new();
        let canceller = token.clone();
        let thread = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            canceller.cancel();
        });
        assert!(wait_child_cancelled(&mut child, &token).is_err());
        thread.join().unwrap();
        assert!(child.try_wait().unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn inventory_rejects_adversarial_records_and_reaps_every_child() {
        struct FailingWriter;
        impl std::io::Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("injected inventory write failure"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        fn run_case(
            odb: bool,
            output: &str,
            maximum: u64,
            initial: u64,
            fail_write: bool,
            cancel: bool,
        ) -> String {
            let temp = tempfile::tempdir().unwrap();
            let pid_path = temp.path().join("pid");
            let tail = if cancel { "; sleep 30" } else { "" };
            let script = format!(
                "echo $$ > '{}'; printf '%s' '{}'{}",
                pid_path.display(),
                output,
                tail
            );
            let mut command = Command::new("sh");
            command
                .args(["-c", &script])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            configure_process_group(&mut command);
            let child = command.spawn().unwrap();
            let token = tokio_util::sync::CancellationToken::new();
            let canceller = cancel.then(|| {
                let token = token.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    token.cancel();
                })
            });
            let mut bytes = Vec::new();
            let mut failing = FailingWriter;
            let writer: &mut dyn std::io::Write =
                if fail_write { &mut failing } else { &mut bytes };
            let error = if odb {
                consume_odb_inventory(child, writer, maximum, initial, &token).unwrap_err()
            } else {
                consume_reachable_inventory(child, writer, maximum, initial, &token).unwrap_err()
            };
            if let Some(canceller) = canceller {
                canceller.join().unwrap();
            }
            let pid: i32 = std::fs::read_to_string(pid_path)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            assert_ne!(
                unsafe { libc::kill(pid, 0) },
                0,
                "inventory child was not reaped"
            );
            format!("{error:#}")
        }

        let oid = "1".repeat(40);
        assert!(run_case(false, "bad\n", 10, 0, false, false).contains("SHA-1"));
        assert!(
            run_case(false, &format!("{oid}\n"), u64::MAX, u64::MAX, false, false)
                .contains("overflow")
        );
        assert!(
            run_case(true, &format!("{oid} delta 1\n"), 10, 0, false, false)
                .contains("unsupported")
        );
        assert!(
            run_case(true, &format!("{oid} blob 1 extra\n"), 10, 0, false, false)
                .contains("trailing")
        );
        assert!(run_case(true, &format!("{oid} blob 1\n"), 0, 0, false, false).contains("exceeds"));
        assert!(
            run_case(true, &format!("{oid} blob 1\n"), 10, 0, true, false)
                .contains("write failure")
        );
        assert!(run_case(true, "partial", 10, 0, false, true).contains("cancelled"));
    }
}
