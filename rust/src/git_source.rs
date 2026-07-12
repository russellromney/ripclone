//! Backend-neutral, immutable Git build sources.
//!
//! A provider fetch is packaged into self-contained pack/index pairs. Uploading
//! those bytes only creates a [`PreparedGitSource`]; a future durable registry
//! must separately authenticate the exact root before a worker can materialize
//! it. This module intentionally contains no database or provider integration.

use crate::artifact_manifest::{CasBlob, GitPackPair};
use crate::cas::Cas;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Read, Seek, Write};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub const GIT_SOURCE_SCHEMA: u32 = 1;
pub const GIT_SOURCE_FORMAT: u32 = 1;
const DIGEST_DOMAIN: &[u8] = b"ripclone/git-build-source/semantic/v1\0";

#[cfg(unix)]
thread_local! {
    static SCRATCH_CHILD_FD: std::cell::Cell<Option<libc::c_int>> = const { std::cell::Cell::new(None) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitObjectFormat {
    Sha1,
    Sha256,
}

impl GitObjectFormat {
    fn oid_len(self) -> usize {
        match self {
            Self::Sha1 => 40,
            Self::Sha256 => 64,
        }
    }

    fn git_name(self) -> &'static str {
        match self {
            Self::Sha1 => "sha1",
            Self::Sha256 => "sha256",
        }
    }
}

/// Explicit extension point. Version one supports a complete cold root only;
/// incremental layouts must earn a new typed variant and verifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitSourceLayout {
    ColdComplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitSourceManifest {
    schema_version: u32,
    source_format_version: u32,
    layout: GitSourceLayout,
    workspace: String,
    repo: String,
    commit: String,
    object_format: GitObjectFormat,
    packs: Vec<GitPackPair>,
    object_count: u64,
    object_set_digest: String,
    aggregate_pack_bytes: u64,
    semantic_digest: String,
}

impl GitSourceManifest {
    pub fn workspace(&self) -> &str {
        &self.workspace
    }
    pub fn repo(&self) -> &str {
        &self.repo
    }
    pub fn commit(&self) -> &str {
        &self.commit
    }
    pub fn object_format(&self) -> GitObjectFormat {
        self.object_format
    }
    pub fn packs(&self) -> &[GitPackPair] {
        &self.packs
    }
    pub fn object_count(&self) -> u64 {
        self.object_count
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate(&GitSourceLimits::default())?;
        serde_json::to_vec(self).context("encode canonical Git source manifest")
    }

    fn decode_canonical(bytes: &[u8], limits: &GitSourceLimits) -> Result<Self> {
        if bytes.is_empty() || bytes.len() as u64 > limits.max_manifest_bytes {
            bail!("Git source manifest exceeds bounded read limit")
        }
        let manifest: Self = serde_json::from_slice(bytes).context("decode Git source manifest")?;
        manifest.validate(limits)?;
        if serde_json::to_vec(&manifest)? != bytes {
            bail!("Git source manifest is not canonical")
        }
        Ok(manifest)
    }

    fn validate(&self, limits: &GitSourceLimits) -> Result<()> {
        limits.validate()?;
        if self.schema_version != GIT_SOURCE_SCHEMA
            || self.source_format_version != GIT_SOURCE_FORMAT
            || self.layout != GitSourceLayout::ColdComplete
        {
            bail!("unsupported Git source manifest version or layout")
        }
        validate_identity(&self.workspace, "workspace")?;
        validate_identity(&self.repo, "repo")?;
        validate_oid(&self.commit, self.object_format)?;
        if self.packs.is_empty() || self.packs.len() > limits.max_packs {
            bail!("Git source pack count is out of bounds")
        }
        if self.object_count == 0 || self.object_count > limits.max_objects as u64 {
            bail!("Git source object count is out of bounds")
        }
        Cas::validate_artifact_id(&self.object_set_digest)?;
        Cas::validate_artifact_id(&self.semantic_digest)?;
        let mut hashes = HashSet::new();
        let mut aggregate = 0u64;
        for pair in &self.packs {
            validate_blob(&pair.pack, limits.max_pack_bytes)?;
            validate_blob(&pair.index, limits.max_index_bytes)?;
            if !hashes.insert(pair.pack.hash.as_str()) || !hashes.insert(pair.index.hash.as_str()) {
                bail!("Git source contains duplicate CAS children")
            }
            aggregate = aggregate
                .checked_add(pair.pack.len)
                .and_then(|n| n.checked_add(pair.index.len))
                .context("Git source aggregate length overflow")?;
            if aggregate > limits.max_total_pack_bytes {
                bail!("Git source aggregate exceeds limit")
            }
        }
        if aggregate != self.aggregate_pack_bytes {
            bail!("Git source aggregate length mismatch")
        }
        if self.compute_semantic_digest()? != self.semantic_digest {
            bail!("Git source semantic digest mismatch")
        }
        Ok(())
    }

    fn compute_semantic_digest(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Semantic<'a> {
            schema_version: u32,
            source_format_version: u32,
            layout: GitSourceLayout,
            workspace: &'a str,
            repo: &'a str,
            commit: &'a str,
            object_format: GitObjectFormat,
            packs: &'a [GitPackPair],
            object_count: u64,
            object_set_digest: &'a str,
            aggregate_pack_bytes: u64,
        }
        let payload = serde_json::to_vec(&Semantic {
            schema_version: self.schema_version,
            source_format_version: self.source_format_version,
            layout: self.layout,
            workspace: &self.workspace,
            repo: &self.repo,
            commit: &self.commit,
            object_format: self.object_format,
            packs: &self.packs,
            object_count: self.object_count,
            object_set_digest: &self.object_set_digest,
            aggregate_pack_bytes: self.aggregate_pack_bytes,
        })?;
        let mut h = Sha256::new();
        h.update(DIGEST_DOMAIN);
        h.update((payload.len() as u64).to_be_bytes());
        h.update(payload);
        Ok(hex::encode(h.finalize()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitSourceLimits {
    pub max_manifest_bytes: u64,
    pub max_packs: usize,
    pub max_pack_bytes: u64,
    pub max_index_bytes: u64,
    pub max_total_pack_bytes: u64,
    pub max_objects: usize,
    pub max_object_bytes: u64,
    pub max_total_object_bytes: u64,
    pub target_pack_raw_bytes: u64,
}

impl Default for GitSourceLimits {
    fn default() -> Self {
        Self {
            max_manifest_bytes: 16 * 1024 * 1024,
            max_packs: 16_384,
            max_pack_bytes: 16 * 1024 * 1024 * 1024,
            max_index_bytes: 2 * 1024 * 1024 * 1024,
            max_total_pack_bytes: 1024 * 1024 * 1024 * 1024,
            max_objects: 50_000_000,
            max_object_bytes: 1024 * 1024 * 1024 * 1024,
            max_total_object_bytes: 4 * 1024 * 1024 * 1024 * 1024,
            target_pack_raw_bytes: 256 * 1024 * 1024,
        }
    }
}

impl GitSourceLimits {
    fn validate(&self) -> Result<()> {
        if self.max_manifest_bytes == 0
            || self.max_packs == 0
            || self.max_pack_bytes == 0
            || self.max_index_bytes == 0
            || self.max_total_pack_bytes == 0
            || self.max_objects == 0
            || self.max_object_bytes == 0
            || self.max_total_object_bytes == 0
            || self.target_pack_raw_bytes == 0
        {
            bail!("Git source limits must be nonzero")
        }
        Ok(())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct BoundScratch {
    root: OwnedFd,
    attempt: OwnedFd,
    name: std::ffi::OsString,
    dev: u64,
    ino: u64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl BoundScratch {
    fn new(path: &Path, prefix: &str) -> Result<Self> {
        let expected = std::fs::metadata(path).context("stat configured Git source scratch")?;
        if !expected.is_dir() {
            bail!("configured Git source scratch is not a directory")
        }
        let canonical = path
            .canonicalize()
            .context("canonicalize configured Git source scratch")?;
        let canonical_c = CString::new(canonical.as_os_str().as_bytes())?;
        let fd = unsafe {
            libc::open(
                canonical_c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("open Git source scratch handle");
        }
        let root = unsafe { OwnedFd::from_raw_fd(fd) };
        let root_stat = fd_stat(root.as_raw_fd())?;
        if root_stat.st_dev as u64 != expected.dev() || root_stat.st_ino != expected.ino() {
            bail!("configured Git source scratch changed while binding")
        }
        let name = std::ffi::OsString::from(format!(
            ".{prefix}.{}",
            hex::encode(rand::random::<[u8; 16]>())
        ));
        let name_c = cstring(&name)?;
        if unsafe { libc::mkdirat(root.as_raw_fd(), name_c.as_ptr(), 0o700) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("create bound Git source scratch attempt");
        }
        let attempt = openat_dir(root.as_raw_fd(), &name)?;
        let stat = fd_stat(attempt.as_raw_fd())?;
        Ok(Self {
            root,
            attempt,
            name,
            dev: stat.st_dev as u64,
            ino: stat.st_ino,
        })
    }

    fn path(&self) -> PathBuf {
        #[cfg(target_os = "linux")]
        return PathBuf::from(format!("/proc/self/fd/{}", self.attempt.as_raw_fd()));
        #[cfg(target_os = "macos")]
        return PathBuf::from(".");
    }

    fn enter(&self) -> Result<ScratchScope> {
        let duplicate = unsafe { libc::dup(self.attempt.as_raw_fd()) };
        if duplicate < 0 {
            return Err(std::io::Error::last_os_error())
                .context("duplicate Git source scratch descriptor");
        }
        let staging = unsafe { OwnedFd::from_raw_fd(duplicate) };
        #[cfg(target_os = "linux")]
        {
            let previous = SCRATCH_CHILD_FD.with(|slot| slot.replace(Some(staging.as_raw_fd())));
            Ok(ScratchScope { staging, previous })
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
                    .context("save Git source installer thread cwd");
            }
            let old = unsafe { OwnedFd::from_raw_fd(old) };
            if unsafe { pthread_fchdir_np(staging.as_raw_fd()) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("bind Git source installer thread cwd");
            }
            let previous = SCRATCH_CHILD_FD.with(|slot| slot.replace(Some(staging.as_raw_fd())));
            Ok(ScratchScope {
                old,
                staging,
                previous,
            })
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for BoundScratch {
    fn drop(&mut self) {
        let _ = remove_dir_contents(self.attempt.as_raw_fd());
        if let Ok(stat) = entry_stat(self.root.as_raw_fd(), &self.name)
            && stat.st_dev as u64 == self.dev
            && stat.st_ino == self.ino
            && let Ok(name) = cstring(&self.name)
        {
            unsafe {
                libc::unlinkat(self.root.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR);
            }
        }
    }
}

#[cfg(target_os = "linux")]
struct ScratchScope {
    staging: OwnedFd,
    previous: Option<libc::c_int>,
}

#[cfg(target_os = "linux")]
impl Drop for ScratchScope {
    fn drop(&mut self) {
        SCRATCH_CHILD_FD.with(|slot| slot.set(self.previous));
        let _ = self.staging.as_raw_fd();
    }
}

#[cfg(target_os = "macos")]
struct ScratchScope {
    old: OwnedFd,
    staging: OwnedFd,
    previous: Option<libc::c_int>,
}

#[cfg(target_os = "macos")]
impl Drop for ScratchScope {
    fn drop(&mut self) {
        unsafe {
            pthread_fchdir_np(self.old.as_raw_fd());
        }
        SCRATCH_CHILD_FD.with(|slot| slot.set(self.previous));
        let _ = self.staging.as_raw_fd();
    }
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn pthread_fchdir_np(fd: libc::c_int) -> libc::c_int;
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
struct BoundScratch;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl BoundScratch {
    fn new(_: &Path, _: &str) -> Result<Self> {
        bail!("handle-bound Git source scratch is unsupported on this platform")
    }
    fn path(&self) -> PathBuf {
        unreachable!()
    }
    fn enter(&self) -> Result<ScratchScope> {
        bail!("handle-bound Git source scratch is unsupported on this platform")
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
struct ScratchScope;

/// Storage-neutral sink. Implementations must verify the declared SHA-256 and
/// length before returning success.
pub trait GitSourceUploader: Send + Sync {
    fn put_file(&self, blob: &CasBlob, source: &Path) -> Result<()>;
    fn put_bytes(&self, blob: &CasBlob, bytes: &[u8]) -> Result<()>;
}

/// Storage-neutral loader. Implementations must create `destination` and must
/// verify the declared SHA-256 and length before returning success.
pub trait GitSourceLoader: Send + Sync {
    fn load_file(
        &self,
        blob: &CasBlob,
        destination: &Path,
        cancelled: &CancellationToken,
    ) -> Result<()>;
    fn load_bytes(
        &self,
        blob: &CasBlob,
        maximum: u64,
        cancelled: &CancellationToken,
    ) -> Result<Vec<u8>>;
}

#[derive(Clone)]
pub struct CasGitSourceStore {
    root: PathBuf,
}
impl CasGitSourceStore {
    pub fn new(cas: &Cas) -> Self {
        let root = if cas.root().is_absolute() {
            cas.root().to_owned()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(cas.root())
        };
        Self {
            root: root.canonicalize().unwrap_or(root),
        }
    }

    fn cas(&self) -> Result<Cas> {
        Cas::new(&self.root)
    }
}

impl GitSourceUploader for CasGitSourceStore {
    fn put_file(&self, blob: &CasBlob, source: &Path) -> Result<()> {
        let len = self.cas()?.put_file_with_hash(&blob.hash, source)?;
        if len != blob.len {
            bail!("uploaded Git source child length mismatch")
        }
        Ok(())
    }
    fn put_bytes(&self, blob: &CasBlob, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 != blob.len {
            bail!("uploaded Git source root length mismatch")
        }
        self.cas()?.put_with_hash(&blob.hash, bytes)
    }
}

impl GitSourceLoader for CasGitSourceStore {
    fn load_file(
        &self,
        blob: &CasBlob,
        destination: &Path,
        cancelled: &CancellationToken,
    ) -> Result<()> {
        check_cancelled(cancelled)?;
        validate_blob(blob, u64::MAX)?;
        copy_verified_create_new(&self.cas()?.path(&blob.hash), destination, blob, cancelled)
    }
    fn load_bytes(
        &self,
        blob: &CasBlob,
        maximum: u64,
        cancelled: &CancellationToken,
    ) -> Result<Vec<u8>> {
        check_cancelled(cancelled)?;
        validate_blob(blob, maximum)?;
        let path = self.cas()?.path(&blob.hash);
        let metadata = std::fs::symlink_metadata(&path).context("stat Git source CAS root")?;
        if !metadata.file_type().is_file() || metadata.len() != blob.len {
            bail!("Git source CAS root is not the declared regular file")
        }
        let capacity: usize = blob
            .len
            .try_into()
            .context("Git source root is too large")?;
        let mut input = File::open(path).context("open Git source CAS root")?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 1024 * 1024];
        while bytes.len() < capacity {
            check_cancelled(cancelled)?;
            let wanted = (capacity - bytes.len()).min(buffer.len());
            let read = input
                .read(&mut buffer[..wanted])
                .context("read Git source CAS root")?;
            if read == 0 {
                break;
            }
            #[cfg(test)]
            std::thread::sleep(Duration::from_millis(1));
            hasher.update(&buffer[..read]);
            bytes.extend_from_slice(&buffer[..read]);
        }
        let mut trailing = [0u8; 1];
        check_cancelled(cancelled)?;
        if input.read(&mut trailing)? != 0
            || bytes.len() != capacity
            || hex::encode(hasher.finalize()) != blob.hash
        {
            bail!("Git source CAS root does not match descriptor")
        }
        Ok(bytes)
    }
}

/// Trusted provider adapter output. Construction remains crate-private so a
/// path supplied by an API caller cannot become provider authority.
pub struct TrustedProviderFetch {
    repo_path: PathBuf,
    workspace: String,
    repo: String,
    commit: String,
    object_format: GitObjectFormat,
}

impl TrustedProviderFetch {
    #[allow(dead_code)] // provider adapter wiring lands in the registry/cutover wave
    pub(crate) fn from_pinned_fetch(
        repo_path: PathBuf,
        workspace: String,
        repo: String,
        commit: String,
    ) -> Result<Self> {
        reject_symlink_dir(&repo_path)?;
        let repo_path = repo_path
            .canonicalize()
            .context("bind trusted provider fetch repository")?;
        validate_identity(&workspace, "workspace")?;
        validate_identity(&repo, "repo")?;
        let object_format = detect_object_format(&repo_path)?;
        validate_oid(&commit, object_format)?;
        let actual = safe_git(
            &repo_path,
            &["rev-parse", "--verify", &format!("{commit}^{{commit}}")],
        )?;
        if text(&actual)? != commit {
            bail!("provider fetch does not contain the exact pinned commit")
        }
        Ok(Self {
            repo_path,
            workspace,
            repo,
            commit,
            object_format,
        })
    }
}

/// Locally verified and uploaded, but deliberately not registered/durable.
pub struct PreparedGitSource {
    root: CasBlob,
    manifest: GitSourceManifest,
}
impl PreparedGitSource {
    pub fn root(&self) -> &CasBlob {
        &self.root
    }
    pub fn manifest(&self) -> &GitSourceManifest {
        &self.manifest
    }
}

/// Exact authority minted only after a future registry transaction establishes
/// durability and retention. It cannot be constructed by untrusted callers.
#[derive(Clone)]
pub struct AuthenticatedGitSource {
    root: CasBlob,
    workspace: String,
    repo: String,
    commit: String,
    object_format: GitObjectFormat,
}

impl AuthenticatedGitSource {
    /// Mint the worker capability from an already authenticated durable
    /// registry row. This function does not perform or pretend to perform that
    /// transaction; the future registry adapter owns that responsibility.
    pub(crate) fn from_registry_record(
        root: CasBlob,
        workspace: String,
        repo: String,
        commit: String,
        object_format: GitObjectFormat,
    ) -> Result<Self> {
        validate_blob(&root, u64::MAX)?;
        validate_identity(&workspace, "workspace")?;
        validate_identity(&repo, "repo")?;
        validate_oid(&commit, object_format)?;
        Ok(Self {
            root,
            workspace,
            repo,
            commit,
            object_format,
        })
    }
}

pub struct GitSourcePackager<'a, U: GitSourceUploader> {
    local_cas: &'a Cas,
    uploader: &'a U,
    scratch: &'a Path,
    limits: GitSourceLimits,
}

impl<'a, U: GitSourceUploader> GitSourcePackager<'a, U> {
    pub fn new(
        local_cas: &'a Cas,
        uploader: &'a U,
        scratch: &'a Path,
        limits: GitSourceLimits,
    ) -> Self {
        Self {
            local_cas,
            uploader,
            scratch,
            limits,
        }
    }

    pub fn prepare(
        &self,
        source: TrustedProviderFetch,
        cancelled: &CancellationToken,
    ) -> Result<PreparedGitSource> {
        self.limits.validate()?;
        let attempt = BoundScratch::new(self.scratch, "git-source-pack")?;
        let _scope = attempt.enter()?;
        let scratch = attempt.path();
        let local_cas = Cas::new(
            self.local_cas
                .root()
                .canonicalize()
                .context("bind local Git source CAS")?,
        )?;
        check_cancelled(cancelled)?;
        let objects = enumerate_closure(
            &source.repo_path,
            &source.commit,
            source.object_format,
            &self.limits,
            cancelled,
            &scratch,
        )?;
        let object_set_digest = objects.digest.clone();
        let pack_scratch = tempfile::Builder::new()
            .prefix("git-source-pack.")
            .tempdir_in(&scratch)?;
        let partitions = partition_inventory(
            &source.repo_path,
            &objects,
            &self.limits,
            pack_scratch.path(),
            cancelled,
        )?;
        let mut built = Vec::with_capacity(partitions.len());
        for (index, partition) in partitions.iter().enumerate() {
            built.push(build_source_pack(
                &source.repo_path,
                partition,
                &local_cas,
                pack_scratch.path(),
                index,
                cancelled,
            )?);
        }
        if built.is_empty() || built.len() > self.limits.max_packs {
            bail!("Git source pack count is out of bounds")
        }
        let mut packs = Vec::with_capacity(built.len());
        for (pack_hash, pack_len, index_hash, index_len) in built {
            packs.push(GitPackPair {
                pack: CasBlob {
                    hash: pack_hash,
                    len: pack_len,
                },
                index: CasBlob {
                    hash: index_hash,
                    len: index_len,
                },
            });
        }
        let aggregate_pack_bytes = packs
            .iter()
            .try_fold(0u64, |n, p| {
                n.checked_add(p.pack.len)?.checked_add(p.index.len)
            })
            .context("Git source aggregate overflow")?;
        let mut manifest = GitSourceManifest {
            schema_version: GIT_SOURCE_SCHEMA,
            source_format_version: GIT_SOURCE_FORMAT,
            layout: GitSourceLayout::ColdComplete,
            workspace: source.workspace,
            repo: source.repo,
            commit: source.commit,
            object_format: source.object_format,
            packs,
            object_count: objects.count,
            object_set_digest,
            aggregate_pack_bytes,
            semantic_digest: String::new(),
        };
        manifest.semantic_digest = manifest.compute_semantic_digest()?;
        manifest.validate(&self.limits)?;
        // Verify the locally produced source exactly before any root can be uploaded.
        verify_local_manifest(&manifest, &local_cas, &scratch, &self.limits, cancelled)?;
        let root_bytes = manifest.canonical_bytes()?;
        let root = CasBlob {
            hash: hex::encode(Sha256::digest(&root_bytes)),
            len: root_bytes.len() as u64,
        };
        // Publication order is a safety property: every child, then the sole root.
        for pair in &manifest.packs {
            check_cancelled(cancelled)?;
            self.uploader
                .put_file(&pair.pack, &local_cas.path(&pair.pack.hash))?;
            self.uploader
                .put_file(&pair.index, &local_cas.path(&pair.index.hash))?;
        }
        check_cancelled(cancelled)?;
        self.uploader.put_bytes(&root, &root_bytes)?;
        Ok(PreparedGitSource { root, manifest })
    }
}

pub struct MaterializedGitSource {
    _scope: ScratchScope,
    _scratch: BoundScratch,
    path: PathBuf,
    commit: String,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl MaterializedGitSource {
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn commit(&self) -> &str {
        &self.commit
    }
}

pub struct GitSourceMaterializer<'a, L: GitSourceLoader> {
    loader: &'a L,
    scratch: &'a Path,
    limits: GitSourceLimits,
}

impl<'a, L: GitSourceLoader> GitSourceMaterializer<'a, L> {
    pub fn new(loader: &'a L, scratch: &'a Path, limits: GitSourceLimits) -> Self {
        Self {
            loader,
            scratch,
            limits,
        }
    }
    pub fn materialize(
        &self,
        authority: &AuthenticatedGitSource,
        cancelled: &CancellationToken,
    ) -> Result<MaterializedGitSource> {
        let attempt = BoundScratch::new(self.scratch, "git-source-materialize")?;
        let scope = attempt.enter()?;
        let scratch = attempt.path();
        let root_bytes =
            self.loader
                .load_bytes(&authority.root, self.limits.max_manifest_bytes, cancelled)?;
        verify_bytes(&authority.root, &root_bytes)?;
        let manifest = GitSourceManifest::decode_canonical(&root_bytes, &self.limits)?;
        if manifest.workspace != authority.workspace
            || manifest.repo != authority.repo
            || manifest.commit != authority.commit
            || manifest.object_format != authority.object_format
        {
            bail!("authenticated Git source identity mismatch")
        }
        let directory = scratch.join("repo");
        std::fs::create_dir(&directory)?;
        init_bare(&directory, manifest.object_format, cancelled)?;
        let pack_dir = directory.join("objects/pack");
        for (i, pair) in manifest.packs.iter().enumerate() {
            check_cancelled(cancelled)?;
            // Verify each pair in an otherwise empty object database. Loading
            // it into the union first would allow a thin/cross-pack delta to
            // resolve its base from a previously installed pair.
            let pair_directory = tempfile::Builder::new()
                .prefix("git-source-pair.")
                .tempdir_in(&scratch)?;
            init_bare(pair_directory.path(), manifest.object_format, cancelled)?;
            let pair_pack_dir = pair_directory.path().join("objects/pack");
            let base = format!("pack-source-{i:08}");
            let pack = pair_pack_dir.join(format!("{base}.pack"));
            let index = pair_pack_dir.join(format!("{base}.idx"));
            self.loader.load_file(&pair.pack, &pack, cancelled)?;
            self.loader.load_file(&pair.index, &index, cancelled)?;
            verify_file_at(&pack, &pair.pack, cancelled)?;
            verify_file_at(&index, &pair.index, cancelled)?;
            safe_git_ok_quiet_cancelled(
                pair_directory.path(),
                &[
                    "verify-pack",
                    index.to_str().context("non-UTF8 index path")?,
                ],
                cancelled,
            )
            .context("Git source pack/index pair failed verification")?;
            std::fs::rename(&pack, pack_dir.join(format!("{base}.pack")))?;
            std::fs::rename(&index, pack_dir.join(format!("{base}.idx")))?;
        }
        safe_git_ok_cancelled(
            &directory,
            &["update-ref", "refs/heads/ripclone-source", &manifest.commit],
            cancelled,
        )?;
        safe_git_ok_cancelled(
            &directory,
            &["symbolic-ref", "HEAD", "refs/heads/ripclone-source"],
            cancelled,
        )?;
        safe_git_ok_quiet_cancelled(
            &directory,
            &["fsck", "--full", "--strict", "--no-dangling"],
            cancelled,
        )?;
        let objects = enumerate_closure(
            &directory,
            &manifest.commit,
            manifest.object_format,
            &self.limits,
            cancelled,
            &scratch,
        )?;
        validate_inventory_sizes(&directory, &objects, &self.limits, None, cancelled)?;
        if objects.count != manifest.object_count || objects.digest != manifest.object_set_digest {
            bail!("materialized Git source closure does not match manifest")
        }
        let all_objects = enumerate_all_objects(
            &directory,
            manifest.object_format,
            &self.limits,
            cancelled,
            &scratch,
        )?;
        if all_objects.count != objects.count || all_objects.digest != objects.digest {
            bail!("materialized Git source contains objects outside the exact closure")
        }
        Ok(MaterializedGitSource {
            _scope: scope,
            _scratch: attempt,
            path: directory,
            commit: manifest.commit,
            _same_thread: std::marker::PhantomData,
        })
    }
}

fn verify_local_manifest(
    manifest: &GitSourceManifest,
    cas: &Cas,
    scratch: &Path,
    limits: &GitSourceLimits,
    cancelled: &CancellationToken,
) -> Result<()> {
    let store = CasGitSourceStore::new(cas);
    let root_bytes = manifest.canonical_bytes()?;
    let prepared = PreparedGitSource {
        root: CasBlob {
            hash: hex::encode(Sha256::digest(&root_bytes)),
            len: root_bytes.len() as u64,
        },
        manifest: manifest.clone(),
    };
    // Local verification uses the same loader path as a remote worker. This is
    // only a CAS write: without `AuthenticatedGitSource` from the registry it
    // conveys no authority, and all pack/index children already exist locally.
    cas.put_with_hash(&prepared.root.hash, &root_bytes)?;
    let auth = AuthenticatedGitSource::from_registry_record(
        prepared.root.clone(),
        manifest.workspace.clone(),
        manifest.repo.clone(),
        manifest.commit.clone(),
        manifest.object_format,
    )?;
    let materializer = GitSourceMaterializer::new(&store, scratch, limits.clone());
    let materialized = materializer.materialize(&auth, cancelled)?;
    let actual = text(&safe_git_cancelled(
        materialized.path(),
        &["rev-parse", "HEAD"],
        cancelled,
    )?)?;
    if actual != manifest.commit {
        bail!("local Git source verification resolved the wrong commit")
    }
    Ok(())
}

fn build_source_pack(
    repo: &Path,
    objects: &Path,
    cas: &Cas,
    scratch: &Path,
    partition: usize,
    cancelled: &CancellationToken,
) -> Result<(String, u64, String, u64)> {
    check_cancelled(cancelled)?;
    let prefix = scratch.join(format!("source-{partition:08}"));
    let input = File::open(objects).context("open Git source pack partition")?;
    let mut command = safe_command();
    command
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null", "-c", "pack.threads=1"])
        .arg("pack-objects")
        .arg(&prefix)
        .stdin(Stdio::from(input))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = ReapedChild::new(command.spawn().context("spawn Git source pack-objects")?)
        .wait_cancelled(cancelled, "Git source pack-objects")?;
    if !status.success() {
        bail!("Git source pack-objects failed")
    }
    check_cancelled(cancelled)?;
    let mut pack = None;
    let mut index = None;
    for entry in std::fs::read_dir(scratch)? {
        let path = entry?.path();
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(&format!("source-{partition:08}-")))
        {
            continue;
        }
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("pack") if pack.is_none() => pack = Some(path),
            Some("idx") if index.is_none() => index = Some(path),
            Some("pack" | "idx") => bail!("Git source emitted multiple partition pack pairs"),
            _ => {}
        }
    }
    let pack = pack.context("Git source emitted no pack")?;
    let index = index.context("Git source emitted no index")?;
    let (pack_hash, pack_len) = cas.put_file(pack)?;
    let (index_hash, index_len) = cas.put_file(index)?;
    Ok((pack_hash, pack_len, index_hash, index_len))
}

struct ObjectInventory {
    _directory: tempfile::TempDir,
    path: PathBuf,
    count: u64,
    digest: String,
}

fn enumerate_closure(
    repo: &Path,
    commit: &str,
    format: GitObjectFormat,
    limits: &GitSourceLimits,
    cancelled: &CancellationToken,
    scratch: &Path,
) -> Result<ObjectInventory> {
    check_cancelled(cancelled)?;
    let inventory = enumerate_inventory(
        repo,
        &["rev-list", "--objects", "--no-object-names", commit],
        format,
        limits.max_objects,
        cancelled,
        scratch,
        "reachable Git source objects",
    )?;
    if inventory.count == 0 || !inventory_contains(&inventory, commit, cancelled)? {
        bail!("Git source closure is empty or excludes target")
    }
    let ty = text(&safe_git_cancelled(
        repo,
        &["cat-file", "-t", commit],
        cancelled,
    )?)?;
    if ty != "commit" {
        bail!("Git source target is not a commit")
    }
    Ok(inventory)
}

fn validate_inventory_sizes(
    repo: &Path,
    inventory: &ObjectInventory,
    limits: &GitSourceLimits,
    partition_scratch: Option<&Path>,
    cancelled: &CancellationToken,
) -> Result<Vec<PathBuf>> {
    let input = File::open(&inventory.path).context("open Git source inventory input")?;
    let expected_file =
        File::open(&inventory.path).context("open expected Git source inventory")?;
    let mut expected = std::io::BufReader::new(expected_file).lines();
    let mut command = safe_command();
    command
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args([
            "cat-file",
            "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        ])
        .stdin(Stdio::from(input))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn().context("spawn bounded Git source sizing")?;
    let mut total = 0u64;
    let mut count = 0u64;
    let mut partitions = Vec::new();
    let mut partition: Option<File> = None;
    let mut partition_bytes = 0u64;
    crate::git::consume_child_lines_cancellable(child, cancelled, |line| {
        let mut fields = line.split_whitespace();
        let oid = fields.next().context("missing sized Git object id")?;
        let kind = fields.next().context("missing sized Git object type")?;
        let size: u64 = fields
            .next()
            .context("missing sized Git object length")?
            .parse()
            .context("parse Git object length")?;
        let wanted = expected
            .next()
            .transpose()?
            .context("Git source sizing emitted too many objects")?;
        if fields.next().is_some()
            || oid != wanted
            || !matches!(kind, "commit" | "tree" | "blob" | "tag")
        {
            bail!("Git source object sizing emitted forged or reordered data")
        }
        if size > limits.max_object_bytes {
            bail!("Git source object exceeds per-object limit")
        }
        total = total
            .checked_add(size)
            .context("Git source object size overflow")?;
        if total > limits.max_total_object_bytes {
            bail!("Git source objects exceed aggregate limit")
        }
        count = count
            .checked_add(1)
            .context("Git source size count overflow")?;
        if count > inventory.count {
            bail!("Git source sizing exceeds inventory count")
        }
        if let Some(root) = partition_scratch {
            if partition.is_none()
                || (partition_bytes > 0
                    && partition_bytes.saturating_add(size) > limits.target_pack_raw_bytes)
            {
                if partitions.len() == limits.max_packs {
                    bail!("Git source partition count exceeds limit")
                }
                let path = root.join(format!("partition-{:08}", partitions.len()));
                partition = Some(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path)?,
                );
                partitions.push(path);
                partition_bytes = 0;
            }
            writeln!(partition.as_mut().unwrap(), "{oid}")?;
            partition_bytes = partition_bytes
                .checked_add(size)
                .context("Git source partition size overflow")?;
        }
        Ok(())
    })?;
    if count != inventory.count || expected.next().transpose()?.is_some() {
        bail!("Git source object sizing returned an incomplete set")
    }
    Ok(partitions)
}

fn enumerate_all_objects(
    repo: &Path,
    format: GitObjectFormat,
    limits: &GitSourceLimits,
    cancelled: &CancellationToken,
    scratch: &Path,
) -> Result<ObjectInventory> {
    check_cancelled(cancelled)?;
    enumerate_inventory(
        repo,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
        format,
        limits.max_objects,
        cancelled,
        scratch,
        "all materialized Git source objects",
    )
}

fn enumerate_inventory(
    repo: &Path,
    args: &[&str],
    format: GitObjectFormat,
    maximum: usize,
    cancelled: &CancellationToken,
    scratch: &Path,
    role: &str,
) -> Result<ObjectInventory> {
    let directory = tempfile::Builder::new()
        .prefix("git-source-inventory.")
        .tempdir_in(scratch)?;
    let unsorted = directory.path().join("unsorted");
    let sorted = directory.path().join("sorted");
    let mut output = std::io::BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&unsorted)?,
    );
    let mut command = safe_command();
    command
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn().with_context(|| format!("spawn {role}"))?;
    let mut emitted = 0usize;
    crate::git::consume_child_lines_cancellable(child, cancelled, |oid| {
        validate_oid(&oid, format)?;
        emitted = emitted
            .checked_add(1)
            .context("Git source line count overflow")?;
        if emitted > maximum {
            bail!("{role} exceeds object limit")
        }
        writeln!(output, "{oid}")?;
        Ok(())
    })?;
    output.flush()?;
    sort_unique_file(&unsorted, &sorted, cancelled)?;
    let mut reader = std::io::BufReader::new(File::open(&sorted)?);
    let mut h = Sha256::new();
    h.update(b"ripclone/git-build-source/object-set/v1\0");
    let mut count = 0u64;
    let mut line = String::with_capacity(format.oid_len() + 1);
    loop {
        check_cancelled(cancelled)?;
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let oid = line.trim_end_matches(&['\r', '\n'][..]);
        validate_oid(oid, format)?;
        count = count
            .checked_add(1)
            .context("Git source object count overflow")?;
        if count > maximum as u64 {
            bail!("{role} exceeds object limit")
        }
        h.update((oid.len() as u64).to_be_bytes());
        h.update(oid.as_bytes());
    }
    Ok(ObjectInventory {
        _directory: directory,
        path: sorted,
        count,
        digest: hex::encode(h.finalize()),
    })
}

fn sort_unique_file(input: &Path, output: &Path, cancelled: &CancellationToken) -> Result<()> {
    let mut command = Command::new("sort");
    let path = std::env::var_os("PATH").unwrap_or_default();
    command
        .env_clear()
        .env("PATH", path)
        .env("LC_ALL", "C")
        .args(["-u", "-o"])
        .arg(output)
        .arg(input)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    configure_process_group(&mut command);
    bind_child_to_scratch(&mut command);
    let status = ReapedChild::new(command.spawn().context("spawn Git source inventory sort")?)
        .wait_cancelled(cancelled, "Git source inventory sort")?;
    if !status.success() {
        bail!("Git source inventory sort failed")
    }
    Ok(())
}

fn inventory_contains(
    inventory: &ObjectInventory,
    wanted: &str,
    cancelled: &CancellationToken,
) -> Result<bool> {
    let reader = std::io::BufReader::new(File::open(&inventory.path)?);
    for line in reader.lines() {
        check_cancelled(cancelled)?;
        if line? == wanted {
            return Ok(true);
        }
    }
    Ok(false)
}

fn partition_inventory(
    repo: &Path,
    inventory: &ObjectInventory,
    limits: &GitSourceLimits,
    scratch: &Path,
    cancelled: &CancellationToken,
) -> Result<Vec<PathBuf>> {
    let partitions = validate_inventory_sizes(repo, inventory, limits, Some(scratch), cancelled)?;
    if partitions.is_empty() {
        bail!("Git source inventory produced no pack partitions")
    }
    Ok(partitions)
}

fn init_bare(path: &Path, format: GitObjectFormat, cancelled: &CancellationToken) -> Result<()> {
    let mut command = safe_command();
    command
        .args([
            "init",
            "--bare",
            &format!("--object-format={}", format.git_name()),
        ])
        .arg(path);
    let output = run_output_bounded_cancelled(
        command,
        cancelled,
        1024 * 1024,
        "initialize isolated Git source",
    )?;
    if !output.status.success() {
        bail!(
            "initialize isolated Git source failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    Ok(())
}

fn safe_git(repo: &Path, args: &[&str]) -> Result<Output> {
    safe_git_cancelled(repo, args, &CancellationToken::new())
}

fn safe_git_cancelled(repo: &Path, args: &[&str], cancelled: &CancellationToken) -> Result<Output> {
    let mut command = safe_command();
    command
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args);
    run_output_bounded_cancelled(command, cancelled, 1024 * 1024, "run isolated Git")
        .with_context(|| format!("run isolated git {:?}", args))
}
fn safe_git_ok_cancelled(repo: &Path, args: &[&str], cancelled: &CancellationToken) -> Result<()> {
    let out = safe_git_cancelled(repo, args, cancelled)?;
    if !out.status.success() {
        bail!(
            "isolated git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        )
    }
    Ok(())
}
#[cfg(test)]
fn safe_git_ok(repo: &Path, args: &[&str]) -> Result<()> {
    safe_git_ok_cancelled(repo, args, &CancellationToken::new())
}
fn safe_git_ok_quiet_cancelled(
    repo: &Path,
    args: &[&str],
    cancelled: &CancellationToken,
) -> Result<()> {
    const MAX_DIAGNOSTIC_BYTES: u64 = 1024 * 1024;
    let diagnostic = tempfile::tempfile().context("create isolated Git diagnostic file")?;
    let diagnostic_child = diagnostic
        .try_clone()
        .context("clone isolated Git diagnostic file")?;
    let mut command = safe_command();
    command
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::from(diagnostic_child));
    let child = command
        .spawn()
        .with_context(|| format!("run quiet isolated git {:?}", args))?;
    let status = ReapedChild::new(child).wait_cancelled(cancelled, "quiet isolated Git")?;
    if !status.success() {
        if diagnostic.metadata()?.len() > MAX_DIAGNOSTIC_BYTES {
            bail!(
                "quiet isolated git {:?} emitted excessive diagnostics",
                args
            )
        }
        let mut diagnostic = diagnostic;
        diagnostic.rewind()?;
        let mut message = String::new();
        diagnostic.read_to_string(&mut message)?;
        bail!("quiet isolated git {:?} failed: {}", args, message.trim())
    }
    Ok(())
}
fn safe_command() -> Command {
    let mut c = Command::new("git");
    let path = std::env::var_os("PATH").unwrap_or_default();
    let system_root = std::env::var_os("SystemRoot");
    c.env_clear().env("PATH", path).env("HOME", "/nonexistent");
    if let Some(system_root) = system_root {
        c.env("SystemRoot", system_root);
    }
    c.env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_PAGER", "cat")
        .env("LC_ALL", "C");
    c.stdin(Stdio::null());
    configure_process_group(&mut c);
    bind_child_to_scratch(&mut c);
    c
}

struct ReapedChild {
    child: Option<std::process::Child>,
}

impl ReapedChild {
    fn new(child: std::process::Child) -> Self {
        Self { child: Some(child) }
    }

    fn wait_cancelled(
        mut self,
        cancelled: &CancellationToken,
        operation: &str,
    ) -> Result<std::process::ExitStatus> {
        loop {
            check_cancelled(cancelled).with_context(|| operation.to_owned())?;
            match self
                .child
                .as_mut()
                .context("child already reaped")?
                .try_wait()
            {
                Ok(Some(status)) => {
                    self.child = None;
                    return Ok(status);
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(error) => return Err(error).with_context(|| format!("poll {operation}")),
            }
        }
    }
}

impl Drop for ReapedChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            kill_child_tree(child);
            let _ = child.wait();
        }
    }
}

fn kill_child_tree(child: &mut std::process::Child) {
    #[cfg(unix)]
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    let _ = child.kill();
}

fn configure_process_group(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
}

fn run_output_bounded_cancelled(
    mut command: Command,
    cancelled: &CancellationToken,
    maximum: u64,
    operation: &str,
) -> Result<Output> {
    let mut stdout = tempfile::tempfile().context("create bounded command stdout")?;
    let mut stderr = tempfile::tempfile().context("create bounded command stderr")?;
    command
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?));
    let child = command.spawn().with_context(|| operation.to_owned())?;
    let status = ReapedChild::new(child).wait_cancelled(cancelled, operation)?;
    let read_bounded = |file: &mut File| -> Result<Vec<u8>> {
        if file.metadata()?.len() > maximum {
            bail!("{operation} output exceeds limit")
        }
        file.rewind()?;
        let mut bytes = Vec::new();
        file.take(maximum.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > maximum {
            bail!("{operation} output exceeds limit")
        }
        Ok(bytes)
    };
    Ok(Output {
        status,
        stdout: read_bounded(&mut stdout)?,
        stderr: read_bounded(&mut stderr)?,
    })
}
fn text(output: &Output) -> Result<String> {
    if !output.status.success() {
        bail!(
            "isolated git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    Ok(String::from_utf8(output.stdout.clone())
        .context("Git output is not UTF-8")?
        .trim()
        .to_owned())
}
#[allow(dead_code)] // reached through the intentionally not-yet-wired provider constructor
fn detect_object_format(repo: &Path) -> Result<GitObjectFormat> {
    match text(&safe_git(repo, &["rev-parse", "--show-object-format"])?)?.as_str() {
        "sha1" => Ok(GitObjectFormat::Sha1),
        "sha256" => Ok(GitObjectFormat::Sha256),
        other => bail!("unsupported Git object format {other}"),
    }
}
fn validate_identity(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > 1024
        || value.chars().any(char::is_control)
        || (field == "repo" && (value.contains("://") || value.starts_with("git@")))
    {
        bail!("invalid Git source {field}")
    }
    Ok(())
}
fn validate_oid(oid: &str, format: GitObjectFormat) -> Result<()> {
    Cas::validate_object_id(oid)?;
    if oid.len() != format.oid_len() {
        bail!("object id does not match Git object format")
    }
    Ok(())
}
fn validate_blob(blob: &CasBlob, maximum: u64) -> Result<()> {
    Cas::validate_artifact_id(&blob.hash)?;
    if blob.len == 0 || blob.len > maximum {
        bail!("Git source CAS child length is out of bounds")
    }
    Ok(())
}
fn verify_bytes(blob: &CasBlob, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != blob.len || hex::encode(Sha256::digest(bytes)) != blob.hash {
        bail!("Git source CAS bytes do not match descriptor")
    }
    Ok(())
}
fn copy_verified_create_new(
    source: &Path,
    destination: &Path,
    blob: &CasBlob,
    cancelled: &CancellationToken,
) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source).context("stat Git source CAS child")?;
    if !metadata.file_type().is_file() {
        bail!("Git source CAS child is not a regular file")
    }
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut h = Sha256::new();
    let mut len = 0u64;
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        check_cancelled(cancelled)?;
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        len += n as u64;
        if len > blob.len {
            bail!("Git source CAS child exceeds declared length")
        }
        h.update(&buffer[..n]);
        output.write_all(&buffer[..n])?;
    }
    output.sync_all()?;
    if len != blob.len || hex::encode(h.finalize()) != blob.hash {
        bail!("Git source CAS child does not match descriptor")
    }
    Ok(())
}
fn verify_file_at(path: &Path, blob: &CasBlob, cancelled: &CancellationToken) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("stat materialized Git source child {}", path.display()))?;
    if !metadata.file_type().is_file() || metadata.len() != blob.len {
        bail!("materialized Git source child is not the declared regular file")
    }
    let mut input = File::open(path)?;
    let mut h = Sha256::new();
    let mut len = 0u64;
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        check_cancelled(cancelled)?;
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        len = len
            .checked_add(n as u64)
            .context("Git source length overflow")?;
        if len > blob.len {
            bail!("materialized Git source child exceeds declared length")
        }
        h.update(&buffer[..n]);
    }
    if len != blob.len || hex::encode(h.finalize()) != blob.hash {
        bail!("materialized Git source child does not match descriptor")
    }
    Ok(())
}
fn reject_symlink_dir(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("stat trusted directory {}", path.display()))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        bail!("trusted directory must be a real directory")
    }
    Ok(())
}
fn check_cancelled(cancelled: &CancellationToken) -> Result<()> {
    if cancelled.is_cancelled() {
        bail!("Git source operation cancelled")
    }
    Ok(())
}

fn bind_child_to_scratch(command: &mut Command) {
    #[cfg(unix)]
    SCRATCH_CHILD_FD.with(|slot| {
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
    CString::new(value.as_bytes()).context("Git source path contains NUL")
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
        return Err(std::io::Error::last_os_error()).context("open Git source scratch child");
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn fd_stat(fd: libc::c_int) -> Result<libc::stat> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("stat Git source scratch handle");
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
        return Err(std::io::Error::last_os_error()).context("stat Git source scratch entry");
    }
    Ok(unsafe { stat.assume_init() })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn directory_entries(fd: libc::c_int) -> Result<Vec<std::ffi::OsString>> {
    let duplicate = unsafe { libc::dup(fd) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error()).context("duplicate scratch directory handle");
    }
    let directory = unsafe { libc::fdopendir(duplicate) };
    if directory.is_null() {
        unsafe { libc::close(duplicate) };
        return Err(std::io::Error::last_os_error()).context("open scratch directory stream");
    }
    let mut entries = Vec::new();
    loop {
        clear_errno();
        let entry = unsafe { libc::readdir(directory) };
        if entry.is_null() {
            let error = std::io::Error::last_os_error();
            unsafe { libc::closedir(directory) };
            if error.raw_os_error() != Some(0) {
                return Err(error).context("read scratch directory stream");
            }
            break;
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if !matches!(name, b"." | b"..") {
            entries.push(std::ffi::OsString::from_vec(name.to_vec()));
        }
    }
    Ok(entries)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn remove_dir_contents(fd: libc::c_int) -> Result<()> {
    for name in directory_entries(fd)? {
        let stat = match entry_stat(fd, &name) {
            Ok(stat) => stat,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        let name_c = cstring(&name)?;
        if stat.st_mode & libc::S_IFMT == libc::S_IFDIR {
            let child = openat_dir(fd, &name)?;
            remove_dir_contents(child.as_raw_fd())?;
            if unsafe { libc::unlinkat(fd, name_c.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("remove Git source scratch directory");
            }
        } else if unsafe { libc::unlinkat(fd, name_c.as_ptr(), 0) } != 0 {
            return Err(std::io::Error::last_os_error()).context("remove Git source scratch entry");
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn clear_errno() {
    unsafe { *libc::__errno_location() = 0 };
}

#[cfg(target_os = "macos")]
fn clear_errno() {
    unsafe { *libc::__error() = 0 };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct MemoryStore {
        objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        writes: Arc<Mutex<Vec<String>>>,
        fail_file_after: Arc<Mutex<Option<usize>>>,
    }

    impl GitSourceUploader for MemoryStore {
        fn put_file(&self, blob: &CasBlob, source: &Path) -> Result<()> {
            let mut remaining = self.fail_file_after.lock().unwrap();
            if let Some(value) = remaining.as_mut() {
                if *value == 0 {
                    bail!("injected child upload failure")
                }
                *value -= 1;
            }
            drop(remaining);
            let bytes = std::fs::read(source)?;
            verify_bytes(blob, &bytes)?;
            self.objects
                .lock()
                .unwrap()
                .insert(blob.hash.clone(), bytes);
            self.writes
                .lock()
                .unwrap()
                .push(format!("child:{}", blob.hash));
            Ok(())
        }

        fn put_bytes(&self, blob: &CasBlob, bytes: &[u8]) -> Result<()> {
            verify_bytes(blob, bytes)?;
            self.objects
                .lock()
                .unwrap()
                .insert(blob.hash.clone(), bytes.to_vec());
            self.writes
                .lock()
                .unwrap()
                .push(format!("root:{}", blob.hash));
            Ok(())
        }
    }

    impl GitSourceLoader for MemoryStore {
        fn load_file(
            &self,
            blob: &CasBlob,
            destination: &Path,
            cancelled: &CancellationToken,
        ) -> Result<()> {
            check_cancelled(cancelled)?;
            let bytes = self
                .objects
                .lock()
                .unwrap()
                .get(&blob.hash)
                .cloned()
                .context("missing memory CAS child")?;
            verify_bytes(blob, &bytes)?;
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)?;
            output.write_all(&bytes)?;
            output.sync_all()?;
            Ok(())
        }

        fn load_bytes(
            &self,
            blob: &CasBlob,
            maximum: u64,
            cancelled: &CancellationToken,
        ) -> Result<Vec<u8>> {
            check_cancelled(cancelled)?;
            validate_blob(blob, maximum)?;
            let bytes = self
                .objects
                .lock()
                .unwrap()
                .get(&blob.hash)
                .cloned()
                .context("missing memory CAS root")?;
            verify_bytes(blob, &bytes)?;
            Ok(bytes)
        }
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        text(&safe_git(repo, args).unwrap()).unwrap()
    }
    fn fixture(format: GitObjectFormat) -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        init_bare(tmp.path(), format, &CancellationToken::new()).unwrap();
        let tree = git(tmp.path(), &["mktree"]);
        let mut command = safe_command();
        command.arg("-C").arg(tmp.path()).args([
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit-tree",
            &tree,
        ]);
        let out = command.output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        (tmp, text(&out).unwrap())
    }

    fn commit_worktree(repo: &Path, message: &str) {
        safe_git_ok(
            repo,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "-m",
                message,
            ],
        )
        .unwrap();
    }

    fn complex_fixture() -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        let out = safe_command()
            .args(["init", "-b", "main"])
            .arg(tmp.path())
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::fs::write(tmp.path().join("unicodé-文件.txt"), "hello \u{1f30d}\n").unwrap();
        std::fs::write(tmp.path().join("run.sh"), "#!/bin/sh\necho safe\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                tmp.path().join("run.sh"),
                std::fs::Permissions::from_mode(0o755),
            )
            .unwrap();
        }
        std::fs::write(tmp.path().join("large.bin"), vec![0x5a; 1024 * 1024 + 17]).unwrap();
        safe_git_ok(tmp.path(), &["add", "--all"]).unwrap();
        commit_worktree(tmp.path(), "base");
        safe_git_ok(tmp.path(), &["checkout", "-b", "feature"]).unwrap();
        std::fs::write(tmp.path().join("feature.txt"), "feature\n").unwrap();
        safe_git_ok(tmp.path(), &["add", "feature.txt"]).unwrap();
        commit_worktree(tmp.path(), "feature");
        safe_git_ok(tmp.path(), &["checkout", "main"]).unwrap();
        std::fs::write(tmp.path().join("main.txt"), "main\n").unwrap();
        safe_git_ok(tmp.path(), &["add", "main.txt"]).unwrap();
        commit_worktree(tmp.path(), "main");
        safe_git_ok(
            tmp.path(),
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "merge",
                "--no-ff",
                "-m",
                "merge",
                "feature",
            ],
        )
        .unwrap();
        safe_git_ok(
            tmp.path(),
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "--allow-empty",
                "-m",
                "empty",
            ],
        )
        .unwrap();
        safe_git_ok(
            tmp.path(),
            &[
                "remote",
                "add",
                "origin",
                "https://token@example.invalid/repo.git",
            ],
        )
        .unwrap();
        safe_git_ok(
            tmp.path(),
            &["config", "core.hooksPath", ".git/untrusted-hooks"],
        )
        .unwrap();
        std::fs::create_dir_all(tmp.path().join(".git/untrusted-hooks")).unwrap();
        std::fs::write(
            tmp.path().join(".git/untrusted-hooks/post-checkout"),
            "exit 99\n",
        )
        .unwrap();
        let commit = git(tmp.path(), &["rev-parse", "HEAD"]);
        (tmp, commit)
    }
    fn roundtrip(
        format: GitObjectFormat,
    ) -> (tempfile::TempDir, tempfile::TempDir, PreparedGitSource) {
        let (repo, commit) = fixture(format);
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let store = CasGitSourceStore::new(&cas);
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "ws".into(),
            "repo".into(),
            commit,
        )
        .unwrap();
        let prepared =
            GitSourcePackager::new(&cas, &store, scratch.path(), GitSourceLimits::default())
                .prepare(fetch, &CancellationToken::new())
                .unwrap();
        let auth = AuthenticatedGitSource::from_registry_record(
            prepared.root.clone(),
            prepared.manifest.workspace.clone(),
            prepared.manifest.repo.clone(),
            prepared.manifest.commit.clone(),
            prepared.manifest.object_format,
        )
        .unwrap();
        let materialized =
            GitSourceMaterializer::new(&store, scratch.path(), GitSourceLimits::default())
                .materialize(&auth, &CancellationToken::new())
                .unwrap();
        assert_eq!(
            git(materialized.path(), &["rev-parse", "HEAD"]),
            prepared.manifest.commit
        );
        (repo, cas_dir, prepared)
    }

    #[test]
    fn sha1_empty_tree_roundtrip() {
        roundtrip(GitObjectFormat::Sha1);
    }
    #[test]
    fn sha256_empty_tree_roundtrip_when_supported() {
        let probe = tempfile::tempdir().unwrap();
        if safe_command()
            .args(["init", "--bare", "--object-format=sha256"])
            .arg(probe.path())
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            roundtrip(GitObjectFormat::Sha256);
        }
    }
    #[test]
    fn unknown_fields_and_noncanonical_json_are_rejected() {
        let (_, _, prepared) = roundtrip(GitObjectFormat::Sha1);
        let mut value = serde_json::to_value(&prepared.manifest).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("forged".into(), serde_json::json!(true));
        assert!(
            GitSourceManifest::decode_canonical(
                &serde_json::to_vec(&value).unwrap(),
                &GitSourceLimits::default()
            )
            .is_err()
        );
        let pretty = serde_json::to_vec_pretty(&prepared.manifest).unwrap();
        assert!(GitSourceManifest::decode_canonical(&pretty, &GitSourceLimits::default()).is_err());
    }
    #[test]
    fn cancellation_and_limits_fail_closed() {
        let (repo, commit) = fixture(GitObjectFormat::Sha1);
        let cas_dir = tempfile::tempdir().unwrap();
        let cas = Cas::new(cas_dir.path()).unwrap();
        let store = CasGitSourceStore::new(&cas);
        let scratch = tempfile::tempdir().unwrap();
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "ws".into(),
            "repo".into(),
            commit.clone(),
        )
        .unwrap();
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(
            GitSourcePackager::new(&cas, &store, scratch.path(), GitSourceLimits::default())
                .prepare(fetch, &cancelled)
                .is_err()
        );
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "ws".into(),
            "repo".into(),
            commit,
        )
        .unwrap();
        let limits = GitSourceLimits {
            max_objects: 1,
            ..GitSourceLimits::default()
        };
        assert!(
            GitSourcePackager::new(&cas, &store, scratch.path(), limits)
                .prepare(fetch, &CancellationToken::new())
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn bounded_child_streams_cancel_reject_and_reap_every_exit() {
        fn run(output: &str, sleep: bool, cancelled: bool, reject: bool) -> String {
            let root = tempfile::tempdir().unwrap();
            let pid_path = root.path().join("pid");
            let tail = if sleep { "; sleep 30" } else { "" };
            let script = format!(
                "echo $$ > '{}'; printf '%s' '{}'{}",
                pid_path.display(),
                output,
                tail
            );
            let mut command = Command::new("sh");
            command
                .args(["-c", &script])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            configure_process_group(&mut command);
            let child = command.spawn().unwrap();
            let token = CancellationToken::new();
            let canceller = cancelled.then(|| {
                let token = token.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(50));
                    token.cancel();
                })
            });
            let mut count = if reject { u64::MAX } else { 0 };
            let error = crate::git::consume_child_lines_cancellable(child, &token, |line| {
                if reject && line == "bad" {
                    bail!("malformed child record")
                }
                if line == "io" {
                    return Err(std::io::Error::other("injected child consumer IO failure").into());
                }
                count = count
                    .checked_add(1)
                    .context("child record count overflow")?;
                Ok(())
            })
            .unwrap_err();
            if let Some(canceller) = canceller {
                canceller.join().unwrap();
            }
            let pid: i32 = std::fs::read_to_string(pid_path)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            assert_ne!(unsafe { libc::kill(pid, 0) }, 0, "child was not reaped");
            format!("{error:#}")
        }

        assert!(run("partial", true, true, false).contains("cancelled"));
        assert!(run("", true, true, false).contains("cancelled"));
        assert!(run("bad\n", false, false, true).contains("malformed"));
        assert!(run("io\n", false, false, false).contains("IO failure"));
        assert!(run("ok\n", false, false, true).contains("overflow"));
    }

    #[test]
    fn root_stream_cancellation_is_prompt_and_hash_never_returns() {
        let root = tempfile::tempdir().unwrap();
        let cas = Cas::new(root.path()).unwrap();
        let bytes = vec![0x5a; 64 * 1024 * 1024];
        let hash = cas.put(&bytes).unwrap();
        let blob = CasBlob {
            hash,
            len: bytes.len() as u64,
        };
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let worker_cas = cas.clone();
        let worker = std::thread::spawn(move || {
            CasGitSourceStore::new(&worker_cas).load_bytes(&blob, 128 * 1024 * 1024, &worker_token)
        });
        std::thread::sleep(Duration::from_millis(10));
        let started = std::time::Instant::now();
        token.cancel();
        assert!(worker.join().unwrap().is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
    }
    #[test]
    fn manifest_forgery_duplicate_and_swapped_children_are_rejected() {
        let (_, _, prepared) = roundtrip(GitObjectFormat::Sha1);
        let mut forged = prepared.manifest.clone();
        forged.workspace = "other".into();
        assert!(forged.validate(&GitSourceLimits::default()).is_err());
        let mut duplicate = prepared.manifest.clone();
        duplicate.packs.push(duplicate.packs[0].clone());
        duplicate.aggregate_pack_bytes *= 2;
        duplicate.semantic_digest = duplicate.compute_semantic_digest().unwrap();
        assert!(duplicate.validate(&GitSourceLimits::default()).is_err());
        let mut swapped = prepared.manifest.clone();
        let pair = &mut swapped.packs[0];
        std::mem::swap(&mut pair.pack, &mut pair.index);
        swapped.semantic_digest = swapped.compute_semantic_digest().unwrap();
        // Envelope is structurally valid; pack-pair verification is the semantic gate.
        assert!(swapped.validate(&GitSourceLimits::default()).is_ok());
    }
    #[cfg(unix)]
    #[test]
    fn symlinked_trusted_staging_is_rejected() {
        use std::os::unix::fs::symlink;
        let real = tempfile::tempdir().unwrap();
        let parent = tempfile::tempdir().unwrap();
        let link = parent.path().join("link");
        symlink(real.path(), &link).unwrap();
        assert!(reject_symlink_dir(&link).is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn bound_scratch_survives_double_swap_and_preserves_replacement() {
        let root = tempfile::tempdir().unwrap();
        let configured = root.path().join("scratch");
        std::fs::create_dir(&configured).unwrap();
        let attempt = BoundScratch::new(&configured, "swap").unwrap();
        let name = attempt.name.clone();
        let scope = attempt.enter().unwrap();
        let moved = root.path().join("moved");
        std::fs::rename(&configured, &moved).unwrap();
        std::fs::create_dir(&configured).unwrap();
        let replacement_attempt = configured.join(&name);
        std::fs::create_dir(&replacement_attempt).unwrap();
        std::fs::write(replacement_attempt.join("sentinel"), b"replacement").unwrap();
        std::fs::create_dir(attempt.path().join("repo")).unwrap();
        std::fs::write(attempt.path().join("repo/original"), b"bound").unwrap();
        let moved_again = root.path().join("moved-again");
        std::fs::rename(&moved, &moved_again).unwrap();
        std::fs::create_dir(&moved).unwrap();
        std::fs::write(attempt.path().join("repo/after-swap"), b"bound").unwrap();
        assert!(moved_again.join(&name).join("repo/original").is_file());
        assert!(moved_again.join(&name).join("repo/after-swap").is_file());
        drop(scope);
        drop(attempt);
        assert!(!moved_again.join(&name).exists());
        assert_eq!(
            std::fs::read(replacement_attempt.join("sentinel")).unwrap(),
            b"replacement"
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn bound_scratch_is_concurrent_and_restores_after_panic() {
        let root = tempfile::tempdir().unwrap();
        let one_root = root.path().join("one");
        let two_root = root.path().join("two");
        std::fs::create_dir(&one_root).unwrap();
        std::fs::create_dir(&two_root).unwrap();
        let one = BoundScratch::new(&one_root, "one").unwrap();
        let two = BoundScratch::new(&two_root, "two").unwrap();
        let one_path = one_root.join(&one.name);
        let two_path = two_root.join(&two.name);
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let spawn =
            |attempt: BoundScratch, barrier: Arc<std::sync::Barrier>, bytes: &'static [u8]| {
                std::thread::spawn(move || {
                    let before = std::env::current_dir().unwrap();
                    let scope = attempt.enter().unwrap();
                    barrier.wait();
                    std::fs::write(attempt.path().join("value"), bytes).unwrap();
                    let mut child = Command::new("sh");
                    child.args(["-c", "pwd > child-cwd"]);
                    bind_child_to_scratch(&mut child);
                    assert!(child.status().unwrap().success());
                    barrier.wait();
                    drop(scope);
                    assert_eq!(std::env::current_dir().unwrap(), before);
                    attempt
                })
            };
        let one_thread = spawn(one, barrier.clone(), b"one");
        let two_thread = spawn(two, barrier.clone(), b"two");
        let global = std::env::current_dir().unwrap();
        barrier.wait();
        assert_eq!(std::env::current_dir().unwrap(), global);
        barrier.wait();
        let one = one_thread.join().unwrap();
        let two = two_thread.join().unwrap();
        assert_eq!(std::fs::read(one_path.join("value")).unwrap(), b"one");
        assert_eq!(std::fs::read(two_path.join("value")).unwrap(), b"two");
        drop(one);
        drop(two);

        let panic_root = root.path().join("panic");
        std::fs::create_dir(&panic_root).unwrap();
        let attempt = BoundScratch::new(&panic_root, "panic").unwrap();
        let before = std::env::current_dir().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _scope = attempt.enter().unwrap();
                panic!("scratch panic");
            }))
            .is_err()
        );
        assert_eq!(std::env::current_dir().unwrap(), before);
    }

    #[test]
    fn remote_store_materializes_without_builder_cas_and_root_is_last() {
        let (repo, commit) = fixture(GitObjectFormat::Sha1);
        let local_dir = tempfile::tempdir().unwrap();
        let local = Cas::new(local_dir.path()).unwrap();
        let remote = MemoryStore::default();
        let scratch = tempfile::tempdir().unwrap();
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "workspace".into(),
            "repository".into(),
            commit,
        )
        .unwrap();
        let prepared =
            GitSourcePackager::new(&local, &remote, scratch.path(), GitSourceLimits::default())
                .prepare(fetch, &CancellationToken::new())
                .unwrap();
        let writes = remote.writes.lock().unwrap().clone();
        assert!(writes.last().unwrap().starts_with("root:"));
        assert!(
            writes[..writes.len() - 1]
                .iter()
                .all(|write| write.starts_with("child:"))
        );
        drop(local);
        drop(local_dir);
        let authority = AuthenticatedGitSource::from_registry_record(
            prepared.root.clone(),
            prepared.manifest.workspace.clone(),
            prepared.manifest.repo.clone(),
            prepared.manifest.commit.clone(),
            prepared.manifest.object_format,
        )
        .unwrap();
        let worker_scratch = tempfile::tempdir().unwrap();
        let materialized =
            GitSourceMaterializer::new(&remote, worker_scratch.path(), GitSourceLimits::default())
                .materialize(&authority, &CancellationToken::new())
                .unwrap();
        assert_eq!(
            git(materialized.path(), &["rev-parse", "HEAD"]),
            prepared.manifest.commit
        );
        assert!(
            !materialized
                .path()
                .join("hooks")
                .join("post-checkout")
                .exists()
        );
        assert!(!materialized.path().join("objects/info/alternates").exists());
        assert!(!materialized.path().join("shallow").exists());
    }

    #[test]
    fn failed_child_upload_never_publishes_root() {
        let (repo, commit) = fixture(GitObjectFormat::Sha1);
        let local_dir = tempfile::tempdir().unwrap();
        let local = Cas::new(local_dir.path()).unwrap();
        let remote = MemoryStore::default();
        *remote.fail_file_after.lock().unwrap() = Some(0);
        let scratch = tempfile::tempdir().unwrap();
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "workspace".into(),
            "repository".into(),
            commit,
        )
        .unwrap();
        assert!(
            GitSourcePackager::new(&local, &remote, scratch.path(), GitSourceLimits::default())
                .prepare(fetch, &CancellationToken::new())
                .is_err()
        );
        assert!(
            remote
                .writes
                .lock()
                .unwrap()
                .iter()
                .all(|write| !write.starts_with("root:"))
        );
    }

    #[test]
    fn missing_or_corrupt_remote_child_fails_closed() {
        let (repo, commit) = fixture(GitObjectFormat::Sha1);
        let local_dir = tempfile::tempdir().unwrap();
        let local = Cas::new(local_dir.path()).unwrap();
        let remote = MemoryStore::default();
        let scratch = tempfile::tempdir().unwrap();
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "workspace".into(),
            "repository".into(),
            commit,
        )
        .unwrap();
        let prepared =
            GitSourcePackager::new(&local, &remote, scratch.path(), GitSourceLimits::default())
                .prepare(fetch, &CancellationToken::new())
                .unwrap();
        let authority = AuthenticatedGitSource::from_registry_record(
            prepared.root.clone(),
            prepared.manifest.workspace.clone(),
            prepared.manifest.repo.clone(),
            prepared.manifest.commit.clone(),
            prepared.manifest.object_format,
        )
        .unwrap();
        let child = prepared.manifest.packs[0].pack.hash.clone();
        remote.objects.lock().unwrap().remove(&child);
        assert!(
            GitSourceMaterializer::new(&remote, scratch.path(), GitSourceLimits::default())
                .materialize(&authority, &CancellationToken::new())
                .is_err()
        );
        remote
            .objects
            .lock()
            .unwrap()
            .insert(child, b"forged pack".to_vec());
        assert!(
            GitSourceMaterializer::new(&remote, scratch.path(), GitSourceLimits::default())
                .materialize(&authority, &CancellationToken::new())
                .is_err()
        );
    }

    #[test]
    fn swapped_pack_index_and_registry_identity_forgery_fail_closed() {
        let (repo, commit) = fixture(GitObjectFormat::Sha1);
        let local_dir = tempfile::tempdir().unwrap();
        let local = Cas::new(local_dir.path()).unwrap();
        let remote = MemoryStore::default();
        let scratch = tempfile::tempdir().unwrap();
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "workspace".into(),
            "repository".into(),
            commit,
        )
        .unwrap();
        let prepared =
            GitSourcePackager::new(&local, &remote, scratch.path(), GitSourceLimits::default())
                .prepare(fetch, &CancellationToken::new())
                .unwrap();
        let wrong_identity = AuthenticatedGitSource::from_registry_record(
            prepared.root.clone(),
            "another-workspace".into(),
            prepared.manifest.repo.clone(),
            prepared.manifest.commit.clone(),
            prepared.manifest.object_format,
        )
        .unwrap();
        assert!(
            GitSourceMaterializer::new(&remote, scratch.path(), GitSourceLimits::default())
                .materialize(&wrong_identity, &CancellationToken::new())
                .is_err()
        );

        let mut swapped = prepared.manifest.clone();
        let pair = &mut swapped.packs[0];
        std::mem::swap(&mut pair.pack, &mut pair.index);
        swapped.semantic_digest = swapped.compute_semantic_digest().unwrap();
        let bytes = swapped.canonical_bytes().unwrap();
        let root = CasBlob {
            hash: hex::encode(Sha256::digest(&bytes)),
            len: bytes.len() as u64,
        };
        remote.put_bytes(&root, &bytes).unwrap();
        let swapped_authority = AuthenticatedGitSource::from_registry_record(
            root,
            swapped.workspace.clone(),
            swapped.repo.clone(),
            swapped.commit.clone(),
            swapped.object_format,
        )
        .unwrap();
        assert!(
            GitSourceMaterializer::new(&remote, scratch.path(), GitSourceLimits::default())
                .materialize(&swapped_authority, &CancellationToken::new())
                .is_err()
        );
    }

    #[test]
    fn incomplete_multi_pack_root_cannot_hide_a_missing_base() {
        let (repo, commit) = complex_fixture();
        let local_dir = tempfile::tempdir().unwrap();
        let local = Cas::new(local_dir.path()).unwrap();
        let remote = MemoryStore::default();
        let scratch = tempfile::tempdir().unwrap();
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "workspace".into(),
            "repository".into(),
            commit,
        )
        .unwrap();
        let limits = GitSourceLimits {
            target_pack_raw_bytes: 1,
            ..GitSourceLimits::default()
        };
        let prepared = GitSourcePackager::new(&local, &remote, scratch.path(), limits.clone())
            .prepare(fetch, &CancellationToken::new())
            .unwrap();
        assert!(prepared.manifest.packs.len() > 1);
        let mut incomplete = prepared.manifest.clone();
        let omitted = incomplete.packs.remove(0);
        incomplete.aggregate_pack_bytes -= omitted.pack.len + omitted.index.len;
        incomplete.semantic_digest = incomplete.compute_semantic_digest().unwrap();
        let bytes = incomplete.canonical_bytes().unwrap();
        let root = CasBlob {
            hash: hex::encode(Sha256::digest(&bytes)),
            len: bytes.len() as u64,
        };
        remote.put_bytes(&root, &bytes).unwrap();
        let authority = AuthenticatedGitSource::from_registry_record(
            root,
            incomplete.workspace.clone(),
            incomplete.repo.clone(),
            incomplete.commit.clone(),
            incomplete.object_format,
        )
        .unwrap();
        assert!(
            GitSourceMaterializer::new(&remote, scratch.path(), limits)
                .materialize(&authority, &CancellationToken::new())
                .is_err()
        );
    }

    #[test]
    fn cross_pack_thin_delta_is_rejected_while_canonical_pairs_pass() {
        let (repo, base) = complex_fixture();
        let mut changed = vec![0x5a; 1024 * 1024 + 17];
        changed[500_000..500_128].fill(0x33);
        std::fs::write(repo.path().join("large.bin"), changed).unwrap();
        safe_git_ok(repo.path(), &["add", "large.bin"]).unwrap();
        commit_worktree(repo.path(), "delta-target");
        let target = git(repo.path(), &["rev-parse", "HEAD"]);
        let local_dir = tempfile::tempdir().unwrap();
        let local = Cas::new(local_dir.path()).unwrap();
        let remote = MemoryStore::default();
        let scratch = tempfile::tempdir().unwrap();

        let canonical_fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "workspace".into(),
            "repository".into(),
            target.clone(),
        )
        .unwrap();
        let canonical =
            GitSourcePackager::new(&local, &remote, scratch.path(), GitSourceLimits::default())
                .prepare(canonical_fetch, &CancellationToken::new())
                .unwrap();
        let canonical_authority = AuthenticatedGitSource::from_registry_record(
            canonical.root.clone(),
            "workspace".into(),
            "repository".into(),
            target.clone(),
            GitObjectFormat::Sha1,
        )
        .unwrap();
        let canonical_worker = tempfile::tempdir().unwrap();
        GitSourceMaterializer::new(&remote, canonical_worker.path(), GitSourceLimits::default())
            .materialize(&canonical_authority, &CancellationToken::new())
            .unwrap();

        let base_fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "workspace".into(),
            "repository".into(),
            base.clone(),
        )
        .unwrap();
        let base_source =
            GitSourcePackager::new(&local, &remote, scratch.path(), GitSourceLimits::default())
                .prepare(base_fetch, &CancellationToken::new())
                .unwrap();

        let thin_dir = tempfile::tempdir().unwrap();
        let thin_pack = thin_dir.path().join("thin.pack");
        let thin_output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&thin_pack)
            .unwrap();
        let mut command = safe_command();
        command
            .arg("-C")
            .arg(repo.path())
            .args(["pack-objects", "--thin", "--stdout", "--revs"])
            .stdin(Stdio::piped())
            .stdout(Stdio::from(thin_output))
            .stderr(Stdio::null());
        let mut child = command.spawn().unwrap();
        {
            let mut stdin = child.stdin.take().unwrap();
            writeln!(stdin, "{target}").unwrap();
            writeln!(stdin, "^{base}").unwrap();
        }
        assert!(child.wait().unwrap().success());
        let thin_index = thin_dir.path().join("thin.idx");
        let mut index_command = safe_command();
        index_command
            .arg("-C")
            .arg(repo.path())
            .args([
                "index-pack",
                "--stdin",
                "--fix-thin",
                "-o",
                thin_index.to_str().unwrap(),
            ])
            .stdin(Stdio::from(File::open(&thin_pack).unwrap()));
        let output = run_output_bounded_cancelled(
            index_command,
            &CancellationToken::new(),
            1024 * 1024,
            "index thin fixture",
        )
        .unwrap();
        assert!(
            output.status.success(),
            "test Git cannot index the cross-pack thin fixture: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let thin_pack_bytes = std::fs::read(&thin_pack).unwrap();
        let thin_index_bytes = std::fs::read(&thin_index).unwrap();
        let thin_pair = GitPackPair {
            pack: CasBlob {
                hash: hex::encode(Sha256::digest(&thin_pack_bytes)),
                len: thin_pack_bytes.len() as u64,
            },
            index: CasBlob {
                hash: hex::encode(Sha256::digest(&thin_index_bytes)),
                len: thin_index_bytes.len() as u64,
            },
        };
        remote.put_bytes(&thin_pair.pack, &thin_pack_bytes).unwrap();
        remote
            .put_bytes(&thin_pair.index, &thin_index_bytes)
            .unwrap();
        let inventory_scratch = tempfile::tempdir().unwrap();
        let inventory = enumerate_closure(
            repo.path(),
            &target,
            GitObjectFormat::Sha1,
            &GitSourceLimits::default(),
            &CancellationToken::new(),
            inventory_scratch.path(),
        )
        .unwrap();
        let mut manifest = GitSourceManifest {
            schema_version: GIT_SOURCE_SCHEMA,
            source_format_version: GIT_SOURCE_FORMAT,
            layout: GitSourceLayout::ColdComplete,
            workspace: "workspace".into(),
            repo: "repository".into(),
            commit: target.clone(),
            object_format: GitObjectFormat::Sha1,
            packs: base_source.manifest.packs.clone(),
            object_count: inventory.count,
            object_set_digest: inventory.digest.clone(),
            aggregate_pack_bytes: 0,
            semantic_digest: String::new(),
        };
        manifest.packs.push(thin_pair);
        manifest.aggregate_pack_bytes = manifest
            .packs
            .iter()
            .map(|pair| pair.pack.len + pair.index.len)
            .sum();
        manifest.semantic_digest = manifest.compute_semantic_digest().unwrap();
        let root_bytes = manifest.canonical_bytes().unwrap();
        let root = CasBlob {
            hash: hex::encode(Sha256::digest(&root_bytes)),
            len: root_bytes.len() as u64,
        };
        remote.put_bytes(&root, &root_bytes).unwrap();
        let authority = AuthenticatedGitSource::from_registry_record(
            root,
            "workspace".into(),
            "repository".into(),
            target,
            GitObjectFormat::Sha1,
        )
        .unwrap();
        let worker = tempfile::tempdir().unwrap();
        assert!(
            GitSourceMaterializer::new(&remote, worker.path(), GitSourceLimits::default())
                .materialize(&authority, &CancellationToken::new())
                .is_err()
        );
    }

    #[test]
    fn complex_history_roundtrips_without_provider_control_metadata() {
        let (repo, commit) = complex_fixture();
        let local_dir = tempfile::tempdir().unwrap();
        let local = Cas::new(local_dir.path()).unwrap();
        let remote = MemoryStore::default();
        let scratch = tempfile::tempdir().unwrap();
        let fetch = TrustedProviderFetch::from_pinned_fetch(
            repo.path().to_owned(),
            "workspace".into(),
            "repository".into(),
            commit,
        )
        .unwrap();
        let prepared =
            GitSourcePackager::new(&local, &remote, scratch.path(), GitSourceLimits::default())
                .prepare(fetch, &CancellationToken::new())
                .unwrap();
        let authority = AuthenticatedGitSource::from_registry_record(
            prepared.root.clone(),
            prepared.manifest.workspace.clone(),
            prepared.manifest.repo.clone(),
            prepared.manifest.commit.clone(),
            prepared.manifest.object_format,
        )
        .unwrap();
        let materialized =
            GitSourceMaterializer::new(&remote, scratch.path(), GitSourceLimits::default())
                .materialize(&authority, &CancellationToken::new())
                .unwrap();
        let listing = git(
            materialized.path(),
            &["-c", "core.quotePath=false", "ls-tree", "-r", "HEAD"],
        );
        #[cfg(unix)]
        assert!(listing.contains("100755 blob"));
        assert!(listing.contains("unicodé-文件.txt"));
        assert_eq!(
            git(materialized.path(), &["rev-list", "--parents", "--all"])
                .lines()
                .filter(|line| line.split_whitespace().count() == 3)
                .count(),
            1
        );
        let config = git(materialized.path(), &["config", "--list"]);
        assert!(!config.contains("example.invalid"));
        assert!(!config.contains("untrusted-hooks"));
        assert!(!materialized.path().join("hooks/post-checkout").exists());
    }

    #[test]
    fn concurrent_builds_are_content_deterministic() {
        let (repo, commit) = complex_fixture();
        let repo_path = repo.path().to_owned();
        let build = || {
            let local_dir = tempfile::tempdir().unwrap();
            let local = Cas::new(local_dir.path()).unwrap();
            let remote = MemoryStore::default();
            let scratch = tempfile::tempdir().unwrap();
            let fetch = TrustedProviderFetch::from_pinned_fetch(
                repo_path.clone(),
                "workspace".into(),
                "repository".into(),
                commit.clone(),
            )
            .unwrap();
            GitSourcePackager::new(&local, &remote, scratch.path(), GitSourceLimits::default())
                .prepare(fetch, &CancellationToken::new())
                .unwrap()
                .root
                .hash
        };
        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(build);
            let second = scope.spawn(build);
            (first.join().unwrap(), second.join().unwrap())
        });
        assert_eq!(first, second);
    }
}
