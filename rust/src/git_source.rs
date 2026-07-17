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
use std::os::unix::ffi::OsStrExt;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::DirBuilderExt;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
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
    attempt: OwnedFd,
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
        let opened_root = unsafe { OwnedFd::from_raw_fd(fd) };
        let root = duplicate_cloexec(opened_root.as_raw_fd())?;
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
        Ok(Self { attempt })
    }

    fn path(&self) -> PathBuf {
        #[cfg(target_os = "linux")]
        return capability_path(self.attempt.as_raw_fd());
        #[cfg(target_os = "macos")]
        return PathBuf::from(".");
    }

    fn enter(&self) -> Result<ScratchScope> {
        enter_scratch_fd(self.attempt.as_raw_fd())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn enter_scratch_fd(fd: libc::c_int) -> Result<ScratchScope> {
    let staging = duplicate_cloexec(fd)?;
    #[cfg(target_os = "linux")]
    {
        let previous = SCRATCH_CHILD_FD.with(|slot| slot.replace(Some(staging.as_raw_fd())));
        Ok(ScratchScope {
            staging: Some(staging),
            previous,
            finished: false,
        })
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
        let opened_old = unsafe { OwnedFd::from_raw_fd(old) };
        let old = duplicate_cloexec(opened_old.as_raw_fd())?;
        let result = unsafe { pthread_fchdir_np(staging.as_raw_fd()) };
        if result != 0 {
            return Err(std::io::Error::from_raw_os_error(result))
                .context("bind Git source installer thread cwd");
        }
        let previous = SCRATCH_CHILD_FD.with(|slot| slot.replace(Some(staging.as_raw_fd())));
        Ok(ScratchScope {
            old: Some(old),
            staging: Some(staging),
            previous,
            finished: false,
        })
    }
}

#[cfg(target_os = "linux")]
struct ScratchScope {
    staging: Option<OwnedFd>,
    previous: Option<libc::c_int>,
    finished: bool,
}

#[cfg(target_os = "linux")]
impl ScratchScope {
    fn finish(mut self) -> Result<()> {
        self.finish_inner()
    }

    fn finish_inner(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        SCRATCH_CHILD_FD.with(|slot| slot.set(self.previous));
        self.staging.take();
        self.finished = true;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for ScratchScope {
    fn drop(&mut self) {
        if self.finish_inner().is_err() {
            std::process::abort();
        }
    }
}

#[cfg(target_os = "macos")]
struct ScratchScope {
    old: Option<OwnedFd>,
    staging: Option<OwnedFd>,
    previous: Option<libc::c_int>,
    finished: bool,
}

#[cfg(target_os = "macos")]
impl ScratchScope {
    fn finish(mut self) -> Result<()> {
        self.finish_inner()
    }

    fn finish_inner(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        #[cfg(test)]
        if FAIL_PTHREAD_CWD_RESTORE.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("injected pthread cwd restoration failure")
        }
        let old = self.old.as_ref().context("missing saved pthread cwd")?;
        let result = unsafe { pthread_fchdir_np(old.as_raw_fd()) };
        if result != 0 {
            return Err(std::io::Error::from_raw_os_error(result))
                .context("restore Git source installer thread cwd");
        }
        SCRATCH_CHILD_FD.with(|slot| slot.set(self.previous));
        self.staging.take();
        self.old.take();
        self.finished = true;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
impl Drop for ScratchScope {
    fn drop(&mut self) {
        if self.finish_inner().is_err() {
            std::process::abort();
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
static FAIL_PTHREAD_CWD_RESTORE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

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

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl ScratchScope {
    fn finish(self) -> Result<()> {
        bail!("handle-bound Git source scratch is unsupported on this platform")
    }
}

/// Storage-neutral sink. Implementations must verify the declared SHA-256 and
/// length before returning success.
pub trait GitSourceUploader: Send + Sync {
    fn put_file(&self, blob: &CasBlob, source: &Path, cancelled: &CancellationToken) -> Result<()>;
    fn put_bytes(&self, blob: &CasBlob, bytes: &[u8], cancelled: &CancellationToken) -> Result<()>;
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
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    root: Arc<OwnedFd>,
}
impl CasGitSourceStore {
    pub fn new(cas: &Cas) -> Result<Self> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let canonical = cas
                .root()
                .canonicalize()
                .context("canonicalize Git source CAS root")?;
            let root =
                open_bound_directory(&canonical).context("bind physical Git source CAS root")?;
            Ok(Self {
                root: Arc::new(root),
            })
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = cas;
            bail!("handle-bound Git source CAS is unsupported on this platform")
        }
    }

    fn with_cas<T>(&self, operation: impl FnOnce(&Cas) -> Result<T>) -> Result<T> {
        #[cfg(target_os = "linux")]
        {
            let cas = Cas::new(capability_path(self.root.as_raw_fd()))?;
            operation(&cas)
        }
        #[cfg(target_os = "macos")]
        {
            let scope = enter_scratch_fd(self.root.as_raw_fd())?;
            let result = Cas::new(".").and_then(|cas| operation(&cas));
            scope
                .finish()
                .context("restore cwd after Git source CAS operation")?;
            result
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        bail!("handle-bound Git source CAS is unsupported on this platform")
    }

    fn open_blob(&self, blob: &CasBlob) -> Result<File> {
        self.with_cas(|cas| {
            let path = cas.path(&blob.hash);
            let metadata = std::fs::symlink_metadata(&path)
                .context("stat handle-bound Git source CAS object")?;
            if !metadata.file_type().is_file() || metadata.len() != blob.len {
                bail!("Git source CAS object is not the declared regular file")
            }
            File::open(path).context("open handle-bound Git source CAS object")
        })
    }
}

impl GitSourceUploader for CasGitSourceStore {
    fn put_file(&self, blob: &CasBlob, source: &Path, cancelled: &CancellationToken) -> Result<()> {
        let source = File::open(source).context("open Git source upload capability")?;
        let source = duplicate_cloexec(source.as_raw_fd())?;
        let source_path = capability_file_path(source.as_raw_fd());
        let len = self.with_cas(|cas| {
            cas.put_file_with_hash_cooperative(&blob.hash, &source_path, || {
                cancelled.is_cancelled()
            })
        })?;
        if len != blob.len {
            bail!("uploaded Git source child length mismatch")
        }
        Ok(())
    }
    fn put_bytes(&self, blob: &CasBlob, bytes: &[u8], cancelled: &CancellationToken) -> Result<()> {
        check_cancelled(cancelled)?;
        if bytes.len() as u64 != blob.len {
            bail!("uploaded Git source root length mismatch")
        }
        self.with_cas(|cas| cas.put_with_hash(&blob.hash, bytes))
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
        let input = self.open_blob(blob)?;
        let output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)?;
        copy_verified_open(input, output, blob, cancelled)
    }
    fn load_bytes(
        &self,
        blob: &CasBlob,
        maximum: u64,
        cancelled: &CancellationToken,
    ) -> Result<Vec<u8>> {
        check_cancelled(cancelled)?;
        validate_blob(blob, maximum)?;
        let mut input = self.open_blob(blob)?;
        let capacity: usize = blob
            .len
            .try_into()
            .context("Git source root is too large")?;
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

#[cfg(test)]
pub(crate) fn prepared_source_for_registry_test(
    workspace: &str,
    repo: &str,
    commit: &str,
    pack: CasBlob,
    index: CasBlob,
) -> Result<PreparedGitSource> {
    let object_format = if commit.len() == 40 {
        GitObjectFormat::Sha1
    } else {
        GitObjectFormat::Sha256
    };
    let aggregate_pack_bytes = pack
        .len
        .checked_add(index.len)
        .context("test source aggregate overflow")?;
    let mut manifest = GitSourceManifest {
        schema_version: GIT_SOURCE_SCHEMA,
        source_format_version: GIT_SOURCE_FORMAT,
        layout: GitSourceLayout::ColdComplete,
        workspace: workspace.into(),
        repo: repo.into(),
        commit: commit.into(),
        object_format,
        packs: vec![GitPackPair { pack, index }],
        object_count: 1,
        object_set_digest: "1".repeat(64),
        aggregate_pack_bytes,
        semantic_digest: String::new(),
    };
    manifest.semantic_digest = manifest.compute_semantic_digest()?;
    let bytes = manifest.canonical_bytes()?;
    Ok(PreparedGitSource {
        root: CasBlob {
            hash: hex::encode(Sha256::digest(&bytes)),
            len: bytes.len() as u64,
        },
        manifest,
    })
}
impl PreparedGitSource {
    pub fn root(&self) -> &CasBlob {
        &self.root
    }
    pub fn manifest(&self) -> &GitSourceManifest {
        &self.manifest
    }

    pub(crate) fn registry_view(&self, limits: &GitSourceLimits) -> Result<GitSourceRegistryView> {
        self.manifest.validate(limits)?;
        let root_bytes = self.manifest.canonical_bytes()?;
        let expected = CasBlob {
            hash: hex::encode(Sha256::digest(&root_bytes)),
            len: root_bytes.len() as u64,
        };
        if expected != self.root {
            bail!("prepared Git source root is not the canonical manifest")
        }
        let mut members = Vec::with_capacity(self.manifest.packs.len() * 2);
        for pair in &self.manifest.packs {
            members.push(GitSourceRegistryMember {
                ordinal: members.len() as u32,
                kind: "pack",
                blob: pair.pack.clone(),
            });
            members.push(GitSourceRegistryMember {
                ordinal: members.len() as u32,
                kind: "index",
                blob: pair.index.clone(),
            });
        }
        Ok(GitSourceRegistryView {
            root: self.root.clone(),
            root_bytes,
            workspace: self.manifest.workspace.clone(),
            repo: self.manifest.repo.clone(),
            commit: self.manifest.commit.clone(),
            source_format_version: self.manifest.source_format_version,
            object_format: self.manifest.object_format.git_name(),
            semantic_digest: self.manifest.semantic_digest.clone(),
            object_set_digest: self.manifest.object_set_digest.clone(),
            object_count: self.manifest.object_count,
            total_bytes: self.manifest.aggregate_pack_bytes,
            members,
        })
    }

    pub(crate) fn matches_publication(
        &self,
        workspace: &str,
        repo: &str,
        commit: &str,
        root: &CasBlob,
    ) -> bool {
        self.root == *root
            && self.manifest.workspace == workspace
            && self.manifest.repo == repo
            && self.manifest.commit == commit
    }
}

pub(crate) struct GitSourceRegistryMember {
    pub(crate) ordinal: u32,
    pub(crate) kind: &'static str,
    pub(crate) blob: CasBlob,
}

pub(crate) struct GitSourceRegistryView {
    pub(crate) root: CasBlob,
    pub(crate) root_bytes: Vec<u8>,
    pub(crate) workspace: String,
    pub(crate) repo: String,
    pub(crate) commit: String,
    pub(crate) source_format_version: u32,
    pub(crate) object_format: &'static str,
    pub(crate) semantic_digest: String,
    pub(crate) object_set_digest: String,
    pub(crate) object_count: u64,
    pub(crate) total_bytes: u64,
    pub(crate) members: Vec<GitSourceRegistryMember>,
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
    _registry_evidence_mac: Option<[u8; 32]>,
}

impl AuthenticatedGitSource {
    pub(crate) fn root_hash(&self) -> &str {
        &self.root.hash
    }
    /// Mint the worker capability from an already authenticated durable
    /// registry row. This function does not perform or pretend to perform that
    /// transaction; the future registry adapter owns that responsibility.
    pub(crate) fn from_registry_record(
        record: crate::git_source_registry::GitSourceRegistryRecord,
    ) -> Result<Self> {
        let root = record.root().clone();
        let workspace = record.workspace().to_owned();
        let repo = record.repo().to_owned();
        let commit = record.commit().to_owned();
        let object_format = record.object_format();
        let registry_evidence_mac = Some(*record.evidence_mac());
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
            _registry_evidence_mac: registry_evidence_mac,
        })
    }

    fn from_local_verification(
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
            _registry_evidence_mac: None,
        })
    }
}

pub struct GitSourcePackager<'a, U: GitSourceUploader> {
    local_cas: &'a Cas,
    uploader: &'a U,
    scratch: &'a Path,
    limits: GitSourceLimits,
}

pub(crate) struct OwnedGitSourceUpload<U: GitSourceUploader> {
    local_cas_root: PathBuf,
    uploader: U,
    root: CasBlob,
    root_bytes: Vec<u8>,
    packs: Vec<GitPackPair>,
}

impl<U: GitSourceUploader> OwnedGitSourceUpload<U> {
    pub(crate) fn publish(self, cancelled: &CancellationToken) -> Result<()> {
        let local_cas = Cas::new(self.local_cas_root)?;
        for pair in &self.packs {
            check_cancelled(cancelled)?;
            self.uploader
                .put_file(&pair.pack, &local_cas.path(&pair.pack.hash), cancelled)?;
            self.uploader
                .put_file(&pair.index, &local_cas.path(&pair.index.hash), cancelled)?;
        }
        check_cancelled(cancelled)?;
        self.uploader
            .put_bytes(&self.root, &self.root_bytes, cancelled)
    }
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

    #[cfg(test)]
    pub fn prepare(
        &self,
        source: TrustedProviderFetch,
        cancelled: &CancellationToken,
    ) -> Result<PreparedGitSource> {
        let prepared = self.prepare_local(source, cancelled)?;
        self.publish_unchecked(&prepared, cancelled)?;
        Ok(prepared)
    }

    /// Build and fully verify the immutable source graph in local CAS. No
    /// durable object is written: callers must first register the returned
    /// graph as a provisional GC root, then publish it through the registry.
    pub fn prepare_local(
        &self,
        source: TrustedProviderFetch,
        cancelled: &CancellationToken,
    ) -> Result<PreparedGitSource> {
        self.limits.validate()?;
        let local_cas = Cas::new(
            self.local_cas
                .root()
                .canonicalize()
                .context("canonicalize local Git source CAS")?,
        )?;
        let attempt = BoundScratch::new(self.scratch, "git-source-pack")?;
        let scope = attempt.enter()?;
        let scratch = attempt.path();
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
        let pack_scratch = create_private_subdir(&scratch, "git-source-pack")?;
        let partitions = partition_inventory(
            &source.repo_path,
            &objects,
            &self.limits,
            &pack_scratch,
            cancelled,
        )?;
        let mut built = Vec::with_capacity(partitions.len());
        for (index, partition) in partitions.iter().enumerate() {
            built.push(build_source_pack(
                &source.repo_path,
                partition,
                &local_cas,
                &pack_scratch,
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
        scope.finish()?;
        Ok(PreparedGitSource { root, manifest })
    }

    pub(crate) fn owned_upload_plan(
        &self,
        prepared: &PreparedGitSource,
    ) -> Result<OwnedGitSourceUpload<U>>
    where
        U: Clone,
    {
        let root_bytes = prepared.manifest.canonical_bytes()?;
        let expected = CasBlob {
            hash: hex::encode(Sha256::digest(&root_bytes)),
            len: root_bytes.len() as u64,
        };
        if expected != prepared.root {
            bail!("prepared Git source root is not canonical")
        }
        Ok(OwnedGitSourceUpload {
            local_cas_root: self
                .local_cas
                .root()
                .canonicalize()
                .context("canonicalize local Git source CAS")?,
            uploader: (*self.uploader).clone(),
            root: prepared.root.clone(),
            root_bytes,
            packs: prepared.manifest.packs.clone(),
        })
    }

    #[cfg(test)]
    fn publish_unchecked(
        &self,
        prepared: &PreparedGitSource,
        cancelled: &CancellationToken,
    ) -> Result<()> {
        let local_cas = Cas::new(
            self.local_cas
                .root()
                .canonicalize()
                .context("canonicalize local Git source CAS")?,
        )?;
        let root_bytes = prepared.manifest.canonical_bytes()?;
        let expected_root = CasBlob {
            hash: hex::encode(Sha256::digest(&root_bytes)),
            len: root_bytes.len() as u64,
        };
        if expected_root != prepared.root {
            bail!("prepared Git source root is not canonical")
        }
        for pair in &prepared.manifest.packs {
            check_cancelled(cancelled)?;
            self.uploader
                .put_file(&pair.pack, &local_cas.path(&pair.pack.hash), cancelled)?;
            self.uploader
                .put_file(&pair.index, &local_cas.path(&pair.index.hash), cancelled)?;
        }
        check_cancelled(cancelled)?;
        self.uploader
            .put_bytes(&prepared.root, &root_bytes, cancelled)?;
        Ok(())
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
            let pair_directory = create_private_subdir(&scratch, "git-source-pair")?;
            init_bare(&pair_directory, manifest.object_format, cancelled)?;
            let pair_pack_dir = pair_directory.join("objects/pack");
            let base = format!("pack-source-{i:08}");
            let pack = pair_pack_dir.join(format!("{base}.pack"));
            let index = pair_pack_dir.join(format!("{base}.idx"));
            self.loader.load_file(&pair.pack, &pack, cancelled)?;
            self.loader.load_file(&pair.index, &index, cancelled)?;
            verify_file_at(&pack, &pair.pack, cancelled)?;
            verify_file_at(&index, &pair.index, cancelled)?;
            safe_git_ok_quiet_cancelled(
                &pair_directory,
                &["verify-pack", &format!("objects/pack/{base}.idx")],
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
    let store = CasGitSourceStore::new(cas)?;
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
    let auth = AuthenticatedGitSource::from_local_verification(
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
    let git_dir = text(&safe_git_cancelled(
        repo,
        &["rev-parse", "--absolute-git-dir"],
        cancelled,
    )?)?;
    let mut command = safe_command();
    command
        .arg("--git-dir")
        .arg(child_bound_path(Path::new(&git_dir))?)
        .args(["-c", "core.hooksPath=/dev/null", "-c", "pack.threads=1"])
        .arg("pack-objects")
        .arg(child_bound_path(&prefix)?)
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
        .arg(child_bound_path(repo)?)
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
    let directory = create_private_subdir(scratch, "git-source-inventory")?;
    let unsorted = directory.join("unsorted");
    let sorted = directory.join("sorted");
    let mut output = std::io::BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&unsorted)?,
    );
    let mut command = safe_command();
    command
        .arg("-C")
        .arg(child_bound_path(repo)?)
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
        path: sorted,
        count,
        digest: hex::encode(h.finalize()),
    })
}

fn sort_unique_file(input: &Path, output: &Path, cancelled: &CancellationToken) -> Result<()> {
    let parent = output
        .parent()
        .context("inventory sort output has no parent")?;
    let sort_tmp = create_private_subdir(parent, "sort-tmp")?;
    let mut command = Command::new("sort");
    let path = std::env::var_os("PATH").unwrap_or_default();
    command
        .env_clear()
        .env("PATH", path)
        .env("LC_ALL", "C")
        .env("TMPDIR", child_bound_path(&sort_tmp)?)
        .args(["-S", "32M", "-u", "-o"])
        .arg(child_bound_path(output)?)
        .arg(child_bound_path(input)?)
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
        .arg(child_bound_path(path)?);
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
        .arg(child_bound_path(repo)?)
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
        .arg(child_bound_path(repo)?)
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
fn copy_verified_open(
    mut input: File,
    mut output: File,
    blob: &CasBlob,
    cancelled: &CancellationToken,
) -> Result<()> {
    let metadata = input.metadata().context("stat Git source CAS child")?;
    if !metadata.file_type().is_file() || metadata.len() != blob.len {
        bail!("Git source CAS child is not a regular file")
    }
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

/// Linux capability paths name descriptors that must remain `CLOEXEC`. Child
/// processes are already `fchdir`-bound to the same directory in `pre_exec`,
/// so translate descendants of that exact directory to relative arguments
/// instead of passing a `/proc/self/fd/*` name that disappears at exec.
fn child_bound_path(path: &Path) -> Result<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        let Some(remainder) = path.strip_prefix("/proc/self/fd").ok() else {
            return Ok(path.to_owned());
        };
        let mut components = remainder.components();
        let Some(std::path::Component::Normal(raw_fd)) = components.next() else {
            return Ok(path.to_owned());
        };
        let Some(raw_fd) = raw_fd
            .to_str()
            .and_then(|value| value.parse::<libc::c_int>().ok())
        else {
            return Ok(path.to_owned());
        };
        let Some(bound_fd) = SCRATCH_CHILD_FD.with(|slot| slot.get()) else {
            return Ok(path.to_owned());
        };
        let source = fd_stat(raw_fd)?;
        let bound = fd_stat(bound_fd)?;
        if source.st_dev != bound.st_dev || source.st_ino != bound.st_ino {
            return Ok(path.to_owned());
        }
        let relative: PathBuf = components.collect();
        if relative.components().any(|part| {
            !matches!(
                part,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        }) {
            bail!("Git source child path escapes its bound scratch directory")
        }
        Ok(if relative.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            relative
        })
    }
    #[cfg(not(target_os = "linux"))]
    Ok(path.to_owned())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn create_private_subdir(parent: &Path, prefix: &str) -> Result<PathBuf> {
    for _ in 0..32 {
        let name = format!(".{prefix}.{}", hex::encode(rand::random::<[u8; 16]>()));
        let path = parent.join(name);
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("create private Git source subdirectory"),
        }
    }
    bail!("exhausted private Git source subdirectory names")
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
    let opened = unsafe { OwnedFd::from_raw_fd(fd) };
    duplicate_cloexec(opened.as_raw_fd())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_bound_directory(path: &Path) -> Result<OwnedFd> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open capability directory");
    }
    let opened = unsafe { OwnedFd::from_raw_fd(fd) };
    duplicate_cloexec(opened.as_raw_fd())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn duplicate_cloexec(fd: libc::c_int) -> Result<OwnedFd> {
    // Keep capabilities out of the descriptor range conventionally reused by
    // stdio/runtime setup so exec-leak tests cannot be confused by reuse.
    let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 64) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error()).context("duplicate capability descriptor");
    }
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

#[cfg(target_os = "linux")]
fn capability_path(fd: libc::c_int) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{fd}"))
}

#[cfg(target_os = "linux")]
fn capability_file_path(fd: libc::c_int) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{fd}"))
}

#[cfg(target_os = "macos")]
fn capability_file_path(fd: libc::c_int) -> PathBuf {
    PathBuf::from(format!("/dev/fd/{fd}"))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn fd_stat(fd: libc::c_int) -> Result<libc::stat> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("stat Git source scratch handle");
    }
    Ok(unsafe { stat.assume_init() })
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
        fn put_file(
            &self,
            blob: &CasBlob,
            source: &Path,
            cancelled: &CancellationToken,
        ) -> Result<()> {
            check_cancelled(cancelled)?;
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

        fn put_bytes(
            &self,
            blob: &CasBlob,
            bytes: &[u8],
            cancelled: &CancellationToken,
        ) -> Result<()> {
            check_cancelled(cancelled)?;
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
        let store = CasGitSourceStore::new(&cas).unwrap();
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
        let auth = AuthenticatedGitSource::from_local_verification(
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
        let store = CasGitSourceStore::new(&cas).unwrap();
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
            CasGitSourceStore::new(&worker_cas).unwrap().load_bytes(
                &blob,
                128 * 1024 * 1024,
                &worker_token,
            )
        });
        std::thread::sleep(Duration::from_millis(10));
        let started = std::time::Instant::now();
        token.cancel();
        assert!(worker.join().unwrap().is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn cas_store_remains_bound_across_root_replacement_and_clone_lifetime() {
        let parent = tempfile::tempdir().unwrap();
        let configured = parent.path().join("cas");
        std::fs::create_dir(&configured).unwrap();
        let cas = Cas::new(&configured).unwrap();
        let store = CasGitSourceStore::new(&cas).unwrap();
        let first_bytes = b"first physical CAS object";
        let first = CasBlob {
            hash: hex::encode(Sha256::digest(first_bytes)),
            len: first_bytes.len() as u64,
        };
        store
            .put_bytes(&first, first_bytes, &CancellationToken::new())
            .unwrap();

        let physical = parent.path().join("physical");
        std::fs::rename(&configured, &physical).unwrap();
        std::fs::create_dir(&configured).unwrap();
        std::fs::write(configured.join("replacement-sentinel"), b"replacement").unwrap();

        let second_bytes = b"second physical CAS object after rename";
        let second = CasBlob {
            hash: hex::encode(Sha256::digest(second_bytes)),
            len: second_bytes.len() as u64,
        };
        let clone = store.clone();
        drop(store);
        clone
            .put_bytes(&second, second_bytes, &CancellationToken::new())
            .unwrap();
        let token = CancellationToken::new();
        assert_eq!(clone.load_bytes(&first, 1024, &token).unwrap(), first_bytes);
        let worker_blob = second.clone();
        let worker = std::thread::spawn(move || {
            clone.load_bytes(&worker_blob, 1024, &CancellationToken::new())
        });
        assert_eq!(worker.join().unwrap().unwrap(), second_bytes);
        assert_eq!(
            std::fs::read(configured.join("replacement-sentinel")).unwrap(),
            b"replacement"
        );
        assert_eq!(std::fs::read_dir(&configured).unwrap().count(), 1);
        assert!(
            physical
                .join(&second.hash[..2])
                .join(&second.hash)
                .is_file()
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn cas_store_canonicalization_failure_is_fatal() {
        let parent = tempfile::tempdir().unwrap();
        let configured = parent.path().join("cas");
        let cas = Cas::new(&configured).unwrap();
        std::fs::remove_dir(&configured).unwrap();
        assert!(CasGitSourceStore::new(&cas).is_err());
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
        let name = std::fs::read_dir(&configured)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .file_name();
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
        let relocated = moved_again.join("relocated-attempt");
        std::fs::rename(moved_again.join(&name), &relocated).unwrap();
        std::fs::create_dir(moved_again.join(&name)).unwrap();
        std::fs::write(moved_again.join(&name).join("final-window"), b"replacement").unwrap();
        std::fs::write(attempt.path().join("repo/after-final-swap"), b"bound").unwrap();
        assert!(relocated.join("repo/original").is_file());
        assert!(relocated.join("repo/after-swap").is_file());
        assert!(relocated.join("repo/after-final-swap").is_file());
        scope.finish().unwrap();
        drop(attempt);
        assert_eq!(
            std::fs::read(moved_again.join(&name).join("final-window")).unwrap(),
            b"replacement"
        );
        assert!(relocated.join("repo/original").is_file());
        assert_eq!(
            std::fs::read(replacement_attempt.join("sentinel")).unwrap(),
            b"replacement"
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn scratch_capabilities_are_closed_by_exec() {
        let root = tempfile::tempdir().unwrap();
        let configured = root.path().join("scratch");
        std::fs::create_dir(&configured).unwrap();
        let cas_root = root.path().join("cas");
        let cas = Cas::new(&cas_root).unwrap();
        let store = CasGitSourceStore::new(&cas).unwrap();
        let attempt = BoundScratch::new(&configured, "exec").unwrap();
        let scope = attempt.enter().unwrap();
        let attempt_fd = attempt.attempt.as_raw_fd().to_string();
        let staging_fd = scope.staging.as_ref().unwrap().as_raw_fd().to_string();
        #[cfg(target_os = "macos")]
        let old_fd = scope.old.as_ref().unwrap().as_raw_fd().to_string();
        #[cfg(target_os = "linux")]
        let old_fd = "999999".to_owned();
        let cas_fd = store.root.as_raw_fd().to_string();
        let mut child = Command::new("sh");
        child
            .args([
                "-c",
                "test ! -e /dev/fd/$ATTEMPT_FD && test ! -e /dev/fd/$STAGING_FD && test ! -e /dev/fd/$OLD_FD && test ! -e /dev/fd/$CAS_FD",
            ])
            .env("ATTEMPT_FD", attempt_fd)
            .env("STAGING_FD", staging_fd)
            .env("OLD_FD", old_fd)
            .env("CAS_FD", cas_fd)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        bind_child_to_scratch(&mut child);
        assert!(child.status().unwrap().success());
        scope.finish().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cloexec_capability_paths_become_bound_relative_child_arguments() {
        let root = tempfile::tempdir().unwrap();
        let attempt = BoundScratch::new(root.path(), "child-path").unwrap();
        let scope = attempt.enter().unwrap();
        std::fs::write(attempt.path().join("sentinel"), b"bound").unwrap();
        let child_path = child_bound_path(&attempt.path().join("sentinel")).unwrap();
        assert_eq!(child_path, Path::new("sentinel"));

        let mut child = Command::new("sh");
        child
            .args([
                "-c",
                "test -f \"$1\" && test ! -e /proc/self/fd/$ATTEMPT_FD",
                "scratch-child",
            ])
            .arg(child_path)
            .env("ATTEMPT_FD", attempt.attempt.as_raw_fd().to_string());
        bind_child_to_scratch(&mut child);
        assert!(child.status().unwrap().success());
        scope.finish().unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn pthread_cwd_restore_failure_aborts_subprocess() {
        const INJECT: &str = "RIPCLONE_TEST_FAIL_PTHREAD_CWD_RESTORE";
        if std::env::var_os(INJECT).is_some() {
            let root = tempfile::tempdir().unwrap();
            let attempt = BoundScratch::new(root.path(), "restore-failure").unwrap();
            let scope = attempt.enter().unwrap();
            FAIL_PTHREAD_CWD_RESTORE.store(true, std::sync::atomic::Ordering::SeqCst);
            drop(scope);
            unreachable!("restore failure must abort")
        }
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "git_source::tests::pthread_cwd_restore_failure_aborts_subprocess",
                "--nocapture",
            ])
            .env(INJECT, "1")
            .status()
            .unwrap();
        assert!(!status.success());
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
        let one_path = std::fs::read_dir(&one_root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let two_path = std::fs::read_dir(&two_root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
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

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn cancelled_large_sort_keeps_all_temporary_state_inside_abandoned_attempt() {
        let root = tempfile::tempdir().unwrap();
        let configured = root.path().join("scratch");
        std::fs::create_dir(&configured).unwrap();
        std::fs::write(root.path().join("outside-sentinel"), b"outside").unwrap();
        let attempt = BoundScratch::new(&configured, "sort-cancel").unwrap();
        let scope = attempt.enter().unwrap();
        let inventory = create_private_subdir(&attempt.path(), "large-sort").unwrap();
        let input = inventory.join("input");
        let output = inventory.join("output");
        let mut writer = std::io::BufWriter::new(File::create(&input).unwrap());
        for value in (0u64..500_000).rev() {
            writeln!(writer, "{:040x}", value).unwrap();
        }
        writer.flush().unwrap();
        let token = CancellationToken::new();
        let cancel = token.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(2));
            cancel.cancel();
        });
        assert!(sort_unique_file(&input, &output, &token).is_err());
        canceller.join().unwrap();
        assert_eq!(
            std::fs::read(root.path().join("outside-sentinel")).unwrap(),
            b"outside"
        );
        assert_eq!(std::fs::read_dir(&configured).unwrap().count(), 1);
        assert!(std::fs::read_dir(&inventory).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".sort-tmp.")
        }));
        scope.finish().unwrap();
        drop(attempt);
        assert_eq!(std::fs::read_dir(&configured).unwrap().count(), 1);
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
        let authority = AuthenticatedGitSource::from_local_verification(
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
        let authority = AuthenticatedGitSource::from_local_verification(
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
        let wrong_identity = AuthenticatedGitSource::from_local_verification(
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
        remote
            .put_bytes(&root, &bytes, &CancellationToken::new())
            .unwrap();
        let swapped_authority = AuthenticatedGitSource::from_local_verification(
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
        remote
            .put_bytes(&root, &bytes, &CancellationToken::new())
            .unwrap();
        let authority = AuthenticatedGitSource::from_local_verification(
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
        let canonical_authority = AuthenticatedGitSource::from_local_verification(
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
        remote
            .put_bytes(&thin_pair.pack, &thin_pack_bytes, &CancellationToken::new())
            .unwrap();
        remote
            .put_bytes(
                &thin_pair.index,
                &thin_index_bytes,
                &CancellationToken::new(),
            )
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
        remote
            .put_bytes(&root, &root_bytes, &CancellationToken::new())
            .unwrap();
        let authority = AuthenticatedGitSource::from_local_verification(
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
        let authority = AuthenticatedGitSource::from_local_verification(
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
