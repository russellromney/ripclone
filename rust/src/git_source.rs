//! Backend-neutral, immutable Git build sources.
//!
//! A provider fetch is packaged into self-contained pack/index pairs. Uploading
//! those bytes only creates a [`PreparedGitSource`]; a future durable registry
//! must separately authenticate the exact root before a worker can materialize
//! it. This module intentionally contains no database or provider integration.

use crate::artifact_manifest::{CasBlob, GitPackPair};
use crate::cas::Cas;
use crate::pack::PackBuilder;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub const GIT_SOURCE_SCHEMA: u32 = 1;
pub const GIT_SOURCE_FORMAT: u32 = 1;
const DIGEST_DOMAIN: &[u8] = b"ripclone/git-build-source/semantic/v1\0";

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

#[derive(Clone, Copy)]
pub struct CasGitSourceStore<'a> {
    cas: &'a Cas,
}
impl<'a> CasGitSourceStore<'a> {
    pub fn new(cas: &'a Cas) -> Self {
        Self { cas }
    }
}

impl GitSourceUploader for CasGitSourceStore<'_> {
    fn put_file(&self, blob: &CasBlob, source: &Path) -> Result<()> {
        let len = self.cas.put_file_with_hash(&blob.hash, source)?;
        if len != blob.len {
            bail!("uploaded Git source child length mismatch")
        }
        Ok(())
    }
    fn put_bytes(&self, blob: &CasBlob, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 != blob.len {
            bail!("uploaded Git source root length mismatch")
        }
        self.cas.put_with_hash(&blob.hash, bytes)
    }
}

impl GitSourceLoader for CasGitSourceStore<'_> {
    fn load_file(
        &self,
        blob: &CasBlob,
        destination: &Path,
        cancelled: &CancellationToken,
    ) -> Result<()> {
        check_cancelled(cancelled)?;
        validate_blob(blob, u64::MAX)?;
        copy_verified_create_new(&self.cas.path(&blob.hash), destination, blob, cancelled)
    }
    fn load_bytes(
        &self,
        blob: &CasBlob,
        maximum: u64,
        cancelled: &CancellationToken,
    ) -> Result<Vec<u8>> {
        check_cancelled(cancelled)?;
        validate_blob(blob, maximum)?;
        let bytes = std::fs::read(self.cas.path(&blob.hash)).context("read Git source CAS root")?;
        verify_bytes(blob, &bytes)?;
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
        reject_symlink_dir(self.scratch)?;
        check_cancelled(cancelled)?;
        let objects = enumerate_closure(
            &source.repo_path,
            &source.commit,
            source.object_format,
            &self.limits,
            cancelled,
        )?;
        let object_set_digest = object_set_digest(&objects);
        let pack_scratch = tempfile::Builder::new()
            .prefix("git-source-pack.")
            .tempdir_in(self.scratch)?;
        let built = if source.object_format == GitObjectFormat::Sha256 {
            // The existing partitioner uses a SHA-1-only gix object-size path.
            // C Git is object-format aware and emits a complete non-thin pack
            // plus its matching index without interpreting provider metadata.
            build_sha256_source_pack(
                &source.repo_path,
                &objects,
                self.local_cas,
                pack_scratch.path(),
                cancelled,
            )?
        } else {
            PackBuilder::new_cancellable_in_scratch(
                &source.repo_path,
                self.local_cas,
                pack_scratch.path(),
                cancelled.clone(),
            )
            .build_object_set_packs(
                &objects,
                self.limits.target_pack_raw_bytes,
                false,
            )?
        };
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
            object_count: objects.len() as u64,
            object_set_digest,
            aggregate_pack_bytes,
            semantic_digest: String::new(),
        };
        manifest.semantic_digest = manifest.compute_semantic_digest()?;
        manifest.validate(&self.limits)?;
        // Verify the locally produced source exactly before any root can be uploaded.
        verify_local_manifest(
            &manifest,
            self.local_cas,
            self.scratch,
            &self.limits,
            cancelled,
        )?;
        let root_bytes = manifest.canonical_bytes()?;
        let root = CasBlob {
            hash: hex::encode(Sha256::digest(&root_bytes)),
            len: root_bytes.len() as u64,
        };
        // Publication order is a safety property: every child, then the sole root.
        for pair in &manifest.packs {
            check_cancelled(cancelled)?;
            self.uploader
                .put_file(&pair.pack, &self.local_cas.path(&pair.pack.hash))?;
            self.uploader
                .put_file(&pair.index, &self.local_cas.path(&pair.index.hash))?;
        }
        check_cancelled(cancelled)?;
        self.uploader.put_bytes(&root, &root_bytes)?;
        Ok(PreparedGitSource { root, manifest })
    }
}

pub struct MaterializedGitSource {
    directory: tempfile::TempDir,
    commit: String,
}
impl MaterializedGitSource {
    pub fn path(&self) -> &Path {
        self.directory.path()
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
        reject_symlink_dir(self.scratch)?;
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
        let directory = tempfile::Builder::new()
            .prefix("git-source-materialized.")
            .tempdir_in(self.scratch)?;
        init_bare(directory.path(), manifest.object_format)?;
        let pack_dir = directory.path().join("objects/pack");
        for (i, pair) in manifest.packs.iter().enumerate() {
            check_cancelled(cancelled)?;
            // Git discovers packs by the `pack-*.idx` convention. The suffix
            // is attempt-local and conveys no identity; content hashes remain
            // exclusively in the authenticated descriptors.
            let base = format!("pack-source-{i:08}");
            let pack = pack_dir.join(format!("{base}.pack"));
            let index = pack_dir.join(format!("{base}.idx"));
            self.loader.load_file(&pair.pack, &pack, cancelled)?;
            self.loader.load_file(&pair.index, &index, cancelled)?;
            verify_file_at(&pack, &pair.pack, cancelled)?;
            verify_file_at(&index, &pair.index, cancelled)?;
            safe_git_ok_quiet_cancelled(
                directory.path(),
                &[
                    "verify-pack",
                    index.to_str().context("non-UTF8 index path")?,
                ],
                cancelled,
            )
            .context("Git source pack/index pair failed verification")?;
        }
        safe_git_ok(
            directory.path(),
            &["update-ref", "refs/heads/ripclone-source", &manifest.commit],
        )?;
        safe_git_ok(
            directory.path(),
            &["symbolic-ref", "HEAD", "refs/heads/ripclone-source"],
        )?;
        safe_git_ok_quiet_cancelled(
            directory.path(),
            &["fsck", "--full", "--strict", "--no-dangling"],
            cancelled,
        )?;
        let objects = enumerate_closure(
            directory.path(),
            &manifest.commit,
            manifest.object_format,
            &self.limits,
            cancelled,
        )?;
        if objects.len() as u64 != manifest.object_count
            || object_set_digest(&objects) != manifest.object_set_digest
        {
            bail!("materialized Git source closure does not match manifest")
        }
        if enumerate_all_objects(
            directory.path(),
            manifest.object_format,
            &self.limits,
            cancelled,
        )? != objects
        {
            bail!("materialized Git source contains objects outside the exact closure")
        }
        Ok(MaterializedGitSource {
            directory,
            commit: manifest.commit,
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
    let actual = text(&safe_git(materialized.path(), &["rev-parse", "HEAD"])?)?;
    if actual != manifest.commit {
        bail!("local Git source verification resolved the wrong commit")
    }
    Ok(())
}

fn build_sha256_source_pack(
    repo: &Path,
    objects: &[String],
    cas: &Cas,
    scratch: &Path,
    cancelled: &CancellationToken,
) -> Result<Vec<(String, u64, String, u64)>> {
    check_cancelled(cancelled)?;
    let prefix = scratch.join("pack");
    crate::git::pack_objects_to_prefix_cancelled(repo, objects, &prefix, cancelled)?;
    check_cancelled(cancelled)?;
    let mut pack = None;
    let mut index = None;
    for entry in std::fs::read_dir(scratch)? {
        let path = entry?.path();
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("pack") if pack.is_none() => pack = Some(path),
            Some("idx") if index.is_none() => index = Some(path),
            Some("pack" | "idx") => bail!("SHA-256 Git source emitted multiple pack pairs"),
            _ => {}
        }
    }
    let pack = pack.context("SHA-256 Git source emitted no pack")?;
    let index = index.context("SHA-256 Git source emitted no index")?;
    let (pack_hash, pack_len) = cas.put_file(pack)?;
    let (index_hash, index_len) = cas.put_file(index)?;
    Ok(vec![(pack_hash, pack_len, index_hash, index_len)])
}

fn enumerate_closure(
    repo: &Path,
    commit: &str,
    format: GitObjectFormat,
    limits: &GitSourceLimits,
    cancelled: &CancellationToken,
) -> Result<Vec<String>> {
    check_cancelled(cancelled)?;
    let stdout = rev_list_bounded(repo, commit, limits.max_objects, cancelled)?;
    let mut objects = Vec::new();
    let mut seen = HashSet::new();
    for oid in &stdout {
        check_cancelled(cancelled)?;
        validate_oid(oid, format)?;
        if !seen.insert(oid.to_owned()) {
            continue;
        }
        if seen.len() > limits.max_objects {
            bail!("Git source object count exceeds limit")
        }
        objects.push(oid.to_owned());
    }
    objects.sort();
    if objects.is_empty() || !objects.iter().any(|oid| oid == commit) {
        bail!("Git source closure is empty or excludes target")
    }
    let ty = text(&safe_git(repo, &["cat-file", "-t", commit])?)?;
    if ty != "commit" {
        bail!("Git source target is not a commit")
    }
    verify_object_sizes_bounded(repo, &objects, limits, cancelled)?;
    Ok(objects)
}

fn verify_object_sizes_bounded(
    repo: &Path,
    objects: &[String],
    limits: &GitSourceLimits,
    cancelled: &CancellationToken,
) -> Result<()> {
    let mut input = tempfile::tempfile().context("create bounded cat-file input")?;
    for oid in objects {
        writeln!(input, "{oid}")?;
    }
    input.rewind()?;
    let lines = run_git_lines_bounded_with_input(
        repo,
        &[
            "cat-file",
            "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        ],
        objects.len(),
        cancelled,
        Some(input),
    )?;
    if lines.len() != objects.len() {
        bail!("Git source object sizing returned an incomplete set")
    }
    let expected: HashSet<&str> = objects.iter().map(String::as_str).collect();
    let mut seen = HashSet::new();
    let mut total = 0u64;
    for line in lines {
        let mut fields = line.split_whitespace();
        let oid = fields.next().context("missing sized Git object id")?;
        let kind = fields.next().context("missing sized Git object type")?;
        let size: u64 = fields
            .next()
            .context("missing sized Git object length")?
            .parse()
            .context("parse Git object length")?;
        if fields.next().is_some()
            || !expected.contains(oid)
            || !seen.insert(oid.to_owned())
            || !matches!(kind, "commit" | "tree" | "blob" | "tag")
        {
            bail!("Git source object sizing emitted forged or duplicate data")
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
    }
    Ok(())
}

fn rev_list_bounded(
    repo: &Path,
    commit: &str,
    max_objects: usize,
    cancelled: &CancellationToken,
) -> Result<Vec<String>> {
    run_git_lines_bounded(
        repo,
        &["rev-list", "--objects", "--no-object-names", commit],
        max_objects,
        cancelled,
    )
}

fn run_git_lines_bounded(
    repo: &Path,
    args: &[&str],
    max_lines: usize,
    cancelled: &CancellationToken,
) -> Result<Vec<String>> {
    run_git_lines_bounded_with_input(repo, args, max_lines, cancelled, None)
}

fn run_git_lines_bounded_with_input(
    repo: &Path,
    args: &[&str],
    max_lines: usize,
    cancelled: &CancellationToken,
    input: Option<File>,
) -> Result<Vec<String>> {
    let mut command = safe_command();
    command
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(input) = input {
        command.stdin(Stdio::from(input));
    }
    let mut child = command
        .spawn()
        .context("spawn bounded Git source command")?;
    let stdout = child.stdout.take().context("capture Git source rev-list")?;
    let result = (|| -> Result<Vec<String>> {
        let mut reader = std::io::BufReader::new(stdout);
        let mut objects = Vec::new();
        let mut line = String::with_capacity(65);
        loop {
            check_cancelled(cancelled)?;
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }
            if line.len() > 256 {
                bail!("bounded Git source command emitted an overlong line")
            }
            let oid = line.trim_end_matches(&['\r', '\n'][..]);
            objects.push(oid.to_owned());
            if objects.len() > max_lines {
                bail!("bounded Git command output exceeds limit")
            }
        }
        Ok(objects)
    })();
    if result.is_err() {
        let _ = child.kill();
    }
    let status = child.wait()?;
    let objects = result?;
    if !status.success() {
        bail!("bounded Git source command failed")
    }
    Ok(objects)
}

fn enumerate_all_objects(
    repo: &Path,
    format: GitObjectFormat,
    limits: &GitSourceLimits,
    cancelled: &CancellationToken,
) -> Result<Vec<String>> {
    check_cancelled(cancelled)?;
    let lines = run_git_lines_bounded(
        repo,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
        limits.max_objects,
        cancelled,
    )?;
    let mut objects = Vec::new();
    let mut seen = HashSet::new();
    for oid in lines {
        check_cancelled(cancelled)?;
        validate_oid(&oid, format)?;
        if seen.insert(oid.clone()) {
            objects.push(oid);
            if objects.len() > limits.max_objects {
                bail!("materialized Git source object count exceeds limit")
            }
        }
    }
    objects.sort();
    Ok(objects)
}

fn object_set_digest(objects: &[String]) -> String {
    let mut h = Sha256::new();
    h.update(b"ripclone/git-build-source/object-set/v1\0");
    for oid in objects {
        h.update((oid.len() as u64).to_be_bytes());
        h.update(oid.as_bytes());
    }
    hex::encode(h.finalize())
}

fn init_bare(path: &Path, format: GitObjectFormat) -> Result<()> {
    let mut command = safe_command();
    let output = command
        .args([
            "init",
            "--bare",
            &format!("--object-format={}", format.git_name()),
        ])
        .arg(path)
        .output()?;
    if !output.status.success() {
        bail!(
            "initialize isolated Git source failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    Ok(())
}

fn safe_git(repo: &Path, args: &[&str]) -> Result<Output> {
    safe_command()
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .output()
        .with_context(|| format!("run isolated git {:?}", args))
}
fn safe_git_ok(repo: &Path, args: &[&str]) -> Result<()> {
    let out = safe_git(repo, args)?;
    if !out.status.success() {
        bail!(
            "isolated git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        )
    }
    Ok(())
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
    let mut child = safe_command()
        .arg("-C")
        .arg(repo)
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::from(diagnostic_child))
        .spawn()
        .with_context(|| format!("run quiet isolated git {:?}", args))?;
    let status = loop {
        if cancelled.is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            bail!("quiet isolated git {:?} cancelled", args)
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
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
    c
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
        init_bare(tmp.path(), format).unwrap();
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
