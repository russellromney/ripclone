//! Typed, immutable publication receipts for independently built artifacts.
//!
//! The manifest is itself addressed by SHA-256 in CAS. Verification starts at
//! that hash, binds all scheduler-key fields, verifies every referenced CAS
//! object, and reconstructs the relevant Git semantics without consulting a
//! mutable provider mirror.

use crate::artifact_scheduler::{
    ArtifactKey, ArtifactKind, ClaimedArtifact, CompletionEvidence, CompletionVerifier,
};
use crate::cas::Cas;
use crate::clonepack::install_manifest_pack_bytes;
use crate::manifest::MetadataChunk;
use anyhow::{Context, Result, bail};
use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

pub const ARTIFACT_MANIFEST_SCHEMA: u32 = 1;
pub const PRODUCTION_VERIFIER_IDENTITY: &str = "ripclone-typed-cas-artifact-v1";
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_PACKS: usize = 16_384;
const MAX_ARCHIVE_CHUNKS: usize = 65_536;
const MAX_COMMIT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestKey {
    pub workspace: String,
    pub repo: String,
    pub commit: String,
    pub kind: ArtifactKind,
    pub format_version: u32,
}

impl From<&ArtifactKey> for ManifestKey {
    fn from(value: &ArtifactKey) -> Self {
        Self {
            workspace: value.workspace.clone(),
            repo: value.repo.clone(),
            commit: value.commit.clone(),
            kind: value.kind,
            format_version: value.format_version,
        }
    }
}

impl ManifestKey {
    fn matches(&self, key: &ArtifactKey) -> bool {
        self.workspace == key.workspace
            && self.repo == key.repo
            && self.commit == key.commit
            && self.kind == key.kind
            && self.format_version == key.format_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasBlob {
    pub hash: String,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitPackPair {
    pub pack: CasBlob,
    pub index: CasBlob,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadArtifact {
    pub packs: Vec<GitPackPair>,
    pub prebuilt_index: CasBlob,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullHistoryArtifact {
    /// Raw bytes of the exact target commit. This authenticates its parent
    /// list without making history depend on the separately published Head.
    pub target_commit_object: CasBlob,
    /// Complete closure of every parent. Empty iff the target is a root.
    pub history_packs: Vec<GitPackPair>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesArtifact {
    /// Raw exact target commit, used to authenticate the expected tree OID.
    pub target_commit_object: CasBlob,
    pub metadata: CasBlob,
    /// Indexed exactly by `FrameInfo.chunk_index`.
    pub archive_chunks: Vec<CasBlob>,
    pub zstd_dictionary: Option<CasBlob>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "artifact",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ArtifactPayload {
    Head(HeadArtifact),
    FullHistory(FullHistoryArtifact),
    Files(FilesArtifact),
}

impl ArtifactPayload {
    fn kind(&self) -> ArtifactKind {
        match self {
            Self::Head(_) => ArtifactKind::Head,
            Self::FullHistory(_) => ArtifactKind::FullHistory,
            Self::Files(_) => ArtifactKind::Files,
        }
    }

    fn artifact_count(&self) -> u64 {
        match self {
            Self::Head(head) => head.packs.len() as u64 * 2 + 1,
            Self::FullHistory(history) => history.history_packs.len() as u64 * 2 + 1,
            Self::Files(files) => {
                2 + files.archive_chunks.len() as u64 + u64::from(files.zstd_dictionary.is_some())
            }
        }
    }
}

#[derive(Serialize)]
struct SemanticManifest<'a> {
    schema_version: u32,
    key: &'a ManifestKey,
    payload: &'a ArtifactPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactManifest {
    pub schema_version: u32,
    pub key: ManifestKey,
    pub semantic_digest: String,
    pub payload: ArtifactPayload,
}

impl ArtifactManifest {
    pub fn new(key: &ArtifactKey, payload: ArtifactPayload) -> Result<Self> {
        validate_commit_oid(&key.commit)?;
        if key.workspace.trim().is_empty() || key.repo.trim().is_empty() || key.format_version == 0
        {
            bail!("artifact manifest key is invalid");
        }
        if payload.kind() != key.kind {
            bail!("artifact payload kind does not match key");
        }
        let key = ManifestKey::from(key);
        let semantic_digest = semantic_digest(ARTIFACT_MANIFEST_SCHEMA, &key, &payload)?;
        Ok(Self {
            schema_version: ARTIFACT_MANIFEST_SCHEMA,
            key,
            semantic_digest,
            payload,
        })
    }

    pub fn store(&self, cas: &Cas) -> Result<CompletionEvidence> {
        self.validate_envelope()?;
        let bytes = serde_json::to_vec(self)?;
        let manifest = cas.put(&bytes)?;
        Ok(CompletionEvidence {
            key: ArtifactKey {
                workspace: self.key.workspace.clone(),
                repo: self.key.repo.clone(),
                commit: self.key.commit.clone(),
                kind: self.key.kind,
                format_version: self.key.format_version,
            },
            manifest,
            artifact_count: self.payload.artifact_count(),
        })
    }

    fn validate_envelope(&self) -> Result<()> {
        if self.schema_version != ARTIFACT_MANIFEST_SCHEMA {
            bail!("unsupported artifact manifest schema");
        }
        validate_commit_oid(&self.key.commit)?;
        if self.key.workspace.trim().is_empty()
            || self.key.repo.trim().is_empty()
            || self.key.format_version == 0
            || self.payload.kind() != self.key.kind
        {
            bail!("artifact manifest key/payload mismatch");
        }
        let expected = semantic_digest(self.schema_version, &self.key, &self.payload)?;
        if self.semantic_digest != expected {
            bail!("artifact manifest semantic digest mismatch");
        }
        Ok(())
    }
}

fn semantic_digest(schema: u32, key: &ManifestKey, payload: &ArtifactPayload) -> Result<String> {
    use sha2::Digest;
    let canonical = serde_json::to_vec(&SemanticManifest {
        schema_version: schema,
        key,
        payload,
    })?;
    Ok(hex::encode(Sha256::digest(canonical)))
}

#[derive(Clone)]
pub struct CasCompletionVerifier {
    cas: Cas,
}

impl CasCompletionVerifier {
    pub fn new(cas: Cas) -> Self {
        Self { cas }
    }

    pub fn verify_manifest(
        &self,
        key: &ArtifactKey,
        manifest_hash: &str,
        artifact_count: u64,
    ) -> Result<ArtifactManifest> {
        Cas::validate_artifact_id(manifest_hash).context("invalid artifact manifest CAS id")?;
        let bytes = self
            .cas
            .get(manifest_hash)
            .context("read artifact manifest")?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            bail!("artifact manifest exceeds verifier limit");
        }
        let manifest: ArtifactManifest =
            serde_json::from_slice(&bytes).context("decode typed artifact manifest")?;
        manifest.validate_envelope()?;
        if !manifest.key.matches(key) {
            bail!("artifact manifest does not match scheduler key");
        }
        if artifact_count != manifest.payload.artifact_count() {
            bail!("artifact completion count does not match typed manifest");
        }
        match &manifest.payload {
            ArtifactPayload::Head(head) => self.verify_head(&key.commit, head)?,
            ArtifactPayload::FullHistory(history) => {
                self.verify_full_history(&key.commit, history)?
            }
            ArtifactPayload::Files(files) => self.verify_files(&key.commit, files)?,
        }
        Ok(manifest)
    }

    fn verify_blob(&self, blob: &CasBlob, role: &str) -> Result<Vec<u8>> {
        Cas::validate_artifact_id(&blob.hash).with_context(|| format!("invalid {role} CAS id"))?;
        let actual = self
            .cas
            .verify_object(&blob.hash)
            .with_context(|| format!("verify {role} CAS object"))?;
        if actual != blob.len {
            bail!("{role} CAS length mismatch");
        }
        self.cas
            .get(&blob.hash)
            .with_context(|| format!("read {role}"))
    }

    fn materialize_packs(&self, packs: &[GitPackPair]) -> Result<tempfile::TempDir> {
        let repo = tempfile::tempdir()?;
        git(repo.path(), &["init", "--quiet"])?;
        let mut bytes = Vec::with_capacity(packs.len());
        for pair in packs {
            bytes.push((
                Bytes::from(self.verify_blob(&pair.pack, "Git pack")?),
                Bytes::from(self.verify_blob(&pair.index, "Git pack index")?),
            ));
        }
        install_manifest_pack_bytes(&repo.path().join(".git/objects/pack"), bytes)?;
        for entry in std::fs::read_dir(repo.path().join(".git/objects/pack"))? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "idx") {
                git(
                    repo.path(),
                    &["verify-pack", path.to_string_lossy().as_ref()],
                )?;
            }
        }
        Ok(repo)
    }

    fn verify_head(&self, commit: &str, head: &HeadArtifact) -> Result<()> {
        if head.packs.is_empty() || head.packs.len() > MAX_PACKS {
            bail!("Head artifact contains no packs");
        }
        let repo = self.materialize_packs(&head.packs)?;
        std::fs::write(repo.path().join(".git/HEAD"), format!("{commit}\n"))?;
        std::fs::write(repo.path().join(".git/shallow"), format!("{commit}\n"))?;
        git(
            repo.path(),
            &["cat-file", "-e", &format!("{commit}^{{commit}}")],
        )?;
        git(
            repo.path(),
            &["fsck", "--connectivity-only", "--no-dangling", commit],
        )?;
        if git(repo.path(), &["rev-list", "--count", "HEAD"])? != "1" {
            bail!("Head artifact is not exact depth one");
        }
        let index = self.verify_blob(&head.prebuilt_index, "Head prebuilt index")?;
        std::fs::write(repo.path().join(".git/index"), index)?;
        let actual_tree = git(repo.path(), &["write-tree"])?;
        let expected_tree = git(repo.path(), &["rev-parse", &format!("{commit}^{{tree}}")])?;
        if actual_tree != expected_tree {
            bail!("Head prebuilt index does not match exact target tree");
        }
        let expected_objects = reachable_objects(repo.path(), &[commit])?;
        if packed_object_ids(repo.path())? != expected_objects {
            bail!("Head packs do not contain the exact depth-one object set");
        }
        Ok(())
    }

    fn verify_full_history(&self, commit: &str, history: &FullHistoryArtifact) -> Result<()> {
        if history.target_commit_object.len > MAX_COMMIT_BYTES
            || history.history_packs.len() > MAX_PACKS
        {
            bail!("FullHistory artifact exceeds verifier limits");
        }
        let commit_bytes =
            self.verify_blob(&history.target_commit_object, "history commit anchor")?;
        let parsed = parse_exact_commit(commit, &commit_bytes)?;
        if parsed.parents.is_empty() {
            if !history.history_packs.is_empty() {
                bail!("root commit history must be empty");
            }
            return Ok(());
        }
        if history.history_packs.is_empty() {
            bail!("non-root commit history contains no packs");
        }
        let repo = self.materialize_packs(&history.history_packs)?;
        for parent in &parsed.parents {
            git(
                repo.path(),
                &["cat-file", "-e", &format!("{parent}^{{commit}}")],
            )?;
            git(
                repo.path(),
                &["fsck", "--connectivity-only", "--no-dangling", parent],
            )?;
        }
        let parents = parsed
            .parents
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        if packed_object_ids(repo.path())? != reachable_objects(repo.path(), &parents)? {
            bail!("FullHistory packs do not contain the exact parent closure");
        }
        Ok(())
    }

    fn verify_files(&self, commit: &str, files: &FilesArtifact) -> Result<()> {
        if files.target_commit_object.len > MAX_COMMIT_BYTES
            || files.archive_chunks.len() > MAX_ARCHIVE_CHUNKS
        {
            bail!("Files artifact exceeds verifier limits");
        }
        let commit_bytes = self.verify_blob(&files.target_commit_object, "files commit anchor")?;
        let parsed = parse_exact_commit(commit, &commit_bytes)?;
        let metadata_bytes = self.verify_blob(&files.metadata, "files metadata")?;
        let mut input = metadata_bytes.as_slice();
        let metadata = MetadataChunk::read(&mut input).context("decode files metadata")?;
        if metadata.encode_to_vec() != metadata_bytes {
            bail!("Files metadata is non-canonical or contains unknown fields");
        }
        if !metadata.skeleton_pack.is_empty()
            || !metadata.skeleton_idx.is_empty()
            || !metadata.prebuilt_index.is_empty()
        {
            bail!("Files metadata contains ambiguous embedded Git artifacts");
        }
        let chunks = files
            .archive_chunks
            .iter()
            .map(|chunk| self.verify_blob(chunk, "files archive chunk"))
            .collect::<Result<Vec<_>>>()?;
        let expected_chunks = metadata
            .frames
            .iter()
            .map(|frame| frame.chunk_index as usize + 1)
            .max()
            .unwrap_or(0);
        if chunks.len() != expected_chunks {
            bail!("Files archive chunk table is not exact");
        }
        let dictionary = files
            .zstd_dictionary
            .as_ref()
            .map(|blob| self.verify_blob(blob, "files zstd dictionary"))
            .transpose()?;
        let root = tempfile::tempdir()?;
        reconstruct_files(&metadata, &chunks, dictionary.as_deref(), root.path())?;
        git(root.path(), &["init", "--quiet"])?;
        git(root.path(), &["config", "core.filemode", "true"])?;
        git(root.path(), &["add", "-f", "--all"])?;
        let actual_tree = git(root.path(), &["write-tree"])?;
        if actual_tree != parsed.tree {
            bail!("Files archive does not reconstruct exact target tree");
        }
        Ok(())
    }
}

impl CompletionVerifier for CasCompletionVerifier {
    fn identity(&self) -> &'static str {
        PRODUCTION_VERIFIER_IDENTITY
    }

    fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
        if evidence.key != claim.record.key {
            bail!("completion evidence does not match claimed artifact key");
        }
        self.verify_manifest(&evidence.key, &evidence.manifest, evidence.artifact_count)?;
        Ok(())
    }
}

struct ParsedCommit {
    tree: String,
    parents: Vec<String>,
}

fn parse_exact_commit(expected: &str, bytes: &[u8]) -> Result<ParsedCommit> {
    validate_commit_oid(expected)?;
    let mut hasher = Sha1::new();
    hasher.update(format!("commit {}\0", bytes.len()).as_bytes());
    hasher.update(bytes);
    if hex::encode(hasher.finalize()) != expected {
        bail!("commit anchor does not hash to exact target commit");
    }
    let headers = bytes
        .split(|byte| *byte == b'\n')
        .take_while(|line| !line.is_empty());
    let mut tree = None;
    let mut parents = Vec::new();
    for line in headers {
        if let Some(value) = line.strip_prefix(b"tree ") {
            if tree.is_some() {
                bail!("commit anchor has duplicate tree headers");
            }
            tree = Some(std::str::from_utf8(value)?.to_owned());
        } else if let Some(value) = line.strip_prefix(b"parent ") {
            parents.push(std::str::from_utf8(value)?.to_owned());
        }
    }
    let tree = tree.context("commit anchor has no tree")?;
    validate_commit_oid(&tree).context("commit anchor tree is invalid")?;
    for parent in &parents {
        validate_commit_oid(parent).context("commit anchor parent is invalid")?;
    }
    Ok(ParsedCommit { tree, parents })
}

fn validate_commit_oid(oid: &str) -> Result<()> {
    if oid.len() != 40
        || !oid
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        bail!("artifact manifest requires canonical lowercase SHA-1 object ids");
    }
    Ok(())
}

fn reconstruct_files(
    metadata: &MetadataChunk,
    chunks: &[Vec<u8>],
    dictionary: Option<&[u8]>,
    root: &Path,
) -> Result<()> {
    let mut frames = Vec::with_capacity(metadata.frames.len());
    let mut chunk_ranges = vec![Vec::new(); chunks.len()];
    for (i, frame) in metadata.frames.iter().enumerate() {
        let chunk = chunks
            .get(frame.chunk_index as usize)
            .with_context(|| format!("frame {i} references missing chunk"))?;
        let start = usize::try_from(frame.chunk_offset)?;
        let end = start
            .checked_add(frame.compressed_len as usize)
            .context("compressed frame bounds overflow")?;
        let compressed = chunk
            .get(start..end)
            .with_context(|| format!("frame {i} extends past archive chunk"))?;
        chunk_ranges[frame.chunk_index as usize].push((start, end));
        let mut raw = Vec::new();
        let expected_raw = frame.raw_len as u64;
        match dictionary {
            Some(dict) => {
                let mut decoder = zstd::stream::Decoder::with_dictionary(compressed, dict)?;
                decoder
                    .by_ref()
                    .take(expected_raw + 1)
                    .read_to_end(&mut raw)?;
            }
            None => {
                let mut decoder = zstd::stream::Decoder::new(compressed)?;
                decoder
                    .by_ref()
                    .take(expected_raw + 1)
                    .read_to_end(&mut raw)?;
            }
        }
        if raw.len() != frame.raw_len as usize {
            bail!("frame {i} raw length mismatch");
        }
        frames.push(raw);
    }
    for (chunk_index, (chunk, ranges)) in chunks.iter().zip(&mut chunk_ranges).enumerate() {
        ranges.sort_unstable();
        let mut cursor = 0usize;
        for &(start, end) in ranges.iter() {
            if start != cursor || end < start {
                bail!("archive chunk {chunk_index} has gaps or overlapping frames");
            }
            cursor = end;
        }
        if cursor != chunk.len() {
            bail!("archive chunk {chunk_index} contains unreferenced bytes");
        }
    }

    let mut seen_paths = HashSet::<PathBuf>::new();
    let mut used_frames = HashSet::new();
    for entry in &metadata.files {
        let path = crate::fsutil::path_from_bytes(&entry.path);
        crate::fsutil::validate_relative_path(path)?;
        if path.components().any(|component| {
            matches!(component, std::path::Component::Normal(name) if name.as_encoded_bytes().eq_ignore_ascii_case(b".git"))
        }) {
            bail!("Files metadata path enters Git administrative namespace");
        }
        if !seen_paths.insert(path.to_path_buf()) {
            bail!("Files metadata contains duplicate paths");
        }
        let mut content = Vec::new();
        for fragment in &entry.fragments {
            used_frames.insert(fragment.frame_index as usize);
            let frame = frames
                .get(fragment.frame_index as usize)
                .context("file fragment references missing frame")?;
            let start = fragment.frame_offset as usize;
            let end = start
                .checked_add(fragment.raw_len as usize)
                .context("file fragment bounds overflow")?;
            content.extend_from_slice(
                frame
                    .get(start..end)
                    .context("file fragment extends past frame")?,
            );
        }
        let blob_oid = git_object_oid("blob", &content);
        if entry.blob_sha1.as_slice() != hex::decode(blob_oid)? {
            bail!("Files metadata blob identity mismatch");
        }
        let destination = root.join(path);
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            crate::fsutil::safe_create_dir_all(root, parent)?;
        }
        match entry.mode {
            0o100644 | 0o100755 => {
                std::fs::write(&destination, &content)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(
                        &destination,
                        std::fs::Permissions::from_mode(if entry.mode == 0o100755 {
                            0o755
                        } else {
                            0o644
                        }),
                    )?;
                }
            }
            0o120000 => {
                #[cfg(unix)]
                std::os::unix::fs::symlink(crate::fsutil::path_from_bytes(&content), &destination)?;
                #[cfg(not(unix))]
                bail!("symlink archive verification is unsupported on this platform");
            }
            mode => bail!("Files metadata contains unsupported mode {mode:o}"),
        }
    }
    if used_frames.len() != frames.len() {
        bail!("Files metadata contains unreferenced frames");
    }
    Ok(())
}

fn git_object_oid(kind: &str, bytes: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(format!("{kind} {}\0", bytes.len()).as_bytes());
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn packed_object_ids(repo: &Path) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    for entry in std::fs::read_dir(repo.join(".git/objects/pack"))? {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "idx") {
            continue;
        }
        let output = git(
            repo,
            &["verify-pack", "-v", path.to_string_lossy().as_ref()],
        )?;
        for line in output.lines() {
            let Some(oid) = line.split_ascii_whitespace().next() else {
                continue;
            };
            if oid.len() == 40
                && oid
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                ids.insert(oid.to_owned());
            }
        }
    }
    Ok(ids)
}

fn reachable_objects(repo: &Path, revisions: &[&str]) -> Result<HashSet<String>> {
    let mut command = Command::new("git");
    configure_git_command(&mut command);
    command
        .arg("-C")
        .arg(repo)
        .args([
            "rev-list",
            "--objects",
            "--no-object-names",
            "--end-of-options",
        ])
        .args(revisions);
    let output = command.output().context("enumerate verified Git closure")?;
    if !output.status.success() {
        bail!(
            "Git artifact verification failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let mut ids = HashSet::new();
    for oid in String::from_utf8(output.stdout)?.lines() {
        validate_commit_oid(oid).context("Git closure emitted invalid object id")?;
        ids.insert(oid.to_owned());
    }
    Ok(ids)
}

fn git(repo: &Path, args: &[&str]) -> Result<String> {
    let mut command = Command::new("git");
    configure_git_command(&mut command);
    let output = command
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .with_context(|| format!("run git {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "Git artifact verification failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn configure_git_command(command: &mut Command) {
    // Verification must not inherit alternates, replacement refs, injected
    // config, or a caller-selected repository/index from the worker process.
    // Keep only executable discovery; all verified state lives in fresh temp
    // directories populated from the typed CAS descriptors above.
    let path = std::env::var_os("PATH").unwrap_or_default();
    command
        .env_clear()
        .env("PATH", path)
        .env("HOME", "/nonexistent")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("LC_ALL", "C");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{ArtifactRecord, ArtifactState};
    use crate::clonepack::{FileEntry, Fragment, FrameInfo};
    use crate::pack::PackBuilder;
    use std::fs;

    struct Fixture {
        _root: tempfile::TempDir,
        repo: PathBuf,
        cas: Cas,
        first: String,
        second: String,
    }

    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let repo = root.path().join("repo");
            fs::create_dir(&repo).unwrap();
            git(&repo, &["init", "--quiet"]).unwrap();
            git(&repo, &["config", "user.name", "Verifier Test"]).unwrap();
            git(&repo, &["config", "user.email", "verifier@example.invalid"]).unwrap();
            fs::write(repo.join("a.txt"), b"one\n").unwrap();
            git(&repo, &["add", "a.txt"]).unwrap();
            git(&repo, &["commit", "--quiet", "-m", "first"]).unwrap();
            let first = git(&repo, &["rev-parse", "HEAD"]).unwrap();
            fs::write(repo.join("a.txt"), b"two\n").unwrap();
            fs::write(repo.join("run.sh"), b"#!/bin/sh\nexit 0\n").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(repo.join("run.sh"), fs::Permissions::from_mode(0o755))
                    .unwrap();
            }
            git(&repo, &["add", "--all"]).unwrap();
            git(&repo, &["commit", "--quiet", "-m", "second"]).unwrap();
            let second = git(&repo, &["rev-parse", "HEAD"]).unwrap();
            let cas = Cas::new(root.path().join("cas")).unwrap();
            Self {
                _root: root,
                repo,
                cas,
                first,
                second,
            }
        }

        fn key(&self, kind: ArtifactKind) -> ArtifactKey {
            ArtifactKey {
                workspace: "workspace-1".into(),
                repo: "owner/repo".into(),
                commit: self.second.clone(),
                kind,
                format_version: 1,
            }
        }

        fn blob(&self, bytes: &[u8]) -> CasBlob {
            let hash = self.cas.put(bytes).unwrap();
            CasBlob {
                hash,
                len: bytes.len() as u64,
            }
        }

        fn commit_blob(&self, commit: &str) -> CasBlob {
            let bytes = git_bytes(&self.repo, &["cat-file", "commit", commit]);
            self.blob(&bytes)
        }

        fn pairs(&self, commit: &str, depth: Option<usize>) -> Vec<GitPackPair> {
            PackBuilder::new(&self.repo, &self.cas)
                .build_depth_packs(commit, depth, 1024 * 1024)
                .unwrap()
                .into_iter()
                .map(|(pack, pack_len, index, index_len)| GitPackPair {
                    pack: CasBlob {
                        hash: pack,
                        len: pack_len,
                    },
                    index: CasBlob {
                        hash: index,
                        len: index_len,
                    },
                })
                .collect()
        }

        fn head(&self) -> ArtifactManifest {
            let builder = PackBuilder::new(&self.repo, &self.cas);
            let (skeleton, _) = builder.build_shallow_skeleton_pack(&self.second).unwrap();
            let index = builder
                .build_prebuilt_index(&self.second, &skeleton)
                .unwrap();
            ArtifactManifest::new(
                &self.key(ArtifactKind::Head),
                ArtifactPayload::Head(HeadArtifact {
                    packs: self.pairs(&self.second, Some(1)),
                    prebuilt_index: CasBlob {
                        len: self.cas.verify_object(&index).unwrap(),
                        hash: index,
                    },
                }),
            )
            .unwrap()
        }

        fn history(&self) -> ArtifactManifest {
            ArtifactManifest::new(
                &self.key(ArtifactKind::FullHistory),
                ArtifactPayload::FullHistory(FullHistoryArtifact {
                    target_commit_object: self.commit_blob(&self.second),
                    history_packs: self.pairs(&self.first, None),
                }),
            )
            .unwrap()
        }

        fn files(&self) -> ArtifactManifest {
            let entries = [
                (b"a.txt".as_slice(), b"two\n".as_slice(), 0o100644),
                (
                    b"run.sh".as_slice(),
                    b"#!/bin/sh\nexit 0\n".as_slice(),
                    0o100755,
                ),
            ];
            let mut metadata = MetadataChunk::new();
            let mut archive = Vec::new();
            for (path, content, mode) in entries {
                let compressed = zstd::encode_all(content, 1).unwrap();
                let frame_index = metadata.frames.len() as u32;
                let offset = archive.len() as u64;
                archive.extend_from_slice(&compressed);
                metadata.frames.push(FrameInfo {
                    chunk_index: 0,
                    chunk_offset: offset,
                    compressed_len: compressed.len() as u32,
                    raw_len: content.len() as u32,
                });
                metadata.files.push(FileEntry {
                    path: path.to_vec(),
                    mode,
                    blob_sha1: hex::decode(git_object_oid("blob", content)).unwrap(),
                    fragments: vec![Fragment {
                        frame_index,
                        frame_offset: 0,
                        raw_len: content.len() as u32,
                    }],
                });
            }
            let mut metadata_bytes = Vec::new();
            metadata.write(&mut metadata_bytes).unwrap();
            ArtifactManifest::new(
                &self.key(ArtifactKind::Files),
                ArtifactPayload::Files(FilesArtifact {
                    target_commit_object: self.commit_blob(&self.second),
                    metadata: self.blob(&metadata_bytes),
                    archive_chunks: vec![self.blob(&archive)],
                    zstd_dictionary: None,
                }),
            )
            .unwrap()
        }

        fn verify(&self, manifest: &ArtifactManifest) -> Result<ArtifactManifest> {
            let evidence = manifest.store(&self.cas)?;
            CasCompletionVerifier::new(self.cas.clone()).verify_manifest(
                &evidence.key,
                &evidence.manifest,
                evidence.artifact_count,
            )
        }
    }

    fn git_bytes(repo: &Path, args: &[&str]) -> Vec<u8> {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    #[test]
    fn verifies_head_history_and_files_from_cas_after_mirror_is_pruned() {
        let f = Fixture::new();
        let manifests = [f.head(), f.history(), f.files()];
        fs::remove_dir_all(&f.repo).unwrap();
        for manifest in manifests {
            f.verify(&manifest).unwrap();
        }
    }

    #[test]
    fn root_commit_has_valid_empty_history_component() {
        let f = Fixture::new();
        let key = ArtifactKey {
            commit: f.first.clone(),
            kind: ArtifactKind::FullHistory,
            ..f.key(ArtifactKind::FullHistory)
        };
        let manifest = ArtifactManifest::new(
            &key,
            ArtifactPayload::FullHistory(FullHistoryArtifact {
                target_commit_object: f.commit_blob(&f.first),
                history_packs: vec![],
            }),
        )
        .unwrap();
        f.verify(&manifest).unwrap();
    }

    #[test]
    fn empty_tree_files_are_valid_and_exact() {
        let f = Fixture::new();
        fs::remove_file(f.repo.join("a.txt")).unwrap();
        fs::remove_file(f.repo.join("run.sh")).unwrap();
        git(&f.repo, &["add", "--all"]).unwrap();
        git(&f.repo, &["commit", "--quiet", "-m", "empty"]).unwrap();
        let commit = git(&f.repo, &["rev-parse", "HEAD"]).unwrap();
        let key = ArtifactKey {
            commit: commit.clone(),
            ..f.key(ArtifactKind::Files)
        };
        let mut bytes = Vec::new();
        MetadataChunk::new().write(&mut bytes).unwrap();
        let manifest = ArtifactManifest::new(
            &key,
            ArtifactPayload::Files(FilesArtifact {
                target_commit_object: f.commit_blob(&commit),
                metadata: f.blob(&bytes),
                archive_chunks: vec![],
                zstd_dictionary: None,
            }),
        )
        .unwrap();
        f.verify(&manifest).unwrap();
    }

    #[test]
    fn unknown_fields_and_unknown_schema_are_rejected() {
        let f = Fixture::new();
        let manifest = f.head();
        let mut value = serde_json::to_value(&manifest).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("future".into(), 1.into());
        let hash = f.cas.put(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .verify_manifest(&f.key(ArtifactKind::Head), &hash, 3)
                .is_err()
        );
        let mut nested = serde_json::to_value(&manifest).unwrap();
        nested["payload"]["artifact"]
            .as_object_mut()
            .unwrap()
            .insert("future".into(), 1.into());
        let hash = f.cas.put(&serde_json::to_vec(&nested).unwrap()).unwrap();
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .verify_manifest(&f.key(ArtifactKind::Head), &hash, 3)
                .is_err()
        );
        let mut wrong_schema = manifest;
        wrong_schema.schema_version = 2;
        let hash = f
            .cas
            .put(&serde_json::to_vec(&wrong_schema).unwrap())
            .unwrap();
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .verify_manifest(&f.key(ArtifactKind::Head), &hash, 3)
                .is_err()
        );
    }

    #[test]
    fn wrong_key_kind_format_and_count_are_rejected() {
        let f = Fixture::new();
        let evidence = f.head().store(&f.cas).unwrap();
        let verifier = CasCompletionVerifier::new(f.cas.clone());
        for wrong in [
            ArtifactKey {
                workspace: "other".into(),
                ..evidence.key.clone()
            },
            ArtifactKey {
                kind: ArtifactKind::Files,
                ..evidence.key.clone()
            },
            ArtifactKey {
                format_version: 2,
                ..evidence.key.clone()
            },
        ] {
            assert!(
                verifier
                    .verify_manifest(&wrong, &evidence.manifest, evidence.artifact_count)
                    .is_err()
            );
        }
        assert!(
            verifier
                .verify_manifest(
                    &evidence.key,
                    &evidence.manifest,
                    evidence.artifact_count + 1
                )
                .is_err()
        );
    }

    #[test]
    fn corrupt_cas_object_is_rejected_even_when_length_is_unchanged() {
        let f = Fixture::new();
        let manifest = f.files();
        let ArtifactPayload::Files(files) = &manifest.payload else {
            unreachable!()
        };
        let path = f
            .cas
            .root()
            .join(&files.archive_chunks[0].hash[..2])
            .join(&files.archive_chunks[0].hash);
        let len = fs::metadata(&path).unwrap().len();
        fs::write(&path, vec![0u8; len as usize]).unwrap();
        assert!(f.verify(&manifest).is_err());
    }

    #[test]
    fn restamped_payload_with_valid_semantic_digest_still_fails_git_semantics() {
        let f = Fixture::new();
        let mut manifest = f.files();
        let ArtifactPayload::Files(files) = &mut manifest.payload else {
            unreachable!()
        };
        let mut metadata: MetadataChunk = {
            let bytes = f.cas.get(&files.metadata.hash).unwrap();
            MetadataChunk::read(&mut bytes.as_slice()).unwrap()
        };
        metadata.files[0].mode = 0o100755;
        let mut bytes = Vec::new();
        metadata.write(&mut bytes).unwrap();
        files.metadata = f.blob(&bytes);
        manifest.semantic_digest =
            semantic_digest(manifest.schema_version, &manifest.key, &manifest.payload).unwrap();
        assert!(f.verify(&manifest).is_err());
    }

    #[test]
    fn head_rejects_wrong_index_and_history_rejects_missing_parent_closure() {
        let f = Fixture::new();
        let mut head = f.head();
        let ArtifactPayload::Head(payload) = &mut head.payload else {
            unreachable!()
        };
        payload.prebuilt_index = f.blob(b"not an index");
        head.semantic_digest =
            semantic_digest(head.schema_version, &head.key, &head.payload).unwrap();
        assert!(f.verify(&head).is_err());

        let mut history = f.history();
        let ArtifactPayload::FullHistory(payload) = &mut history.payload else {
            unreachable!()
        };
        payload.history_packs.clear();
        history.semantic_digest =
            semantic_digest(history.schema_version, &history.key, &history.payload).unwrap();
        assert!(f.verify(&history).is_err());
    }

    #[test]
    fn digest_descriptor_length_and_payload_kind_cannot_be_relabelled() {
        let f = Fixture::new();
        let mut digest = f.head();
        digest.semantic_digest = "0".repeat(64);
        assert!(f.verify(&digest).is_err());

        let mut length = f.head();
        let ArtifactPayload::Head(payload) = &mut length.payload else {
            unreachable!()
        };
        payload.packs[0].pack.len += 1;
        length.semantic_digest =
            semantic_digest(length.schema_version, &length.key, &length.payload).unwrap();
        assert!(f.verify(&length).is_err());

        assert!(ArtifactManifest::new(&f.key(ArtifactKind::Files), f.head().payload,).is_err());
    }

    #[test]
    fn root_history_rejects_nonempty_payload_and_commit_anchor_restamp() {
        let f = Fixture::new();
        let key = ArtifactKey {
            commit: f.first.clone(),
            ..f.key(ArtifactKind::FullHistory)
        };
        let root_with_junk = ArtifactManifest::new(
            &key,
            ArtifactPayload::FullHistory(FullHistoryArtifact {
                target_commit_object: f.commit_blob(&f.first),
                history_packs: f.pairs(&f.first, None),
            }),
        )
        .unwrap();
        assert!(f.verify(&root_with_junk).is_err());

        let mut restamped = f.history();
        let ArtifactPayload::FullHistory(payload) = &mut restamped.payload else {
            unreachable!()
        };
        payload.target_commit_object = f.commit_blob(&f.first);
        restamped.semantic_digest =
            semantic_digest(restamped.schema_version, &restamped.key, &restamped.payload).unwrap();
        assert!(f.verify(&restamped).is_err());
    }

    #[test]
    fn files_reject_unreferenced_archive_padding() {
        let f = Fixture::new();
        let mut manifest = f.files();
        let ArtifactPayload::Files(payload) = &mut manifest.payload else {
            unreachable!()
        };
        let mut chunk = f.cas.get(&payload.archive_chunks[0].hash).unwrap();
        chunk.extend_from_slice(b"padding");
        payload.archive_chunks[0] = f.blob(&chunk);
        manifest.semantic_digest =
            semantic_digest(manifest.schema_version, &manifest.key, &manifest.payload).unwrap();
        assert!(f.verify(&manifest).is_err());
    }

    #[test]
    fn files_reject_unknown_protobuf_fields_even_when_restamped() {
        let f = Fixture::new();
        let mut manifest = f.files();
        let ArtifactPayload::Files(payload) = &mut manifest.payload else {
            unreachable!()
        };
        let mut metadata = f.cas.get(&payload.metadata.hash).unwrap();
        // Unknown varint field 127. Prost would otherwise silently discard it.
        metadata.extend_from_slice(&[0xf8, 0x07, 0x01]);
        payload.metadata = f.blob(&metadata);
        manifest.semantic_digest =
            semantic_digest(manifest.schema_version, &manifest.key, &manifest.payload).unwrap();
        assert!(f.verify(&manifest).is_err());
    }

    #[test]
    fn verifier_identity_and_scheduler_claim_binding_are_stable() {
        let f = Fixture::new();
        let evidence = f.head().store(&f.cas).unwrap();
        let verifier = CasCompletionVerifier::new(f.cas.clone());
        assert_eq!(verifier.identity(), PRODUCTION_VERIFIER_IDENTITY);
        let claim = ClaimedArtifact {
            record: ArtifactRecord {
                id: 1,
                key: evidence.key.clone(),
                state: ArtifactState::Running,
                owner: Some("worker".into()),
                lease_expires_at: Some(i64::MAX),
                lease_generation: 1,
                claim_attempts: 1,
                retry_count: 0,
                manifest: None,
                error: None,
                failure_class: None,
            },
        };
        verifier.verify(&claim, &evidence).unwrap();
        let mut wrong = evidence;
        wrong.key.repo = "other/repo".into();
        assert!(verifier.verify(&claim, &wrong).is_err());
    }
}
