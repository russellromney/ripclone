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
use crate::manifest::MetadataChunk;
use anyhow::{Context, Result, bail};
use hmac::{Hmac, KeyInit, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::{cell::RefCell, time::Duration};

thread_local! {
    static VERIFICATION_CANCEL: RefCell<Option<tokio_util::sync::CancellationToken>> = const { RefCell::new(None) };
    static VERIFICATION_SCRATCH: RefCell<Option<std::path::PathBuf>> = const { RefCell::new(None) };
}

struct VerificationCancelGuard {
    token: Option<tokio_util::sync::CancellationToken>,
    scratch: Option<std::path::PathBuf>,
}
impl Drop for VerificationCancelGuard {
    fn drop(&mut self) {
        VERIFICATION_CANCEL.with(|slot| *slot.borrow_mut() = self.token.take());
        VERIFICATION_SCRATCH.with(|slot| *slot.borrow_mut() = self.scratch.take());
    }
}

fn verification_tempdir() -> Result<tempfile::TempDir> {
    VERIFICATION_SCRATCH.with(|slot| match slot.borrow().as_ref() {
        Some(root) => {
            std::fs::create_dir_all(root)?;
            Ok(tempfile::Builder::new()
                .prefix("verify.")
                .tempdir_in(root)?)
        }
        None => Ok(tempfile::tempdir()?),
    })
}

fn verification_cancelled() -> bool {
    VERIFICATION_CANCEL.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
    })
}

/// Wire/storage generation for typed artifacts. Any change to manifest payload
/// shape must advance both constants before old scheduler rows can collide
/// with newly built evidence.
pub const ARTIFACT_MANIFEST_SCHEMA: u32 = 2;
pub const ARTIFACT_FORMAT_VERSION: u32 = 2;
pub const PRODUCTION_VERIFIER_IDENTITY: &str = "ripclone-typed-cas-artifact-v1";

fn verifier_identity(limits: &ArtifactVerificationLimits, proof_key: Option<&[u8]>) -> String {
    use sha2::Digest;
    let base = if limits == &ArtifactVerificationLimits::default() {
        PRODUCTION_VERIFIER_IDENTITY.to_owned()
    } else {
        limits.verifier_identity()
    };
    match proof_key {
        Some(key) => {
            let mut digest = Sha256::new();
            digest.update(b"ripclone/artifact-proof-key-id/v1\0");
            digest.update(key);
            format!("{base}:proof:{}", hex::encode(digest.finalize()))
        }
        None => format!("{base}:proof:none"),
    }
}

fn configured_proof_key() -> Option<std::sync::Arc<Vec<u8>>> {
    // This authority is deliberately separate from client/server auth and job
    // bearer tokens. Only trusted verifier processes receive it.
    let configured = std::env::var("RIPCLONE_ARTIFACT_PROOF_KEY").ok();
    #[cfg(test)]
    let configured = configured.or_else(|| Some("ripclone-test-artifact-proof-key-32bytes".into()));
    configured
        .filter(|value| value.len() >= 32)
        .map(|value| std::sync::Arc::new(value.into_bytes()))
}
pub const FULL_HISTORY_BASE_TIER: u32 = 63;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactVerificationLimits {
    pub manifest_bytes: u64,
    pub packs: usize,
    pub pack_bytes: u64,
    pub pack_index_bytes: u64,
    pub total_pack_bytes: u64,
    pub git_objects: u64,
    pub commit_bytes: u64,
    pub metadata_bytes: u64,
    pub files: usize,
    pub frames: usize,
    pub fragments: usize,
    pub archive_chunks: usize,
    pub archive_chunk_bytes: u64,
    pub frame_compressed_bytes: u64,
    pub frame_raw_bytes: u64,
    pub file_raw_bytes: u64,
    pub total_archive_compressed_bytes: u64,
    pub total_archive_raw_bytes: u64,
    pub dictionary_bytes: u64,
}

impl Default for ArtifactVerificationLimits {
    fn default() -> Self {
        Self {
            manifest_bytes: 16 * 1024 * 1024,
            packs: 16_384,
            pack_bytes: 16 * 1024 * 1024 * 1024,
            pack_index_bytes: 2 * 1024 * 1024 * 1024,
            total_pack_bytes: 1024 * 1024 * 1024 * 1024,
            git_objects: 50_000_000,
            commit_bytes: 16 * 1024 * 1024,
            metadata_bytes: 32 * 1024 * 1024,
            files: 250_000,
            frames: 250_000,
            fragments: 1_000_000,
            archive_chunks: 65_536,
            archive_chunk_bytes: 16 * 1024 * 1024 * 1024,
            // Production CDC frames top out at 16 MiB. Keep headroom for zstd
            // overhead without permitting each of the eight concurrent Files
            // verifiers to reserve hundreds of MiB for one hostile frame.
            frame_compressed_bytes: 32 * 1024 * 1024,
            frame_raw_bytes: 32 * 1024 * 1024,
            file_raw_bytes: 1024 * 1024 * 1024 * 1024,
            total_archive_compressed_bytes: 1024 * 1024 * 1024 * 1024,
            total_archive_raw_bytes: 4 * 1024 * 1024 * 1024 * 1024,
            dictionary_bytes: 16 * 1024 * 1024,
        }
    }
}

impl ArtifactVerificationLimits {
    fn validate(&self) -> Result<()> {
        if self.manifest_bytes == 0
            || self.packs == 0
            || self.pack_bytes == 0
            || self.pack_index_bytes == 0
            || self.total_pack_bytes == 0
            || self.git_objects == 0
            || self.commit_bytes == 0
            || self.metadata_bytes == 0
            || self.files == 0
            || self.frames == 0
            || self.fragments == 0
            || self.archive_chunks == 0
            || self.archive_chunk_bytes == 0
            || self.frame_compressed_bytes == 0
            || self.frame_raw_bytes == 0
            || self.file_raw_bytes == 0
            || self.total_archive_compressed_bytes == 0
            || self.total_archive_raw_bytes == 0
            || self.dictionary_bytes == 0
        {
            bail!("artifact verification limits must be nonzero");
        }
        Ok(())
    }

    fn verifier_identity(&self) -> String {
        use sha2::Digest;
        let values = format!(
            "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            self.manifest_bytes,
            self.packs,
            self.pack_bytes,
            self.pack_index_bytes,
            self.total_pack_bytes,
            self.git_objects,
            self.commit_bytes,
            self.metadata_bytes,
            self.files,
            self.frames,
            self.fragments,
            self.archive_chunks,
            self.archive_chunk_bytes,
            self.frame_compressed_bytes,
            self.frame_raw_bytes,
            self.file_raw_bytes,
            self.total_archive_compressed_bytes,
            self.total_archive_raw_bytes,
            self.dictionary_bytes,
            ARTIFACT_MANIFEST_SCHEMA,
        );
        format!(
            "{PRODUCTION_VERIFIER_IDENTITY}:{}",
            hex::encode(Sha256::digest(values))
        )
    }
}

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
    /// Immutable, oldest-to-newest LSM ranges whose disjoint union is the
    /// complete closure of every parent. Empty iff the target is a root.
    /// A level may contain many physical packs; physical download sizing is
    /// deliberately independent from the number of logical levels.
    pub levels: Vec<FullHistoryLevel>,
}

/// One exact immutable history range. Its object set is
/// `reachable(tips) - reachable(base_exclusive)`. The first (cold) level has
/// no exclusions. Later levels chain exactly to the preceding level's tips.
/// Tails begin at tier zero; adjacent compatible equal tiers are recursively
/// compacted into the next tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullHistoryLevel {
    pub tier: u32,
    pub base_exclusive: Vec<String>,
    pub tips: Vec<String>,
    pub packs: Vec<GitPackPair>,
    /// Verifier-authenticated proof that `packs` contain exactly this level's
    /// semantic object set. Reuse validates this small receipt and immutable CAS
    /// descriptors instead of rescanning old pack bytes.
    pub proof: FullHistoryLevelProof,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullHistoryLevelProof {
    pub verifier: String,
    pub object_count: u64,
    pub object_set_digest: String,
    pub seal: String,
}

impl FullHistoryLevelProof {
    pub(crate) fn unsealed() -> Self {
        Self {
            verifier: String::new(),
            object_count: 0,
            object_set_digest: String::new(),
            seal: String::new(),
        }
    }
}

#[derive(Serialize)]
struct HistoryLevelSealPayload<'a> {
    verifier: &'a str,
    tier: u32,
    base_exclusive: &'a [String],
    tips: &'a [String],
    packs: &'a [GitPackPair],
    object_count: u64,
    object_set_digest: &'a str,
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
    /// Git tree entries omitted from worktree archives by design.
    pub gitlinks: Vec<GitlinkEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitlinkEntry {
    /// Raw repository-relative Git path bytes.
    pub path: Vec<u8>,
    pub commit: String,
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
            Self::FullHistory(history) => {
                history
                    .levels
                    .iter()
                    .map(|level| level.packs.len() as u64 * 2)
                    .sum::<u64>()
                    + 1
            }
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
        if key.workspace.trim().is_empty()
            || key.repo.trim().is_empty()
            || key.format_version != ARTIFACT_FORMAT_VERSION
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
        CompletionEvidence::from_manifest(
            ArtifactKey {
                workspace: self.key.workspace.clone(),
                repo: self.key.repo.clone(),
                commit: self.key.commit.clone(),
                kind: self.key.kind,
                format_version: self.key.format_version,
            },
            manifest,
            self.payload.artifact_count(),
        )
    }

    fn validate_envelope(&self) -> Result<()> {
        if self.schema_version != ARTIFACT_MANIFEST_SCHEMA {
            bail!("unsupported artifact manifest schema");
        }
        validate_commit_oid(&self.key.commit)?;
        if self.key.workspace.trim().is_empty()
            || self.key.repo.trim().is_empty()
            || self.key.format_version != ARTIFACT_FORMAT_VERSION
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

struct BoundedWriter<W> {
    inner: W,
    written: u64,
    maximum: u64,
}

impl<W> BoundedWriter<W> {
    fn new(inner: W, maximum: u64) -> Self {
        Self {
            inner,
            written: 0,
            maximum,
        }
    }

    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if verification_cancelled() {
            return Err(std::io::Error::other("artifact verification cancelled"));
        }
        let next = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("bounded writer length overflow"))?;
        if next > self.maximum {
            return Err(std::io::Error::other("bounded writer limit exceeded"));
        }
        self.inner.write_all(bytes)?;
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Clone)]
pub struct CasCompletionVerifier {
    cas: Cas,
    limits: ArtifactVerificationLimits,
    identity: String,
    proof_key: Option<std::sync::Arc<Vec<u8>>>,
    level_scan_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    owned_verify_calls: std::sync::Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    level_scanned_hashes: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl CasCompletionVerifier {
    pub fn new(cas: Cas) -> Self {
        let limits = ArtifactVerificationLimits::default();
        let proof_key = configured_proof_key();
        Self {
            cas,
            identity: verifier_identity(&limits, proof_key.as_deref().map(Vec::as_slice)),
            limits,
            proof_key,
            level_scan_bytes: Default::default(),
            #[cfg(test)]
            owned_verify_calls: Default::default(),
            #[cfg(test)]
            level_scanned_hashes: Default::default(),
        }
    }

    /// Production constructor: normalized scheduling must fail readiness when
    /// the dedicated proof authority is absent or too short.
    pub fn from_env(cas: Cas) -> Result<Self> {
        Self::from_env_with_limits(cas, ArtifactVerificationLimits::default())
    }

    pub fn from_env_with_limits(cas: Cas, limits: ArtifactVerificationLimits) -> Result<Self> {
        let key = std::env::var("RIPCLONE_ARTIFACT_PROOF_KEY")
            .context("RIPCLONE_ARTIFACT_PROOF_KEY must be set for normalized artifacts")?;
        Self::with_limits(cas, limits)?
            .with_proof_key(key.as_bytes())
            .context("RIPCLONE_ARTIFACT_PROOF_KEY must contain at least 32 bytes")
    }

    pub fn with_limits(cas: Cas, limits: ArtifactVerificationLimits) -> Result<Self> {
        limits.validate()?;
        let proof_key = configured_proof_key();
        let identity = verifier_identity(&limits, proof_key.as_deref().map(Vec::as_slice));
        Ok(Self {
            cas,
            limits,
            identity,
            proof_key,
            level_scan_bytes: Default::default(),
            #[cfg(test)]
            owned_verify_calls: Default::default(),
            #[cfg(test)]
            level_scanned_hashes: Default::default(),
        })
    }

    pub fn with_proof_key(mut self, key: &[u8]) -> Result<Self> {
        if key.len() < 32 {
            bail!("artifact proof key must contain at least 32 bytes");
        }
        self.proof_key = Some(std::sync::Arc::new(key.to_vec()));
        self.identity = verifier_identity(&self.limits, Some(key));
        Ok(self)
    }

    #[cfg(test)]
    pub(crate) fn take_level_scan_bytes(&self) -> u64 {
        self.level_scan_bytes
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn take_level_scanned_hashes(&self) -> Vec<String> {
        std::mem::take(&mut *self.level_scanned_hashes.lock().unwrap())
    }

    #[cfg(test)]
    pub(crate) fn take_owned_verify_calls(&self) -> u64 {
        self.owned_verify_calls
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    /// Verify one freshly built/compacted level exactly once and attach a
    /// durable authenticated receipt. Future syncs validate the receipt and CAS
    /// descriptors in O(level-count), without rereading untouched pack bytes.
    pub(crate) fn verify_and_seal_history_level(
        &self,
        level: &mut FullHistoryLevel,
        expected_objects: &[String],
        cancelled: &tokio_util::sync::CancellationToken,
        scratch: &Path,
    ) -> Result<()> {
        let key = self
            .proof_key
            .as_ref()
            .context("artifact proof key is required to seal history levels")?;
        if cancelled.is_cancelled() {
            bail!("history level verification cancelled");
        }
        let previous =
            VERIFICATION_CANCEL.with(|slot| slot.borrow_mut().replace(cancelled.clone()));
        let previous_scratch =
            VERIFICATION_SCRATCH.with(|slot| slot.borrow_mut().replace(scratch.to_path_buf()));
        let _guard = VerificationCancelGuard {
            token: previous,
            scratch: previous_scratch,
        };
        let repo = self.materialize_packs(&level.packs)?;
        let scanned = level.packs.iter().try_fold(0u64, |total, pair| {
            total
                .checked_add(pair.pack.len)
                .and_then(|value| value.checked_add(pair.index.len))
                .context("history level scan byte overflow")
        })?;
        self.level_scan_bytes
            .fetch_add(scanned, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        self.level_scanned_hashes.lock().unwrap().extend(
            level
                .packs
                .iter()
                .flat_map(|pair| [pair.pack.hash.clone(), pair.index.hash.clone()]),
        );
        let actual = packed_object_ids(repo.path(), self.limits.git_objects)?;
        let mut expected = expected_objects.to_vec();
        expected.sort();
        if expected.windows(2).any(|pair| pair[0] == pair[1]) {
            bail!("expected history level object set contains duplicates");
        }
        if actual != expected {
            bail!("history level packs do not match expected exact object set");
        }
        let object_set_digest = object_set_digest(&expected);
        let verifier = self.identity.clone();
        let seal = history_level_seal(
            key,
            &verifier,
            level,
            expected.len() as u64,
            &object_set_digest,
        )?;
        level.proof = FullHistoryLevelProof {
            verifier,
            object_count: expected.len() as u64,
            object_set_digest,
            seal,
        };
        Ok(())
    }

    fn verify_history_level_receipt(&self, level: &FullHistoryLevel) -> Result<()> {
        // This O(levels) path relies on immutable content addressing after the
        // initial exact scan. Downloaders still hash every CAS object, and an
        // independent storage scrub must quarantine same-length at-rest damage;
        // routine sync never trades back to O(total-history) rescans.
        let key = self
            .proof_key
            .as_ref()
            .context("artifact proof key is required to verify history receipt")?;
        if level.proof.verifier != self.identity
            || level.proof.object_count == 0
            || !is_sha256(&level.proof.object_set_digest)
            || !is_sha256(&level.proof.seal)
        {
            bail!("history level proof envelope is invalid");
        }
        let expected = history_level_seal(
            key,
            &level.proof.verifier,
            level,
            level.proof.object_count,
            &level.proof.object_set_digest,
        )?;
        if !constant_time_eq(expected.as_bytes(), level.proof.seal.as_bytes()) {
            bail!("history level proof authentication failed");
        }
        for pair in &level.packs {
            for (role, blob) in [("pack", &pair.pack), ("index", &pair.index)] {
                Cas::validate_artifact_id(&blob.hash)?;
                let actual = std::fs::metadata(self.cas.path(&blob.hash))
                    .with_context(|| format!("stat proved history {role}"))?
                    .len();
                if actual != blob.len {
                    bail!("proved history {role} length mismatch");
                }
            }
        }
        Ok(())
    }

    pub fn verify_manifest(
        &self,
        key: &ArtifactKey,
        manifest_hash: &str,
        artifact_count: u64,
    ) -> Result<ArtifactManifest> {
        Cas::validate_artifact_id(manifest_hash).context("invalid artifact manifest CAS id")?;
        let bytes = self.read_hash_bounded(
            manifest_hash,
            self.limits.manifest_bytes,
            "artifact manifest",
        )?;
        let manifest: ArtifactManifest =
            serde_json::from_slice(&bytes).context("decode typed artifact manifest")?;
        if serde_json::to_vec(&manifest)? != bytes {
            bail!("artifact manifest JSON is not canonical");
        }
        drop(bytes);
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

    /// Cancellation-aware verifier entrypoint for lease-owned builders. The
    /// token is scoped to this thread and observed by streamed CAS writes and
    /// every verifier Git child, which is killed and reaped on lease loss.
    pub fn verify_manifest_cancelled(
        &self,
        key: &ArtifactKey,
        manifest_hash: &str,
        artifact_count: u64,
        cancelled: &tokio_util::sync::CancellationToken,
    ) -> Result<ArtifactManifest> {
        self.verify_manifest_cancelled_in_scratch(
            key,
            manifest_hash,
            artifact_count,
            cancelled,
            None,
        )
    }

    pub fn verify_manifest_cancelled_in_scratch(
        &self,
        key: &ArtifactKey,
        manifest_hash: &str,
        artifact_count: u64,
        cancelled: &tokio_util::sync::CancellationToken,
        scratch: Option<&Path>,
    ) -> Result<ArtifactManifest> {
        if cancelled.is_cancelled() {
            bail!("artifact verification cancelled");
        }
        let previous =
            VERIFICATION_CANCEL.with(|slot| slot.borrow_mut().replace(cancelled.clone()));
        let previous_scratch = VERIFICATION_SCRATCH.with(|slot| {
            std::mem::replace(&mut *slot.borrow_mut(), scratch.map(Path::to_path_buf))
        });
        let _guard = VerificationCancelGuard {
            token: previous,
            scratch: previous_scratch,
        };
        let result = self.verify_manifest(key, manifest_hash, artifact_count);
        if cancelled.is_cancelled() {
            bail!("artifact verification cancelled");
        }
        result
    }

    fn read_hash_bounded(&self, hash: &str, maximum: u64, role: &str) -> Result<Vec<u8>> {
        Cas::validate_artifact_id(hash).with_context(|| format!("invalid {role} CAS id"))?;
        let mut output = BoundedWriter::new(Vec::new(), maximum);
        self.cas
            .copy_to_writer_verified(hash, &mut output)
            .with_context(|| format!("stream and verify {role}"))?;
        Ok(output.into_inner())
    }

    fn read_small_blob(&self, blob: &CasBlob, maximum: u64, role: &str) -> Result<Vec<u8>> {
        if blob.len > maximum {
            bail!("{role} exceeds verifier limit");
        }
        let bytes = self.read_hash_bounded(&blob.hash, maximum, role)?;
        if bytes.len() as u64 != blob.len {
            bail!("{role} CAS length mismatch");
        }
        Ok(bytes)
    }

    fn stream_blob_to(&self, blob: &CasBlob, maximum: u64, role: &str, path: &Path) -> Result<()> {
        Cas::validate_artifact_id(&blob.hash).with_context(|| format!("invalid {role} CAS id"))?;
        if blob.len > maximum {
            bail!("{role} exceeds verifier limit");
        }
        let on_disk = std::fs::metadata(self.cas.path(&blob.hash))
            .with_context(|| format!("stat {role} CAS object"))?
            .len();
        if on_disk != blob.len {
            bail!("{role} CAS length mismatch");
        }
        let output =
            std::fs::File::create(path).with_context(|| format!("create streamed {role}"))?;
        let mut output = BoundedWriter::new(output, blob.len);
        let actual = self
            .cas
            .copy_to_writer_verified(&blob.hash, &mut output)
            .with_context(|| format!("stream and verify {role}"))?;
        if actual != blob.len {
            bail!("{role} CAS length mismatch");
        }
        output.into_inner().sync_all()?;
        Ok(())
    }

    fn materialize_packs(&self, packs: &[GitPackPair]) -> Result<tempfile::TempDir> {
        if packs.len() > self.limits.packs {
            bail!("Git pack count exceeds verifier limit");
        }
        let mut descriptors = HashSet::with_capacity(packs.len().saturating_mul(2));
        let mut total = 0u64;
        for pair in packs {
            if !descriptors.insert(pair.pack.hash.as_str())
                || !descriptors.insert(pair.index.hash.as_str())
            {
                bail!("duplicate Git pack descriptor");
            }
            if pair.pack.len > self.limits.pack_bytes
                || pair.index.len > self.limits.pack_index_bytes
            {
                bail!("Git pack pair exceeds per-object verifier limit");
            }
            total = total
                .checked_add(pair.pack.len)
                .and_then(|value| value.checked_add(pair.index.len))
                .context("Git pack aggregate length overflow")?;
        }
        if total > self.limits.total_pack_bytes {
            bail!("Git pack aggregate exceeds verifier limit");
        }
        let repo = verification_tempdir()?;
        git(repo.path(), &["init", "--quiet"])?;
        let pack_dir = repo.path().join(".git/objects/pack");
        std::fs::create_dir_all(&pack_dir)?;
        for (index, pair) in packs.iter().enumerate() {
            let incoming_pack = repo.path().join(format!("incoming-{index}.pack"));
            self.stream_blob_to(
                &pair.pack,
                self.limits.pack_bytes,
                "Git pack",
                &incoming_pack,
            )?;
            if pair.pack.len < 20 {
                bail!("Git pack is too short");
            }
            let mut pack_file = std::fs::File::open(&incoming_pack)?;
            pack_file.seek(SeekFrom::End(-20))?;
            let mut trailer = [0u8; 20];
            pack_file.read_exact(&mut trailer)?;
            let basename = format!("pack-{}", hex::encode(trailer));
            let pack_path = pack_dir.join(format!("{basename}.pack"));
            let index_path = pack_dir.join(format!("{basename}.idx"));
            if pack_path.exists() || index_path.exists() {
                bail!("duplicate Git pack identity");
            }
            std::fs::rename(incoming_pack, &pack_path)?;
            self.stream_blob_to(
                &pair.index,
                self.limits.pack_index_bytes,
                "Git pack index",
                &index_path,
            )?;
        }
        for entry in std::fs::read_dir(&pack_dir)? {
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
        if head.packs.is_empty() || head.packs.len() > self.limits.packs {
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
        self.stream_blob_to(
            &head.prebuilt_index,
            self.limits.pack_index_bytes,
            "Head prebuilt index",
            &repo.path().join(".git/index"),
        )?;
        let actual_tree = git(repo.path(), &["write-tree"])?;
        let expected_tree = git(repo.path(), &["rev-parse", &format!("{commit}^{{tree}}")])?;
        if actual_tree != expected_tree {
            bail!("Head prebuilt index does not match exact target tree");
        }
        compare_exact_object_sets(repo.path(), &[commit], self.limits.git_objects)?;
        Ok(())
    }

    fn verify_full_history(&self, commit: &str, history: &FullHistoryArtifact) -> Result<()> {
        let pack_count = history
            .levels
            .iter()
            .try_fold(0usize, |count, level| count.checked_add(level.packs.len()))
            .context("FullHistory pack count overflow")?;
        if history.target_commit_object.len > self.limits.commit_bytes
            || history.levels.len() > self.limits.packs
            || pack_count > self.limits.packs
        {
            bail!("FullHistory artifact exceeds verifier limits");
        }
        let commit_bytes = self.read_small_blob(
            &history.target_commit_object,
            self.limits.commit_bytes,
            "history commit anchor",
        )?;
        let parsed = parse_exact_commit(commit, &commit_bytes)?;
        drop(commit_bytes);
        if parsed.parents.is_empty() {
            if !history.levels.is_empty() {
                bail!("root commit history must be empty");
            }
            return Ok(());
        }
        if history.levels.is_empty() {
            bail!("non-root commit history contains no levels");
        }
        let mut expected_tips = parsed.parents.clone();
        expected_tips.sort();
        expected_tips.dedup();
        let mut previous_tips: Option<&[String]> = None;
        let mut previous_tier = FULL_HISTORY_BASE_TIER + 1;
        for (index, level) in history.levels.iter().enumerate() {
            validate_canonical_oids(&level.tips, "history level tips")?;
            validate_canonical_oids(&level.base_exclusive, "history level exclusions")?;
            if level.tips.is_empty() || level.packs.is_empty() {
                bail!("history level must contain tips and packs");
            }
            if index == 0 {
                if level.tier != FULL_HISTORY_BASE_TIER || !level.base_exclusive.is_empty() {
                    bail!("history cold level has noncanonical range or tier");
                }
            } else {
                if level.tier >= previous_tier || level.tier >= FULL_HISTORY_BASE_TIER {
                    bail!("history tail tiers are not canonically compacted");
                }
                if Some(level.base_exclusive.as_slice()) != previous_tips {
                    bail!("history level range is not adjacent to its predecessor");
                }
            }
            previous_tips = Some(level.tips.as_slice());
            previous_tier = level.tier;
            self.verify_history_level_receipt(level)?;
        }
        if history.levels.last().map(|level| level.tips.as_slice())
            != Some(expected_tips.as_slice())
        {
            bail!("history levels do not terminate at the target parents");
        }

        Ok(())
    }

    fn verify_files(&self, commit: &str, files: &FilesArtifact) -> Result<()> {
        if files.target_commit_object.len > self.limits.commit_bytes
            || files.archive_chunks.len() > self.limits.archive_chunks
        {
            bail!("Files artifact exceeds verifier limits");
        }
        let commit_bytes = self.read_small_blob(
            &files.target_commit_object,
            self.limits.commit_bytes,
            "files commit anchor",
        )?;
        let parsed = parse_exact_commit(commit, &commit_bytes)?;
        drop(commit_bytes);
        let metadata_bytes = self.read_small_blob(
            &files.metadata,
            self.limits.metadata_bytes,
            "files metadata",
        )?;
        preflight_metadata_counts(&metadata_bytes, &self.limits)?;
        let metadata =
            MetadataChunk::decode(metadata_bytes.as_slice()).context("decode files metadata")?;
        metadata
            .validate_geometry()
            .context("validate files metadata geometry")?;
        if metadata.encode_to_vec() != metadata_bytes {
            bail!("Files metadata is non-canonical or contains unknown fields");
        }
        drop(metadata_bytes);
        if !metadata.skeleton_pack.is_empty()
            || !metadata.skeleton_idx.is_empty()
            || !metadata.prebuilt_index.is_empty()
        {
            bail!("Files metadata contains ambiguous embedded Git artifacts");
        }
        let fragment_count = metadata.files.iter().try_fold(0usize, |total, file| {
            total
                .checked_add(file.fragments.len())
                .context("Files fragment count overflow")
        })?;
        let logical_entries = metadata
            .files
            .len()
            .checked_add(files.gitlinks.len())
            .context("Files logical entry count overflow")?;
        if logical_entries > self.limits.files
            || metadata.frames.len() > self.limits.frames
            || fragment_count > self.limits.fragments
        {
            bail!("Files metadata count exceeds verifier limit");
        }
        let expected_chunks = metadata.frames.iter().try_fold(0usize, |maximum, frame| {
            let index = usize::try_from(frame.chunk_index)
                .context("archive chunk index is not addressable")?;
            let count = index
                .checked_add(1)
                .context("archive chunk index overflow")?;
            Ok::<_, anyhow::Error>(maximum.max(count))
        })?;
        if files.archive_chunks.len() != expected_chunks {
            bail!("Files archive chunk table is not exact");
        }
        let dictionary = files
            .zstd_dictionary
            .as_ref()
            .map(|blob| {
                self.read_small_blob(blob, self.limits.dictionary_bytes, "files zstd dictionary")
            })
            .transpose()?;
        validate_dictionary_policy(&metadata, dictionary.as_deref())?;

        let scratch = verification_tempdir()?;
        let chunk_dir = scratch.path().join("chunks");
        std::fs::create_dir(&chunk_dir)?;
        let mut total_compressed = 0u64;
        for (index, chunk) in files.archive_chunks.iter().enumerate() {
            total_compressed = total_compressed
                .checked_add(chunk.len)
                .context("archive aggregate compressed length overflow")?;
            if total_compressed > self.limits.total_archive_compressed_bytes {
                bail!("archive aggregate compressed bytes exceed verifier limit");
            }
            self.stream_blob_to(
                chunk,
                self.limits.archive_chunk_bytes,
                "files archive chunk",
                &chunk_dir.join(index.to_string()),
            )?;
        }

        let repo = scratch.path().join("repo");
        std::fs::create_dir(&repo)?;
        git(&repo, &["init", "--quiet"])?;
        reconstruct_files_to_index(
            &metadata,
            &chunk_dir,
            dictionary.as_deref(),
            &files.gitlinks,
            &repo,
            &self.limits,
        )?;
        let actual_tree = git(&repo, &["write-tree"])?;
        if actual_tree != parsed.tree {
            bail!("Files archive does not reconstruct exact target tree");
        }
        Ok(())
    }
}

impl CompletionVerifier for CasCompletionVerifier {
    fn identity(&self) -> &str {
        &self.identity
    }

    fn verify(&self, claim: &ClaimedArtifact, evidence: &CompletionEvidence) -> Result<()> {
        if evidence.key() != &claim.record.key {
            bail!("completion evidence does not match claimed artifact key");
        }
        self.verify_manifest(
            evidence.key(),
            evidence.manifest(),
            evidence.artifact_count(),
        )?;
        Ok(())
    }

    fn verify_owned(
        &self,
        claim: &ClaimedArtifact,
        evidence: &CompletionEvidence,
        context: &crate::artifact_scheduler::ExecutionContext,
    ) -> Result<()> {
        #[cfg(test)]
        self.owned_verify_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if evidence.key() != &claim.record.key {
            bail!("completion evidence does not match claimed artifact key");
        }
        self.verify_manifest_cancelled_in_scratch(
            evidence.key(),
            evidence.manifest(),
            evidence.artifact_count(),
            &context.cancelled,
            Some(&context.scratch),
        )?;
        Ok(())
    }
}

struct ParsedCommit {
    tree: String,
    parents: Vec<String>,
}

fn parse_exact_commit(expected: &str, bytes: &[u8]) -> Result<ParsedCommit> {
    validate_commit_oid(expected)?;
    if git_object_oid("commit", bytes) != expected {
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

fn validate_dictionary_policy(metadata: &MetadataChunk, dictionary: Option<&[u8]>) -> Result<()> {
    if metadata.frames.is_empty() && dictionary.is_some() {
        bail!("Files dictionary is forbidden when there are no frames");
    }
    let dictionary_id = dictionary.and_then(zstd::zstd_safe::get_dict_id_from_dict);
    if dictionary.is_some() && dictionary_id.is_none() {
        bail!("Files dictionary has no canonical zstd dictionary id");
    }
    Ok(())
}

/// Count repeated protobuf messages before prost allocates their backing
/// vectors. A tiny encoding such as repeated zero-length `FileEntry` messages
/// otherwise amplifies a bounded metadata blob into an unbounded heap
/// allocation before the post-decode limits can run.
fn preflight_metadata_counts(bytes: &[u8], limits: &ArtifactVerificationLimits) -> Result<()> {
    let mut input = bytes;
    let mut frames = 0usize;
    let mut files = 0usize;
    let mut fragments = 0usize;
    while !input.is_empty() {
        let (field, value) = take_protobuf_field(&mut input)?;
        match (field, value) {
            (1..=3, ProtobufValue::Bytes(_)) => {}
            (4, ProtobufValue::Bytes(_)) => {
                frames = frames
                    .checked_add(1)
                    .context("Files frame count overflow")?;
                if frames > limits.frames {
                    bail!("Files frame count exceeds verifier limit");
                }
            }
            (5, ProtobufValue::Bytes(file)) => {
                files = files.checked_add(1).context("Files file count overflow")?;
                if files > limits.files {
                    bail!("Files file count exceeds verifier limit");
                }
                let mut file = file;
                while !file.is_empty() {
                    let (file_field, file_value) = take_protobuf_field(&mut file)?;
                    if file_field == 4 && matches!(file_value, ProtobufValue::Bytes(_)) {
                        fragments = fragments
                            .checked_add(1)
                            .context("Files fragment count overflow")?;
                        if fragments > limits.fragments {
                            bail!("Files fragment count exceeds verifier limit");
                        }
                    }
                }
            }
            // These are the only canonical top-level MetadataChunk fields.
            // Rejecting anything else here is consistent with the later
            // encode-byte equality check, which also rejects unknown fields.
            _ => bail!("Files metadata contains an unknown field or wire type"),
        }
    }
    Ok(())
}

enum ProtobufValue<'a> {
    Varint,
    Fixed64,
    Bytes(&'a [u8]),
    Fixed32,
}

fn take_protobuf_field<'a>(input: &mut &'a [u8]) -> Result<(u64, ProtobufValue<'a>)> {
    let key = take_protobuf_varint(input)?;
    let field = key >> 3;
    if field == 0 {
        bail!("protobuf field number zero is invalid");
    }
    let value = match key & 7 {
        0 => {
            take_protobuf_varint(input)?;
            ProtobufValue::Varint
        }
        1 => {
            take_protobuf_bytes(input, 8)?;
            ProtobufValue::Fixed64
        }
        2 => {
            let len = take_protobuf_varint(input)?;
            let len = usize::try_from(len).context("protobuf field length is not addressable")?;
            ProtobufValue::Bytes(take_protobuf_bytes(input, len)?)
        }
        5 => {
            take_protobuf_bytes(input, 4)?;
            ProtobufValue::Fixed32
        }
        _ => bail!("unsupported protobuf wire type"),
    };
    Ok((field, value))
}

fn take_protobuf_varint(input: &mut &[u8]) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = input.split_first().context("truncated protobuf varint")?;
        *input = rest;
        if shift == 63 && byte > 1 {
            bail!("protobuf varint overflow");
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("protobuf varint overflow")
}

fn take_protobuf_bytes<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if len > input.len() {
        bail!("protobuf field extends past metadata");
    }
    let (value, rest) = input.split_at(len);
    *input = rest;
    Ok(value)
}

fn reconstruct_files_to_index(
    metadata: &MetadataChunk,
    chunk_dir: &Path,
    dictionary: Option<&[u8]>,
    gitlinks: &[GitlinkEntry],
    repo: &Path,
    limits: &ArtifactVerificationLimits,
) -> Result<()> {
    let frame_dir = repo
        .parent()
        .context("verification repo has no parent")?
        .join("frames");
    let file_dir = repo
        .parent()
        .context("verification repo has no parent")?
        .join("files");
    std::fs::create_dir(&frame_dir)?;
    std::fs::create_dir(&file_dir)?;

    let mut chunk_ranges = vec![
        Vec::new();
        metadata.frames.iter().try_fold(0usize, |maximum, frame| {
            let index = usize::try_from(frame.chunk_index)
                .context("archive chunk index is not addressable")?;
            let count = index
                .checked_add(1)
                .context("archive chunk index overflow")?;
            Ok::<_, anyhow::Error>(maximum.max(count))
        })?
    ];
    let mut fragment_ranges = vec![Vec::new(); metadata.frames.len()];
    let mut total_raw = 0u64;
    for file in &metadata.files {
        let file_len = file.fragments.iter().try_fold(0u64, |total, fragment| {
            total
                .checked_add(fragment.raw_len as u64)
                .context("file raw length overflow")
        })?;
        if file_len > limits.file_raw_bytes {
            bail!("file raw bytes exceed verifier limit");
        }
        if file_len == 0 && (file.fragments.len() != 1 || file.fragments[0].raw_len != 0) {
            bail!("empty file must have exactly one empty fragment");
        }
        if file_len > 0 && file.fragments.iter().any(|fragment| fragment.raw_len == 0) {
            bail!("non-empty file contains a redundant empty fragment");
        }
        for fragment in &file.fragments {
            let frame_index = fragment.frame_index as usize;
            let end = fragment
                .frame_offset
                .checked_add(fragment.raw_len)
                .context("file fragment bounds overflow")?;
            fragment_ranges
                .get_mut(frame_index)
                .context("file fragment references missing frame")?
                .push((fragment.frame_offset, end));
        }
    }
    for (index, frame) in metadata.frames.iter().enumerate() {
        if frame.raw_len as u64 > limits.frame_raw_bytes
            || frame.compressed_len as u64 > limits.frame_compressed_bytes
        {
            bail!("archive frame exceeds verifier limit");
        }
        total_raw = total_raw
            .checked_add(frame.raw_len as u64)
            .context("archive aggregate raw length overflow")?;
        if total_raw > limits.total_archive_raw_bytes {
            bail!("archive aggregate raw bytes exceed verifier limit");
        }
        let start = frame.chunk_offset;
        let end = start
            .checked_add(frame.compressed_len as u64)
            .context("compressed frame bounds overflow")?;
        let chunk_index =
            usize::try_from(frame.chunk_index).context("archive chunk index is not addressable")?;
        chunk_ranges
            .get_mut(chunk_index)
            .context("archive frame references missing chunk")?
            .push((start, end));

        let compressed = open_file_range(
            &chunk_dir.join(chunk_index.to_string()),
            start,
            frame.compressed_len as u64,
        )?;
        const ZSTD_FRAME_HEADER_MAX: usize = 18;
        let mut header_reader = open_file_range(
            &chunk_dir.join(chunk_index.to_string()),
            start,
            frame.compressed_len as u64,
        )?;
        let mut header = [0u8; ZSTD_FRAME_HEADER_MAX];
        let header_len =
            usize::try_from(u64::from(frame.compressed_len).min(ZSTD_FRAME_HEADER_MAX as u64))?;
        header_reader.read_exact(&mut header[..header_len])?;
        let frame_dict_id = zstd::zstd_safe::get_dict_id_from_frame(&header[..header_len]);
        let expected_dict_id = dictionary.and_then(zstd::zstd_safe::get_dict_id_from_dict);
        if frame_dict_id != expected_dict_id {
            bail!("archive frame dictionary policy mismatch");
        }
        let output_path = frame_dir.join(index.to_string());
        let mut output = std::fs::File::create(&output_path)?;
        let written = match dictionary {
            Some(dict) => {
                let mut decoder =
                    zstd::stream::Decoder::with_dictionary(BufReader::new(compressed), dict)?;
                decoder.window_log_max(25)?;
                copy_bounded(decoder, &mut output, frame.raw_len as u64)?
            }
            None => {
                let mut decoder = zstd::stream::Decoder::new(compressed)?;
                decoder.window_log_max(25)?;
                copy_bounded(decoder, &mut output, frame.raw_len as u64)?
            }
        };
        if written != frame.raw_len as u64 {
            bail!("archive frame raw length mismatch");
        }
    }

    for (chunk_index, ranges) in chunk_ranges.iter_mut().enumerate() {
        ranges.sort_unstable();
        let mut cursor = 0u64;
        for &(start, end) in ranges.iter() {
            if start != cursor || end < start {
                bail!("archive chunk {chunk_index} has gaps or overlapping frames");
            }
            cursor = end;
        }
        if cursor != std::fs::metadata(chunk_dir.join(chunk_index.to_string()))?.len() {
            bail!("archive chunk {chunk_index} contains unreferenced bytes");
        }
    }
    for (frame_index, ranges) in fragment_ranges.iter_mut().enumerate() {
        ranges.sort_unstable();
        let frame_len = metadata.frames[frame_index].raw_len;
        if frame_len == 0 {
            if ranges.is_empty() || ranges.iter().any(|range| *range != (0, 0)) {
                bail!("empty frame must be referenced only by empty fragments");
            }
            continue;
        }
        let mut cursor = 0u32;
        for &(start, end) in ranges.iter() {
            // Empty files may point at a boundary inside a non-empty CDC
            // frame. They consume no bytes and therefore do not participate
            // in the frame's exact positive-byte partition.
            if start == end {
                continue;
            }
            if start != cursor || end <= start {
                bail!("frame {frame_index} raw bytes have gaps or overlap");
            }
            cursor = end;
        }
        if cursor != frame_len {
            bail!("frame {frame_index} contains unreferenced raw bytes");
        }
    }

    let mut paths = HashSet::<Vec<u8>>::with_capacity(metadata.files.len() + gitlinks.len());
    for (file_index, entry) in metadata.files.iter().enumerate() {
        validate_artifact_path(&entry.path)?;
        if !paths.insert(entry.path.clone()) {
            bail!("Files artifact contains duplicate paths");
        }
        let file_len: u64 = entry
            .fragments
            .iter()
            .map(|fragment| fragment.raw_len as u64)
            .sum();
        let content_path = file_dir.join(file_index.to_string());
        let mut content = std::fs::File::create(&content_path)?;
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {file_len}\0").as_bytes());
        for fragment in &entry.fragments {
            let mut frame = std::fs::File::open(frame_dir.join(fragment.frame_index.to_string()))?;
            frame.seek(SeekFrom::Start(fragment.frame_offset as u64))?;
            let copied =
                copy_with_observer(frame.take(fragment.raw_len as u64), &mut content, |bytes| {
                    hasher.update(bytes)
                })?;
            if copied != fragment.raw_len as u64 {
                bail!("file fragment is truncated");
            }
        }
        content.sync_all()?;
        let oid = hex::encode(hasher.finalize());
        if entry.blob_sha1.as_slice() != hex::decode(&oid)? {
            bail!("Files metadata blob identity mismatch");
        }
        let stored = git_hash_object(repo, &content_path)?;
        if stored != oid {
            bail!("Git stored blob identity mismatch");
        }
        git_update_index(repo, entry.mode, &oid, &entry.path)?;
    }
    for gitlink in gitlinks {
        validate_artifact_path(&gitlink.path)?;
        validate_commit_oid(&gitlink.commit).context("gitlink commit is invalid")?;
        if !paths.insert(gitlink.path.clone()) {
            bail!("Files artifact contains duplicate paths");
        }
        git_update_index(repo, 0o160000, &gitlink.commit, &gitlink.path)?;
    }
    Ok(())
}

fn validate_artifact_path(bytes: &[u8]) -> Result<()> {
    let path = crate::fsutil::path_from_bytes(bytes);
    crate::fsutil::validate_relative_path(path)?;
    if path.components().any(|component| {
        matches!(component, std::path::Component::Normal(name) if name.as_encoded_bytes().eq_ignore_ascii_case(b".git"))
    }) {
        bail!("Files artifact path enters Git administrative namespace");
    }
    Ok(())
}

fn open_file_range(path: &Path, start: u64, len: u64) -> Result<std::io::Take<std::fs::File>> {
    let mut file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    if start > file_len || len > file_len - start {
        bail!("archive frame extends past chunk");
    }
    file.seek(SeekFrom::Start(start))?;
    Ok(file.take(len))
}

fn copy_bounded<R: Read, W: Write>(reader: R, writer: &mut W, expected: u64) -> Result<u64> {
    let mut limited = reader.take(expected.saturating_add(1));
    let copied = std::io::copy(&mut limited, writer)?;
    if copied > expected {
        bail!("decompressed frame exceeds declared raw length");
    }
    Ok(copied)
}

fn copy_with_observer<R: Read, W: Write, F: FnMut(&[u8])>(
    mut reader: R,
    writer: &mut W,
    mut observer: F,
) -> Result<u64> {
    let mut buffer = [0u8; 1024 * 1024];
    let mut total = 0u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        observer(&buffer[..read]);
        total = total
            .checked_add(read as u64)
            .context("copy length overflow")?;
    }
    Ok(total)
}

fn git_object_oid(kind: &str, bytes: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(format!("{kind} {}\0", bytes.len()).as_bytes());
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn validate_canonical_oids(oids: &[String], role: &str) -> Result<()> {
    let mut previous: Option<&str> = None;
    for oid in oids {
        Cas::validate_object_id(oid).with_context(|| format!("invalid {role} object id"))?;
        if previous.is_some_and(|value| value >= oid.as_str()) {
            bail!("{role} must be sorted and duplicate-free");
        }
        previous = Some(oid);
    }
    Ok(())
}

fn compare_exact_object_sets(repo: &Path, revisions: &[&str], maximum: u64) -> Result<()> {
    let scratch = verification_tempdir()?;
    let packed = scratch.path().join("packed");
    let reachable = scratch.path().join("reachable");
    let mut packed_output = std::io::BufWriter::new(std::fs::File::create(&packed)?);
    let mut count = 0u64;
    for entry in std::fs::read_dir(repo.join(".git/objects/pack"))? {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "idx") {
            continue;
        }
        let mut command = Command::new("git");
        configure_git_command(&mut command);
        let mut child = command
            .arg("-C")
            .arg(repo)
            .args(["verify-pack", "-v"])
            .arg(&path)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .context("capture git verify-pack output")?;
        let enumeration = (|| -> Result<()> {
            for line in BufReader::new(stdout).lines() {
                if verification_cancelled() {
                    bail!("artifact verification cancelled");
                }
                let line = line?;
                let Some(oid) = line.split_ascii_whitespace().next() else {
                    continue;
                };
                if is_oid(oid) {
                    count = count.checked_add(1).context("Git object count overflow")?;
                    if count > maximum {
                        bail!("Git object count exceeds verifier limit");
                    }
                    writeln!(packed_output, "{oid}")?;
                }
            }
            Ok(())
        })();
        if enumeration.is_err() {
            let _ = child.kill();
        }
        let status = child.wait()?;
        enumeration?;
        if !status.success() {
            bail!("Git pack object enumeration failed");
        }
    }
    packed_output.flush()?;

    let mut command = Command::new("git");
    configure_git_command(&mut command);
    let mut child = command
        .arg("-C")
        .arg(repo)
        .args([
            "rev-list",
            "--objects",
            "--no-object-names",
            "--end-of-options",
        ])
        .args(revisions)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let stdout = child.stdout.take().context("capture git rev-list output")?;
    let mut reachable_output = std::io::BufWriter::new(std::fs::File::create(&reachable)?);
    let mut reachable_count = 0u64;
    let enumeration = (|| -> Result<()> {
        for line in BufReader::new(stdout).lines() {
            if verification_cancelled() {
                bail!("artifact verification cancelled");
            }
            let oid = line?;
            validate_commit_oid(&oid).context("Git closure emitted invalid object id")?;
            reachable_count = reachable_count
                .checked_add(1)
                .context("Git reachable object count overflow")?;
            if reachable_count > maximum {
                bail!("Git reachable object count exceeds verifier limit");
            }
            writeln!(reachable_output, "{oid}")?;
        }
        Ok(())
    })();
    if enumeration.is_err() {
        let _ = child.kill();
    }
    let status = child.wait()?;
    enumeration?;
    if !status.success() {
        bail!("Git closure enumeration failed");
    }
    reachable_output.flush()?;

    external_sort(&packed)?;
    external_sort(&reachable)?;
    let mut packed_lines = BufReader::new(std::fs::File::open(&packed)?).lines();
    let mut reachable_lines = BufReader::new(std::fs::File::open(&reachable)?).lines();
    let mut previous = None;
    loop {
        if verification_cancelled() {
            bail!("artifact verification cancelled");
        }
        let packed_oid = packed_lines.next().transpose()?;
        if packed_oid.is_some() && packed_oid == previous {
            bail!("duplicate Git object appears across artifact packs");
        }
        let reachable_oid = reachable_lines.next().transpose()?;
        if packed_oid != reachable_oid {
            bail!("artifact packs do not contain the exact reachable object set");
        }
        if packed_oid.is_none() {
            return Ok(());
        }
        previous = packed_oid;
    }
}

fn external_sort(path: &Path) -> Result<()> {
    let path_value = std::env::var_os("PATH").unwrap_or_default();
    let mut child = Command::new("sort")
        .env_clear()
        .env("PATH", path_value)
        .env("LC_ALL", "C")
        .arg("-o")
        .arg(path)
        .arg(path)
        .spawn()?;
    let status = loop {
        if verification_cancelled() {
            child.kill()?;
            let _ = child.wait();
            bail!("artifact verification cancelled");
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    if !status.success() {
        bail!("external object-id sort failed");
    }
    Ok(())
}

fn packed_object_ids(repo: &Path, maximum: u64) -> Result<Vec<String>> {
    let mut objects = Vec::new();
    for entry in std::fs::read_dir(repo.join(".git/objects/pack"))? {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "idx") {
            continue;
        }
        let mut command = Command::new("git");
        configure_git_command(&mut command);
        let mut child = command
            .arg("-C")
            .arg(repo)
            .args(["verify-pack", "-v"])
            .arg(path)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = child.stdout.take().context("capture verify-pack output")?;
        let enumeration = (|| -> Result<()> {
            for line in BufReader::new(stdout).lines() {
                if verification_cancelled() {
                    bail!("history level verification cancelled");
                }
                let line = line?;
                let Some(oid) = line.split_ascii_whitespace().next() else {
                    continue;
                };
                if is_oid(oid) {
                    if objects.len() as u64 >= maximum {
                        bail!("history level object count exceeds verifier limit");
                    }
                    objects.push(oid.to_owned());
                }
            }
            Ok(())
        })();
        if enumeration.is_err() {
            let _ = child.kill();
        }
        let status = child.wait()?;
        enumeration?;
        if !status.success() {
            bail!("history level pack enumeration failed");
        }
    }
    objects.sort();
    if objects.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("duplicate object appears across history level packs");
    }
    Ok(objects)
}

fn object_set_digest(objects: &[String]) -> String {
    use sha2::Digest;
    let mut digest = Sha256::new();
    for oid in objects {
        digest.update(oid.as_bytes());
        digest.update(b"\n");
    }
    hex::encode(digest.finalize())
}

fn history_level_seal(
    key: &[u8],
    verifier: &str,
    level: &FullHistoryLevel,
    object_count: u64,
    object_set_digest: &str,
) -> Result<String> {
    let payload = serde_json::to_vec(&HistoryLevelSealPayload {
        verifier,
        tier: level.tier,
        base_exclusive: &level.base_exclusive,
        tips: &level.tips,
        packs: &level.packs,
        object_count,
        object_set_digest,
    })?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key)?;
    mac.update(&payload);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0
}

fn is_oid(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn git_hash_object(repo: &Path, path: &Path) -> Result<String> {
    let path = path
        .to_str()
        .context("verification scratch path is not UTF-8")?;
    let oid = git(repo, &["hash-object", "-w", "--", path])?;
    validate_commit_oid(&oid).context("Git blob storage emitted invalid object id")?;
    Ok(oid)
}

fn git_update_index(repo: &Path, mode: u32, oid: &str, path: &[u8]) -> Result<()> {
    let mut command = Command::new("git");
    configure_git_command(&mut command);
    let mut child = command
        .arg("-C")
        .arg(repo)
        .args(["update-index", "-z", "--add", "--index-info"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut stdin = child.stdin.take().context("open git update-index input")?;
    write!(stdin, "{mode:o} {oid}\t")?;
    stdin.write_all(path)?;
    stdin.write_all(&[0])?;
    drop(stdin);
    let status = loop {
        if verification_cancelled() {
            child.kill()?;
            let _ = child.wait();
            bail!("artifact verification cancelled");
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    if !status.success() {
        bail!("Git index entry construction failed");
    }
    Ok(())
}

fn git(repo: &Path, args: &[&str]) -> Result<String> {
    const MAX_GIT_DIAGNOSTIC_BYTES: u64 = 1024 * 1024;
    let scratch = verification_tempdir()?;
    let stdout_path = scratch.path().join("stdout");
    let stderr_path = scratch.path().join("stderr");
    let stdout = std::fs::File::create(&stdout_path)?;
    let stderr = std::fs::File::create(&stderr_path)?;
    let mut command = Command::new("git");
    configure_git_command(&mut command);
    let mut child = command
        .arg("-C")
        .arg(repo)
        .args(args)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("spawn git {}", args.join(" ")))?;
    let status = loop {
        if verification_cancelled() {
            child.kill().context("kill cancelled verifier Git child")?;
            let _ = child.wait();
            bail!("artifact verification cancelled");
        }
        if let Some(status) = child.try_wait().context("poll verifier Git child")? {
            break status;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    let stdout_len = std::fs::metadata(&stdout_path)?.len();
    let stderr_len = std::fs::metadata(&stderr_path)?.len();
    if stdout_len > MAX_GIT_DIAGNOSTIC_BYTES || stderr_len > MAX_GIT_DIAGNOSTIC_BYTES {
        bail!("Git artifact verification emitted excessive output");
    }
    let stdout = std::fs::read(&stdout_path)?;
    let stderr = std::fs::read(&stderr_path)?;
    if !status.success() {
        bail!(
            "Git artifact verification failed: {}",
            String::from_utf8_lossy(&stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&stdout).trim().to_owned())
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
    use std::path::PathBuf;

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
                format_version: ARTIFACT_FORMAT_VERSION,
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
            let objects =
                crate::git::list_object_shas_with_depth(&self.repo, &self.first, None).unwrap();
            let mut level = FullHistoryLevel {
                tier: FULL_HISTORY_BASE_TIER,
                base_exclusive: vec![],
                tips: vec![self.first.clone()],
                packs: self.pairs(&self.first, None),
                proof: FullHistoryLevelProof::unsealed(),
            };
            let scratch = tempfile::tempdir().unwrap();
            CasCompletionVerifier::new(self.cas.clone())
                .verify_and_seal_history_level(
                    &mut level,
                    &objects,
                    &tokio_util::sync::CancellationToken::new(),
                    scratch.path(),
                )
                .unwrap();
            ArtifactManifest::new(
                &self.key(ArtifactKind::FullHistory),
                ArtifactPayload::FullHistory(FullHistoryArtifact {
                    target_commit_object: self.commit_blob(&self.second),
                    levels: vec![level],
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
                    gitlinks: vec![],
                }),
            )
            .unwrap()
        }

        fn verify(&self, manifest: &ArtifactManifest) -> Result<ArtifactManifest> {
            let evidence = manifest.store(&self.cas)?;
            CasCompletionVerifier::new(self.cas.clone()).verify_manifest(
                evidence.key(),
                evidence.manifest(),
                evidence.artifact_count(),
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
                levels: vec![],
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
        let mut manifest = ArtifactManifest::new(
            &key,
            ArtifactPayload::Files(FilesArtifact {
                target_commit_object: f.commit_blob(&commit),
                metadata: f.blob(&bytes),
                archive_chunks: vec![],
                zstd_dictionary: None,
                gitlinks: vec![],
            }),
        )
        .unwrap();
        f.verify(&manifest).unwrap();
        let ArtifactPayload::Files(payload) = &mut manifest.payload else {
            unreachable!()
        };
        payload.zstd_dictionary = Some(f.blob(b"unused"));
        manifest.semantic_digest =
            semantic_digest(manifest.schema_version, &manifest.key, &manifest.payload).unwrap();
        assert!(f.verify(&manifest).is_err());
    }

    #[test]
    fn multiple_empty_files_may_share_one_empty_frame() {
        let f = Fixture::new();
        fs::remove_file(f.repo.join("a.txt")).unwrap();
        fs::remove_file(f.repo.join("run.sh")).unwrap();
        fs::write(f.repo.join("empty-a"), b"").unwrap();
        fs::write(f.repo.join("empty-b"), b"").unwrap();
        git(&f.repo, &["add", "--all"]).unwrap();
        git(&f.repo, &["commit", "--quiet", "-m", "empty files"]).unwrap();
        let commit = git(&f.repo, &["rev-parse", "HEAD"]).unwrap();
        let compressed = zstd::encode_all(b"".as_slice(), 1).unwrap();
        let mut metadata = MetadataChunk::new();
        metadata.frames.push(FrameInfo {
            chunk_index: 0,
            chunk_offset: 0,
            compressed_len: compressed.len() as u32,
            raw_len: 0,
        });
        for path in [b"empty-a".as_slice(), b"empty-b".as_slice()] {
            metadata.files.push(FileEntry {
                path: path.to_vec(),
                mode: 0o100644,
                blob_sha1: hex::decode(git_object_oid("blob", b"")).unwrap(),
                fragments: vec![Fragment {
                    frame_index: 0,
                    frame_offset: 0,
                    raw_len: 0,
                }],
            });
        }
        let mut metadata_bytes = Vec::new();
        metadata.write(&mut metadata_bytes).unwrap();
        let key = ArtifactKey {
            commit: commit.clone(),
            ..f.key(ArtifactKind::Files)
        };
        let manifest = ArtifactManifest::new(
            &key,
            ArtifactPayload::Files(FilesArtifact {
                target_commit_object: f.commit_blob(&commit),
                metadata: f.blob(&metadata_bytes),
                archive_chunks: vec![f.blob(&compressed)],
                zstd_dictionary: None,
                gitlinks: vec![],
            }),
        )
        .unwrap();
        f.verify(&manifest).unwrap();
    }

    #[test]
    fn empty_file_may_share_a_nonempty_cdc_frame_boundary() {
        let f = Fixture::new();
        fs::write(f.repo.join("empty"), b"").unwrap();
        git(&f.repo, &["add", "empty"]).unwrap();
        git(&f.repo, &["commit", "--quiet", "-m", "add empty"]).unwrap();
        let target = git(&f.repo, &["rev-parse", "HEAD"]).unwrap();

        let ArtifactPayload::Files(mut payload) = f.files().payload else {
            unreachable!()
        };
        payload.target_commit_object = f.commit_blob(&target);
        let metadata_bytes = f.cas.get(&payload.metadata.hash).unwrap();
        let mut metadata = MetadataChunk::read(&mut metadata_bytes.as_slice()).unwrap();
        metadata.files.push(FileEntry {
            path: b"empty".to_vec(),
            mode: 0o100644,
            blob_sha1: hex::decode(git_object_oid("blob", b"")).unwrap(),
            fragments: vec![Fragment {
                frame_index: 0,
                frame_offset: 0,
                raw_len: 0,
            }],
        });
        let mut bytes = Vec::new();
        metadata.write(&mut bytes).unwrap();
        payload.metadata = f.blob(&bytes);
        let key = ArtifactKey {
            commit: target,
            ..f.key(ArtifactKind::Files)
        };
        let manifest = ArtifactManifest::new(&key, ArtifactPayload::Files(payload)).unwrap();
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
        wrong_schema.schema_version = ARTIFACT_MANIFEST_SCHEMA + 1;
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
                ..evidence.key().clone()
            },
            ArtifactKey {
                kind: ArtifactKind::Files,
                ..evidence.key().clone()
            },
            ArtifactKey {
                format_version: 1,
                ..evidence.key().clone()
            },
        ] {
            assert!(
                verifier
                    .verify_manifest(&wrong, evidence.manifest(), evidence.artifact_count())
                    .is_err()
            );
        }
        assert!(
            verifier
                .verify_manifest(
                    evidence.key(),
                    evidence.manifest(),
                    evidence.artifact_count() + 1
                )
                .is_err()
        );
    }

    #[test]
    fn persisted_v1_manifest_fails_closed_and_has_a_distinct_scheduler_key() {
        let f = Fixture::new();
        let current = f.head();
        let current_evidence = current.store(&f.cas).unwrap();
        let mut legacy = current;
        legacy.schema_version = 1;
        legacy.key.format_version = 1;
        legacy.semantic_digest =
            semantic_digest(legacy.schema_version, &legacy.key, &legacy.payload).unwrap();
        let hash = f.cas.put(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        let legacy_key = ArtifactKey {
            workspace: legacy.key.workspace.clone(),
            repo: legacy.key.repo.clone(),
            commit: legacy.key.commit.clone(),
            kind: legacy.key.kind,
            format_version: 1,
        };
        let legacy_evidence = CompletionEvidence::from_manifest(
            legacy_key.clone(),
            hash,
            legacy.payload.artifact_count(),
        )
        .unwrap();
        assert_ne!(legacy_key, *current_evidence.key());
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .verify_manifest(
                    &legacy_key,
                    legacy_evidence.manifest(),
                    legacy_evidence.artifact_count(),
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
        payload.levels.clear();
        history.semantic_digest =
            semantic_digest(history.schema_version, &history.key, &history.payload).unwrap();
        assert!(f.verify(&history).is_err());
    }

    #[test]
    fn history_rejects_noncanonical_tiers_ranges_and_tip_order() {
        let f = Fixture::new();

        let mut wrong_tier = f.history();
        let ArtifactPayload::FullHistory(payload) = &mut wrong_tier.payload else {
            unreachable!()
        };
        payload.levels[0].tier = 0;
        let wrong_tier =
            ArtifactManifest::new(&f.key(ArtifactKind::FullHistory), wrong_tier.payload).unwrap();
        assert!(f.verify(&wrong_tier).is_err());

        let mut wrong_tip = f.history();
        let ArtifactPayload::FullHistory(payload) = &mut wrong_tip.payload else {
            unreachable!()
        };
        payload.levels[0].tips = vec![f.second.clone()];
        let wrong_tip =
            ArtifactManifest::new(&f.key(ArtifactKind::FullHistory), wrong_tip.payload).unwrap();
        assert!(f.verify(&wrong_tip).is_err());

        let mut duplicate_tip = f.history();
        let ArtifactPayload::FullHistory(payload) = &mut duplicate_tip.payload else {
            unreachable!()
        };
        payload.levels[0].tips.push(f.first.clone());
        let duplicate_tip =
            ArtifactManifest::new(&f.key(ArtifactKind::FullHistory), duplicate_tip.payload)
                .unwrap();
        assert!(f.verify(&duplicate_tip).is_err());
    }

    #[test]
    fn history_level_receipts_survive_restart_and_reject_rotation_or_corruption() {
        let f = Fixture::new();
        let key_a = [b'a'; 32];
        let key_b = [b'b'; 32];
        let identity_a = CasCompletionVerifier::new(f.cas.clone())
            .with_proof_key(&key_a)
            .unwrap()
            .identity()
            .to_owned();
        let identity_b = CasCompletionVerifier::new(f.cas.clone())
            .with_proof_key(&key_b)
            .unwrap()
            .identity()
            .to_owned();
        assert_ne!(identity_a, identity_b, "rotated keys must fence the fleet");
        assert!(!identity_a.contains(std::str::from_utf8(&key_a).unwrap()));
        let mut manifest = f.history();
        let ArtifactPayload::FullHistory(history) = &mut manifest.payload else {
            panic!("wrong payload")
        };
        let objects = crate::git::list_object_shas_with_depth(&f.repo, &f.first, None).unwrap();
        let scratch = tempfile::tempdir().unwrap();
        CasCompletionVerifier::new(f.cas.clone())
            .with_proof_key(&key_a)
            .unwrap()
            .verify_and_seal_history_level(
                &mut history.levels[0],
                &objects,
                &tokio_util::sync::CancellationToken::new(),
                scratch.path(),
            )
            .unwrap();
        manifest.semantic_digest =
            semantic_digest(manifest.schema_version, &manifest.key, &manifest.payload).unwrap();
        let evidence = manifest.store(&f.cas).unwrap();

        // A fresh verifier process with the same dedicated key validates only
        // the durable receipt/envelope.
        CasCompletionVerifier::new(f.cas.clone())
            .with_proof_key(&key_a)
            .unwrap()
            .verify_manifest(
                evidence.key(),
                evidence.manifest(),
                evidence.artifact_count(),
            )
            .unwrap();
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .with_proof_key(&key_b)
                .unwrap()
                .verify_manifest(
                    evidence.key(),
                    evidence.manifest(),
                    evidence.artifact_count()
                )
                .is_err()
        );

        let mut corrupt = manifest;
        let ArtifactPayload::FullHistory(history) = &mut corrupt.payload else {
            unreachable!()
        };
        history.levels[0].proof.seal.replace_range(0..2, "00");
        corrupt.semantic_digest =
            semantic_digest(corrupt.schema_version, &corrupt.key, &corrupt.payload).unwrap();
        let corrupt = corrupt.store(&f.cas).unwrap();
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .with_proof_key(&key_a)
                .unwrap()
                .verify_manifest(corrupt.key(), corrupt.manifest(), corrupt.artifact_count())
                .is_err()
        );
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
                levels: vec![FullHistoryLevel {
                    tier: FULL_HISTORY_BASE_TIER,
                    base_exclusive: vec![],
                    tips: vec![f.first.clone()],
                    packs: f.pairs(&f.first, None),
                    proof: FullHistoryLevelProof::unsealed(),
                }],
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
    fn canonical_manifest_rejects_reordered_or_whitespace_json() {
        let f = Fixture::new();
        let manifest = f.head();
        let pretty = serde_json::to_vec_pretty(&manifest).unwrap();
        let hash = f.cas.put(&pretty).unwrap();
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .verify_manifest(
                    &f.key(ArtifactKind::Head),
                    &hash,
                    manifest.payload.artifact_count(),
                )
                .is_err()
        );

        let value = serde_json::to_value(&manifest).unwrap();
        let object = value.as_object().unwrap();
        let reordered = serde_json::json!({
            "payload": object["payload"].clone(),
            "semantic_digest": object["semantic_digest"].clone(),
            "key": object["key"].clone(),
            "schema_version": object["schema_version"].clone(),
        });
        let hash = f.cas.put(&serde_json::to_vec(&reordered).unwrap()).unwrap();
        assert!(
            CasCompletionVerifier::new(f.cas.clone())
                .verify_manifest(
                    &f.key(ArtifactKind::Head),
                    &hash,
                    manifest.payload.artifact_count(),
                )
                .is_err()
        );
    }

    #[test]
    fn duplicate_pack_descriptors_and_overlapping_distinct_packs_are_rejected() {
        let f = Fixture::new();
        let mut identical = f.head();
        let ArtifactPayload::Head(payload) = &mut identical.payload else {
            unreachable!()
        };
        payload.packs.push(payload.packs[0].clone());
        identical.semantic_digest =
            semantic_digest(identical.schema_version, &identical.key, &identical.payload).unwrap();
        assert!(f.verify(&identical).is_err());

        let mut overlapping = f.head();
        let tuple = PackBuilder::new(&f.repo, &f.cas)
            .build_object_set_packs(std::slice::from_ref(&f.second), 1024 * 1024, true)
            .unwrap()
            .remove(0);
        let ArtifactPayload::Head(payload) = &mut overlapping.payload else {
            unreachable!()
        };
        payload.packs.push(GitPackPair {
            pack: CasBlob {
                hash: tuple.0,
                len: tuple.1,
            },
            index: CasBlob {
                hash: tuple.2,
                len: tuple.3,
            },
        });
        overlapping.semantic_digest = semantic_digest(
            overlapping.schema_version,
            &overlapping.key,
            &overlapping.payload,
        )
        .unwrap();
        assert!(f.verify(&overlapping).is_err());
    }

    #[test]
    fn files_reject_uncovered_raw_frame_byte() {
        let f = Fixture::new();
        let mut manifest = f.files();
        let ArtifactPayload::Files(payload) = &mut manifest.payload else {
            unreachable!()
        };
        let metadata_bytes = f.cas.get(&payload.metadata.hash).unwrap();
        let mut metadata = MetadataChunk::read(&mut metadata_bytes.as_slice()).unwrap();
        let archive = f.cas.get(&payload.archive_chunks[0].hash).unwrap();
        let first = &metadata.frames[0];
        let first_end = first.chunk_offset as usize + first.compressed_len as usize;
        let new_first = zstd::encode_all(b"two\nX".as_slice(), 1).unwrap();
        let mut rebuilt = new_first.clone();
        rebuilt.extend_from_slice(&archive[first_end..]);
        let delta = new_first.len() as i64 - first.compressed_len as i64;
        metadata.frames[0].compressed_len = new_first.len() as u32;
        metadata.frames[0].raw_len += 1;
        metadata.frames[1].chunk_offset = (metadata.frames[1].chunk_offset as i64 + delta) as u64;
        let mut bytes = Vec::new();
        metadata.write(&mut bytes).unwrap();
        payload.metadata = f.blob(&bytes);
        payload.archive_chunks[0] = f.blob(&rebuilt);
        manifest.semantic_digest =
            semantic_digest(manifest.schema_version, &manifest.key, &manifest.payload).unwrap();
        assert!(f.verify(&manifest).is_err());
    }

    #[test]
    fn files_reject_unused_dictionary_and_support_exact_gitlinks() {
        let f = Fixture::new();
        let mut unused = f.files();
        let ArtifactPayload::Files(payload) = &mut unused.payload else {
            unreachable!()
        };
        payload.zstd_dictionary = Some(f.blob(b"not a trained dictionary"));
        unused.semantic_digest =
            semantic_digest(unused.schema_version, &unused.key, &unused.payload).unwrap();
        assert!(f.verify(&unused).is_err());

        git(
            &f.repo,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("160000,{},vendor/sub", f.first),
            ],
        )
        .unwrap();
        git(&f.repo, &["commit", "--quiet", "-m", "gitlink"]).unwrap();
        let target = git(&f.repo, &["rev-parse", "HEAD"]).unwrap();
        let key = ArtifactKey {
            commit: target.clone(),
            ..f.key(ArtifactKind::Files)
        };
        let ArtifactPayload::Files(mut payload) = f.files().payload else {
            unreachable!()
        };
        payload.target_commit_object = f.commit_blob(&target);
        payload.gitlinks.push(GitlinkEntry {
            path: b"vendor/sub".to_vec(),
            commit: f.first.clone(),
        });
        let manifest = ArtifactManifest::new(&key, ArtifactPayload::Files(payload)).unwrap();
        f.verify(&manifest).unwrap();
    }

    #[test]
    fn files_accept_canonical_dictionary_used_by_every_frame() {
        let f = Fixture::new();
        let samples = (0..256)
            .map(|index| {
                format!("shared-prefix-ripclone-dictionary-sample-{index:04}-two-shell-exit-zero\n")
                    .into_bytes()
            })
            .collect::<Vec<_>>();
        let sample_refs = samples.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let dictionary = zstd::dict::from_samples(&sample_refs, 1024).unwrap();
        assert!(zstd::zstd_safe::get_dict_id_from_dict(&dictionary).is_some());

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
            let mut encoder =
                zstd::stream::Encoder::with_dictionary(Vec::new(), 1, &dictionary).unwrap();
            encoder.write_all(content).unwrap();
            let compressed = encoder.finish().unwrap();
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
        let manifest = ArtifactManifest::new(
            &f.key(ArtifactKind::Files),
            ArtifactPayload::Files(FilesArtifact {
                target_commit_object: f.commit_blob(&f.second),
                metadata: f.blob(&metadata_bytes),
                archive_chunks: vec![f.blob(&archive)],
                zstd_dictionary: Some(f.blob(&dictionary)),
                gitlinks: vec![],
            }),
        )
        .unwrap();
        f.verify(&manifest).unwrap();
    }

    #[test]
    fn configured_limits_fail_before_large_payload_materialization() {
        let f = Fixture::new();
        let evidence = f.head().store(&f.cas).unwrap();
        let limits = ArtifactVerificationLimits {
            manifest_bytes: 1,
            ..ArtifactVerificationLimits::default()
        };
        let verifier = CasCompletionVerifier::with_limits(f.cas.clone(), limits).unwrap();
        assert_ne!(verifier.identity(), PRODUCTION_VERIFIER_IDENTITY);
        assert!(
            verifier
                .verify_manifest(
                    evidence.key(),
                    evidence.manifest(),
                    evidence.artifact_count()
                )
                .is_err()
        );

        let evidence = f.head().store(&f.cas).unwrap();
        let limits = ArtifactVerificationLimits {
            git_objects: 1,
            ..ArtifactVerificationLimits::default()
        };
        let verifier = CasCompletionVerifier::with_limits(f.cas.clone(), limits).unwrap();
        assert!(
            verifier
                .verify_manifest(
                    evidence.key(),
                    evidence.manifest(),
                    evidence.artifact_count()
                )
                .is_err()
        );
    }

    #[test]
    fn metadata_repeated_counts_are_rejected_before_prost_allocation() {
        let limits = ArtifactVerificationLimits {
            files: 4,
            frames: 3,
            fragments: 2,
            ..ArtifactVerificationLimits::default()
        };

        // Five zero-length FileEntry messages occupy only ten bytes but would
        // force prost to allocate five FileEntry values before a post-decode
        // limit could observe them.
        let mut too_many_files = Vec::new();
        for _ in 0..5 {
            too_many_files.extend_from_slice(&[0x2a, 0x00]);
        }
        assert!(
            preflight_metadata_counts(&too_many_files, &limits)
                .unwrap_err()
                .to_string()
                .contains("file count exceeds")
        );

        let too_many_frames = [0x22, 0x00, 0x22, 0x00, 0x22, 0x00, 0x22, 0x00];
        assert!(preflight_metadata_counts(&too_many_frames, &limits).is_err());

        // One FileEntry containing three zero-length Fragment messages.
        let too_many_fragments = [0x2a, 0x06, 0x22, 0x00, 0x22, 0x00, 0x22, 0x00];
        assert!(preflight_metadata_counts(&too_many_fragments, &limits).is_err());
        assert!(preflight_metadata_counts(&[0x2a, 0x80], &limits).is_err());
        assert!(
            preflight_metadata_counts(
                &[
                    0x2a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02
                ],
                &limits,
            )
            .is_err()
        );
    }

    #[test]
    fn verifier_identity_and_scheduler_claim_binding_are_stable() {
        let f = Fixture::new();
        let evidence = f.head().store(&f.cas).unwrap();
        let verifier = CasCompletionVerifier::new(f.cas.clone());
        assert!(
            verifier
                .identity()
                .starts_with(PRODUCTION_VERIFIER_IDENTITY)
        );
        assert!(verifier.identity().contains(":proof:"));
        let claim = ClaimedArtifact {
            record: ArtifactRecord {
                id: 1,
                key: evidence.key().clone(),
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
        let mut wrong_key = evidence.key().clone();
        wrong_key.repo = "other/repo".into();
        let wrong = CompletionEvidence::from_manifest(
            wrong_key,
            evidence.manifest(),
            evidence.artifact_count(),
        )
        .unwrap();
        assert!(verifier.verify(&claim, &wrong).is_err());
    }

    #[test]
    fn from_env_child_probe() {
        let Ok(mode) = std::env::var("RIPCLONE_PROOF_KEY_TEST_PROBE") else {
            return;
        };
        let root = tempfile::tempdir().unwrap();
        let cas = Cas::new(root.path().join("cas")).unwrap();
        match mode.as_str() {
            "missing" | "short" => assert!(CasCompletionVerifier::from_env(cas).is_err()),
            "valid" => assert!(CasCompletionVerifier::from_env(cas).is_ok()),
            _ => panic!("unknown proof-key probe mode"),
        }
    }

    #[test]
    fn production_constructor_strictly_requires_dedicated_proof_key() {
        let executable = std::env::current_exe().unwrap();
        for (mode, key) in [
            ("missing", None),
            ("short", Some("too-short")),
            ("valid", Some("a-dedicated-artifact-proof-key-of-32-bytes")),
        ] {
            let mut command = std::process::Command::new(&executable);
            command
                .args([
                    "--exact",
                    "artifact_manifest::tests::from_env_child_probe",
                    "--nocapture",
                ])
                .env("RIPCLONE_PROOF_KEY_TEST_PROBE", mode)
                .env_remove("RIPCLONE_ARTIFACT_PROOF_KEY");
            if let Some(key) = key {
                command.env("RIPCLONE_ARTIFACT_PROOF_KEY", key);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{mode} proof-key subprocess failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
