//! Immutable source roots for normalized synchronization.
//!
//! A source snapshot is the exact verified Head + FullHistory pair for one
//! commit. Head owns the target commit/tree/blobs and FullHistory owns every
//! parent closure, so the pair can reconstruct a complete Git object store on
//! any worker without consulting a mutable provider mirror.

use crate::artifact_scheduler::{ArtifactKind, CompletionEvidence, VerifiedCompletionEvidence};
use crate::cas::Cas;
use crate::sync_coordinator::DurableSourceSnapshot;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SOURCE_SNAPSHOT_SCHEMA: u32 = 1;
const DIGEST_DOMAIN: &[u8] = b"ripclone-source-snapshot-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceArtifactRef {
    pub manifest: String,
    pub artifact_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSnapshotManifest {
    pub schema_version: u32,
    pub workspace: String,
    pub repo: String,
    pub commit: String,
    pub format_version: u32,
    pub head: SourceArtifactRef,
    pub history: SourceArtifactRef,
    pub semantic_digest: String,
}

impl SourceSnapshotManifest {
    pub fn from_verified_pair(
        head: &VerifiedCompletionEvidence,
        history: &VerifiedCompletionEvidence,
    ) -> Result<Self> {
        Self::from_evidence_pair(head.evidence(), history.evidence())
    }

    fn from_evidence_pair(head: &CompletionEvidence, history: &CompletionEvidence) -> Result<Self> {
        validate_pair(head, history)?;
        let key = head.key();
        let mut manifest = Self {
            schema_version: SOURCE_SNAPSHOT_SCHEMA,
            workspace: key.workspace.clone(),
            repo: key.repo.clone(),
            commit: key.commit.clone(),
            format_version: key.format_version,
            head: SourceArtifactRef {
                manifest: head.manifest().to_owned(),
                artifact_count: head.artifact_count(),
            },
            history: SourceArtifactRef {
                manifest: history.manifest().to_owned(),
                artifact_count: history.artifact_count(),
            },
            semantic_digest: String::new(),
        };
        manifest.semantic_digest = manifest.compute_digest()?;
        manifest.validate_envelope()?;
        Ok(manifest)
    }

    pub fn validate_envelope(&self) -> Result<()> {
        if self.schema_version != SOURCE_SNAPSHOT_SCHEMA {
            bail!("unsupported source snapshot schema")
        }
        if self.workspace.trim().is_empty() || self.repo.trim().is_empty() {
            bail!("source snapshot identity is empty")
        }
        crate::artifact_scheduler::validate_canonical_commit_oid(&self.commit)?;
        if self.format_version == 0 {
            bail!("source snapshot format version is zero")
        }
        for (role, artifact) in [("Head", &self.head), ("FullHistory", &self.history)] {
            Cas::validate_artifact_id(&artifact.manifest)
                .with_context(|| format!("invalid {role} source manifest"))?;
            if artifact.artifact_count == 0 {
                bail!("{role} source artifact count is zero")
            }
        }
        if self.head.manifest == self.history.manifest {
            bail!("source snapshot aliases Head and FullHistory roots")
        }
        let expected = self.compute_digest()?;
        if !constant_time_eq(expected.as_bytes(), self.semantic_digest.as_bytes()) {
            bail!("source snapshot semantic digest mismatch")
        }
        Ok(())
    }

    /// Store the canonical small root locally. Its children must already have
    /// passed strict artifact verification and durable publication; remote root
    /// upload is the source-acquirer commit point layered above this primitive.
    pub fn store_local(&self, cas: &Cas) -> Result<DurableSourceSnapshot> {
        self.validate_envelope()?;
        let bytes = serde_json::to_vec(self)?;
        let manifest = cas.put(&bytes)?;
        Ok(DurableSourceSnapshot {
            workspace: self.workspace.clone(),
            repo: self.repo.clone(),
            commit: self.commit.clone(),
            manifest,
        })
    }

    pub fn decode_canonical(bytes: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(bytes).context("decode source snapshot")?;
        if serde_json::to_vec(&manifest)? != bytes {
            bail!("source snapshot JSON is not canonical")
        }
        manifest.validate_envelope()?;
        Ok(manifest)
    }

    fn compute_digest(&self) -> Result<String> {
        let mut digest = Sha256::new();
        digest.update(DIGEST_DOMAIN);
        digest_part(&mut digest, &self.schema_version.to_be_bytes())?;
        digest_part(&mut digest, self.workspace.as_bytes())?;
        digest_part(&mut digest, self.repo.as_bytes())?;
        digest_part(&mut digest, self.commit.as_bytes())?;
        digest_part(&mut digest, &self.format_version.to_be_bytes())?;
        digest_part(&mut digest, self.head.manifest.as_bytes())?;
        digest_part(&mut digest, &self.head.artifact_count.to_be_bytes())?;
        digest_part(&mut digest, self.history.manifest.as_bytes())?;
        digest_part(&mut digest, &self.history.artifact_count.to_be_bytes())?;
        Ok(hex::encode(digest.finalize()))
    }
}

fn validate_pair(head: &CompletionEvidence, history: &CompletionEvidence) -> Result<()> {
    if head.key().kind != ArtifactKind::Head || history.key().kind != ArtifactKind::FullHistory {
        bail!("source snapshot requires exact Head + FullHistory evidence")
    }
    if head.key().workspace != history.key().workspace
        || head.key().repo != history.key().repo
        || head.key().commit != history.key().commit
        || head.key().format_version != history.key().format_version
    {
        bail!("source snapshot artifact identities do not match")
    }
    if head.key().workspace.trim().is_empty()
        || head.key().repo.trim().is_empty()
        || head.key().format_version == 0
    {
        bail!("source snapshot artifact identity is invalid")
    }
    crate::artifact_scheduler::validate_canonical_commit_oid(&head.key().commit)?;
    for evidence in [head, history] {
        Cas::validate_artifact_id(evidence.manifest())?;
        if evidence.artifact_count() == 0 {
            bail!("source snapshot evidence contains no artifacts")
        }
    }
    if head.manifest() == history.manifest() {
        bail!("source snapshot aliases Head and FullHistory roots")
    }
    Ok(())
}

fn digest_part(digest: &mut Sha256, value: &[u8]) -> Result<()> {
    let len = u64::try_from(value.len()).context("source snapshot digest component too large")?;
    digest.update(len.to_be_bytes());
    digest.update(value);
    Ok(())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_scheduler::{ArtifactKey, ArtifactKind};

    const COMMIT: &str = "1111111111111111111111111111111111111111";
    const HEAD_ROOT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HISTORY_ROOT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn evidence(kind: ArtifactKind, root: &str, count: u64) -> CompletionEvidence {
        CompletionEvidence::from_manifest(
            ArtifactKey {
                workspace: "acme".into(),
                repo: "owner/repo".into(),
                commit: COMMIT.into(),
                kind,
                format_version: 2,
            },
            root,
            count,
        )
        .unwrap()
    }

    #[test]
    fn exact_pair_round_trips_canonically_through_cas() {
        let temp = tempfile::tempdir().unwrap();
        let cas = Cas::new(temp.path()).unwrap();
        let source = SourceSnapshotManifest::from_evidence_pair(
            &evidence(ArtifactKind::Head, HEAD_ROOT, 3),
            &evidence(ArtifactKind::FullHistory, HISTORY_ROOT, 7),
        )
        .unwrap();
        let receipt = source.store_local(&cas).unwrap();
        assert_eq!(receipt.workspace, "acme");
        assert_eq!(receipt.repo, "owner/repo");
        assert_eq!(receipt.commit, COMMIT);
        assert_eq!(
            SourceSnapshotManifest::decode_canonical(&cas.get(&receipt.manifest).unwrap()).unwrap(),
            source
        );
    }

    #[test]
    fn wrong_kind_identity_alias_and_semantic_mutations_fail_closed() {
        let head = evidence(ArtifactKind::Head, HEAD_ROOT, 3);
        assert!(
            SourceSnapshotManifest::from_evidence_pair(
                &evidence(ArtifactKind::Files, HEAD_ROOT, 3),
                &evidence(ArtifactKind::FullHistory, HISTORY_ROOT, 7),
            )
            .is_err()
        );
        assert!(
            SourceSnapshotManifest::from_evidence_pair(
                &head,
                &CompletionEvidence::from_manifest(
                    ArtifactKey {
                        repo: "other/repo".into(),
                        ..head.key().clone()
                    },
                    HISTORY_ROOT,
                    7,
                )
                .unwrap(),
            )
            .is_err()
        );
        assert!(
            SourceSnapshotManifest::from_evidence_pair(
                &head,
                &evidence(ArtifactKind::FullHistory, HEAD_ROOT, 7),
            )
            .is_err()
        );

        let mut source = SourceSnapshotManifest::from_evidence_pair(
            &head,
            &evidence(ArtifactKind::FullHistory, HISTORY_ROOT, 7),
        )
        .unwrap();
        source.history.artifact_count += 1;
        assert!(source.validate_envelope().is_err());
        source.history.artifact_count -= 1;
        source.semantic_digest = "0".repeat(64);
        assert!(source.validate_envelope().is_err());
    }

    #[test]
    fn unknown_fields_whitespace_and_invalid_hashes_are_rejected() {
        let source = SourceSnapshotManifest::from_evidence_pair(
            &evidence(ArtifactKind::Head, HEAD_ROOT, 3),
            &evidence(ArtifactKind::FullHistory, HISTORY_ROOT, 7),
        )
        .unwrap();
        let canonical = serde_json::to_vec(&source).unwrap();
        let mut whitespace = canonical.clone();
        whitespace.push(b'\n');
        assert!(SourceSnapshotManifest::decode_canonical(&whitespace).is_err());
        let mut value: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
        value["extra"] = serde_json::json!(true);
        assert!(
            SourceSnapshotManifest::decode_canonical(&serde_json::to_vec(&value).unwrap()).is_err()
        );
        let mut invalid = source;
        invalid.head.manifest = "../escape".into();
        assert!(invalid.validate_envelope().is_err());
    }
}
